"""
xray_tracing.py
===============
Distributed tracing for the local multi-agent run - AWS X-Ray, no daemon.

Why this module exists
----------------------
`configure_observability()` in agent_orchestrator.py enables X-Ray on the
*AgentCore Runtime*, but `python src/agent_orchestrator.py test` runs the agent
graph in-process - it never calls the deployed runtime. Nothing in that path
emits a trace, so the X-Ray Service Map stays empty.

This module closes that gap by writing X-Ray segment documents directly from
the process that actually runs the agents, using the same public API the X-Ray
SDK targets (`xray:PutTraceSegments`). No daemon, no OTel collector, no extra
dependency - boto3 is already required.

How the Service Map graph is built
----------------------------------
X-Ray draws an edge between two service nodes when:

  1. the caller records a subsegment with `"namespace": "remote"`, and
  2. the callee records a *separate full segment* carrying the same `trace_id`
     and a `parent_id` equal to that subsegment's id.

`traced_call()` does both halves in one context manager, so each instrumented
hop becomes its own node in the map:

    NovaMart-Orchestrator
      |-- NovaMart-InventoryAgent
      |-- NovaMart-PolicyAgent
      |     |-- NovaMart-ReturnsKB
      |     |-- NovaMart-ShippingKB
      |     `-- NovaMart-WarrantyKB
      |-- NovaMart-RefundAgent
      |-- NovaMart-CommunicationAgent
      `-- DynamoDB          (aws-namespace subsegment)

Context propagation
-------------------
The active segment lives in a `contextvars.ContextVar`. Strands' sync entry
point copies the current context into its worker thread
(`strands/_async.py` - `executor.submit(contextvars.copy_context().run, ...)`),
and `asyncio.to_thread` does the same for sync tools, so nesting survives every
hop the SDK makes on its own. The one place that does NOT propagate is a plain
`ThreadPoolExecutor.submit`, so `search_all_policies()` hands its context to
the retriever threads explicitly via `contextvars.copy_context()`.

Environment variables
---------------------
    XRAY_TRACING         '0'/'false'/'no'/'off' disables tracing entirely;
                         every context manager below becomes a no-op.
    XRAY_SERVICE_PREFIX  Node-name prefix in the Service Map (default NovaMart).
    AWS_REGION           Region the segments are sent to (default us-east-1).

Tracing is strictly best-effort: every emission path is wrapped so that a
failure to trace can never break an agent run.
"""

import atexit
import contextvars
import json
import logging
import os
import queue
import re
import secrets
import threading
import time
from contextlib import contextmanager
from typing import Iterator, Optional

import boto3
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────

# Settings below are read once at import time, so .env has to be loaded before
# that - this module must not depend on config.py having been imported first.
# load_dotenv() is idempotent and never overrides real environment variables.
load_dotenv()

AWS_REGION = os.environ.get('AWS_REGION', 'us-east-1')

_DISABLED_VALUES = ('0', 'false', 'no', 'off')
ENABLED = os.environ.get('XRAY_TRACING', '1').strip().lower() not in _DISABLED_VALUES

SERVICE_PREFIX = os.environ.get('XRAY_SERVICE_PREFIX', 'NovaMart').strip() or 'NovaMart'

# X-Ray annotation keys must be alphanumeric/underscore; values are indexed and
# searchable in the console, so they are kept short.
_ANNOTATION_KEY_CHARS = re.compile(r'[^A-Za-z0-9_]')
_ANNOTATION_MAX_LEN = 250


def node_name(suffix: str) -> str:
    """Service-map node name for one agent: 'PolicyAgent' -> 'NovaMart-PolicyAgent'."""
    return f"{SERVICE_PREFIX}-{suffix}"


def new_trace_id() -> str:
    """Generate an X-Ray trace id: 1-<8 hex epoch seconds>-<24 hex random>."""
    return f"1-{int(time.time()):08x}-{secrets.token_hex(12)}"


def _new_id() -> str:
    """Generate a 16-hex-character segment / subsegment id."""
    return secrets.token_hex(8)


def console_url(trace_id: str) -> str:
    """Deep link to one trace in the X-Ray console."""
    return (f"https://{AWS_REGION}.console.aws.amazon.com/cloudwatch/home"
            f"?region={AWS_REGION}#xray:traces/{trace_id}")


def service_map_url() -> str:
    """Deep link to the X-Ray Service Map - the rubric deliverable."""
    return (f"https://{AWS_REGION}.console.aws.amazon.com/cloudwatch/home"
            f"?region={AWS_REGION}#xray:service-map/map")


# ─────────────────────────────────────────────────────
# SEGMENT SHIPPER
# ─────────────────────────────────────────────────────

class _Shipper:
    """
    Background sender for finished segment documents.

    Segments are queued and shipped by a single daemon thread so PutTraceSegments
    latency never lands on the agent's critical path. The boto3 client is created
    lazily on first use - importing this module must not touch AWS.
    """

    _MAX_BATCH = 20        # PutTraceSegments accepts multiple documents per call

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self._client = None
        self._thread: Optional[threading.Thread] = None
        self._sending = False
        self._lock = threading.Lock()

    def _ensure_started(self) -> None:
        with self._lock:
            if self._thread is not None:
                return
            self._client = boto3.client('xray', region_name=AWS_REGION)
            self._thread = threading.Thread(
                target=self._run, name='xray-shipper', daemon=True)
            self._thread.start()
            atexit.register(self.flush)

    def submit(self, document: dict) -> None:
        """Queue one finished segment document. Never raises."""
        try:
            self._ensure_started()
            self._queue.put(json.dumps(document))
        except Exception as exc:                       # pragma: no cover - defensive
            logger.debug("X-Ray: could not queue segment: %s", exc)

    def _run(self) -> None:
        while True:
            batch = [self._queue.get()]
            try:
                while len(batch) < self._MAX_BATCH:
                    batch.append(self._queue.get_nowait())
            except queue.Empty:
                pass
            self._sending = True
            try:
                self._send(batch)
            finally:
                self._sending = False

    def _send(self, batch: list) -> None:
        try:
            response = self._client.put_trace_segments(
                TraceSegmentDocuments=batch)
            rejected = response.get('UnprocessedTraceSegments', [])
            if rejected:
                logger.debug("X-Ray rejected %d segment(s): %s",
                             len(rejected), rejected)
        except Exception as exc:
            logger.debug("X-Ray: put_trace_segments failed: %s", exc)

    def flush(self, timeout: float = 5.0) -> None:
        """Block until the queue drains (or timeout). Registered with atexit."""
        if self._thread is None:
            return
        deadline = time.time() + timeout
        while (not self._queue.empty() or self._sending) and time.time() < deadline:
            time.sleep(0.05)


_SHIPPER = _Shipper()


def flush(timeout: float = 5.0) -> None:
    """Wait for queued segments to reach X-Ray. Called automatically at exit."""
    _SHIPPER.flush(timeout)


# ─────────────────────────────────────────────────────
# SEGMENTS
# ─────────────────────────────────────────────────────

class _Segment:
    """One X-Ray segment (a service node) or subsegment (work inside a node)."""

    def __init__(self, name: str, trace_id: str, parent_id: str = None,
                 is_subsegment: bool = False, namespace: str = None,
                 aws: dict = None) -> None:
        self.name = name
        self.id = _new_id()
        self.trace_id = trace_id
        self.parent_id = parent_id
        self.is_subsegment = is_subsegment
        self.namespace = namespace
        self.aws = aws or None
        self.start_time = time.time()
        self.end_time: Optional[float] = None
        self.annotations: dict = {}
        self.metadata: dict = {}
        self._subsegments: list = []
        self._fault: Optional[dict] = None
        self._lock = threading.Lock()

    # ── recording ──────────────────────────────────────────────────────────

    def annotate(self, **values) -> None:
        """Add indexed, searchable key/value pairs to this segment."""
        for key, value in values.items():
            if value is None:
                continue
            key = _ANNOTATION_KEY_CHARS.sub('_', key)
            if not isinstance(value, (str, int, float, bool)):
                value = str(value)
            if isinstance(value, str):
                value = value[:_ANNOTATION_MAX_LEN]
            self.annotations[key] = value

    def add_metadata(self, **values) -> None:
        """Add non-indexed detail (visible when a node is selected in the console)."""
        self.metadata.update({k: v for k, v in values.items() if v is not None})

    def record_exception(self, exc: BaseException) -> None:
        """Mark this node as faulting so the map draws the edge in red."""
        self._fault = {
            'working_directory': os.getcwd(),
            'exceptions': [{
                'id': _new_id(),
                'type': type(exc).__name__,
                'message': str(exc)[:1000],
            }],
        }

    def add_child(self, child: '_Segment') -> None:
        with self._lock:
            self._subsegments.append(child)

    def close(self) -> None:
        if self.end_time is None:
            self.end_time = time.time()

    # ── serialisation ──────────────────────────────────────────────────────

    def document(self) -> dict:
        doc = {
            'name': self.name,
            'id': self.id,
            'start_time': self.start_time,
            'end_time': self.end_time if self.end_time is not None else time.time(),
        }
        # Subsegments are embedded in their parent's document and inherit its
        # trace_id; a downstream *segment* carries trace_id + parent_id itself.
        if not self.is_subsegment:
            doc['trace_id'] = self.trace_id
        if self.parent_id:
            doc['parent_id'] = self.parent_id
        if self.namespace:
            doc['namespace'] = self.namespace
        if self.aws:
            doc['aws'] = self.aws
        if self.annotations:
            doc['annotations'] = self.annotations
        if self.metadata:
            doc['metadata'] = {'default': self.metadata}
        with self._lock:
            if self._subsegments:
                doc['subsegments'] = [s.document() for s in self._subsegments]
        if self._fault:
            doc['fault'] = True
            doc['cause'] = self._fault
        return doc


class _NullSegment:
    """No-op stand-in yielded when tracing is disabled or no trace is active."""

    id = None
    trace_id = None

    def annotate(self, **values) -> None:
        pass

    def add_metadata(self, **values) -> None:
        pass

    def record_exception(self, exc: BaseException) -> None:
        pass


_NULL = _NullSegment()

# The segment currently being recorded. Copied into Strands' worker threads by
# the SDK itself; handed to our own ThreadPoolExecutor explicitly.
_current: contextvars.ContextVar = contextvars.ContextVar(
    'xray_current_segment', default=None)


def current_segment():
    """The segment being recorded on this context, or None."""
    return _current.get() if ENABLED else None


# ─────────────────────────────────────────────────────
# PUBLIC CONTEXT MANAGERS
# ─────────────────────────────────────────────────────

@contextmanager
def segment(name: str, *, link: tuple = None, annotations: dict = None) -> Iterator:
    """
    Record one service node.

    Args:
        name:        Service-map node name (use node_name()).
        link:        (trace_id, parent_id) from downstream(); omit to start a
                     new trace with this node as the root.
        annotations: Indexed key/values attached to the segment.
    """
    if not ENABLED:
        yield _NULL
        return

    trace_id, parent_id = link if link else (new_trace_id(), None)
    seg = _Segment(name, trace_id=trace_id, parent_id=parent_id)
    if annotations:
        seg.annotate(**annotations)

    token = _current.set(seg)
    try:
        yield seg
    except BaseException as exc:
        seg.record_exception(exc)
        raise
    finally:
        _current.reset(token)
        seg.close()
        _SHIPPER.submit(seg.document())


@contextmanager
def subsegment(name: str, *, namespace: str = None, aws: dict = None,
               annotations: dict = None) -> Iterator:
    """Record work inside the current segment. No-op when no trace is active."""
    parent = current_segment()
    if parent is None:
        yield _NULL
        return

    sub = _Segment(name, trace_id=parent.trace_id, is_subsegment=True,
                   namespace=namespace, aws=aws)
    if annotations:
        sub.annotate(**annotations)

    token = _current.set(sub)
    try:
        yield sub
    except BaseException as exc:
        sub.record_exception(exc)
        raise
    finally:
        _current.reset(token)
        sub.close()
        parent.add_child(sub)


@contextmanager
def traced_call(name: str, annotations: dict = None) -> Iterator:
    """
    Record a call from the current node into a downstream service node.

    Writes the caller-side `remote` subsegment and the callee-side segment
    together - the pair X-Ray needs to draw an edge in the Service Map.

        with traced_call(node_name('PolicyAgent'), {'session_id': sid}):
            result = policy_agent(prompt)
    """
    with subsegment(name, namespace='remote') as hop:
        if hop.id is None:               # tracing off, or called outside a turn
            yield _NULL
            return
        with segment(name, link=(hop.trace_id, hop.id),
                     annotations=annotations) as seg:
            yield seg


@contextmanager
def aws_subsegment(name: str, *, operation: str = None, **aws_fields) -> Iterator:
    """
    Record a call to an AWS service so it appears as an AWS node in the map.

        with aws_subsegment('DynamoDB', operation='UpdateItem',
                            table_name=config.WORKFLOW_STATE_TABLE):
            ...
    """
    aws = {k: v for k, v in aws_fields.items() if v is not None}
    if operation:
        aws['operation'] = operation
    aws.setdefault('region', AWS_REGION)
    with subsegment(name, namespace='aws', aws=aws) as sub:
        yield sub


@contextmanager
def traced_turn(session_id: str, customer_id: str = None,
                request: str = None) -> Iterator:
    """
    Open the root segment for one customer request.

    Wraps a whole orchestrator() call, so every agent hop recorded underneath
    lands in the same trace.
    """
    annotations = {'session_id': session_id, 'customer_id': customer_id}
    with segment(node_name('Orchestrator'), annotations=annotations) as seg:
        if request:
            seg.add_metadata(request=request[:1000])
        yield seg


def carrier() -> Optional[tuple]:
    """
    Snapshot of the active trace context as (trace_id, segment_id).

    Only needed for handing the context to code that does not inherit
    contextvars; prefer contextvars.copy_context() where possible.
    """
    seg = current_segment()
    return (seg.trace_id, seg.id) if seg is not None else None
