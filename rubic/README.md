# Project's RUBIC

## .env

![alt text](env.png)

## `tests/test_agent.py`

### Task 2:
The provided aws account does not have permission for claude model, so use amazon.nova-pro v1 instead.

![alt text](task2.png)

### Task 3

![alt text](task3.png)

### Task 4

![alt text](task4.png)

### Task 5

![alt text](task5.png)

### Task 6

![alt text](task6.png)

# Single pre-scripted request — runs one full refund scenario
python src/demo.py

![alt text](test_demo.png)

## `src/aws_novamart_agents/agent_orchestrator.py`

Runs 3 hardcoded scenarios and prints raw responses

### CUST-001

![alt text](test_cust_001.png)

### CUST-002

![alt text](test_cust_002_a.png)

![alt text](test_cust_002_b.png)

### CUST-003

![alt text](test_cust_003.png)

### X-Ray trace

![alt text](trace.png)
