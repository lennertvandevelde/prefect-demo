from prefect import flow, task, get_run_logger

import time
import random


@task()
def blocking_task():
    time.sleep(5)
    logger = get_run_logger()

    chance = random.randint(1, 3) # 1/3 chance of error
    if chance == 1:
        raise ValueError("ValueError occurred!")

    logger.info("Wait for me!")

@task()
def double(i):
    return i*2

@task()
def log(i):
    time.sleep(5)
    logger = get_run_logger()
    logger.info(i)
    
@flow(name="test-subflow")
def sub_flow():
    log("Task inside subflow")

@flow(name="test-flow")
def main_flow(max_i: int):
    i = 0
    sub_flow()
    while i < max_i:
        i += 1
        double_i = double.submit(i) 
        result_wait = blocking_task.with_options(
            retries=6, # Retries when task fails
            retry_delay_seconds=[1, 2, 3, 4, 5, 6] # Exponential backoff
        ).submit() 
        log.submit(double_i, wait_for=result_wait) 

if __name__ == "__main__":
    main_flow(5)