from prefect import flow, task, get_run_logger
from prefect.blocks.system import String
import time
import random
from prefect_meemoo.mediahaven.credentials import MediahavenCredentials
from prefect_meemoo.mediahaven.tasks import search_records

    

@flow(name="test-flow")
def main_flow(
    max_i: int,
    block_name_mediahaven_query: str,
    mediahaven_credentials_block: str
    ):
    i = 0
    mediahaven_query = String.load(block_name_mediahaven_query).value # Load block and get value
    mediahaven_client = MediahavenCredentials.load(mediahaven_credentials_block).get_client()
    while i < max_i:
        i += 1
        search_records.submit(mediahaven_client, mediahaven_query, start_index=i, nr_of_results=1)

if __name__ == "__main__":
    main_flow(5, 'query', 'mediahaven_prd')