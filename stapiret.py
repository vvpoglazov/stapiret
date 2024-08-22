import os
import asyncio
import aiohttp
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
import urllib.parse

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()

# Get configuration from environment variables
BASE_URL = os.getenv('STACKROX_API_ENDPOINT')
API_TOKEN = os.getenv('STACKROX_API_TOKEN')
PROXY_URL = os.getenv('PROXY_URL', 'http://localhost:8080')

# Set the maximum number of concurrent API calls and retries
MAX_CONCURRENT_REQUESTS = 20
MAX_RETRIES = 3
RETRY_DELAY = 1

headers = {
    "Authorization": f"Bearer {API_TOKEN}",
    "Accept": "application/json"
}

async def make_request(session, url, semaphore, params=None):
    async with semaphore:
        for attempt in range(MAX_RETRIES):
            try:
                logger.info(f"Making request to: {url}")
                async with session.get(url, headers=headers, params=params, ssl=False, proxy=PROXY_URL) as response:
                    response.raise_for_status()
                    logger.info(f"Request to {url} successful. Status: {response.status}")
                    data = await response.json()
                    return data
            except (aiohttp.ClientResponseError, aiohttp.ClientConnectorError, json.JSONDecodeError) as e:
                logger.error(f"Attempt {attempt + 1} failed for URL {url}: {str(e)}")
                if attempt == MAX_RETRIES - 1:
                    logger.error(f"All attempts failed for URL: {url}")
                    return None
                retry_time = RETRY_DELAY * (2 ** attempt)
                logger.info(f"Retrying in {retry_time} seconds...")
                await asyncio.sleep(retry_time)

async def get_paginated_data(session, url, semaphore, data_key, params=None):
    all_data = []
    offset = 0
    limit = 1000  # Maximum limit allowed by the API

    while True:
        current_params = {'pagination.limit': limit, 'pagination.offset': offset}
        if params:
            current_params.update(params)

        data = await make_request(session, url, semaphore, params=current_params)
        if not data:
            break

        # Log the structure of the response
        logger.debug(f"Response structure for {url}: {list(data.keys())}")  # Convert dict_keys to list

        # Extract the relevant data using the provided key
        items = data.get(data_key, [])
        
        if isinstance(items, dict):
            items = [items]  # Convert single item to list
        
        logger.debug(f"Extracted {len(items)} items from the response for {url}")
        all_data.extend(items)

        if len(items) < limit:
            break

        offset += limit

    return all_data

async def get_clusters(session, semaphore):
    url = urllib.parse.urljoin(BASE_URL, '/v1/clusters')
    logger.info("Fetching clusters")
    clusters = await get_paginated_data(session, url, semaphore, 'clusters')
    logger.info(f"Fetched {len(clusters)} clusters")
    return clusters

async def get_deployments(session, semaphore):
    url = urllib.parse.urljoin(BASE_URL, '/v1/deployments')
    logger.info("Fetching deployments")
    deployments = await get_paginated_data(session, url, semaphore, 'deployments')
    logger.info(f"Fetched {len(deployments)} deployments")
    return deployments

async def get_pods(session, semaphore):
    url = urllib.parse.urljoin(BASE_URL, '/v1/pods')
    logger.info("Fetching pods")
    pods = await get_paginated_data(session, url, semaphore, 'pods')
    logger.info(f"Fetched {len(pods)} pods")
    return pods

async def get_namespaces(session, semaphore):
    url = urllib.parse.urljoin(BASE_URL, '/v1/namespaces')
    logger.info("Fetching namespaces")
    namespaces = await make_request(session, url, semaphore)
    if namespaces and 'namespaces' in namespaces:
        namespaces = namespaces['namespaces']
    logger.info(f"Fetched {len(namespaces)} namespaces")
    return namespaces

async def get_nodes(session, semaphore, cluster_id):
    url = urllib.parse.urljoin(BASE_URL, f'/v1/nodes/{cluster_id}')
    logger.info(f"Fetching nodes for cluster {cluster_id}")
    nodes = await make_request(session, url, semaphore)
    return nodes

async def main():
    if not all([BASE_URL, API_TOKEN]):
        logger.error("Missing required environment variables. Please check your .env file.")
        return

    logger.info(f"Starting script execution. Connecting to API at: {BASE_URL}")

    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

        # Fetch clusters, namespaces, deployments, and pods concurrently
        clusters_task = get_clusters(session, semaphore)
        namespaces_task = get_namespaces(session, semaphore)
        deployments_task = get_deployments(session, semaphore)
        pods_task = get_pods(session, semaphore)

        clusters, namespaces, deployments, pods = await asyncio.gather(
            clusters_task, namespaces_task, deployments_task, pods_task
        )

        # Fetch nodes for each cluster
        nodes = {}
        for cluster in clusters:
            cluster_id = cluster['id']
            nodes[cluster_id] = await get_nodes(session, semaphore, cluster_id)

        # Save results to JSON files
        def save_to_json(data, filename, data_key):
            with open(filename, 'w') as f:
                json.dump({data_key: data}, f, indent=2)
            logger.info(f"Saved {data_key} data to {filename}. Total {data_key}: {len(data)}")

        save_to_json(clusters, 'clusters.json', 'clusters')
        save_to_json(namespaces, 'namespaces.json', 'namespaces')
        save_to_json(deployments, 'deployments.json', 'deployments')
        save_to_json(pods, 'pods.json', 'pods')

        # Save nodes data
        with open('nodes.json', 'w') as f:
            json.dump(nodes, f, indent=2)
        logger.info("Saved nodes data to nodes.json")

        # Log summary
        logger.info(f"Total clusters: {len(clusters)}")
        logger.info(f"Total nodes: {sum(len(n.get('nodes', [])) for n in nodes.values())}")
        logger.info(f"Total namespaces: {len(namespaces)}")
        logger.info(f"Total deployments: {len(deployments)}")
        logger.info(f"Total pods: {len(pods)}")

if __name__ == "__main__":
    logger.info("Script execution started")
    start_time = datetime.now()
    asyncio.run(main())
    end_time = datetime.now()
    execution_time = end_time - start_time
    logger.info(f"Script execution completed. Total execution time: {execution_time}")
