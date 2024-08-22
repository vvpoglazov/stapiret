import json
from collections import defaultdict
from datetime import datetime

def load_json(filename):
    try:
        with open(filename, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"File not found: {filename}")
        return {}

def combine_kubernetes_data():
    # Load all JSON files
    clusters_data = load_json('clusters.json')
    nodes_data = load_json('nodes.json')
    namespaces_data = load_json('namespaces.json')
    deployments_data = load_json('deployments.json')
    pods_data = load_json('pods.json')

    # Create the master structure
    master_data = defaultdict(lambda: {
        'info': {},
        'nodes': [],
        'namespaces': defaultdict(lambda: {'deployments': defaultdict(lambda: {'info': {}, 'pods': []})})
    })

    # Process clusters
    for cluster in clusters_data.get('clusters', []):
        cluster_id = cluster['id']
        master_data[cluster_id]['info'] = {
            'name': cluster['name'],
            'type': cluster['type'],
            'labels': cluster.get('labels', {})
        }

    # Process nodes
    for cluster_id, cluster_nodes in nodes_data.items():
        for node in cluster_nodes.get('nodes', []):
            master_data[cluster_id]['nodes'].append({
                'id': node['id'],
                'name': node['name'],
                'labels': node.get('labels', {}),
                'taints': node.get('taints', [])
            })

    # Process namespaces
    for namespace in namespaces_data.get('namespaces', []):
        metadata = namespace['metadata']
        cluster_id = metadata['clusterId']
        namespace_name = metadata['name']
        master_data[cluster_id]['namespaces'][namespace_name].update({
            'id': metadata['id'],
            'labels': metadata.get('labels', {}),
            'annotations': metadata.get('annotations', {})
        })

    # Process deployments
    for deployment in deployments_data.get('deployments', []):
        cluster_id = deployment['clusterId']
        namespace = deployment['namespace']
        deployment_id = deployment['id']
        master_data[cluster_id]['namespaces'][namespace]['deployments'][deployment_id]['info'] = {
            'name': deployment['name'],
            'created': deployment['created']
        }

    # Process pods
    for pod in pods_data.get('pods', []):
        cluster_id = pod['clusterId']
        namespace = pod['namespace']
        deployment_id = pod.get('deploymentId')
        
        pod_info = {
            'id': pod['id'],
            'name': pod['name'],
            'node': None,
            'containers': []
        }
        
        # Process live instances to get node information and containers
        for instance in pod.get('liveInstances', []):
            instance_id = instance['instanceId']
            pod_info['node'] = instance_id['node']
            pod_info['containers'].append({
                'name': instance['containerName'],
                'id': instance_id['id'],
                'runtime': instance_id['containerRuntime']
            })
        
        if deployment_id:
            master_data[cluster_id]['namespaces'][namespace]['deployments'][deployment_id]['pods'].append(pod_info)
        else:
            # Handle pods not associated with a deployment
            if 'standalone_pods' not in master_data[cluster_id]['namespaces'][namespace]:
                master_data[cluster_id]['namespaces'][namespace]['standalone_pods'] = []
            master_data[cluster_id]['namespaces'][namespace]['standalone_pods'].append(pod_info)

    return dict(master_data)

def save_master_json(data, filename):
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)

if __name__ == "__main__":
    master_data = combine_kubernetes_data()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"kubernetes_master_data_{timestamp}.json"
    save_master_json(master_data, filename)
    print(f"Master JSON file created: {filename}")

    # Print some statistics
    cluster_count = len(master_data)
    total_nodes = sum(len(cluster['nodes']) for cluster in master_data.values())
    total_namespaces = sum(len(cluster['namespaces']) for cluster in master_data.values())
    total_deployments = sum(
        sum(len(ns['deployments']) for ns in cluster['namespaces'].values())
        for cluster in master_data.values()
    )
    total_pods = sum(
        sum(len(deployment['pods']) for deployment in ns['deployments'].values()) +
        len(ns.get('standalone_pods', []))
        for cluster in master_data.values()
        for ns in cluster['namespaces'].values()
    )

    print(f"Total clusters: {cluster_count}")
    print(f"Total nodes: {total_nodes}")
    print(f"Total namespaces: {total_namespaces}")
    print(f"Total deployments: {total_deployments}")
    print(f"Total pods: {total_pods}")
