import numpy as np
import waste_pickup_sim
import json
import random
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential  # Import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient


# Fetch the Azure Blob Storage connection string from Azure Key Vault using DefaultAzureCredential
vault_url = "https://keyvaultforhamk.vault.azure.net/"
secret_name = "inputdatatoazureblob"  # Replace with the name of your secret
credential = DefaultAzureCredential()
key_vault_secret = SecretClient(vault_url=vault_url, credential=credential).get_secret(secret_name)
# Use the fetched connection string
connection_string = key_vault_secret.value
container_name = "inputdata"
geojson_container_name = "geojsonfiles"  # container name for GeoJSON files
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# List all blobs in the container
blobs = blob_service_client.get_container_client(container_name).list_blobs()

# Find the latest JSON blob based on the last modified timestamp
latest_blob = None
latest_timestamp = None

for blob in blobs:
    if blob.name.endswith(".json"):
        if latest_blob is None or blob['last_modified'] > latest_timestamp:
            latest_blob = blob
            latest_timestamp = blob['last_modified']

# Download and read the JSON blob
if latest_blob is not None:
    blob_client = blob_service_client.get_blob_client(container_name, latest_blob.name)
    blob_content = blob_client.download_blob()
    sim_config_json = blob_content.readall()
    sim_config_values = json.loads(sim_config_json)

    sim_config = {
            'sim_name': 'your area and nearby regions',
            'sim_runtime_days': sim_config_values.get('sim_runtime_days'),
            'pickup_sites_filename': './geo_data/sim_test_sites.geojson',
            'depots_filename': './geo_data/sim_test_terminals.geojson',
            'terminals_filename': './geo_data/sim_test_terminals.geojson',
            'vehicle_template': {
                'load_capacity': sim_config_values.get('load_capacity'),  # Tonnes
                'max_route_duration': sim_config_values.get('max_route_duration'),  # Minutes
                'break_duration': sim_config_values.get('break_duration'),  # Minutes
                'num_breaks_per_shift': sim_config_values.get('num_breaks_per_shift'),
                'pickup_duration': sim_config_values.get('pickup_duration'),  # Minutes
            },
        'depots': [
            {
                'num_vehicles': 1
            },
            {
                'num_vehicles': 1
            }
        ]
    }
    # Now you can use the populated sim_config dictionary in your code
    print("sim_config:")
    print(json.dumps(sim_config, indent=4))
else:
    print("No JSON blob found in the container.")









#PART for updating geojson files

vault_url = "https://keyvaultforhamk.vault.azure.net/"
secret_name = "inputdatatoazureblob"  # Replace with the name of your secret
credential = DefaultAzureCredential()
key_vault_secret = SecretClient(vault_url=vault_url, credential=credential).get_secret(secret_name)
# Use the fetched connection string
connection_string = key_vault_secret.value

container_name = "geojsonfiles" 

# Initialize the BlobServiceClient
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# List all blobs in the container with the specified prefix
blob_prefix = "sim_test_sites.geojson"  # The name of the GeoJSON file you're looking for
blobs = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=blob_prefix)

# Initialize variables to keep track of the latest blob and its timestamp
latest_blob = None
latest_timestamp = None

# Iterate through the blobs to find the latest one
for blob in blobs:
    if blob.name.endswith(".geojson"):
        if latest_blob is None or blob['last_modified'] > latest_timestamp:
            latest_blob = blob
            latest_timestamp = blob['last_modified']

# Check if a latest blob was found
if latest_blob:
    latest_blob_name = latest_blob['name']
    print(f"Latest GeoJSON blob in the container for pickup_sites_filename: {latest_blob_name}")

    # Download the latest GeoJSON blob
    with open("latest_geojson.geojson", "wb") as f:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=latest_blob_name)
        blob_data = blob_client.download_blob()
        f.write(blob_data.readall())
        
    # Update the pickup_sites_filename in sim_config to use the downloaded GeoJSON file
    sim_config['pickup_sites_filename'] = './latest_geojson.geojson'
else:
    print("No GeoJSON file with the specified name found in the container.")   














#TERMINALS UPDATE
# Now, similarly, update the terminals_filename
# Reset latest_blob and latest_timestamp
latest_blob = None
latest_timestamp = None

# Specify the prefix for terminals_filename
blob_prefix_terminals = "sim_test_terminals.geojson"

# List blobs with the specified prefix (terminals_filename)
blobs_terminals = blob_service_client.get_container_client(container_name).list_blobs(name_starts_with=blob_prefix_terminals)

# Iterate through the blobs to find the latest one for terminals_filename
for blob in blobs_terminals:
    if blob.name.endswith(".geojson"):
        if latest_blob is None or blob['last_modified'] > latest_timestamp:
            latest_blob = blob
            latest_timestamp = blob['last_modified']

# Check if a latest blob was found for terminals_filename
if latest_blob:
    latest_blob_name = latest_blob['name']
    print(f"Latest GeoJSON blob in the container for pickup_sites_filename: {latest_blob_name}")

    # Download the latest GeoJSON blob for terminals_filename
    with open("latest_geojson_terminals.geojson", "wb") as f:
        blob_client_terminals = blob_service_client.get_blob_client(container=container_name, blob=latest_blob_name)
        blob_data_terminals = blob_client_terminals.download_blob()
        f.write(blob_data_terminals.readall())

    # Update the terminals_filename in sim_config to use the downloaded GeoJSON file
    sim_config['terminals_filename'] = './latest_geojson_terminals.geojson'
    sim_config['depots_filename'] = './latest_geojson_terminals.geojson'
else:
    print("No GeoJSON file with the specified name found in the container for terminals_filename.")

# Print the updated sim_config
print("Updated sim_config:")
print(json.dumps(sim_config, indent=2))


def hypothesis_test():
    """
    """
    # Runs N simulation
    # logs them to list of jsons
    pass

def test_record():
    """
    """
    # List of jsons of sim runs 
    # Metadata on time average time of computation and vehicle driving time
    # config
    pass


random.seed(42)
np.random.seed(42)
waste_pickup_sim.preprocess_sim_config(sim_config, 'temp/sim_preprocessed_config.json')
sim = waste_pickup_sim.WastePickupSimulation(sim_config)
sim.sim_run()
sim.save_log()
sim.sim_record()
