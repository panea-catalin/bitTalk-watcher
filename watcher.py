from pymongo import MongoClient
import sys
from datetime import datetime
# Import an RPC client library
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException

def get_database():
    # MongoDB connection string
    CONNECTION_STRING = "your-coonection-string"
    client = MongoClient(CONNECTION_STRING)
    return client['your-database']

def watch_and_execute():
    db = get_database()
    collection = db['your-collection']
    results_collection = db['collection-results']
    
    # Setup RPC connection
    rpc_user = 'bitcoin-node-username'
    rpc_password = 'bitcoin-node-password'
    rpc_host = 'bitcoin-node-ip'
    rpc_port = '8332'
    rpc_connection = AuthServiceProxy(f"http://{rpc_user}:{rpc_password}@{rpc_host}:{rpc_port}/")

    pipeline = [
        {'$match': {'operationType': 'insert'}}
    ]

    try:
        with collection.watch(pipeline) as stream:
            for change in stream:
                print(change)
                code_to_run = change['fullDocument']['command']
                change_id = str(change['_id'])
                result_id = f"{datetime.now().isoformat()}-{change_id}"
                try:
                    local_vars = {}
                    exec(code_to_run, {}, local_vars)
                    result = local_vars.get('result')

                    results_collection.insert_one({
                        '_id': result_id,
                        'change_id': change_id,
                        'result': result,
                        'timestamp': datetime.now()
                    })
                    print(f"Result stored with ID: {result_id}")
                    
                    # Fetch blockchain block count
                    block_count = rpc_connection.getblockcount()
                    print(block_count)
                except Exception as e:
                    print(f"An error occurred: {e}")
                    results_collection.insert_one({
                        '_id': result_id,
                        'change_id': change_id,
                        'error': str(e),
                        'timestamp': datetime.now()
                    })
                    print(f"Error stored with ID: {result_id}")
    except KeyboardInterrupt:
        sys.exit()

if __name__ == "__main__":
    watch_and_execute()

