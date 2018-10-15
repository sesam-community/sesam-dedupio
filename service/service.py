"""
Microservice for data deduplication for Sesam.io powered applications

"""

import json
import os
import logging
import re
import numpy
from flask import Flask, request, Response, abort
import requests
import dedupe

import sesamclient

APP = Flask(__name__)

# Logging
logging.getLogger().setLevel(logging.DEBUG)

# List with keys that need to be analysed for duplicate values
# ex. ['Email', 'FirstName', 'LastName', 'Phone']
# or pass as comma separated String
KEYS = os.environ.get('KEYS', [])

if isinstance(KEYS, str):
    KEYS = [x.strip() for x in KEYS.split(',')]

if not KEYS:
    logging.error("No keys for analysis were found, checking will not be possible.")
    exit(1)

# file with trained model (local or url)
SETTINGS_FILE = os.environ.get('SETTINGS_FILE')

# if you wish to add original fields used for analysis in output
ADD_ORIGINALS = os.environ.get('ADD_ORIGINALS', True)

# if you wish to add canonical representation of a duplicate fields in output
ADD_CANONICALS = os.environ.get('ADD_CANONICALS', False)

# we need it to get access to node, place your jwt token here
JWT = os.environ.get('JWT')

# full API URL like 'https://datahub-xxxxxxx.sesam.cloud/api'
# we support only same source<->target instance
INSTANCE = os.environ.get('INSTANCE')

# right now we support only "published" sesam endpoints (they have namespaces striped)
SOURCE = os.environ.get('SOURCE')

# receiver endpoint (http_endpoint type pipe)
# if not assigned then output as response
TARGET = os.environ.get('TARGET')

# some validation for input parameters

if JWT is None:
    logging.error('jwt token missing. Add environment variable \'JWT\' with token string.')
    exit(1)

if SETTINGS_FILE.startswith('http'):  # then we have url
    logging.info("Found URL, retrieving trained model")
    r = requests.get(SETTINGS_FILE, stream=True)
    temp_file_name = 'settings_file'
    with open(temp_file_name, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:  # filter out keep-alive new chunks
                f.write(chunk)
        SETTINGS_FILE = temp_file_name

if not os.path.exists(SETTINGS_FILE):
    logging.error("Couldn't load model, settings file not found.")
    exit(1)

if SOURCE is None:
    logging.error('Source pipe is not set. Add environment variable SOURCE with source pipe.')
    exit(1)

if TARGET is None:
    logging.info('Target pipe is not set. Will return data instead of publishing it to target pipe.')


class NumpyEncoder(json.JSONEncoder):
    """Custom encoder of numpy datatypes"""

    def default(self, obj):
        if isinstance(obj, numpy.integer):
            return int(obj)
        elif isinstance(obj, numpy.floating):
            return float(obj)
        elif isinstance(obj, numpy.ndarray):
            return obj.tolist()
        else:
            return super(NumpyEncoder, self).default(obj)


def pre_process_string_data(item: dict):
    """
    remove extra whitespaces, linebreaks, quotes from strings
    :param item: dictionary with data for analysis
    :return: cleaned item
    """
    try:
        result_item = {key: item[key] for key in KEYS + ['_id']}
        for prop in result_item:
            if type(result_item[prop]) is str and prop != '_id':
                result_item[prop] = re.sub('  +', ' ', item[prop])
                result_item[prop] = re.sub('\n', ' ', item[prop])
                result_item[prop] = item[prop].strip().strip('"').strip("'").lower().strip()
        return result_item
    except KeyError:
        logging.warning("Wrong formed entity with id %s", item['_id'])
        return None


def read_data(raw_data: list):
    """clean raw_data dict from undesired keys and preprocessing string data"""
    cleaned_data = {}
    for data_item in raw_data:
        clean_data_item = pre_process_string_data(data_item)
        if clean_data_item is not None:
            cleaned_data[clean_data_item['_id']] = clean_data_item
    return cleaned_data


@APP.route('/', methods=["GET", "POST"])
def process():
    """entry point"""
    api = sesamclient.Connection(sesamapi_base_url=INSTANCE, jwt_auth_token=JWT, timeout=60 * 10)
    result = api.session.get(
        api.sesamapi_base_url + "publishers/{}/entities?deleted=false&history=false".format(SOURCE))

    if result.status_code != 200:
        logging.warning('Sesam API returned non 200 code {}'.format(result.status_code))
        iterator = iter([])
    else:
        iterator = iter(result.json())
    raw_data = []

    for dataset_entity in iterator:
        raw_data.append(dataset_entity)
    # this is in memory service, for large datasets we probably may want to have service with DB storage
    if len(raw_data) > 100_000:
        logging.warning('This service is not suitable for datasets with more than 100 000 elements'
                        ' and may lead to OutOfMemory errors')

    cleaned_data = read_data(raw_data)

    logging.info('reading from %s', SETTINGS_FILE)

    with open(SETTINGS_FILE, 'rb') as f:
        deduper = dedupe.StaticDedupe(f)

    threshold = deduper.threshold(cleaned_data, recall_weight=1)

    logging.info('clustering...')
    clustered_dupes = deduper.match(cleaned_data, threshold)
    logging.info('%d duplicate sets found', len(clustered_dupes))

    cluster_membership = {}
    cluster_id = 0

    for (cluster_id, cluster) in enumerate(clustered_dupes):
        id_set, scores = cluster
        cluster_d = [cleaned_data[c] for c in id_set]
        canonical_rep = dedupe.canonicalize(cluster_d)
        for record_id, score in zip(id_set, scores):
            cluster_membership[record_id] = {
                "cluster id": cluster_id,
                "canonical representation": canonical_rep,
                "confidence": score
            }

    result_dataset = []
    for row in raw_data:
        row_id = row['_id']
        if row_id in cluster_membership:
            result_dict = {
                '_id': row_id,
                'cluster_id': cluster_membership[row_id]["cluster id"],
                'confidence_score': cluster_membership[row_id]['confidence']
            }

            if ADD_ORIGINALS:
                result_dict['originals'] = {key: row[key] for key in KEYS}

            if ADD_CANONICALS:
                result_dict['canonical_rep'] = cluster_membership[row_id]["canonical representation"]

            result_dataset.append(result_dict)
    if TARGET is not None:
        target_url = api.sesamapi_base_url + "receivers/{}/entities".format(TARGET)
        requests.post(target_url,
                      json.dumps(sorted(result_dataset, key=lambda k: k['cluster_id']), cls=NumpyEncoder),
                      headers={'Authorization': 'Bearer {}'.format(JWT), 'Content-Type': 'application/json'})
        return Response()

    return Response(json.dumps(sorted(result_dataset, key=lambda k: k['cluster_id']), cls=NumpyEncoder),
                    mimetype='application/json')


if __name__ == '__main__':
    logging.info("Starting service")
    APP.run(debug=True, host='0.0.0.0', threaded=True, port=os.environ.get('PORT', 5000))
