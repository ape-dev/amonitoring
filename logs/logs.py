import json
import datetime
import elasticsearch
import pandas

from common.elasticsearch import create_index, add_docs_to_index


def generate_report_hosts_amount_bytes():
    """
    Сгенерировать отчет об общей сумме байт по каждому хосту
    """
    es_object = elasticsearch.Elasticsearch([{'host': 'localhost', 'port': 9200}])

    if not es_object.ping():
        return

    mapping = {
        'dynamic': 'strict',
        'properties': {
            'host': {'type': 'text', 'fielddata': True},
            'bytes': {'type': 'integer'},
            'utc_time': {'type': 'date'}
        }

    }
    index_name = 'amonitoring_logs'
    create_index(es_object, index_name, mapping)

    with open('logs/logs.json', 'r') as read_file:
        logs = json.load(read_file) or []

    fields = ['host', 'bytes', 'utc_time']
    add_docs_to_index(es_object, index_name, logs, fields)
    search_params = {
        "aggs": {
            "hosts": {
                "terms": {
                    "field": "host"
                },
                "aggs": {
                    "total_bytes": {
                        "sum": {"field": "bytes"}
                    }
                }
            }
        }
    }
    pre_result = es_object.search(index=index_name, body=search_params)['aggregations']['hosts']['buckets']

    result = {'Host': [], 'Sum of bytes': []}
    for host in pre_result:
        result['Host'].append(host.get('key'))
        result['Sum of bytes'].append(host.get('total_bytes', {}).get('value'))

    pandas.DataFrame(result).to_csv('logs/logs.csv')
