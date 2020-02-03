import json
import elasticsearch
import pandas

from common.elasticsearch import create_index, add_docs_to_index


def generate_report_avg_flight_delay():
    """
    Сгенерировать отчет о средней задержки рейса
    """
    es_object = elasticsearch.Elasticsearch([{'host': 'localhost', 'port': 9200}])

    if not es_object.ping():
        return

    mapping = {
        'dynamic': 'strict',
        'properties': {
            'Carrier': {'type': 'text', 'fielddata': True},
            'FlightDelayMin': {'type': 'integer'},
            'DistanceKilometers': {'type': 'integer'},
        }

    }
    index_name = 'amonitoring_flights'
    create_index(es_object, index_name, mapping)

    with open('flights/flights.json', 'r') as read_file:
        flights = json.load(read_file) or []

    fields = ['Carrier', 'FlightDelayMin', 'DistanceKilometers']
    add_docs_to_index(es_object, index_name, flights, fields)
    search_params = {
        'aggs': {
            'carriers': {
                'terms': {
                    'field': 'Carrier'
                },
                'aggs': {
                    'avg_delay': {
                        'avg': {'field': 'FlightDelayMin'}
                    }
                }
            }
        }
    }
    pre_result = es_object.search(index=index_name, body=search_params)['aggregations']['carriers']['buckets']

    result = {'Carrier': [], 'Average FlightDelayMin': [], }
    for carrier in pre_result:
        result['Carrier'].append(carrier.get('key'))
        result['Average FlightDelayMin'].append(carrier.get('avg_delay', {}).get('value'))

    pandas.DataFrame(result).to_csv('flights/flights.csv')
