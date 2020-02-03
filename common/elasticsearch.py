from datetime import datetime, timezone
from elasticsearch.helpers import bulk


def create_index(es_object, index_name, mapping):
    """
    Создать индекс
    :param es_object: Elasticsearch Объект ES
    :param index_name: str Имя индекса
    :param mapping: dict Маппинг индекса
    """
    if es_object.indices.exists(index_name):
        return es_object

    settings = {
        "settings": {
            "number_of_shards": 1
        },
        "mappings": mapping
    }

    es_object.indices.create(index=index_name, body=settings)


def add_docs_to_index(es_object, index_name, docs, fields):
    """
    Добавить документы в индекс
    :param es_object: Elasticsearch Объект ES
    :param index_name: str Имя индекса
    :param docs: [dict] Список документов
    :param fields: [str] Список необходимых полей
    """
    bulk(es_object, _gen_data(index_name, docs, fields))


def _gen_data(index_name, docs, fields):
    """
    Генератор для индексации списка документов
    :param index_name: str Имя индекса
    :param docs: [dict] Список документов
    :param fields: [str] Список необходимых полей
    """
    for doc in docs:
        yield ({
            "_index": index_name,
            "_source": {key: value for key, value in doc.items() if key in fields},
        })
