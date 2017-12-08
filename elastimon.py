import os 
import yaml 
from pprint import pprint 
from elasticsearch import Elasticsearch 
from elasticsearch import helpers as es_helpers 
import time
import datetime 
import arrow 
import logging
import json
import uuid
import schedule
from collections import defaultdict
from collections import Counter
from collections import OrderedDict
import re 
import sqlite3

logging.basicConfig(format="%(asctime)s - %(name)s - [ %(levelname)s ] - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

SHOME = os.path.abspath(os.path.join(os.path.dirname(__file__)))

_TARGET_INDEX = 'elastimon-{}'.format(arrow.utcnow().format('YYYY-MM-DD'))

def load_config(path=os.path.join(SHOME, 'config.yml')):
    with open(path, 'r') as f:
        try:
            doc = yaml.load(f)
            return doc 
        except Exception as e:
            logger.exception('Unable to open yaml config at: {}, reason: {}'.format(path, e))

config = load_config()

def find_in_clusters(name):
    for cluster in config.get('es_clusters'):
        if name == cluster['name']: 
            return cluster 

def get_timestamp():
    return arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ')

def es_index(client, actions):
    try:
        bulk_index = es_helpers.bulk(client, actions)
        logger.info('Bulk ES insert has been completed, items inserted: {}'.format(bulk_index[0]))
    except Exception as e:
        logger.exception('Unable to bulk index items, reason: {}'.format(e))

def bulk_format_document(document, doc_type, index=_TARGET_INDEX, tags=[]):
    retd = {
            #'_op_type': 'index',
            #'_id': str(uuid.uuid4()),
            '_type': doc_type, 
            '_index': index, 
            '_source': document
        }

    retd['_source']['@timestamp'] = arrow.utcnow().format('YYYY-MM-DDTHH:mm:ssZ')
    retd['_source']['tags'] = tags 
    return retd 

def clusterStats():
    logger.info("clusterStats Thread started...")
    for cluster in config.get('es_clusters'):
        bulk_stats = []
        cluster_name = cluster['name']
        try:
            es = Elasticsearch(cluster['elasticsearch']['hosts'], **cluster['elasticsearch']['args'])
        except Exception as e:
            logger.exception('Unable to connect to Elasticsearch Cluster {} - {}, reason: {}'.format(cluster_name, cluster['elasticsearch']['hosts'], e))

        # /_cluster/health
        try:
            cluster_health_poll = es.cluster.health()
            cluster_health = bulk_format_document(cluster_health_poll, 'cluster-health', tags=cluster['tags'])
            bulk_stats.append(cluster_health)
        except Exception as e:
            logger.exception('Unable to perform cluster health on cluster: {}, reason: {}'.format(cluster_name, e))

        # /_stats
        try:
            cluster_stats_poll = es.cluster.stats()
            cluster_stats = bulk_format_document(cluster_stats_poll, 'cluster-stats', tags=cluster['tags'])
            bulk_stats.append(cluster_stats)
        except Exception as e:
            logger.exception('Unable to perform cluster stats on cluster: {}, reason: {}'.format(cluster_name, e))

        # /_nodes/stats 
        try:
            node_stats_to_collect = ["indices", "os", "process", "jvm", "thread_pool", "fs", "transport", "http", "script", "ingest"]
            node_stats = es.nodes.stats()

            cluster_name = node_stats['cluster_name']

            for node_id, node in node_stats['nodes'].items():
                node_data = {
                    'cluster_name': cluster_name,
                    'source_node': {
                        'uuid': node_id,
                        'host': node['host'],
                        'transport_address': node['transport_address'],
                        'ip': node['ip'],
                        'name': node['name'],
                    }
                }   

                node_data['node_stats'] = {
                    'node_id': node_id,
                    'node_master': 'master' in node['roles'],
                    'node_roles': node['roles'],
                }

                for k in node_stats_to_collect:
                    node_data['node_stats'][k] = node[k]

                bulk_stats.append(bulk_format_document(node_data, 'node-stats', tags=cluster['tags']))

        except Exception as e:
            logger.exception('Unable to perform node stats on cluster: {}, reason: {}'.format(cluster_name, e))

        if config.get('manage_index_template', True):
            with open(os.path.join(SHOME, 'index-template.json'), 'r') as f:
                index_template = json.loads(f.read())
            try:
                put_template = es.indices.put_template(name='elastimon', body=index_template)
                logger.info('Index template managed for cluster: {}'.format(cluster_name))
            except Exception as e:
                logger.exception('Unable to put index template on cluster: {}, reason: {}'.format(cluster_name, e))

        if cluster.get('output'):
            if 'elasticsearch' in cluster['output']['method']:
                es = Elasticsearch( es_cluster['elasticsearch']['hosts'], **es_cluster['elasticsearch']['args'])
                es_index(es, bulk_stats)
            elif 'rabbitmq' in cluster['output']['method']:
                logger.error('RabbitMQ output is not supported yet')
        else:
            es_index(es, bulk_stats)

        logger.info("clusterStats Thread ended...")

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def timeSeriesIndexStats():
    logger.info("timeSeriesIndexStats Thread started...")
    index_regexes = dict(
        YEARLY = '(\d{4})$',
        MONTHLY = '(\d{4}.\d{2})$',
        DAILY = '(\d{4}\.\d{2}\.\d{2})$',
        HOURLY = '(\d{4}\.\d{2}\.\d{2}\.\d{1,2})$'
    )

    con = sqlite3.connect(':memory:')
    con.row_factory = dict_factory
    cur = con.cursor()    

    cur.execute('drop table if exists indices')
    cur.execute('create table if not exists indices( name varchar, health varchar, index_type varchar, docs_count int, pri_shards, rep_shards, store_size_kb int, pri_store_size_kb int)')
         
    for cluster in config.get('es_clusters'):
        bulk_stats = []
        cluster_name = cluster['name']
        try:
            es = Elasticsearch(cluster['elasticsearch']['hosts'], **cluster['elasticsearch']['args'])
        except Exception as e:
            logger.exception('Unable to connect to Elasticsearch Cluster {} - {}, reason: {}'.format(cluster_name, cluster['elasticsearch']['hosts'], e))

        # cat /_cluster/stats
        try:
            cat_indices = es.cat.indices(bytes='k')
        except Exception as e:
            logger.exception('Unable to perform cluster stats on cluster: {}, reason: {}'.format(cluster_name, e))

        headers = [ 'health', 'status', 'name', 'uuid', 'pri.shards', 'rep.shards', 'docs.count', 'docs.deleted', 'store.size.kb', 'pri.store.size.kb' ]
        lines = cat_indices.split('\n')
        indices = []
        for line in lines:
            parts = line.split()
            item = dict(zip(headers, parts))
            if item.get('name'): indices.append(item)

        index_type = 'N/A'
        indices_formmated_dict = {} 
        for index in indices:
            index['index_type'] = index_type
            for regex_name, pattern in index_regexes.items():
                match = re.search(pattern, index['name'])
                if match:
                    index['time_match'] = match.group(1)
                    index_type = regex_name
                    index['index_type'] = index_type
                    break

            if index_type != 'N/A':
                index['name'] = re.sub( index_regexes[index_type], '', index.get('name'))

        for i in indices:
            cur.execute('insert into indices (name, health, index_type, pri_store_size_kb, docs_count, pri_shards, rep_shards ) VALUES (?, ?, ?, ?, ?, ?, ?)', ( i['name'], i['health'], i['index_type'], i['pri.store.size.kb'], i['docs.count'], i['pri.shards'], i['rep.shards']))

        query = 'select distinct name as [tistats.name], count(*) as [tistats.instance.count], index_type as [tistats.index_type], sum(pri_store_size_kb) as [tistats.pri.store.size.kb], sum(docs_count) as [tistats.docs.count], sum( pri_shards + rep_shards ) as [tistats.shard.count.total] from indices group by name;'
        cur.execute(query) 
        con.commit()
        rows = cur.fetchall()

        bulk_stats = []
        for row in rows:
            row.update( { 'cluster_name': cluster_name })
            formatted = bulk_format_document( row, 'time-series-index-stats', tags=[ 'time-series-index'] ) 
            bulk_stats.append(formatted)

        if cluster.get('output'):
            if 'elasticsearch' in cluster['output']['method']:
                es = Elasticsearch( es_cluster['elasticsearch']['hosts'], **es_cluster['elasticsearch']['args'])
                es_index(es, bulk_stats)
            elif 'rabbitmq' in cluster['output']['method']:
                logger.error('RabbitMQ output is not supported yet')
        else:
            es_index(es, bulk_stats)
        
        logger.info("timeSeriesIndexStats Thread ended...")
                

        
if __name__ == '__main__':
    schedule.every(10).seconds.do(clusterStats)
    schedule.every(10).minutes.do(timeSeriesIndexStats)
       
    while True:
        schedule.run_pending()
        time.sleep(1)
