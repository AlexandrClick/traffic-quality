import numpy as np
import pandas as pd

import yaml
import sys

import clickhouse_driver as chd
import MySQLdb
from sqlalchemy import create_engine

from discrepSplitter import DiscrepSplitter

dev_yml = sys.argv[1] if len(sys.argv) > 1 else 'dev.yml'
splitter_yml = sys.argv[2] if len(sys.argv) > 2 else 'splitter.yml'

cfg = yaml.safe_load(open(dev_yml, 'r'))
cfg_ch = cfg['stats']['connection']
cfg_ms = cfg['mysql']

cfg_algo = yaml.safe_load(open(splitter_yml, 'r'))

ch_db = cfg_ch['database']
conn = chd.connect(**{
    'user': str(cfg_ch['username']),
    'password': str(cfg_ch['password']),
    'host': cfg_ch['host'],
    'port': cfg_ch['port']
})

auth_mysql = {
    'user': cfg_ms['username'],
    'password': cfg_ms['password'],
    'host': cfg_ms['host'],
    'port': cfg_ms['port'],
    'database': cfg_ms['database']
}
auth_mysql_str = f"mysql://{cfg_ms['username']}:{cfg_ms['password']}@{cfg_ms['host']}:{cfg_ms['port']}/{cfg_ms['database']}"
engine_mysql = create_engine(auth_mysql_str)

q = f"""SELECT direction_id, zone_id, geo, sum(clicks) AS clck, sum(pixels) AS pix
FROM {ch_db}.mv_discrep_to_pixel
WHERE date >= NOW() - INTERVAL {cfg_algo['interval_window']} DAY
  AND direction_id IN ({", ".join([str(d) for d in cfg_algo['directions']])})
GROUP BY direction_id, zone_id, geo
HAVING clck > {cfg_algo['clicks_threshold']};"""
data = pd.read_sql_query(q, conn)

data['clck'] = np.where(data['pix'] > data['clck'], data['pix'], data['clck'])
data['discrep'] = 1 - data['pix'] / data['clck']


def map_groups(data):
    model = DiscrepSplitter(int(data['clck'].sum() * cfg_algo['min_clck_share_in_group']),
                            int(data['pix'].sum() * cfg_algo['min_pix_share_in_group']),
                            int(data.shape[0] * cfg_algo['min_slices_share_in_group']))
    model.fit(data['clck'].tolist(), data['pix'].tolist(), cfg_algo['n_groups'])

    thresholds = model.get_thresholds()
    print(thresholds)

    data['quality_group'] = 1
    for thr in thresholds:
        data['quality_group'] = np.where(data['discrep'] > thr, data['quality_group'] + 1, data['quality_group'])
    data['row_key'] = (data['zone_id'] * 2 ** cfg_algo['geo_bits'] + data['geo'].map(cfg_algo['geo_map'])).astype(int)
    return data[['row_key', 'quality_group']]


old_table = pd.read_sql('SELECT * FROM traffic_quality', engine_mysql)
old_table['quality_group'] = np.where(old_table['deleted_at'] == old_table['deleted_at'], None,
                                      old_table['quality_group'])

new_table = pd.concat([map_groups(data[data['direction_id'] == dir_id]) for dir_id in cfg_algo['directions']])
new_table['updated_at'] = 'now()'

table = new_table.merge(old_table, on='row_key', how='outer', suffixes=('', '_old'))
table = table[table['quality_group'] != table['quality_group_old']]

table_to_update = table[table['quality_group'] == table['quality_group']]

if cfg_algo['delete_data']:
    deleted_filter = ~(table['deleted_at'] == table['deleted_at']) & ~(table['quality_group'] == table['quality_group'])
    table_to_delete = table[deleted_filter]
    table_to_delete['deleted_at'] = 'now()'
else:
    table_to_delete = pd.DataFrame(columns=['row_key', 'deleted_at'])

table_update = []
for tv in table_to_update[['quality_group', 'row_key', 'updated_at']].values:
    table_update += [f"({','.join([str(tv_) for tv_ in tv])},NULL)"]

table_delete = []
for tv in table_to_delete[['row_key', 'deleted_at']].values:
    table_delete += [f"({','.join([str(tv_) for tv_ in tv])})"]

conn = MySQLdb.connect(**auth_mysql)
cur = conn.cursor()

if len(table_update):
    update_query = f"""INSERT INTO
    traffic_quality (quality_group, row_key, updated_at, deleted_at) VALUES
    {",".join(table_update)}
    ON DUPLICATE KEY UPDATE quality_group = VALUES(quality_group),
    updated_at = VALUES(updated_at), deleted_at = VALUES(deleted_at);"""
    cur.execute(update_query)

if len(table_delete):
    delete_query = f"""INSERT INTO
    traffic_quality (row_key, deleted_at) VALUES
    {",".join(table_delete)}
    ON DUPLICATE KEY UPDATE deleted_at = VALUES(deleted_at);"""
    cur.execute(delete_query)

conn.commit()
cur.close()
conn.close()
