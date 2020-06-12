import os, sys
import logging
import gzip

import pandas as pd
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import networkx as nx
import Preprocessing
from collections import Counter
# Import the data analysis tools
#import openbadge_analysis as ob
#import openbadge_analysis.preprocessing
#import openbadge_analysis.core

SELECTED_BEACON = 12

time_zone = 'US/Eastern'
log_version = '2.0'
time_bins_size = '1min'

proximity_data_filenames = []

# get all files from the folder
for i in range(1, 18):
    if i < 10:
        filename = 'CTSIserver{:02d}_proximity_2019-06-01.txt'.format(i)
    else:
        filename = 'CTSIserver{}_proximity_2019-06-01.txt'.format(i)
        
    proximity_data_filenames.append(filename)
    
members_metadata_filename = "Member-2019-05-28.csv"
beacons_metadata_filename = "location table.xlsx"
attendees_metadata_filename = "Badge assignments_Attendees_2019.xlsx"
data_dir = "../proximity_2019-06-01/"

members_metadata = pd.read_csv(data_dir+members_metadata_filename)
members_metadata.dropna()
beacons_metadata = pd.read_excel(data_dir+beacons_metadata_filename, sheet_name='Sheet1')
attendees_metadata = pd.read_excel(data_dir+attendees_metadata_filename)
# import all the data - preprocessing
idmaps = []
# get the proximity data from the file, raw data
for proximity_data_filename in proximity_data_filenames:
    with open(os.path.join(data_dir, proximity_data_filename), 'r') as f:
        #idmaps.append(ob.preprocessing.id_to_member_mapping(f, time_bins_size, tz=time_zone))
        idmaps.append(Preprocessing.id_to_member_mapping(f, time_bins_size, tz=time_zone))
tmp_idmaps = idmaps[0]
for i in range(1, len(idmaps)):
    tmp_idmaps = pd.concat([tmp_idmaps, idmaps[i]])
    
    
idmap_beacon = tmp_idmaps
startmin = 25

for i in beacons_metadata[['key', 'id']].iterrows():
    beacon_id = i[1][1]
    beacon_key = i[1][0]
    
    tmp = pd.DataFrame(['2019-06-01 14:{}:00-04:00'.format(startmin), beacon_id, beacon_key]).transpose()
    startmin += 1
    tmp.columns = ['datetime', 'id', 'member']
    idmap_beacon = pd.concat([idmap_beacon, tmp])

m2badges = []

for proximity_data_filename in proximity_data_filenames:
    with open(os.path.join(data_dir, proximity_data_filename), 'r') as f:
        m2badges.append(Preprocessing.member_to_badge_proximity(f, time_bins_size, tz=time_zone))
        
tmp_m2badges = m2badges[0]

for i in range(1, len(m2badges)):
    tmp_m2badges = pd.concat([tmp_m2badges, m2badges[i]])
    

# Member to member
m2ms = []
for (m2badge, idmap) in zip(m2badges, idmaps):
    m2ms.append(Preprocessing.member_to_member_proximity(m2badge, idmap))
    
tmp_m2ms = m2ms[0]
for i in range(1, len(m2ms)):
    tmp_m2ms = pd.concat([tmp_m2ms, m2ms[i]])


# Member to location beacon
m2bs = []
for m2badge in m2badges:
    m2bs.append(Preprocessing.member_to_beacon_proximity(m2badge, beacons_metadata.set_index('id')['beacon']))
    
tmp_m2bs = m2bs[0]
for i in range(1, len(m2bs)):
    tmp_m2bs = pd.concat([tmp_m2bs, m2bs[i]])


m5cb = tmp_m2bs.reset_index().groupby(['datetime', 'member'])['rssi', 'beacon'] \
        .apply(lambda x: x.nlargest(5, columns=['rssi']) \
        .reset_index(drop=True)[['beacon']]).unstack()['beacon'].fillna(-1).astype(int)


# match the badge ID in attendee data with the name in members 
attend_id = attendees_metadata['BADGE #'].values.astype('str').tolist()
member_id = members_metadata['name'].values.astype('str').tolist()
drop_member = []
for i in member_id:
    if i not in attend_id:
        drop_member.append(i)
drop_name = []
key_name = members_metadata['member'].values.astype('str').tolist()
value_id = members_metadata['name'].values.astype('str').tolist()
dic_name_id = dict(zip(key_name,value_id))
for i in dic_name_id:
    if dic_name_id[i] in drop_member:
        drop_name.append(i)

# delete the member to member records which does not have attendee information
drop_index = []
tmp_m2ms = tmp_m2ms.reset_index()
for index,row in tmp_m2ms.iterrows():
    if row['member1'] in drop_name:
        drop_index.append(index)
    elif row['member2'] in drop_name:
        drop_index.append(index)


tmp_m2ms = tmp_m2ms.drop(drop_index)
tmp_m2ms = tmp_m2ms.set_index(['datetime'])


import heatmap_functions

time_slice = slice('2019-06-01 10:00', '2019-06-01 10:00')
breakout1 = slice('2019-06-01 09:50', '2019-06-01 10:39')
breakout2 = slice('2019-06-01 10:40', '2019-06-01 11:30')
breakout3 = slice('2019-06-01 13:10', '2019-06-01 13:59')
breakout4 = slice('2019-06-01 14:00', '2019-06-01 14:50')
lunch = slice('2019-06-01 11:40','2019-06-01 13:00')
whole_session = slice('2019-06-01 9:05','2019-06-01 14:50')
time_period = whole_session
background,background_affiliation = heatmap_functions.background_dataframe(attendees_metadata,members_metadata)
all_name,hm = heatmap_functions.big_heatmap(tmp_m2ms,background_affiliation,time_period)
hm_copy,hm,name_major,drop_col = heatmap_functions.groupby_same_major_aff(background_affiliation,all_name,hm)
hm_copy_2 = heatmap_functions.groupby_diff_major_aff(hm_copy,hm,name_major,drop_col)
heatmap_functions.p_values(background_affiliation,all_name,hm,hm_copy_2)
