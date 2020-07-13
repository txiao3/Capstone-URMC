# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 16:49:36 2020

@author: Yumen
"""
import os

import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
import Preprocessing as pp
import Data_Cleaning as dc

def DataCleaning(SELECTED_BEACON,time_zone, attendees_metadata_filename,
                 log_version, time_bins_size, members_metadata_filename,
                 beacons_metadata_filename,data_dir):
    #set global variables 

    SELECTED_BEACON = 12
    time_zone = 'US/Eastern'
    log_version = '2.0'
    time_bins_size = '1min'
    
    proximity_data_filenames = []
    
    for i in range(1, 18):
        if i < 10:
            filename = 'CTSIserver{:02d}_proximity_2019-06-01.txt'.format(i)
        else:
            filename = 'CTSIserver{}_proximity_2019-06-01.txt'.format(i)
            
        proximity_data_filenames.append(filename)
        
  
    
    #Pre-processing on data
    '''
    First, we load two lists that will help us with some of the analysis: list of 
    membmers and list of location beacons
    '''
    members_metadata = pd.read_csv(data_dir+members_metadata_filename)
    beacons_metadata = pd.read_excel(data_dir+beacons_metadata_filename, sheet_name='Sheet1')
    
    
    #beacon data peprocessing 
    '''
    We create a translation table between the badge ID and member key. This is done
    based on the data itself, since it should contain data from all the badges that 
    take part in the study.
    
    Note that we create a <id,member key> pair for ever time bin. While this is not
    necessary at this point, it allows this mapping to change (for example, if a 
    badge is re-assigned to a different member).
    '''
    idmaps = []
    
    for proximity_data_filename in proximity_data_filenames:
        with open(os.path.join(data_dir, proximity_data_filename), 'r') as f:
            idmaps.append(pp.id_to_member_mapping(f, time_bins_size, tz=time_zone))
    tmp_idmaps = idmaps[0]
    for i in range(1, len(idmaps)):
        tmp_idmaps = pd.concat([tmp_idmaps, idmaps[i]])
    #tmp_idmaps.shape   
     
        
        
        
        
    '''
    Using this translation table and the proximity data, we can create a list of 
    "pings" - every time two badges were in close proximity
    '''
    m2badges = []
    
    for proximity_data_filename in proximity_data_filenames:
        with open(os.path.join(data_dir, proximity_data_filename), 'r') as f:
            m2badges.append(pp.member_to_badge_proximity(f, time_bins_size, tz=time_zone))
            
    tmp_m2badges = m2badges[0]
    
    for i in range(1, len(m2badges)):
        tmp_m2badges = pd.concat([tmp_m2badges, m2badges[i]])
    
    
    #tmp_m2badges.shape
    
    
    
    '''
    Since a badge can either be a badge worn by a participant, or a location 
    beacon, we split the dataset into member-to-member (for network graphs) and 
    member-to-beacon (for localization)
    '''
    
    # Member to member
    m2ms = []
    for (m2badge, idmap) in zip(m2badges, idmaps):
        m2ms.append(pp.member_to_member_proximity(m2badge, idmap))
    
    tmp_m2ms = m2ms[0]
    for i in range(1, len(m2ms)):
        tmp_m2ms = pd.concat([tmp_m2ms, m2ms[i]]) 
    
    #tmp_m2ms.shape
        
        
        
    '''
    
    '''   
    # Member to location beacon
    m2bs = []
    for m2badge in m2badges:
        m2bs.append(pp.member_to_beacon_proximity(m2badge, beacons_metadata.set_index('id')['beacon']))
        
    tmp_m2bs = m2bs[0]
    for i in range(1, len(m2bs)):
        tmp_m2bs = pd.concat([tmp_m2bs, m2bs[i]])  
        
    # match the badge ID in attendee data with the name in members 
    attendees_metadata_filename = "Badge assignments_Attendees_2019.xlsx"
    attendees_metadata = pd.read_excel(data_dir+attendees_metadata_filename)
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
    
    return tmp_m2ms,tmp_m2bs,attendees_metadata,members_metadata
    