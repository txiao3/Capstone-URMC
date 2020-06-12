# -*- coding: utf-8 -*-
"""
Dynamic Network Graph Exploration

This program aims to: 
Explore interactive design and parameters combo to better detect and define 
interactions between members.

Code credits - Yumeng Xi Changed based on the original code provided by Xavier 
Lambein

"""
#program requirement 
import os

import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
import Preprocessing as pp


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
    
attendees_metadata_filename = "Badge assignments_Attendees_2019.xlsx"
members_metadata_filename = "Member-2019-05-28.csv"
beacons_metadata_filename = "location table.xlsx"
data_dir = "./proximity_2019-06-01/"





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
    
    
    
    
    
    

    
#Network Graph Preparation
'''
Run every block in under this title to prepare for all Dynamic Network Member 
Graph analysis
'''  
 # create time slices
def generate_time_slices(start_h, start_m, end_h, end_m, interval=2):
    
    time_slices = []
    
    start = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
    duration = (end_h - start_h) * 60 + (end_m - start_m)
    
    for i in range(int(duration/interval)+1):
        
        if start_h > end_h:
            break
        elif start_h == end_h:
            if start_m > end_m:
                break
            
        tmp_h = start_h
        tmp_m = start_m
        
        if tmp_m < 60-interval+1:
            tmp_m += interval-1
        else:
            tmp_h += 1
            if interval > 1:
                tmp_m = tmp_m - 60 + interval-1
            else:
                tmp_m = tmp_m - 60 + interval
            
        tmp_time = '2019-06-01 {:02}:{:02}'.format(tmp_h, tmp_m)
        
        time_slices.append(slice(start, tmp_time))
        
        start_h = tmp_h
        start_m = tmp_m
        
        if start_m == 59:
            start_h += 1
            start_m = 0
        else:
            start_m += 1
        
        start = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
    return time_slices   
    
    
# create time slices
def generate_time_points(start_h, start_m, end_h, end_m, interval=2):
    time_points = []
    tmp_time = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
    duration = (end_h - start_h) * 60 + (end_m - start_m)
    start_m=start_m-1
    for i in range(int(duration/interval)+1):
        if start_m < 60-interval:
            start_m += interval
        else:
            start_h += 1
            start_m = start_m - 60 + interval
        tmp_time = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
        if start_h>=end_h and start_m>=end_m:
            tmp_time = '2019-06-01 {:02}:{:02}'.format(end_h, end_m)
            time_points.append(tmp_time)
            break
        time_points.append(tmp_time)
        
    return time_points

#data prep
tmp_m2ms_sorted = tmp_m2ms.sort_index(0,0)  
    
    
#Define draw_graph style method 
def draw_graph(G, graph_layout='shell',
               node_size=600, node_color='blue', node_alpha=0.3,
               node_text_size=8,
               edge_color='blue', edge_alpha=0.3, edge_tickness=1,
               edge_text_pos=0.3,
               text_font='sans-serif'):

    # these are different layouts for the network you may try
    # shell seems to work best
    if graph_layout == 'spring':
        graph_pos=nx.spring_layout(G)
    elif graph_layout == 'spectral':
        graph_pos=nx.spectral_layout(G)
    elif graph_layout == 'random':
        graph_pos=nx.random_layout(G)
    else:
        graph_pos=nx.shell_layout(G)

    # draw graph
    nx.draw_networkx_nodes(G,graph_pos,node_size=node_size, 
                           alpha=node_alpha, node_color=node_color, cmap=plt.get_cmap('jet'))
    nx.draw_networkx_edges(G,graph_pos,width=edge_tickness,
                           alpha=edge_alpha,edge_color=edge_color)
    nx.draw_networkx_labels(G, graph_pos,font_size=node_text_size,
                            font_family=text_font)

    plt.show()
   
    
    
#Network Graph Basic Example
'''
This trunk is only for demonstration, not for analysis.
'''    
def NetworkGraphBasicExample(timestart,timeend):
    # Filter data from specific time period
    
    time_slice = slice(timestart,timeend)
    m2m_breakout = tmp_m2ms_sorted.loc[time_slice]
    
    # keep only instances with strong signal
    m2m_filter_rssi = m2m_breakout[m2m_breakout.rssi >= -70].copy()
    print(len(m2m_filter_rssi))
    
    # Count number of time members were in close proximity
    # We name the count column "weight" so that networkx will use it as weight for the spring layout
    m2m_edges = m2m_filter_rssi.groupby(['member1', 'member2'])[['rssi_weighted_mean']].count().rename(columns={'rssi_weighted_mean':'weight'})
    m2m_edges = m2m_edges[["weight"]].reset_index()
    
    # Keep strongest edges (threshold set manually)
    m2m_edges = m2m_edges[m2m_edges.weight > 15]
    print(len(m2m_edges))
    
    # Create a graph
    graph=nx.from_pandas_edgelist(m2m_edges, "member1", "member2", "weight")
    fig = plt.figure(figsize=(12, 10), dpi=150)
    ax = plt.subplot(1,1,1)
    plt.title("Dynamic Member Network Graph Original")
    draw_graph(graph, graph_layout="spring",node_size=200)
        
    
#Lunch Time Analysis
'''
Use data from lunch time to find the definition of close interaction This part will create a html interactive interface to identify the interactions threshold
'''
def LunchTimeAnalysis():
    # try time slice iteratively
    # Filter data from specific time period
    time_slices=[slice('2019-06-01 11:30', '2019-06-01 11:35'),slice('2019-06-01 11:35', '2019-06-01 11:40'),
                 slice('2019-06-01 11:40', '2019-06-01 11:45'),slice('2019-06-01 11:45', '2019-06-01 11:50'),
                 slice('2019-06-01 11:50', '2019-06-01 11:55'),slice('2019-06-01 11:55', '2019-06-01 12:00'),
                slice('2019-06-01 12:00', '2019-06-01 12:05'),slice('2019-06-01 12:05', '2019-06-01 12:10'),
                slice('2019-06-01 12:10', '2019-06-01 12:15'),slice('2019-06-01 12:15', '2019-06-01 12:20'),]
    for i in range(1,11):
        time_slice = time_slices[i-1]
        m2m_breakout = tmp_m2ms_sorted.loc[time_slice]
        # keep only instances with strong signal
        m2m_filter_rssi = m2m_breakout[m2m_breakout.rssi >= -75].copy()
        # Count number of time members were in close proximity
        # We name the count column "weight" so that networkx will use it as weight for the spring layout
        m2m_edges = m2m_filter_rssi.groupby(['member1', 'member2'])[['rssi_weighted_mean']
                                                                   ].count().rename(columns={'rssi_weighted_mean':'weight'})
        m2m_edges = m2m_edges[["weight"]].reset_index()
        # Keep strongest edges (threshold set manually)
        m2m_edges = m2m_edges[m2m_edges.weight > 8]
        # Create a graph
        graph=nx.from_pandas_edgelist(m2m_edges, "member1", "member2", "weight")
        fig = plt.figure(figsize=(12,90), dpi=150)
        ax = plt.subplot(10,1,i)
        plt.title("Dynamic Member Network Graph Lunch Time Analysis")
        draw_graph(graph, graph_layout="spring",node_size=200)
   
plt.show()




#Break-out Session Analysis
'''
Use data from lunch time to find the definition of close interaction
'''
def BreakoutSessionAnalysis():
    #try time slice iteratively
    # Filter data from specific time period
    time_slices=generate_time_slices(9,50,11,20,interval=2)
    
    # time slice creation
    
    
    for i in range(1,46):
        time_slice = time_slices[i-1]
        m2m_breakout = tmp_m2ms_sorted.loc[time_slice]
        # keep only instances with strong signal
        m2m_filter_rssi = m2m_breakout[m2m_breakout.rssi >= -73].copy()
        # Count number of time members were in close proximity
        # We name the count column "weight" so that networkx will use it as weight for the spring layout
        m2m_edges = m2m_filter_rssi.groupby(['member1', 'member2'])[['rssi_weighted_mean']
                                                                   ].count().rename(columns={'rssi_weighted_mean':'weight'})
        m2m_edges = m2m_edges[["weight"]].reset_index()
        # Keep strongest edges (threshold set manually)
        m2m_edges = m2m_edges[m2m_edges.weight > 1]
        print(len(m2m_edges))
        # Create a graph
        graph=nx.from_pandas_edgelist(m2m_edges, "member1", "member2", "weight")
        fig = plt.figure(figsize=(12,400), dpi=150)
        ax = plt.subplot(45,1,i)
        plt.title("Dynamic Member Network Graph Breakout Session")
        draw_graph(graph, graph_layout="spring",node_size=200)
       
    plt.show()

#Interaction Network Graph
'''
This tool helps find interaction between members in a certain amount of time 
with designated parameters, such as signal strength using the distribution of 
signal strength. This graph draws multiple pictures so that less detailed are 
lost in generalization.

To choose a threshold for signal strength, the program analyzes the 
distribution of frequencies of signal strength and it will take the frequency 
of the peak -2. -2 for leave some room for fluctuation.

'''
    
def InteractionNetworkGraph(time_interval_start_h, time_interval_start_m,
                            time_interval_end_h, time_interval_end_m,
                            interval,t_count_threshold):
    #Data&variable preparation 
   
    #create a historgram that is used to find the threhold of signal strength 
    bo1 = generate_time_points(time_interval_start_h, time_interval_start_m, 
                               time_interval_end_h, time_interval_end_m, 
                               interval)
    freq_list_1 = []
    for i in bo1:
        freq_list_1.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])
    
    hist_list_1 = []
    for freq in freq_list_1:
        tmp_freq = []
        for row in freq.iterrows():
            tmp = [row[1][3]]*int(row[1][5])
            tmp_freq = tmp_freq + tmp
        hist_list_1.append(tmp_freq)
    
    
    vals = {}
    for i in range(len(hist_list_1)): 
        for j in hist_list_1[i]:
            if j not in vals.keys():
                vals[j]=1; 
            else: 
                vals[j]=vals[j]+1

    #find the threshold of signal strength 
    import copy
    vals_sorted = copy.deepcopy(sorted(vals.items(), key = 
                 lambda vals:(vals[1], vals[0]),reverse = True))
    sign_threshold = vals_sorted[1][0]
    print("Signal strength threshold determined is " + str(sign_threshold))

    #Data processing and manipulation for the graph 
    time_slices=generate_time_slices(time_interval_start_h,time_interval_start_m,time_interval_end_h,
                                     time_interval_end_m,interval)
    
    for i in range(1,len(time_slices)+1):
        time_slice = time_slices[i-1]
        m2m_breakout = tmp_m2ms_sorted.loc[time_slice]
        # keep only instances with strong signal
        m2m_filter_rssi = m2m_breakout[m2m_breakout.rssi >= sign_threshold].copy()
        # Count number of time members were in close proximity
        # We name the count column "weight" so that networkx will use it as weight for the spring layout
        m2m_edges = m2m_filter_rssi.groupby(['member1', 'member2'])[['rssi_weighted_mean']
                                                                   ].count().rename(columns={'rssi_weighted_mean':'weight'})
        m2m_edges = m2m_edges[["weight"]].reset_index()
        # Keep strongest edges (threshold set manually)
        m2m_edges = m2m_edges[m2m_edges.weight > interval/2]
        if i == 1: 
            m2m_edges_count = m2m_edges.copy().assign(t_count = [1 for x in range(1,len(m2m_edges['member1'])+1)])
            m2m_edges_count = m2m_edges_count.assign(appeared = [False for x in range(1,len(m2m_edges_count)+1)])
        else: 
            for h in range(1,len(m2m_edges['member1'])):        
                for j in range(1,len(m2m_edges_count['member1'])):
                    if m2m_edges.iloc[h,0]==m2m_edges_count.iloc[j,0] and m2m_edges.iloc[h,1]==m2m_edges_count.iloc[j,1]: 
                        m2m_edges_count.iloc[j,3]=m2m_edges_count.iloc[j,3]+1 
                        m2m_edges_count.iloc[j,4]=True
                    elif j==len(m2m_edges_count['member1']):
                        m2m_edges_count.append({'member1':m2m_edges.iloc[h,0],'member2':m2m_edges.iloc[h,1],
                                                'weight':m2m_edges.iloc[h,2],'t_count':m2m_edges.iloc[h,3],
                                               'appeared':True})
            for j in m2m_edges_count.index.values:            
                if m2m_edges_count.loc[j,'appeared']==False:
                    if m2m_edges_count.loc[j,'t_count']<t_count_threshold: 
                        m2m_edges_count.drop(j,inplace=True)
                else: 
                    m2m_edges_count.loc[j,'appeared']=False



    # plot the graph with filtered interactions 
    graph=nx.from_pandas_edgelist(m2m_edges_count, "member1", "member2", "t_count")
    fig = plt.figure(figsize=(12,10), dpi=120)
    plt.title("Interaction Network Graph")
    draw_graph(graph, graph_layout="spring",node_size=200)
    plt.show()
    
    # incorporate background information into the dynamic graph
    
    # read member background data  
    attendees_metadata_filename = "Badge assignments_Attendees_2019.xlsx"
    attendees_metadata = pd.read_excel(data_dir+attendees_metadata_filename)
    background = pd.DataFrame(columns=['name','badge','background','affiliation'])
    background_affiliation = pd.DataFrame(columns=['name','badge','background','affiliation'])
    members_metadata = pd.read_csv(data_dir+members_metadata_filename)
    background['name'] = members_metadata['member']
    background['badge'] = members_metadata['BADGE IP']
    for i in background['badge']:
        if i in attendees_metadata['BADGE IP'].values:
            a = background.loc[background['badge'] == i]
            b = attendees_metadata.loc[attendees_metadata['BADGE IP']==i]
            a.loc['background'] = b['Cleaned Primary discipline/field of interest'].values
            a.loc['affiliation'] = b['Affiliation'].values
            background_affiliation = pd.concat([background_affiliation, a])
        else:
            a = background.loc[background['badge']==i]
            background_affiliation = pd.concat([background_affiliation, a])
            
    
    #create a dictionary for member id and background info
    bg_dict = {}
    for i in range(0,len(background_affiliation)):
        bg_dict.update({background_affiliation.loc[i,'name']: str(background_affiliation.loc[i,'background'])+", "+
                                                              str(background_affiliation.loc[i,'affiliation'])+", "+
                                                              str(i)})
        
    #relabel the nodes with the background infomation
    graph_bg = nx.relabel_nodes(graph,bg_dict)
    
    # create the graph with filtered interactions 
    fig = plt.figure(figsize=(12,10), dpi=150)
    plt.title("Interaction Network Graph with Background Information")
    draw_graph(graph_bg, graph_layout="spring",node_size=200)
    plt.show()




def main():
    NetworkGraphBasicExample('2019-06-01 10:00','2019-06-01 11:20')
    LunchTimeAnalysis()
    BreakoutSessionAnalysis()
    InteractionNetworkGraph(time_interval_start_h=9,time_interval_start_m=50,
                            time_interval_end_h=11,time_interval_end_m=20,
                            interval=2,t_count_threshold = 2)
    
if __name__=="__main__":
    main()
