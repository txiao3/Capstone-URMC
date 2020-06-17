#!/usr/bin/env python
# coding: utf-8

# # Signal Strength Distribution
# Generate histograms for distribution of signal strength with count as frequency.

# ### Required Libraries

# In[1]:


import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import networkx as nx

import os, sys
import logging
import json
import gzip
import glob

from collections import Counter

# Import the data analysis tools
import openbadge_analysis as ob
import openbadge_analysis.preprocessing
import openbadge_analysis.core

SELECTED_BEACON = 12
plt.rcParams.update({'figure.max_open_warning': 0})


# In[2]:


ob.__version__


# ### Settings
# The time_zone variable will be used when converting the timestamp form UTC time to your local time.

# In[3]:


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
    
members_metadata_filename = "Member-2019-05-28.csv"
beacons_metadata_filename = "location table.xlsx"
attendees_metadata_filename = "Badge assignments_Attendees_2019.xlsx"
# data_dir = "/your_local_directory_to_proximity_2019-06-01/"
data_dir = "/Users/songziyu/Desktop/Capstone-URMC/proximity_2019-06-01/"


# ### Pre-processing
# First, we load two lists that will help us with some of the analysis: list of membmers and list of location beacons

# In[4]:


members_metadata = pd.read_csv(data_dir+members_metadata_filename)
members_metadata.dropna()
beacons_metadata = pd.read_excel(data_dir+beacons_metadata_filename, sheet_name='Sheet1')
attendees_metadata = pd.read_excel(data_dir+attendees_metadata_filename)


# In[5]:


members_metadata


# ### beacons_metadata

# We create a translation table between the badge ID and member key. This is done based on the data itself, since it should contain data from all the badges that take part in the study. 
# 
# Note that we create a <id,member key> pair for ever time bin. While this is not necessary at this point, it allows this mapping to change (for example, if a badge is re-assigned to a different member).

# In[6]:


idmaps = []

for proximity_data_filename in proximity_data_filenames:
    with open(os.path.join(data_dir, proximity_data_filename), 'r') as f:
        idmaps.append(ob.preprocessing.id_to_member_mapping(f, time_bins_size, tz=time_zone))
        
tmp_idmaps = idmaps[0]
for i in range(1, len(idmaps)):
    tmp_idmaps = pd.concat([tmp_idmaps, idmaps[i]])


# In[7]:


idmap_beacon = tmp_idmaps
startmin = 25
for i in beacons_metadata[['key', 'id']].iterrows():
    beacon_id = i[1][1]
    beacon_key = i[1][0]
    
    tmp = pd.DataFrame(['2019-06-01 14:{}:00-04:00'.format(startmin), beacon_id, beacon_key]).transpose()
    startmin += 1
    tmp.columns = ['datetime', 'id', 'member']
    idmap_beacon = pd.concat([idmap_beacon, tmp])


# Using this translation table and the proximity data, we can create a list of "pings" - every time two badges were in close proximity

# In[8]:


m2badges = []

for proximity_data_filename in proximity_data_filenames:
    with open(os.path.join(data_dir, proximity_data_filename), 'r') as f:
        m2badges.append(ob.preprocessing.member_to_badge_proximity(f, time_bins_size, tz=time_zone))
        
tmp_m2badges = m2badges[0]

for i in range(1, len(m2badges)):
    tmp_m2badges = pd.concat([tmp_m2badges, m2badges[i]])


# Since a badge can either be a badge worn by a participant, or a location beacon, we split the dataset into member-to-member (for network graphs) and member-to-beacon (for localization)

# In[9]:


# Member to member
m2ms = []
for (m2badge, idmap) in zip(m2badges, idmaps):
    m2ms.append(ob.preprocessing.member_to_member_proximity(m2badge, idmap))
    
tmp_m2ms = m2ms[0]
for i in range(1, len(m2ms)):
    tmp_m2ms = pd.concat([tmp_m2ms, m2ms[i]])


# In[10]:


tmp_m2ms.reset_index().set_index('datetime').loc[slice('2019-06-01 07:45', '2019-06-01 14:50')]


# In[11]:


# Member to location beacon
m2bs = []
for m2badge in m2badges:
    m2bs.append(ob.preprocessing.member_to_beacon_proximity(m2badge, beacons_metadata.set_index('id')['beacon']))
    
tmp_m2bs = m2bs[0]
for i in range(1, len(m2bs)):
    tmp_m2bs = pd.concat([tmp_m2bs, m2bs[i]])


#  Member 5 closest beacons

# In[12]:


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


# In[13]:


drop_index = []
tmp_m2ms = tmp_m2ms.reset_index()
for index,row in tmp_m2ms.iterrows():
    if row['member1'] in drop_name:
        drop_index.append(index)
    elif row['member2'] in drop_name:
        drop_index.append(index)


# In[14]:


tmp_m2ms = tmp_m2ms.drop(drop_index)


# In[15]:


tmp_m2ms = tmp_m2ms.set_index(['datetime'])


# In[16]:


# create time slices
def generate_time_points(start_h, start_m, end_h, end_m, interval=3):
    time_slices = []
    start = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
    duration = (end_h - start_h) * 60 + (end_m - start_m)
    for i in range(duration/interval+1):
        if start_m < 60-interval+1:
            start_m += interval-1
        else:
            start_h += 1
            start_m = start_m - 60 + interval
        tmp_time = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
        
        time_slices.append(slice(start, tmp_time))
        
        if start_m >= 59:
            start = '2019-06-01 {:02}:{:02}'.format(start_h+1, 0)
        else:
            start_m += 1
            start = '2019-06-01 {:02}:{:02}'.format(start_h, start_m)
        
    return time_slices


# In[17]:


bo1 = generate_time_points(9, 50, 10, 30)
bo2 = generate_time_points(10, 40, 11, 20)
bo3 = generate_time_points(13, 10, 13, 50)
bo4 = generate_time_points(14, 0, 14, 40)
lunch = generate_time_points(11, 30, 12, 20)

breakout1 = slice('2019-06-01 9:50', '2019-06-01 10:35')
breakout2 = slice('2019-06-01 10:40', '2019-06-01 11:25')
breakout3 = slice('2019-06-01 13:10', '2019-06-01 13:55')
breakout4 = slice('2019-06-01 14:00', '2019-06-01 14:45')
breakout = [breakout1, breakout2, breakout3, breakout4]


# **Signal Strength Distribution For Entire Day**

# In[18]:


# histogram for whole day
whole_day = slice('2019-06-01 9:05', '2019-06-01 14:50')
day = [whole_day]
day_freq_list = []
for i in day:
    day_freq_list.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

day_hist_list = []
for freq in day_freq_list:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    day_hist_list.append(tmp_freq)

pic_dir = "./histograms/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

plt.figure(figsize=(10,8))
plt.hist(day_hist_list, bins=50)
plt.xlabel('RSSI', fontsize=16)
plt.ylabel('Frequency', fontsize=16)
plt.title('All Sessions', fontsize=20)
plt.savefig(pic_dir + 'all sessions.png')


# **Signal Strength Distribution for Each Breakout Session and Lunch Break**

# In[19]:


# each breakout session
freq_list = []
for i in breakout:
    freq_list.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

hist_list = []
for freq in freq_list:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    hist_list.append(tmp_freq)

pic_dir = "./histograms/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

for i in range(1, 5):
    plt.figure(figsize=(10,8))
    plt.hist(hist_list[i-1], bins=50)
    plt.xlim([-100, -40])
    plt.ylim([0, 15000])
    plt.xlabel('RSSI', fontsize=16)
    plt.ylabel('Frequency', fontsize=16)
    plt.title('breakout{}'.format(i), fontsize=20)
    plt.savefig(pic_dir + 'breakout{}.png'.format(i))


# In[20]:


# histogram for lunch break
lunch_time = slice('2019-06-01 11:30', '2019-06-01 12:20')
lunch_break = [lunch_time]
lunch_freq_list = []
for i in lunch_break:
    lunch_freq_list.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

lunch_hist_list = []
for freq in lunch_freq_list:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    lunch_hist_list.append(tmp_freq)

pic_dir = "./histograms/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

plt.figure(figsize=(10,8))
plt.hist(lunch_hist_list, bins=50)
plt.xlim([-100, -40])
plt.ylim([0, 10000])
plt.xlabel('RSSI', fontsize=16)
plt.ylabel('Frequency', fontsize=16)
plt.title('lunch break', fontsize=20)
plt.savefig(pic_dir + 'lunch break.png')


# **Generate signal strength distribution by every 3 minutes**

# **breakout session 1**

# In[21]:


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

pic_dir = "./histograms/breakout1/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

for i in range(1, 15):
    plt.figure(figsize=(20,10))
    plt.hist(hist_list_1[i-1], bins=50)
    plt.xlim([-100, -40])
    plt.ylim([0, 1500])
    plt.xlabel('RSSI', fontsize=16)
    plt.ylabel('Frequency', fontsize=16)
    plt.title('breakout1_{}'.format(i), fontsize=20)
    plt.savefig(pic_dir + 'breakout1_{}.png'.format(i))


# In[22]:


os.getcwd()


# In[23]:


from natsort import natsorted
from moviepy.editor import *

base_dir = os.path.realpath('./histograms/breakout1')
print(base_dir)

img_list = glob.glob('./histograms/breakout1/*.png')  # Get all the pngs in the current directory
img_list_sorted = natsorted(img_list, reverse=False)  # Sort the images

clips = [ImageClip(m).set_duration(0.8)
         for m in img_list_sorted]

concat_clip = concatenate_videoclips(clips, method="compose")
concat_clip.write_videofile("breakout1.mp4", fps=25)


# **breakout session 2**

# In[24]:


freq_list_2 = []
for i in bo2:
    freq_list_2.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

hist_list_2 = []
for freq in freq_list_2:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    hist_list_2.append(tmp_freq)

pic_dir = "./histograms/breakout2/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

for i in range(1, 15):
    plt.figure(figsize=(20,10))
    plt.hist(hist_list_2[i-1], bins=50)
    plt.xlim([-100, -40])
    plt.ylim([0, 1500])
    plt.xlabel('RSSI', fontsize=16)
    plt.ylabel('Frequency', fontsize=16)
    plt.title('breakout2_{}'.format(i), fontsize=20)
    plt.savefig(pic_dir + 'breakout2_{}.png'.format(i))


# In[25]:


from natsort import natsorted
from moviepy.editor import *

base_dir = os.path.realpath('./histograms/breakout2')
print(base_dir)

img_list = glob.glob('./histograms/breakout2/*.png')  # Get all the pngs in the current directory
img_list_sorted = natsorted(img_list, reverse=False)  # Sort the images

clips = [ImageClip(m).set_duration(0.8)
         for m in img_list_sorted]

concat_clip = concatenate_videoclips(clips, method="compose")
concat_clip.write_videofile("breakout2.mp4", fps=25)


# **lunch session**

# In[26]:


freq_list_l = []
for i in lunch:
    freq_list_l.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

hist_list_l = []
for freq in freq_list_l:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    hist_list_l.append(tmp_freq)

pic_dir = "./histograms/lunch/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

for i in range(1, 15):
    plt.figure(figsize=(20,10))
    plt.hist(hist_list_l[i-1], bins=50)
    plt.xlim([-100, -40])
    plt.ylim([0, 1500])
    plt.xlabel('RSSI', fontsize=16)
    plt.ylabel('Frequency', fontsize=16)
    plt.title('lunch_{}'.format(i), fontsize=20)
    plt.savefig(pic_dir + 'lunch_{}.png'.format(i))


# In[27]:


from natsort import natsorted
from moviepy.editor import *

base_dir = os.path.realpath('./histograms/lunch')
print(base_dir)

img_list = glob.glob('./histograms/lunch/*.png')  # Get all the pngs in the current directory
img_list_sorted = natsorted(img_list, reverse=False)  # Sort the images

clips = [ImageClip(m).set_duration(0.8)
         for m in img_list_sorted]

concat_clip = concatenate_videoclips(clips, method="compose")
concat_clip.write_videofile("lunch.mp4", fps=25)


# **breakout session 3**

# In[28]:


freq_list_3 = []
for i in bo3:
    freq_list_3.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

hist_list_3 = []
for freq in freq_list_3:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    hist_list_3.append(tmp_freq)

pic_dir = "./histograms/breakout3/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

for i in range(1, 15):
    plt.figure(figsize=(20,10))
    plt.hist(hist_list_3[i-1], bins=50)
    plt.xlim([-100, -40])
    plt.ylim([0, 1500])
    plt.xlabel('RSSI', fontsize=16)
    plt.ylabel('Frequency', fontsize=16)
    plt.title('breakout3_{}'.format(i), fontsize=20)
    plt.savefig(pic_dir + 'breakout3_{}.png'.format(i))


# In[29]:


from natsort import natsorted
from moviepy.editor import *

base_dir = os.path.realpath('./histograms/breakout3')
print(base_dir)

img_list = glob.glob('./histograms/breakout3/*.png')  # Get all the pngs in the current directory
img_list_sorted = natsorted(img_list, reverse=False)  # Sort the images

clips = [ImageClip(m).set_duration(0.8)
         for m in img_list_sorted]

concat_clip = concatenate_videoclips(clips, method="compose")
concat_clip.write_videofile("breakout3.mp4", fps=25)


# **breakout session 4**

# In[30]:


freq_list_4 = []
for i in bo4:
    freq_list_4.append(tmp_m2ms.reset_index().set_index('datetime').loc[i])

hist_list_4 = []
for freq in freq_list_4:
    tmp_freq = []
    for row in freq.iterrows():
        tmp = [row[1][3]]*int(row[1][5])
        tmp_freq = tmp_freq + tmp
    hist_list_4.append(tmp_freq)

pic_dir = "./histograms/breakout4/"
if not os.path.isdir(pic_dir):
    os.makedirs(pic_dir)

for i in range(1, 15):
    plt.figure(figsize=(20,10))
    plt.hist(hist_list_4[i-1], bins=50)
    plt.xlim([-100, -40])
    plt.ylim([0, 1500])
    plt.xlabel('RSSI', fontsize=16)
    plt.ylabel('Frequency', fontsize=16)
    plt.title('breakout4_{}'.format(i), fontsize=20)
    plt.savefig(pic_dir + 'breakout4_{}.png'.format(i))


# In[31]:


from natsort import natsorted
from moviepy.editor import *

base_dir = os.path.realpath('./histograms/breakout4')
print(base_dir)

img_list = glob.glob('./histograms/breakout4/*.png')  # Get all the pngs in the current directory
img_list_sorted = natsorted(img_list, reverse=False)  # Sort the images

clips = [ImageClip(m).set_duration(0.8)
         for m in img_list_sorted]

concat_clip = concatenate_videoclips(clips, method="compose")
concat_clip.write_videofile("breakout4.mp4", fps=25)


# In[ ]:




