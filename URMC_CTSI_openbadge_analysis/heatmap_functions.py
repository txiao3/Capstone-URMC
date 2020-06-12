# get a dataframe with the member badge Key and corresponding background field and affiliation
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def background_dataframe(attendees_metadata,members_metadata):
    background = pd.DataFrame(columns=['name','badge','background','affiliation'])
    background_affiliation = pd.DataFrame(columns=['name','badge','background','affiliation'])
    background['name'] = members_metadata['member']
    background['badge'] = members_metadata['BADGE IP']
    for i in background['badge']:
        if i in attendees_metadata['BADGE IP'].values:
            a = background.loc[background['badge'] == i]
            b = attendees_metadata.loc[attendees_metadata['BADGE IP']==i]
            a['background'] = b['Cleaned Primary discipline/field of interest More generalized'].values
            a['affiliation'] = b['Affiliation'].values
            background_affiliation = pd.concat([background_affiliation, a])
        else:
            a = background.loc[background['badge']==i]
            background_affiliation = pd.concat([background_affiliation, a])
    return background, background_affiliation


def big_heatmap(tmp_m2ms,background_affiliation,time_period):
    #time_slice = slice('2019-06-01 10:00', '2019-06-01 10:00')
    #breakout1 = slice('2019-06-01 09:50', '2019-06-01 10:39')
    #breakout2 = slice('2019-06-01 10:40', '2019-06-01 11:30')
    #breakout3 = slice('2019-06-01 13:10', '2019-06-01 13:59')
    #breakout4 = slice('2019-06-01 14:00', '2019-06-01 14:50')
    #lunch = slice('2019-06-01 11:40','2019-06-01 13:00')
    #whole_session = slice('2019-06-01 9:05','2019-06-01 14:50')
    period = tmp_m2ms.loc[time_period]
    period = period[period.rssi >= -76].copy()

    name_1 = period['member1'].unique().tolist()
    name_2 = period['member2'].unique().tolist()
    all_name = name_1+name_2
    all_name = set(all_name)
    all_name = sorted(all_name)

    hm = pd.DataFrame(columns=all_name)

# loop through the whole list:
    for n in all_name:
        haha = []
        name_values = [0]*len(all_name)
        dimension_ = dict(zip(all_name,name_values))
    # get all the rows with one name in the name list
        t = period.loc[period['member1'] == n]
        t2 = t['member2'].unique()
        t2_values = [0]*len(t2)
        hahaha = dict(zip(t2,t2_values))
    
        for i in range(len(t2)):
            interacted = t.loc[t['member2'] == t2[i]]
            hahaha[t2[i]] = sum(interacted['count_sum'])
        for i in dimension_: 
            if i in hahaha:
                dimension_[i] = hahaha[i]
    #haha = dimension_.values()
        for i in all_name:
            haha.append(dimension_[i])
        temporary = pd.DataFrame([haha], columns = all_name)
        hm = pd.concat([hm, temporary],ignore_index = True)
        
    label_background = []
    label_aff = []
    for i in all_name:
        j = background_affiliation.loc[background_affiliation['name']== i]
        k = j['background'].values.astype('str')
        k = k.item()
        o = j['affiliation'].values.astype('str')
        o = o.item()
        label_background.append(k)
        label_aff.append(o)


# fig, ax = plt.subplots(figsize=(8,8))
# Sample figsize in inches

    plt.figure(figsize = (25,20))
    cmap = sns.cubehelix_palette(8,start=2,rot=0,dark =0, light = 0.95,as_cmap=True)
    hm_values = hm.values.astype(str).astype(float)
    ax = sns.heatmap(hm_values, xticklabels= label_background, yticklabels= label_background,cmap=cmap)
    plt.title('Member to Member Interaction Distribtion For The Whole Conference',fontsize = 40)
    plt.xlabel('All Members Fields (following alphabetical order)',fontsize = 40)
    plt.ylabel('All Members Fields (following alphabetical order)',fontsize = 40)
    cax = plt.gcf().axes[-1]
    cax.tick_params(labelsize=40)
    
    return all_name,hm

def groupby_same_major_aff(background_affiliation,all_name,hm):
    name_major = dict(zip(background_affiliation['name'].values.astype('str').tolist(),
                      background_affiliation['background'].values.astype('str').tolist()))
    hm['member_name'] = all_name
    hm = hm.set_index('member_name')
    # only take the ones with targeted background field
    drop_col = []
    target_major = 'Data Science'
    hm_copy = hm
    for index,row in hm.iterrows():
        if name_major[index] != target_major:
            hm_copy = hm_copy.drop(index)
            drop_col.append(index)
    hm_copy = hm_copy.drop(drop_col,axis = 1)

    plt.figure(figsize = (25,20))
    cmap = sns.cubehelix_palette(8,start=2,rot=0,dark =0, light = 0.95,as_cmap=True)
#cmap = sns.cubehelix_palette(8,start=0.5,rot=-0.75,as_cmap=True)
#cmap = sns.cubehelix_palette(8)
    hm_copy_values = hm_copy.values.astype(str).astype(float)
    ax = sns.heatmap(hm_copy_values,vmin=0, vmax=800,cmap=cmap)
    cax = plt.gcf().axes[-1]
    cax.tick_params(labelsize=40)
    ax.tick_params(axis='y',labelsize= 20,rotation =0)
    plt.title('Interaction within Data Science Field for the whole session',fontsize = 40)
    plt.xlabel('All Data Science Member (Following alphabetical order)', fontsize = 40)
    plt.ylabel('All Data Science Member (Following alphabetical order)', fontsize = 40)

    return hm_copy,hm,name_major,drop_col

def groupby_diff_major_aff(hm_copy,hm,name_major,drop_col):
    test = hm_copy.values.astype(str).astype(float)
    self = []
    for l in test.tolist():
        self += l
    # get the target field with fields other than this field
    drop_col_2 = []
    target_major = 'Data Science'
    hm_copy_2 = hm
    for index,row in hm.iterrows():
        if name_major[index] == target_major:
            hm_copy_2 = hm_copy_2.drop(index)
            drop_col_2.append(index)
    hm_copy_2 = hm_copy_2.drop(drop_col,axis = 1)

    hm_copy_2 = hm_copy_2.transpose()
    
    plt.figure(figsize = (22,16))
    hm_copy_values_2 = hm_copy_2.values.astype(str).astype(float)
    cmap = sns.cubehelix_palette(8,start=2,rot=0,dark =0, light = 0.95,as_cmap=True)
    ax = sns.heatmap(hm_copy_values_2,vmin=0, vmax=800,cmap=cmap)
    cax = plt.gcf().axes[-1]
    cax.tick_params(labelsize=40)
    plt.title('Interaction between Data Science Field and Non-Data Science Filed for the whole session',fontsize = 30)
    plt.ylabel('All Data Science Member', fontsize = 20)
    plt.xlabel('ALL Members other than Data Science Members', fontsize = 40)

    return hm_copy_2

def p_values(background_affiliation,all_name,hm,hm_copy_2):
    test_2 = hm_copy_2.values.astype(str).astype(float)
    self_2 = []
    for l in test_2.tolist():
        self_2 += l
    # get p-values from each comparison

    from scipy.stats import ttest_ind
    name_major = dict(zip(background_affiliation['name'].values.astype('str').tolist(),
                      background_affiliation['background'].values.astype('str').tolist()))
    hm['member_name'] = all_name
    hm = hm.set_index('member_name')
    unique_back = list(set(background_affiliation['background'].values.astype('str')))
    all_p = []
    for i in range(len(unique_back)):
    
        drop_col = []
        target_major = unique_back[i]
        hm_copy = hm
        for index,row in hm.iterrows():
            if name_major[index] != target_major:
                hm_copy = hm_copy.drop(index)
                drop_col.append(index)
        hm_copy = hm_copy.drop(drop_col,axis = 1)

        test = hm_copy.values.astype(str).astype(float)
        self = []
        for l in test.tolist():
            self += l

        drop_col_2 = []
        hm_copy_2 = hm
        for index,row in hm.iterrows():
            if name_major[index] == target_major:
                hm_copy_2 = hm_copy_2.drop(index)
                drop_col_2.append(index)
        hm_copy_2 = hm_copy_2.drop(drop_col,axis = 1)

        test_2 = hm_copy_2.values.astype(str).astype(float)
        self_2 = []
        for l in test_2.tolist():
            self_2 += l
    
        ttest,pval = ttest_ind(self,self_2)
        all_p.append(pval)
    back_pvalue = dict(zip(unique_back,all_p))
    ordered_key = []
    ordered_value = []
    for key in back_pvalue:
        ordered_key.append(key)
        ordered_value.append(back_pvalue[key])
    plt.barh(ordered_key,ordered_value)
    plt.axvline(x=0.05,color = 'red',label = 'alpha: 0.05')
    plt.legend()
    plt.title('P-values for all the comparisions', fontsize = 20)
    plt.xlabel('P-values')