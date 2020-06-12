#For Hublog, import the following packages
from __future__ import absolute_import, division, print_function
import pandas as pd
import re

#for core, import the following packages
import crc16

#for raw, import the following packages
import json
import os
import datetime

#for metadata, import the following packages
import pandas as pd
import json
import io

#for proximity, import the following packages
import pandas as pd
import json
import collections


#from core import mac_address_to_id
def mac_address_to_id(mac):
    """Converts a MAC address to an id used by the badges for the proximity pings.
    """
    # convert hex to bytes and reverse
    macstr = mac.replace(':', '').decode('hex')[::-1]
    crc = crc16.crc16xmodem("b"+macstr,0xFFFF)
    return crc



#from .raw import split_raw_data_by_day
def split_raw_data_by_day(fileobject, target, kind, log_version=None):
    """Splits the data from a raw data file into a single file for each day.

    Parameters
    ----------
    fileobject : object, supporting tell, readline, seek, and iteration.
        The raw data to be split, for instance, a file object open in read mode.

    target : str
        The directory into which the files will be written.  This directory must
        already exist.

    kind : str
        The kind of data being extracted, either 'audio' or 'proximity'.

    log_version : str
        The log version, in case no metadata is present.
    """
    # The days fileobjects
    
    # It's a mapping from iso dates (e.g. '2017-07-29') to fileobjects
    days = {}
    
    # Extract log version from metadata, if present
    log_version = extract_log_version(fileobject) or log_version

    if log_version not in ('1.0', '2.0'):
        raise Exception('file log version was not set and cannot be identified')

    if log_version in ('1.0'):
        raise Exception('file version '+str(log_version)+'is no longer supported')

    # Read each line
    for line in fileobject:
        data = json.loads(line)

        # Keep only relevant data
        if not data['type'] == kind + ' received':
            continue

        # Extract the day from the timestamp
        day = datetime.date.fromtimestamp(data['data']['timestamp']).isoformat()

        # If no fileobject exists for that day, create one
        if day not in days:
            days[day] = open(os.path.join(target, day), 'a')

        # Write the data to the corresponding day file
        json.dump(data, days[day])
        days[day].write('\n')
    
    # Free the memory
    for f in days.values():
        f.close()



#from .metadata import id_to_member_mapping
def id_to_member_mapping(mapper, time_bins_size='1min', tz='US/Eastern', fill_gaps=True):
    """Creates a pd.Series mapping member numeric IDs to the string
    member key associated with them. 

    If the 'mapper' provided is a DataFrame, assumes it's metadata and that ID's 
        do not change mapping throughout the project, and proceeds to create a
        Series with only a member index.
    If the 'mapper' provided is a file object, assumes the old version of id_map
        and creates a Series with a datetime and member index.

    Parameters
    ----------
    fileobject : file object
        A file to read to determine the mapping.
    
    members_metadata : pd.DataFrame
        Metadata dataframe, as downloaded from the server, to map IDs to keys.
        
    Returns
    -------
    pd.Series : 
        The ID to member key mapping.
    
    """
    if isinstance(mapper, io.BufferedIOBase) | isinstance(mapper, io.IOBase):
        idmap = legacy_id_to_member_mapping(mapper, time_bins_size=time_bins_size, tz=tz, fill_gaps=fill_gaps)
        print(type(mapper))
        return idmap
    elif isinstance(mapper, pd.DataFrame):
        idmap = {row.member_id: row.member for row in mapper.itertuples()}
        return pd.DataFrame.from_dict(idmap, orient='index')[0].rename('member')
    else:
        raise ValueError("You must provide either a fileobject or metadata dataframe as the mapper.")


# from .metadata import legacy_id_to_member_mapping
def legacy_id_to_member_mapping(fileobject, time_bins_size='1min', tz='US/Eastern', fill_gaps=True):
    """Creates a mapping from badge id to member, for each time bin, from proximity data file.
    Depending on the version of the logfile (and it's content), it will either use the member_id
    field to generate the mapping (newer version), or calculate an ID form the MAC address (this
    was the default behavior of the older version of the hubs and badges)
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    fill_gaps : boolean
        If True, the code will ensure that a value exists for every time by by filling the gaps
        with the last seen value

    Returns
    -------
    pd.Series :
        A mapping from badge id to member, indexed by datetime and id.
    """
    
    def readfile(fileobject):
        no_id_warning = False
        for line in fileobject:
            data = json.loads(line)['data']
            member_id = None
            if 'member_id' in data:
                member_id = data['member_id']
            else:
                member_id = mac_address_to_id(data['badge_address'])
                if not no_id_warning:
                    print("Warning - no id provided in data. Calculating id from MAC address")
                no_id_warning = True

            yield (data['timestamp'],
                   member_id,
                   str(data['member']))
    
    df = pd.DataFrame(readfile(fileobject), columns=['timestamp', 'id', 'member'])
    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
            .dt.tz_convert(tz)
    del df['timestamp']

    # Group by id and resample
    df = df.groupby([
        pd.Grouper(freq = time_bins_size, key='datetime'),
        'id'
    ]).first()

    # Extract series
    s = df.sort_index()['member']

    # Fill in gaps, if requested to do so
    if fill_gaps:
        s = _id_to_member_mapping_fill_gaps(s, time_bins_size=time_bins_size)

    return s



# from .metadata import voltages
def voltages(fileobject, time_bins_size='1min', tz='US/Eastern', skip_errors=False):
    """Creates a DataFrame of voltages, for each member and time bin.
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    skip_errors : boolean
        If set to True, skip errors in the data file

    Returns
    -------
    pd.Series :
        Voltages, indexed by datetime and member.
    """
    
    def readfile(fileobject, skip_errors):
        i = 0
        for line in fileobject:
            i = i + 1
            try:
                data = json.loads(line)['data']

                yield (data['timestamp'],
                       str(data['member']),
                       float(data['voltage']))
            except:
                print("Error in line#:", i, line)
                if skip_errors:
                    continue
                else:
                    raise

    df = pd.DataFrame(readfile(fileobject, skip_errors), columns=['timestamp', 'member', 'voltage'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
                       .dt.tz_convert(tz)
    del df['timestamp']

    # Group by id and resample
    df = df.groupby([
        pd.TimeGrouper(time_bins_size, key='datetime'),
        'member'
    ]).mean()
    
    df.sort_index(inplace=True)
    
    return df['voltage']


# from .metadata import sample_counts
def sample_counts(fileobject, tz='US/Eastern', keep_type=False, skip_errors=False):
    """Creates a DataFrame of sample counts, for each member and raw record

    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity or audio data, as an iterable of JSON strings.

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    keep_type : boolean
        If set to True, the type of the record will be returned as well

    skip_errors : boolean
        If set to True, skip errors in the data file

    Returns
    -------
    pd.Series :
        Counts, indexed by datetime, type and member.
    """

    def readfile(fileobject, skip_errors=False):
        i = 0
        for line in fileobject:
            i = i + 1
            try:
                raw_data = json.loads(line)
                data = raw_data['data']
                type = raw_data['type']

                if type == 'proximity received':
                    cnt = len(data['rssi_distances'])
                elif type == 'audio received':
                    cnt = len(data['samples'])
                else:
                    cnt = -1

                yield (data['timestamp'],
                       str(type),
                       str(data['member']),
                       int(cnt))
            except:
                print("Error in line#:", i, line)
                if skip_errors:
                    continue
                else:
                    raise

    df = pd.DataFrame(readfile(fileobject, skip_errors), columns=['timestamp' ,'type', 'member',
                                                     'cnt'])

    # Convert the timestamp to a datetime, localized in UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
        .dt.tz_convert(tz)
    del df['timestamp']

    if keep_type:
        df.set_index(['datetime','type','member'],inplace=True)
    else:
        del df['type']
        df.set_index(['datetime', 'member'], inplace=True)
    df.sort_index(inplace=True)

    return df


def _id_to_member_mapping_fill_gaps(idmap, time_bins_size='1min'):
    """ Fill gaps in a idmap
    Parameters
    ----------
    idmap : id mapping object

    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.

    Returns
    -------
    pd.DataFrame :
        idmap, after filling gaps.
    """
    df = idmap.to_frame().reset_index()
    df.set_index('datetime', inplace=True)
    #df.index = pd.to_datetime(df.index,unit='s')
    s = df.groupby(['id'])['member'].resample(time_bins_size).fillna(method='ffill')
    s = s.reorder_levels((1,0)).sort_index()
    return s


# from .proximity import member_to_badge_proximity
def member_to_badge_proximity(fileobject, time_bins_size='1min', tz='US/Eastern'):
    """Creates a member-to-badge proximity DataFrame from a proximity data file.
    
    Parameters
    ----------
    fileobject : file or iterable list of str
        The proximity data, as an iterable of JSON strings.
    
    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.
    
    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.
    
    Returns
    -------
    pd.DataFrame :
        The member-to-badge proximity data.
    """
    
    def readfile(fileobject):
        for line in fileobject:
            data = json.loads(line)['data']

            for (observed_id, distance) in data['rssi_distances'].items():
                yield (
                    data['timestamp'],
                    str(data['member']),
                    int(observed_id),
                    float(distance['rssi']),
                    float(distance['count']),
                )

    df = pd.DataFrame(
            readfile(fileobject),
            columns=('timestamp', 'member', 'observed_id', 'rssi', 'count')
    )

    # Convert timestamp to datetime for convenience, and localize to UTC
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
        .dt.tz_convert(tz)
    del df['timestamp']

    # Group per time bins, member and observed_id,
    # and take the first value, arbitrarily
    df = df.groupby([
        pd.Grouper(freq = time_bins_size, key='datetime'),
        'member',
        'observed_id'
    ]).first()

    # Sort the data
    df.sort_index(inplace=True)

    return df

# from .proximity import member_to_member_proximity
def member_to_member_proximity(m2badge, id2m):
    """Creates a member-to-member proximity DataFrame from member-to-badge proximity data.

    Parameters
    ----------
    m2badge : pd.DataFrame
        The member-to-badge proximity data, as returned by `member_to_badge_proximity`.

    id2m : pd.Series
        The badge IDs used by each member, indexed by datetime and badge id, as returned by
        `id_to_member_mapping`.

    Returns
    -------
    pd.DataFrame :
        The member-to-member proximity data.
    """

    df = m2badge.copy().reset_index()
    
    # Join the member names using their badge ids
    # If id2m index is a MultiIndex, assume it is a time series and use legacy method
    if type(id2m.index) == pd.MultiIndex:
        df = df.join(id2m, on=['datetime', 'observed_id'], lsuffix='1', rsuffix='2')
    # Otherwise, assume it is not time-based, and join without datetime
    else:
        df = df.join(id2m, on=['observed_id'], lsuffix='1', rsuffix='2')
    
    # Filter out the beacons (i.e. those ids that did not have a mapping)
    df.dropna(axis=0, subset=['member2'], inplace=True)

    # Reset the members type to their original type
    # This is done because pandas likes to convert ints to floats when there are
    # missing values
    df['member2'] = df['member2'].astype(id2m.dtype)
        
    # Set the index and sort it
    df.set_index(['datetime', 'member1', 'member2'], inplace=True)
    df.sort_index(inplace=True)

    # Remove duplicate indexes, keeping the first (arbitrarily)
    df = df[~df.index.duplicated(keep='first')]

    # If the dataframe is empty after the join, we can (and should) stop
    # here
    if len(df) == 0:
        print(df)
        return df

    # Reorder the index such that 'member1' is always lexicographically smaller than 'member2'
    df.index = df.index.map(lambda ix: (ix[0], min(ix[1], ix[2]), max(ix[1], ix[2])))
    df.index.names = ['datetime', 'member1', 'member2']

    # For cases where we had proximity data coming from both sides,
    # we calculate two types of rssi:
    # * weighted_mean - take the average RSSI weighted by the counts, and the sum of the counts
    # * max - take the max value
    df['rssi_weighted'] = df['count'] * df['rssi']
    agg_f = collections.OrderedDict([('rssi', ['max']), ('rssi_weighted', ['sum']), ('count', ['sum'])])

    df = df.groupby(level=df.index.names).agg(agg_f)
    df['rssi_weighted'] /= df['count']

    # rename columns
    df.columns = ['rssi_max', 'rssi_weighted_mean', 'count_sum']
    df['rssi'] = df['rssi_weighted_mean']  # for backward compatibility

    # Select only the fields 'rssi' and 'count'
    print(df)
    return df[['rssi', 'rssi_max', 'rssi_weighted_mean', 'count_sum']]

# from .proximity import member_to_beacon_proximity
def member_to_beacon_proximity(m2badge, id2b):
    """Creates a member-to-beacon proximity DataFrame from member-to-badge proximity data.
    
    Parameters
    ----------
    m2badge : pd.DataFrame
        The member-to-badge proximity data, as returned by `member_to_badge_proximity`.
    
    id2b : pd.Series
        A mapping from badge ID to beacon name.  Index must be ID, and series name must be 'beacon'.
    
    Returns
    -------
    pd.DataFrame :
        The member-to-beacon proximity data.
    """
    
    df = m2badge.copy().reset_index()

    # Join the beacon names using their badge ids
    df = df.join(id2b, on='observed_id') 

    # Filter out the members (i.e. those ids that did not have a mapping)
    df.dropna(axis=0, subset=['beacon'], inplace=True)

    # Reset the beacons type to their original type
    # This is done because pandas likes to convert ints to floats when there are
    # missing values
    df['beacon'] = df['beacon'].astype(id2b.dtype)

    # Set the index and sort it
    df.set_index(['datetime', 'member', 'beacon'], inplace=True)
    df.sort_index(inplace=True)

    # Remove duplicate indexes, keeping the first (arbitrarily)
    df = df[~df.index.duplicated(keep='first')]

    return df[['rssi']]

# from .proximity import member_to_beacon_proximity_smooth
def member_to_beacon_proximity_smooth(m2b, window_size = '5min',
                                      min_samples = 1):
    """ Smooths the given object using 1-D median filter
    Parameters
    ----------
    m2b : Member to beacon object

    window_size : str
        The size of the window used for smoothing.  Defaults to '5min'.

    min_samples : int
        Minimum number of samples required for smoothing

    Returns
    -------
    pd.DataFrame :
        The member-to-beacon proximity data, after smoothing.
    """
    df = m2b.copy().reset_index()
    df = df.sort_values(by=['member', 'beacon', 'datetime'])
    df.set_index('datetime', inplace=True)

    df2 = df.groupby(['member', 'beacon'])[['rssi']] \
        .rolling(window=window_size, min_periods=min_samples) \
        .median()

    # For std, we put-1 when std was NaN. This handles the case
    # when there was only one record. If there were no records (
    # median was not calculated because of min_samples), the record
    # will be dropped because of the NaN in 'rssi'
    df2['rssi_std']\
        = df.groupby(['member', 'beacon'])[['rssi']] \
        .rolling(window=window_size, min_periods=min_samples) \
        .std().fillna(-1)

    # number of records used for calculating the median
    df2['rssi_smooth_window_count']\
        = df.groupby(['member', 'beacon'])[['rssi']] \
        .rolling(window=window_size, min_periods=min_samples) \
        .count()

    df2 = df2.reorder_levels(['datetime', 'member', 'beacon'], axis=0)\
        .dropna().sort_index()
    return df2

# from .proximity import member_to_beacon_proximity_fill_gaps
def member_to_beacon_proximity_fill_gaps(m2b, time_bins_size='1min',
                                        max_gap_size = 2):
    """ Fill gaps in a given member to beacon object
    Parameters
    ----------
    m2b : Member to beacon object

    time_bins_size : str
        The size of the time bins used for resampling.  Defaults to '1min'.

    max_gap_size : int
         this is the maximum number of consecutive NaN values to forward/backward fill

    Returns
    -------
    pd.DataFrame :
        The member-to-beacon proximity data, after filling gaps.
    """

    df = m2b.copy().reset_index()
    df = df.sort_values(by=['member', 'beacon', 'datetime'])
    df.set_index('datetime', inplace=True)

    df = df.groupby(['member', 'beacon']) \
        [['rssi', 'rssi_std','rssi_smooth_window_count']] \
        .resample(time_bins_size) \
        .fillna(method='ffill', limit=max_gap_size)

    df = df.reorder_levels(['datetime', 'member', 'beacon'], axis=0)\
        .dropna().sort_index()
    return df

# from .hublog import hublog_scans
def hublog_scans(fileobject, log_tz, tz='US/Eastern'):
    """Creates a DataFrame of hub scans.

    Parameters
    ----------
    fileobject : file or iterable list of str
        The raw log file from a hub.

    log_tz : str
        The time zone used in the logfile itself

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    Returns
    -------
    pd.Series :
        A scan record with mac, rssi, and device status (if available)
    """

    def readfile(fileobject):
        for line in fileobject:
            line_num = line_num + 1
            data = _hublog_read_scan_line(line)
            if data:
                yield (data['datetime'],
                       str(data['mac']),
                       float(data['rssi']),
                       data['voltage'],
                       data['badge_id'],
                       data['project_id'],
                       data['sync_status'],
                       data['audio_status'],
                       data['proximity_status'],
                       )
            else:
                continue  # skip unneeded lines

    df = pd.DataFrame(readfile(fileobject), columns=['datetime', 'mac', 'rssi', 'voltage', 'badge_id', \
                                                     'project_id', 'sync_status', 'audio_status', \
                                                     'proximity_status'])

    # Localized record date
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True) \
        .dt.tz_localize(log_tz).dt.tz_convert(tz)

    # Sort
    df = df.set_index('datetime')
    df.sort_index(inplace=True)
    return df


# from .hublog import hublog_resets
def hublog_resets(fileobject, log_tz, tz='US/Eastern'):
    """Creates a DataFrame of reset events - when badge were previously not synced and
        the hub sent a new date

    Parameters
    ----------
    fileobject : file or iterable list of str
        The raw log file from a hub.

    log_tz : str
        The time zone used in the logfile itself

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    Returns
    -------
    pd.Series :
        A record with mac and timestamp
    """
    def readfile(fileobject):
        for line in fileobject:
            data = _hublog_read_reset_line(line)
            if data:
                yield (data['datetime'],
                       str(data['mac']),
                       )
            else:
                continue  # skip unneeded lines

    df = pd.DataFrame(readfile(fileobject), columns=['datetime', 'mac'])

    # Localized record date
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True) \
        .dt.tz_localize(log_tz).dt.tz_convert(tz)

    # Sort
    df = df.set_index('datetime')
    df.sort_index(inplace=True)
    return df


# from .hublog import hublog_clock_syncs
def hublog_clock_syncs(fileobject, log_tz, tz='US/Eastern'):
    """Creates a DataFrame of sync events - when badge were previously not synced and
        the hub sent a new date

    Parameters
    ----------
    fileobject : file or iterable list of str
        The raw log file from a hub.

    log_tz : str
        The time zone used in the logfile itself

    tz : str
        The time zone used for localization of dates.  Defaults to 'US/Eastern'.

    Returns
    -------
    pd.Series :
        A record with mac and timestamps
    """

    def readfile(fileobject):
        for line in fileobject:
            data = _hublog_read_clock_sync_line(line)
            if data:
                yield (data['datetime'],
                       str(data['mac']),
                       str(data['badge_timestamp']),
                       )
            else:
                continue  # skip unneeded lines

    df = pd.DataFrame(readfile(fileobject), columns=['datetime', 'mac', 'badge_timestamp'])

    # Localized record date
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True) \
        .dt.tz_localize(log_tz).dt.tz_convert(tz)

    # Convert the badge timestamp to a datetime, localized in UTC
    df['badge_datetime'] = pd.to_datetime(df['badge_timestamp'], unit='s', utc=True) \
        .dt.tz_localize('UTC').dt.tz_convert(tz)
    del df['badge_timestamp']

    # Sort
    df = df.set_index('datetime')
    df.sort_index(inplace=True)
    return df

    