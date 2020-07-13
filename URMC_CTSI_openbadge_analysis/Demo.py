import Dynamic_Network_Graph_Exploration_py3 as dynamic
#import heatmap_functions as heatmap
#import Member_Distribution as dist


'''
So far the main method only includes dynamic network graph functions

Heatmap_functions and Member_Distribution functions to be added 
'''

def main():
    #dynamic.NetworkGraphBasicExample('2019-06-01 10:00','2019-06-01 11:20')
    '''
    This function is the code given in the Open Badge Analysis Library 
    '''
    
    #dynamic.LunchTimeAnalysis()
    '''
    Find interactive groups during lunch time
    All the parameters in this function are preset 
    '''
    
    #dynamic.BreakoutSessionAnalysis()
    '''
    Find interactive groups during breakout session
    All the parameters in this function are preset 
    '''
    
    dynamic.InteractionNetworkGraph(time_interval_start_h=9,time_interval_start_m=50,
                            time_interval_end_h=11,time_interval_end_m=20,
                            interval=2,t_count_threshold = 2)
    '''
    Find interactive groups during any given time with any given parameters 
    The values of the parameters are manually determined by the activities 
    engaged in the time frame given
    '''

if __name__ == "__main__":
    main()