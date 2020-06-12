import Dynamic_Network_Graph_Exploration_py3 as dynamic
import heatmap_functions as heatmap
import Member_Distribution as dist


'''
So far the main method only includes dynamic network graph functions

Heatmap_functions and Member_Distribution functions to be added 
'''

def main():
    dynamic.NetworkGraphBasicExample('2019-06-01 10:00','2019-06-01 11:20')
    dynamic.LunchTimeAnalysis()
    dynamic.BreakoutSessionAnalysis()
    dynamic.InteractionNetworkGraph(time_interval_start_h=9,time_interval_start_m=50,
                            time_interval_end_h=11,time_interval_end_m=20,
                            interval=2,t_count_threshold = 2)

if __name__ == "__main__":
    main()