import sys
import hadoop
import config

"""
data = file in hadoop

mapper arguments in case of group_by_mapper in bellow example is 
groupby = 5th column
aggretate = 6th column
"""

command = sys.argv[1]

if(command == 'group_by'):

    result = hadoop.execute(data=sys.argv[4],
                    mapper='group_by_mapper.py', mapper_arguments=[int(sys.argv[2]),int(sys.argv[3])],
                    reducer='value_summation_reducer.py',
                    hadoop_path=config.HADOOP_PATH, streaming_path=config.HADOOP_STREAMING_PATH,
                    )

    print('output is',result)