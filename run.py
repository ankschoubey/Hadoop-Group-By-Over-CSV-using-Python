import sys
from hadoop import Hadoop
import config
import os

"""
data = file in hadoop

mapper arguments in case of group_by_mapper in bellow example is 
groupby = 5th column
aggretate = 6th column
"""

#engine = Hadoop('bin/hadoop','/usr/local/hadoop-2.7.0/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar')

engine = Hadoop(config.HADOOP_PATH, config.HADOOP_STREAMING_PATH)

file_path = 'test.csv'
file_name = os.path.basename(file_path)

engine.put_file(local_src=file_path, hadoop_dest=file_name, override=False)

result = engine.map_reduce(data_src='test.csv', mapper='group_by_mapper.py', mapper_arguments=[3,6],
                reducer='value_summation_reducer.py')

print('output is',result)

result = engine.map_reduce(data_src='test.csv', mapper='group_by_mapper.py', mapper_arguments=[5,6],
                reducer='value_summation_reducer.py')

print('output is',result)