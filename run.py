import sys
from hadoop import Hadoop
import config
import os

cache={}

def main():
    #engine = Hadoop('bin/hadoop','/usr/local/hadoop-2.7.0/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar')

    engine = Hadoop(config.HADOOP_PATH, config.HADOOP_STREAMING_PATH)

    # Put files into Hadoop
    file_path = 'h1b_kaggle_1.csv'
    file_name = os.path.basename(file_path)

    engine.put_file(local_src=file_path, hadoop_dest=file_name, override=False)

    # Map-Reduce Tasks: default output_dir is 'output'

    result = engine.map_reduce(data_src=file_path, mapper='group_by_mapper.py', mapper_arguments=[3,6],
                    reducer='value_summation_reducer.py')

    print('output is',result)

    """
    mapper arguments in case of group_by_mapper in bellow example is 
    groupby = 5th column
    aggretate = 6th column
    """

    result = engine.map_reduce(data_src=file_path, mapper='group_by_mapper.py', mapper_arguments=[5,6],
                    reducer='value_summation_reducer.py')

    
    print('output is',result)
    
    cache[(3,6)]= result

    with open('sample_output.txt','w') as file:
        file.write(str(cache))

    
if __name__ == "__main__":
    import profile
    profile.run("main()")
