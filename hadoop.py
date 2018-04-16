import os

# os.popen: returns terminal output after execution

def put_test_file(local_src, input_dir, input_file, hadoop_path='hadoop'):
    """
    :param local_src: name of the local source file with complete path
    :param input_dir: name of the input directory in hadoop
    :param input_file: name of the input file in hadoop
    """

    put_input_file_command = """{hadoop_path} fs -put {local_src} /{input_dir}/{input_file}""".format(local_src=local_src, input_dir=input_dir,input_file=input_file,hadoop_path=hadoop_path)

    data = os.popen(put_input_file_command).read()

    print('input\n',put_input_file_command,'\n',data)
    data = data.split('\n')
    result = {}
    for key, value in [i.split('\t') for i in data if len(i)>0]:
        result[key] = value
    print('output_result')
    print(result)

    return """/{input_dir}/{input_file}""".format(input_dir=input_dir, input_file=input_file)

def output_fetch(output_dir, output_file='part*', hadoop_path='hadoop'):
    """
    :param output: name of the output directory in hadoop
    :return: returns the data as dict
    """

    data_fetch_command = """{hadoop_path} fs -cat {output_dir}/{output_file}""".format(output_dir=output_dir,output_file=output_file,hadoop_path=hadoop_path)

    data = os.popen(data_fetch_command).read()

    print('output\n',data_fetch_command,'\n',data)
    data = data.split('\n')
    result = {}
    for key, value in [i.split('\t') for i in data if len(i)>0]:
        result[key] = value
    return result

def directory_exists(dir_name, hadoop_path='hadoop'):
    """
    :param dir_name: name of directory on hdfs
    :return: True if directory exists
    """
    command = "{hadoop_path} fs -ls | grep '{name}'".format(name = dir_name,hadoop_path=hadoop_path)
    output = os.popen(command).read()
    print('searching directory',command,len(output))

    return True if len(output) > 0 else False

if(os.path.exists('.env')):
    with open('.env', 'r') as f:
        count = int(f.read())
else:
    with open('.env', 'w') as f:
        f.write('0')
    count = 0

def find_unique_output_dir(desired_name='output',hadoop_path='hadoop'):
    global count
    name = desired_name

    print('finding unique ')

    while True:
        name = desired_name + str(count)
        if not directory_exists(name, hadoop_path=hadoop_path):
            with open('.env', 'w') as f:
                f.write(str(count + 1))
            return name
        count +=1

def execute(data, mapper, reducer, hadoop_path = 'bin/hadoop', streaming_path = 'contrib/streaming/hadoop-*streaming*.jar' ,mapper_arguments=None, reducer_arguments=None, output = 'output'):
    """
    :param data: location of data on hdfs
    :param mapper:  mapper file
    :param reducer: reducer file
    :param mapper_arguments: any arguments to pass to mapper
    :param reducer_arguments: any arguments to pass to reducer
    :param output:  optional desired output file
    :return:
    """

    if(os.name == 'nt'):
        splitter = '\\'
    else:
        splitter = '/'

    input_path = put_test_file(data, 'test', data.split(splitter)[-1], hadoop_path)

    if directory_exists(output,hadoop_path=hadoop_path):
        print('directory exists', output)

        output = find_unique_output_dir(output,hadoop_path=hadoop_path)

    if mapper_arguments:
        mapper_arguments = [str(i) for i in mapper_arguments]
        mapper_arguments = "'"+mapper +' '+' '.join(mapper_arguments) + "'"
    else:
        mapper_arguments = mapper

    if reducer_arguments:
        reducer_arguments = [str(i) for i in reducer_arguments]

        reducer_arguments = "'" + mapper +' '+ ' '.join(reducer_arguments) + "'"
    else:
        reducer_arguments = reducer

    command = """{hadoop_path} jar {streaming_path} \
         -mapper {mapper_arguments}    -file {mapper}  \
         -reducer {reducer_arguments}    -file {reducer}  \
        -input {input}  -output {output}
    """.format(hadoop_path=hadoop_path,mapper=mapper, mapper_arguments = mapper_arguments, reducer_arguments = reducer_arguments, reducer=reducer, input=input_path, output=output, streaming_path=streaming_path)

    print(command)

    os.popen(command)

    return output_fetch(output, hadoop_path=hadoop_path)


"""
bin/hadoop jar /usr/local/hadoop-2.7.0/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar \
 -mapper 'group_by_mapper.py 7 6' -file group_by_mapper.py \
 -reducer value_summation_reducer.py -file value_summation_reducer.py \
 -input test.csv -output output_dir1 \
"""