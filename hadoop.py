import subprocess

"""
bin/hadoop jar /usr/local/hadoop-2.7.0/share/hadoop/tools/lib/hadoop-streaming-2.7.0.jar \
 -mapper 'group_by_mapper.py 7 6' -file group_by_mapper.py \
 -reducer value_summation_reducer.py -file value_summation_reducer.py \
 -input test.csv -output output_dir1 \
"""

class Hadoop:

    def __init__(self,hadoop_path = '', streaming_path = 'contrib/streaming/hadoop-*streaming*.jar'):
        self.hadoop_path = hadoop_path
        self.streaming_path = streaming_path
        self.output_no = {}

    def shell(self, command):
        """
        :param command: pass in a command without hadoop path. hadoop path will be added automatically
        :return:
        """
        command = '{hadoop_path} {command}'.format(hadoop_path=self.hadoop_path, command = command)
        print('executing', command)
        process = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        (output, err) = process.communicate()

        process.wait()

        if output is None:
            return ''

        return output.decode("utf-8")

    def file_exists(self, name):
        """
        :param name: name for any type of file which could be directory file or regular file
        :return: True if file exists
        """
        command = "fs -ls | grep '{name}'".format(name=name)

        output = self.shell(command)

        print('Searching for',name, command, len(output))
        print(output)

        return True if len(output) > 0 else False

    def find_unique_output_directory(self, directory_prefix):

        self.output_no.setdefault(directory_prefix, 0)
        count = self.output_no[directory_prefix]

        name = directory_prefix + str(count)

        if not self.file_exists(name):
            self.output_no[directory_prefix] = count + 1
            return name

        print('Finding unique directory', directory_prefix)

        command = "fs -ls | grep '{name}' | awk '{{print $8}}'".format(name=directory_prefix)
        output = self.shell(command)

        name_len = len(directory_prefix)

        numbers = [-1]
        for i in output.split('\n'):
            try:
                numbers.append(int(i[name_len:]))
            except:
                # some alpha_numeric_value
                pass

        count = max(numbers) + 1
        self.output_no[directory_prefix] = count

        return  directory_prefix + str(count)

    def remove_file(self, hadoop_dest):
        command = 'fs -rm {hadoop_dest}'.format(hadoop_dest=hadoop_dest)
        print('removing file', hadoop_dest)
        self.shell(command)
        #exit()

    def put_file(self, local_src, hadoop_dest, override=False):
        """
        :param local_src: name of the local source file with complete path
        :param hadoop_dest: location at which to store file includes file name also
        :param override: should we override if file already exists
        :return:
        """

        found_file = self.file_exists(hadoop_dest)
        if not override and found_file:
            return True

        if override and found_file:
            self.remove_file(hadoop_dest)

        put_file_command = """fs -put {local_src} {hadoop_dest}""".format(
            local_src=local_src, hadoop_dest=hadoop_dest)

        print('Copying',local_src,'to hadoop: ', hadoop_dest)
        data = self.shell(put_file_command)

        return True

    def _format_map_reduce_args(self, name,args):
        if args:
            args = [str(i) for i in args]
            return "'{name} {args}'".format(name = name, args = ' '.join(args))

        return name

    def map_reduce(self,data_src, mapper, reducer, mapper_arguments=None, reducer_arguments=None, output_dir = 'output'):

        if not self.file_exists(data_src):
            raise Exception('No such file exists: '+ data_src)

        output_dir = self.find_unique_output_directory(output_dir)

        mapper_arguments = self._format_map_reduce_args(mapper, mapper_arguments)
        reducer_arguments = self._format_map_reduce_args(reducer, reducer_arguments)

        command = """jar {streaming_path} \
             -mapper {mapper_arguments}    -file {mapper}  \
             -reducer {reducer_arguments}    -file {reducer}  \
            -input {input}  -output {output}
        """.format(mapper=mapper, mapper_arguments=mapper_arguments,
                   reducer_arguments=reducer_arguments, reducer=reducer, input=data_src, output=output_dir,
                   streaming_path=self.streaming_path)

        print(command)

        self.shell(command)

        return self.output_fetch(output_dir)

    def _output_reformat(self, line):
        line = line.strip('(')
        line = line.strip(')')
        line = line.strip("\'")
        key, value = line.split("\', \'")[0], line.split("\', \'")[-1]
        return key, value

    def output_fetch(self,output_dir, output_file='part*'):
        command = """fs -cat {output_dir}/{output_file}""".format(output_dir=output_dir, output_file=output_file)

        raw_output = self.shell(command)
        raw_output = raw_output.split('\n')

        result = {}
        for key, value in [i.split('\t') for i in raw_output if len(i) > 0]:
            result[key] = value
        return result