"""
Copyright [2021] [David Baumgartner]

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import os
import random
from typing import Generator
from pathlib import Path, PosixPath
from operator import itemgetter
from itertools import groupby
from functools import reduce


def wc_mapper(key: str, values: str) -> Generator:
    # in MapReduce with Hadoop Streaming the input comes from standard input STDIN
    # remove leading and trailing whitespaces
    line = values.strip()
    # split the line into words
    words = line.split()

    for word in words:
        # write the results to standard output STDOUT
        yield word, 1


def wc_reducer(key: str, values: Generator) -> Generator:
    current_count = 0
    word = key

    for value in values:
        current_count += value

    yield word, current_count


def read_content_of_file(file: PosixPath) -> Generator:
    """
    Read given file content as generator - line per line
    :param file: path to the input file
    :return: generator with each line of the file
    """
    with open(file.as_posix(), 'r', encoding='utf-8-sig') as f:
        for line in f.readlines():
            yield line


def write_content_to_file(file: PosixPath, content: list) -> None:
    """
    Writes given lines in list to the given file path
    :param file: path to the output file
    :param content: lines in a list to write
    :return: None
    """
    with open(file.as_posix(), 'w', encoding='utf-8-sig') as f:
        for kv in content:
            f.write(f'{kv[0]}\t{kv[1]}\n')


def file_generator(files: list) -> Generator:
    """
    Directory file reader generator reads each file in a directory
    and reads each line of each file
    :param files: list of file paths to be read
    :return: generator of files with generator for each line
    """
    for file in files:
        yield file.as_posix(), read_content_of_file(file)


def run_map(generator, mapper) -> list:
    """
    Map function caller for each line in each file
    :param generator: file and line generator
    :param mapper: map function to be called for each line in the files
    :return: generator of key value pairs returned by the map function
    """
    return reduce(lambda x, y: x + y,
                  map(lambda kv_file_lines:
                      reduce(lambda x, y: x + y,
                             map(lambda line:
                                 list(mapper(kv_file_lines[0], line)),
                                 kv_file_lines[1])),
                      generator))


def shuffle_values(values: Generator) -> Generator:
    """
    Shuffle function to emulate the group and shuffle behaviour of the Hadoop MapReduce Framework
    :param values: generator with key value pairs
    :return: generator with shuffled values from the input generator
    """
    value_list = list(map(lambda vs: vs[1], values))
    random.shuffle(value_list)
    for v in value_list:
        yield v


def run_reducer(generator, reducer) -> list:
    """
    Reduce function caller for each group in the generator
    the Reduce function will be called ones
    :param generator: group generator with value generator to each key
    :param reducer: reducer function to call for each key/group and its values
    :return: generator of key value pairs with the reduced values
    """
    return reduce(lambda x, y: x + y,
                  map(lambda kvs: list(reducer(kvs[0], shuffle_values(kvs[1]))), generator))


def MapReduceSlim(source_path: str, target_path: str, mapper, reducer) -> None:
    """
    MapReduceSlim Framework emulates the core of the Hadoop MapReduce Framework
    without scattering the computation to multiple nodes
    :param source_path: input path to a file or directory to read from
    :param target_path: output path to a file to write the result to
    :param mapper: function argument to use as map function in the MapReduce process
    :param reducer: function argument to use as reduce function in the MapReduce process
    :return:
    """
    data_source_path = Path(source_path)
    if not data_source_path.exists():
        raise ValueError('Given path does not exist')
    target_source_path = Path(target_path)
    if target_source_path.exists():
        target_source_path.unlink()

    print("0/5: Starting Prepare Input Stage")
    map_generator = None
    if data_source_path.is_file():
        map_generator = file_generator([data_source_path])
    if data_source_path.is_dir():
        map_generator = file_generator(data_source_path.glob('**/*'))
    if map_generator is None:
        raise ValueError('File or Files not readable')
    print("1/5: Finished Prepare Input Stage")
    print("1/5: Starting Map Stage")
    kv_pairs = run_map(map_generator, mapper)
    print("2/5: Finished Map Stage")
    # sort/shuffle
    print("2/5: Starting Sort and Shuffle Stage")
    kv_pairs.sort(key=itemgetter(0))
    # group by key
    kvs_pairs = groupby(kv_pairs, key=itemgetter(0))
    print("3/5: Finished Sort and Shuffle Stage")
    print("3/5: Starting Reduce Stage")
    mr_result = run_reducer(kvs_pairs, reducer)
    print("4/5: Finished Reduce Stage")
    print("4/5: Starting Write Result Stage")
    write_content_to_file(target_source_path, mr_result)
    print("5/5: Finished Write Result Stage")


if __name__ == '__main__':
    MapReduceSlim('content/davinci.txt', '4_0_a_result.txt', wc_mapper, wc_reducer)
