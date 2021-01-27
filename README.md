# MapReduceSlim

MapReduce Education Script for learning and testing map reduce jobs

## Purpose

This script allows to implement map reduce jobs and to test them 
without having a full Apache Hadoop setup available. 

## MapReduce Theory

First implement a mapper and a reduce function. 

The mapper function 
has to perform a mapping defined like this: $m(k, v) > \[(k', v')\]$.
It takes a key-value pair and returns new key-value pairs, such as: 
`('Hello', 1)`. The produced key-value pair must not correlate with 
the input key-value pair in this function. 

The reducer takes again a key-value pair but the value is therefore 
a list of values that share the same key, such as: `('Hello', [1, 1, 1])`. 
This function produces another key-value pair after some transformation, 
like a sum aggregation and would result in `('Hello', 3)`.
This can be expresed as: $r(k', v'*) > (k', v'')$.

To but it into the context of Python. The mapper is called for each line
in a file ones and receives the file name as key and a line from this file.
The mapper can therefore produce a list of key-value pairs for one call 
and hast to provide those as generator or list of key-value pairs.
The reducer on the otherside is called for each unique key with all 
values that share the same key ones. The produced result can be 
one key-value pair or again a generator or list with key-value pairs.

## Quick Start

**Write a mapper function like this:**

```python
# Hint: in MapReduce with Hadoop Streaming the input comes from standard input STDIN
def wc_mapper(key: str, values: str):
    # remove leading and trailing whitespaces
    line = values.strip()
    # split the line into words
    words = line.split()
    for word in words:
        # write the results to standard output STDOUT
        yield word, 1
```

**Next write a reducer function:**

```python
def wc_reducer(key: str, values: list):
    current_count = 0
    word = key
    for value in values:
        current_count += value
    yield word, current_count
```

**Finally, call the function with the MapReduceSlim framework:**

*One input file version:*

This example reades the content from **one file** and uses its content 
as input for the run.

```python
MapReduceSlim('davinci.txt', 'davinci_wc_result_one_file.txt', wc_mapper, wc_reducer)
```

*Directory input version:*

This example reads the content from **all files in the given directory**
and uses their content as input for the run.

```python
MapReduceSlim('davinci_split', 'davinci_wc_result_multiple_file.txt', wc_mapper, wc_reducer)
```

## Requirements

**Only Python 3.7+**
