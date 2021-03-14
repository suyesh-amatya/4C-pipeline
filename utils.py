#!/usr/bin/env python
# coding: utf-8


import logging
import os

import yaml
from hops import hdfs

LANE_SEPAROTOR = '_L'
SPACE = ' '
EMPTY = ''
NO_CONFIG_ERR = 'No user configuration file provided'
DIAMOND_ERR = 'Diamond Installation failed'
TRIMMOMATIC_NOT_FOUND = 'Trimmomatic Jar file not found'
TRIMMOMATIC_ADAPTER_NOT_FOUND = 'Trimmomatic Adapter file not found'



# group r1 and r2
def group_R1R2(files):
    odd_i = []
    even_i = []
    # Separating odd and even index for R1(odd) and R2(even) 
    for i, file in enumerate(files):
        tail = os.path.split(file)[1]
        if i % 2:
            even_i.append(tail)
        else:
            odd_i.append(tail)

    return list(zip(odd_i, even_i))


def load_file_names(hdfs_root):
    # split file name
    return [os.path.split(x)[1] for x in hdfs.ls(hdfs_root)]


def load_arguments(argv):
    try:
        settings = argv[1]
        logging.info('Reading configuration file at ', settings)
        if hdfs.exists(settings):

            hdfs.copy_to_local(settings, overwrite=True)
            settings = os.path.split(settings)[1]
            with open(settings) as file:
                args = yaml.full_load(file)

            return args
        else:
            return

    except IndexError:

        return


def combine_all_lanes(files, nbr_of_lanes):
    l1, l2, l3, l4, combined_bam = ([],) * 5

    for i in range(0, len(files), nbr_of_lanes):
        combined_bam.append(files[i:i + nbr_of_lanes])

    return combined_bam


def build_command(tool, params):
    if params:
        str_list = [tool]

        for key, value in params.items():
            str_list.append(SPACE)
            str_list.append(key)
            str_list.append(SPACE)
            str_list.append(str(value))

        return ''.join(str_list)
    else:
        return -1


def find_number_of_lanes(files):
    duplicates = [os.path.splitext(f)[0].split(LANE_SEPAROTOR)[0] for f in files]
    return duplicates.count(duplicates[0])


def find_file_like(search):
    all_files = os.listdir()
    for i in all_files:
        if search in i:
            return i
    return None
