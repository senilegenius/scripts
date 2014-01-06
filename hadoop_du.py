#!/usr/bin/env python

import sys, subprocess, os, re
from operator import itemgetter, attrgetter
from tabulate import tabulate


def sizeof_fmt(num):
	for x in ['bytes','KB','MB','GB']:
		if num < 1024.0:
			return "%3.1f%s" % (num, x)
		num /= 1024.0
	return "%3.1f%s" % (num, 'TB')


def trunc_at(s, d, n=3):
    "Returns s truncated at the n'th (3rd by default) occurrence of the delimiter, d."
    return d.join(s.split(d)[n:])


def get_data(dir):
	"Get data from HDFS and make hash table out of data; return the hash table"
	raw_output = []
	dirs_size_dict={}

	p = subprocess.Popen(["hadoop", "fs", "-du", dir], bufsize=1048576, stdout=subprocess.PIPE)
	raw_output, err = p.communicate()
	
	for line in raw_output.rstrip().split("\n"):
		if not re.match("^Found\s\d*\sitems$", line):
			size_in_bytes, dir_fullname = line.split()
			dirs_size_dict[dir_fullname] = int(size_in_bytes)
	return(dirs_size_dict)


def manip_data(dirs_dict):
	
	new_tuple_list = []

	# turn dictionary into list
	dict_to_list = [x for x in dirs_dict.iteritems()]

	# add human readable size to each tuple
	for j,k in dict_to_list:
		new_tuple_list.append((j,k,sizeof_fmt(k)))

	# sort that list
	sorted_by_size = sorted(new_tuple_list, key=itemgetter(1), reverse=True)

	# create a table from the list
	table = tabulate(sorted_by_size)

	# short_dir = "/" + trunc_at(out.split()[0], "/")
	# bytes = int(out.split()[1])
	# hum = sizeof_fmt(bytes)

	return (table)

raw_output = get_data(sys.argv[1])
data_tabled = manip_data(raw_output)

print data_tabled

