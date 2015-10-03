__author__ = 'wanyanxie'
import collections
import sys

with open("data_geo.txt", "r") as fin:
    with open("cleaned_data_geo.txt", "w") as fout:
        for line in fin:
            line = line.split(',')
            fout.write(line[0] + ',' + line[1] +  ',' + \
                       line[2] + ',' + line[3])
            fout.write('\n')

# stations = mrt.get_map() # station data including longitude and latitude
#
# data = [ x.strip().split(',') for x in file('data_geo.txt') ]
#
# print 'var pnts = ['
# for line in data:
#     try:
#         st = stations[st_key]
#         print "  [%s, %f,'%s','%s','%s',%s ]," % (st[0], st[1], st_name, st[2], lang, cnt)
#     except:
#         sys.stderr.write('Failed to process key "%s"\n' % st_key)
#
# print '];'