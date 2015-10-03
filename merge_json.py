__author__ = 'wanyanxie'
import json
import glob

read_files = glob.glob("./twitter-users/*.json")
output_list = []

# for f in read_files:
#     with open(f, "rb") as infile:
#
# with open("merged_file.json", "wb") as outfile:
#     json.dump(output_list, outfile)