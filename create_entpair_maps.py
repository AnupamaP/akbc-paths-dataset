#!/usr/bin/python
from collections import defaultdict
import sys, os.path, string, re, time, pickle, glob

INPUT_DIR = '/iesl/canvas/anupama/scratch/entpair_maps'#'/iesl/canvas/anupama/scratch/merged'#sys.argv[1]
#OUTPUT_DIR = sys.argv[2]


def create_map(INPUT_FILE): 
	entity_pair_map = defaultdict(list)
	# check sentence length (must be <500 words)
	with open(INPUT_FILE, 'r') as infile:
		for line in infile:
			if line.strip():
				line = line.split("\t")
				ent1ID = line[2]
				ent2ID = line[3]
				sent = line[8]
				if len(sent.split(" ")) < 500:
					entity_pair_map[(ent1ID, ent2ID)].append(sent)
	return entity_pair_map
	# pickle.dump( entity_pair_map, open( OUTPUT_DIR+"/"+os.path.basename(INPUT_FILE)+".p", "wb" ) )


COUNT = 1
cw_maps_list = []
for INPUT_FILE in glob.iglob(INPUT_DIR+"/*.p"): 
	print "FILE# "+str(COUNT)
	# cw_maps_list.append(create_map(INPUT_FILE))
	cw_map = pickle.load( open( INPUT_FILE, "rb" ) )
	cw_maps_list.append(cw_map)
	COUNT += 1






