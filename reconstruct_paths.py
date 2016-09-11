#!/usr/bin/python

import sys, os, string, re, time, random, glob, json
from collections import defaultdict

MAIN_INPUT_DIR = sys.argv[1] 
MAIN_OUTPUT_DIR = sys.argv[2] 
CLUEWEB_DATASET_PATH = sys.argv[3]
FB_NAMES = sys.argv[4]
### pass these arguments to script for testing
# /home/anupama/akbc-paths-dataset/run_scripts/test
# /home/anupama/akbc-paths-dataset/run_scripts/test/test_output
# /home/anupama/akbc-paths-dataset/run_scripts/test/test_clueweb 
###

def make_output_dir(INPUT_DIR, MAIN_OUTPUT_DIR):
	''' Makes output directory for each sub-folder in AKBC dataset directory '''
	folder = str(os.path.split(os.path.basename(INPUT_DIR))[1])

	try:
		if not os.path.exists(MAIN_OUTPUT_DIR+"/"+folder):
			os.makedirs(MAIN_OUTPUT_DIR+"/"+folder)
	except OSError:
		if e.errno != 17:
			raise
		pass

	OUTPUT_DIR = MAIN_OUTPUT_DIR+"/"+folder
	return OUTPUT_DIR

def create_map(CLUEWEB_DATASET):
	''' Takes clueweb dataset (5061 files) and builds a list of 5061 entitypair dicts '''
	cw_data_maps = []
	with open(MAIN_OUTPUT_DIR+"/log.txt", 'a') as log:
		log.write("Reading clueweb files...\n")
	print"Reading clueweb files...\n"
	file_count = 1
	for INPUT_CW_FILE in glob.iglob(CLUEWEB_DATASET+"/*.txt"): 
		entity_pair_map = defaultdict(list)
		
		# check sentence length (must be <500 words)
		with open(INPUT_CW_FILE, 'r') as infile:
			with open(MAIN_OUTPUT_DIR+"/log.txt", 'a') as log:
				log.write("Reading file #"+str(file_count))
			print"Reading file #"+str(file_count)
			for line in infile:
				if line.strip():
					line = line.split("\t")

					ent1ID = line[2]
					ent2ID = line[3]
					sent = line[8]
					if len(sent.split(" ")) < 500:
						entity_pair_map[(ent1ID, ent2ID)].append(sent)
		file_count += 1
		cw_data_maps.append(entity_pair_map)
	return cw_data_maps

def replace(l, X, Y):
	''' Replaces element X with element Y, in list l ''' 
	for i,v in enumerate(l):
		if v == X:
			l.pop(i)
			l.insert(i, Y)
	return l

def reconstruct_relation(prev_ent, cw_relation, next_ent, cw_data_maps_list):
	''' Completes clueweb relation i.e. replaces words with full relation'''
	full_cw_relation = None
	if cw_relation.startswith('_'):
		key = (next_ent, prev_ent)
	else:
		key = (prev_ent, next_ent)
	sample_count = 0
	randomized_maps = random.sample(cw_data_maps_list, len(cw_data_maps_list))
	while(full_cw_relation is None):
		for cw_map in randomized_maps:
			sample_count += 1
			if key in cw_map:
				cw_relations = cw_map[key]
				if cw_relations:
					full_cw_relation = random.sample(cw_relations, 1)[0]
			if(full_cw_relation is not None):
				return full_cw_relation.strip()
			if sample_count > len(cw_data_maps_list):
				return None

def reformat_data(ent1ID, ent2ID, path_list, label, fb_name_map):
	''' Reformat each line in AKBC data file to format that can be saved as JSON '''
	''' Format: { entity1: '/m/xxxx', entity2: '/m/xxxx', paths: [[]] }'''
	reconstructed_dict = {}
	ent1ID = get_fb_name(ent1ID, fb_name_map)
	ent2ID = get_fb_name(ent2ID, fb_name_map)
	reconstructed_dict['entity1'] = ent1ID
	reconstructed_dict['entity2'] = ent2ID
	reconstructed_dict['paths'] = reformat_paths(path_list, fb_name_map)
	reconstructed_dict['label'] = label
	return reconstructed_dict

def reformat_paths(path_list, fb_name_map):
	''' Reformat paths in AKBC data file '''
	''' Format: list of lists of dicts [ [ {}* ]* ] '''
	reformatted_final_list = []
	for path in path_list:
		
		list_for_path = []
		path_tokens = path.split("-")
		for index, token in enumerate(path_tokens):
			if index % 2 == 0:
				if index+1 == len(path_tokens):
					r_dict = {}
					r_dict['relation'] = token
					r_dict['next_entity'] = None
					list_for_path.append(r_dict)
				else:
					r_dict = {}
					r_dict['relation'] = token
					r_dict['next_entity'] = get_fb_name(path_tokens[index+1], fb_name_map)

					list_for_path.append(r_dict)
		reformatted_final_list.append(list_for_path)
	return reformatted_final_list

def assign_label(filename, tokens):
	if filename.startswith('positive'):
		label = '1'
	elif filename.startswith('negative'):
		label = '-1'
	else:
		label = tokens[3]
	return label.strip()

def get_fb_name(entityID, fb_name_map):
	if entityID in fb_name_map:
		return fb_name_map[entityID]
	else:
		return entityID


with open(MAIN_OUTPUT_DIR+"/log.txt", 'w') as log:
	log.write("\nCreating maps...\n")

print"\nCreating maps...\n"
cw_data_maps_list = create_map(CLUEWEB_DATASET_PATH)

with open(MAIN_OUTPUT_DIR+"/log.txt", 'a') as log:
	log.write("\nMapping Freebase names & IDs...\n")

print"\nMapping Freebase names & IDs...\n"
fb_name_map = {}
with open(FB_NAMES, 'r') as fb_name_dump:
	for line in fb_name_dump:
		line = line.split('\t')
		entity = line[0]
		fb_name = line[1].strip()
		fb_name_map[entity] = fb_name



akbc_input_folders = [x[0] for x in os.walk(MAIN_INPUT_DIR)]
for INPUT_DIR in akbc_input_folders:
	INPUT_FILES = glob.glob(INPUT_DIR+'/*.translated')
	with open(INPUT_DIR+"/log.txt", 'w') as log:
		log.write("\nCreating output directory...\n")
	print"\nCreating output directory...\n"
	OUTPUT_DIR = make_output_dir(INPUT_DIR, MAIN_OUTPUT_DIR)

	train_file = open(OUTPUT_DIR+'/train_matrix.tsv.translated', 'w+')


	for INPUT_FILE in INPUT_FILES:
		with open(INPUT_FILE, 'r') as infile:
			input_filename = os.path.basename(INPUT_FILE)

			if input_filename.startswith('positive'):
				OUTFILE = OUTPUT_DIR+"/train_matrix.tsv.translated"
				mode = 'a'
			elif input_filename.startswith('negative'):
				OUTFILE = OUTPUT_DIR+"/train_matrix.tsv.translated"
				mode = 'a'
			else:
				OUTFILE = OUTPUT_DIR+'/'+input_filename
				mode = 'w'

			with open(OUTFILE, mode) as outfile:
				data_for_json = []
				
				with open(INPUT_DIR+"/log.txt", 'a') as log:
					log.write("\nReconstructing relations...\n")

				print"\nReconstructing relations...\n"
				for line in infile:
					tokens = line.split("\t")
					ent1ID, ent2ID = tokens[0], tokens[1]
					

					paths = tokens[2]
					label = assign_label(input_filename, tokens)

					reconstructed_paths = []
					path_list = paths.split("###")
					for path in path_list:
						path_tokens = path.split("-")
						for index, token in enumerate(path_tokens):
							if index % 2 == 0:
								if not re.match(r'^_?/', token):
									cw_relation = token
									if index == 0: 
										prev_ent = ent1ID
									else:
										prev_ent = path_tokens[index-1]
									if index == len(path_tokens) - 1:
										next_ent = ent2ID
									else:
										next_ent = path_tokens[index+1]	
									
									full_cw_relation = reconstruct_relation(prev_ent, cw_relation, next_ent, cw_data_maps_list)
									if full_cw_relation:
										path_tokens = replace(path_tokens, cw_relation, full_cw_relation)				
						path = replace(path_list, path, path_tokens)
					for l in path_list:
						reconstructed_paths.append('-'.join(l))

					reconstructed_dict = reformat_data(ent1ID, ent2ID, reconstructed_paths, label, fb_name_map)
					
					data_for_json.append(reconstructed_dict)
					json.dump(data_for_json, outfile)
					outfile.write("\n")
					with open(INPUT_DIR+"/log.txt", 'a') as log:
						log.write("\nDONE!!!...\n")
					#old format (not JSON):
					#outfile.write(ent1ID+'\t'+ent2ID+'\t'+str('###'.join(str(x) for x in reconstructed_paths))+'\t'+label+'\n')

				

















