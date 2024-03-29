#!/usr/bin/python

import sys, os, logging
import string, re, glob, json
import codecs, time, datetime, random 
from collections import defaultdict

MAIN_INPUT_DIR = sys.argv[1] 
MAIN_OUTPUT_DIR = sys.argv[2] 
CLUEWEB_DATASET_PATH = sys.argv[3]
FB_NAMES = sys.argv[4]
### pass these arguments to script for testing on single relation test folder
# /home/anupama/akbc-paths-dataset/run_scripts/test/test_akbc
# /home/anupama/akbc-paths-dataset/run_scripts/test/test_output
# /home/anupama/akbc-paths-dataset/run_scripts/test/test_clueweb 
# /home/anupama/akbc-paths-dataset/run_scripts/freebase_names
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
		if file_count < 5062:
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

def create_fb_names_map(FB_NAMES):
	fb_name_map = {}
	with codecs.open(FB_NAMES, 'r', encoding='UTF-8') as fb_name_dump:
		for line in fb_name_dump:
			tokens = line.strip('\n').split('\t')
			try: 	
				entity = "/"+tokens[0]
				fb_name = tokens[1].strip()
				fb_name_map[entity] = fb_name
			except IndexError:
				with codecs.open(MAIN_OUTPUT_DIR+"/log.txt", 'a', encoding='UTF-8') as log:
					log.write("\nNot added to freebase map:"+line)
	return fb_name_map

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
	''' Reformat each line in AKBC relations file to format that can be saved as JSON '''
	''' Format: { entity1: '/m/xxxx', entity2: '/m/xxxx', paths: [[]], label:'' }'''
	reconstructed_dict = {}
	entity1_dict = {}
	entity2_dict = {}
	entity1_dict['name'] = get_fb_name(ent1ID, fb_name_map)
	entity1_dict['mID'] = ent1ID
	entity2_dict['name'] = get_fb_name(ent2ID, fb_name_map)
	entity2_dict['mID'] = ent2ID	
	reconstructed_dict['entity1'] = entity1_dict
	reconstructed_dict['entity2'] = entity2_dict
	reconstructed_dict['paths'] = reformat_paths(path_list, fb_name_map)
	reconstructed_dict['label'] = label
	reconstructed_dict['num_paths'] = len(reconstructed_dict['paths']) #path_list
	reconstructed_dict['path_lengths'] = [len(path) for path in reconstructed_dict['paths']]
	return reconstructed_dict

def reformat_paths(path_list, fb_name_map):
	''' Reformat paths in AKBC relations file '''
	''' Format: list of lists of dicts [ [ {}* ]* ] '''
	reformatted_final_list = []
	for path in path_list:
		
		list_for_path = []
		path_tokens = path.split("-")
		for index, token in enumerate(path_tokens):
			if index % 2 == 0:
				if index+1 == len(path_tokens):
					r_dict = {}
					next_entity_dict = {} 
					r_dict['relation'] = token.rstrip('\n')
					next_entity_dict['name'] = None
					next_entity_dict['mID'] = None
					r_dict['next_entity'] = next_entity_dict
					list_for_path.append(r_dict)
				else:
					r_dict = {}
					next_entity_dict = {}
					r_dict['relation'] = token.rstrip('\n')
					next_entity_dict['name'] = get_fb_name(path_tokens[index+1], fb_name_map)
					next_entity_dict['mID'] = path_tokens[index+1]
					r_dict['next_entity'] = next_entity_dict
					list_for_path.append(r_dict)
		reformatted_final_list.append(list_for_path)
	return reformatted_final_list

def assign_label(filename, tokens):
	'''Assign postive/negative label to unlabeled lines in file'''
	if filename.startswith('positive'):
		label = '1'
	elif filename.startswith('negative'):
		label = '-1'
	else:
		label = tokens[3]
	return label.strip()

def get_fb_name(entityID, fb_name_map):
	'''Get freebase name based on entityID from fb_name_map'''
	if entityID in fb_name_map:
		return fb_name_map[entityID]
	else:
		return entityID

def main():
	with codecs.open(MAIN_OUTPUT_DIR+"/log.txt", 'w', encoding='UTF-8') as log:
		log.write("\nCreating maps...\n")
	print"\nCreating maps...\n"

	cw_data_maps_list = create_map(CLUEWEB_DATASET_PATH)

	with codecs.open(MAIN_OUTPUT_DIR+"/log.txt", 'a', encoding='UTF-8') as log:
		log.write("\nMapping Freebase names & IDs...\n")
	print"\nMapping Freebase names & IDs...\n"

	fb_name_map = create_fb_names_map(FB_NAMES)
	cw_relations_replacement_log = codecs.open(MAIN_OUTPUT_DIR+"/cw_relations_replacement.log", 'w+', encoding='UTF-8')
	err_log = codecs.open(MAIN_OUTPUT_DIR+"/err.log", 'w+', encoding='UTF-8')
	akbc_input_folders = [x[0] for x in os.walk(MAIN_INPUT_DIR)][1:]
	folder_count=0
	for INPUT_DIR in akbc_input_folders:
		try:
			folder_count += 1
			INPUT_FILES = glob.glob(INPUT_DIR+'/*.translated')
			OUTPUT_DIR = make_output_dir(INPUT_DIR, MAIN_OUTPUT_DIR)

			with codecs.open(OUTPUT_DIR+"/log.txt", 'w', encoding='UTF-8') as log:
				log.write("\nCreated output directory...\n")
			print("Folder #"+str(folder_count))
			print"\nCreated output directory...\n"

			train_file = codecs.open(OUTPUT_DIR+'/train_matrix.tsv.translated', 'w+', encoding='UTF-8')
			
			for INPUT_FILE in INPUT_FILES:
				try:
					# logging clueweb relations that are not found in map AND those replaced
					with codecs.open(MAIN_OUTPUT_DIR+"/cw_relations_replacement.log", 'a', encoding='UTF-8') as failed:
						failed.write(INPUT_FILE+'\n')

					with codecs.open(MAIN_OUTPUT_DIR+"/log.txt", 'a', encoding='UTF-8') as log:
						log.write("\nWorking on "+INPUT_FILE+"...\n")

					with codecs.open(INPUT_FILE, 'r', encoding='UTF-8') as infile:
						input_filename = os.path.basename(INPUT_FILE)

						if input_filename.startswith('positive'):
							OUTFILE = OUTPUT_DIR+"/train_matrix.tsv.translated"
							OUTFILE_filtered = OUTPUT_DIR+"/train_matrix.tsv.translated.filtered"
							mode = 'a'
						elif input_filename.startswith('negative'):
							OUTFILE = OUTPUT_DIR+"/train_matrix.tsv.translated"
							OUTFILE_filtered = OUTPUT_DIR+"/train_matrix.tsv.translated.filtered"
							mode = 'a'
						else:
							OUTFILE = OUTPUT_DIR+'/'+input_filename
							OUTFILE_filtered = OUTPUT_DIR+'/'+input_filename+'.filtered'
							mode = 'w'
					
						with codecs.open(OUTPUT_DIR+"/log.txt", 'a', encoding='UTF-8') as log:
							log.write("\nReconstructing relations...\n")
						print"\nReconstructing relations...\n"
						lineIndex = 1
						num_paths_in_file = 0
						num_path_tokens_replaced = 0
						num_paths_tokens = 0
						for line in infile: 
							tokens = line.split("\t")
							ent1ID, ent2ID = tokens[0], tokens[1]
							paths = tokens[2]
							label = assign_label(input_filename, tokens)
							reconstructed_paths = []
							reconstructed_paths_copy = []
							path_list = paths.split("###")
							path_list_copy = paths.split("###")
							num_paths_in_file += len(path_list)
							pathIndex = 0

							for path in path_list:
								pathIndex = pathIndex + 1
								path_copy = path
								path_tokens = path.split("-")
								path_tokens_copy = path.split("-")
								num_paths_tokens += len(path_tokens)
								for index, token in enumerate(path_tokens):
									if index % 2 == 0:
									# if relation
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
												with codecs.open(MAIN_OUTPUT_DIR+"/cw_relations_replacement.log", 'a', encoding='UTF-8') as failed:
													try:
														failed.write(str(lineIndex)+'\t'+str(pathIndex)+'\t'+prev_ent+'\t'+get_fb_name(prev_ent, fb_name_map)+'\t'+next_ent+'\t'+get_fb_name(next_ent, fb_name_map)+'\t'+cw_relation+'\t'+full_cw_relation+'\n')
													except UnicodeDecodeError:
														failed.write('Found a bad char in file'+str(lineIndex)+'\t'+str(pathIndex)+'\n')
											else: 
												path_tokens_copy = replace(path_tokens_copy, cw_relation, '***NA***')
												with codecs.open(MAIN_OUTPUT_DIR+"/cw_relations_replacement.log", 'a', encoding='UTF-8') as failed:
													try:
														failed.write(str(lineIndex)+'\t'+str(pathIndex)+'\t'+prev_ent+'\t'+get_fb_name(prev_ent, fb_name_map)+'\t'+next_ent+'\t'+get_fb_name(next_ent, fb_name_map)+'\t'+cw_relation+'\t'+'***NA***'+'\n')
													except UnicodeDecodeError:
														failed.write('Found a bad char in file'+str(lineIndex)+'\t'+str(pathIndex)+'\n') 

										num_path_tokens_replaced += 1
									path_tokens_copy = filter(lambda element: element != '***NA***', path_tokens_copy)
								
								path = replace(path_list, path, path_tokens)
								path_copy = replace(path_list_copy, path_copy, path_tokens_copy)
							
							for l in path_list:
								try:
									reconstructed_paths.append('-'.join(l))
								except UnicodeDecodeError:
									continue
							for l in path_list_copy:
								try:
									reconstructed_paths_copy.append('-'.join(l))
								except UnicodeDecodeError:
									continue
							reconstructed_dict = reformat_data(ent1ID, ent2ID, reconstructed_paths, label, fb_name_map)
							reconstructed_dict_copy = reformat_data(ent1ID, ent2ID, reconstructed_paths_copy, label, fb_name_map)
							with codecs.open(OUTFILE, mode, encoding='UTF-8') as outfile:
								try:
									outline = json.dumps(reconstructed_dict, outfile)
									outfile.write(outline+"\n")
								except UnicodeDecodeError:
									outfile.write('Found a bad char in json'+"\n")
									
							with codecs.open(OUTFILE_filtered, mode, encoding='UTF-8') as outfile:
								try:
									outline = json.dumps(reconstructed_dict_copy, outfile)
									outfile.write(outline+"\n")
								except UnicodeDecodeError:
									outfile.write('Found a bad char in json'+"\n")				

							lineIndex = lineIndex + 1
					with codecs.open(OUTPUT_DIR+"/log.txt", 'a', encoding='UTF-8') as log:
						log.write("----->STATS<-----\n")
						log.write('File:'+INPUT_FILE+'\n')
						log.write('Number of paths:'+str(num_paths_in_file)+'\n')
						log.write('Number of path tokens:'+str(num_paths_tokens)+'\n')
						log.write('Number of path tokens replaced:'+str(num_path_tokens_replaced)+'\n')
					print"DONE:"+INPUT_FILE
				except: 
					with codecs.open(OUTPUT_DIR+"/log.txt", 'a', encoding='UTF-8') as log:
						log.write(str(logging.exception("message")))

			print"\nDONE:ALL FILES IN FOLDER\n"
		except:
			with codecs.open(MAIN_OUTPUT_DIR+"/err.log", 'a', encoding='UTF-8') as errlog:
				errlog.write(str(logging.exception("message")))



									
if __name__ == '__main__':
	main()
	

