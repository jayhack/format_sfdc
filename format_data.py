#!/usr/bin/python
#
# Script: format_data.py
# ---------------------
# Usage: ./format_data.py input_directory output_directory
#
# this script takes all of the .csv files in the input directory and generates splunk-readable files in the
# output directory, one for each input file and with the extension changed to .txt. special treatment for the 
# 'campaigns' files!

import csv
import os
import sys
import string



#--- for all data types ---
break_pattern = '|||BREAK|||'

#--- for 'contact' files ---
postal_code_field_name = 'Mailing_Zip/Postal_Code'
mailing_city_name = "Mailing_City"
mailing_state_name = "Mailing_State/Province"
mailing_country_name = "Mailing_Country"

#---- for sentiment ----
PATTERN_MODULE_MAC = "/Users/Shared/pattern-2.5"
PATTERN_MODULE_WIMPY = "/home/jhack/Programming/Scraper2/pattern_install_directory/lib/python2.6/site-packages"
if PATTERN_MODULE_MAC not in sys.path:
	sys.path.append(PATTERN_MODULE_MAC)
if PATTERN_MODULE_WIMPY not in sys.path:
	sys.path.append (PATTERN_MODULE_WIMPY)
from pattern.en import sentiment


#---- for dumping rows ----
from dump_info import dump_row


#--- misc. data ---
us_cities = ['New York,' 'Philadelphia', 'Denver', 'Atlanta', 'Seattle', 'Chicago', 'Palo Alto', 'Orange County', 'Seattle',
				'Miami', 'Minneapolis', 'Houston', 'Charlotte', 'Baltimore', 'St Louis', 'San Francisco', 'Boston', 'Orlando', 
				'Raleigh', 'Los Angeles', 'Dallas', 'Salt Lake City', 'Phoenix', 'San Diego', 'Scottsdale', 'Cincinnati', 'Kansas City', 
				'Detroit', 'Indianapolis', 'DC', 'St. Louis', 'LA', 'NYC', 'Columbus', 'San Jose', 'Pittsburg', 'NJ', 'Washington']


months = {	'jan.':1, 'jan':1, 'january':1, 
			'feb.':2, 'feb':2, 'february':2,
			'mar.':3, 'mar':3, 'march':3,
			'apr.':4, 'apr':4, 'april':4,
			'may':5, 'may':5, 'may':5,
			'jun.':6, 'jun':6, 'june':6,
			'jul.':7, 'jul':7, 'july':7,
			'aug.':8, 'aug':8, 'august':8,
			'sep.':9, 'sep':9, 'september':9, 'sept.':9, 'sept':9,
			'oct.':10, 'oct':10, 'october':10,
			'nov.':11, 'nov':11, 'november':11,
			'dec.':12, 'dec':12, 'december':12}


##################################################################################################################
########################[--- UTILITIES ---]#######################################################################
##################################################################################################################
# Function: print_error
# ---------------------
# notifies the user of an error, how to correct it, then exits
def print_error (error_message, correction_message):
	print "ERROR: 	", error_message
	print "	---"
	print "	", correction_message
	exit ()


# Function: get_output_file_name
# ---------------------------
# given the input file name and the output directory, this will return the directory name
def get_output_file_name (input_filename, output_dir):

	input_base = os.path.splitext (input_filename)[0]
	output_end = input_base + ".txt"
	output_total = os.path.join (output_dir, output_end)
	return output_total











##################################################################################################################
########################[--- MAIN OPERATION ---]##################################################################
##################################################################################################################

# Function: reformat_csv_file
# ---------------------------
# given the input filename and output filename (both absolute), this function
# will create a splunk-readable file containing the input file's information
def reformat_csv_file (filename_raw, filename_formatted, filetype):

	# print ("---> reformatting" + filename_raw + "... ")

	raw_file = open (filename_raw, 'r')
	formatted_file = open(filename_formatted, 'w')
	reader = csv.reader (raw_file, delimiter=',', quotechar='"')

	fields_raw = reader.next ()
	

	#--- get rid of spaces ---
	fields_nospaces = [f.replace(' ', '_') for f in fields_raw]
	
	#--- get rid of non-alphanumeric ---
	fields = []
	exclude = set(string.punctuation)
	exclude.remove('_')
	for field in fields_nospaces:
		new_field = ''.join(ch for ch in field if ch not in exclude)
		fields.append (new_field)


	#--- get the campaign name ---
	campaign_name_index = fields.index ('Campaign_Name')

	for row in reader:
		if len(row) == len(fields):

			campaign_name = row[campaign_name_index]

			#--- check to see if the city is in the US ---
			for city in us_cities:
				if campaign_name.find (city) != -1:

					event_name = os.path.split(filename_raw)[1].split('.')[0]
					dump_row (formatted_file, fields, row, event_name, filetype)
					break

	raw_file.close ()
	formatted_file.close ()















if __name__ == "__main__":

	
	if len (sys.argv) != 4:
		print_error ('incorrect # of arguments', 'correct usage: ./format_data.py input_directory output_directory filetype')


	### Step 1: get targeted files ###
	input_dir = sys.argv[1]
	output_dir = sys.argv[2]
	filetype = sys.argv[3]


	### Step 2: make sure filetype is supported ###
	if filetype == 'surveys':
		print "---> Parsing: surveys"
	elif filetype == 'campaigns':
		print "---> Parsing: campaigns"
	elif filetype == 'contacts':
		print "---> Parsing: contacts"
	elif filetype == 'leads':
		print "---> Parsing: leads" 
	else:
		print_error ('filetype unrecognized - ' + filetype, "supported: 'surveys', 'leads', 'campaigns' or 'contacts'")


	### Step 3: get the absolute paths to input/output directories###
	if not os.path.isabs(input_dir):
		input_dir = os.path.abspath(input_dir)
	if not os.path.isabs(output_dir):
		output_dir = os.path.abspath (output_dir)
	print "input dir: " + input_dir
	print "output dir: " + output_dir 
	print "\n"


	### Step 4: for each file in the directory, format it ###
	for input_filename in os.listdir (input_dir):

		### discard files that start with '.' (i.e. .DS_Store, etc) ###
		if input_filename[0] != '.':
		
			### get the real input/output names ###
			input_filename_abs = os.path.join (input_dir, input_filename)
			output_filename_abs = get_output_file_name (input_filename, output_dir)


			### reformat them ###
			reformat_csv_file (input_filename_abs, output_filename_abs, filetype)

			print "	" + input_filename_abs + " -> " + output_filename_abs
			print "\n"









