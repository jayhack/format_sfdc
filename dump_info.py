#!/usr/bin/python
import csv
import os
import sys
import string



#--- for zip codes, longitude/latitude ---
# from pyzipcode import ZipCodeDatabase

#--- for all data types ---
break_pattern = '|||BREAK|||'

#--- for 'contact' files ---
postal_code_field_name = 'Mailing_Zip/Postal_Code'
mailing_city_name = "Mailing_City"
mailing_state_name = "Mailing_State/Province"
mailing_country_name = "Mailing_Country"
# zcdb = ZipCodeDatabase ()

#---- for sentiment ----
PATTERN_MODULE_MAC = "/Users/Shared/pattern-2.5"
PATTERN_MODULE_WIMPY = "/home/jhack/Programming/Scraper2/pattern_install_directory/lib/python2.6/site-packages"
if PATTERN_MODULE_MAC not in sys.path:
	sys.path.append(PATTERN_MODULE_MAC)
if PATTERN_MODULE_WIMPY not in sys.path:
	sys.path.append (PATTERN_MODULE_WIMPY)
from pattern.en import sentiment




#--- misc. data ---
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
########################[--- CAMPAIGN ---]########################################################################
##################################################################################################################

# Function: get_timestamp
# -----------------------
# this function will extract the time of the event from its name; makes the assumption that an event occured
# in july if it is not listed. timestamp returned as timestamp="month(#) year"
def get_timestamp (campaign_name):
	#--- break down the date ---
	campaign_name = campaign_name.replace ('-', ' ')
	campaign_name = campaign_name.replace(',', ' ')
	campaign_name = campaign_name.replace ('.', ' ')
	' '.join(campaign_name.split())

	splits = campaign_name.split (' ')
	splits = [s for s in splits if len(s) >= 1]
	last_3 = splits[-3:]

	#--- get year ---
	year = ''
	for s in last_3:
		if len(s) == 4 and s[0] == '2':
			year = s
		elif s[-2:].isdigit ():
			year = '20' + s[-2:]
	if year in last_3:
		last_3.remove(year)

	#--- get month ---
	month = ''
	for s in last_3:
		
		s_without_numbers = [d for d in s if not d.isdigit ()]
		s_without_numbers = ''.join(s_without_numbers)
		month_candidate = s_without_numbers.split ('_')[-1]

		if month_candidate.lower () in months.keys ():
			month = month_candidate.lower ()

	if month == '':
		month = 'July'


	month_number = months[month.lower()]
	day_number = '1'

	#--- get day ---
	#NOTE: not getting this for now	
	assert(year != '')
	assert (month != '')

	time_string = ' timestamp=' + year + '-' + str(month_number) + '-' + str(day_number)
	return time_string



# Function: dump_row_campaigns
# ---------------------------
# (for a campaigns file)
# dumps all the information from a given row into the splunk-readable file
def dump_row_campaigns (outfile, fields, row):

	# country_index = fields.index(mailing_country_name)
	# country = row[country_index]

	# zip_code_index = fields.index(postal_code_field_name)
	# zip_code = row[zip_code_index]

	# state_index = fields.index(mailing_state_name)
	# state = row[state_index].lower()

	# city_index = fields.index(mailing_city_name)
	# city = row[city_index].lower ()


	# latitude = ''
	# longitude = ''
	# location_string = " latitude=" + str(latitude) + " longitude=" + str(longitude)
	# if country == 'United States':
		# zip_code = zip_code.split('-')[0]

		# try: #--- look at zip code --- 
		# 	zip_code_object = zcdb[zip_code]
		# 	latitude = zip_code_object.latitude
		# 	longitude = zip_code_object.longitude
		# 	location_string = " latitude=" + str(latitude) + " longitude=" + str(longitude)


		# except: #--- look at the city ---

		# 	try: 		
		# 		zip_code = zcdb.find_zip (city=city, state=state)[0]
		# 		# print "city, state = " + city + ", " + state
		# 		# print " retrieved zip code = ", zip_code.zip
		# 		zip_code_object = zcdb[zip_code.zip]	
		# 		# print "found zip_code for city " + city
		# 		latitude = zip_code_object.latitude
		# 		longitude = zip_code_object.longitude
		# 		location_string = " latitude=" + str(latitude) + " longitude=" + str(longitude)
		# 	except:
				# pass
			


	#--- get the campaign name ---
	campaign_name_index = fields.index ('Campaign_Name')
	campaign_name = row[campaign_name_index]

	#--- get (derive) the time stamp ---
	# time_string = get_timestamp (campaign_name)

	start_date_index = fields.index ("Start_Date")
	start_date = row[start_date_index]
	time_string = ' timestamp="' + start_date.replace('/', '-') + '"'
	

	dump_string = break_pattern
	dump_string += time_string
	for i in range (len(row)):
		dump_string += ' ' + fields[i] + '="' + row[i] + '"'

	dump_string = dump_string.replace ('\n', ' ')

	dump_string += "\n\n\n"
	outfile.write (dump_string)
	return 



##################################################################################################################
########################[--- CONTACTS ---]########################################################################
##################################################################################################################

# Function: dump_row_contacts
# ---------------------------
# (for a campaigns file)
# dumps all the information from a given row into the splunk-readable file
def dump_row_contacts (outfile, fields, row):

	#--- get the campaign name ---
	campaign_name_index = fields.index ('Campaign_Name')
	campaign_name = row[campaign_name_index]

	#--- get (derive) the time stamp ---
	# time_string = get_timestamp (campaign_name)

	start_date_index = fields.index ("Start_Date")
	start_date = row[start_date_index]
	time_string = ' timestamp="' + start_date.replace('/', '-') + '"'


	dump_string = break_pattern
	dump_string += time_string
	for i in range (len(row)):
		dump_string += ' ' + fields[i] + '="' + row[i] + '"'

	dump_string = dump_string.replace ('\n', ' ')

	dump_string += "\n\n\n"
	outfile.write (dump_string)
	return 

##################################################################################################################
########################[--- LEADS ---]########################################################################
##################################################################################################################

# Function: dump_row_leads
# ---------------------------
# (for a campaigns file)
# dumps all the information from a given row into the splunk-readable file
def dump_row_leads (outfile, fields, row):

	#--- get the campaign name ---
	campaign_name_index = fields.index ('Campaign_Name')
	campaign_name = row[campaign_name_index]

	#--- get (derive) the time stamp ---
	# time_string = get_timestamp (campaign_name)

	start_date_index = fields.index ("Start_Date")
	start_date = row[start_date_index]
	time_string = ' timestamp="' + start_date.replace('/', '-') + '"'


	dump_string = break_pattern
	dump_string += time_string
	for i in range (len(row)):
		dump_string += ' ' + fields[i] + '="' + row[i] + '"'

	dump_string = dump_string.replace ('\n', ' ')

	dump_string += "\n\n\n"
	outfile.write (dump_string)
	return 


##################################################################################################################
########################[--- SURVEY ---]########################################################################
##################################################################################################################

# Function: get_survey_sentiment 
# ------------------------------
# given field #10 (on any extra comments), this function will return its polarity/subjectivity.
def get_sentiment_string (comment):
	(polarity, subjectivity) = sentiment(comment)
	polarity_string = '  polarity=' + str(polarity)
	subjectivity_string = '  subjectivity=' + str(subjectivity)
	return polarity_string + subjectivity_string



# Function: get_info_from_event_name
# ---------------------------------------
# given the name of an event, this will extract the month and year
# that it took place in and return (month, year)
def get_info_from_event_name (event_name):

	splits = event_name.split('_')
	location = splits[2]
	month_year = splits[3]
	year = month_year[-4:]
	month = month_year[:-4]

	return (location, month, year)



# Function: dump_row_survey
# ---------------------------
# (for a survey file)
# dumps all the information from a given row into the splunk-readable file
def dump_row_survey (outfile, fields, row, event_name):

	#--- event name and shit ---
	dump_string = break_pattern
	dump_string += '  event_name="' + event_name + '"'


	#--- time/location ---
	(location, month, year) = get_info_from_event_name (event_name)
	time_string = ' month="' + month + '"  year="' + year + '"'
	location_string = ' city="' + location + '"'
	dump_string += time_string + location_string




	satisfaction_fields = [fields[i] for i in range(len(row)) if fields[i][0] == '4']
	satisfaction_fields_filled = 0
	total_satisfaction = 0
	for i in range (len(row)):

		val = row[i]

		#--- get average satisfaction ---
		if fields[i][0] == '4':
			if val == '':
				val = '0'
			else:
				satisfaction_fields_filled += 1
				total_satisfaction += float (val)


		#--- get comment subjectivity/polarity ---
		elif fields[i][:2] == '10':
			if len(val) > 0:
				dump_string += get_sentiment_string(val)


		dump_string += '  ' + fields[i] + '="' + val + '"'


	avg_satisfaction = 0
	if satisfaction_fields_filled != 0:
		avg_satisfaction = total_satisfaction / satisfaction_fields_filled

	dump_string += '  avg_satisfaction="' + str(avg_satisfaction) + '"'

	dump_string += "\n\n\n"

	outfile.write (dump_string)
	return 



# Function: dump_row
# ------------------
# this function will dump the contents of a row into the outfile in a presentable format.
# customized for each filetype.
def dump_row (outfile, fields, row, event_name, filetype):

	if filetype == 'campaigns':
		return dump_row_campaigns (outfile, fields, row)
	elif filetype == 'contacts':
		return dump_row_contacts (outfile, fields, row)
	elif filetype == 'surveys':
		return dump_row_survey (outfile, fields, row, event_name)
	elif filetype == 'leads':
		return dump_row_leads (outfile, fields, row)
