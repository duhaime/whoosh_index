#!/usr/bin/env python

#set pythonpath environment variable
import os
os.environ["PYTHONPATH"] = "/afs/crc.nd.edu/user/d/dduhaime/code/site-packages/lib/python"

'''This script was designed to create an FTS index of several corpora on the CRC. Supported metadata fields are as follows: Path, Encoding, Text Name, Author, Publication Date, Full Text

Whoosh demands that all inputs be in unicode format, so it might be helpful to review some encoding fundamentals: We can think of encoding in monetary terms.
A commodity has an abstract 'value' that is not tied to any currency, but we can express that value in terms of a specific currency. In a vaguely similar
way, all text has an abstract unicode representation that is not tied to any encoding, but we can express that representation in certain encodings,
depending on the contents of the text. (Certain characters can not be expressed in certain encodings.)

In general, we want to get text into utf-8, which is a fairly permissive encoding (the set of possibilities in utf-u encoding, to the best of my knowledge, includes
and surpasses the set of possibilities in many other encodings, like ascii).

Byte-strings (type = str) have encoding; unicode text (type = unicode) does not.
One 'decodes' byte-string into unicode using a method like: unicode(byte_string, encoding_of_bytestring)
One 'encodes' unicode into byte-strings with a particular encoding using a method like: unicode_string.encode(encoding_to_use)'''

from whoosh.index import create_in
from whoosh.fields import *
import glob, os, chardet, codecs

def determine_string_encoding(string):
    result = chardet.detect(string)
    string_encoding = result['encoding']
    return string_encoding

###########################################################################################################################################################
# Port in Metadata ########################################################################################################################################
###########################################################################################################################################################
  
import glob, codecs

def create_eebo_metadata_dictionary():

    '''function to create a dictionary of the eebo_tcp metadata containing author, pub_year, and title, keyed to filename'''

    eebo_metadata_dictionary = {}
    
    with open("/afs/crc.nd.edu/user/d/dduhaime/data/metadata/TCP_list1.sgm") as eebo_metadata:
        eebo_metadata = eebo_metadata.readlines()
        for k in eebo_metadata:
            
            filename = k.split("<ENTRY")[1].split('"')[1].split('"')[0]
            
            try:
                author = k.split("AUTHOR")[1].split(">")[1].split("<")[0]
                
            except:
                author = ""
                
            title = k.split("<STIT")[1].split(">")[1].split("<")[0]
            publication_year = k.split("<YR")[1].split(">")[1].split("<")[0]
            
            if "-" in publication_year:
                publication_year = publication_year.split("-")[0]
                
            if filename not in eebo_metadata_dictionary.keys():
                eebo_metadata_dictionary[filename] = {}
                eebo_metadata_dictionary[filename]["author"] = author.strip()
                eebo_metadata_dictionary[filename]["title"] = title.strip()
                eebo_metadata_dictionary[filename]["publication_year"] = publication_year.strip()
    
        return eebo_metadata_dictionary


def create_ecco_metadata_dictionary():
    
    #'''function to create a dictionary of the ecco_tcp metadata containing author, pub_year, and title, keyed to filename'''

    with open("/afs/crc.nd.edu/user/d/dduhaime/data/metadata/TCPtexts.csv") as ecco_metadata:
        ecco_metadata_dictionary = {}
        
        ecco_metadata = ecco_metadata.readlines()
        for m in ecco_metadata:
            m_s = m.split(",")
            
            filename = m_s[1]
            author = m_s[5]
            title = m_s[8]
            publication_year = m_s[0]
            
            if filename not in ecco_metadata_dictionary.keys():
                ecco_metadata_dictionary[filename] = {}
                ecco_metadata_dictionary[filename]["author"] = author.strip()
                ecco_metadata_dictionary[filename]["title"] = title.strip()
                ecco_metadata_dictionary[filename]["publication_year"] = publication_year.strip()
              
        return ecco_metadata_dictionary


def create_early_english_drama_metadata_dictionary():

    with open("/afs/crc.nd.edu/user/d/dduhaime/data/metadata/drama_metadata.txt") as metadata_in:
        
        early_english_drama_metadata_dictionary = {}
        
        metadata_rows = metadata_in.read().split("\r\n")
        for row in metadata_rows:
            split_row        = row.split("\t")
            if len(split_row) > 4:
                filename         = split_row[0].strip()
                author           = split_row[1].strip()
                short_title            = split_row[2].strip()
                publication_year = split_row[4].strip()
            
            if filename not in early_english_drama_metadata_dictionary.keys():
                early_english_drama_metadata_dictionary[filename] = {}
                early_english_drama_metadata_dictionary[filename]["author"]           = author
                early_english_drama_metadata_dictionary[filename]["title"]            = short_title
                early_english_drama_metadata_dictionary[filename]["publication_year"] = publication_year
                
        return early_english_drama_metadata_dictionary
        
        
def create_gutenberg_metadata_dictionary():
    with codecs.open("/afs/crc.nd.edu/user/d/dduhaime/data/metadata/gutenberg_metadata.txt") as gutenberg_metadata:
        gutenberg_metadata_dictionary = {}
        split_gutenberg_metadata = gutenberg_metadata.read().split("\r\n")
        for i in split_gutenberg_metadata[1:]:
            split_i = i.split("\t")
            
            if len(split_i) > 3:
            
                gutenberg_filename = split_i[4].replace("\r","")
                gutenberg_author   = split_i[0]
                gutenberg_title    = split_i[1]
                
                if gutenberg_filename not in gutenberg_metadata_dictionary.keys():
                    gutenberg_metadata_dictionary[gutenberg_filename] = {}
                    gutenberg_metadata_dictionary[gutenberg_filename]["author"]           = gutenberg_author
                    gutenberg_metadata_dictionary[gutenberg_filename]["title"]            = gutenberg_title
                    gutenberg_metadata_dictionary[gutenberg_filename]["publication_year"] = "UNSPECIFIED"
                    
        return gutenberg_metadata_dictionary
        
        
def create_latin_library_metadata_dictionary():
    latin_library_metadata_dictionary = {}
    for i in glob.glob("/afs/crc.nd.edu/user/d/dduhaime/data/latin_library_single_directory/*.txt"):
        file_name = i.split("/")[-1].replace(".txt","")
        author = file_name
        title = (file_name + "_corpus").replace(" ","_")
        publication_year = "unspecified"
        
        if file_name not in latin_library_metadata_dictionary.keys():
            latin_library_metadata_dictionary[file_name] = {}
            latin_library_metadata_dictionary[file_name]["author"] = author
            latin_library_metadata_dictionary[file_name]["title"] = title
            latin_library_metadata_dictionary[file_name]["publication_year"] = publication_year
            
    return latin_library_metadata_dictionary


eebo_metadata = create_eebo_metadata_dictionary()
ecco_metadata = create_ecco_metadata_dictionary()
gutenberg_metadata = create_gutenberg_metadata_dictionary()
latin_library_metadata = create_latin_library_metadata_dictionary()

############################################################################################################################################################
# Create Index #############################################################################################################################################
############################################################################################################################################################    

#specify a list of paths that contain all of the texts we wish to index
text_dirs = [

"/afs/crc.nd.edu/user/d/dduhaime/data/ecco",
"/afs/crc.nd.edu/user/d/dduhaime/data/eebo/eebo_plaintext",
"/afs/crc.nd.edu/user/d/dduhaime/data/gutenberg_single_directory",
"/afs/crc.nd.edu/user/d/dduhaime/data/latin_library_single_directory"

]

title=TEXT(stored=True, analyzer=analysis.StandardAnalyzer(stoplist=None))

#establish the schema to be used when storing texts; storing content allows us to retrieve hightlighted extracts from texts in which matches occur
schema = Schema( path=ID(stored=True), encoding=TEXT(stored=True), content=TEXT(stored=True, analyzer=analysis.StandardAnalyzer(stoplist=None)), title=TEXT(stored=True), author=TEXT(stored=True), publication_year=TEXT(stored=True) )

#check to see if we already have an index directory. If we don't, make it)
if not os.path.exists("index"):
    os.mkdir("index")
ix = create_in("index", schema)

#create writer object we'll use to write each of the documents in text_dir to the index
writer = ix.writer()

#for each directory in our list
for i in text_dirs:

    #for each text file in that directory (j is now the path to the current file within the current directory)
    for j in glob.glob( i + "/*.txt" ):
        
        try:
            text_content_encoding = "utf-8"
            
            #first, let's grab j filename:
            text_filename = j.split("/")[-1][:-4]
            
            ####################
            # Consult Metadata #
            ####################
            
            try:
                if "eebo" in j:
                    author           = eebo_metadata[text_filename]["author"]
                    title            = eebo_metadata[text_filename]["title"]
                    publication_year = eebo_metadata[text_filename]["publication_year"]
                    
                if "ecco" in j:
                
                    author           = ecco_metadata[text_filename]["author"]
                    title            = ecco_metadata[text_filename]["title"]
                    publication_year = ecco_metadata[text_filename]["publication_year"]
                    
                if "drama" in j:
                    author           = early_english_metadata[text_filename]["author"]
                    title            = early_english_metadata[text_filename]["title"]
                    publication_year = early_english_metadata[text_filename]["publication_year"]
                    
                if "gutenberg" in j:
                    author           = gutenberg_metadata[text_filename]["author"]
                    title            = gutenberg_metadata[text_filename]["title"]
                    publication_year = gutenberg_metadata[text_filename]["publication_year"]
                    
                if "latin" in j:
                    author           = latin_library_metadata[text_filename]["author"]
                    title            = latin_library_metadata[text_filename]["title"]
                    publication_year = latin_library_metadata[text_filename]["publication_year"]
                    
            #if you get a key error, then the given text doesn't have metadata fields available, so pass appropriate values to variables
            except KeyError:
            
                author               = "Metadata Missing. See: " + text_filename
                title                = "Metadata Missing. See: " + text_filename
                publication_year     = "Metadata Missing. See: " + text_filename
                
                print "error at 239 with file ", j
                
            #######################
            # Index File Segments #
            #######################
            
            with open( j, "r" ) as text_content:  
                
                text_content = text_content.read()
                
                decoded_content = text_content.decode(text_content_encoding, errors = "ignore")  
                
                #use method defined above to determine encoding of path and text_content
                path_encoding = determine_string_encoding(j)
                
                #decode text_title, path, and text_content to unicode using the encodings we determined for each above
                try:
			unicode_text_path        = unicode(j, path_encoding)
		except Error as er:
			print "error at 258 with file", j, er
					
		try:
			unicode_encoding         = unicode(text_content_encoding)
		except Error as er:
			print "error at 263 with file", j, er
					
                try:
			unicode_content          = decoded_content
		except Error as er:
			print "error at 268 with file", j, er
					
		try:
			unicode_title            = unicode(title, "utf-8")
		except Error as er:
			print "error at 273 with file", j, er
					
		try:
			unicode_author           = unicode(author, "utf-8")
		except Error as er:
			print "error at 278 with file", j, er
                
		try:
			unicode_publication_year = unicode(publication_year, "utf-8")
		except Error as er:
			print "error at 283 with file", j, er
                    
                #use writer method to add document to index
                writer.add_document( path = unicode_text_path, encoding = unicode_encoding, content = unicode_content, author = unicode_author, title = unicode_title, publication_year = unicode_publication_year )
                
            print "loaded ", text_filename
            
        #if you hit an error, print j (the error may well be related to the encoding of j)
        except Exception as e:
            print j, e
            
#after you've added all of your documents, commit changes to the index
writer.commit()
