#!/usr/bin/python
# -*- coding: utf-8 -*-

from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, ID, analysis
from whoosh.qparser import QueryParser, SequencePlugin, PhrasePlugin
from whoosh.query import spans, Term
from whoosh.index import open_dir
from os import path, mkdir
from string import maketrans, punctuation
from sys import maxunicode
from nltk import clean_html
import string, itertools, codecs, unicodedata, numpy


#################### 
# Load Punctuation #
####################

#load unicode punctuation
tbl = dict.fromkeys(i for i in xrange(maxunicode)
                      if unicodedata.category(unichr(i)).startswith('P'))

#create function with which to strip punct from unicode
def remove_punctuation(unicode_text):
    return unicode_text.translate(tbl)


##############
# Load Input #
##############

with codecs.open("/afs/crc.nd.edu/user/d/dduhaime/data/hill/hill_poetic_corpus.txt","r","utf-8") as raw_input_text:
    clean_input_text = remove_punctuation( raw_input_text.read() )
    split_input_text = clean_input_text.lower().split()


#############################
# Specify Search Parameters #
#############################

maximum_number_of_hits_per_query = 5
proximity_value                  = 8
window_length                    = 3
window_slide_interval            = 1
variant_spelling_desired         = 0

proximity_search_desired         = 1
exact_search_desired             = 0

    
##################
# Create Outfile #
##################

def find_outfile_name():

    '''Get an unused outfile name so we don't write over extant outfiles. Write search parameters into outfile name'''

    outfile_integer = 0
    outfile_name    = "hill_search_results_" + str(variant_spelling_desired) + str(maximum_number_of_hits_per_query) + str(proximity_value) + str(window_length) + str(window_slide_interval) + "_" + str(outfile_integer) + ".txt"
    while path.isfile( outfile_name ):
        outfile_integer += 1
        outfile_name = "hill_search_results_" + str(variant_spelling_desired) + str(maximum_number_of_hits_per_query) + str(proximity_value) + str(window_length) + str(window_slide_interval) + "_" + str(outfile_integer) + ".txt"
    return outfile_name
    

###################################
# Prepare Orthographical Variants #
###################################

#load orthographic equivalents once as a list or set that you can ping hereafter. The most efficient data structure for this list is likely a dictionary that contains as its various keys the words from the input text, and contains as its values the variants associated with those keys
if variant_spelling_desired == 1:
    with codecs.open("/afs/crc.nd.edu/user/d/dduhaime/data/orthographic_variants/aggregate_variants.txt","r","utf-8") as variants:
        variants_dictionary = {}
        variants = variants.read().split("\n")
        for i in variants:
            s_i = i.split("\t")
        
            #we now have a list containing each word in the current row. Alas, we need to make each of the members of this list a key in the dict, and must load all words in the row into the values for that key:
            for j in s_i:
            
                new_dict_key = j.strip()
                if new_dict_key not in variants_dictionary.keys():
                    variants_dictionary[new_dict_key] = u" ".join(j.strip() for j in s_i)

    def find_orthographical_variants(word):
        '''this function reads in a word and, after parsing our variants_dictionary, returns a list of all variants for that word'''
        word = word.strip()
        if word in variants_dictionary.keys():
            return variants_dictionary[word].split()

################################ 
# Exhaustive Exact Match Check #
################################

def recursive_exact_match(x,y):
    return set([t+1 for t in x]) & set(y)

def check_for_path_through_indices(l):
    '''this function reads in a list of lists l, then calls recusive_exact_match recursively,
    which looks for a path through the indices of l such that each step along the path is +1
    one of the values of l[0]. If it finds such a path, it returns True, indicating that there is
    an exact match in this hit. Otherwise, it returns false. See question 25458091 on SO.'''
    
    return bool(reduce(recursive_exact_match, l))

    
##############################
# Exhaustive Proximity Check #
##############################

def check_for_proximity(A, threshold):

    '''this function reads in a list of sublists and a threshold value (int). It then checks to see if there is a way of traversing the sublists with a maximum distance value = threshold parameter. For more details, see SO question 25249663'''

    for i in range(len(A) - 1):
        #print "Finding edges from layer", i, "to", i + 1, "..."
        diffs = numpy.array(A[i]).reshape((-1, 1)) - numpy.array(A[i + 1]).reshape((1, -1))
        reached = numpy.any(numpy.abs(diffs) <= threshold, axis = 0)
        A[i + 1] = [A[i + 1][j] for j in range(len(reached)) if reached[j]]
        #print "Reachable nodes of next layer:", A[i + 1]
    return any(reached)

    
########################
# Exact Match Function #
########################

def process_results_with_exact_function(search_terms, results):

    for hit in results:
                        
        #open file so we can grab highlights
        with codecs.open( hit["path"], "r", "utf-8") as fileobj:
            filecontents  = fileobj.read()
            hit_highlights = hit.highlights("play_text", text=filecontents, top=100000)
            
            #hit_highlight will now be a list of hits for each text. Let's split the list (n.b. this line assumes you've altered highlights.py source code to insert \t rather than *** between hits)
            list_of_hits = hit_highlights.split("\t")
            
            #iterate through these hits, which are still unclean (they have hit markup and they have punctuation)
            for dirty_hit in list_of_hits:
            
                #clean the hit by first removing the html markup and then by removing punctuation and line breaks
                clean_hit = remove_punctuation( clean_html(dirty_hit) ).replace(u"\n",u" ")
                
                #now encode clean_hit as utf-8 and split it
                split_hit = clean_hit.encode("utf-8").split()
                
                #in a moment, we'll compare each word in split_hit to each word of our search terms, and we'll record where the words in split_hit that match one of the search terms fall in split_hit (we'll record the index position of our search terms in split_hit)
                bag_of_indices = []
                
                #for each of our current search terms
                for search_term in search_terms:
                
                    #find the index position(s) for that term in our current split_hit: j = word in split_hit , i = index position of that word
                    index_positions_for_search_term = [i for i, j in enumerate(split_hit) if j.lower() == search_term.encode("utf-8").lower()]
                    
                    #if the current term has indices, then the current search term is somewhere in our current match
                    if index_positions_for_search_term:
                        
                        #store the index positions for the current match in a list within our bag_of_indices
                        bag_of_indices.append(index_positions_for_search_term)
                        
                #now that you've checked for the index positions of each search term within your split_hit, check to see how long the bag_of_indices. It will only == k if it contains all of the words in k.check to see if the length of the search term index list is equivalent to the length of the split_search_terms (this will only be the case if all n words from the query are present in the "hit")
                if len(bag_of_indices) == len(search_terms):
                
                    #here we know we have a list of lists that contains matches for all search terms. We now need to determine whether those terms occur in the right order of one another.
                    
                    if check_for_path_through_indices( bag_of_indices ):
                        
                        out.write( " ".join(search_terms).decode('utf-8')
                        + "\t" + hit["author"].decode('utf-8')
                        + "\t" + hit["filename"].decode('utf-8')
                        + "\t" + hit["path"].decode('utf-8')
                        + "\t" + "..." + " ".join(split_hit).decode('utf-8') + "..."
                        + "\n" )
                        
                        
############################
# Proximity Match Function #
############################

def process_results_with_proximity_function(search_terms, results, proximity_value):

    for hit in results:
                        
        #open file so we can grab highlights
        with codecs.open( hit["path"], "r", "utf-8") as fileobj:
            filecontents  = fileobj.read()
            hit_highlight = hit.highlights("play_text", text=filecontents, top=100000)
            
            #hit_highlight will now be a list of hits for each text. Let's split the list
            list_of_hits = hit_highlight.split("\t")
            
            #iterate through these hits, which are still unclean (they have hit markup and they have punctuation)
            for dirty_hit in list_of_hits:
            
                #clean the hit by first removing the html markup and then by removing punctuation and line breaks
                clean_hit = remove_punctuation( clean_html(dirty_hit) ).replace(u"\n",u" ")
                
                #now encode clean_hit as utf-8 and split it
                split_hit = clean_hit.encode("utf-8").split()
                
                #in a moment, we'll compare each word in split_hit to each word of our search terms, and we'll record where the words in split_hit that match one of the search terms fall in split_hit (we'll record the index position of our search terms in split_hit)
                bag_of_indices = []
                
                #for each of our current search terms
                for search_term in search_terms:
                
                    #find the index position(s) for that term in our current split_hit: j = word in split_hit , i = index position of that word
                    index_positions_for_search_term = [i for i, j in enumerate(split_hit) if j.lower() == search_term.encode("utf-8").lower()]
                    
                    #if the current term has indices, then the current search term is somewhere in our current match
                    if index_positions_for_search_term:
                        
                        #store the index positions for the current match in a list within our bag_of_indices
                        bag_of_indices.append(index_positions_for_search_term)
                        
                #now that you've checked for the index positions of each search term within your split_hit, check to see how long the bag_of_indices. It will only == k if it contains all of the words in k.check to see if the length of the search term index list is equivalent to the length of the split_search_terms (this will only be the case if all n words from the query are present in the "hit")
                if len(bag_of_indices) == len(search_terms):
                
                    '''Now the real work begins. We know that the current split_hit contains all of the words in k. To know if those words co-occur within
                    a certain proximity of one-another, though, we use a function defined above, passing it the three lists of indices for each search
                    term in our split_search_term. Say for example we have k = ["the","glory","days"] and the index positions for each of these words in
                    the current hit are [10] [12] [14]. In that case, we want to determine whether there is a path across these arrays such that, for each
                    "step" of the path, the distance of that step is less than the desired proximity value (three). See SO question 25249663'''
                    
                    #we pass itertools bag_of_indices (a list of lists). Using permutations, we create a new list of lists for each possible arrangement of our initial sublists. 
                    for i in itertools.permutations(bag_of_indices):
                        new_list = []
                        
                        #because itertools returns a set of lists, and our function reads in a list of lists, we extend our new_list item with all members of i, which preserves the order of those members
                        for j in i:
                            new_list.extend([j])
                            
                        if check_for_proximity(new_list, proximity_value):
                            out.write( " ".join(search_terms) + "\t" + hit["author"] + "\t" + hit["filename"] + "\t" + hit["path"] + "\t" + "..." + " ".join(clean_hit.split()) + "..." + "\n" )
                            break

    
###############
# Run Queries #
###############

with codecs.open( find_outfile_name(), "w", "utf-8" ) as out:
    ix = open_dir("/afs/crc.nd.edu/user/d/dduhaime/code/create_whoosh_index/index")

    # the most important method on the Searcher object is search(), which takes a whoosh.query.Query object and returns a Results object
    with ix.searcher() as searcher:
    
        #variable to keep track of when we reach the final window in the input text, and i=0 to set initial anchor for window at first word of input
        end_of_input_text = 0
        i = 0
        
        while end_of_input_text == 0:
        
            rolling_window = split_input_text[i:i+window_length]
            
            #create a variable we'll use below to keep track of the number of hits we get for the words in this rolling window
            number_of_hits_for_words_in_rolling_window = 0
            bag_of_hits_for_words_in_rolling_window = []
            
            #now create a list of lists for the three words in the rolling window
            list_containing_word_and_variant_lists = []
            
            #for each word currently in the rolling window, if user is using variant spellings, look up all orthographic equivalents to that word.
            for j in rolling_window:
                if variant_spelling_desired == 1:
                    word_and_variants = find_orthographical_variants(j)
                    if not word_and_variants:
                        word_and_variants = [j]
                    list_containing_word_and_variant_lists.append(word_and_variants)
                else:
                    list_containing_word_and_variant_lists.append(j)
                    
            #find all combinations of our three words with itertools.product, which takes list of lists and gives all combinations, e.g. A1,B1,C1, A1,B1,C2...An,Bn,Cn as a list
            exhaustive_combinations = list(itertools.product(*list_containing_word_and_variant_lists))
            
            #now we need only iterate through exhaustive_combinations, searching for each, and writing any hits to outfile
            for search_terms in exhaustive_combinations:
            
                #list of query components will start empty, we'll populate it, then submit our query
                list_of_query_components = []
                
                #iterate through search terms and add each to our list_of_query_components
                for term in search_terms:
                    query_component = Term("content", term)
                    list_of_query_components.append(query_component)
                    
                #now take all of those query components and submit them to the spans.SpanNear2 function, which (loosely speaking) facilitates proximity search (with high recall and low precision, thus why we have to iterate through all results and select only the true matches in our process_results() function defined above and called below)
                q = spans.SpanNear2(list_of_query_components, slop=proximity_value, ordered=False)
                 
                #by default the results contains at most the first 10 matching documents. To get more results, use the limit keyword: results = searcher.search(q, limit=20). printing "results" object is handy because it gives runtime for each query. Terms=true allows us to determine which of the search terms each hit has
                results = searcher.search(q, limit=None, terms=True)
                
                #the following line allows one to retrieve hits from farther into the document than 32K characters (so if character 32,001 is the beginning of a new word that matches query, we can grab that hit with the following line but will fail to catch it without that line)
                results.fragmenter.charlimit = None
                
                #add the current results to a bag of results for our three terms (e.g. find all results for all variant spellings of "so so now", then count up the total number of times that series of words yielded hits)
                if results:
                    if proximity_search_desired == 1:
                        process_results_with_proximity_function(search_terms, results, proximity_value)
                    
                    if exact_search_desired == 1:
                        process_results_with_exact_function(search_terms, results)
                    
            #check to make sure window can advance, and if it can, advance it:            
            if i + window_length + window_slide_interval <= len(split_input_text):
                i += window_slide_interval
            else:
                end_of_input_text = 1