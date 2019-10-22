# -*- coding: utf-8 -*-
"""
Created on Sun May  5 11:25:49 2019

@author: BMansfield

Phase 2:
    Work with each event and fill trade_name etc...

"""
import csv
infn = './sources/sky_truth_sorted_raw.csv'
outfn = './sources/sky_truth_flagged.csv'

def get_lines(infn=infn):
    with open(infn,'r') as f:
        lines = f.readlines()
    print(f'Number of lines read in from {infn}: {len(lines)}\n')
    return lines
    
def flag_casig_dupes(lines):
    # adding flag inplace 
    curr_api = ''
    likely_mult = []
    for i,l in enumerate(csv.reader(lines, quotechar='$', delimiter=',',
                          quoting=csv.QUOTE_ALL)):    
        
        if i==0: # deal with header
            l.append('potential_casig_dupe')
            #l.append('likely multiple')
            lines[0] = l
            next
            
        else: 
            if l[2]!=curr_api:  # then reset field memory
                # new event
                curr_api = l[2]
                curr_ig = ''
                curr_cas = ''
                curr_per_hf = ''
                curr_sup = ''
                
            if (curr_ig==l[20]) & (curr_cas==l[21]) & (curr_per_hf==l[23]) \
                  & (curr_per_hf != 'None') & (l[25]!='proprietary'):
                l.append('True')
                #print('found a dupe')
            else:
                l.append('False')
            
            # puts a True in second of dupe, but first should be deleted.
            #   so keep track of index of record to drop                
            if (l[-1] == 'True') & (curr_sup!='') & (l[18]==''):
                likely_mult.append(i-1)
                l[-1] = 'False'  # remove flag because it has been fixed
                
            curr_ig = l[20]
            curr_cas = l[21]
            curr_per_hf = l[23]
            curr_sup = l[18]
              
            lines[i] = l            
            
    with open(outfn,'w',newline='') as csvfile:
        outw = csv.writer(csvfile, delimiter=',',
                          quotechar='$', quoting=csv.QUOTE_ALL)
        for i,ln in enumerate(lines):
            if i not in likely_mult:
                outw.writerow(ln)

    
def show_line(line):
    for l in line.split('\t'):
        print(l)
        
if __name__ == '__main__':
    lines = get_lines()
    print(lines[0])
    lines = flag_casig_dupes(lines)
    
