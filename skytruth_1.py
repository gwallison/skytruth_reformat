# -*- coding: utf-8 -*-
"""
Created on Sun May  5 11:25:49 2019

@author: BMansfield

Step 1:check integrity of the raw skytruth files for line breaks and delimiters
- what chars to use to quote?  (looks like '$')

"""
import csv
import pandas as pd

infn1 = './sources/2012_FracFocusCombined.txt'
infn2 = './sources/2013_FracFocusCombined.txt'
infns = [infn1,infn2]
outfn = './sources/skytruth_cleaned.csv'

def get_all(infns):
    raw = ''
    for infn in infns:
        print(infn)
        with open(infn,'r') as f:
            raw += f.read() 
    return raw

def check_sep(raw):
    c = raw.count("\n")
    print(f'Number of line breaks: {c}')
    lines = raw.split('\n')
    print(f'Number of lines:       {len(lines)}')
    s = set()
    for i,l in enumerate(lines):
        num = l.count('\t')
        if num==0:
            print(f'No tabs found on line: {i}')
        s.add(num)
    print(f'set of tab numbers: {s}')

def find_any_char(raw,chars=['^','$','#','%','!','*',"'", '"','\t\n']):
    for c in chars:
        print(f'{c}: {raw.count(c)}')
    
def reformat(outfn=outfn):
    with open(outfn,'w',newline='') as csvfile:
        outw = csv.writer(csvfile, delimiter=',',
                          quotechar='$', quoting=csv.QUOTE_ALL)
        with open(infn1,'r') as f:
            raw = f.read()
            lines = raw.split('\n')
            lines[0] +=  '\traw_filename' # add to the header
            for i,line in enumerate(lines):
                if i>0:
                    line += '\tSKY_2012'
                l = line.split('\t')
                outw.writerow(l)
        with open(infn2,'r') as f:
            raw = f.read()
            lines = raw.split('\n')
            for i,line in enumerate(lines[1:]):  # skip header
                line += '\tSKY_2013'
                l = line.split('\t')
                outw.writerow(l)

def pandas_sort_df():
    df = pd.read_csv(outfn,quotechar='$',low_memory=False)
    df = df.sort_values(by=['raw_filename','r_seqid','pdf_seqid','c_seqid','row'])    
    df.to_csv('./sources/sky_truth_sorted_raw.csv',index=False,
              quotechar='$',quoting=csv.QUOTE_ALL)
    
def get_sorted():
    df = pd.read_csv('./sources/sky_truth_sorted_raw.csv',
                     quotechar='$',low_memory=False)
    return df
    
if __name__ == '__main__':
    raw = get_all(infns)
    check_sep(raw)    
    find_any_char(raw)
    raw = None
    reformat()
    pandas_sort_df()
    df = get_sorted()