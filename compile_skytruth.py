# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 15:58:06 2019

@author: Gary

"""

import skytruth_1, skytruth_2, skytruth_3

#####  from skytruth_1 -- many of these functions leave files as intermediates
print('Starting phase 1')
#raw = skytruth_1.get_all(skytruth_1.infns)
#skytruth_1.check_sep(raw)    
#skytruth_1.find_any_char(raw)
#raw = None
skytruth_1.reformat()
skytruth_1.pandas_sort_df()
df = skytruth_1.get_sorted()

#####  from skytruth_2
print('Starting phase 2')
lines = skytruth_2.get_lines()
#print(lines[0])
skytruth_2.flag_casig_dupes(lines)

##### from skytruth_3
print('Starting phase 3')
df = skytruth_3.get_df()
df = skytruth_3.fill_fields(df)
#skytruth_3.save_tn_expansion(df)
df = skytruth_3.rename_fields(df)
skytruth_3.save_final(df)
