# -*- coding: utf-8 -*-
"""
Created on Tue Oct 22 13:19:43 2019

@author: Gary

code brought in from the jupyter notebook 'import shytruth - final version'
"""

import pandas as pd
import numpy as np
import csv

eTradeName = 'unrecorded trade name'
eSupplier = 'unrecorded supplier'
ePurpose = 'unrecorded purpose'

def get_df():
    return pd.read_csv('./sources/sky_truth_flagged.csv',
                       low_memory=False,quotechar='$')

def proc_rec(row,state):
    """part of state machine"""
    if row.api != state['api']:
        state['api'] = row.api
        state['cnt'] += 1
        state['ingredient_mode'] = False
        state['last_tn'] = eTradeName
        state['last_sup'] = eSupplier
        state['last_purp'] = ePurpose
        #state['last_row'] = row.row

    if row.trade_name not in ['','0','-',np.nan]:
        if row.trade_name[:10] == 'Ingredient':
            state['ingredient_mode'] = True
            state['last_tn'] = 'ingredient_mode'
            state['last_sup'] = eSupplier
            state['last_purp'] = ePurpose
        else:
            state['last_tn'] = row.trade_name
    if row.purpose not in ['','0','-',np.nan]:
        state['last_purp'] = row.purpose
    if row.supplier not in ['','0','-',np.nan]:
        state['last_sup'] = row.supplier
            
    row.trade_name = state['last_tn']
    row.supplier = state['last_sup']
    row.purpose = state['last_purp']
    return row


def fill_fields(df):
    """skytruth_raw mirrors the pdfs in that TradeName, Purpose and Supplier 
    are only mentioned once (and in an empty row) and subsequent rows have 
    their ingredients. We need to match up those fields with the ingredients 
    so that every row has all the appropriate fields."""
    
    state = {'api':None, 'last_tn':eTradeName, 'last_sup':eSupplier,
             'last_purp':ePurpose,'ingredient_mode': False, 'cnt':0, 'last_row':None}
    
    
    return df.apply(lambda x: proc_rec(x,state),axis=1)

def save_tn_expansion(df):
    df.to_csv('./sources/sky_truth_tn_expansion.csv',
              quotechar='$',quoting=csv.QUOTE_ALL,index=False)
    
def rename_fields(df):
    """to make merge with newer FracFocus data easier"""
    # give the 'st' prefix so that we can compare FF to ST if we want to...
    rn = {'api':'APINumber','state':'StateName','county':'CountyName',
          'operator':'OperatorName','well_name':'WellName',
          'latitude':'Latitude','longitude':'Longitude',
          'datum':'Projection','true_vertical_depth':'TVD',
          'total_water_volume':'TotalBaseWaterVolume',
          'trade_name':'TradeName','supplier':'Supplier','purpose':'Purpose',
          'ingredients':'IngredientName','cas_number':'CASNumber',
          'hf_fluid_concentration':'PercentHFJob',
          'additive_concentration':'PercentHighAdditive',
          'fracture_date':'JobEndDate','comments':'IngredientComment'
          }
    
    df = df.rename(index=str,columns=rn)
    
    
    df['IngredientKey'] = 'SKY_pdf_'+ df.raw_filename.astype('str').str[:4] \
                           + '_' + df.c_seqid.astype('str').str[:-2]
    return df

def save_final(df):
    df.to_csv('./sources/sky_truth_final.csv',
              quotechar='$',quoting=csv.QUOTE_ALL,index=False)
    

if __name__ == '__main__':
    df = get_df()
    df = fill_fields(df)
    save_tn_expansion(df)
    df = rename_fields(df)
    