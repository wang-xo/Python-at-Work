#!/usr/bin/env python
# coding: utf-8

# ## AST Difference Engine
# version 0.8
# 
# ### Goal
# In the most recent AAR, identify assignments additions, deletions, and changes as compared to the LKG AAR.
# 
# ### Output
# One xls file showing changes between the two files
# 
# ### Methodology
# 1. Ingest two AAR, CURRENT and LKG, as xls  
# 2. Clean the files  
# a. Drop junk columns  
# b. Make a unique key column
# 6. For each row, using != mask identify the difference in each column between CURRENT and LKG  
# 7. Output file with only differences, sorted
# 
# Config file contains sensitive information
# 
# ### Outstanding bugs
# 1. First row of output has bad formatting
# 
# ### Future implementation
# 1. Sign off feature to accept the changes and create a new LKG
# 2. Categorize output (new, modified, deleted)

# In[84]:


import pandas as pd
import numpy as np
import yaml


# In[85]:


with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)


# In[86]:


JUNKCOL = cfg['JUNKCOL']
KEYCOL = cfg['KEYCOL']
JUNKCOLSQL = cfg['JUNKCOLSQL']
KEYCOLSQL = cfg['KEYCOLSQL']


# In[87]:


pd.set_option('display.max_columns', 50)


# In[88]:


def clean_sql_inputs(infile, tag):
    """
    infile is string identifying the csv filename to read
    tag indicates whether the file is the CUR current version or the LKG last known good version
    return: writes a csv that is cleaned, with a good index (unique)
    
    assumes:
    """
    df = pd.read_excel(infile)
    df = df[[c for c in df if c not in JUNKCOLSQL]]
    df = df.dropna(axis=0, how='all')
    df[cfg['SALES']] = df[cfg['SALES']].astype('int')
    
    for c in KEYCOLSQL:
        df[c] = df[c].astype('str')
    df['Key'] = df[KEYCOLSQL].apply(lambda x: ''.join(x), axis=1)
    df = df.set_index('Key')

    df.to_excel("{} AAR worked.xlsx".format(tag), index=True)
    return df


# In[89]:


def clean_inputs(infile, tag):
    """
    infile is string identifying the csv filename to read
    tag indicates whether the file is the CUR current version or the LKG last known good version
    return: writes a csv that is cleaned, with a good index (unique)
    
    assumes: first two rows are dropped b/c they are normally header indicating which filters used
    """
    df = pd.read_excel(infile, skiprows=2)
    df = df[[c for c in df if c not in JUNKCOL]]
    df = df.dropna(axis=0, how='all')
    df[cfg['SALES']] = df[cfg['SALES']].astype('int')
    
    for c in KEYCOL:
        df[c] = df[c].astype('str')
    df['Key'] = df[KEYCOL].apply(lambda x: ''.join(x), axis=1)
    df = df.set_index('Key')

    df.to_excel("{} AAR worked.xlsx".format(tag), index=True)
    # df.to_csv("{} AAR worked.csv".format(tag), index=False)
    return df


# In[90]:


CUR = clean_sql_inputs('AAR SQL 20190322.xlsx', "CUR")
LKG = clean_sql_inputs('AAR SQL 20190319.xlsx', "LKG")


# In[91]:


# CUR = clean_sql_inputs('AAR 20190322 test.xlsx', "CUR")
# LKG = clean_sql_inputs('AAR 20190319 test.xlsx', "LKG")


# In[92]:


print(CUR.shape)
print(LKG.shape)


# In[93]:


# make a mask of any differences, true = changed
ids = CUR[KEYCOLSQL]


# In[94]:


mask = CUR.ne(LKG)
print(mask.shape)


# In[95]:


CUR = CUR[mask]


# In[96]:


LKG = LKG[mask]


# In[97]:


CUR = CUR.dropna(axis=0, how='all')


# In[98]:


LKG = LKG.dropna(axis=0, how='all')


# In[99]:


merged = CUR.merge(LKG, how = 'outer', on=['Key'], suffixes=['_CUR', '_LKG'])
# merged = CUR.merge(LKG, on=['Key'], suffixes=['_CUR', '_LKG'])


# In[102]:


# sort column names
merged = merged.sort_index(axis=1)

# Join on index -- ugly syntax could probably be improved 
merged = ids.merge(merged, how='right', left_index=True, right_index=True)

# clean = merged.dropna(thresh=1)
clean = merged.fillna("")
clean.sort_values(by=cfg['SORTCOL']).to_excel("diff.xlsx", index=True) 
# clean.sort_values(by=['EmailAlias_CUR', 'MSSalesTPID_CUR']).to_csv("diff.csv", index=False)


# In[82]:


"""
# debug code

CUR.index.value_counts()
CUR._data

print(CUR['SellerAssigmentID'])

for section in cfg:
    print(section)
print(cfg)
print(cfg['JUNKCOL'])

now we know, it takes the shape of the biggest file
mask.shape

"""


# In[83]:


CUR.index.value_counts()

