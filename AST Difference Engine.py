#!/usr/bin/env python
# coding: utf-8

# ## AST Difference Engine
# version 0.7
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
# a. Drop rows until data table (drops 2 right now)  
# b. Drop junk columns  
# c. Make a unique key column
# 6. For each row, using != mask identify the difference in each column between CURRENT and LKG  
# 7. Output file with only differences, sorted
# 
# ### Outstanding bugs
# 1. Does this pick up deleted lines if the key is not there?  
# a. if using AssignmentStatus, may carry some risk
# 
# ### Future implementation
# 1. export to notepad++ for testing
# 2. use config file
# 2. factor code  
# a. why does it not work to create df for use outside of function?
# 3. Sign off feature to accept the changes and create a new LKG

# In[ ]:


import pandas as pd
import numpy as np
import yaml


# In[ ]:


with open("config.yaml", 'r') as ymlfile:
        cfg = yaml.load(ymlfile)


# In[ ]:


JUNKCOL = cfg['JUNKCOL']
KEYCOL = cfg['KEYCOL']
JUNKCOLSQL = cfg['JUNKCOLSQL']
KEYCOLSQL = cfg['KEYCOLSQL']


# In[ ]:


pd.set_option('display.max_columns', 50)


# In[ ]:


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


# In[ ]:


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


# In[ ]:


CUR = clean_sql_inputs('AAR SQL 20190322.xlsx', "CUR")
LKG = clean_sql_inputs('AAR SQL 20190319.xlsx', "LKG")


# In[ ]:


# CUR = clean_sql_inputs('AAR 20190322 test.xlsx', "CUR")
# LKG = clean_sql_inputs('AAR 20190319 test.xlsx', "LKG")


# In[ ]:


print(CUR.shape)
print(LKG.shape)


# In[ ]:


# make a mask of any differences, true = changed
ids = CUR[KEYCOLSQL]


# In[ ]:


mask = CUR.ne(LKG)
print(mask.shape)


# In[ ]:


CUR = CUR[mask]


# In[ ]:


LKG = LKG[mask]


# In[ ]:


CUR = CUR.dropna(axis=0, how='all')


# In[ ]:


LKG = LKG.dropna(axis=0, how='all')


# In[ ]:


merged = CUR.merge(LKG, how = 'outer', on=['Key'], suffixes=['_CUR', '_LKG'])


# In[ ]:


# sort column names
merged = merged.sort_index(axis=1)

# Join on index -- ugly syntax could probably be improved 
merged = ids.merge(merged, how='right', left_index=True, right_index=True)

# clean = merged.dropna(thresh=1)
clean = merged.fillna("")
clean.sort_values(by=cfg['SORTCOL']).to_excel("diff.xlsx", index=True) 
# clean.sort_values(by=['EmailAlias_CUR', 'MSSalesTPID_CUR']).to_csv("diff.csv", index=False)


# In[ ]:


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


# In[ ]:


CUR.index.value_counts()

