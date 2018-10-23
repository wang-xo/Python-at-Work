
# coding: utf-8

# ## AST Difference Engine
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
# a. Drop first two rows  
# b. Convert each to df  
# 5. Merge the dfs   
# 6. For each row, using != mask identify the difference in each column between CURRENT and LKG  
# 7. Output file with only differences  
# 
# ### Outstanding bugs
# 1. Does this pick up deleted lines if the key is not there?  
# a. if using AssignmentStatus, may carry some risk
# 
# ### Future implementation
# 1. Ingest xls  
# 1. export to notepad++ for testing
# 2. use config file
# 2. factor code  
# a. why does it not work to create df for use outside of function?
# 3. Sign off feature to accept the changes and create a new LKG

# In[1]:


import pandas as pd
import numpy as np


# In[2]:


# see config

# In[3]:


def clean_inputs(infile, tag):
    """
    infile is string identifying the csv filename to read
    tag indicates whether the file is the CUR current version or the LKG last known good version
    return: writes a csv that is cleaned, with a good index (unique)
    
    assumes: first two rows are dropped b/c they are normally header indicating which filters used
    """
    df = pd.read_csv(infile, skiprows=2)
    df = df[[c for c in df if c not in JUNKCOL]]
    df = df.dropna(axis=0, how='all')
    # read as int instead of float
    df[SALES] = df[SALES].astype('int')
    
    for c in KEYCOL:
        df[c] = df[c].astype('str')
    df['Key'] = df[KEYCOL].apply(lambda x: ''.join(x), axis=1)
    # if you set the index, then it strips out as a column and does not write to csv

    df.to_csv("{} AAR worked.csv".format(tag), index=False)
    return df


if __name__ == '__main__':

    # In[4]:


    CUR = clean_inputs('AAR 20181011 alt.csv', "CUR")


    # In[5]:


    LKG = clean_inputs('AAR 20181012.csv', "LKG")


    # In[6]:


    print(CUR.shape)
    print(LKG.shape)


    # In[7]:


    # make a mask of any differences, true = changed
    ids = CUR[KEYCOL]
    mask = CUR.ne(LKG)
    CUR = CUR[mask]
    LKG = LKG[mask]

    CUR = CUR.dropna(axis=0, how='all')
    LKG = LKG.dropna(axis=0, how='all')

    merged = CUR.merge(LKG, on=['Key'], suffixes=['_CUR', '_LKG'])

    # sort column names
    merged = merged.sort_index(axis=1)

    # Join on index -- ugly syntax could probably be improved 
    merged = ids.merge(merged, how='right', left_index=True, right_index=True)

    # clean = merged.dropna(thresh=1)
    clean = merged.fillna("")
    clean.sort_values(by=['EmailAlias_CUR', '{}_CUR'.format(SALES)]).to_csv("diff.csv", index=False) 


    # In[8]:


    # now we know, it takes the shape of the biggest file
    mask.shape
