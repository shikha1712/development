# -*- coding: utf-8 -*-
"""
Created on Tue Apr 13 13:21:11 2021

@author: shikha agrawal
"""

from elasticsearch import Elasticsearch
import os
import glob
import pandas as pd
import PyPDF2

os.chdir('C:/Users/shikha agrawal/Documents/HRDocument')
files = glob.glob("*.*")

len(files)

for document in files:
    print(document)
    
def extractPDFFiles(files) :
    this_loc = 1
    df = pd.DataFrame(columns = ("name" , "content"))
    
    for file in files:
        pdffileObj = open(file , 'rb')
        pdfrender = PyPDF2.PdfFileReader(pdffileObj)
        n_pages = pdfrender.numPages
        this_doc = ''
        for i in range(n_pages):
            pageObj = pdfrender.getPage(i)
            this_text = pageObj.extractText()
            this_doc += this_text
        df.loc[this_loc] = file , this_doc
        this_loc = this_loc +1
    return df


df = extractPDFFiles(files)
df.head()


es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=90)
col_names = df.columns
for row_number in range(df.shape[0]) : 
    body = dict([(name , str(df.iloc[row_number][name])) for name in col_names])
    es.index(index = 'data_science'  , doc_type = 'books' , body = body)

    