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
import base64

def extractPDFFiles(files) :
 
   
        pdffileObj = open(file , 'rb')
        pdfrender = PyPDF2.PdfFileReader(pdffileObj)
        n_pages = pdfrender.numPages
        this_doc = ''
        for i in range(n_pages):
            pageObj = pdfrender.getPage(i)
            this_text = pageObj.extractText()
            this_doc += this_text
        encodedText = convertBase64Encoded(this_doc)
        return encodedText



def convertBase64Encoded(fileText) : 
        
        bytes_string = bytes(fileText, 'utf-8')
        encodedText = base64.b64encode(bytes_string)
        return encodedText
    
os.chdir('C:/Users/shikha agrawal/Documents/HRDocument')
files = glob.glob("*.*")

len(files)

for document in files:
    print(document)
    

        
for file in files :
     encdodedtext = extractPDFFiles(file)
     es = Elasticsearch([{'host': 'localhost', 'port': 9200}], timeout=90)
     col_names = df.columns
     for row_number in range(df.shape[0]) : 
         body = dict([(name , str(df.iloc[row_number][name])) for name in col_names])
         es.index(index = 'data_science'  , doc_type = 'books' , body = body)





    