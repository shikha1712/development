import PyPDF2
import requests
import json
import os


class ElasticModel:

    name = ""
    msg = ""

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, 
            sort_keys=True, indent=4)

def __readPDF__(path):
    # pdf file object
    # you can find find the pdf file with complete code in below
    pdfFileObj = open(path, 'rb')
    # pdf reader object
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    # number of pages in pdf
    print(pdfReader.numPages)
    # a page object
    pageObj = pdfReader.getPage(0)
    # extracting text from page.
    # this will print the text you can also save that into String
    line = pageObj.extractText() 
    line = line.replace("\n","")
    print(line)
    return line


#line = pageObj.extractText()

def __prepareElasticModel__(line, name):
    eModel = ElasticModel();

    eModel.name = name
    eModel.msg = line
    return eModel


def __sendToElasticSearch__(elasticModel):
    print("Name : " + str(eModel))

############################################
####  #CHANGE INDEX NAME IF NEEDED
#############################################
    index = "samplepdf"

    url = "http://localhost:9200/" + index +"/_doc?pretty"
    data = elasticModel.toJSON()
    #data = serialize(eModel)
    response = requests.post(url, data=data,headers={
                    'Content-Type':'application/json',
                    'Accept-Language':'en'

                })
    print("Url : " + url)
    print("Data : " + str(data))

    print("Request : " + str(requests))
    print("Response : " + str(response))


#################################
#Change pdf dir path
###################################
pdfdir = "C:/Users/shikha agrawal/Documents/HRDocument"

listFiles = os.listdir(pdfdir)
for file in listFiles :
    path = pdfdir + "/" + file
    print(path)

    line = __readPDF__(path)
    eModel = __prepareElasticModel__(line, file)
    __sendToElasticSearch__(eModel)