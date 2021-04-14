import base64
from uuid import uuid4
from elasticsearch.client.ingest import IngestClient
from elasticsearch.exceptions import NotFoundError
from elasticsearch_dsl import analyzer, DocType, Index
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.field import  Text


# Establish a connection
host = 'localhost'
port = 9200
es = connections.create_connection(host=host, port=port ,  timeout=120)


class ExampleIndex(DocType):
    class Meta:
        index = 'example'
        doc_type = 'Example'

    id = Text()
    uuid = Text()
    name = Text()
    town = Text()
    my_file = Text()


def save_document(doc):
    """

    :param obj doc: Example object containing values to save
    :return:
    """
    try:
        # Create the Pipeline BEFORE creating the index
        p = IngestClient(es)
        p.put_pipeline(id='myattachment', body={
            'description': "Extract attachment information",
            'processors': [
                {
                    "attachment": {
                        "field": "my_file"
                    }
                }
            ]
        })

        # Create the index. An exception will be raise if it already exists
        i = Index('example')
        i.doc_type(ExampleIndex)
        i.create()
    except Exception:
        # todo - should be restricted to the expected Exception subclasses
        pass

    indices = ExampleIndex()
   
    indices.uuid = doc.uuid
    indices.name = doc.name
    indices.town = doc.town
    if doc.my_file:
        with open(doc.my_file, 'rb') as f:
            contents = f.read()
            
        indices.my_file = base64.b64encode(contents).decode("ascii")

    # Save the index, using the Attachment pipeline if a file was attached
    return indices.save(pipeline="myattachment") if indices.my_file else indices.save()


class MyObj(object):
    uuid = uuid4()
    name = ''
    town = ''
    my_file = ''

    def __init__(self, name, town, file):
        self.name = name
        self.town = town
        self.my_file = file


me = MyObj("Steve", "London", 'C:/Users/shikha agrawal/Documents/HRDocument/Human_Resource_Management_32088.pdf')

res = save_document(me)