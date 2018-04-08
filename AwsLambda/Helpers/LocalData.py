import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import os



class LocalData(object):
    """description of class"""

    def CreateAvroFileWithMinData():
        '''
        Create a new avro table with minimum data
        '''
        local_folder_format = '/tmp/{}'#in lambda function we have a temporary folder /tmp
                                        #with storage capacity of 512Mo max, cleaned after the
                                        #lambda function has finished
        avro_table_name = os.environ['AVROTABLENAME']

        avro_table_schema = """{"namespace": "example.avro",
                                "type": "record",
                                "name": "User",
                                "fields": [
                                    {"name": "name", "type": "string"},
                                    {"name": "favorite_number",  "type": ["int", "null"]},
                                    {"name": "favorite_color", "type": ["string", "null"]}
                                ]
                            }"""


        if(not avro_table_name):
            raise ValueError('Invalid avro file name')
        
        file_full_path = local_folder_format.format(avro_table_name)#"/tmp/users.avro"
        
        schema = avro.schema.Parse(avro_table_schema)
        writer = DataFileWriter(open(file_full_path, "wb"), DatumWriter(), schema)
        writer.append({"name": "Toufik", "favorite_number": 512})
        writer.append({"name": "Alyssa", "favorite_number": 256})
        writer.append({"name": "Ben", "favorite_number": 7, "favorite_color": "red"})
        writer.close()
        

    def ReadFromAvroFile():
        '''
        Read the content of existing avro table
        '''
        local_folder_format = '/tmp/{}'
        avro_table_name = os.environ['AVROTABLENAME']
        if(not avro_table_name):
            raise ValueError('Invalid avro file name')

        file_full_path = local_folder_format.format(avro_table_name)#"/tmp/users.avro"
        
        if(os.path.exists(file_full_path)):
            reader = DataFileReader(open(file_full_path, "rb"), DatumReader())
            for user in reader:
                print(user)
            reader.close()
        else:
            raise ValueError('File not found')