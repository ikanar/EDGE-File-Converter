import sys
import os
import json
from itertools import chain, starmap
import xmltodict
import bcpy
import pandas
import traceback


def SetSqlConfig(ConnectionString):
    cs = ConnectionString[:-1].split(';')       
    cs_list=[]
    for i in cs:
       if isinstance(i, str):
            cs_list.append(i.split("="))
    
    sql_config = {}

    for i in enumerate(cs_list):
        sql_config[cs_list[i][0]] = cs_list[i][1]
                
    sql_config["server"] = sql_config.pop("Data_Source", ".")
    sql_config["database"] = sql_config.pop("Initial_Catalog", "ClientEx")
    sql_config["schema"] = sql_config.pop("schema", "src")
    sql_config["username"] = sql_config.pop("User_ID", "")
    sql_config["password"] = sql_config.pop("Password", "")
     
    return sql_config


def convert_files(infiles, outfiles):
    #Secondary function will zip file lists and call JSON converter for each file pair
    for pair in list(zip(infiles, outfiles)):
        xml2json(pair[0], pair[1])


def read_xml(f):
    #Applies the module's parse method to an XML file
    doc = xmltodict.parse(f.read())
    return doc


def xml2json(infile, outfile):
    #Converts the processed XML from a Python dictionary to JSON
    with open(infile, 'rb') as f:
        parsed_file = read_xml(f)

    with open(outfile, 'w') as json_file:
        json_file.write(json.dumps(parsed_file, indent=2))
        json_file.close()


def json2DF(dictionary):
    #Converts JSON file to pandas DataFrame


    def unpack(parent_key, parent_value):
        #Unpack one level of nesting in json file
        
        if isinstance(parent_value, dict):
            for key, value in parent_value.items():
                temp1 = key
                yield temp1, value
        elif isinstance(parent_value, list):
            for i, value in enumerate(parent_value):
                temp2 = str(i)
                yield temp2, value
        else:
            yield parent_key, parent_value    

    while True:
        dictionary = dict(chain.from_iterable(starmap(unpack, dictionary.items())))
        if not any(isinstance(value, dict) for value in dictionary.values()) and \
           not any(isinstance(value, list) for value in dictionary.values()):
            break

    return dictionary


def main(ExecutionID, FileID, FilePath, ConnectionString, isDebug):
    #Main function will open file and execute program
    infiles = os.listdir(FilePath) # list of XML files for processing
    outfiles = [file.rstrip('.xml') + '.json' for file in infiles] # list of outfile names
    data_frames = []
    try:
        convert_files(infiles, outfiles)
        for file in outfiles:
            with open(file, 'r') as f:
                data = json.load(f)
                flat = json2DF(data)
                df = pd.Series(flat).to_frame()
                data_frames.append(df) # list of dataframes (can change to diff 
                # format if necessary)
            
        sql_config = SetSqlConfig(ConnectionString)
        bdf = bcpy.DataFrame(data_frames)
        sql_table = bcpy.SqlTable(sql_config, table=srcTable)
        bdf.to_sql(sql_table, use_existing_sql_table = True, batch_size = 20000)

        return 0

    except:
        print(traceback.format_exc())
        return 999


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5])