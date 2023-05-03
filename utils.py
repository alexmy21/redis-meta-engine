import csv
import importlib
import re
import string
import vocabulary as voc

import os
import yaml
import hashlib
import redis

from cerberus import Validator

doc_0 = {'props': {}}

def importModule(module: str, package: str):
    try:
        return importlib.import_module(module, package=package) 
    except ImportError as err:
        print('Error:', err)
        return None

'''
    This method lists all files in the directory skipping sub-directories.
'''
def listDirFiles(dir: str) -> list|None:
    return [f for f in os.listdir(dir) if os.isfile(os.join(dir, f))]

def listAllFiles(dir: str, ext: str) -> list|None:
    _ext = ext.replace('.', '')
    _list: list = []
    for root, dirs, files in os.walk(dir):
        for file in files:
            if file.endswith(f".{_ext}"):
                _list.append(os.path.join(root, file))

    return _list

def getSchemaFromFile(file_name):     
    with open(file_name, 'r') as file:
        return yaml.safe_load(file)

def schema_name(file_name: str) -> str|None:
    return file_name.split('.')[0]

def idxFileWithExt(schema: str) -> str|None:
    if schema.endswith('.yaml'):
        return
    else:
        return schema + '.yaml' 

def getConfig(file_name: str|None) -> dict|None:
    config: dict | None
    try:
        with open(file_name, 'r') as file:
            config = yaml.safe_load(file)
    except:
        raise RuntimeError(f"Error: Problem with '{file_name}' file.")            
    return config

def getProps(doc: dict, schema_path: str):        
    v = Validator()
    schema = getSchemaFromFile(schema_path)
    p_dict: dict = {}
    if v.validate(schema, doc):
        n_doc = v.normalized(doc_0, doc)
        p_dict = n_doc.get('props').items() 
        return p_dict, n_doc
    else:
        print('Inavalid: {}'.format(schema_path))
        return None, None

# Generates SHA1 hash code from key fields of props 
# dictionary
def sha1(keys: list, doc: dict) -> str|None:

    props = doc[voc.PROPS]
    '''
        Join values from schema header by dropping "props" property
    '''
    sha = dropAndJoin(doc, voc.PROPS, '')
    '''
        Append values from props dictionary with keys from "keys" list
    '''
    for key in keys:
        # Normalize strings by turing to low case
        # and removing spaces
        if props.get(str(key)) != None:
            sha+= str(props.get(str(key))).lower().replace(' ', '')

    m = hashlib.sha1()
    m.update(sha.encode())
    return m.hexdigest()

'''
    Misc methods
'''
def prefix(term: str) -> str:
    if term.endswith(':'):
        return term
    else:
        return term + ':'

def underScore(term: str) -> str|None:
    if term.startswith('_'):
        return term
    else:
        return '_' + term

def csvHeader(file_path: str) -> bool|None:
    try:
        with open(file_path, mode = 'r', encoding="utf-8", errors="backslashreplace") as csvfile:
            return csv.Sniffer().has_header(csvfile.read(4096))
    except:
        print('Error reading file')
        return None

def replaceChars(text:str) -> str|None:
    chars = re.escape(string.punctuation)
    return re.sub(r'['+chars+']', '', text)

def quotes(text: str) -> str:
    return "\'" + text + "\'"

# get redis client
def getRedisClient(host: str, port: int, db: int=0) -> redis.Redis|None:
    try:
        return redis.Redis(host=host, port=port, db=db)
    except:
        print('Error: Redis connection failed.')
        return None
    
# get item from dict by condition and concatenate values into string
def dropAndJoin(dict: dict, skip_keys: str, separator: str) -> str:
    return separator.join([v for k, v in dict.items() if k not in skip_keys])