import os

import utils as utl
from vocabulary import Vocabulary as voc
from commands import Commands as cmd

def run(data:dict) -> dict|None:
    _data = {}
    _case = data.get(voc.LABEL)
    print(_case)
    
    print('===> processing . . .')
    _data: dict = process(path, data)
    print('Data', _data)

    return _data