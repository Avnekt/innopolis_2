import sqlite3
import json
import re

file_names = [
    'notik.json',
    'spcomputer.json',
    'topcomputer.json'
]
for file in file_names:
    with open(file, 'r') as in_f:
        text = in_f.readlines()
        text = ' '.join([elem.replace('\n', '') for elem in text])
        text = json.loads(text)
for item in text[0:10]:
    print(text.index(item), ''.join(re.findall(r'(\d+[\.?|\,?]?\d?)["|\'\']', str(item.keys()))[0]))
    print(item)