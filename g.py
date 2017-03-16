import json
import os
import codecs
filename = 'tax.xml.json'
with open(filename, 'r+') as f:
    data = json.load(f)
    tmp = data["Document"]
    data["Document"].append({'name':'badrmansour'})
#
# os.remove(filename)
with open('new.json', 'w') as f:
    f.seek(0)  # rewind


    json.dump(data, f, indent=4)
    f.truncate()