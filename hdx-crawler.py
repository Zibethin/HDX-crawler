"""
CKAN API documentation: http://docs.ckan.org/en/latest/api/
Python CKAN library: https://github.com/ckan/ckanapi
"""

import ckanapi, json, sys, time, urllib
from itertools import islice

DELAY = 5
"""Time delay in seconds between datasets, to give HDX a break."""

CKAN_URL = 'https://data.humdata.org'
"""Base URL for the CKAN instance."""
i = 0

def readCsv(csvLocation):
    try:
        content = urllib.request.urlopen(csvLocation) 
    except:
        print("-------!--- urllib not working\n")
    count = 0
    for line in content: 
        count += 1
        print (line)
        if count == 4:
            print("")
            break
    

def find_hxl_datasets(start, rows):
    """Return a page of HXL datasets."""
    return ckan.action.package_search(start=start, rows=rows, fq="tags:hxl")


# Open a connection to HDX
ckan = ckanapi.RemoteCKAN(CKAN_URL)
result_start_pos = 0
result_page_size = 25
result = find_hxl_datasets(result_start_pos, result_page_size)
result_total_count = result['count']
print(result['count'])
print(result['results'][0]['title'])
#text_file = open("Output.txt", "w")
#text_file.write(json.dumps(result))
#text_file.close()
package = result['results']
# Iterate through all the datasets ("packages") and resources on HDX
for package_id in package:
    #package = ckan.action.package_show(id=package_id)
    print("Package: {}".format(package[i]['title']))
    for resource in package[i]['resources']:
        print("  {}".format(resource['name']))
        print("    {}".format(resource['url']))
        print ("format: ",resource['format'])
    print("")
    time.sleep(DELAY) # give HDX a short rest
    i += 1
    if i == 3:
        break
    if resource['format'] == "CSV":
        try:
            readCsv(resource['url'])
        except:
            print("Error loading csv :(\n")

# end
