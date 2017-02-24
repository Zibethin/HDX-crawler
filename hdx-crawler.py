"""
CKAN API documentation: http://docs.ckan.org/en/latest/api/
Python CKAN library: https://github.com/ckan/ckanapi
"""

import ckanapi, json, sys, time, urllib
from itertools import islice

DELAY = 5
"""Time delay in seconds between datasets, to give HDX a break."""

CKAN_URL = "https://data.humdata.org"
"""Base URL for the CKAN instance."""
i = 0
#The ultimate JSON file
JSON = {}
hxl_header_row = 1

# format for output: the dictionary key is the file name, as it should be unique:
# { some_file_name : { 
#                    url: "/whatever/bla.csv",
#                    all_hxl_tags: {
#                         #affected: {
#                             line_1: 3, 
#                             line_2: 6, 
#                             line_3: 398
#                                     },
#                         #lorem_ipsum: {lines},
#                         #blabla+bla: {lines}
#                                   }
#                      } 
#   name_of_file_2 : { url:"", all_hxl_tags: etc...}, 
#   ... }

def populateJSON(resource, csvData):
    # escaping the "/" in the url
    #url = str(resource["url"]).replace("/", "\/")
    JSON[resource["name"]] = {}
    JSON[resource["name"]]["url"] = str(resource["url"])
    JSON[resource["name"]]["all_hxl_tags"] = csvData

def readCsv(csvLocation):
    try:
        content = urllib.request.urlopen(csvLocation)
        #csv = csv.reader(content, delimiter=',') 
    except:
        print("-------!--- urllib not working\n")
    count = 0

    # to do: make count==1 dynamic (find line starting with #, if none return no valid HXL found)
    temp_dataset = []
    for line in content:
        clean_line = line.decode("utf-8").replace("\n","")
        split_line = clean_line.split(",")
        temp_dataset.append(split_line)
        if count == 4:
            break
        count += 1
    # create dictionary from data
    data = {}
    i = 0
    try:
        for hxl_tag in temp_dataset[1]:
            data[str(hxl_tag)] = {}
            data[str(hxl_tag)]["line1"] = str(temp_dataset[2][i])
            data[str(hxl_tag)]["line2"] = str(temp_dataset[3][i])
            data[str(hxl_tag)]["line3"] = str(temp_dataset[4][i])
            i += 1
    # if there is an error, return data={} so the code can continue on to other datasets
    except:
        data = {}
        print("Error assigning data to json from csv")
    print("data = ",data)
    return data

# find datasets tagged HXL
def find_hxl_datasets(start, rows):
    """Return a page of HXL datasets."""
    return ckan.action.package_search(start=start, rows=rows, fq="tags:hxl")


# Open a connection to HDX
ckan = ckanapi.RemoteCKAN(CKAN_URL)
result_start_pos = 0
result_page_size = 25

result = find_hxl_datasets(result_start_pos, result_page_size)
result_total_count = result["count"]
print(result["count"])
print(result["results"][0]["title"])

#text_file = open("Output.txt", "w")
#text_file.write(json.dumps(result))
#text_file.close()

package = result["results"]

# Iterate through all the datasets ("packages") and resources on HDX
for package_id in package:
    # package = ckan.action.package_show(id=package_id)
    print("Package: {}".format(package[i]["title"]))

    # for each resource in a package (some packages have multiple csv files for example), print the name, url and format
    for resource in package[i]["resources"]:
        print("  {}".format(resource["name"]))
        print("    {}".format(resource["url"]))
        print ("format: ",resource["format"])

        # if the resource is a csv then print content
        if resource["format"] == "CSV":
            try:
                csvData = readCsv(resource["url"])
            except:
                print("Error loading csv :(\n")
            try:
                populateJSON(resource, csvData)
            except:
                print("Error adding data to Json for file ", resource["name"], " :'( \n")

    print("")
    time.sleep(DELAY) # give HDX a short rest
    i += 1
    if i == 2:
        text_file = open("json.txt", "w")
        text_file.write(json.dumps(JSON))
        text_file.close()
        break
    
# end
