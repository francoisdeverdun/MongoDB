import os
import json
import time
from pymongo import MongoClient


def build_database(sites):
    # final Json file to sent to DB
    final_data = []

    # meta file definitino
    meta_file = open(os.getcwd() + '\\all-data\\meta\\all_sites.csv','r')
    meta_list = list(meta_file)

    # data folder definition
    data_folder = os.getcwd() +  "\\all-data\\csv\\"

    # get each sites
    for i, meta_line in enumerate(meta_list[1:]):
        begin_site =time.time()
        # Get each lines of the sites file
        meta = meta_line.split(",")

        # build the basis JSON of the site
        site_data = {
            "id":meta[0],
            "industry":meta[1],
            "sub_industry":meta[2],
            "so_ft":meta[3],
            "lat":meta[4],
            "lng":meta[5],
            "time_zone":meta[6],
            "tz_offset":meta[7].split("\n")[0]
        }

        # get the data file of the curent site
        data_file = open (data_folder + meta[0] +".csv")
        data_list = list(data_file)

        # build the data section of the site Json
        site_data["data"] = []

        # get all the data for the current site
        for j, data_line in enumerate(data_list[1:]):
            data = data_line.split(",")
            single_data = {
                "timestamp": data[0],
                "dttm_utc":data[1],
                "value":data[2],
                "estimated":float(data[3]),
                "anomaly":data[4].split("\n")[0]
                }
            site_data["data"].append(single_data)

        # save the result in the final json
        final_data.append(site_data)

        # insert site data in the mango DB
        result = sites.insert_many(final_data)
        # reset the json file
        final_data = []

        # Logger follow up
        duration_site = time.time()-begin_site
        print("site ",meta[0]," processed in ",duration_site)
        print("action in mango DB: ",result)


if __name__=="__main__":
    begin =time.time()

    # Connect to mango client
    client = MongoClient('localhost', 27017)

    # delete previous database
    client.drop_database('database')

    # create new database
    db = client.database
    sites = db.sites

    # build the Json and database in mango
    build_database(sites)

    # get duration of process
    duration = time.time()-begin
    print("Programs ends at {}. Total duration is {}".format(time.time(),duration))
