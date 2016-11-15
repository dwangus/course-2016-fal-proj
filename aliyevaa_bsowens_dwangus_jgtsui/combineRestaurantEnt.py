import json
import dml
import prov.model
import datetime
import uuid
import time
import re # regular expression
import ast
from geopy.distance import vincenty as vct
from bson.code import Code


class combineRestaurantEnt(dml.Algorithm):
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'

    oldSetExtensions = ['food_licenses', 'entertainment_licenses']

    titles = ['Distinct Entertainment Licenses (without restaurants)']

    setExtension = 'entertainment_licenses_no_restaurants'

    reads = ['aliyevaa_bsowens_dwangus_jgtsui.' + dataSet for dataSet in oldSetExtensions]
    writes = 'aliyevaa_bsowens_dwangus_jgtsui' + setExtension

    dataSetDict = {}
    dataSetDict[setExtension] = writes, titles[0]

    @staticmethod
    def execute(trial=False):

        """
        rep = {'av': '', \
               'ave': '', \
               'bi': '', \
               'bl': '', \
               'dr': '', \
               'hw':'', \
               'pk': '', \
               'pl': '', \
               'pw': '', \
               'pz': '', \
               'rd': '', \
               'ro': '', \
               'saint': '', \
               'st': '', \
               'sq': '', \
               'wh': '', \
               'wy': ''}

        rep = dict((re.escape(k), v) for k, v in rep.iteritems())
        pattern = re.compile("|".join(rep.keys()))
        """
        start = time.time()
        startTime = datetime.datetime.now()
        print("Starting execution of script @{}".format(startTime))

        # set up database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(combineRestaurantEnt.contributor, combineRestaurantEnt.contributor)
        myrepo = repo.aliyevaa_bsowens_dwangus_jgtsui

        key = combineRestaurantEnt.setExtension # == entertainment_licenses_no_restaurants
        repo.dropPermanent(key)
        repo.createPermanent(key)

        print("Now copying  and transforming {} entries from food_licenses to create new dataset {}.\n".format(myrepo['food_licenses'].count(), combineRestaurantEnt.setExtension))

        myrepo[key].insert_many(myrepo['entertainment_licenses'].find())

        newSet = myrepo[key]
        newSet.create_index([('location', '2dsphere')])

        restaurants = myrepo['food_licenses']

        longLats = {}
        locations = {}
        for restaurant in restaurants.find(modifiers={'$snapshot': True}):
            if 'location' in restaurant.keys():
                address = restaurant['address'].split(" ")

                st_no = address[0]
                st_name = " ".join(address[1:]).lower().replace('st', '').replace(' ','')
                city =  (restaurant['city'].lower()).replace(' ', '')

                ###########################################################################
                # "location" : { "coordinates" : [ -71.0792, 42.38254 ], "type" : Point}  #
                #            where coordinates are in the form long, lat                  #
                ###########################################################################

                long = round(float(restaurant['location']['coordinates'][0]), 3)
                lat = round(float(restaurant['location']['coordinates'][1]), 3)

                ###########################################################################
                # name = (restaurant['businessname']).lower                               #
                # name = re.sub(r'[\s.\'\",]', '', restaurant['businessname'].lower())    #
                # name = name.replace('&', 'and')                                         #
                # name = name.replace('@', 'at')                                          #
                # The following line is equivalent to these four preceding lines:         #
                ###########################################################################

                name = (re.sub(r'[\s\'\",.;:]', '', restaurant['businessname'].lower()).replace('&', 'and')).replace('@', 'at')

                longLats[long] = {lat : name}
                locations[city] = {st_name : {st_no : name}}


        print('Generating new {} dataset...'.format(key))

        # go through food_licenses collection, create two dictionaries:
        # {long : {lat : name}}
        # {city : {street : {st_no : name}}}

        # then, go through entertainment licenses, and only keep the entertainment
        # licenses that didn't have a match to either of those dictionaries

        for doc in newSet.find(modifiers={"$snapshot": True}):
            if 'location' in doc.keys() and 'location' is not None:
                st_no_ent = doc['stno']

                st_name_ent = (doc['address'][1:].lower()).replace('av', '').replace('ave', '').replace( 'bi', '').replace('bl', '')
                st_name_ent = st_name_ent.replace('dr', '').replace('rd', '').replace('ro', '').replace('saint', '').replace('st','')
                st_name_ent = st_name_ent.replace('sq', '').replace('wh','').replace('wy','').replace(' ', '')

                city_ent = (doc['city'].lower()).replace(' ', '')

                long = round(float(doc['location']['coordinates'][0]), 3)
                lat = round(float(doc['location']['coordinates'][1]), 3)

                ###########################################################################
                # name = (restaurant['businessname']).lower                               #
                # name = re.sub(r'[\s.\'\",]', '', restaurant['businessname'].lower())    #
                # name = name.replace('&', 'and')                                         #
                # name = name.replace('@', 'at')                                          #
                # The following line is equivalent to these four preceding lines:         #
                ###########################################################################

                name = (re.sub(r'[\s\'\",.;:]', '', doc['dbaname'].lower()).replace('&', 'and')).replace('@', 'at')


        endTime = datetime.datetime.now()
        print("Creating {} took {} seconds.".format(key, time.time() - start))
        repo.logout()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''
        Create the provenance document describing everything happening
        in this script. Each run of the script will generate a new
        document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(combineRestaurantEnt.contributor, combineRestaurantEnt.contributor)

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#combineRestaurantEnt',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        for key in combineRestaurantEnt.dataSetDict.keys():
            get_something = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_something, this_script)
            something = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#' + key,
                                   {prov.model.PROV_LABEL: combineRestaurantEnt.dataSetDict[key][1],
                                    prov.model.PROV_TYPE: 'ont:DataSet'})
            doc.wasAttributedTo(something, this_script)
            doc.wasGeneratedBy(something, get_something, endTime)


        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()

        return doc


combineRestaurantEnt.execute()
doc = combineRestaurantEnt.provenance()
# print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof