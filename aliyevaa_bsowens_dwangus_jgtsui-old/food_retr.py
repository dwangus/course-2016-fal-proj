import sys
import requests
import dml
import json
import time
import prov.model
import datetime
import uuid
import ast
import urllib.request


class prop_retr(dml.Algorithm):
    # Set up the database connection.
    contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
    reads = []
    writes = ['aliyevaa_bsowens_dwangus_jgtsui.food_retr']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(prop_retr.contributor, prop_retr.contributor)
        link = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        limit = 50000
        offset = 0
        repo.dropPermanent("food_licenses")
        repo.createPermanent("food_licenses")
        x = 50000
        while x == 50000:
            response = urllib.request.urlopen(link + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode(
                "utf-8")
            r = json.loads(response)
            repo['aliyevaa_bsowens_dwangus_jgtsui.food_licenses'].insert_many(r)
            offset += 50000
            x = len(r)

        print("Transforming food_licenses dataset...")

        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.food_licenses.find():
            if elem['location']['coordinates'][0] != "0" and elem['location']['coordinates'][0] != "0" and \
                            elem['location']['coordinates'][1] != "#N/A" and elem['location']['coordinates'][1] != "#N/A":
                repo.aliyevaa_bsowens_dwangus_jgtsui.food_licenses.update({'_id': elem['_id']}, {'$set': {
                    'location': {'type': 'Point', 'coordinates': [float(elem['location']['coordinates'][0]), float(elem['location']['coordinates'][0])]}}})
        repo.aliyevaa_bsowens_dwangus_jgtsui.food_licenses.create_index([('location', '2dsphere')])

        repo.aliyevaa_bsowens_dwangus_jgtsui.food_licenses.create_index([('location', '2dsphere')])
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(prop_retr.contributor, prop_retr.contributor)
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#food_retr',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        get_liquor_data = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_liquor_data, this_script)
        found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#food_licenses',
                           {prov.model.PROV_LABEL: '', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_liquor_data, endTime)
        repo.record(doc.serialize())  # Record the provenance document.
        repo.logout()
        return doc


prop_retr.execute()
doc = prop_retr.provenance()
#print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
