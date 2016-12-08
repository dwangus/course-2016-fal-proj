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

class datasets(dml.Algorithm):
	contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
	reads = []
	writes = ['aliyevaa_bsowens_dwangus_jgtsui.entertainment', 'aliyevaa_bsowens_dwangus_jgtsui.restaurants', 'aliyevaa_bsowens_dwangus_jgtsui.moving_tracks']
	dataSetDict = {}
	dataSetDict.update({'Entertainment':('https://data.cityofboston.gov/resource/qq8y-k3gp.json', 'entertainment', 'Ent. liscences', 'qq8y-k3gp')})
	dataSetDict.update({'Restaurants':('https://data.cityofboston.gov/resource/6c8f-xrde.json', 'restaurants', 'Restaurants', '6c8f-xrde')})
	dataSetDict.update({'Moving Track Permits':('https://data.cityofboston.gov/resource/bzif-fkwd.json', 'moving_tracks', 'Moving Tracks', 'bzif-fkwd')})
	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(datasets.contributor, datasets.contributor)
		link= ['https://data.cityofboston.gov/resource/qq8y-k3gp.json', 'https://data.cityofboston.gov/resource/6c8f-xrde.json','https://data.cityofboston.gov/resource/bzif-fkwd.json'] 
		
		limit = 50000
		offset = 0
		repo.dropPermanent("entertainment")
		repo.createPermanent("entertainment")
		x = 50000
		while x == 50000:
			response = urllib.request.urlopen(link[0] + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode("utf-8")
			r=json.loads(response)
			repo['aliyevaa_bsowens_dwangus_jgtsui.entertainment'].insert_many(r)
			offset += 50000
			x = len(r)
		print("Transforming entertainment dataset...")
                                
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment.find():
			if elem['location']!='NULL':
				coor=elem['location']
				coor=coor.split(',')
				long=coor[0][1:]
				lat=coor[1][1:-1]
				repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment.update({'_id': elem['_id']}, {'$set': {'location': {'type': 'Point', 'coordinates': [float(long),float(lat)]}}})
			else:
				repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment.remove(elem)
		repo.aliyevaa_bsowens_dwangus_jgtsui.entertainment.create_index([('location', '2dsphere')])
		
		repo.dropPermanent("restaurants")
		repo.createPermanent("restaurants")
		x = 50000
		offset=0
		while x == 50000:
			response = urllib.request.urlopen(link[1] + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode("utf-8")
		
			r=json.loads(response)
			
			repo['aliyevaa_bsowens_dwangus_jgtsui.restaurants'].insert_many(r)
			offset += 50000
			x = len(r)
		print("Transforming restaurants dataset...")
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.restaurants.find():
			if elem['location_1']!='NULL' and 'latitude' in elem['location_1']:
				lat=elem['location_1']['latitude']
				long=elem['location_1']['longitude']
				repo.aliyevaa_bsowens_dwangus_jgtsui.restauratns.update({'_id': elem['_id']}, {'$set': {'location': {'type': 'Point', 'coordinates': [float(long),float(lat)]}}})
			else:
				repo.aliyevaa_bsowens_dwangus_jgtsui.restauratns.remove(elem)
		repo.aliyevaa_bsowens_dwangus_jgtsui.restaurants.create_index([('location', '2dsphere')])
	
		repo.dropPermanent("moving_tracks")
		repo.createPermanent("moving_tracks")
		x = 50000
		offset=0
		while x == 50000:
			response = urllib.request.urlopen(link[2] + '?$limit=' + str(limit) + '&$offset=' + str(offset)).read().decode("utf-8")
			r=json.loads(response)
			repo['aliyevaa_bsowens_dwangus_jgtsui.moving_tracks'].insert_many(r)
			offset += 50000
			x = len(r)
		print("Transforming moving_tracks dataset...")
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.moving_tracks.find():
			if 'location' in elem and 'latitude' in elem['location']:
				lat=elem['location']['latitude']
				long=elem['location']['longitude']
				repo.aliyevaa_bsowens_dwangus_jgtsui.moving_tracks.update({'_id': elem['_id']}, {'$set': {'location': {'type': 'Point', 'coordinates': [float(long),float(lat)]}}})
			else:
				repo.aliyevaa_bsowens_dwangus_jgtsui.moving_tracks.remove(elem)
		repo.aliyevaa_bsowens_dwangus_jgtsui.moving_tracks.create_index([('location', '2dsphere')])
		print("FINISHED")
		repo.logout()
		endTime = datetime.datetime.now()
		return {"start":startTime, "end":endTime}
	@staticmethod
	def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime = None):
		client =  dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(datasets.contributor, datasets.contributor)
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/')
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/')
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
		this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#datasets',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		for key in datasets.dataSetDict.keys():
			resource = doc.entity('bdp:' + datasets.dataSetDict[key][3], {'prov:label':datasets.dataSetDict[key][2], prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
			get_something = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
			doc.wasAssociatedWith(get_something, this_script)
			something = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#' + key, {prov.model.PROV_LABEL:datasets.dataSetDict[key][2], prov.model.PROV_TYPE:'ont:DataSet'})
			doc.wasAttributedTo(something, this_script)
			doc.wasGeneratedBy(something, get_something, endTime)
			doc.wasDerivedFrom(something, resource, get_something, get_something, get_something) 
		repo.record(doc.serialize())
		repo.logout()
		return doc
datasets.execute()
doc=datasets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

