import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
from math import sqrt

#some code from lecture notes

def avg(x):
	return sum(x)/len(x)

def stddev(x):
	m = avg(x)
	return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): 
	return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): 
	if stddev(x)*stddev(y) != 0:
		return cov(x, y)/(stddev(x)*stddev(y))


class correlation(dml.Algorithm):
	contributor = 'aliyevaa_bsowens_dwangus_jgtsui'
	reads = ['aliyevaa_bsowens_dwangus_jgtsui.boston_grid_community_values_cellSize1000sqft', 'aliyevaa_bsowens_dwangus_jgtsui.boston_grid_crime_rates_cellSize1000sqft']
	writes=[]

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(correlation.contributor, correlation.contributor)
		data=[]
		
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_community_values_cellSize1000sqft.find():
			cmp=str(elem['cell_center_longitude'])+' '+str(elem['cell_center_latitude'])
			for item in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_crime_rates_cellSize1000sqft.find():
				try:
					if item[cmp]!="0":
						data.append((int(item[cmp]),elem['cell_community_value']))
				except:
					pass
		
		x=[xi for (xi, yi) in data]
		y=[yi for (xi, yi) in data]
		correlation_crime=corr(x,y)
		print(corr(x,y))
		#correlation_crime=-0.1364860758511357
		print("############################################################")
		data_prop=[]
		count=0
		k=0
		for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_community_values_cellSize1000sqft.find():
			cmp=str(elem['cell_center_longitude'])+' '+str(elem['cell_center_latitude'])
			for item in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_properties_cellSize1000sqft.find():
				try:
					if item[cmp]!=["0"] and len(item[cmp])!=0:
						s=0
						k+=1
						for i in item[cmp]:
							s+=int(i)
						r=int(s/len(item[cmp]))		
						data_prop.append((r,elem['cell_community_value']))	
					
				except:
					count+=1
					pass
		
		#print("COUNT", count)
		#print("K",k)
		a=[xi for (xi, yi) in data_prop]	
		b=[yi for (xi, yi) in data_prop]
		correlation_prop=corr(a,b)
		print(correlation_prop)# -0.2011404796534216
		return
	@staticmethod
	def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
		client =  dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate(correlation.contributor,correlation.contributor)
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
		doc.add_namespace('dat', 'http://datamechanics.io/data/') 
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
		doc.add_namespace('log', 'http://datamechanics.io/log/') 	

		this_script = doc.agent('alg:aliyevaa_bsowens_dwangus_jgtsui#correlation',{prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		
		get_liquor_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)	
		doc.wasAssociatedWith(get_liquor_data, this_script)

		doc.usage(get_liquor_data , startTime, None)	
		found = doc.entity('dat:aliyevaa_bsowens_dwangus_jgtsui#correlation', {prov.model.PROV_LABEL:'computing correlation between #of crimes &avg property value in the cell & community score for the cell', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_liquor_data, endTime)
		doc.wasDerivedFrom(found, get_liquor_data, get_liquor_data, get_liquor_data)
		repo.record(doc.serialize()) # Record the provenance document.
		repo.logout()
		return doc
		return

correlation.execute()
doc = correlation.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))				

