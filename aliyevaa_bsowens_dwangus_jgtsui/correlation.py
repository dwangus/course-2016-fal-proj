import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import time
import math
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
    reads = ['aliyevaa_bsowens_dwangus_jgtsui.boston_grid_community_values_cellSize1000sqft',\
             'aliyevaa_bsowens_dwangus_jgtsui.boston_grid_crime_rates_cellSize1000sqft',\
             'aliyevaa_bsowens_dwangus_jgtsui.boston_grid_properties_cellSize1000sqft']
    writes = []

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate(correlation.contributor, correlation.contributor)

        ###David-added
        communityValCellKeys = []
	
        crime_data = []
        crimeHeatmapValues = []
        for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_community_values_cellSize1000sqft.find():
            cmp = str(elem['cell_center_longitude'])+' '+str(elem['cell_center_latitude'])
	    
            ###David-added
            thisCellCommunityValue = elem['cell_community_value']
            communityValCellKeys.append((cmp, thisCellCommunityValue))

	    ###Asselya's Code
	    #'''
            #Idk why you did this, isn't this really inefficient? Can't you just query MongoDB using the exact key as the index?
            for item in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_crime_rates_cellSize1000sqft.find():
                try:
                    if item[cmp] != "0":
                        crime_data.append( ( int( item[cmp] ) , thisCellCommunityValue ) )
                        crimeHeatmapValues.append((cmp, int( item[cmp] )))
                except:
                    pass
            #'''
            ###
		
        x=[xi for (xi, yi) in crime_data]
        y=[yi for (xi, yi) in crime_data]
        correlation_crime = corr(x,y)
        print("Crime-to-Community Correlation Score: ", corr(x,y))
        #correlation_crime = -0.1364860758511357
	
        print("############################################################")
	
        property_val_data = []
        count = 0
        k = 0 #k is the number of actually parsed items


        ###David-added
        propertyDict = {}
        print(repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_properties_cellSize1000sqft.count())
        for obj in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_properties_cellSize1000sqft.find():
            for key in obj.keys():
                if key != '_id':
                    valsList = [float(val) for val in obj[key] if float(val) > 1]
                    if len(valsList) > 0:
                        #propertyDict[key] = sum(valsList)/len(valsList)
                        propertyDict[key] = math.log(sum(valsList)/len(valsList))###Since the values range from literally 6,200 to 1,291,777,584
                    else:
                        propertyDict[key] = 0

        propertValHeatmapValues = []
        for tup in communityValCellKeys:
            cmp = tup[0]
            cellCommValue = tup[1]
            
            if cmp not in propertyDict:
                continue
            
            avgPropertyValue = propertyDict[cmp]
            if avgPropertyValue > 0:
                property_val_data.append( (avgPropertyValue , cellCommValue ) )
                propertValHeatmapValues.append((cmp, avgPropertyValue))
        ###

        
	###Asselya's Commented-Out Code
        '''
        #Idk why you did this, isn't this really inefficient? Can't you just query MongoDB using the exact key as the index?
	for elem in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_community_values_cellSize1000sqft.find():
	    cmp = str(elem['cell_center_longitude'])+' '+str(elem['cell_center_latitude'])
	    for item in repo.aliyevaa_bsowens_dwangus_jgtsui.boston_grid_properties_cellSize1000sqft.find():
		try:
		    if item[cmp] != ["0"] and len(item[cmp]) != 0:
			s = 0
			k += 1
			for i in item[cmp]:
			    s += int(i)
			r = int(s / len(item[cmp]))
			property_val_data.append((r,elem['cell_community_value']))	
		except:
                    count+=1
		    pass
        #'''
	###
	
        #print("COUNT", count)
        #print("K", k)

        def printRange(arr):
            print("{} to {}".format(min(arr), max(arr)))

        #print(property_val_data[:20])
        #for v in property_val_data:
        #    if v[0] > 10**9:
        #        print(v)
        
        a = [xi for (xi, yi) in property_val_data]
        b = [yi for (xi, yi) in property_val_data]
        
        printRange(a)
        printRange(b)
        
        correlation_propertyVal = corr(a,b)
        print("Average-Property-Value-to-Community Correlation Score: ", correlation_propertyVal)# -0.2011404796534216

        def createTxtFiles(tuples, name):
            with open(name, 'w') as f:
                for t in tuples:
                    gpsLatLong = t[0].split()
                    tupLat = gpsLatLong[1]
                    tupLong = gpsLatLong[0]
                    tupVal = t[1]
                    f.write('{lat:' + str(tupLong) + ',long:' + str(tupLat) + ',count:' + str(tupVal) + "},\n")
                f.close()

        createTxtFiles(crimeHeatmapValues, 'crimeHeatmapValues.txt')
        createTxtFiles(propertValHeatmapValues, 'propertyHeatmapValues.txt')
        createTxtFiles(communityValCellKeys, 'communityHeatmapValues.txt')
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

#correlation.execute()
#doc = correlation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))				

def main():
    print("Executing: correlation.py")
    correlation.execute()
    doc = correlation.provenance()
