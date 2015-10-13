#!/usr/bin/python

import sys
import csv
from pymongo import MongoClient

mongo = MongoClient()
db = mongo.happydb

city = db.cities
db.cities.ensure_index( [ ("loc", "2dsphere") ] )

resto = db.resto
db.resto.ensure_index( [ ("loc", "2dsphere") ] )

cityPathname = "/Users/philpot/Documents/project/happy/gdata/Resto-cities.tsv"

def loadCity(pathname=cityPathname):
    with open(pathname, 'rb') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        for line in tsvin:
            (rank, typeCode, name, pop, prevpop, delta, coreCity, coreState, coreZip) = line
            rank = int(rank)
            typeCode = int(typeCode)
            pop = int(pop)
            prevpop = int(prevpop)
            delta = None
            try:
                delta = float(delta.strip('%'))
            except:
                pass
            d = {"rank": rank,
                 "typeCode": typeCode,
                 "cityType": {1: "CSA", 2: "MSA", 3: "uSA"}.get(typeCode, None),
                 "population": pop,
                 "previousPopulation": prevpop,
                 "delta": delta,
                 "coreCity": coreCity,
                 "coreState": coreState}
            try:
                coreZipInt = int(coreZip)
                coreZip = "%05d" % coreZipInt
                d["coreZip"] = coreZip
            except:
                pass
            print "inserting %s" % (d)
            # city.insert_one(d)
            city.update_one(d, {"$set": d}, True)

cityLocationPathname = "/Users/philpot/Documents/project/happy/geocode/aux.csv"

def loadCityLocations(pathname=cityLocationPathname):
    with open(pathname, 'rb') as csvin:
        csvin = csv.reader(csvin)
        for line in csvin:
            try:
                (cityState, latitude, longitude) = line
                (cityName, stateName) = cityState.split(',', 1)
                cityName = cityName.strip()
                stateName = stateName.strip()
                latitude = float(latitude)
                longitude = float(longitude)
            except:
                continue
            loc = {"type": "Point",
                   "coordinates": [longitude, latitude]}
            try:
                print >> sys.stderr, "Try to update %s with %s" % ({"coreCity": cityName, "coreState": stateName}, 
                                                                   loc)
                v = city.update({"coreCity": cityName, "coreState": stateName}, 
                                {"$set": {"loc": loc}},
                                False)
                print "Update_one result %s" % v
            except Exception as e:
                print >> sys.stderr, "Not able to update based on %s" % (cityState)
                print >> sys.stderr, "[%s]" % e


restoPathname = "/Users/philpot/Documents/project/happy/gdata/Resto-harvested.tsv"

def loadResto(pathname=restoPathname):
    """Hooters 3340 Mowry Ave, Fremont CA      94538   (510) 797-9464  37.552544       -121.98513      Hooters 100     95351"""
    with open(pathname, 'rb') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        for line in tsvin:
            (name, streetAddress, city, state, zip, phone, lat, lon, seedName, seedScore, seedZip) = line
            d = {"name": name,
                 "streetAddress": streetAddress,
                 "city": city,
                 "state": state,
                 "phone": phone,
                 "seedName": seedName,
                 "seedScore": seedScore}
            try:
                d["lat"] = float(lat)
            except Exception as e:
                print e
            try:
                d["lon"] = float(lon)
            except Exception as e:
                pass
            try:
                d["loc"] = {"type": "Point",
                            "coordinates": [d["lon"], d["lat"]]}
            except Exception as e:
                pass
            try:
                d["zip"] = "%05d" % int(zip)
            except Exception as e:
                pass
            try:
                d["seedZip"] = "%05d" % int(seedZip)
            except Exception as e:
                pass
            print "inserting %s" % (d)
            # resto.insert_one(d)
            resto.update_one(d, {"$set": d}, True)

def query(radiusMeters=10000, limit=None,
          lat=None, lon=None,
          city=None, state=None):
    def findCands(lon, lat):
        # this syntax is not correct
        q = {"loc": {"$near": {"$geometry": {"type": "Point", "coordinates": [lon, lat]}},
                    "maxDistance": radiusMeters}}
        if limit:
            q["loc"]["limit"] = limit
        return db.resto.find(q)
    if lat and lon:
        cands = findCands(lon, lat)
    elif city and state:
        centerPoint = db.cities.find_one({"coreCity": city, "coreState": state}, {"loc": 1, "_id": 0})
        lat, lon = centerPoint.get("coordinates")
        cands = findCands(lon, lat)
    else:
        return None
    print cands
    for cand in cands:
        print cand

# for doc in db.resto.find({"loc": {"$geoNear": {"type": "Point", "coordinates": [-95.88642, 36.010754]}}}).limit(10):
#     print doc

