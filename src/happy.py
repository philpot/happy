#!/usr/bin/python

import csv
from pymongo import MongoClient

mongo = MongoClient()
db = mongo.happydb
city = db.city

print mongo, db, city

cityPathname = "/Users/philpot/Documents/project/happy/gdata/Resto-cities.tsv"
def loadCity():
    with open(cityPathname, 'rb') as tsvin:
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
            city.insert_one(d)

resto = db.resto

LINE = None


restoPathname = "/Users/philpot/Documents/project/happy/gdata/Resto-harvested.tsv"
def loadResto():
    """Hooters 3340 Mowry Ave, Fremont CA      94538   (510) 797-9464  37.552544       -121.98513      Hooters 100     95351"""
    global LINE
    with open(restoPathname, 'rb') as tsvin:
        tsvin = csv.reader(tsvin, delimiter='\t')
        for line in tsvin:
            LINE = line
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
            except:
                pass
            try:
                d["lon"] = float(lon)
            except:
                pass
            try:
                d["zip"] = "%05d" % int(zip)
            except:
                pass
            try:
                d["seedZip"] = "%05d" % int(seedZip)
            except:
                pass
            print "inserting %s" % (d)
            resto.insert_one(d)
