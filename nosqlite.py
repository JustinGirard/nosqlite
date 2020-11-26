import sys,time
import sqlite3,os,datetime,json
from unqlite import UnQLite

# I'm noticing file pointers are hanging around. If this becomes an issue may need to detect and close the old ones.
#import psutil
#for proc in psutil.process_iter():
#    print proc.open_files()
    
    
class nosqlite():
    def get_db(source='orders',suffix='',directory=''):
        col_path = source + suffix
        base_path = directory
        if not base_path.endswith('/'):
            base_path = base_path +'/'
        if not os.path.isdir(base_path):
            os.mkdir(base_path) 
        db = UnQLite(base_path+col_path+'.db')  # Create an in-memory database.
        col = db.collection(col_path)
        col.create()
        return col,db

    def insert(source='orders',suffix='_history', filterval=None, limit=1000,setval=None,directory=''):
        col,db = nosqlite.get_db(source,suffix,directory)
        db.begin()

        v = {}
        if filterval:
            filterval= dateencode.dumps(filterval)
            filterval = json.loads(filterval)
            v.update(filterval)
        if setval:
            setval= dateencode.dumps(setval)
            setval = json.loads(setval)
            v.update(setval)
        ret= col.store(v)
        db.commit()
        db.close()
        if ret > 0:
            return True
        return False
    
    
    def upsert(source='orders',suffix='_history', filterval=None, limit=1000,setval=None,directory=''):
        col,db = nosqlite.get_db(source,suffix,directory)
        lst = nosqlite.find(filterval,source,suffix,limit,directory)
        db.commit()
        if lst == None or len(lst)==0:
            db.close()
            return  nosqlite.insert(source,suffix, filterval, limit,setval,directory)
        else:    
            v = {}
            db.begin()
            
            for l in lst:
                v = l.copy()
                setval= dateencode.dumps(setval)
                setval = json.loads(setval)
                for k in setval:
                    v[k] = setval[k]
                up= col.update(l['__id'],v)
            #db.commit()
            db.close()
            return True
            

    def find(filterval,source='orders',suffix='_history',  limit=1000,directory=''):
        #import psutil
        #print("OPEN FILES")
        #for proc in psutil.process_iter():
        #    print(proc.open_files())

        
        col,db = nosqlite.get_db(source,suffix,directory)
        #print(col.all())
        if filterval == None:
            filterval = {}
        def filfunc(document):
            ret = True
            ops = ['$lt','$gt','$lte','$gte','$exists','$in']
            for k in filterval:
                if type(filterval[k]) == dict:
                    for op in filterval[k].keys():
                        assert op in ops
                        arg = filterval[k][op]
                        if op == '$lt' and not document.get(k) < arg:
                            ret = False
                        if op == '$gt' and not document.get(k) > arg:
                            ret = False
                        if op == '$gte' and not document.get(k) >= arg:
                            ret = False
                        if op == '$lte' and not document.get(k) <= arg:
                            ret = False
                        if op == '$exists' and ( document.get(k) == None)== arg:
                            ret = False
                        if op == '$in' and not ( document.get(k) in  arg):
                            ret = False

                elif not document.get(k) == filterval[k]:
                    ret = False
            if ret == True:
                return True
            return False
        
        lst = col.filter(filfunc)
        db.close()

        return lst

    def delete(filterval,source='orders',suffix='_history',  limit=1000,directory=''):
        col,db = nosqlite.get_db(source,suffix,directory)
        def filfunc(document):
            ret = True
            ops = ['$lt','$gt','$lte','$gte','$exists']
            for k in filterval:
                if type(filterval[k]) == dict:
                    for op in filterval[k].keys():
                        assert op in ops
                        arg = filterval[k][op]
                        if op == '$lt' and not document.get(k) < arg:
                            ret = False
                        if op == '$gt' and not document.get(k) > arg:
                            ret = False
                        if op == '$gte' and not document.get(k) >= arg:
                            ret = False
                        if op == '$lte' and not document.get(k) <= arg:
                            ret = False
                        if op == '$exists' and ( document.get(k) == None)== arg:
                            ret = False

                elif not document.get(k) == filterval[k]:
                    ret = False        
            if ret == True:
                return True
            return False
        lst = col.filter(filfunc)
        db.begin()
        for l in lst:
            col.delete(l['__id'])
        db.commit()
        db.close()
        
        return True
    #users.delete(0)    

import json
import datetime

class dateencode:
    def loads(dic):
        return json.loads(dic,object_hook=datetime_parser)
    def dumps(dic):
        return json.dumps(dic,default=datedefault)

def datedefault(o):
    if isinstance(o, tuple):
        l = ['__ref']
        l = l + o
        return l
    if isinstance(o, (datetime.date, datetime.datetime,)):
        return o.isoformat()
    
def datetime_parser(dct):
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%S'
    DATE_FORMAT_MICRO = '%Y-%m-%dT%H:%M:%S.%f'
    for k, v in dct.items():
        if isinstance(v, str) and "T" in v:
            try:
                dct[k] = datetime.datetime.strptime(v, DATE_FORMAT)
            except:
                try:
                    dct[k] = datetime.datetime.strptime(v, DATE_FORMAT_MICRO)
                except:
                    pass
    return dct    
