import pyspark
import pymongo
import csv
import json
import io


def filter_content_by_filetype(filename_content_pairs,
                               extension):
    '''
    Return the value (file content) of
    filename_content_pairs includes extension.
    '''
    return filename_content_pairs.values()\
                                 .flatMap(lambda x: x)\
                                 .filter(lambda x: extension in x[0])\
                                 .values()


def add_key_value(x, key, value):
    '''
    Add "key":"value" on the dictionary/
    '''
    non_empty_dict = {k: v for k, v in x.items() if v is not None and v != ''}
    non_empty_dict[key] = value
    return non_empty_dict


class SFRecords:
    def __init__(self, sc, data_dir):
        '''
        In __init__(self, data_dir),
        it sets two attributes.

        self.date_content : pair RDDs of
        (date string in file path, (filename, contents))

        self.unique_dates  : an RDD including distinct date string
        in file path in ascending order.
        '''
        data = sc.wholeTextFiles(f'{data_dir}*')
        self.date_content = data.map(lambda x: (x[0].split('/')[-2],
                                                (x[0].split('/')[-1], x[1])))\
                                .partitionBy(6)\
                                .cache()

        self.unique_dates = self.date_content.keys()\
                                .distinct()\
                                .sortBy(lambda x: x)\
                                .cache()

        self.sc = sc
        self.aggregates = list()

    def filter_by_date(self, date):
        '''
        Return an RDD of key-value pair including (date,
        [(file_name1, file_contents1), (file_name2, file_contents2)])
        when there are two files in the date folder.
        If not, return an empty RDD.
        '''
        if (date in self.unique_dates.collect() and
            self.date_content
                .filter(lambda x: x[0] == date)
                .count() == 2):
            return (self.date_content
                        .filter(lambda x: x[0] == date)
                        .groupByKey()
                        .mapValues(lambda x: list(x)))
        else:
            return self.sc.parallelize([])

    def check_aggregates(self, date):
        for dic in self.aggregates:
            for k, v in dic.items():
                if (v == date):
                    return v

    def return_law_weather_aggregate(self, date):
        '''
        Return an RDD of dictionaries that includes law and weather data.
        If it is not in self.aggregates yet, append the list with
        a dictionary {'date' : date, 'aggregated_rdd' : RDDs of dictionaries}.
        '''
        if (date in self.unique_dates.collect()):
            filename_content_pairs = self.filter_by_date(date)

            if (filename_content_pairs.count() != 0):
                csv_content = filter_content_by_filetype(filename_content_pairs,
                                                         '.csv')
                json_content = filter_content_by_filetype(filename_content_pairs,
                                                          '.json')
                weather = json.load(io.StringIO(json_content.first()))
                aggregated_rdd = self.sc.parallelize(
                    csv.DictReader(io.StringIO(csv_content.first())))
                aggregated_rdd = aggregated_rdd.map(lambda x:
                                                    add_key_value(x,
                                                                  'weather',
                                                                  weather))
                if self.check_aggregates(date) is None:
                    self.aggregates.append(dict({'date': date,
                                                 'aggregated_rdd': aggregated_rdd}))
                return aggregated_rdd

    def add_aggregates_to_mongo(self,
                                date,
                                mongodb_collection):
        '''
        Using mongodb_collection, a class object of MongoDBCollection,
        insert aggregates from the given date
        returned from return_law_weather_aggregate().

        Covert the key, 'id' to '_id' for each aggregate to
        use the value of 'id' as a primary key.
        If there is a document with the same '_id' already,
        skip the process and insert the next one.
        '''
        rdd = self.return_law_weather_aggregate(date)
        
        for i in rdd.map(lambda x: x['id']).collect():
            temp =  rdd.filter(lambda x: x['id']==i).collect()[0]
            temp['_id'] = i
            # print(temp)
            count = 0
            date_regex = "^"+ date
            for i in mongodb_collection.collection.find({'_id':i}):
                count += 1
            if count > 0:
                continue
            else: 
                mongodb_collection.collection.insert_one(temp) 

class MongoDBCollection:
    def __init__(self, ip_address, database_name, collection_name):
        '''
        Using ip_address, database_name, collection_name,
        initiate the instance's attributes including ip_address,
        database_name, collection_name, client, db and collection.

        For pymongo, see more details in the following.
        https://pymongo.readthedocs.io
        '''
        self.ip_address = ip_address
        self.database_name = database_name
        self.collection_name = collection_name

        self.client = pymongo.MongoClient()
        self.db = self.client[self.database_name]
        self.collection = self.db[self.collection_name]
    
    def return_db(self):
        '''
        Return db which is the database in the client
        '''
        return self.db
        
    def return_collection(self):
        '''
        Return db which belongs to the db.
        '''
        return self.collection

    def return_num_docs(self, query):
        '''
        Return the number of documents satisfying the given query.
        '''
        return self.collection.count_documents(query)

    def drop_collection(self):
        '''
        Drop the collection
        '''
        return self.collection.drop()

    def find(self, query, projection):
        '''
        Return an iteratatable using query and projection.
        '''
        cur = self.collection.find(filter=query, projection=projection)
        return cur


    def insert_one(self, doc):
        '''
        Insert the given document
        '''
        self.collection.insert_one(doc)


    def insert_many(self, docs):
        '''
        Insert the given documents
        '''
        self.collection.insert_many(docs)    

    def update_many(self, filter, update):
        '''
        Update documents satisfying filter with update.
        Both filter and update are dictionaries.
        '''
        self.collection.update_many(filter = filter, update= update)
    
    def find_id_for_cad_number(self, cad_number):
        '''
        Return _id for the given cad_numer
        '''
        list_id = []
        for i in self.collection.find(filter = {'cad_number': cad_number}, projection = {"_id": 1}):
            # print(i)
            list_id.append(i)
        return list_id
        

    def remove_record_in_weather(self, cad_number, val):
        '''
        Remove items in the weather array where the "value" is val.
        '''
        self.collection.update_many({'cad_number': cad_number}, 
                { '$pull': 
            { 'weather':{'value': val} }} )


 

            
