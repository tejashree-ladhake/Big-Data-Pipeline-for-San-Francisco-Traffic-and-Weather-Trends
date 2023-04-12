import pymongo


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

        self.client = pymongo.MongoClient(f"mongodb://{ip_address}:27017/")
        self.db = self.client[database_name]
        self.collection = self.db[collection_name]

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
        self.collection.drop()

    def find(self, query, projection):
        '''
        Return an iteratatable using query and projection.
        '''
        output = self.collection.find(query, projection)
        return return_generator(output)

    def find_sorted_doc(self, n):
        '''
        Return an iteratable including 
        the first "n" number of documents including
        "received_datetime" and "close_datetime" 
        ordered by  "received_datetime"in descending order.
        '''
        return self.collection.find(filter = {}, projection= {"received_datetime":1, "close_datetime":1, "_id":0}).sort("received_datetime", -1).limit(10)
    
    def return_call_type_weather(self,
                                 call_type_substring, 
                                 weather_limit):
        '''
        When "call_type_original_desc" includes
        "call_type_substring" in its value (case insensitive)
        and any of its "value" in "weather" 
        is greater than or equal to "weather_limit", 
        return an iteratable including its "_id", 
        "call_type_original_desc" and 
        the first satisfying "weather" elements
        ordered by "_id" in ascending order.
        '''
        return self.collection.find({"call_type_original_desc": {"$regex":"\w*"+call_type_substring+"\w*", "$options":"i"}, \
                 "weather":  {"$elemMatch": {"value": {"$gte":weather_limit}}}},\
                {"_id":1, "call_type_original_desc":1, "weather.$":1}).sort("_id", 1)
    
    def frequent_call_type_count(self, n):
        '''
        Return the "n" most frequent "call_type_original_desc"
        including its "call_type_original_desc" and "count"
        '''
        return self.collection.aggregate([{"$group":{"_id": "$call_type_original_desc", "count":{"$sum":1}}},\
            {"$sort":{"count":-1}},{"$limit":n},\
            {"$project":{"_id":0,"call_type_original_desc":"$_id", "count":"$count"}}])



        
    def daily_frequent_call_type_count(self):
        '''
        Return the "count" and "max_weather_val" 
        (the maximum value in "weather" array)
        grouped by "received_date" 
        (string value of the date portion in "received_datetime")
        and "call_type_original_desc" 
        ordered by "count" (descending order) 
        and "received_date" (ascending order)
        '''
        pipeline = [
        {
            "$addFields": {
                "received_date": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$toDate": "$received_datetime"}}}
            }
        },
        {
            "$group": {
                "_id": {
                    "received_date": "$received_date",
                    "call_type_original_desc": "$call_type_original_desc"
                },
                "count": {"$sum": 1},
                "max_weather_val": {"$max":{"$max": "$weather.value"}}
            }
        },
        {
            "$sort": {
                "count": -1,
                "_id.received_date": 1
            }
        },
        {
            "$project":{
                "_id":0,
                "max_weather_val":"$max_weather_val", "received_date":"$_id.received_date", 
                "call_type_original_desc":"$_id.call_type_original_desc", "count":"$count" #, "_id":0
            }
        }
        ]

        return self.collection.aggregate(pipeline)


    def common_sensitive_call_with_different_final_type(self, n):
        '''
        For "sensitive_call" being true, return the n most 
        "call_type_original_desc" and its "count" (number) of 
        "call_type_original_desc" being same as
        "call_type_final_desc"
        ordered by "count" in descending order.
        '''
        pipeline = [
            {"$match": {"sensitive_call": "True"}},
            {"$group": {"_id": "$call_type_original_desc", "count": {"$sum": {"$cond": [{"$eq": ["$call_type_final_desc", "$call_type_original_desc"]}, 1, 0]}}}},
            {"$project": {"count":"$count", "call_type_original_desc":"$_id", "_id":0}},
            {"$sort": {"count": -1}},
            {"$limit": n},
        ]
        return self.collection.aggregate(pipeline)

        
