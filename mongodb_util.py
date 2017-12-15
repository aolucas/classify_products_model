# coding:utf8

from pymongo import MongoClient
from pymongo import MongoReplicaSetClient
from pymongo import ReadPreference
import config as config

db_pool = {}


class MongodbUtils(object):
    def __init__(self, table, ip=config.DB_IP, port=config.DB_PORT, collection='suvdata', db_insert=False,
                 replicaset_name=None, read_preference=ReadPreference.SECONDARY):
        # print ip
        self.ip = ip
        self.port = port
        self.collection = collection
        self.table = table
        self.db_insert = db_insert

        self.replicaset_name = replicaset_name
        self.read_preference = read_preference

        if (ip, port) not in db_pool:
            db_pool[(ip, port)] = self.db_connection()
        elif not db_pool[(ip, port)]:
            db_pool[(ip, port)] = self.db_connection()

        self.db = db_pool[(ip, port)]
        self.db_table = self.db_table_connect()

    def __enter__(self):
        return self.db_table

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def db_connection(self):
        db = None
        try:
            if self.replicaset_name:
                db = MongoReplicaSetClient(self.ip, read_preference=self.read_preference, replicaSet=self.replicaset_name)
            elif self.port:
                db = MongoClient(self.ip, self.port)
            else:
                if self.db_insert:
                    db = MongoReplicaSetClient(self.ip, read_preference=ReadPreference.PRIMARY_PREFERRED)
                else:
                    db = MongoReplicaSetClient(self.ip, read_preference=ReadPreference.SECONDARY_PREFERRED)
        except Exception as e:
            print e
        return db

    def db_table_connect(self):
        table_db = self.db[self.collection][self.table]
        return table_db


# 获取mongo2.6数据库配置
def db_data(db_type):
    rpt_dict = config.DB_MONGO[db_type]
    return rpt_dict["DB_IP"], rpt_dict["DB_PORT"], rpt_dict["DB_COLLECTION"]


# 获取mongo3申请表副本集配置
def db_app():
    app_dict = config.AppForm_MONGODB
    app_ip = "mongodb://%s/?replicaSet=%s" % (app_dict["USER_DATA_REPLSET_MEMBERS"], app_dict["USER_DATA_REPLSET"])
    app_collection = app_dict["USER_DATA_REPLSET_COLLECTION"]
    return app_ip, app_collection


def db_monitor(table, collection):
    monitor_ip, monitor_port, monitor_collection = db_data("MONITOR")
    monitor_collection = monitor_collection if not collection else collection
    db_monitor = MongodbUtils(ip=monitor_ip, port=monitor_port, collection=monitor_collection, table=table)
    return db_monitor.db_table
