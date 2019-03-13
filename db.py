# -*- coding:utf-8 -*-
import MySQLdb
import yaml
import codecs
from orderdict_yaml_loader import OrderedDictYAMLLoader

config = yaml.load(codecs.open('C:\\Users\\admin\\PycharmProjects\\project2\\config.yaml', 'r', 'utf8'), Loader=OrderedDictYAMLLoader)

class database(object):

    def __init__(self, config):
        super(database, self).__init__()
        self.config = config

    def get_conn(self):
        try:
            conn = MySQLdb.connect(
                host = config['mysql']['host'],
                port = config['mysql']['port'],
                user = config['mysql']['user'],
                passwd = config['mysql']['passwd'],
                db = config['mysql']['db'],
            )

        except MySQLdb.Error as e:
                print("Myposition_sql Error %d: %s" % (e.args[0], e.args[1]))
        return conn