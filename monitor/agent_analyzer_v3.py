#!/opt/python-2.7/bin/python
import os, re
import threading
from elasticsearch import Elasticsearch
import time
import urllib, json
#POST, PUT json data
import requests

def analyzer(id,ip,agent,discontinuity,dropframe,index,elasticsearch):
        #ip:multicast IP address
        #agent: agent IP address
        result = elasticsearch.search(
                index='%s' % (index),
                size=1000,
                body= {
                   "query":{
                   "bool":{
                       "must":[
                           {"match":{"host":"{0}".format(agent)}},
                           {"match":{"message":"{0}".format(ip)}},
                           {"match":{"message":"Detecte discontinuity"}}
                           ],
                       "filter":[
                               {"range":{"@timestamp":{"gt":"now-1m", "lt":"now"}}}
                       ]
                   }
               }})
        #print result['hits']['total']
        discontinuity_new = 0
        #print(len(result['hits']['hits']))
        if result['hits']['total']['value']!=0:
                for i in range(0, result['hits']['total']['value']):
                        discontinuity_new+=int(re.search('(?<=skips:)\d+',result['hits']['hits'][i]['_source']['message']).group(0))
                        #print int(re.search('(?<=skips:)\d+',result['hits']['hits'][i]['_source']['message']).group(0))
        if discontinuity_new != discontinuity:
                try:
                        requests.put(api+"profile_agent/"+str(id)+"/", auth=(api_user, api_paswd), json={"discontinuity": discontinuity_new})
                        print("discontinuity_new: {0}".format(discontinuity_new))
                except requests.exceptions.RequestException:
                        print "can't connect API!"

configfile='/opt/monitor/config.py'
if os.path.exists(configfile):
        execfile(configfile)
else:
        print "can't read file config";
        exit(1)
es = Elasticsearch(elastic_server['ip'],api_key=(elastic_server['api_id'], elastic_server['api_key']),timeout=3.5)
localtime = time.gmtime()
date_now=time.strftime("%Y.%m.%d",localtime)
index='logstash-%s' % (date_now)

#response = urllib.urlopen(api+"profile_agent/analyzer/")
response = requests.get(api+"profile_agent/analyzer/",auth=(api_user, api_paswd))
#print response.getcode()
####analyzer##
if response.status_code==200:
#    print "200"
    profile_agents = json.loads(response.text)
    for profile_agent in profile_agents['data']:
        while threading.activeCount() > 20:
            time.sleep(1)
        t = threading.Thread(target=analyzer, args=(profile_agent['id'],profile_agent['ip'].split(':30120')[0],profile_agent['agent_ip'],profile_agent['discontinuity'],profile_agent['dropframe'],index,es,))
        t.start()
time.sleep(20)
