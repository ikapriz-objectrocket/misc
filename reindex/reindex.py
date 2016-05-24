# make sure ES is up and running
import requests
import json
import sys


url='http://dfwesapp0vz32.dfw.objectrocket.com:9200'
index='analytics_transaction'
size=10
indexurl=url + '/' + index 

res = requests.get(url)

#indextypes=['send', 'view', 'open', 'click', 'registration_click'] 
indextypes=['registration_click'] 

fields=['districts', 'flyers', 'flyer_count', 'mandrill_id', 'owners', 'schools', 'email', 'sender', 'subject', 'source', 'ts', 'user', 'user_agent', 'ip', 'first_open', 'position']
renamedfields=['district', 'flyer', 'flyer_count', 'email_id', 'owner', 'school', 'to_email', 'from_email', 'subject', 'source', 'ts', 'user', 'user_agent', 'ip', 'first_open', 'position']
convertedfields=[True, True, False, False, True, True, False, False, False, False, False, False, False, False, False, True]

for indextype in indextypes:
  print "Processing indextype " + indextype

  # Do it in batches
  res = requests.get(indexurl + '/' + indextype + '/_search?scroll=1m&size=' + str(size))

  if res.status_code != 200:
    print "Error"
    sys.exit(1)

  rs=json.loads(res.content)


  scroll_id = rs['_scroll_id']

  hits=len(rs['hits']['hits'])

  while hits > 0:

    print "Number of hits: %d" % (hits)

    for hit in rs['hits']['hits']:
      print json.dumps(hit, sort_keys=True, indent=4, separators=(',', ': '))
      newsource = {}
      newsource['_id'] = hit['_id']

      for i in range(len(fields)):
        field=fields[i]
        renamedfield=renamedfields[i]
        convertedfield=convertedfields[i]

        if field in hit['_source']:
          original = hit['_source'][field]
        elif 'msg' in hit['_source'] and field in hit['_source']['msg']:
          original = hit['_source']['msg'][field]
        else:
          continue 
       
        try:  
          if convertedfield:
            if type(original) is list:
              newsource[renamedfield] = map (int, original)
            else: 
              newsource[renamedfield] = int ( original )
          else:
            newsource[renamedfield] = original
        except TypeError:
          pass
	
      print json.dumps(newsource, sort_keys=True, indent=4, separators=(',', ': '))
	
    sys.exit(0)

    res = requests.get(url + '/_search/scroll?scroll=1m&scroll_id=' + scroll_id)

    if res.status_code != 200:
      print res.content
      print "Error"
      sys.exit(1)

    rs=json.loads(res.content)

    #print json.dumps(rs, sort_keys=True, indent=4, separators=(',', ': '))

    scroll_id = rs['_scroll_id']
    hits=len(rs['hits']['hits'])

