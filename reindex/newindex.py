# make sure ES is up and running
import requests
import json
import sys

new_mapping = """{
  "analytics_transaction" : {
    "mappings" : {
      "send" : {
        "properties" : {
          "district" : {
            "type" : "long"
          },
          "flyer_count" : {
            "type" : "long"
          },
          "flyer" : {
            "type" : "long"
          },
          "email_id" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
    "to_email" : {
      "type" : "string"
    },
    "from_email" : {
      "type" : "string"
    },
    "subject" : {
      "type" : "string"
          },
          "owner" : {
            "type" : "long"
          },
          "school" : {
            "type" : "long"
          },
          "source" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "ts" : {
            "type" : "date",
            "format" : "dateOptionalTime"
          }
        }
      },
      "view" : {
        "properties" : {
          "district" : {
            "type" : "long"
          },
          "flyer" : {
            "type" : "long"
          },
          "owner" : {
            "type" : "long"
          },
          "school" : {
            "type" : "long"
          },
          "source" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "ts" : {
            "type" : "date",
            "format" : "dateOptionalTime"
          },
          "user" : {
            "type" : "long"
          },
          "user_agent" : {
            "type" : "string"
          }
        }
      },
      "open" : {
        "properties" : {
          "district" : {
            "type" : "long"
          },
          "first_open" : {
            "type" : "boolean"
          },
          "flyer_count" : {
            "type" : "long"
          },
          "flyer" : {
            "type" : "long"
          },
          "ip" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "email_id" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
    "to_email" : {
      "type" : "string"
    },
    "from_email" : {
      "type" : "string"
    },
    "subject" : {
      "type" : "string"
          },
          "owner" : {
            "type" : "long"
          },
           "school" : {
            "type" : "long"
          },
          "source" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "ts" : {
            "type" : "date",
            "format" : "dateOptionalTime"
          },
          "user_agent" : {
            "type" : "string"
          }
        }
      },
      "click" : {
        "properties" : {
          "district" : {
            "type" : "long"
          },
          "first_click" : {
            "type" : "boolean"
          },
          "flyer_count" : {
            "type" : "long"
          },
          "flyer" : {
            "type" : "long"
          },
          "ip" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "email_id" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "position" : {
            "type" : "long"
          },
    "to_email" : {
      "type" : "string"
    },
    "from_email" : {
      "type" : "string"
    },
    "subject" : {
      "type" : "string"
          },
          "owner" : {
            "type" : "long"
          },
          "school" : {
            "type" : "long"
          },
          "source" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "ts" : {
            "type" : "date",
            "format" : "dateOptionalTime"
          },
          "user_agent" : {
            "type" : "string"
          }
        }
      },
      "registration_click" : {
        "properties" : {
          "district" : {
            "type" : "long"
          },
          "flyer" : {
            "type" : "long"
          },
          "ip" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "email_id" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
    "to_email" : {
      "type" : "string"
    },
    "from_email" : {
      "type" : "string"
    },
    "subject" : {
      "type" : "string"
          },
          "owner" : {
            "type" : "long"
          },
          "school" : {
            "type" : "long"
          },
          "source" : {
            "type" : "string",
            "index":  "not_analyzed"
          },
          "ts" : {
            "type" : "date",
            "format" : "dateOptionalTime"
          },
          "user_agent" : {
            "type" : "string"
          }
        }
      }
    }
  }
}"""


url='http://dfwesapp0vz32.dfw.objectrocket.com:9200'
index='analytics_transaction'
size=1000
indexurl=url + '/' + index 

res = requests.get(url)
print res.status_code
print(res.content)
print type(res.content)

types=['send', 'view', 'open', 'click', 'registration_click'] 

for type in types:
  print "Processing type " + type

  # Do it in batches
  res = requests.get(indexurl + '/' + type + '/_search?scroll=1m&size=11')

  if res.status_code != 200:
    print "Error"
    sys.exit(1)

  rs=json.loads(res.content)

  #print json.dumps(rs, sort_keys=True, indent=4, separators=(',', ': '))

  scroll_id = rs['_scroll_id']

  hits=len(rs['hits']['hits'])

  while hits > 0:

    print "Number of hits: %d" % (hits)
    res = requests.get(url + '/_search/scroll?scroll=1m&scroll_id=' + scroll_id)

    if res.status_code != 200:
      print res.content
      print "Error"
      sys.exit(1)

    rs=json.loads(res.content)

    #print json.dumps(rs, sort_keys=True, indent=4, separators=(',', ': '))

    scroll_id = rs['_scroll_id']
    hits=len(rs['hits']['hits'])

