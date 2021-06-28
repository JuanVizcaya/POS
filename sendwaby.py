import requests

url = "https://api-messaging.movile.com/v1/whatsapp/send"
headers = {
    'username': "USER_NAME",
    'authenticationtoken': "TOKEN",
    }


#msg = u"Test \\n estoy haciendo pruebas. \\n y luego al reves /n a ver que pasa. *fomrato* y _formato_"
msg= "https://www.google.com/maps/@19.3475056,-99.2009822,15z"

payload = """
        {
            "destinations": [{
                "correlationId": "TESTCORR2",
                "destination": "+525555555555"
            }],
            "message": {
                "messageText": "%s"
            }
        }
        """ % msg


payload = payload.encode(encoding='UTF-8',errors='strict')

response = requests.request("POST", url, data=payload, headers=headers)

print(response.text)
print(response)






payload ="""
{  
           "destinations":[  
              {  
                 "correlationId":"MyCorrelationId",
                 "destination":"+521555555555555"
              }
           ],
           "message":{  
              "contacts":[  
                 {  
                    "addresses":[  
                       {  
                          "city":"Menlo Park",
                          "country":"United States",
                          "country_code":"us",
                          "state":"CA",
                          "street":"1 Hacker Way",
                          "type":"HOME",
                          "zip":"94025"
                       },
                       {  
                          "city":"Menlo Park",
                          "country":"United States",
                          "country_code":"us",
                          "state":"CA",
                          "street":"200 Jefferson Dr",
                          "type":"WORK",
                          "zip":"94025"
                       }
                    ],
                    "birthday":"2012-08-18",
                    "emails":[  
                       {  
                          "email":"test@fb.com",
                          "type":"WORK"
                       },
                       {  
                          "email":"test@whatsapp.com",
                          "type":"WORK"
                       }
                    ],
                    "name":{  
                       "first_name":"John",
                       "formatted_name":"John Smith",
                       "last_name":"Smith"
                    },
                    "org":{  
                       "company":"WhatsApp",
                       "department":"Design",
                       "title":"Manager"
                    },
                    "phones":[  
                       {  
                          "phone":"+1 (940) 555-1234",
                          "type":"HOME"
                       },
                       {  
                          "phone":"+1 (650) 555-1234",
                          "type":"WORK",
                          "wa_id":"16505551234"
                       }
                    ],
                    "urls":[  
                       {  
                          "url":"https://www.fb.com",
                          "type":"WORK"
                       }
                    ]
                 }
              ]
           }
        }
    """