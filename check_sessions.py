from utils_op import *
import io
import requests
import json
import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
import os
from libraries import *
#sc = SlackClient(os.environ["SLACK_TOKEN"])



def main():
    url = "http://api-messaging.movile.com/v1/session?customerId={}"

    payload = {}
    headers = {
      'authenticationToken': 'TOKEN',
      'userName': 'USER_NAME'
    }

    response = requests.request("GET", url, headers=headers, data = payload)

    sessions = json.loads(response.text.encode('utf8'))['file_url']

    s=requests.get(sessions).content
    c=pd.read_csv(io.StringIO(s.decode('utf-8')))


    c.columns = ['resp']
    c['resp'] = c['resp'].str.split(';')

    c['source'] = c['resp'].apply(lambda x: x[0])
    c['session_created_at'] = pd.to_datetime(c['resp'].apply(lambda x: x[1]))

    c.drop(columns='resp', inplace=True)

    # TO-Do Write in DB
    params = {
       'database': os.environ["POSTGRES_DATABASE_POS"],
       'user': os.environ["POSTGRES_USER_POS"],
       'password': os.environ["POSTGRES_PASSWORD_POS"],
       'host': os.environ["POSTGRES_HOST_POS"],
       'port':  5432
    }
    con = psycopg2.connect(**params)


    sqlio.execute("""DELETE FROM active_sessions;""", con)

    insert_dataframe_to_postgresql(con, "active_sessions", c)
    print c
    con.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        message = traceback.format_exc()
        sc.api_call(
            "chat.postMessage",
            channel="#orax_da",
            link_names=1,
            text=" CHECK SESSIONS ERROR <@UKMH82VE1>, <@UP30MGTTL>, <@U77NK393J>\n" + message,
            username="POS ARREGLA",
            icon_emoji=":this-is-fine-fire:")
        print(message)
