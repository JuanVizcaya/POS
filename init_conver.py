# -*- coding: utf-8 -*-
from helpers import *
from libraries import *
#sc = SlackClient(os.environ["SLACK_TOKEN"])


def main():
	params = {
		'database': 'db_name',
		'user': 'user',
		'password': 'password',
		'host':  'url.to.db.rds.amazonaws.com',
		'port':  1234
	}

	conn = psycopg2.connect(**params)

	def get_recent_active(conn):
		#gets recents as list
		recientes = sqlio.read_sql_query(
			"""
			SELECT source
			FROM active_sessions
			WHERE source NOT IN (
				SELECT whatsapp_id
				FROM log_messages
	            WHERE whatsapp_id IS NOT NULL
				GROUP BY whatsapp_id
			) AND source NOT IN (
				SELECT whatsapp_id
				FROM log_iniciatives
				WHERE whatsapp_id IS NOT NULL
				GROUP BY whatsapp_id
			);
			""",
			conn
		)

		recientes = recientes['source'].to_list()
		if len(recientes) > 0:
			print(recientes)

		return recientes

	recientes = get_recent_active(conn)


	welcome_message =  u"Bienvenido a POS Intelligence ORAX. Una herramienta de Grupo Abraxas, orgullosa empresa de Bimbo Ventures. En ella podrás obtener información de los más de 800,000 puntos de venta de GB a través de este chat interactivo. Mándame un mensaje para empezar."

	def init_conv(recientes, message=welcome_message):
		for num in recientes:
			print(num)
			correlation_id = uuid.uuid4()
			# Inserting correlation_id
			add_correlation_id(correlation_id, conn, table='log_iniciatives')
			# Inserting data to postgres
			init_df = pd.DataFrame({
				'generated_date': datetime.now(),
				'message': message,
				'whatsapp_id': num,
				'correlation_id': correlation_id
			}, index=[0])

			update_postgres_iniciatives(conn, init_df, correlation_id)

			response = send_message(message, num, correlation_id, rodman=False)


			#add_response_to_db(response)
		return 1

	#init_conv(['4646531959'])

	while True:
		os.system("python /home/ubuntu/flaskapp/check_sessions.py")
		init_conv(get_recent_active(conn))
	        time.sleep(4)

	def add_response_to_db(response):
		pass


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        message = traceback.format_exc()
        sc.api_call(
            "chat.postMessage",
            channel="#orax_da",
            link_names=1,
            text=" INIT CONVER ERROR <@UKMH82VE1>, <@UP30MGTTL>, <@U77NK393J>\n" + message,
            username="POS ARREGLA")
        print(message)
