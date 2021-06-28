# -*- coding: utf-8 -*-
from helpers import *
from libraries import *
import uuid


reload(sys)
sys.setdefaultencoding('utf-8')
FLAG_MANTIMIENTO = False

params_aws = {
'database': 'db_name',
'user': 'user',
'password': 'password',
'host':  'url.to.db.rds.amazonaws.com',
'port':  1234
}

params_azure = {
'database': 'db_name',
'user': 'user',
'password': 'password',
'host':  'url.to.db.azure.com',
'port':  1223
}

trustedIPs = ["0.0.0.0","1.11.1.1","2.2.2.2.2"]


app = Flask(__name__)
debug_apm= False
if debug_apm:
    from elasticapm.contrib.flask import ElasticAPM
    app.config['ELASTIC_APM'] = {
    'SERVICE_NAME': 'service_name',
    'SECRET_TOKEN': 'token',
    'SERVER_URL': 'url.to.eapm.aws.cloud.es.io',
    }
    apm = ElasticAPM(app)


@app.route("/")
def forbiden():
    return(trusted_ip(trustedIPs))

#home
@app.route("/waby", methods=['POST'])
def get_data_waby():

    if request.remote_addr not in trustedIPs:
        return abort(403, description="Shall not pass")

    json_data = request.json
    #print ("Esta es la DATA TAL CUAL: \n" + str(json_data))
    #print ("Esta es Head: \n" + str(request.headers))
    #print ("sentStatusCode" not in str(json_data))
    #if image return 403, sólo te puedo ayudar
    #if request is type message (Solo hay dos tipos de mensaje? TEXT Y LOCATION. NO. Falta imagen)
    #MENSAJE DEL CLIENTE (SIEMPRE HAY RESPUESTA y ESTATUS DE ESA RESPUESTA)
    if "message" in str(json_data):
        # Generating correlation_id
        correlation_id = str(uuid.uuid4())
        conn_aws = psycopg2.connect(**params_aws)
        conn_azure = psycopg2.connect(**params_azure)
        conns = [conn_aws, conn_azure]

        for conn in conns:
            add_correlation_id(correlation_id, conn)
        parsed_request, message_data = parse_json_request(json_data)

        update_postgres_message_wraper(conn_aws, parsed_request, correlation_id, message_data) # Actualiza el mensaje dependiendo del tipo de mensaje
        update_postgres_request(conn_azure, parsed_request, correlation_id) # Actualiza el mensaje

        message,geo,lat,lon,number,nombre = get_msg(json_data) # Obtiene datos del usuario
        trusted_numbers = numbers()
        if parsed_request['whatsapp_id'][0] not in trusted_numbers:# si no es número aceptado, lo registra en ambas conexiones y responde que NO
            message = {'text':"Para usar el POS Pregunta envía un mensaje desde tu correo institucional de Grupo Bimbo con tu número de celular a services@orax.io para autorizar el acceso.", 'estatus': "403"}
            for conn in conns:
                update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
                conn.close()
            return(send_message(message['text'],number,correlation_id))


        if parsed_request['message_type'][0] == "UNKNOWN":
            message = get_message(codigo="live",nombre=nombre,clicod="")
            for conn in conns:
                update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
                conn.close()
            return(send_message(message['text'],number,correlation_id))

        elif parsed_request['message_type'][0] == "IMAGE":
            message = get_message(codigo="image_message", nombre=nombre,clicod="")
            for conn in conns:
                update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
                conn.close()
            return(send_message(message['text'],number,correlation_id))
        # if parsed_request['message_type'][0] == "IMAGE":
        #     message = get_message(codigo="live",nombre=nombre,clicod="")
        #     update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
        #     conn.close()
        #     return(send_message(message['text'],number,correlation_id))

        #if parsed_request['message_type'][0] == "media":
        for conn in conns:
            conn.close()


    # if status (PUEDE SER ESTATUS DE UNA RESPUESTA O DE UN MENSAJE INICIADO POR MI)
    elif "sentStatus" in str(json_data):
        # SI ES ESTATUS DE RESPUESTA TIENE CORRELATION ID EN LA BASE
        parsed_status = parse_json_status(json_data)
        # Si el correlation id no esta hay que crear uno (mensaje de estatus que NO es originado por MO)
        #print(parsed_status)
        conn_aws = psycopg2.connect(**params_aws)
        conn_azure = psycopg2.connect(**params_azure)
        conns = [conn_aws, conn_azure]

        # Finding correlation_id
        corr_id = parsed_status['correlation_id'][0]

        for conn in conns:
            table_corr_id = find_table(corr_id, conn)
            update_postgres(conn, parsed_status, corr_id, table_corr_id)
            conn.close()
        # PARA SABER SI ES ESTATUS DE RESPUESTA POR MENSAJE INICIADO POR MI EL NO DEBE ESTAR EN LA BASE DE LOGS.
        # PERO SI EN LA BASE DE LOGS_INICIADOS_POR_ORAX

        # Si fue Tiendita exitosa conn geo: (checar correlation y TYPE GEO)
        #message = "Gracias.Send Pics"
        # send_message(message,number,correlation_id)
        #si no, seguir
        return("Fallback")
    #elif "media" in str(json_data):
        #pass
        #correlation_id = "???"
        #parsed_request, message_data = parse_json_request(json_data)
        #update_postgres_image(conn, message_data, correlation_id)

    body = message


    if FLAG_MANTIMIENTO:
        #message = (u"Para brindarte un mejor servicio el Bot se encuentra en mantenimiento. Favor de comunicarte al +525541339883 para mas información.")
        message = get_message(codigo=666)
        #parsed_request['message'] = message
        return(send_message(message['text'],number,correlation_id))

    # Get the message the user sent our Twilio number (lower)
    try:
        int(body)
        valid_clicod = True
    except:
        valid_clicod = False

    if valid_clicod:
        #send_message("Gracias por tu consulta. La estamos procesando.",number,correlation_id)
        clicod = body
        #print clicod
        conn_aws = psycopg2.connect(**params_aws)
        conn_azure = psycopg2.connect(**params_azure)
        conns = [conn_aws, conn_azure]
        #print conn
        is_not_bimbo = sqlio.read_sql_query(
            """
            SELECT *
            FROM view_catalogo_clientes_recientes
            WHERE clicod='{}'
            """.format(clicod),
            conn_aws
        ).empty

        if is_not_bimbo:
            stats = dict()
        else:
            stats = get_stats(clicod, conn_aws)
        #print stats
        if len(stats) > 0:
            message = get_print_summary_short(clicod=clicod,stats=stats,geo=False)
            #print message
            for conn in conns:
                update_postgres_message(conn, message, correlation_id, codigo = 200)

                conn.close()

            return(send_message(message,number,correlation_id))
        else:
            #message = str("No sé encontró el cliente: " + str(clicod))
            message = get_message(codigo=404,nombre=nombre,clicod=clicod)

            for conn in conns:
                update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
                conn.close()

            return(send_message(message['text'],number,correlation_id))
    #If message has cordinates, then do geo stuff
    #palabras=["cliente","clicod","pos", "punto", "#"]
    palabras=["#"]
    #welcome_message =  u"Bienvenido a POS Intelligence ORAX. Una herramienta de Grupo Abraxas, orgullosa empresa de Bimbo Ventures. En ella podrás obtener información de los más de 800,000 puntos de venta de GB a través de este chat interactivo."

    if (lat and lon):
        #send_message("Gracias por tu consulta. La estamos procesando.",number,correlation_id)
        conn_aws = psycopg2.connect(**params_aws)
        conn_azure = psycopg2.connect(**params_azure)
        conns = [conn_aws, conn_azure]
        # get nearest
        near = sqlio.read_sql_query(
            """
            SELECT clicod
            FROM view_catalogo_clientes_recientes AS v
            GROUP BY clicod, v.geom
            ORDER BY v.geom <->'SRID=4326;POINT({} {})'::geometry
            LIMIT 4
            """.format(lon,lat,lon,lat),
            conn_aws
        )
        near = near['clicod'].unique()
        near = list(near)
        # Check if nearses exists
        if len(near)>0:
            clicod = str(near.pop(0))
            near_geo = sqlio.read_sql_query(
                """
                SELECT
                    longitud,
                    latitud
                FROM
                    catalogo_clientes
                WHERE
                    clicod='{}';
                """.format(clicod),
                conn_aws
            )
            near_lat = str(near_geo['latitud'][0])
            near_lon = str(near_geo['longitud'][0])
            #Get stats
            stats = get_stats(clicod, conn_aws)
            # check if nearest has info
            if len(stats)>0:
                info_others = [parse_info_clicod(cliente, get_info_clicod(cliente, conn_aws)) for cliente in near]
                message = get_print_summary_short(clicod,stats,geo=True,lat=near_lat, lon=near_lon, info_others=info_others)

                for conn in conns:
                    update_postgres_message(conn, message, correlation_id, codigo=200)
                    conn.close()

                return(send_message(message,number,correlation_id))
            else:
                #message = str("No sé encontró el cliente: " + str(clicod))
                message = get_message(codigo=404,nombre=nombre,clicod=clicod)

                for conn in conns:
                    update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
                    conn.close()

                return(send_message(message['text'],number,correlation_id))
        else:
            # ESTO PASA?
            message=u"No se encontró una tiendita cercana"

    #if it has no geo info it looks for info for clicod
    elif (any(x in body for x in palabras) or valid_clicod):
        #send_message("Gracias por tu consulta. La estamos procesando.",number,correlation_id)
        conn_aws = psycopg2.connect(**params_aws)
        conn_azure = psycopg2.connect(**params_azure)
        conns = [conn_aws, conn_azure]

        if not ("#" in body):
            body = filter(str.isdigit, str(body))
        else:
            body = "#"+filter(str.isdigit, str(body))

        clicod = body
        stats = get_stats(clicod, conn_aws)
        #check if client has info
        if len(stats)>0:
            #get address
            address = sqlio.read_sql_query(
                """
                SELECT
                    longitud,
                    latitud
                FROM
                    catalogo_clientes
                WHERE clicod='{}';""".format(clicod),
                conn_aws
            )
            #check
            if len(address) > 0:
                geo=True
                near_lat = str(address['latitud'][0])
                near_lon = str(address['longitud'][0])
            message = get_print_summary_short(clicod,stats,geo)

            for conn in conns:
                update_postgres_message(conn, message, correlation_id, codigo=200)
                conn.close()

            return(send_message(message,number,correlation_id))
        else:
            #message = str("No sé encontró el cliente: " + str(clicod))
            message = get_message(codigo=404,nombre=nombre,clicod=clicod)

            for conn in conns:
                update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
                conn.close()

            return(send_message(message['text'],number,correlation_id))


    else:
        #message = u"Estimado {}, sólo te puedo ayudar si me das un numero de cliente o comparteme tu ubicación.".format(nombre)
        message = get_message(codigo="bad_request", nombre=nombre, clicod=None)
        conn_aws = psycopg2.connect(**params_aws)
        conn_azure = psycopg2.connect(**params_azure)
        conns = [conn_aws, conn_azure]

        for conn in conns:
            update_postgres_message(conn, message['text'], correlation_id, message['estatus'])
            conn.close()

        return(send_message(message['text'],number,correlation_id))

    return("No se mando nada")

if __name__ == "__main__":
    app.run(debug=True, port=80, host='0.0.0.0')
