# -*- coding: utf-8 -*-
from libraries import *
from flask import jsonify
reload(sys)
sys.setdefaultencoding('utf-8')
#sale2?
weekDays = ("Lunes","Martes","Miércoles","Jueves","Viernes","Sábado","Domingo")

dict_ciclo_frecuencia = {
    'LMV': 8, #Diario, se espera que vayan frecuencia*8 usando 8 semanas
    'Mensual S1': 2,
    'Mensual S4': 2,
    'MJS': 2,
    'M S1': 2,
    'M S2': 2,
    'M S3': 2,
    'M S4': 2,
    'Q1': 1,
    'Q2': 1,
    'Semanal': 8,
    'Semana Non': 4,
    'Semana Par': 4,
    'S Non': 4,
    'S Par': 4}

dict_canales = {'560': 'Detalle',
                '561': 'Autoservicios',
                '562': 'Institucionales',
                '569': 'Recuperacion',
                '570': 'Consumos',
                '573': 'Escuelas',
                '574': 'Conveniencia'}

pd.set_option('use_inf_as_na', True)

def get_info_clicod(clicod, conn):
    # Obtener nombre de tienda
    q = """
        WITH rp AS (
            SELECT *
            FROM
                dim_routeplan AS rp
            WHERE
                retailer_code='{}'
        ), info_cliente AS (
            SELECT
                contact_person1 AS nombre_cliente,
                taxpayer_name AS descripcion_cliente,
                inv_street_name || ' No. ' || inv_ext_number || ', ' || inv_neighborhood AS direccion,
                sales_center_code,
                retailer_code
            FROM
                dim_retailer
            WHERE
                retailer_code='{}'
        ), master_info AS (
            SELECT
                rp.route_code AS codigo_ruta,
                COALESCE(ic.nombre_cliente, 'N/D') AS nombre_cliente,
                COALESCE(ic.descripcion_cliente, 'N/D') AS descripcion_cliente,
                COALESCE(ic.direccion, 'N/D') AS direccion,
                COALESCE(dsc.sc_name, 'N/D') AS sc_name,
                COALESCE(r.cost_center, 'N/D') AS cost_center,
                COALESCE(dsc.organization, 'N/D') AS organization,
                (monday::int + tuesday::int
                    +thursday::int
                    +friday::int
                    +saturday::int
                    +sunday::int)::text AS frecuencia,
                ((monday::int + tuesday::int
                  +thursday::int
                  +friday::int
                  +saturday::int
                  +sunday::int) * 2 * (wk1::int + wk2::int + wk3::int + wk4::int)
                  ) AS frecuencia_esperada --expected freq each 8 weeks
            FROM
                rp
            LEFT JOIN
                dim_route AS r
            ON
                rp.route_code = r.route_code
                AND rp.sc_code::int = r.sc_code::int
            LEFT JOIN
                 dim_sales_center AS dsc
            ON
                rp.sc_code::int = dsc.sc_code::int
            LEFT JOIN
                info_cliente AS ic
            ON
                rp.retailer_code = ic.retailer_code
                AND rp.sc_code::int = ic.sales_center_code::int
        )

        SELECT
            DISTINCT *
        FROM
            master_info
        WHERE
            organization IN ('OBM', 'OBL')
            AND frecuencia::int > 0
        """.format(clicod, clicod)

    info_clicod = sqlio.read_sql_query(q, conn)

    if len(info_clicod) == 0:
        dueno = ''
        nombre_tienda = dueno
        direccion = dueno
        canal = 'No identificado'
        agencia = 'No identificado'
        ruta_frec = info_clicod
    else:
        info_clicod['canal'] = info_clicod['cost_center'].replace(dict_canales)
        canal = ', '.join(info_clicod['canal'].unique())
        agencia = ', '.join(info_clicod['sc_name'].unique())
        nombre_tienda = info_clicod['nombre_cliente'].iloc[0]
        dueno = info_clicod['descripcion_cliente'].iloc[0]
        direccion = info_clicod['direccion'].iloc[0]
        ruta_frec = info_clicod[['codigo_ruta', 'frecuencia', 'frecuencia_esperada', 'organization']]

    return dueno, nombre_tienda, direccion, canal, agencia, ruta_frec

def parse_info_clicod(clicod, info):

    dueno, name, dir, canal, agencia, ruta_frec = info
    print(info)

    #info_cliente = str(clicod) + ': *' + name + "*" + ' / ' + "*"+  dueno + "*" + ' / ' + dir
    if name == "":
        info_cliente = "*"+  dueno + "*: "
    else:
        info_cliente = '*' + name + "*" + ' / ' + "*"+  dueno + "*: "
    info_cliente += 'https://api.url/?text={}'.format(clicod)

    return info_cliente

def get_stats(clicod, conn):
    #Takes a client and a connection and returns relevant stats
    dueno, nombre_tienda, direccion, canal, codigo_agencia, ruta_frec = get_info_clicod(clicod, conn)

    ##########CODIGO RUTA
    codigo_ruta = sqlio.read_sql_query(
        """
        SELECT route_code AS ruta
        FROM dim_routeplan
        WHERE retailer_code='{}'
        GROUP BY route_code
        """.format(clicod),
        conn
    )

    if len(codigo_ruta) > 0:
        codigo_ruta = list(codigo_ruta['ruta'].unique())
        codigo_ruta = ', '.join(codigo_ruta)
    else:
        codigo_ruta = 'No identificado'


    ##########VENTAS
    cantidad = sqlio.read_sql_query(
        """
        SELECT
            fecha, codigo_producto,
            precio_venta * cantidad_de_venta AS venta,
            cantidad_de_venta AS cantidad,
            cp.nombre_producto,
            COALESCE(rt.route_type, 'N/D') as cod_linea_ruta
        FROM fact_det AS fd
        LEFT JOIN
            catalogo_productos AS cp
            ON cp.sku = fd.codigo_producto
        LEFT JOIN dim_route AS rt
            ON fd.codigo_agencia::int = rt.sc_code::int
            AND fd.codigo_ruta::int = rt.route_code::int
            AND fd.canal_id::int = rt.cost_center::int
        WHERE
            fd.codigo_cliente='{}'
            AND fd.fecha >= '2020-01-01'
            AND fd.entidad_legal IN ('OBM', 'OBL')
        """.format(clicod),
        conn
    )

    cantidad = cantidad.drop_duplicates(['fecha', 'codigo_producto', 'cod_linea_ruta', 'venta'])

    if len(cantidad) == 0:
        return dict()

    venta_del_mes = cantidad.groupby(cantidad['fecha'].dt.month).sum().sort_values('fecha', ascending = False).reset_index().loc[0]['venta'].round().astype(int)
    min_30d = str(pd.to_datetime(cantidad['fecha'].max()) - timedelta(days=56))
    ventas_30d=  cantidad[cantidad['fecha'] >= min_30d]['venta'].sum()
    cantidad = cantidad[cantidad['fecha'] >= min_30d]

    #Semal promedio
    cantidad['week'] = cantidad['fecha'].dt.week
    cantidad['day'] = cantidad['fecha'].dt.dayofweek

    venta_semanal_promedio = cantidad.groupby(['week']).sum().mean().map(lambda x: round(x))
    venta_semanal_promedio = '{:,}'.format(venta_semanal_promedio['venta'])

    venta_semanal_promedio_ruta = cantidad.groupby(['cod_linea_ruta', 'week']).sum()
    venta_semanal_promedio_ruta = venta_semanal_promedio_ruta.reset_index().groupby('cod_linea_ruta').mean()['venta']
    venta_semanal_promedio_ruta = venta_semanal_promedio_ruta.map(lambda x: 0 if isclose(x, 0, abs_tol=0.1) else round(x, 2)).map('{:,}'.format)
    venta_semanal_promedio_ruta.index = venta_semanal_promedio_ruta.index.str.replace(' ', '')
    venta_semanal_promedio_ruta = venta_semanal_promedio_ruta.index.str.cat(venta_semanal_promedio_ruta, join='right', sep=': *$').map(lambda x: x+'*').str.cat(sep='\\n')

    #Semanal diario
    venta_sem_prom_dia = cantidad.groupby(['week', 'day']).sum().reset_index().groupby('day').mean()['venta']
    venta_sem_prom_dia = venta_sem_prom_dia.map(lambda x: 0 if isclose(x, 0, abs_tol=0.1) else round(x, 2)).map('{:,}'.format)
    venta_sem_prom_dia.index = venta_sem_prom_dia.index.map(lambda x: weekDays[x])
    venta_sem_prom_dia = venta_sem_prom_dia.index.str.cat(venta_sem_prom_dia, join='left', sep=': *$').map(lambda x: x+'*').str.cat(sep='\\n')

    cantidad['devolucion'] = (cantidad['venta']<0)*cantidad['venta']
    cantidad['venta'] = (cantidad['venta']>=0)*cantidad['venta']
    cantidad = cantidad.sort_values(['codigo_producto', 'nombre_producto'], ascending=True)

    pct_devolucion = cantidad[['venta', 'devolucion']].sum()
    pct_devolucion = 100*abs(pct_devolucion['devolucion'])/pct_devolucion['venta'] if pct_devolucion['venta'] > 0 else 0
    pct_devolucion = round(pct_devolucion, 2)
    pct_devolucion = str(pct_devolucion) + '%'

    #Devolucion ruta
    pct_devolucion_ruta = cantidad.groupby(['cod_linea_ruta'])[['venta', 'devolucion']].sum()
    pct_devolucion_ruta = abs(pct_devolucion_ruta['devolucion'])/pct_devolucion_ruta['venta']
    pct_devolucion_ruta = pct_devolucion_ruta.fillna(0)
    pct_devolucion_ruta = pct_devolucion_ruta.map(lambda x: round(100*abs(x), 2)).map('{:,}'.format)
    pct_devolucion_ruta = pct_devolucion_ruta.map(lambda x: '*' + x + '%*')
    pct_devolucion_ruta.index = pct_devolucion_ruta.index.str.replace(' ', '')
    pct_devolucion_ruta = pct_devolucion_ruta.index.str.cat(pct_devolucion_ruta, join='left', sep=': ').str.cat(sep='\\n')

    #Top 5
    top_5 = cantidad.groupby(['codigo_producto', 'week']).agg({'venta': 'sum', 'devolucion': 'sum', 'nombre_producto': 'first'})
    top_5['pct_devolucion'] = (100*top_5['devolucion']/top_5['venta']).fillna(0)
    top_5 = top_5.groupby(['codigo_producto']).agg({'venta': 'mean', 'devolucion': 'mean', 'pct_devolucion': 'mean', 'nombre_producto': 'first'})
    top_5 = top_5.reset_index().nlargest(5, 'venta')[['codigo_producto', 'nombre_producto', 'venta', 'devolucion', 'pct_devolucion']]
    top_5['pct_devolucion'] = top_5['pct_devolucion'].map(lambda x: abs(round(x, 2))).map('{:,}'.format)
    top_5['venta'] = top_5['venta'].map(lambda x: 0 if isclose(x, 0, abs_tol=0.1) else round(x, 2)).map('{:,}'.format)
    top_5['pct_devolucion'] = top_5['pct_devolucion'].apply(lambda x: '*' + x + '%*')
    top_5_f = (top_5['codigo_producto'] + ' - ' + top_5['nombre_producto'].apply(lambda x: '' if x is None else x)).str[:30]
    top_5_f = top_5_f.str.cat(top_5['venta'], sep=',\\n    Ventas: *$').map(lambda x: x+'*')
    top_5_f = top_5_f.str.cat(top_5['pct_devolucion'], sep=',\\n    Porcentaje de devolución: ')
    top_5_f = top_5_f.str.cat(sep='\\n')

    top_devolucion_ruta = cantidad.groupby(['codigo_producto', 'cod_linea_ruta']).agg({'venta': 'sum', 'devolucion': 'sum', 'nombre_producto': 'first'})
    top_devolucion_ruta = top_devolucion_ruta.query('(devolucion < 0.001) & (venta > 0)')
    top_devolucion_ruta = top_devolucion_ruta.reset_index().groupby('cod_linea_ruta').apply(lambda df: df.nsmallest(3, 'devolucion'))
    top_devolucion_ruta = top_devolucion_ruta[['codigo_producto', 'nombre_producto', 'venta', 'devolucion']].reset_index()
    top_devolucion_ruta['cod_linea_ruta'] = top_devolucion_ruta['cod_linea_ruta'].str.replace(' ', '')
    top_devolucion_ruta['pct_devolucion'] = (100*top_devolucion_ruta['devolucion']/top_devolucion_ruta['venta']).fillna(0)
    top_devolucion_ruta = top_devolucion_ruta.sort_values(['cod_linea_ruta', 'pct_devolucion'])
    top_devolucion_ruta['pct_devolucion'] = top_devolucion_ruta['pct_devolucion'].map(lambda x: abs(round(x, 2))).map('{:,}'.format)
    top_devolucion_ruta['pct_devolucion'] = top_devolucion_ruta['pct_devolucion'].apply(lambda x: '*' + x + '%*')
    top_devolucion_ruta['codigo_producto'] = (top_devolucion_ruta['codigo_producto'] + ' - ' + top_devolucion_ruta['nombre_producto'].apply(lambda x: '' if x is None else x)).str[:30]
    lineas = top_devolucion_ruta['cod_linea_ruta'].unique()

    top_devolucion_ruta_f = ''
    for linea in lineas:
        str_linea = '{}: \\n     '.format(linea)
        aux = top_devolucion_ruta.query('cod_linea_ruta == @linea')
        aux_f = aux['codigo_producto'].str.cat(aux['pct_devolucion'], sep=': ')
        aux_f = aux_f.str.cat(sep='\\n     ')
        aux_f = str_linea + aux_f + '\\n'
        top_devolucion_ruta_f += aux_f

    #top_devolucion_ruta_f = top_devolucion_ruta['cod_linea_ruta'].str.cat(top_devolucion_ruta['codigo_producto'], sep=': \\n')
    #top_devolucion_ruta_f = top_devolucion_ruta_f.str.cat(top_devolucion_ruta['pct_devolucion'], sep=',\\n     Porcentaje de devolución: ')
    #top_devolucion_ruta_f = top_devolucion_ruta_f.str.cat(sep='\\n')

    # Top 5 ruta
    top_5_ruta = cantidad.groupby(['codigo_producto', 'cod_linea_ruta', 'week']).agg({'venta': 'sum', 'devolucion': 'sum', 'nombre_producto': 'first'})
    top_5_ruta['pct_devolucion'] = (100 * top_5_ruta['devolucion'] / top_5_ruta['venta']).fillna(0)
    top_5_ruta = top_5_ruta.groupby(['codigo_producto', 'cod_linea_ruta']).agg({'venta': 'mean', 'devolucion': 'mean', 'pct_devolucion': 'mean', 'nombre_producto': 'first'})
    top_5_ruta = top_5_ruta.reset_index().groupby('cod_linea_ruta').apply(lambda df: df.nlargest(5, 'venta'))
    top_5_ruta = top_5_ruta[['codigo_producto', 'nombre_producto', 'venta', 'devolucion', 'pct_devolucion']].reset_index()
    top_5_ruta['cod_linea_ruta'] = top_5_ruta['cod_linea_ruta'].str.replace(' ', '')
    top_5_ruta['pct_devolucion'] = top_5_ruta['pct_devolucion'].map(lambda x: abs(round(x, 2))).map('{:,}'.format)
    top_5_ruta['pct_devolucion'] = top_5_ruta['pct_devolucion'].apply(lambda x: '*' + x + '%*')
    top_5_ruta['venta'] = top_5_ruta['venta'].map(lambda x: 0 if isclose(x, 0, abs_tol=0.1) else round(x, 2)).map('{:,}'.format)
    top_5_ruta['codigo_producto'] = (top_5_ruta['codigo_producto'] + ' - ' + top_5_ruta['nombre_producto'].apply(lambda x: '' if x is None else x)).str[:30]
    top_5_ruta_f = top_5_ruta['cod_linea_ruta'].str.cat(top_5_ruta['codigo_producto'], sep=': ')
    top_5_ruta_f = top_5_ruta_f.str.cat(top_5_ruta['venta'], sep=',\\n     Ventas: *$').map(lambda x: x+'*')
    top_5_ruta_f = top_5_ruta_f.str.cat(top_5_ruta['pct_devolucion'], sep=',     Porcentaje de devolución: ')
    top_5_ruta_f = top_5_ruta_f.str.cat(sep='\\n')


    min_6d = pd.to_datetime(cantidad['fecha'].max()) - timedelta(days=6)
    min_6d = str(min_6d.date())
    ventas_promedio_6d =  round(ventas_30d/26*6,2)
    grouped = cantidad.groupby(['nombre_producto', 'codigo_producto'])['venta'].sum().sort_values(ascending=False).reset_index()
    if len(grouped) == 0:
        prod_mas_exitoso_int=None
        prod_mas_exitoso= None
    else:
        prod_mas_exitoso_int = int(grouped['codigo_producto'][0])
        prod_mas_exitoso = grouped['nombre_producto'][0]
    #prod_mas_devuelto_int = int(grouped['codigo_producto'].to_numpy()[-1])
    grouped_c = cantidad.groupby(['nombre_producto', 'codigo_producto'])['cantidad'].sum().sort_values(ascending=False).reset_index()
    if len(grouped_c) ==0:
        prod_mas_devuelto_int= None
        prod_mas_devuelto = None
    else:
        prod_mas_devuelto_int= int(grouped_c['codigo_producto'].to_numpy()[-1])
        prod_mas_devuelto = grouped_c['nombre_producto'].to_numpy()[-1]
    #print(prod_mas_devuelto_int)
    #prod_mas_exitoso = sqlio.read_sql_query("""SELECT nombre_producto FROM catalogo_productos WHERE sku='{}'""".format(prod_mas_exitoso_int), conn)['nombre_producto']
    #if len(prod_mas_exitoso) > 0:
    #    prod_mas_exitoso = prod_mas_exitoso[0]
    #else:
    #    prod_mas_exitoso = str(prod_mas_exitoso_int)

    #prod_mas_devuelto = sqlio.read_sql_query("""SELECT nombre_producto FROM catalogo_productos WHERE sku='{}'""".format(prod_mas_devuelto_int), conn)['nombre_producto']
    #if len(prod_mas_devuelto) > 0:
    #    prod_mas_devuelto = prod_mas_devuelto[0]
    #else:
    #    prod_mas_devuelto = str(prod_mas_devuelto_int)
    grouped_fecha = cantidad[(cantidad['fecha'] == str(cantidad['fecha'].max().date())) & (cantidad['venta'] >= 0)]
    ultima_venta = grouped_fecha['venta'].sum().round().astype(int)
    ultima_venta_fecha = str(pd.to_datetime(grouped_fecha['fecha'].unique()[0]).date())

    #### Vendedor
    vendedor = sqlio.read_sql_query(
        """
        SELECT CONCAT(empleado_nombre, ' ', empleado_apellido_paterno, ' ', empleado_apellido_materno) AS nombre_vendedor,
                date_part('year', age(now(), fecha_antiguedad)) AS antiguedad,
                date_part('year', age(now(), fecha_nacimiento)) AS edad
        FROM catalogo_empleados
        WHERE empleado_id IN (
            SELECT id_vend
            FROM catalogo_rutas
            WHERE ruta IN (
                SELECT id_ruta
                FROM catalogo_info_clientes
                WHERE id_cliente='{}'
            )
        )
        LIMIT 1;
        """.format(clicod),
        conn
    )
    if len(vendedor) == 0:
        nombre_vendedor = None
        antiguedad = None
        edad = None
    else:
        nombre_vendedor = vendedor['nombre_vendedor'].iloc[0]
        antiguedad = vendedor['antiguedad'].iloc[0]
        edad = vendedor['edad'].iloc[0]

    #Crédito pesito
    pesito = sqlio.read_sql_query(
        """
        SELECT imp_pesito_cliente, dias_credito
        FROM catalogo_info_clientes
        WHERE id_cliente='{}'
            AND imp_pesito_cliente > 0
            AND dias_credito<>'0';
        """.format(clicod),
        conn
    )

    if len(pesito) == 0:
        importe_pesito = None
        dias_credito = None
    else:
        importe_pesito = pesito['imp_pesito_cliente'].iloc[0]
        dias_credito = pesito['dias_credito'].iloc[0]

    #TODO FEDE
    #vendedor_dsd = []
    vendedor_dsd = sqlio.read_sql_query(
        """
        SELECT nombre_vendedor, codigo_vendedor
        FROM catalogo_empleados_dsd
        WHERE codigo_cliente='{}';
        """.format(clicod),
        conn
    )
    if len(vendedor_dsd) == 0:
        nombre_vendedor_dsd = None
        codigo_vendedor_dsd = None
    else:
        vendedor_dsd = vendedor_dsd.sample(1, random_state=420)
        nombre_vendedor_dsd = vendedor_dsd['nombre_vendedor'].iloc[0]
        codigo_vendedor_dsd = vendedor_dsd['codigo_vendedor'].iloc[0]

    #Frecuencia de visita y efectividad de venta
    if not ruta_frec.empty:
        ruta_frec['ruta_frec'] = ruta_frec['codigo_ruta'].str.cat(ruta_frec['frecuencia'], sep=': ')
        rutas_df = ruta_frec.groupby('organization')['ruta_frec']
        rutas_df = rutas_df.apply(lambda x: x.str.cat(sep = '\\n'))
        rutas_df = rutas_df.reset_index()

        query_frec_ventas = """
            WITH last_date AS (
                SELECT
                    date_trunc('week', fecha::date - 3)::date + 3 AS fecha
                FROM
                    fact_det
                WHERE
                    codigo_cliente = '{}'
                ORDER BY fecha DESC
                LIMIT 1
            )

            SELECT codigo_ruta, COUNT(*) AS visitas
            FROM (
                SELECT codigo_ruta, fecha
                FROM
                    fact_det
                WHERE
                    codigo_cliente='{}'
                AND
                    fecha <= (SELECT fecha FROM last_date)
                AND
                    fecha >= (SELECT fecha::DATE - INTERVAL '8 WEEK' FROM last_date)
                GROUP BY codigo_ruta,  fecha
            ) AS foo
            GROUP BY codigo_ruta
            """.format(clicod, clicod)

        ruta_frec_ventas = sqlio.read_sql_query(query_frec_ventas, conn)

        if len(ruta_frec_ventas) > 0:
            efectividad_venta = ruta_frec_ventas.merge(ruta_frec, how='left', on=['codigo_ruta'])
            efectividad_venta['efectividad'] = efectividad_venta['visitas'] / efectividad_venta['frecuencia_esperada']
            efectividad = efectividad_venta.groupby('organization')['efectividad'].mean()
            efectividad = (100 * efectividad).round(2).astype(str)
            efectividad = efectividad.reset_index()
            efectividad['efectividad'] = efectividad['efectividad'] + '%'

            rutas_df = rutas_df.merge(efectividad, how='left', on=['organization'])
        else:
            rutas_df['efectividad'] = 'No disponible'

        rutas_df.loc[rutas_df['efectividad'].isna(), 'efectividad'] = 'Ventas no detectadas'
        rutas_efectividad = ''
        for _, row in rutas_df.iterrows():
            rutas_efectividad += '*' + row['organization'] + '*:\\n'
            rutas_efectividad += 'Número de Ruta / Frecuencia de Visita: \\n' + row['ruta_frec'] + '\\n'
            rutas_efectividad += 'Efectividad de venta promedio: \\n' + row['efectividad'] + 2 * '\\n'

    else:
        rutas_efectividad = 'Número de Ruta / Frecuencia de Visita: \\n'
        rutas_efectividad += 'No identificado' + 2*'\\n'
        rutas_efectividad += 'Efectividad de venta promedio: \\n'
        rutas_efectividad += 'No identificado'

    dict_return = {
        'canal': canal,
        'agencia': codigo_agencia,
        'ruta': codigo_ruta,
        'venta_del_mes': venta_del_mes,
        'ventas_30d': ventas_30d,
        'venta_semanal_promedio': venta_semanal_promedio,
        'venta_semanal_promedio_ruta': venta_semanal_promedio_ruta,
        'venta_sem_prom_dia': venta_sem_prom_dia,
        'pct_devolucion': pct_devolucion,
        'pct_devolucion_ruta': pct_devolucion_ruta,
        'top_5': top_5_f,
        'top_devolucion_ruta': top_devolucion_ruta_f,
        'top_5_ruta': top_5_ruta_f,
        'ventas_promedio_6d': ventas_promedio_6d,
        'prod_mas_exitoso': prod_mas_exitoso,
        'prod_mas_devuelto': prod_mas_devuelto,
        'ultima_venta': ultima_venta,
        'ultima_venta_fecha': ultima_venta_fecha,
        'dueno' : dueno,
        'nombre_tienda': nombre_tienda,
        'direccion': direccion,
        'nombre_vendedor': nombre_vendedor,
        'nombre_vendedor_dsd': nombre_vendedor_dsd,
        'codigo_vendedor_dsd': codigo_vendedor_dsd,
        'antiguedad': antiguedad,
        'edad': edad,
        'importe_pesito': importe_pesito,
        'dias_credito': dias_credito,
        'rutas_efectividad': rutas_efectividad
    }

    return dict_return


def parse_json_text(text_message):
    dict_message = {}
    dict_message['message_text'] = text_message['messageText']
    dict_message['message_type'] = 'TEXT'

    return_data = pd.DataFrame(dict_message, index=[0])

    return return_data

def parse_json_live_location(live_location):
    dict_message = {}
    dict_message['message_text'] = "LIVE LOCATION"
    dict_message['message_type'] = 'UNKNOWN'
    return_data = pd.DataFrame(dict_message, index=[0])
    return return_data

def parse_json_image(image_message):
    dict_message={}
    dict_message['mediaUrl'] = image_message['mediaUrl']
    dict_message['message_type'] = 'IMAGE'
    return_data = pd.DataFrame(dict_message, index=[0])
    return return_data


def parse_json_location(location_message):
    dict_message = {}

    dict_message['geo_point'] = location_message['location']['geoPoint']
    dict_message['message_type'] = 'LOCATION'

    return_data = pd.DataFrame(dict_message, index=[0])

    return return_data

def parse_json_request(json_data):
    data = json_data['data'][0]
    dict_data = {}
    dict_data['source'] = data['source']
    dict_data['origin'] = data['origin']
    dict_data['user_name'] = data['userProfile']['name']
    dict_data['whatsapp_id'] = data['userProfile']['whatsAppId']
    dict_data['message_type'] = data['message']['type']
    dict_data['received_date'] = data['receivedDate']
    # Additional variables

    dict_data['sent_status_code'] = None
    dict_data['updated_date'] = None
    dict_data['sent'] = None
    dict_data['sent_date'] = None
    dict_data['sent_status'] = None
    dict_data['message'] = None


    if dict_data['message_type'] == "TEXT":
        return_data_message = parse_json_text(data['message'])
        dict_data['short_content'] = return_data_message['message_text'][0]
    elif dict_data['message_type'] == "LOCATION":
        return_data_message = parse_json_location(data['message'])
        dict_data['short_content'] = return_data_message['geo_point'][0]
    elif dict_data['message_type'] == "UNKNOWN":
        return_data_message = parse_json_live_location(data['message'])
        dict_data['short_content'] = "LIVE LOCATION"
    elif dict_data['message_type'] == "IMAGE":
        return_data_message = parse_json_image(data['message'])
        dict_data['short_content'] = return_data_message['mediaUrl'][0]
    else:
        dict_data['short_content'] = "MENSAJE NO SOPORTADO"
        return_data_message = "MENSAJE NO SOPORTADO"

    return_data = pd.DataFrame(dict_data, index=[0])

    return return_data, return_data_message


def parse_json_status(json_data):
    data = json_data['data'][0]
    dict_data = {}
    dict_data['sent_status_code'] = data['sentStatusCode']
    dict_data['sent_status'] = data['sentStatus']
    dict_data['updated_date'] = data['updatedDate']
    dict_data['sent'] = data['sent']
    dict_data['sent_date'] = data['sentDate']
    dict_data['correlation_id'] = data['correlationId']
    #agregar nu
    return_data = pd.DataFrame(dict_data, index=[0])
    return return_data

def add_correlation_id(correlation_id,conn,table='log_messages'):
    sqlio.execute(
        """
        INSERT INTO {}(correlation_id)
        VALUES ('{}')
        """.format(table, correlation_id),
        conn
    )
    conn.commit()
    return 1


def update_postgres_iniciatives(conn, df, corr_id, table='log_iniciatives'):

    generated_date = df['generated_date'][0]
    whatsapp_id = df['whatsapp_id'][0]
    message = df['message'][0]
    c=conn.cursor

    c.execute(
        """
        UPDATE {}
        SET generated_date = '{}'::timestamp AT TIME ZONE 'Z',
            message = '{}',
            whatsapp_id = '{}'
        WHERE correlation_id = '{}'
        """.format(table, generated_date, message, whatsapp_id, corr_id),
        conn
    )

    conn.commit()

    print('Updated table: ' + str(table))


def update_postgres_request(conn, df, corr_id, table='log_messages'):
    source = df['source'][0]
    origin = df['origin'][0]
    user_name = df['user_name'][0]
    whatsapp_id = df['whatsapp_id'][0]
    message_type = df['message_type'][0]
    received_date = df['received_date'][0]
    short_content = df['short_content'][0]


    sqlio.execute(
        """
        UPDATE {}
        SET source = '{}',
            origin = '{}',
            user_name = '{}',
            whatsapp_id = '{}',
            message_type = '{}',
            received_date = '{}'::timestamp AT TIME ZONE 'Z',
            short_content = '{}'
        WHERE correlation_id='{}'
        """.format(table, source, origin, user_name, whatsapp_id, message_type, received_date, short_content, corr_id),
        conn
    )
    conn.commit()
    print('updated table:' + str(table))


def update_postgres_text(conn, df, corr_id, table='log_messages_text'):
    message_text = df['message_text']
    message_type = df['message_type']


    sqlio.execute(
        """
        UPDATE {}
        SET message_text = '{}',
            message_type = '{}',
            correlation_id = '{}'
        """.format(table, message_text, message_type, corr_id),
        conn
    )
    conn.commit()
    print('updated table: ' + str(table))


def update_postgres_location(conn, df, corr_id, table='log_messages_location'):
    geo_point = df['geo_point']
    message_type = df['message_type']


    sqlio.execute(
        """
        UPDATE {}
        SET geo_point = '{}',
            message_type = '{}',
            correlation_id = '{}'
        """.format(table, geo_point, message_type, corr_id),
        conn
    )
    conn.commit()
    print('updated table: ' + str(table))

def update_postgres_image(conn, df, corr_id, table='log_messages_location'):
    #TODO UPDATE
    pass
    # geo_point = df['geo_point']
    # message_type = df['message_type']
    #
    #
    # sqlio.execute(
    #     """
    #     UPDATE {}
    #     SET geo_point = '{}',
    #         message_type = '{}',
    #         correlation_id = '{}'
    #     """.format(table, geo_point, message_type, corr_id),
    #     conn
    # )
    # conn.commit()
    # print('updated table')


def update_postgres_message(conn, message, corr_id, codigo,table='log_messages'):
    sqlio.execute(
        """
        UPDATE {}
        SET message = '{}'
        WHERE correlation_id='{}'
        """.format(table, message, corr_id),
        conn
    )
    conn.commit()
    print('updated table: ' + str(table))


def update_postgres_message_wraper(conn, parsed_request, correlation_id, message_data):
    #Updating log_messages
    # Initial insert
    message_type = parsed_request['message_type'][0]
    update_postgres_request(conn, parsed_request, correlation_id)
    if message_type == 'TEXT':
        update_postgres_text(conn, message_data, correlation_id)
    elif message_type == 'LOCATION':
        update_postgres_location(conn, message_data, correlation_id)
    elif message_type == 'IMAGE':
        update_postgres_image(conn, message_data, correlation_id)
    #print(parsed_request.iloc[0])
    #print(message_data)


def find_table(corr_id, conn):

    search = sqlio.read_sql_query(
        """
        SELECT *
        FROM log_messages
        WHERE correlation_id='{}'
        """.format(corr_id),
        conn
    )

    if search.empty:
        return 'log_iniciatives'
    else:
        return 'log_messages'

def update_postgres(conn, df, corr_id, table='log_messages'):
    _sent = df['sent'][0]
    _sent_date = df['sent_date'][0]
    _sent_status = df['sent_status'][0]
    _sent_status_code = df['sent_status_code'][0]
    _updated_date =  df['updated_date'][0]

    sqlio.execute(
        """
        UPDATE {}
        SET sent = '{}',
            sent_date = '{}'::timestamp AT TIME ZONE 'Z',
            sent_status = '{}',
            sent_status_code = '{}',
            updated_date = '{}'::timestamp AT TIME ZONE 'Z'
        WHERE correlation_id='{}'
        """.format(table, _sent, _sent_date, _sent_status, _sent_status_code, _updated_date, corr_id),
        conn
    )
    conn.commit()

    print('updated table: ' + str(table))

def get_msg(json_data):
    #json_data = request.json
    if json_data['data'][0]['message']['type'] == "TEXT":
        message = json_data['data'][0]['message']['messageText'].lower()
        geo = False
        lat = None
        lon = None
        number = json_data['data'][0]['userProfile']['whatsAppId']
        nombre = json_data['data'][0]['userProfile']['name']
    elif json_data['data'][0]['message']['type'] == "LOCATION":
        message = json_data['data'][0]['message']['location']['geoPoint']
        geo = True
        lat = message.split(",")[0]
        lon = message.split(",")[1]
        number = json_data['data'][0]['userProfile']['whatsAppId']
        nombre = json_data['data'][0]['userProfile']['name']
    elif json_data['data'][0]['message']['type'] == "UNKNOWN":
        message = "LIVE LOCATION"
        geo = False
        lat = None
        lon = None
        number = json_data['data'][0]['userProfile']['whatsAppId']
        nombre = json_data['data'][0]['userProfile']['name']
    elif json_data['data'][0]['message']['type'] == "IMAGE":
        message = json_data['data'][0]['message']['mediaUrl']
        geo = False
        lat = None
        lon = None
        number = json_data['data'][0]['userProfile']['whatsAppId']
        nombre = json_data['data'][0]['userProfile']['name']
    else:
        message = json_data['data'][0]['message']
        geo = False
        lat = None
        lon = None
        number = json_data['data'][0]['userProfile']['whatsAppId']
        nombre = json_data['data'][0]['userProfile']['name']

    return message, geo, lat, lon, number, nombre

def send_message(message,number,correlation_id,rodman=True):
    url = "https://api-messaging.movile.com/v1/whatsapp/send"
    headers = {
        'username': "USER_NAME",
        'authenticationtoken': "TOKEN",
        }

    destination = "+"+str(number)
    payload = """
            {
                "destinations": [{
                    "correlationId": "%s",
                    "destination": "%s"
                }],
                "message": {
                    "messageText": "%s"
                }
            }
            """ % (correlation_id,destination,message)

    response = requests.request("POST", url, data=payload, headers=headers)
    #return(jsonify(message))
    if rodman:
        return(jsonify(message))
    else:
        return(1)


def get_print_summary_short(clicod,stats,geo=False,lat=False,lon=False,info_others=None):
    if not stats:
        return str(u"No se encontró el cliente: " + str(clicod))

    canal = stats['canal']
    agencia = stats['agencia']
    ruta = stats['ruta']
    venta_del_mes = stats['venta_del_mes']
    ventas_30d = stats['ventas_30d']
    venta_semanal_promedio = stats['venta_semanal_promedio']
    venta_semanal_promedio_ruta = stats['venta_semanal_promedio_ruta']
    venta_sem_prom_dia = stats['venta_sem_prom_dia']
    pct_devolucion = stats['pct_devolucion']
    pct_devolucion_ruta = stats['pct_devolucion_ruta']
    top_5 = stats['top_5']
    top_devolucion_ruta = stats['top_devolucion_ruta']
    top_5_ruta = stats['top_5_ruta']
    ventas_promedio_6d = stats['ventas_promedio_6d']
    prod_mas_exitoso = stats['prod_mas_exitoso']
    prod_mas_devuelto = stats['prod_mas_devuelto']
    ultima_venta = stats['ultima_venta']
    ultima_venta_fecha = stats['ultima_venta_fecha']
    dueno = stats['dueno']
    nombre_tienda =  stats['nombre_tienda']
    direccion = stats['direccion']
    nombre_vendedor = stats['nombre_vendedor']
    nombre_vendedor_dsd = stats['nombre_vendedor_dsd']
    codigo_vendedor_dsd = stats['codigo_vendedor_dsd']
    importe_pesito = stats['importe_pesito']
    dias_credito = stats['dias_credito']
    antiguedad = stats['antiguedad']
    edad = stats['edad']
    rutas_efectividad = stats['rutas_efectividad']

    salto= """

    -"""
    new_line = "\\n"

    info_cliente = str("Nombre del establecimiento " + str(clicod)) + ' : *' + nombre_tienda + "*"
    canal_msg = str("Canal: " +'*'+canal+'*')
    agencia_msg = "Nombre de CeVe: " + '*'+agencia+'*'
    ruta_msg = "Número de Ruta: " + '*' + ruta + '*'
    rutas_efectividad_msg = new_line + rutas_efectividad
    venta_ultimos_30 = str("Ventas totales (últimos 30 días): *$" + '{:,}'.format(ventas_30d)) +"*"
    semanal_promedio = "Venta promedio semanal (total): *$" + venta_semanal_promedio +"*"
    semanal_promedio_ruta = str("Venta promedio semanal (línea de ruta): " + new_line +venta_semanal_promedio_ruta)
    semanal_promedio_dia = str("Venta promedio semanal (día): " + new_line + venta_sem_prom_dia)
    pct_devolucion = str("Porcentaje de devolución (total): " + "*" + pct_devolucion + "*")
    pct_devolucion_ruta = str("Porcentaje de devolución (línea de ruta): "+ new_line +pct_devolucion_ruta)
    top_5 = str("Top 5 de productos ($ promedio semanal):" + new_line + top_5)
    top_5_ruta = str("Top 5 productos por línea de ruta ($ promedio semanal):" + new_line + top_5_ruta)
    top_devolucion_ruta = str("Productos con exceso de devolución por línea de ruta (%):" + new_line + top_devolucion_ruta)
    ultima = str("Sales last visit *$" + '{:,}'.format(ultima_venta)) +"*"
    if prod_mas_exitoso is None:
        max_exit=""
    else:
        max_exit = str("Top producto ($): *" + prod_mas_exitoso) +"*"
    if prod_mas_devuelto is None:
        mas_dev=""
    else:
        mas_dev = str("Producto más devuelto (cantidad): *" + prod_mas_devuelto) +"*"
    if nombre_vendedor is not None:
        info_vend = "Salesperson: *" + str(nombre_vendedor) + '*, ' + str(int(edad)) + ' years, seniority: ' + str(int(antiguedad)) + ' years'
    else:
        info_vend = ""
    if nombre_vendedor_dsd is not None:
        info_vend = "Salesperson: *" + str(nombre_vendedor_dsd) + '*'+ new_line + "Seller id: *" + str(int(codigo_vendedor_dsd)) + "*"


    if importe_pesito is not None:
        info_pesito = "Saldo _pesito_: *" + str(dias_credito) + '* credit days. Ammount *$' + '{:,}*'.format(importe_pesito)
    else:
        info_pesito = "Saldo _pesito_: no aplica."

    if canal.lower() in ["detalle", "conveniencia"]:
        message = info_cliente + ' / ' + "*"+  dueno + "*" + new_line + canal_msg + new_line + agencia_msg + \
              2*new_line + rutas_efectividad + \
              new_line + semanal_promedio + \
              2*new_line + semanal_promedio_ruta + 2*new_line + \
              pct_devolucion + 2*new_line + \
              pct_devolucion_ruta + \
              2*new_line + top_5 + \
              2*new_line + top_devolucion_ruta + 2*new_line
        #if canal.lower() == "detalle":
        #    message = message + new_line +  info_pesito
    elif canal.lower() == "autoservicios":
        message = info_cliente + new_line + canal_msg + new_line + agencia_msg + \
              2*new_line + rutas_efectividad + \
              new_line + semanal_promedio + \
              2*new_line + semanal_promedio_ruta + 2*new_line + \
              semanal_promedio_dia + 2*new_line + \
              pct_devolucion + 2*new_line + \
              pct_devolucion_ruta + \
              2*new_line + top_5_ruta + \
              2*new_line + top_devolucion_ruta + 2*new_line
    else:
        #message = str(u"No se encontró el cliente: " + str(clicod))
        message = info_cliente + ' / ' + "*"+  dueno + "*" + new_line + canal_msg + new_line + agencia_msg + \
              2*new_line + rutas_efectividad + \
              new_line + semanal_promedio + \
              2*new_line + semanal_promedio_ruta + 2*new_line + \
              pct_devolucion + 2*new_line + \
              pct_devolucion_ruta + \
              2*new_line + top_5 + \
              2*new_line + top_devolucion_ruta + 2*new_line

        return message

    #print "This is the message: \n" + message
    #print "This is geo: \n" + str(geo)

    if geo==True:
    	near_lat=lat
    	near_lon=lon
        direccion = str(u"*Visita la tienda:* en: " + direccion + " http://www.google.com/maps/place/{},{}".format(near_lat,near_lon))
        message = message + new_line + new_line + direccion + new_line + new_line + "*Ayúdanos* a mejorar la información de Grupo Bimbo enviando una foto de la tienda."

        #Add info of another clicod
        message = message + 2*new_line + 'Encontramos otros POS cercanos. Presiona cualquiera de los enlaces debajo para obtener su información.' + 2*new_line
        info_others = new_line.join(info_others)
        message = message + info_others


        return message
    else:
        return message


def trusted_ip(trustedIPs):
	if request.remote_addr not in trustedIPs:
		return abort(403, description="Shall not pass")


def numbers():
    numbers = ["1234567901","1234567902","1234567903","1234567904","1234567905","1234567906","1234567907","1234567908"]


    return numbers


def send_pics():
    pass


def get_message(codigo, nombre="", clicod=None):
    if codigo == 400 or codigo=="bad_request":
        message_text = u"Hola {}, para compartirte información a nivel punto de venta, por favor compárteme tu ubicación o bien, un código de cliente.".format(nombre)
    if codigo == 404:
        message_text = str(u"No se encontró el cliente: " + str(clicod))
    if codigo == 503 or codigo == "mantenimiento":
        message_text = (u"Para brindarte un mejor servicio el Bot se encuentra en mantenimiento. Favor de comunicarte al +555555555 para mas información.")
    if codigo == 999 or codigo == "live":
        message_text = (u"Please share with me your current location. (Not live location)")
    if codigo == 666:
        message_text = 'Gracias por compartir tu consulta con POS Pregunta. Por el momento, la aplicación se encuentra en mantenimiento. Nos encontramos trabajando para darte un mejor servicio, vuelve pronto.'
    if codigo == "image_message":
        message_text = u"{}, gracias por compartir. Para continuar recibiendo información a nivel punto de venta por favor comparte conmigo tu ubicación actual o un codigo de cliente.".format(nombre)

    message = {'text':message_text,'estatus':codigo}
    return message
