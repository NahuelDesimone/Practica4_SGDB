from pymongo import MongoClient
from configparser import ConfigParser
import psycopg2
import re
import matplotlib.pyplot as plt
from geopandas import GeoDataFrame
import math
from wordcloud import WordCloud

def obtenerPais(listaPaisesyCiudades, locacionTweet):
    locTweet = locacionTweet.lower()
    paisUsuario = None

    if (locTweet == "usa"):
        paisUsuario = "united states"
        return paisUsuario

    if (locTweet == "españa"):
        paisUsuario = "spain"
        return paisUsuario

    if (locTweet == "méxico"):
        paisUsuario = "mexico"
        return paisUsuario
    
    for ciudadPais in listaPaisesyCiudades:
        ciudad = ciudadPais[0].lower()
        pais = ciudadPais[1].lower()
        if (ciudad == locTweet) or (pais == locTweet):
            paisUsuario = pais
            return paisUsuario

    if (paisUsuario == None):
        return paisUsuario


def conexionMongoDB():

    cliente = MongoClient('localhost', port=27017)

    baseDeDatos = cliente['BaseDeDatosSGDB']
    colleccion = baseDeDatos['tweets2']

    respuesta = colleccion.find(
        {}, {"_id": 0, "user.id": 1, "user.location": 1})

    consultaPaisesyCiudades = "select ci.name,c.name from country c inner join city ci on c.code = ci.countrycode"

    listaCiudadesYPaises = obtenerDatosDBPostgresql(consultaPaisesyCiudades)

    for item in respuesta:
        dicc = dict(item.items())
        user = list(dicc.values())
        for elem in user:
            userID = elem["id"]
            location = elem["location"]
            if (location != None):
                palabrasLocacion = re.split('[;,-.]', location)
                for palabraConEspacio in palabrasLocacion:
                    palabra = palabraConEspacio.lstrip()
                    paisUsuario = obtenerPais(listaCiudadesYPaises, palabra)
                    if (paisUsuario != None):
                        colleccion.update_one({"user.id": userID}, {
                                              "$set": {"user.pais": paisUsuario}})


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception(
            'Section {0} not found in the {1} file'.format(section, filename))

    return db


def abrirBaseDeDatosPostgresql():
    conn = None
    params = config()
    # connect to the PostgreSQL database
    conn = psycopg2.connect(**params)
    return conn


def obtenerDatosDBPostgresql(sql):
    lista = []
    conn = None
    try:
        conn = abrirBaseDeDatosPostgresql()
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        for fila in cur:
            lista.append(fila)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

    return lista

def cargar(sql):

    conn = None
    try:
        conn = abrirBaseDeDatosPostgresql()
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        cur.execute(sql)

        row = cur.fetchone()
        diccionario = {}
        while row is not None:
            clave = row[0]
            valor = row[1]

            diccionario[clave] = valor
            row = cur.fetchone()

        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()

    return diccionario


def obtenerDiccionarioCodigos():
    cliente = MongoClient('localhost', port=27017)

    baseDeDatos = cliente['BaseDeDatosSGDB']
    colleccion = baseDeDatos['tweets2']

    respuesta = colleccion.aggregate(
        [{"$group": {"_id": "$user.pais", "total": {"$sum": 1}}}])

    dic = {}
    for elem in respuesta:
        dic[elem["_id"]] = elem["total"]

    diccPaisesCodigos = cargar("select name,code from country")

    paises = {k.lower(): v for k, v in diccPaisesCodigos.items()}

    dicNuevo = {}

    for nombrePais, cantTweets in dic.items():
        for pais, codigoPais in paises.items():
            if pais == nombrePais:
                dicNuevo[codigoPais] = cantTweets

    return dicNuevo


def graficarMapaChoroplet(columnaDf):
    dic = obtenerDiccionarioCodigos()
    world = GeoDataFrame.from_file('ne_10m_admin_0_countries.shp').sort_values(
        'ISO_A3').set_index('ISO_A3')
    listaCodigosDf = world.index.tolist()
    listaCodigosDic = list(dic.keys())

    for code in listaCodigosDf:
        if ((code in listaCodigosDic) and (dic[code] > 0)):
            world.at[code, columnaDf] = math.log2(float(dic[code]))
        else:
            world.at[code, columnaDf] = 0

    world.plot(column=columnaDf, colormap='Reds', alpha=1,
               categorical=False, legend=True, axes=None)

    plt.title(columnaDf)
    plt.show()


def crearNube(dic):
    nube = WordCloud(
        max_words=20, background_color='white',min_font_size=20).generate_from_frequencies(dic)
    plt.imshow(nube, interpolation='bilinear')
    plt.axis("off")
    plt.show()


def generarNubeDePalabras(pais):
    cliente = MongoClient('localhost', port=27017)

    baseDeDatos = cliente['BaseDeDatosSGDB']
    colleccion = baseDeDatos['tweets2']

    respuestaPais = colleccion.find(
        {"user.pais" : pais }, {"_id" : 0, "text" : 1}
    )
    
    dic = {}
    for elem in respuestaPais:
        procesarTexto(elem["text"],dic)

    crearNube(dic)

def procesarTexto(textoTweet, diccionarioPalabras):
    tweetAMinuscula = textoTweet.lower()
    palabrasEnLinea = tweetAMinuscula.split()
    rellenarDiccionario(palabrasEnLinea,diccionarioPalabras)

def rellenarDiccionario(listaPalabras,dic):
    for palabra in listaPalabras:
        if (palabra in dic):
            dic[palabra] = dic.get(palabra) + 1
        else:
            dic[palabra] = 1

#graficarMapaChoroplet("Tweets")

generarNubeDePalabras("united kingdom")

generarNubeDePalabras("argentina")

#conexionMongoDB()
