#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = 'rsanchezav'
"""Funciones para obtener datos de Apis Abiertas de gobierno
http://www.inegi.org.mx/desarrolladores/
"""

import json
import urllib2
import pandas as pn

def INEGI(indicador="1002000001",area="01",idioma="es",tipo="false"):
	token = "2b393191-23d5-7139-fbe1-3018ff170045"
	formato = "json"
	"""
	Indicador:
	El primer paso que se debe realizar para obtener información de la API es seleccionar el indicador e identificar su clave. Esto lo puede realizar consultando el "Constructor de consultas".

		#Busca aquí: http://www.inegi.org.mx/desarrolladores/indicadores/apiindicadores.aspx
					 http://www.inegi.org.mx/inegi/contenidos/serviciosweb/infoestadistica.aspx

	Área geográfica:
	Puede ser nacional [00], por entidad federativa [99] o por municipio [999], dependiendo de cada indicador.

	Idioma:
	La información está disponible en español [es] e inglés [en].

	Dato más reciente o Serie histórica:
	Puede consultarse solo el dato más reciente [true] o la serie histórica completa [false].

	Formato:
	Se ofrece la información en 3 tipos de formatos: JSON [json], JSONP [jsonp] o XML [xml].

	Token:
	Para utilizar la API es necesario mandarle un token válido, el cual puede obtener al registrarse aquí.
	"""
	formato = formato
	token = token

	search_url = "http://www3.inegi.org.mx/sistemas/api/indicadores/v1/Indicador/{0}/{1}/{2}/{3}/{4}/{5}".format(indicador,area,idioma,tipo,formato,token)
	temp = json.load(urllib2.urlopen(search_url))
	data = {}
	data["Data"] = {}
	data["Metadata"] = {}
	for each in range(len(temp.get("Data").get("Serie"))):
		Value = temp.get("Data").get("Serie")[each].get("CurrentValue")
		TimePeriod = temp.get("Data").get("Serie")[each].get("TimePeriod")
		data["Data"][TimePeriod] = Value

	data["Metadata"]['Freq'] =  temp.get("MetaData").get('Freq')
	data["Metadata"]['Indicator'] =  temp.get("MetaData").get('Indicator')
	data["Metadata"]['Region'] =  temp.get("MetaData").get('Region')
	data["Metadata"]['Unit'] =  temp.get("MetaData").get('Unit')


	pandasDF = pn.DataFrame.from_dict({area:data.get("Data")},orient='columns')
	return pandasDF, data["Metadata"]

def denue(condicion="camiones",coordenadas="21.85717833,-102.28487238",distancia="250"):
	token = "9fd58bbc-4198-44a8-b5c6-6b102c434856"
	formato = "json"
	"""
	#Api para Denue
	La API de DENUE te permite consultar datos de identificación, ubicación, actividad económica y tamaño de
	cerca de 5 millones de establecimientos a nivel nacional, por entidad federativa y municipio. Puedes utilizar
	la API para crear aplicaciones que muestren la información directamente de las bases de datos del INEGI en el
	preciso momento en que se actualiza.

	http://www.inegi.org.mx/desarrolladores/denue/apidenue.aspx

	Para el método de búsqueda se utilizan los siguientes parámetros:

	Condición:
	Palabra(s) a buscar dentro del nombre del establecimiento, razón social, calle, colonia, clase de actividad
	económica, entidad, municipio y localidad. Para buscar todos los establecimientos se deberá ingresar la palabra 'todos'.

	Coordenadas:
	Coordenadas centrales donde se hará la búsqueda.

	Distancia:
	Distancia a la redonda donde se realizará la búsqueda de establecimientos en metros. La distancia máxima es de 5 000 metros.

	Token:
	Es un número único que permite hacer consultas, el cual puede obtener al registrarse aquí

	"""
	token = token
	search_url = "http://www3.inegi.org.mx/sistemas/api/denue/v1/consulta/buscar/{0}/{1}/{2}/{3}".format(condicion,coordenadas,distancia,token)
	temp = json.load(urllib2.urlopen(search_url))
	data = pn.DataFrame(temp)

	return data

#GIS
