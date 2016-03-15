#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'rsanchezav'
"""
Ver catálogo nacional de Indicadores
http://www3.inegi.org.mx/sistemas/cni/indicadores.aspx?idOrden=1.1&txt=

*Encontramos proporcion de poblacion urbana en areas marginadas
http://www3.inegi.org.mx//sistemas/api/indicadores/v1//Indicador/6300000035/00000/es/false/json/

"""
import pandas as pn
from tools import INEGI


def state_db(indicador="3102009001"):
    """Construct a State level DataFrame from INEGI API
    http://www.inegi.org.mx/desarrolladores/indicadores/apiindicadores.aspx
    """
    #merge data from every state, create databases for each index:
    db_state, meta = INEGI(indicador=indicador,area="01")
    db_state.reset_index(inplace=True)

    for each in range(2,32):
        if len(str(each))==1:
            each = "0"+str(each)
        data, meta = INEGI(indicador=indicador,area=each)
        data.reset_index(inplace=True)
        db_state = db_state.merge(data,on="index")
    return db_state, meta

