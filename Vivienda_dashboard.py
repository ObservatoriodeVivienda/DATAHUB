#!/usr/bin/env python
# -*- coding: utf-8 -*-
# __author__ = 'rsanchezav'

import pandas as pn
from tools import INEGI

def state_db(indicador="3102009001"):
    """Construct a State level DataFrame from INEGI API"""
    #merge data from every state, create databases for each index:
    db_state = INEGI(indicador=indicador,area="01")
    db_state.reset_index(inplace=True)
    for each in range(2,32):
        if len(str(each))==1:
            each = "0"+str(each)
        data = INEGI(indicador=indicador,area=each)
        data.reset_index(inplace=True)
        db_state = db_state.merge(data,on="index")
    return db_state

