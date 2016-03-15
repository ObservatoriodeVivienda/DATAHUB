__author__ = 'rsanchezavalos & ojdo'

__all__ = ["read_shp", "write_shp", "match_vertices_and_edges"]

import itertools
import numpy as np
import pandas as pd
import shapefile
import warnings
from urllib import urlretrieve
from shapely.geometry import LineString, Point, Polygon
from zipfile import ZipFile
from tempfile import mktemp
import os
import glob
import shptools


def MXshp(type="state"):
    """Download latest shp maps from gov CONABIO and Unzip and save ir into a temporal folder.
    http://www.inegi.org.mx/geo/contenidos/geoestadistica/m_g_0.aspx
    """

    filename = mktemp('.zip')
    #choose temporal folder to save the file
    destDir = str(os.getcwd()) + "/temp/"
    if type=='state':
        theurl = "http://mapserver.inegi.org.mx/MGN/mge2014v6_2.zip"
    elif type=='municipality':
        theurl = "http://mapserver.inegi.org.mx/MGN/mgm2014v6_2.zip"
    name, hdrs = urlretrieve(theurl, filename)
    thefile=ZipFile(filename)
    thefile.extractall(destDir)
    thefile.close()
    print("file uploaded")

def shp_to_pandas(filename):
    """
    Read shapefile to dataframe w/ geometry.

    filename = glob.glob(destDir + '/*.shp')[0]
    Args:
        filename: ESRI shapefile name to be read  (without .shp extension)
    Returns:
        pandas DataFrame with column geometry, containing individual shapely
        Geometry objects (i.e. Point, LineString, Polygon) depending on
        the shapefiles original shape type

    Usage:
        # calculate population density from shapefile of cities
        cities = shp_to_pandas('cities_germany_projected')
        cities['popdens'] = cities['population'] / cities['area']
        pandas_to_shp(cities, 'cities_germany_projected_popdens')
    """
    sr = shapefile.Reader(filename)

    cols = sr.fields[:] # [:] = duplicate field list
    if cols[0][0] == 'DeletionFlag':
        cols.pop(0)
    cols = [col[0] for col in cols] # extract field name only
    cols.append('geometry')

    records = [row for row in sr.iterRecords()]

    if sr.shapeType == shapefile.POLYGON:
        geometries = [Polygon(shape.points)
                      if len(shape.points) > 2 else np.NaN  # invalid geometry
                      for shape in sr.iterShapes()]
    elif sr.shapeType == shapefile.POLYLINE:
        geometries = [LineString(shape.points) for shape in sr.iterShapes()]
    elif sr.shapeType == shapefile.POINT:
        geometries = [Point(*shape.points[0]) for shape in sr.iterShapes()]
    else:
        raise NotImplementedError

    data = [r+[g] for r,g in itertools.izip(records, geometries)]

    df = pd.DataFrame(data, columns=cols)
    df = df.convert_objects(convert_numeric=True)

    if np.NaN in geometries:
        # drop invalid geometries
        df = df.dropna(subset=['geometry'])
        num_skipped = len(geometries) - len(df)
        warnings.warn('Skipped {} invalid geometrie(s).'.format(num_skipped))
    return df

def pandas_to_shp(filename, dataframe, write_index=True):
    """Write dataframe w/ geometry to shapefile.

    Args:
        filename: ESRI shapefile name to be written (without .shp extension)
        dataframe: a pandas DataFrame with column geometry and homogenous
                   shape types (Point, LineString, or Polygon)
        write_index: add index as column to attribute tabel (default: true)

    Returns:
        Nothing.
    """

    df = dataframe.copy()
    if write_index:
        df.reset_index(inplace=True)

    # split geometry column from dataframe
    geometry = df.pop('geometry')

    # write geometries to shp/shx, according to geometry type
    if isinstance(geometry.iloc[0], Point):
        sw = shapefile.Writer(shapefile.POINT)
        for point in geometry:
            sw.point(point.x, point.y)

    elif isinstance(geometry.iloc[0], LineString):
        sw = shapefile.Writer(shapefile.POLYLINE)
        for line in geometry:
            sw.line([list(line.coords)])

    elif isinstance(geometry.iloc[0], Polygon):
        sw = shapefile.Writer(shapefile.POLYGON)
        for polygon in geometry:
            sw.poly([list(polygon.exterior.coords)])
    else:
        raise NotImplementedError

    # add fields for dbf
    for k, column in enumerate(df.columns):
        column = str(column) # unicode strings freak out pyshp, so remove u'..'

        if np.issubdtype(df.dtypes[k], np.number):
            # detect and convert integer-only columns
            if (df[column] % 1 == 0).all():
                df[column] = df[column].astype(np.integer)

            # now create the appropriate fieldtype
            if np.issubdtype(df.dtypes[k], np.floating):
                sw.field(column, 'N', decimal=5)
            else:
                sw.field(column, 'I', decimal=0)
        else:
            sw.field(column)

    # add records to dbf
    for record in df.itertuples():
        sw.record(*record[1:]) # drop first tuple element (=index)

    sw.save(filename)

def standarize_MX_states_names(shp_dataframe, extraDB, state_column):
    """When you cant make the merge with the state code use this function
    to standarize the names in both DB"""

    #list = shp_dataframe["ENTIDAD"].unique()
    #list2 = extraDB["state_column"].unique()
    
    Hashtable = {
    'AGUASCALIENTES':                'Aguascalientes',
    'BAJA CALIFORNIA':               "Baja California",
    'BAJA CALIFORNIA SUR':           "Baja California Sur",
    'Chihuahua':                     "CHIHUAHUA",
    'Coahuila de Zaragoza':          "COAHUILA DE ZARAGOZA",
    'Colima':                        "COLIMA",
    'Distrito Federal':              'DISTRITO FEDERAL',
    'Durango':                       'DURANGO',
    'Guanajuato':                    'GUANAJUATO',
    'Guerrero':                      "GUERRERO",
    'Hidalgo':                       'HIDALGO',
    'Jalisco':                       'JALISCO',
    'Michoac\xc3\xa1n de Ocampo':    'MICHOACAN DE OCAMPO',
    'Morelos':                       'MORELOS',
    'M\xc3\xa9xico':                 'MEXICO',
    'Nayarit':                       'NAYARIT',
    'Nuevo Le\xc3\xb3n':             'NUEVO LEON',
    'Quer\xc3\xa9taro de Arteaga|Queretaro de Arteaga':   'Quer\xe9taro',
    'San Luis Potos\xc3\xad|San Luis Potosi':        'San Luis Potos\xed',
    'Yucat\xc3\xa1n|yucatan':                "Yucat\xe1n'",
    }

    extraDB = pd.DataFrame({state_column: Hashtable.keys()})
    # Filter for valid rows
    extraDB = extraDB[extraDB[state_column].isin(Hashtable.keys())]
    # Replace the values according to dict
    extraDB[state_column].replace(Hashtable, inplace=True)
    print extraDB
