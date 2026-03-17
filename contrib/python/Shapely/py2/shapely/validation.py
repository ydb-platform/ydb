# TODO: allow for implementations using other than GEOS

from shapely.geos import lgeos

def explain_validity(ob):
    return lgeos.GEOSisValidReason(ob._geom)
