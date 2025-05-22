from clickhouse_connect.datatypes import registry, dynamic, geometric

dynamic.SHARED_DATA_TYPE = registry.get_from_name('Array(String, String)')
dynamic.STRING_DATA_TYPE = registry.get_from_name('String')

point = 'Tuple(Float64, Float64)'
ring = f'Array({point})'
polygon = f'Array({ring})'
multi_polygon = f'Array({polygon})'

geometric.POINT_DATA_TYPE = registry.get_from_name(point)
geometric.RING_DATA_TYPE = registry.get_from_name(ring)
geometric.POLYGON_DATA_TYPE = registry.get_from_name(polygon)
geometric.MULTI_POLYGON_DATA_TYPE = registry.get_from_name(multi_polygon)
