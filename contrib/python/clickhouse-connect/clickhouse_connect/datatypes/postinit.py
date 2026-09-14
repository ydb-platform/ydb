from clickhouse_connect.datatypes import dynamic, geometric, registry
from clickhouse_connect.datatypes.base import TypeDef
from clickhouse_connect.datatypes.container import Map

dynamic.STRING_DATA_TYPE = registry.get_from_name("String")

# Build a private Map(String, String) for JSON shared data decoding.
# We must NOT reuse the cached registry instance because we replace
# value_type with SharedDataString (reads raw bytes, encoding=None).
# Mutating the cached instance would break all normal Map(String, String) columns.
_shared_map = Map(TypeDef((), (), ("String", "String")))
_shared_map.value_type = dynamic.SharedDataString(dynamic.STRING_DATA_TYPE.type_def)
dynamic.SHARED_DATA_TYPE = _shared_map

dynamic.SHARED_VARIANT_TYPE = dynamic.SharedVariant(dynamic.STRING_DATA_TYPE.type_def)

point = "Tuple(Float64, Float64)"
ring = f"Array({point})"
polygon = f"Array({ring})"
multi_polygon = f"Array({polygon})"

geometric.POINT_DATA_TYPE = registry.get_from_name(point)
geometric.RING_DATA_TYPE = registry.get_from_name(ring)
geometric.POLYGON_DATA_TYPE = registry.get_from_name(polygon)
geometric.MULTI_POLYGON_DATA_TYPE = registry.get_from_name(multi_polygon)
