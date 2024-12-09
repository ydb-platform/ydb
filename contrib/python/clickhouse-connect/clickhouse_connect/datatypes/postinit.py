from clickhouse_connect.datatypes import registry, dynamic

dynamic.SHARED_DATA_TYPE = registry.get_from_name('Array(String, String)')
dynamic.STRING_DATA_TYPE = registry.get_from_name('String')
