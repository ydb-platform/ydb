from ydb.tests.sql.lib.test_query import Query

from ydb.tests.datashard.lib.types_of_variables import cleanup_type_name, format_sql_value


class BinaryStringConverter:
    def __init__(self, name, data_type, vector_type):
        self.name = name
        self.data_type = data_type
        self.vector_type = vector_type


def get_vector(type, numb, size_vector):
    if type == "Float":
        values = [float(i) for i in range(size_vector - 1)]
        values.append(float(numb))
        return ",".join(f'{val}f' for val in values)

    values = [i for i in range(size_vector - 1)]
    values.append(numb)
    return ",".join(str(val) for val in values)


targets = {
    "similarity": {"inner_product": "Knn::InnerProductSimilarity", "cosine": "Knn::CosineSimilarity"},
    "distance": {
        "cosine": "Knn::CosineDistance",
        "manhattan": "Knn::ManhattanDistance",
        "euclidean": "Knn::EuclideanDistance",
    },
}

to_binary_string_converters = {
    "float": BinaryStringConverter(
        name="Knn::ToBinaryStringFloat", data_type="Float", vector_type="FloatVector"
    ),
    "uint8": BinaryStringConverter(
        name="Knn::ToBinaryStringUint8", data_type="Uint8", vector_type="Uint8Vector"
    ),
    "int8": BinaryStringConverter(name="Knn::ToBinaryStringInt8", data_type="Int8", vector_type="Int8Vector"),
}


class VectorIndexOperations:
    def __init__(self, query_object: Query):
        self.query_object = query_object

    def query(self, text):
        return self.query_object.query(text)

    def _drop_index(self, table_path, index_name):
        drop_index_sql = f"""
            ALTER TABLE `{table_path}`
            DROP INDEX `{index_name}`;
        """
        self.query(drop_index_sql)

    def _drop_table(self, table_path):
        drop_table_sql = f"""
            DROP TABLE `{table_path}`;
        """
        self.query(drop_table_sql)

    def _upsert_values(
        self,
        table_name,
        all_types,
        prefix,
        pk_types,
        vector_type,
        vector_dimension,
        to_binary_string_converters,
        rows_count,
        count_prefix
    ):
        converter = to_binary_string_converters[vector_type]
        values = []

        for key in range(1, rows_count + 1):
            vector = get_vector(vector_type, key % 127, vector_dimension)
            name = converter.name
            vector_type = converter.vector_type
            values.append(
                f'''(
                    {", ".join([format_sql_value(key, type_name) for type_name in pk_types.keys()])},
                    {", ".join([format_sql_value(key, type_name) for type_name in all_types.keys()])},
                    {", ".join([format_sql_value(key % count_prefix, type_name) for type_name in prefix.keys()])},
                    Untag({name}([{vector}]), "{vector_type}")
                    )
                    '''
            )
        upsert_sql = f"""
            UPSERT INTO `{table_name}` (
                {", ".join([f"pk_{cleanup_type_name(type_name)}" for type_name in pk_types.keys()])},
                {", ".join([f"col_{cleanup_type_name(type_name)}" for type_name in all_types.keys()])},
                {", ".join([f"prefix_{cleanup_type_name(type_name)}" for type_name in prefix.keys()])},
                vec_String
            )
            VALUES {",".join(values)};
        """
        self.query(upsert_sql)

    def _select(
        self,
        table_path,
        vector_type,
        vector_name,
        col_name,
        knn_func,
        statements,
        numb,
        prefix,
        vector_dimension,
        to_binary_string_converters,
        rows_count,
        count_prefix,
    ):
        vector = get_vector(vector_type, numb, vector_dimension)
        select_sql = f"""
                                    $Target = {to_binary_string_converters[vector_type].name}(Cast([{vector}] AS List<{vector_type}>));
                                    select {", ".join(statements)}
                                    from {table_path} view idx_vector_{vector_name}
                                    {f"WHERE prefix_String = {prefix}" if prefix != "" else ""}
                                    order by {knn_func}({col_name}, $Target) {"DESC" if knn_func in targets["similarity"].values() else "ASC"}
                                    limit {rows_count//count_prefix if prefix != "" else rows_count};
                                    """
        return self.query(select_sql)
