import ydb.convert


class TestPgBase():
    @classmethod
    def get_extra_feature_flags(cls):
        return super(TestPgBase, cls).get_extra_feature_flags() + ["enable_table_pg_types"]

    @classmethod
    def setup_class(cls):
        # Hack to support pg types in Python SDK. TODO: use native Python SDK pg types support
        ydb.convert._to_native_map["pg_type"] = lambda type_pb, value_pb, table_client_settings: value_pb.text_value

        super(TestPgBase, cls).setup_class()
