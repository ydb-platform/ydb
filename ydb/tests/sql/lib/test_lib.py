class TestLib:

    def split_data_into_fixed_size_chunks(data, chunk_size):
        """Splits data to N chunks of chunk_size size"""
        for i in range(0, len(data), chunk_size):
            yield data[i:i + chunk_size]

    # Function to perform bulk upserts
    def bulk_upsert_operation(self, table_name, data_slice):
        self.driver.table_client.bulk_upsert(
            f"{self.database}/{table_name}",
            data_slice,
            self.tpch_bulk_upsert_col_types()
        )
