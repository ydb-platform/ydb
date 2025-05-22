def split_data_into_fixed_size_chunks(data, chunk_size):
    """Splits data to N chunks of chunk_size size"""
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]
