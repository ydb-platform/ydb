import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc 
import numpy as np
import string
import argparse 
import random
def random_string_array(size):
    """
    Generate a numpy array of random strings
    """

    strs = [
        ":Fpj]&KhQB7?sPK+M9nSsOJo~xq*B{A$NQQlE5Uy0q^z",
        "k*d(n=MT-VYT:Z8Yad(D49j82.PUoy",
        "<0-x()DDycQ5P#fk@t4,>DCX%;FS1/!SyzB",
        "j>.(tjZ\\v4fLr3>}P0VR7K^"
    ]
    
    return np.tile(strs, size // len(strs))


def stream_to_parquet_pyarrow(output_file, total_rows=1_000, chunk_size=100_000):
    """Stream data to Parquet using PyArrow's streaming writer"""
    
    # Define schema
    schema = pa.schema([
        pa.field('k', pa.int64()),
        pa.field('ints_no_index', pa.int64()),
        pa.field('ints_minmax', pa.int64()),
        pa.field('strings_no_index', pa.string()),
        pa.field('strings_minmax', pa.string()),
        pa.field('needle_in_a_haystack_minmax', pa.int64())
    ])
    
    # Create writer
    writer = pq.ParquetWriter(output_file, schema)

    needle_row_index = int(random.random()*(total_rows-1))
    needle = 123
    print(f"needle in {needle_row_index} row")
    needle_chunk_index = needle_row_index // chunk_size
    try:
        for chunk_num in range(0, total_rows, chunk_size):
            # Generate chunk data
            chunk_rows = min(chunk_size, total_rows - chunk_num)

            ints = np.tile([1,2,3,4], chunk_size // 4)
            
            strings = np.tile([
                ":Fpj]&KhQB7?sPK+M9nSsOJo~xq*B{A$NQQlE5Uy0q^z",
                "k*d(n=MT-VYT:Z8Yad(D49j82.PUoy",
                "<0-x()DDycQ5P#fk@t4,>DCX%;FS1/!SyzB",
                "j>.(tjZ\\v4fLr3>}P0VR7K^"
            ], chunk_size // 4)

            haystack = np.tile([1,2,3,4], chunk_size // 4)
            if chunk_num == needle_chunk_index:
                haystack[needle_row_index%chunk_size] = needle


            # Create PyArrow Table
            table = pa.Table.from_pydict({
                'k': np.arange(chunk_num*chunk_size, chunk_num*chunk_size+chunk_size),
                'ints_no_index': ints,
                "ints_minmax": ints,
                'strings_no_index': strings,
                'strings_minmax': strings,
                'needle_in_a_haystack_minmax': haystack
            })
            
            # Write chunk
            writer.write_table(table)
            print(f"Written chunk {chunk_num//chunk_size + 1} of {total_rows//chunk_size + 1}")
            
    finally:
        writer.close()

def main(args=None):
    parser = argparse.ArgumentParser()

    parser.add_argument("--output", "-o", required=True, help="output parquet(csv and others are not supported) file path")
    parser.add_argument("--rows", "-r", required=True, help="output row count")

    args = parser.parse_args()


    stream_to_parquet_pyarrow(args.output, total_rows=int(args.rows), chunk_size=1_000_000)


# Usage
# stream_to_parquet_pyarrow('kv/data.parquet', total_rows=100_000)
