import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc 
import numpy as np
import string
import argparse 
def random_string_array(size):
    """
    Generate a numpy array of random strings
    """

    strs = [
        "asd;nlasnjdfpoanfasdo[fvnwvnwqp0v vojv noqwvc]",
        " cpka9324tphcp35 gn2[s dn[weqodf niwq0fpo ]]",
        "ncvopwdcvqow opcddwo dvoc1 j   wfvdcq  3wv",
        "cascbiq vpiqewvvpvcv23114uu2381904"
     ]
    
    return np.tile(strs, size // len(strs))


def stream_to_parquet_pyarrow(output_file, total_rows=1_000, chunk_size=100_000):
    """Stream data to Parquet using PyArrow's streaming writer"""
    
    # Define schema
    schema = pa.schema([
        pa.field('k', pa.int64()),
        pa.field('v_no_index', pa.int64()),
        pa.field('v_minmax', pa.int64()),
        pa.field('v_bloom', pa.int64()),
        pa.field('string_data', pa.string())
    ])
    
    # Create writer
    writer = pq.ParquetWriter(output_file, schema)
    
    try:
        for chunk_num in range(0, total_rows, chunk_size):
            # Generate chunk data
            chunk_rows = min(chunk_size, total_rows - chunk_num)
            
            # Create PyArrow Table
            table = pa.Table.from_pydict({
                'k': np.arange(chunk_num*chunk_size, chunk_num*chunk_size+chunk_size),
                'v_no_index': np.tile([1,2,3,4], chunk_size // 4),
                "v_minmax": np.tile([1,2,3,4], chunk_size // 4),
                'v_bloom': np.tile([1,2,3,4], chunk_size // 4),
                'string_data': random_string_array(chunk_size)
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
