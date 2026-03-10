import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc 
import numpy as np
import string
import argparse 
import random
from hamcrest import assert_that, is_
# import yatest.common
import os

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


def a_lot_of_skip_index_ddl(output_folder, total_ddl_statements, index_type):
    num_files = 1000
    statements_per_file = (total_ddl_statements + num_files - 1) // num_files

    # base, ext = os.path.splitext(output_file)
    base = output_folder
    os.makedirs(base, exist_ok=True)

    for file_idx in range(num_files):
        start = file_idx * statements_per_file
        end = min(start + statements_per_file, total_ddl_statements)
        if start >= total_ddl_statements:
            break
        file_path = f"{base}/{file_idx:04d}.sql"
        with open(file_path, 'w') as f:
            for i in range(start, end):
                if index_type == "minmax":
                    f.write(f"ALTER OBJECT `/Root/testdb/log_writer_test` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_c{i}_minmax, TYPE=MINMAX, FEATURES=`{{\"column_name\" : \"c{i}\"}}`);\n")
                elif index_type == "bloom":
                    f.write(f"ALTER OBJECT `/Root/testdb/log_writer_test` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=index_c{i}_bloom_filter, TYPE=BLOOM_FILTER, FEATURES=`{{\"column_name\" : \"c{i}\",  \"false_positive_probability\" : 0.01}}`);\n")
                else:
                    raise ValueError(f"Invalid index type: {index_type}")
        print(f"Written {file_path} ({end - start} statements)")
    



def main(args=None):
    parser = argparse.ArgumentParser()
    # INSERT_YOUR_CODE
    subparsers = parser.add_subparsers(dest="command")

    # Subparser for "yaem-ddl"
    ddl_parser = subparsers.add_parser("yaem-ddl", help="Generate a lot of MINMAX DDL statements")
    ddl_parser.add_argument("--cols", type=int, required=True, help="Number of columns to generate DDL for. since statement is identical for int and string columns, pass summ of int and srtings here")
    ddl_parser.add_argument("--output", "-o", required=True, help="output DDL folder path")
    ddl_parser.add_argument(
        "--index", 
        type=str, 
        choices=["minmax", "bloom"], 
        help="index type to test (only 'minmax' and 'bloom' supported)", 
        default="minmax"
    )

    big_parquet = subparsers.add_parser("big-parquet", help="Generate big parquest for minmax indexes test")
    big_parquet.add_argument("--output", "-o", required=True, help="output parquet(csv and others are not supported) file path")
    big_parquet.add_argument("--rows", "-r", required=True, help="output row count")

    args = parser.parse_args()
    if args.command == "yaem-ddl":
        a_lot_of_skip_index_ddl(args.output, int(args.cols), args.index)
    elif args.command == "big-parquet":
        stream_to_parquet_pyarrow(args.output, total_rows=int(args.rows), chunk_size=1_000_000)
    else:   
        parser.print_help()
    return




# Usage
# stream_to_parquet_pyarrow('kv/data.parquet', total_rows=100_000)
