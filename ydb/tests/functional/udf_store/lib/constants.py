# -*- coding: utf-8 -*-
# Shared constants for the UDF store.
# Must match paths under .metadata/udf_store.

UDF_STORE_PATH = ".metadata/udf_store"
UDF_TABLE_META_PATH = UDF_STORE_PATH + "/meta"
UDF_KV_BINARIES_PATH = UDF_STORE_PATH + "/binaries"
UDF_TABLE_WASM_SOURCE_PATH = UDF_STORE_PATH + "/wasm_source"
UDF_TABLE_WASM_SOURCE_CHUNKS_PATH = UDF_STORE_PATH + "/wasm_source_chunks"
UDF_TABLE_LIBRARY_SOURCE_PATH = UDF_STORE_PATH + "/library_source"
UDF_TABLE_LIBRARY_SOURCE_CHUNKS_PATH = UDF_STORE_PATH + "/library_source_chunks"

# Must match NKikimr::NUdfStore::WasmBlobChunkSize
WASM_BLOB_CHUNK_SIZE = 8 * 1024 * 1024
