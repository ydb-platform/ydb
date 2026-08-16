# -*- coding: utf-8 -*-
# Shared constants for the UDF store.
# Must match TUdfBehaviour::GetInternalStorageTablePath() under .metadata/.

UDF_STORE_PATH = ".metadata/udf_store"
UDF_TABLE_META_PATH = UDF_STORE_PATH + "/meta"
UDF_KV_BINARIES_PATH = UDF_STORE_PATH + "/binaries"
