#include "pg_compat.h"

extern "C" {
#include "catalog/namespace.h"
#include "mb/pg_wchar.h"
}

#include <ydb/library/yql/parser/pg_catalog/catalog.h>

Oid	FindDefaultConversionProc(int32 for_encoding, int32 to_encoding) {
    return NYql::NPg::LookupConversion(pg_encoding_to_char(for_encoding), pg_encoding_to_char(to_encoding)).ProcId;
}
