#include "as_json_node.h"
#include "compile_path.h"
#include "parse.h"
#include "serialize.h"
#include "sql_exists.h"
#include "sql_query.h"
#include "sql_value.h"

#include <ydb/library/yql/public/udf/udf_helpers.h>

namespace NJson2Udf {
    SIMPLE_MODULE(TJson2Module,
            TParse,
            TSerialize<EDataSlot::Json>,
            TSerialize<EDataSlot::JsonDocument>,
            TCompilePath,
            TSqlValue<EDataSlot::Json, TUtf8>,
            TSqlValue<EDataSlot::Json, TUtf8, true>,
            TSqlValue<EDataSlot::Json, i64>,
            TSqlValue<EDataSlot::Json, double>,
            TSqlValue<EDataSlot::Json, bool>,
            TSqlValue<EDataSlot::JsonDocument, TUtf8>,
            TSqlValue<EDataSlot::JsonDocument, TUtf8, true>,
            TSqlValue<EDataSlot::JsonDocument, i64>,
            TSqlValue<EDataSlot::JsonDocument, double>,
            TSqlValue<EDataSlot::JsonDocument, bool>,
            TSqlExists<EDataSlot::Json, false>,
            TSqlExists<EDataSlot::Json, true>,
            TSqlExists<EDataSlot::JsonDocument, false>,
            TSqlExists<EDataSlot::JsonDocument, true>,
            TSqlQuery<EDataSlot::Json, EJsonQueryWrap::NoWrap>,
            TSqlQuery<EDataSlot::Json, EJsonQueryWrap::Wrap>,
            TSqlQuery<EDataSlot::Json, EJsonQueryWrap::ConditionalWrap>,
            TSqlQuery<EDataSlot::JsonDocument, EJsonQueryWrap::NoWrap>,
            TSqlQuery<EDataSlot::JsonDocument, EJsonQueryWrap::Wrap>,
            TSqlQuery<EDataSlot::JsonDocument, EJsonQueryWrap::ConditionalWrap>,
            TAsJsonNode<TUtf8>,
            TAsJsonNode<double>,
            TAsJsonNode<bool>,
            TAsJsonNode<TJson>)
}

REGISTER_MODULES(NJson2Udf::TJson2Module)
