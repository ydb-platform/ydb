#pragma once

#include <ydb/library/yql/providers/common/db_id_async_resolver/db_async_resolver.h>
#include <ydb/library/yql/providers/common/db_id_async_resolver/mdb_host_transformer.h>

namespace NFq {
    NYql::IMdbHostTransformer::TPtr MakeTMdbHostTransformerLegacy();
    NYql::IMdbHostTransformer::TPtr MakeTMdbHostTransformerGeneric();
    NYql::IMdbHostTransformer::TPtr MakeTMdbHostTransformerNoop();
}
