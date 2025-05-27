#pragma once
#include <ydb/core/tx/schemeshard/olap/columns/schema.h>
#include <ydb/core/tx/schemeshard/olap/indexes/schema.h>

namespace NKikimr::NSchemeShard {
struct TOperationContext;
}

namespace NKikimr::NSchemeShard {
    class TTTLValidator {
    public:
        static bool ValidateColumnTableTtl(const NKikimrSchemeOp::TColumnDataLifeCycle::TTtl& ttl, const TOlapIndexesDescription& indexes,
            const THashMap<ui32, TOlapColumnsDescription::TColumn>& sourceColumns,
            const THashMap<ui32, TOlapColumnsDescription::TColumn>& alterColumns,
            const THashMap<TString, ui32>& colName2Id, const TOperationContext& context,
            IErrorCollector& errors);

    };
}
