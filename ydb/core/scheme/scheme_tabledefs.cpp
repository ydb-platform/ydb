#include "scheme_tabledefs.h"

namespace NKikimr {

bool TTableRange::IsEmptyRange(TConstArrayRef<const NScheme::TTypeId> cellTypeIds) const {
    if (Point)
        return false;

    const int compares = CompareBorders<true, false>(To, From, InclusiveTo, InclusiveFrom, cellTypeIds);
    return (compares < 0);
}

bool TTableRange::IsFullRange(ui32 columnsCount) const {
    if (!InclusiveFrom) {
        return false;
    }

    if (!To.empty()) {
        return false;
    }

    if (!From.size() == columnsCount) {
        return false;
    }

    for (const auto& value : From) {
        if (value) {
            return false;
        }
    }

    return true;
}

bool TSerializedTableRange::IsEmpty(TConstArrayRef<NScheme::TTypeId> type) const
{
    auto cmp = CompareBorders<true, false>(To.GetCells(), From.GetCells(), ToInclusive, FromInclusive, type);
    return (cmp < 0);
}

void TKeyDesc::Out(IOutputStream& o, TKeyDesc::EStatus x) {
#define KEYDESCRIPTION_STATUS_TO_STRING_IMPL(name, ...) \
    case EStatus::name: \
        o << #name; \
        return;

    switch (x) {
        SCHEME_KEY_DESCRIPTION_STATUS_MAP(KEYDESCRIPTION_STATUS_TO_STRING_IMPL)
    default:
        o << static_cast<int>(x);
        return;
    }
}

struct TSystemColumnsData {
    const TString PartitionColumnName = "_yql_partition_id";

    const TMap<TString, TSystemColumnInfo> SystemColumns = {
        {PartitionColumnName, {TKeyDesc::EColumnIdDataShard, NScheme::NTypeIds::Uint64}}
    };
};

bool IsSystemColumn(ui32 columnId) {
    switch (columnId) {
    case TKeyDesc::EColumnIdDataShard:
        return true;
    default:
        return false;
    }
}

bool IsSystemColumn(const TStringBuf columnName) {
    return GetSystemColumns().FindPtr(columnName);
}

const TMap<TString, TSystemColumnInfo>& GetSystemColumns() {
    return Singleton<TSystemColumnsData>()->SystemColumns;
}

}
