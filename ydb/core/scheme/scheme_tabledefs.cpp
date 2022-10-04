#include "scheme_tabledefs.h"

namespace NKikimr {

bool TTableRange::IsEmptyRange(TConstArrayRef<const NScheme::TTypeInfo> types) const {
    if (Point)
        return false;

    const int compares = CompareBorders<true, false>(To, From, InclusiveTo, InclusiveFrom, types);
    return (compares < 0);
}

bool TTableRange::IsFullRange(ui32 columnsCount) const {
    if (!InclusiveFrom) {
        return false;
    }

    if (!To.empty()) {
        return false;
    }

    if (From.size() != columnsCount) {
        return false;
    }

    for (const auto& value : From) {
        if (value) {
            return false;
        }
    }

    return true;
}

namespace {
    // There are many places that use a non-inclusive -inf/+inf
    // We special case empty keys anyway, so the requirement is temporarily relaxed
    static constexpr bool RelaxEmptyKeys = true;
}

const char* TTableRange::IsAmbiguousReason(size_t keyColumnsCount) const noexcept {
    if (Point) {
        if (Y_UNLIKELY(From.size() != keyColumnsCount)) {
            return "Ambiguous table point: does not match key columns count";
        }

        return nullptr;
    }

    if (!From) {
        if (Y_UNLIKELY(!InclusiveFrom) && !RelaxEmptyKeys) {
            return "Ambiguous table range: empty From must be inclusive";
        }
    } else if (From.size() < keyColumnsCount) {
        if (Y_UNLIKELY(InclusiveFrom)) {
            return "Ambiguous table range: incomplete From must be non-inclusive (any/+inf is ambiguous otherwise)";
        }
    } else if (Y_UNLIKELY(From.size() > keyColumnsCount)) {
        return "Ambiguous table range: From is too large";
    }

    if (!To) {
        if (Y_UNLIKELY(!InclusiveTo && !RelaxEmptyKeys)) {
            return "Ambiguous table range: empty To must be inclusive";
        }
    } else if (To.size() < keyColumnsCount) {
        if (Y_UNLIKELY(!InclusiveTo)) {
            return "Ambiguous table range: incomplete To must be inclusive (any/+inf is ambiguous otherwise)";
        }
    } else if (Y_UNLIKELY(To.size() > keyColumnsCount)) {
        return "Ambiguous table range: To is too large";
    }

    return nullptr;
}

bool TSerializedTableRange::IsEmpty(TConstArrayRef<NScheme::TTypeInfo> types) const
{
    auto cmp = CompareBorders<true, false>(To.GetCells(), From.GetCells(), ToInclusive, FromInclusive, types);
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
