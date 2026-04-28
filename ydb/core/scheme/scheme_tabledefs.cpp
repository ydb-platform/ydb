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

NKikimr::TTableRange Intersect(TConstArrayRef<NScheme::TTypeInfo> types, const TTableRange& first, const TTableRange& second)
{
    // all variants
    //=================
    //-----------[aaa]---
    //----[bbb]----------
    //=================
    //-----------[aaa]---
    //----[bbbbbb]-------
    //=================
    //-----------[aaa]---
    //----[bbbbbbbb]-----
    //=================
    //-----------[aaa]---
    //----[bbbbbbbbbb]---
    //=================
    //---------[aaa]----
    //--[bbbbbbbbbbbbb]-

    //=================
    //----[aaaaaa]----------
    //----[]----------
    //=================
    //----[aaaaaa]----------
    //----[bbb]----------
    //=================
    //----[aaaaaa]----------
    //----[bbbbbb]----------
    //=================
    //----[aaaaaa]----------
    //----[bbbbbbbb]----------


    //=================
    //-----[aaaaaaa]----------
    //-------[bbb]----------
    //=================
    //-----[aaaaaaa]----------
    //-------[bbbbb]----------
    //=================
    //-----[aaaaaaa]----------
    //-------[bbbbbbb]----------

    //=================
    //-----[aaa]----------
    //---------[]----------
    //=================
    //-----[aaa]----------
    //---------[bbb]----------


    //=================
    //-----[aaa]----------
    //------------[bbb]---

    if (first.IsEmptyRange(types)) {
        return first;
    }

    if (second.IsEmptyRange(types)) {
        return second;
    }

    int cmpFF = CompareBorders<false, false>(first.From,
                                             second.From,
                                             first.InclusiveFrom,
                                             second.InclusiveFrom,
                                             types);
    int cmpTT = CompareBorders<true, true>(first.To,
                                           second.To,
                                           first.InclusiveTo,
                                           second.InclusiveTo,
                                           types);
    int cmpFT = CompareBorders<false, true>(first.From,
                                            second.To,
                                            first.InclusiveFrom,
                                            second.InclusiveTo,
                                            types);
    int cmpTF = CompareBorders<true, false>(first.To,
                                            second.From,
                                            first.InclusiveTo,
                                            second.InclusiveFrom,
                                            types);
    if (cmpFF < 0)
    {
        if (cmpTF < 0) {
            //=================
            //-----------[aaa]----------
            //----[bbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               first.To, first.InclusiveTo);
        } else if (cmpTF == 0) {
            //=================
            //-----------[aaa]----------
            //----[bbbbbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               first.To, first.InclusiveTo);
        } else { // if (cmpTF > 0) {
            if (cmpTT < 0) {
                //=================
                //-----------[aaa]----------
                //----[bbbbbbbb]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTT == 0) {
                //=================
                //-----------[aaa]----------
                //----[bbbbbbbbbb]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            } else { // if (cmpTT > 0) {
                //=================
                //---------[aaa]----
                //--[bbbbbbbbbbbbb]-
                return TTableRange(second.From, second.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            }
        }
    } else if (cmpFF == 0) {
        if (cmpTT < 0) {
            if (cmpTF == 0) {
                //=================
                //----[aaaaaa]----------
                //----[]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTF > 0) {
                //=================
                //----[aaaaaa]----------
                //----[bbb]----------
                return TTableRange(second.From, second.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else { // if (cmpTF < 0)
                Y_ENSURE(false, "unreachable");
            }
        } else if (cmpTT == 0) {
            //=================
            //----[aaaaaa]----------
            //----[bbbbbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               first.To, first.InclusiveTo);
        } else { // if (cmpTT > 0)
            //=================
            //----[aaaaaa]----------
            //----[bbbbbbbb]----------
            return TTableRange(second.From, second.InclusiveFrom,
                               second.To, second.InclusiveTo);
        }
    } else { //if (cmpFF > 0) {
        if (cmpFT < 0) {
            if (cmpTT < 0) {
                //=================
                //-----[aaaaaaa]----------
                //-------[bbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTT == 0) {
                //=================
                //-----[aaaaaaa]----------
                //-------[bbbbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else { //if (cmpTT > 0) {
                //=================
                //-----[aaaaaaa]----------
                //-------[bbbbbbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            }
        } else if (cmpFT == 0) {
            if (cmpTT == 0) {
                //=================
                //-----[aaa]----------
                //---------[]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   first.To, first.InclusiveTo);
            } else if (cmpTT > 0) {
                //=================
                //-----[aaa]----------
                //---------[bbb]----------
                return TTableRange(first.From, first.InclusiveFrom,
                                   second.To, second.InclusiveTo);
            } else {
                // cmpTT < 0
                Y_ENSURE(false, "unreachable");
            }
        } else { // if (cmpFT > 0)
            //=================
            //-----[aaa]----------
            //------------[bbb]---
            return TTableRange(first.From, first.InclusiveFrom,
                               second.To, second.InclusiveTo);
        }
    }
}

std::vector<TPartitioning::TIntersection> TPartitioning::GetIntersectionWithRange(const std::vector<NScheme::TTypeInfo>& keyColumnTypes, const TTableRange& range) const
{
    const auto& partitions = Partitions;
    // Binary search of the index to start with.
    size_t idxStart = 0;
    size_t idxFinish = partitions.size();
    while ((idxFinish - idxStart) > 1) {
        size_t idxCur = (idxFinish + idxStart) / 2;
        const auto& partCur = partitions[idxCur].Range->EndKeyPrefix.GetCells();
        Y_ENSURE(partCur.size() <= keyColumnTypes.size());
        int cmp = CompareTypedCellVectors(partCur.data(), range.From.data(), keyColumnTypes.data(),
                                          std::min(partCur.size(), range.From.size()));
        if (cmp < 0) {
            idxStart = idxCur;
        } else {
            idxFinish = idxCur;
        }
    }

    std::vector<TCell> minusInf(keyColumnTypes.size());

    std::vector<TPartitioning::TIntersection> rangePartition;
    for (size_t idx = idxStart; idx < partitions.size(); ++idx) {
        TTableRange partitionRange{
            idx == 0 ? minusInf : partitions[idx - 1].Range->EndKeyPrefix.GetCells(),
            idx == 0 ? true : !partitions[idx - 1].Range->IsInclusive,
            partitions[idx].Range->EndKeyPrefix.GetCells(),
            partitions[idx].Range->IsInclusive
        };

        if (range.Point) {
            int intersection = ComparePointAndRange(
                range.From,
                partitionRange,
                keyColumnTypes,
                keyColumnTypes);

            if (intersection == 0) {
                rangePartition.emplace_back(partitions[idx].ShardId, TOwnedTableRange(range));
            } else if (intersection < 0) {
                break;
            }
        } else {
            int intersection = CompareRanges(range, partitionRange, keyColumnTypes);

            if (intersection == 0) {
                auto rangeIntersection = Intersect(keyColumnTypes, range, partitionRange);
                rangePartition.emplace_back(partitions[idx].ShardId, TOwnedTableRange(rangeIntersection));
            } else if (intersection < 0) {
                break;
            }
        }
    }

    return rangePartition;
}

bool TSerializedTableRange::IsEmpty(TConstArrayRef<NScheme::TTypeInfo> types) const
{
    auto cmp = CompareBorders<true, false>(To.GetCells(), From.GetCells(), ToInclusive, FromInclusive, types);
    return (cmp < 0);
}

TKeyDesc::TKeyDesc(const TVector<NScheme::TTypeInfo> &keyColumnTypes)
    : RowOperation(ERowOperation::Unknown)
    , KeyColumnTypes(keyColumnTypes.begin(), keyColumnTypes.end())
    , Reverse(false)
    , Status(EStatus::Unknown)
    , Partitioning(std::make_shared<TPartitioning>())
{}

THolder<TKeyDesc> TKeyDesc::CreateMiniKeyDesc(const TVector<NScheme::TTypeInfo> &keyColumnTypes) {
    return THolder<TKeyDesc>(new TKeyDesc(keyColumnTypes));
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
