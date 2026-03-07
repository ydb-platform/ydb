#include <ydb/core/scheme_types/scheme_type_info.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_stat_table.h>

#include "split.h"

namespace {

using namespace NKikimr;

static bool IsIntegerType(NScheme::TTypeInfo type) {
    switch (type.GetTypeId()) {
    case NScheme::NTypeIds::Bool:

    case NScheme::NTypeIds::Int8:
    case NScheme::NTypeIds::Uint8:
    case NScheme::NTypeIds::Int16:
    case NScheme::NTypeIds::Uint16:
    case NScheme::NTypeIds::Int32:
    case NScheme::NTypeIds::Uint32:
    case NScheme::NTypeIds::Int64:
    case NScheme::NTypeIds::Uint64:

    case NScheme::NTypeIds::Date:
    case NScheme::NTypeIds::Datetime:
    case NScheme::NTypeIds::Timestamp:
    case NScheme::NTypeIds::Interval:
    case NScheme::NTypeIds::Date32:
    case NScheme::NTypeIds::Datetime64:
    case NScheme::NTypeIds::Timestamp64:
    case NScheme::NTypeIds::Interval64:
        return true;

    default:
        return false;
    }
}

}  // anonymous namespace

namespace NKikimr::NSplitMerge {

TSerializedCellVec SelectShortestMedianKeyPrefix(const NKikimr::NTable::THistogram& histogram, ui64 totalSize, const TKeyTypes& keyColumnTypes) {
    if (histogram.empty()) {
        return {};
    }

    ui64 idxLo = Max<ui64>(), idxMed = Max<ui64>(), idxHi = Max<ui64>();
    { // search for median and acceptable bounds range so that after the split smallest size is >= 25%
        ui64 idxMedDiff = Max<ui64>(), idx = 0;
        for (const auto& point : histogram) {
            ui64 leftSize = Min(point.Value, totalSize);
            ui64 rightSize = totalSize - leftSize;

            // search for a median point at which abs(leftSize - rightSize) is minimum
            ui64 sizesDiff = Max(leftSize, rightSize) - Min(leftSize, rightSize);
            if (idxMedDiff > sizesDiff) {
                idxMed = idx;
                idxMedDiff = sizesDiff;
            }

            if (leftSize * 4 >= totalSize && idxLo == Max<ui64>()) {
                idxLo = idx; // first point at which leftSize >= 25%
            }
            if (rightSize * 4 >= totalSize) {
                idxHi = idx; // last point at which rightSize >= 25%
            }

            idx++;
        }

        bool canSplit = idxLo != Max<ui64>() && idxLo <= idxMed && idxMed <= idxHi && idxHi != Max<ui64>();

        if (!canSplit) {
            return {};
        }
    }

    TSerializedCellVec keyLo(histogram[idxLo].EndKey);
    TSerializedCellVec keyMed(histogram[idxMed].EndKey);
    TSerializedCellVec keyHi(histogram[idxHi].EndKey);

    TVector<TCell> splitKey(keyMed.GetCells().size());

    for (size_t i = 0; i < keyMed.GetCells().size(); ++i) {
        auto columnType = keyColumnTypes[i];

        if (0 == CompareTypedCells(keyLo.GetCells()[i], keyHi.GetCells()[i], columnType)) {
            // lo == hi, so we add this value and proceed to the next column
            splitKey[i] = keyLo.GetCells()[i];
            continue;
        }

        if (0 != CompareTypedCells(keyLo.GetCells()[i], keyMed.GetCells()[i], columnType)) {
            // med != lo
            splitKey[i] = keyMed.GetCells()[i];
        } else {
            // med == lo and med != hi, so we want to find a value that is > med and <= hi
            // TODO: support this optimization for integer pg types
            if (IsIntegerType(columnType) && !keyMed.GetCells()[i].IsNull()) {
                // For integer types we can add 1 to med
                ui64 val = 0;
                size_t sz =  keyMed.GetCells()[i].Size();
                Y_ABORT_UNLESS(sz <= sizeof(ui64));
                memcpy(&val, keyMed.GetCells()[i].Data(), sz);
                val++;
                splitKey[i] = TCell((const char*)&val, sz);
            } else {
                // For other types let's do binary search between med and hi to find smallest key > med

                // Compares only i-th cell in keys
                auto fnCmpCurrentCell = [i, columnType] (const auto& keyMed, const auto& bucket) {
                    TSerializedCellVec bucketCells(bucket.EndKey);
                    return CompareTypedCells(keyMed.GetCells()[i], bucketCells.GetCells()[i], columnType) < 0;
                };

                const auto bucketsBegin = histogram.begin();
                const auto it = UpperBound(
                            bucketsBegin + idxMed,
                            bucketsBegin + idxHi,
                            keyMed,
                            fnCmpCurrentCell);
                TSerializedCellVec keyFound(it->EndKey);
                splitKey[i] = keyFound.GetCells()[i];
            }
        }
        break;
    }

    return TSerializedCellVec(splitKey);
}

}  // namespace NKikimr::NSplitMerge
