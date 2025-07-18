#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_value.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <yql/essentials/public/udf/udf_data_type.h>

#include <util/generic/buffer.h>
#include <yql/essentials/utils/prefetch.h>

#include "tuple.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

class Histogram {
public:
    Histogram() = default;

    template <typename F, typename... Args>
    void AddData(const TTupleLayout* layout, const ui8* data, ui32 nTuples, F HashMapper, Args&&... args) {
        const auto layoutTotalRowSize = layout->TotalRowSize;

        for (ui32 i = 0; i < nTuples; i += 1 /*step*/) {
            const ui8* tuple = data + i * layoutTotalRowSize;
            const auto hash = ReadUnaligned<ui32>(tuple);
            NYql::PrefetchForRead(tuple + layoutTotalRowSize * 32); // prefetch next tuples

            ui64 overflowSize = 0;
            for (const auto& col: layout->VariableColumns) {
                ui32 size = ReadUnaligned<ui8>(tuple + col.Offset);
                if (size == 255) { // overflow buffer used
                    const auto prefixSize = (col.DataSize - 1 - 2 * sizeof(ui32));
                    const auto overflowSize = ReadUnaligned<ui32>(tuple + col.Offset + 1 + 1 * sizeof(ui32));
                    size = prefixSize + overflowSize;
                }
                overflowSize += size;
            }

            auto key = HashMapper(hash, std::forward<Args>(args)...);
            TuplesCountStatistics_[key]++;
            TuplesSizeStatistics_[key] += layoutTotalRowSize;
            OverflowSizeStatistics_[key] += overflowSize;
        }
    }

    // hist vector must have proper size
    void GetHistogram(std::vector<ui64, TMKQLAllocator<ui64>>& hist) {
        for (auto [key, count]: TuplesCountStatistics_) {
            hist[key] += count;
        }
    }

    // sizes vector must have proper size
    void EstimateSizes(std::vector<std::pair<ui64, ui64>, TMKQLAllocator<std::pair<ui64, ui64>>>& sizes)
    {
        for (auto [key, _]: TuplesSizeStatistics_) {
            auto accTupleSize = TuplesSizeStatistics_[key];
            auto accOverflowSize = OverflowSizeStatistics_[key];

            auto& [prevTS, prevOS] = sizes[key];
            prevTS += accTupleSize;
            prevOS += accOverflowSize;
        }
    }

    void Clear() {
        TuplesCountStatistics_.clear();
        TuplesSizeStatistics_.clear();
        OverflowSizeStatistics_.clear();
    }

private:
    using TMap = std::unordered_map<ui32, ui64>;

private:
    TMap TuplesCountStatistics_;
    TMap TuplesSizeStatistics_;
    TMap OverflowSizeStatistics_;
};

} // namespace NPackedTuple
} // namespace NMiniKQL
} // namespace NKikimr
