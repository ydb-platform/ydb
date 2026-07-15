#include "yql_yt_reduce_reader.h"

#include <util/generic/array_ref.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>

namespace NYql::NFmr {

TReduceReader::TReduceReader(
    const std::vector<IBlockIterator::TPtr>& inputs,
    const TSortingColumns& reduceBy
)
    : TFmrIndexedBlockReader(inputs)
    , ReduceBy_(reduceBy)
{
    for (ui32 i = 0; i < Sources_.size(); ++i) {
        Sources_[i].EnsureRow();
        if (Sources_[i].Valid()) {
            Heap_.push_back(i);
        }
    }

    HeapComparator_ = [this](ui32 a, ui32 b) {
        return Sources_[b] < Sources_[a];
    };

    std::make_heap(Heap_.begin(), Heap_.end(), HeapComparator_);
}

TString TReduceReader::GetReduceKeyFromRow() {
    auto& s = Sources_[ActiveSource_];
    TStringBuf rowData = s.Block.GetRowBytes(s.RowIndex);
    TParserFragmentListIndex parser(rowData);
    parser.Parse();
    auto rowMarkup = parser.MoveAllColumnsRows()[0];

    TString result;
    result.append(1, NYson::NDetail::BeginMapSymbol);
    bool firstColumn = true;
    THashSet<TStringBuf> columnsToKeep(ReduceBy_.Columns.begin(), ReduceBy_.Columns.end());

    for (const auto& col : rowMarkup.Columns) {
        if (!columnsToKeep.contains(col.Name)) {
            continue;
        }

        if (!firstColumn) {
            result.append(1, NYson::NDetail::KeyedItemSeparatorSymbol);
        }
        firstColumn = false;

        const auto& kvRange = col.KeyValueRange;
        result.append(rowData.data() + kvRange.StartOffset, kvRange.EndOffset - kvRange.StartOffset);
    }
    result.append(1, NYson::NDetail::EndMapSymbol);
    result.append(1, NYson::NDetail::ListItemSeparatorSymbol);
    return result;
}


size_t TReduceReader::DoRead(void* buf, size_t len) {
    auto out = MakeArrayRef(static_cast<char*>(buf), len);
    size_t total = 0;
    size_t remaining = len;

    while (remaining > 0) {
        if (IsKeyMarkerActive_) {
            auto keySwitchMarker = TStringBuf(KeySwitchMarker);
            Y_ENSURE(KeyMarkerOffset_ <= keySwitchMarker.size(), "key marker offset out of bounds");
            const size_t avail = keySwitchMarker.size() - KeyMarkerOffset_;
            const size_t toCopy = std::min(avail, remaining);

            std::copy_n(keySwitchMarker.begin() + KeyMarkerOffset_, toCopy, out.begin() + total);
            remaining -= toCopy;
            total += toCopy;
            KeyMarkerOffset_ += toCopy;

            if (KeyMarkerOffset_ == keySwitchMarker.size()) {
                // finished writing marker, should write rows with fixed reduce key.
                IsKeyMarkerActive_ = false;
                KeyMarkerOffset_ = 0;
                continue;
            }
        }

        if (HasActive_) {
            auto& s = Sources_[ActiveSource_];
            TStringBuf row = s.Block.GetRowBytes(s.RowIndex);

            auto reduceKey = GetReduceKeyFromRow();

            if (reduceKey != CurrentReduceKey_) {
                CurrentReduceKey_ = reduceKey;
                IsKeyMarkerActive_ = true;
                ActiveOffset_ = 0;
                continue;
            }

            Y_ENSURE(ActiveOffset_ <= row.size(), "Active offset out of bounds");

            const size_t avail = row.size() - ActiveOffset_;
            const size_t toCopy = std::min(avail, remaining);

            std::copy_n(row.begin() + ActiveOffset_, toCopy, out.begin() + total);
            remaining -= toCopy;
            total += toCopy;
            ActiveOffset_ += toCopy;

            if (ActiveOffset_ == row.size()) {
                s.Next();
                if (s.Valid()) {
                    Heap_.push_back(ActiveSource_);
                    std::push_heap(Heap_.begin(), Heap_.end(), HeapComparator_);
                }
                HasActive_ = false;
            }

            continue;
        }

        if (Heap_.empty()) {
            break;
        }

        std::pop_heap(Heap_.begin(), Heap_.end(), HeapComparator_);
        ActiveSource_ = Heap_.back();
        Heap_.pop_back();
        ActiveOffset_ = 0;
        HasActive_ = true;
    }
    return total;
}

} // namespace NYql::NFmr
