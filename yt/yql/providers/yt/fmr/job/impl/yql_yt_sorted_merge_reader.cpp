#include "yql_yt_sorted_merge_reader.h"

#include <algorithm>

#include <util/generic/array_ref.h>

namespace NYql::NFmr {

TSortedMergeReader::TSortedMergeReader(
    const std::vector<IBlockIterator::TPtr>& inputs
)
    : TFmrIndexedBlockReader(inputs)
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

size_t TSortedMergeReader::DoRead(void* buf, size_t len) {
    auto out = MakeArrayRef(static_cast<char*>(buf), len);
    size_t total = 0;
    size_t remaining = len;

    while (remaining > 0) {
        if (HasActive_) {
            auto& s = Sources_[ActiveSource_];
            TStringBuf row = s.Block.GetRowBytes(s.RowIndex);

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
