#include "yql_yt_sorted_merge_reader.h"

#include <yt/yql/providers/yt/fmr/utils/comparator/yql_yt_binary_yson_compare_impl.h>

#include <algorithm>

#include <util/generic/array_ref.h>

namespace NYql::NFmr {

int TSortedMergeReader::TSourceState::CompareTo(const TSourceState& rhs) const {
    int c = CompareKeyRowsAcrossYsonBlocks(
        Block.Data,
        Markup(),
        rhs.Block.Data,
        rhs.Markup(),
        SortOrders.get()
    );
    if (c != 0) {
        return c;
    }

    // stable sort
    if (SourceId < rhs.SourceId) {
        return -1;
    }
    if (SourceId > rhs.SourceId) {
        return 1;
    }
    return 0;
}

bool TSortedMergeReader::TSourceState::operator<(const TSourceState& rhs) const {
    return CompareTo(rhs) < 0;
}

void TSortedMergeReader::TSourceState::EnsureRow() {
    while (!Eof && (Block.Rows.empty() || RowIndex >= Block.Rows.size())) {
        TIndexedBlock next;
        if (!It || !It->NextBlock(next)) {
            Eof = true;
            Block = {};
            RowIndex = 0;
            break;
        }
        Block = std::move(next);
        RowIndex = 0;
    }
}

bool TSortedMergeReader::TSourceState::Valid() const {
    return !Eof && !Block.Rows.empty() && RowIndex < Block.Rows.size();
}

const TRowIndexMarkup& TSortedMergeReader::TSourceState::Markup() const {
    return Block.Rows[RowIndex];
}

TStringBuf TSortedMergeReader::TSourceState::RowBytes() const {
    auto boundary = Markup().back();

    // If row with a separator
    if (boundary.EndOffset < Block.Data.size() && Block.Data[boundary.EndOffset] == ';') {
        ++boundary.EndOffset;
    }
    return SliceRange(Block.Data, boundary);
}

void TSortedMergeReader::TSourceState::Next() {
    ++RowIndex;
    EnsureRow();
}

int TSortedMergeReader::CompareSources(ui32 lhsSourceId, ui32 rhsSourceId) const {
    const auto& lhs = Sources_[lhsSourceId];
    const auto& rhs = Sources_[rhsSourceId];
    return lhs.CompareTo(rhs);
}

TSortedMergeReader::TSortedMergeReader(
    std::vector<IBlockIterator::TPtr> inputs,
    std::vector<ESortOrder> sortOrders
)
    : SortOrders_(std::move(sortOrders))
{
    Sources_.reserve(inputs.size());
    for (ui32 i = 0; i < inputs.size(); ++i) {
        Sources_.push_back(TSourceState{
            .SourceId = i,
            .SortOrders = std::cref(SortOrders_),
            .It = std::move(inputs[i]),
        });
    }

    const auto heapCmp = [this](ui32 a, ui32 b) {
        return Sources_[b] < Sources_[a];
    };

    for (ui32 i = 0; i < Sources_.size(); ++i) {
        Sources_[i].EnsureRow();
        if (Sources_[i].Valid()) {
            Heap_.push_back(i);
        }
    }

    std::make_heap(Heap_.begin(), Heap_.end(), heapCmp);
}

bool TSortedMergeReader::Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) {
    return false;
}

void TSortedMergeReader::ResetRetries() {
}

bool TSortedMergeReader::HasRangeIndices() const {
    return false;
}

size_t TSortedMergeReader::DoRead(void* buf, size_t len) {
    auto out = MakeArrayRef(static_cast<char*>(buf), len);
    size_t total = 0;
    size_t remaining = len;

    const auto heapCmp = [this](ui32 a, ui32 b) {
        return Sources_[b] < Sources_[a];
    };

    while (remaining > 0) {
        if (HasActive_) {
            auto& s = Sources_[ActiveSource_];
            TStringBuf row = s.RowBytes();

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
                    std::push_heap(Heap_.begin(), Heap_.end(), heapCmp);
                }
                HasActive_ = false;
            }

            continue;
        }

        if (Heap_.empty()) {
            break;
        }

        std::pop_heap(Heap_.begin(), Heap_.end(), heapCmp);
        ActiveSource_ = Heap_.back();
        Heap_.pop_back();
        ActiveOffset_ = 0;
        HasActive_ = true;
    }

    return total;
}

} // namespace NYql::NFmr

