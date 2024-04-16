// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <queue>
#include "merging_sorted_input_stream.h"
#include "switch_type.h"
#include "size_calcer.h"

namespace NKikimr::NArrow {

class TRowsBuffer : public IRowsBuffer {
public:
    using TBuilders = std::vector<std::unique_ptr<arrow::ArrayBuilder>>;

    static constexpr const size_t BUFFER_SIZE = 256;

    TRowsBuffer(TBuilders& columns, size_t maxRows)
        : Columns(columns)
        , MaxRows(maxRows)
    {
        Rows.reserve(BUFFER_SIZE);
    }

    bool AddRow(const TSortCursor& cursor) override {
        Rows.emplace_back(cursor->all_columns, cursor->getRow());
        if (Rows.size() >= BUFFER_SIZE) {
            Flush();
        }
        ++AddedRows;
        return true;
    }

    void Flush() override {
        if (Rows.empty()) {
            return;
        }
        for (size_t i = 0; i < Columns.size(); ++i) {
            arrow::ArrayBuilder& builder = *Columns[i];
            for (auto& [srcColumn, rowPosition] : Rows) {
                Y_ABORT_UNLESS(Append(builder, *srcColumn->at(i), rowPosition));
            }
        }
        Rows.clear();
    }

    bool Limit() const override {
        return MaxRows && (AddedRows >= MaxRows);
    }

    bool HasLimit() const override {
        return MaxRows;
    }

private:
    TBuilders& Columns;
    std::vector<std::pair<const TArrayVec*, size_t>> Rows;
    size_t MaxRows = 0;
    size_t AddedRows = 0;
};

class TSlicedRowsBuffer : public IRowsBuffer {
public:
    TSlicedRowsBuffer(size_t maxRows)
        : MaxRows(maxRows)
    {}

    bool AddRow(const TSortCursor& cursor) override {
        if (!Batch) {
            Batch = cursor->current_batch;
            Offset = cursor->getRow();
        }
        if (Batch.get() != cursor->current_batch.get()) {
            // append from another batch
            return false;
        } else if (cursor->getRow() != (Offset + AddedRows)) {
            // append from the same batch with data hole
            return false;
        }
        ++AddedRows;
        return true;
    }

    void Flush() override {
    }

    bool Limit() const override {
        return MaxRows && (AddedRows >= MaxRows);
    }

    bool HasLimit() const override {
        return MaxRows;
    }

    std::shared_ptr<arrow::RecordBatch> GetBatch() {
        if (Batch) {
            return Batch->Slice(Offset, AddedRows);
        }
        return {};
    }

private:
    std::shared_ptr<arrow::RecordBatch> Batch;
    size_t Offset = 0;
    size_t MaxRows = 0;
    size_t AddedRows = 0;
};

TMergingSortedInputStream::TMergingSortedInputStream(const std::vector<IInputStream::TPtr>& inputs,
                                                     std::shared_ptr<TSortDescription> description,
                                                     size_t maxBatchRows, bool slice)
    : Description(description)
    , MaxBatchSize(maxBatchRows)
    , SliceSources(slice)
    , SourceBatches(inputs.size())
    , Cursors(inputs.size())
{
    Children.insert(Children.end(), inputs.begin(), inputs.end());
    Header = Children.at(0)->Schema();
}

/// Read the first blocks, initialize the queue.
void TMergingSortedInputStream::Init() {
    Y_ABORT_UNLESS(First);
    First = false;
    size_t totalRows = 0;
    for (size_t i = 0; i < SourceBatches.size(); ++i) {
        auto& batch = SourceBatches[i];
        if (batch) {
            continue;
        }

        batch = Children[i]->Read();
        if (!batch || batch->num_rows() == 0) {
            continue;
        }

        for (i32 i = 0; i < batch->num_columns(); ++i) {
            ColumnSize[batch->column_name(i)] += NArrow::GetArrayDataSize(batch->column(i));
        }

        totalRows += batch->num_rows();
        Cursors[i] = TSortCursorImpl(batch, Description, i);
    }

    ExpectedBatchSize = MaxBatchSize ? std::min(totalRows, MaxBatchSize) : totalRows;
    if (MaxBatchSize && MaxBatchSize < totalRows) {
        ColumnSize.clear();
    }

    Queue = TSortingHeap(Cursors, Description->NotNull);

    /// Let's check that all source blocks have the same structure.
    for (const auto& batch : SourceBatches) {
        if (batch) {
            Y_DEBUG_ABORT_UNLESS(batch->schema()->Equals(*Header));
        }
    }
}

std::shared_ptr<arrow::RecordBatch> TMergingSortedInputStream::ReadImpl() {
    if (Finished) {
        return {};
    }

    if (Children.size() == 1 && !Description->Replace()) {
        return Children[0]->Read();
    }

    if (First) {
        Init();
    }

    if (SliceSources) {
        Y_DEBUG_ABORT_UNLESS(!Description->Reverse);
        TSlicedRowsBuffer rowsBuffer(MaxBatchSize);
        Merge(rowsBuffer, Queue);
        auto batch = rowsBuffer.GetBatch();
        Y_ABORT_UNLESS(batch);
        if (!batch->num_rows()) {
            Y_ABORT_UNLESS(Finished);
            return {};
        }
        return batch;
    } else {
        auto builders = NArrow::MakeBuilders(Header, ExpectedBatchSize, ColumnSize);
        if (builders.empty()) {
            return {};
        }

        Y_ABORT_UNLESS(builders.size() == (size_t)Header->num_fields());
        TRowsBuffer rowsBuffer(builders, MaxBatchSize);
        Merge(rowsBuffer, Queue);

        auto arrays = NArrow::Finish(std::move(builders));
        Y_ABORT_UNLESS(arrays.size());
        if (!arrays[0]->length()) {
            Y_ABORT_UNLESS(Finished);
            return {};
        }
        return arrow::RecordBatch::Make(Header, arrays[0]->length(), arrays);
    }
}

/// Get the next block from the corresponding source, if there is one.
void TMergingSortedInputStream::FetchNextBatch(const TSortCursor& current, TSortingHeap& queue) {
    size_t order = current->order;
    Y_ABORT_UNLESS(order < Cursors.size() && &Cursors[order] == current.Impl);

    while (true) {
        SourceBatches[order] = Children[order]->Read();
        auto& batch = SourceBatches[order];

        if (!batch) {
            queue.RemoveTop();
            break;
        }

        if (batch->num_rows()) {
            Y_DEBUG_ABORT_UNLESS(batch->schema()->Equals(*Header));

            Cursors[order].Reset(batch);
            queue.ReplaceTop(TSortCursor(&Cursors[order], Description->NotNull));
            break;
        }
    }
}

/// Take rows in required order and put them into `rowBuffer`,
/// while the number of rows are no more than `max_block_size`
template <bool replace, bool limit>
void TMergingSortedInputStream::MergeImpl(IRowsBuffer& rowsBuffer, TSortingHeap& queue) {
    if constexpr (replace) {
        if (!PrevKey && queue.IsValid()) {
            auto current = queue.Current();
            PrevKey = std::make_shared<TReplaceKey>(current->replace_columns, current->getRow());
            if (!rowsBuffer.AddRow(current)) {
                return;
            }
            // Do not get Next() for simplicity. Lead to a dup
        }
    }

    while (queue.IsValid()) {
        if constexpr (limit) {
            if (rowsBuffer.Limit()) {
                return;
            }
        }

        auto current = queue.Current();

        if constexpr (replace) {
            TReplaceKey key(current->replace_columns, current->getRow());

            if (key == *PrevKey) {
                // do nothing
            } else if (rowsBuffer.AddRow(current)) {
                *PrevKey = key;
            } else {
                return;
            }
        } else {
            if (!rowsBuffer.AddRow(current)) {
                return;
            }
        }

        if (!current->isLast()) {
            queue.Next();
        } else {
            rowsBuffer.Flush();
            FetchNextBatch(current, queue);
        }
    }

    /// We have read all data. Ask children to cancel providing more data.
    Cancel();
    Finished = true;
}

void TMergingSortedInputStream::Merge(IRowsBuffer& rowsBuffer, TSortingHeap& queue) {
    const bool replace = Description->Replace();
    const bool limit = rowsBuffer.HasLimit();

    if (replace) {
        if (limit) {
            MergeImpl<true, true>(rowsBuffer, queue);
        } else {
            MergeImpl<true, false>(rowsBuffer, queue);
        }
    } else {
        if (limit) {
            MergeImpl<false, true>(rowsBuffer, queue);
        } else {
            MergeImpl<false, false>(rowsBuffer, queue);
        }
    }

    rowsBuffer.Flush();
}

}
