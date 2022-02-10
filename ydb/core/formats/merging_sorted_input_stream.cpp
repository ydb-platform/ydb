// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#include <queue>
#include "merging_sorted_input_stream.h"
#include "switch_type.h"

namespace NKikimr::NArrow {

TMergingSortedInputStream::TMergingSortedInputStream(const std::vector<IInputStream::TPtr>& inputs,
                                                     std::shared_ptr<TSortDescription> description,
                                                     size_t maxBatchRows,
                                                     ui64 limit)
    : Description(description)
    , MaxBatchSize(maxBatchRows)
    , Limit(limit)
    , SourceBatches(inputs.size())
    , Cursors(inputs.size())
{
    Y_VERIFY(MaxBatchSize);
    Children.insert(Children.end(), inputs.begin(), inputs.end());
    Header = Children.at(0)->Schema();
}

/// Read the first blocks, initialize the queue.
void TMergingSortedInputStream::Init() {
    Y_VERIFY(First);
    First = false;

    for (size_t i = 0; i < SourceBatches.size(); ++i) {
        auto& batch = SourceBatches[i];
        if (batch) {
            continue;
        }

        batch = Children[i]->Read();
        if (!batch || batch->num_rows() == 0) {
            continue;
        }

        const size_t rows = batch->num_rows();
        if (ExpectedBatchSize < rows) {
            ExpectedBatchSize = std::min(rows, MaxBatchSize);
        }

        Cursors[i] = TSortCursorImpl(batch, Description, i);
    }

    Queue = TSortingHeap(Cursors, Description->NotNull);

    /// Let's check that all source blocks have the same structure.
    for (const auto& batch : SourceBatches) {
        if (batch) {
            Y_VERIFY(batch->schema()->Equals(*Header));
        }
    }
}

std::shared_ptr<arrow::RecordBatch> TMergingSortedInputStream::ReadImpl() {
    if (Finished) {
        return {};
    }

    if (Children.size() == 1 && !Description->Replace() && !Limit) {
        return Children[0]->Read();
    }

    if (First) {
        Init();
    }
    auto builders = NArrow::MakeBuilders(Header, ExpectedBatchSize);
    if (builders.empty()) {
        return {};
    }

    Y_VERIFY(builders.size() == (size_t)Header->num_fields());
    Merge(builders, Queue);

    auto arrays = NArrow::Finish(std::move(builders));
    Y_VERIFY(arrays.size());
    return arrow::RecordBatch::Make(Header, arrays[0]->length(), arrays);
}

/// Get the next block from the corresponding source, if there is one.
void TMergingSortedInputStream::FetchNextBatch(const TSortCursor& current, TSortingHeap& queue) {
    size_t order = current->order;
    Y_VERIFY(order < Cursors.size() && &Cursors[order] == current.Impl);

    while (true) {
        SourceBatches[order] = Children[order]->Read();
        auto& batch = SourceBatches[order];

        if (!batch) {
            queue.RemoveTop();
            break;
        }

        if (batch->num_rows()) {
#if 1
            Y_VERIFY(batch->schema()->Equals(*Header));
#endif
            Cursors[order].Reset(batch);
            queue.ReplaceTop(TSortCursor(&Cursors[order], Description->NotNull));
            break;
        }
    }
}

static void AppendResetRows(TMergingSortedInputStream::TBuilders& columns,
                            std::vector<std::pair<const TArrayVec*, size_t>>& rows) {
    if (rows.empty()) {
        return;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        arrow::ArrayBuilder& builder = *columns[i];
        for (auto& [srcColumn, rowPosition] : rows) {
            Append(builder, *srcColumn->at(i), rowPosition);
        }
    }
    rows.clear();
}

/// Take rows in required order and put them into `mergedColumns`,
/// while the number of rows are no more than `max_block_size`
void TMergingSortedInputStream::Merge(TBuilders& mergedColumns, TSortingHeap& queue) {
    static constexpr const size_t BUFFER_SIZE = 256;
    std::vector<std::pair<const TArrayVec*, size_t>> rowsBuffer;
    rowsBuffer.reserve(BUFFER_SIZE);

    for (size_t mergedRows = 0; queue.IsValid();) {
        if (Limit && TotalMergedRows >= Limit) {
            break;
        }
        if (mergedRows >= MaxBatchSize) {
            AppendResetRows(mergedColumns, rowsBuffer);
            return;
        }

        auto current = queue.Current();

        bool append = true;
        if (Description->Replace()) {
            auto key = std::make_shared<TReplaceKey>(current->replace_columns, current->getRow());
            if (PrevKey && *key == *PrevKey) {
                append = false;
            }
            PrevKey = key;
        }

        if (append) {
            rowsBuffer.emplace_back(current->all_columns.get(), current->getRow());
            ++mergedRows;
            ++TotalMergedRows;
        }

        if (rowsBuffer.size() == BUFFER_SIZE) {
            AppendResetRows(mergedColumns, rowsBuffer);
        }

        if (!current->isLast()) {
            queue.Next();
        } else {
            AppendResetRows(mergedColumns, rowsBuffer);
            FetchNextBatch(current, queue);
        }
    }

    AppendResetRows(mergedColumns, rowsBuffer);

    /// We have read all data. Ask children to cancel providing more data.
    Cancel();
    Finished = true;
}

}
