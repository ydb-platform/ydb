// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "input_stream.h"
#include "sort_cursor.h"

namespace NKikimr::NArrow {

/// Merges several sorted streams into one sorted stream.
class TMergingSortedInputStream : public IInputStream {
public:
    using TBuilders = std::vector<std::unique_ptr<arrow::ArrayBuilder>>;

    TMergingSortedInputStream(const std::vector<IInputStream::TPtr>& inputs,
                              std::shared_ptr<TSortDescription> description,
                              size_t maxBatchRows,
                              ui64 limit = 0);

    std::shared_ptr<arrow::Schema> Schema() const override { return Header; }

protected:
    std::shared_ptr<arrow::RecordBatch> ReadImpl() override;

private:
    std::shared_ptr<arrow::Schema> Header;
    std::shared_ptr<TSortDescription> Description;
    const ui64 MaxBatchSize;
    ui64 Limit;
    ui64 TotalMergedRows = 0;
    bool First = true;
    bool Finished = false;
    ui64 ExpectedBatchSize = 0; /// May be smaller or equal to max_block_size. To do 'reserve' for columns.

    std::vector<std::shared_ptr<arrow::RecordBatch>> SourceBatches;
    std::shared_ptr<TReplaceKey> PrevKey;

    std::vector<TSortCursorImpl> Cursors;
    TSortingHeap Queue;

    void Init();
    void FetchNextBatch(const TSortCursor& current, TSortingHeap& queue);
    void Merge(TBuilders& builders, TSortingHeap& queue);
};

}
