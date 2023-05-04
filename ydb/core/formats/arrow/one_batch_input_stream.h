// The code in this file is based on original ClickHouse source code
// which is licensed under Apache license v2.0
// See: https://github.com/ClickHouse/ClickHouse/

#pragma once
#include "input_stream.h"

namespace NKikimr::NArrow {

class TOneBatchInputStream : public IInputStream {
public:
    explicit TOneBatchInputStream(std::shared_ptr<arrow::RecordBatch> batch)
        : Batch(batch)
        , Header(Batch->schema())
    {}

    std::shared_ptr<arrow::Schema> Schema() const override {
        return Header;
    }

protected:
    std::shared_ptr<arrow::RecordBatch> ReadImpl() override {
        if (Batch) {
            auto out = Batch;
            Batch.reset();
            return out;
        }
        return {};
    }

private:
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::shared_ptr<arrow::Schema> Header;
};

}
