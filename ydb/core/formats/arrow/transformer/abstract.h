#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow::NTransformation {

class ITransformer {
protected:
    virtual std::shared_ptr<arrow::RecordBatch> DoTransform(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;
    virtual TString DoDebugString() const = 0;
public:
    using TPtr = std::shared_ptr<ITransformer>;
    virtual ~ITransformer() = default;

    TString DebugString() const {
        return DoDebugString();
    }

    std::shared_ptr<arrow::RecordBatch> Transform(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        return DoTransform(batch);
    }
};

}
