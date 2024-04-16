#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/status.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <util/generic/string.h>

namespace NKikimr::NArrow::NSerialization {

class ISerializer {
protected:
    virtual TString DoSerialize(const std::shared_ptr<arrow::RecordBatch>& batch) const = 0;
public:
    using TPtr = std::shared_ptr<ISerializer>;
    virtual ~ISerializer() = default;

    TString Serialize(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        return DoSerialize(batch);
    }

    virtual bool IsHardPacker() const = 0;
};

class IDeserializer {
protected:
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoDeserialize(const TString& data) const = 0;
    virtual TString DoDebugString() const = 0;
public:
    using TPtr = std::shared_ptr<IDeserializer>;
    virtual ~IDeserializer() = default;

    TString DebugString() const {
        return DoDebugString();
    }

    arrow::Result<std::shared_ptr<arrow::RecordBatch>> Deserialize(const TString& data) const;
};

}
