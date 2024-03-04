#pragma once
#include "abstract.h"
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/options.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/type.h>

namespace NKikimr::NArrow::NSerialization {

class TBatchPayloadSerializer: public ISerializer {
private:
    const arrow::ipc::IpcWriteOptions Options;
protected:
    virtual TString DoSerialize(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
public:
    virtual bool IsHardPacker() const override {
        return Options.codec && Options.codec->compression_type() == arrow::Compression::ZSTD && Options.codec->compression_level() > 3;
    }
    TBatchPayloadSerializer(const arrow::ipc::IpcWriteOptions& options)
        : Options(options) {

    }
};

class TBatchPayloadDeserializer: public IDeserializer {
private:
    const std::shared_ptr<arrow::Schema> Schema;
protected:
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoDeserialize(const TString& data) const override;
    virtual TString DoDebugString() const override {
        return "type=BATCH_PAYLOAD;";
    }
public:
    TBatchPayloadDeserializer(const std::shared_ptr<arrow::Schema> schema)
        : Schema(schema) {

    }
};

}
