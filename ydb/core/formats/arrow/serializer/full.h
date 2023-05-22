#pragma once

#include "abstract.h"
#include <ydb/library/accessor/accessor.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/options.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>

namespace NKikimr::NArrow::NSerialization {

class TFullDataSerializer: public ISerializer {
private:
    const arrow::ipc::IpcWriteOptions Options;
protected:
    virtual TString DoSerialize(const std::shared_ptr<arrow::RecordBatch>& batch) const override;
public:
    TFullDataSerializer(const arrow::ipc::IpcWriteOptions& options)
        : Options(options) {

    }
};

class TFullDataDeserializer: public IDeserializer {
protected:
    virtual arrow::Result<std::shared_ptr<arrow::RecordBatch>> DoDeserialize(const TString& data) const override;
public:
    TFullDataDeserializer() {

    }
};

}
