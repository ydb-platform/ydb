#pragma once

#include <contrib/libs/apache/arrow/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/result.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/ipc/options.h>

namespace NYdb {

inline namespace Dev {

namespace NArrow {

arrow::Result<std::shared_ptr<arrow::Schema>> DeserializeSchema(const std::string& str);

arrow::Result<std::shared_ptr<arrow::RecordBatch>> DeserializeBatch(const std::string& blob, const std::shared_ptr<arrow::Schema>& schema);

std::string SerializeBatch(const std::shared_ptr<arrow::RecordBatch>& batch, const arrow::ipc::IpcWriteOptions& options);

arrow::Result<std::string> CombineSerializedBatches(const std::string& first, const std::string& second, const std::string& serializedSchema);

}
}
}
