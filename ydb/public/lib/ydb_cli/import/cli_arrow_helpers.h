/* 
    This file contains code copied from core/formats/arrow_helpers.h in order to cut client dependecies
*/

#pragma once 

#include <contrib/libs/apache/arrow_next/cpp/src/arrow/api.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type_traits.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/ipc/writer.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/util/compression.h>
#include <util/generic/vector.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/folder/path.h>

namespace NYdb_cli::NArrow {
    std::shared_ptr<arrow20::RecordBatch> ToBatch(const std::shared_ptr<arrow20::Table>& combinedTable);
    ui64 GetBatchDataSize(const std::shared_ptr<arrow20::RecordBatch>& batch);
    ui64 GetArrayDataSize(const std::shared_ptr<arrow20::Array>& column);
    TString SerializeSchema(const arrow20::Schema& schema);
    TString SerializeBatch(const std::shared_ptr<arrow20::RecordBatch>& batch, const arrow20::ipc::IpcWriteOptions& options);
    TString SerializeBatchNoCompression(const std::shared_ptr<arrow20::RecordBatch>& batch);
    inline bool HasNulls(const std::shared_ptr<arrow20::Array>& column) {
        return column->null_bitmap_data();
    }
}