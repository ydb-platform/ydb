#pragma once

#include <ydb/library/yql/providers/native/read_stream.h>
#include <ydb/library/yql/providers/ydb_external/common/read_limits.h>
#include <ydb/library/yql/providers/ydb_external/proto/source.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NYql::NYdbExternal {

void ValidateSource(const TSource& source);
TString BuildReadQuery(const TSource& source);
std::shared_ptr<arrow::RecordBatch> DecodeArrowResult(const NYdb::TResultSet& result,
    const TSource& source, ui64 maxDecodedBytes);
// Accepts a validated, Bool-normalized batch from DecodeArrowResult. Copies the
// largest prefix fitting the output target, or one row up to the
// explicit row limit. Output buffers never retain the input IPC allocation.
std::shared_ptr<arrow::RecordBatch> TakeOutputBatch(const arrow::RecordBatch& batch,
    int64_t offset, ui64 targetBytes, ui64 maxRowBytes);
std::shared_ptr<NNative::IReadStream> CreateReadStream(std::shared_ptr<NYdb::NQuery::TQueryClient> client,
    const TSource& source, const NNative::TReadContext& context);

} // namespace NYql::NYdbExternal
