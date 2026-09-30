#pragma once

#include <ydb/library/yql/providers/native/read_stream.h>
#include <ydb/library/yql/providers/ydb_remote/proto/source.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NYql::NYdbRemote {

// The baseline SDK decodes protobuf/compression before provider validation.
// This transport limit and the decoded batch checks are not an RM reservation
// or a hard bound on allocations inside the SDK.
inline constexpr ui64 MaxInboundMessageBytes = 8 * 1024 * 1024;

void ValidateSource(const TSource& source);
TString BuildReadQuery(const TSource& source);
std::shared_ptr<arrow::RecordBatch> DecodeArrowResult(const NYdb::TResultSet& result,
    const TSource& source, ui64 maxBatchBytes);
std::shared_ptr<NNative::IReadStream> CreateReadStream(std::shared_ptr<NYdb::NQuery::TQueryClient> client,
    const TSource& source, const NNative::TReadContext& context);

} // namespace NYql::NYdbRemote
