#pragma once

#include <ydb/library/yql/providers/native/read_stream.h>
#include <ydb/library/yql/providers/ydb_remote/proto/source.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NYql::NYdbRemote {

inline constexpr ui64 MaxInboundMessageBytes = 8 * 1024 * 1024;
// Request data envelope with bounded protobuf decoding and one SDK completion
// worker; shared gRPC/channel allocations are outside this reservation.
inline constexpr ui64 ReadMemoryReservation = 64 * 1024 * 1024;

void ValidateSource(const TSource& source);
TString BuildReadQuery(const TSource& source);
std::shared_ptr<arrow::RecordBatch> DecodeArrowResult(const NYdb::TResultSet& result,
    const TSource& source, ui64 maxBatchBytes);
std::shared_ptr<NNative::IReadStream> CreateReadStream(std::shared_ptr<NYdb::NQuery::TQueryClient> client,
    const TSource& source, const NNative::TReadContext& context);

} // namespace NYql::NYdbRemote
