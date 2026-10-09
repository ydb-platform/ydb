#pragma once

#include <ydb/udfs/wasm/sdk/services/transport.h>

namespace NYdb::NWasm::NServices::NProfile {

// Domain types for the private P2 profile service, not a public YQL ABI.
inline constexpr uint32_t ProfileVersion = 1;
inline constexpr uint32_t MaxProfileNameBytes = 256;
inline constexpr uint32_t MaxProfilePayloadBytes = 1024;
inline constexpr uint32_t MaxProfileBatchRows = 64;
inline constexpr uint32_t MaxProfileBatchBytes = 32768;

enum class EServiceError : uint32_t { None, Transport, Decode, InvalidProfile, InvalidArguments };
enum class EProfileMode : uint64_t { Http = 3, Grpc, HttpThenGrpc, GrpcThenHttp, Parallel, HttpBatch, GrpcBatch };

struct TProfile {
    uint64_t Id = 0;
    uint32_t Score = 0;
    uint32_t NameBytes = 0;
    char Name[MaxProfileNameBytes]{};
};

struct TProfileResult {
    EServiceError Error = EServiceError::None;
    EClientError ClientError = EClientError::None;
    int32_t Code = 0;
    int32_t NativeCode = 0;
    TProfile Profile;
};

struct TProfileResults {
    uint32_t Version = ProfileVersion;
    uint32_t Count = 0;
    TProfileResult Items[2];
};

// Batch arguments carry this header followed by Count uint64 IDs; results carry
// the same header followed by Count TProfileResult items, in input order.
struct TProfileBatchHeader {
    uint32_t Version = ProfileVersion;
    uint32_t Count = 0;
    uint32_t MaxBytes = MaxProfileBatchBytes;
    uint32_t Reserved = 0;
};

struct TProfileBatchResults {
    TProfileBatchHeader Header;
    TProfileResult Items[MaxProfileBatchRows];
};

static_assert(sizeof(TProfileBatchHeader) == 16 && std::is_trivially_copyable_v<TProfileBatchResults>);

static_assert(sizeof(TProfile) == 272 && sizeof(TProfileResult) == 288 && sizeof(TProfileResults) == 584);
static_assert(std::is_trivially_copyable_v<TProfileResults>);

} // namespace NYdb::NWasm::NServices::NProfile
