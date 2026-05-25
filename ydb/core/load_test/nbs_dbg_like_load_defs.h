#pragma once

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <util/system/types.h>

#include <array>

namespace NKikimr::NNbsDbgLike {

constexpr ui32 kHostsPerDbgMax = 5;
constexpr ui32 kPrimaryHostsPerDbg = 3;
constexpr ui32 kMinHostsPerDbg = kPrimaryHostsPerDbg;
constexpr ui32 kSectorSize = 4096;

struct TDirectBlockGroup {
    ui32 DbgIndex = 0;
    ui64 DirectBlockGroupId = 0;

    std::array<NKikimrBlobStorage::NDDisk::TDDiskId, kHostsPerDbgMax> DDiskIds{};
    std::array<NKikimrBlobStorage::NDDisk::TDDiskId, kHostsPerDbgMax> PBIds{};

    std::array<ui64, kHostsPerDbgMax> DDGuid{};
    std::array<ui64, kHostsPerDbgMax> PBGuid{};
};

enum class ETabletPhase {
    // no AllocConfig persisted (also the value used
    // before the schema has been loaded - the
    // KV-flat base intercepts all events until
    // CreatedHook fires, so we never observe this
    // value from outside)
    Uninitialized,

    Allocating,
    Ready,
    Deleting,
};

enum class EPBufferState {
    PBufferIncompleteWrite = 0,
    PBufferWritten,
    PBufferFlushing,
    PBufferFlushed,
    PBufferErasing,
    PBufferErased,
};

enum class EOp {
    Write = 0,
    Flush,
    Erase,
    ReadPB ,
    ReadDDisk,
    OpCount,
};

enum class EDecodeAddressError {
    InvalidIoSize,
    NotConfigured,
    DbgOutOfRange,
    VChunkOutOfRange,
    CrossesVChunkBoundary,
};

} // namespace NKikimr::NNbsDbgLike
