#pragma once

#include <ydb/core/protos/blobstorage_ddisk.pb.h>

#include <util/generic/vector.h>
#include <util/system/types.h>

#include <array>

namespace NKikimr::NNbsDbgLike {

constexpr ui32 kHostsPerDbgMax = 5;
constexpr ui32 kPrimaryHostsPerDbg = 3;
constexpr ui32 kMinHostsPerDbg = kPrimaryHostsPerDbg;
constexpr ui32 kSectorSize = 4096;

// Upper bounds (microseconds) for load-actor op ResponseTimeUs Solomon histogram.
inline const TVector<double>& LoadActorResponseTimeUsBounds() {
    static const TVector<double> bounds = [] {
        TVector<double> b;
        b.reserve(64);
        for (double v = 10; v <= 100; v += 10) {
            b.push_back(v);
        }
        for (double v = 150; v <= 1000; v += 50) {
            b.push_back(v);
        }
        for (double v = 1250; v <= 5000; v += 250) {
            b.push_back(v);
        }
        b.push_back(10000);
        b.push_back(32000);
        return b;
    }();
    return bounds;
}

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
