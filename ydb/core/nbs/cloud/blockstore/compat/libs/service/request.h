#pragma once

#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/io.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/mount.pb.h>
#include <ydb/core/nbs/cloud/blockstore/compat/public/api/protos/ping.pb.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Lists the classic NBS methods supported by the NBS2 frontend MVP.
#define BLOCKSTORE_SERVICE(xxx, ...)                                           \
    xxx(Ping, __VA_ARGS__)                                                     \
    xxx(MountVolume, __VA_ARGS__)                                              \
    xxx(UnmountVolume, __VA_ARGS__)                                            \
    xxx(ReadBlocks, __VA_ARGS__)                                               \
    xxx(WriteBlocks, __VA_ARGS__)                                              \
    // BLOCKSTORE_SERVICE

#define BLOCKSTORE_GRPC_SERVICE(xxx, ...)                                      \
    BLOCKSTORE_SERVICE(xxx, __VA_ARGS__)                                       \
    // BLOCKSTORE_GRPC_SERVICE

}   // namespace NCloud::NBlockStore
