#pragma once

#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/io.pb.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/mount.pb.h>
#include <ydb/core/nbs/nbs1_compat_api/cloud/blockstore/public/api/protos/ping.pb.h>

namespace NYdb::NBS::NNbs1CompatApi::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Lists the classic NBS methods supported by the NBS2 frontend MVP.
#define NBS1_COMPAT_SERVICE(xxx, ...)                                          \
    xxx(Ping, __VA_ARGS__)                                                     \
    xxx(MountVolume, __VA_ARGS__)                                              \
    xxx(UnmountVolume, __VA_ARGS__)                                            \
    xxx(ReadBlocks, __VA_ARGS__)                                               \
    xxx(WriteBlocks, __VA_ARGS__)                                              \
    // NBS1_COMPAT_SERVICE

#define NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE(xxx, ...)                          \
    NBS1_COMPAT_SERVICE(xxx, __VA_ARGS__)                                      \
    // NBS1_COMPAT_BLOCKSTORE_GRPC_SERVICE

}   // namespace NYdb::NBS::NNbs1CompatApi::NBlockStore
