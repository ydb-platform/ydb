#include "test_tablet.h"
#include "test_shard_impl.h"

namespace NKikimr::NTestShard {

    IActor *CreateTestShard(const TActorId& tablet, TTabletStorageInfo *info) {
        return new NTestShard::TTestShard(tablet, info);
    }

} // NKikimr::NTestShard
