#pragma once

#include "schemeshard_identificators.h"

#include <ydb/core/protos/test_shard_control.pb.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NSchemeShard {

struct TTestShardSetInfo : public TSimpleRefCount<TTestShardSetInfo> {
    using TPtr = TIntrusivePtr<TTestShardSetInfo>;

    NKikimrClient::TTestShardControlRequest::TCmdInitialize CmdInitialize;
    THashMap<TShardIdx, TTabletId> TestShards; // ShardIdx -> TabletId
    ui64 AlterVersion = 0;

    explicit TTestShardSetInfo(ui64 alterVersion)
        : AlterVersion(alterVersion)
    {}
};

}
}
