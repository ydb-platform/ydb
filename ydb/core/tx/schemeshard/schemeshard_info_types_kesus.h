#pragma once

#include "schemeshard_info_types_base.h"

#include <ydb/public/api/protos/ydb_coordination.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TKesusInfo : public TSimpleRefCount<TKesusInfo> {
    using TPtr = TIntrusivePtr<TKesusInfo>;

    TShardIdx KesusShardIdx = InvalidShardIdx;
    TTabletId KesusTabletId = InvalidTabletId;
    Ydb::Coordination::Config Config;
    ui64 Version = 0;
    THolder<Ydb::Coordination::Config> AlterConfig;
    ui64 AlterVersion = 0;

    void FinishAlter() {
        Y_ENSURE(AlterConfig, "No alter config at Alter completion");
        Y_ENSURE(AlterVersion, "No alter version at Alter completion");
        Config.CopyFrom(*AlterConfig);
        ++Version;
        Y_ENSURE(Version == AlterVersion);
        AlterConfig.Reset();
        AlterVersion = 0;
    }
};

}
}
