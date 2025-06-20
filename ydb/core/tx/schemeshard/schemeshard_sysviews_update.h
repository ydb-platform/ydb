#pragma once

#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NSchemeShard {

struct TModifySysViewRequestInfo {
    NKikimrSchemeOp::EOperationType OperationType;
    TString WorkingDir;
    TString TargetName;
    TMaybe<NKikimrSysView::ESysViewType> SysViewType;

    TString DebugString() const;
};

THolder<NActors::IActor> CreateSysViewsRosterUpdate(TTabletId selfTabletId, NActors::TActorId selfActorId,
                                                    TVector<std::pair<TTxId, TModifySysViewRequestInfo>>&& sysViewUpdates);

} // namespace NKikimr::NSchemeShard
