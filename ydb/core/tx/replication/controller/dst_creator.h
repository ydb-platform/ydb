#pragma once

#include "replication.h"

namespace NKikimrSchemeOp {
    class TTableReplicationConfig;
}

namespace NKikimr::NReplication::NController {

struct TTopicMirrorSettings;

enum class EReplicationMode {
    ReadOnly,
};

enum class EConsistencyLevel {
    Row,
    Global,
};

void FillReplicationConfig(
    NKikimrSchemeOp::TTableReplicationConfig& out,
    EReplicationMode mode,
    EConsistencyLevel consistency);
bool CheckReplicationConfig(
    const NKikimrSchemeOp::TTableReplicationConfig& in,
    EReplicationMode mode,
    EConsistencyLevel consistency,
    TString& error);

IActor* CreateDstCreator(TReplication* replication, ui64 targetId, const TActorContext& ctx);
IActor* CreateDstCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
    const TString& database, const TPathId& pathId,
    ui64 rid, ui64 tid, TReplication::ETargetKind kind, const TString& srcPath, const TString& dstPath,
    EReplicationMode mode = EReplicationMode::ReadOnly,
    EConsistencyLevel consistency = EConsistencyLevel::Row, bool initialScan = true,
    TReplication::EState desiredState = TReplication::EState::Ready);
IActor* CreateDstTopicCreator(const TActorId& parent, ui64 schemeShardId, const TActorId& proxy,
    const TString& database, const TPathId& pathId,
    ui64 rid, ui64 tid, const TString& srcPath, const TString& dstPath,
    TTopicMirrorSettings&& topicMirror);

}
