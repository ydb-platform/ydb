#pragma once

#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

namespace NKikimr::NReplication::NController {

struct TDstCdcSettings {
    TPathId TablePathId;
    Ydb::Table::ChangefeedDescription SourceStream;
    NYdb::NTopic::TTopicDescription SourceTopic;
    TString Consumer;
    TPathId StreamPathId;
    TPathId TopicPathId;
};

// Enrollment persists the normalized CREATE definition and an unused allocated CREATE id.
// The caller keeps original identities immutable and holds an exclusive reset phase until
// replacement identities are persisted and the reserved id is retired.
struct TDstCdcResetSettings {
    TPathId TablePathId;
    TPathId StreamPathId;
    TPathId TopicPathId;
    ui64 CreateTxId = 0;
    NKikimrSchemeOp::TCreateCdcStream Definition;
    bool ActivateOnDetach = false;
};

NActors::IActor* CreateDstCdcResetter(const NActors::TActorId& parent, ui64 schemeShardId,
    const TString& database, const TPathId& replicationPathId,
    ui64 rid, ui64 tid, TDstCdcResetSettings&& settings);

NActors::IActor* CreateDstCdcCreator(const NActors::TActorId& parent, ui64 schemeShardId,
    const TString& database, const TPathId& replicationPathId,
    ui64 rid, ui64 tid, TDstCdcSettings&& settings);

}
