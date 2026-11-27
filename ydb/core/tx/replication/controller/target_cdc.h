#pragma once

#include "target_topic.h"

#include <ydb/core/tx/replication/controller/protos/cdc_target.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/fwd.h>

namespace NKikimr::NIceDb {
class TNiceDb;
}

namespace NKikimr::NReplication::NController {

class TTargetCdc: public TTargetTopic {
public:
    struct TConfig: public TConfigBase {
        TConfig(const TString& srcPath, const TString& dstPath, const NKikimrReplicationController::TCdcTarget& cdc)
            : TConfigBase(ETargetKind::CdcTopic, srcPath, dstPath)
            , Cdc(cdc)
        {}

        NKikimrReplicationController::TCdcTarget Cdc;
    };

    static TTargetCdc* Cast(TReplication::ITarget* target);
    static const TTargetCdc* Cast(const TReplication::ITarget* target);
    static bool IsUserStream(const NYdb::NTable::TChangefeedDescription& stream);
    static bool IsUserStream(const Ydb::Table::ChangefeedDescription& stream);

    TTargetCdc(TReplication* replication, ui64 id, const IConfig::TPtr& config);

    NKikimrReplicationController::TCdcTarget& GetCdc();
    const NKikimrReplicationController::TCdcTarget& GetCdc() const;
    bool ActivateOnDetach() const;
    bool OnDstAltered(NIceDb::TNiceDb& db, const TActorContext& ctx);
    bool IsExpectedDstCreator(const TActorId& actor) const override;
    void Progress(const TActorContext& ctx) override;
    void Shutdown(const TActorContext& ctx) override;

private:
    NKikimrReplicationController::TCdcTarget Cdc;
    TActorId HandoffActor;
};

}
