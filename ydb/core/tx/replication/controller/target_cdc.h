#pragma once

#include "target_topic.h"
#include <ydb/core/tx/replication/controller/protos/cdc_target.pb.h>

namespace NKikimr::NIceDb {
class TNiceDb;
}

namespace NKikimr::NReplication::NController {

class TTargetCdc: public TTargetTopic {
public:
    struct TConfig: public TConfigBase {
        TConfig(const TString& srcPath, const TString& dstPath)
            : TConfigBase(ETargetKind::CdcTopic, srcPath, dstPath)
        {}
    };

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

void FinishCdcChildren(const TReplication& replication, ui64 parentId, NIceDb::TNiceDb& db);
TTargetCdc* GetCdcTarget(TReplication::ITarget* target);
const TTargetCdc* GetCdcTarget(const TReplication::ITarget* target);
TVector<TTargetCdc*> GetCdcChildren(const TReplication& replication, ui64 parentId);

}
