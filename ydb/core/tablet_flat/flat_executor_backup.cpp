#include "flat_executor_backup.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::LOCAL_DB_BACKUP, stream)

namespace NKikimr::NTabletFlatExecutor {

class TFileSystemWriter : public TActorBootstrapped<TFileSystemWriter> {
public:
    using TBase = TActorBootstrapped<TFileSystemWriter>;

    void Bootstrap() {
        LOG_D("Bootstrap");
        Become(&TThis::StateWork);
    }

    void PassAway() override {
        LOG_D("PassAway");
        TBase::PassAway();
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            cFunc(TEvents::TEvPoisonPill::EventType, PassAway);
        }
    }
};

IActor* CreateBackupWriter(const NKikimrConfig::TSystemTabletBackupConfig& config, TTabletTypes::EType, ui64, ui32) {
    if (config.HasFilesystem()) {
        return new TFileSystemWriter();
    } else {
        return nullptr;
    }
}

} // NKikimr::NTabletFlatExecutor

