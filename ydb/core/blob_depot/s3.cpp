#include "s3.h"

#include <ydb/core/wrappers/s3_wrapper.h>

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    TS3Manager::TS3Manager(TBlobDepot *self)
        : Self(self)
    {}

    TS3Manager::~TS3Manager() = default;

    void TS3Manager::Init(const NKikimrBlobDepot::TS3BackendSettings *settings) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDTS05, "Init", (Settings, settings));
        if (settings) {
            auto externalStorageConfig = NWrappers::IExternalStorageConfig::Construct(settings->GetSettings());
            WrapperId = Self->Register(NWrappers::CreateS3Wrapper(externalStorageConfig->ConstructStorageOperator()));
            BasePath = TStringBuilder() << settings->GetSettings().GetObjectKeyPattern() << '/' << Self->Config.GetName();
            Bucket = settings->GetSettings().GetBucket();
            SyncMode = settings->HasSyncMode();
            AsyncMode = settings->HasAsyncMode();
            RunScannerActor();
        } else {
            SyncMode = false;
            AsyncMode = false;
        }
    }

    void TS3Manager::TerminateAllActors() {
        for (TActorId actorId : ActiveUploaders) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
        for (TActorId actorId : ActiveDeleters) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, Self->SelfId(), nullptr, 0));
        }
        if (ScannerActorId) {
            Self->Send(new IEventHandle(TEvents::TSystem::Poison, 0, ScannerActorId, Self->SelfId(), nullptr, 0));
        }
    }

    void TS3Manager::Handle(TAutoPtr<IEventHandle> ev) {
        STRICT_STFUNC_BODY(
            fFunc(TEvPrivate::EvDeleteResult, HandleDeleter)
            fFunc(TEvPrivate::EvScanFound, HandleScanner)
        )
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

} // NKikimr::NBlobDepot
