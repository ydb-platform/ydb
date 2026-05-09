#include "s3.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/protos/s3_settings.pb.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    using TS3Manager = TBlobDepot::TS3Manager;

    TS3Manager::TS3Manager(TBlobDepot *self)
        : Self(self)
    {}

    TS3Manager::~TS3Manager() = default;

    void TS3Manager::Init(const NKikimrBlobDepot::TS3BackendSettings *settings) {
        YDBLOG_DEBUG("Init",
            {"Marker", "BDTS05"},
            {"Settings", settings});
        if (settings) {
            auto externalStorageConfig = NWrappers::IExternalStorageConfig::Construct(AppData()->AwsClientConfig, settings->GetSettings());
            WrapperId = Self->Register(NWrappers::CreateStorageWrapper(externalStorageConfig->ConstructStorageOperator()));
            BasePath = TStringBuilder() << settings->GetSettings().GetObjectKeyPattern() << '/' << Self->Config.GetName();
            Bucket = settings->GetSettings().GetBucket();
            SyncMode = settings->HasSyncMode();
            AsyncMode = settings->HasAsyncMode();
            Enabled = true;
        } else {
            SyncMode = false;
            AsyncMode = false;
            Enabled = false;
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

    void TS3Manager::OnDataLoaded() {
        if (Enabled) {
            RunScannerActor();
        }
    }

    void TBlobDepot::InitS3Manager() {
        S3Manager->Init(Config.HasS3BackendSettings() ? &Config.GetS3BackendSettings() : nullptr);
    }

} // NKikimr::NBlobDepot
