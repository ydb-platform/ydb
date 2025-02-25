#pragma once

#include "defs.h"
#include "blob_depot_tablet.h"
#include "data.h"

#include <ydb/core/wrappers/abstract.h>

namespace NKikimr::NBlobDepot {

    class TBlobDepot::TS3Manager {
        class TUploaderActor;

        struct TEvUpload;
        struct TEvUploadResult;

        TBlobDepot* const Self;
        NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
        TActorId WrapperId;
        TActorId UploaderId;
        TString BasePath;

        THashSet<TActorId> ActiveUploaders;

        ui64 NextKeyId = 1;

    public:
        TS3Manager(TBlobDepot *self);
        ~TS3Manager();

        void Init(const NKikimrBlobDepot::TS3BackendSettings *settings);
        void TerminateAllUploaders();

        void OnKeyWritten(const TData::TKey& key, const TValueChain& valueChain);

        TString MakeKeyName(const TData::TKey& key, const NKikimrBlobDepot::TS3Locator& locator);

        const TActorId& GetWrapperId() const { return WrapperId; }
    };

} // NKikimr::NBlobDepot
