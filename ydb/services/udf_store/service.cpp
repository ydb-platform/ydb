#include "service.h"
#include "store_initializer.h"
#include "metadata_subscription/fetcher.h"
#include "kv_body_store.h"
#include "metadata_subscription/udf_meta.h"

#include <ydb/services/metadata/service.h>
#include <ydb/core/base/appdata.h>

#include <util/folder/path.h>
#include <util/system/fs.h>

#include <algorithm>

namespace NKikimr::NUdfStore {

bool TUdfStoreService::IsMd5Pending(const TString& md5) const {
    return std::any_of(PendingUdfs.begin(), PendingUdfs.end(),
        [&](const TPendingUdf& pending) { return pending.Md5 == md5; });
}

void TUdfStoreService::EnqueueUdfIfNeeded(const TString& md5, ui64 expectedSize) {
    if (expectedSize == 0) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: UDF '" << md5 << "' has zero size in metadata, skipping fetch";
        return;
    }
    if (LoadedUdfs.contains(md5) || IsMd5Pending(md5)) {
        return;
    }
    PendingUdfs.push_back({md5, expectedSize});
}

void TUdfStoreService::Bootstrap() {
    Become(&TUdfStoreService::StateMain);
    Register(new TUdfStoreInitializer(SelfId(), KvStorageMedia));
}

void TUdfStoreService::Handle(TEvUdfStoreInit::TEvStoreInitialized::TPtr& ev) {
    KvVolumePath = ev->Get()->KvVolumePath;
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: infrastructure initialized, KV Volume path: " << KvVolumePath;
    Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()),
        new NMetadata::NProvider::TEvSubscribeExternal(std::make_shared<TSnapshotsFetcher>()));
}

void TUdfStoreService::Handle(TEvUdfStoreInit::TEvStoreInitFailed::TPtr& ev) {
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreService: infrastructure initialization failed: " << ev->Get()->ErrorMessage;
    PassAway();
}

void TUdfStoreService::Handle(NProvider::TEvRefreshSubscriberData::TPtr& ev) {
    auto snapshot = ev->Get()->GetSnapshotPtrAs<TSnapshot>();
    if (!snapshot) {
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received non-UDF snapshot";
        return;
    }
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received UDF snapshot";

    for (const auto& [md5, udf] : snapshot->GetUdfs()) {
        const TUdfMeta* existing = CurrentSnapshot ? CurrentSnapshot->GetUdfByMd5(md5) : nullptr;
        const bool isNew = !existing;

        if (isNew) {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: UDF added"
                << ", md5=" << md5
                << ", name=" << udf.GetName()
                << ", type=" << udf.GetType()
                << ", size=" << udf.GetSize();
        } else if (existing->GetSize() != udf.GetSize()) {
            ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: UDF size changed"
                << ", md5=" << md5
                << ", old_size=" << existing->GetSize()
                << ", new_size=" << udf.GetSize();
            LoadedUdfs.erase(md5);
            FetchRetryCounts.erase(md5);
        }
        switch(udf.GetType()) {
            case EUdfType::NATIVE_UNSAFE:
                if (!EnableUnsafeNativeUdfFlag) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableUnsafeNativeUdf is not set,"
                        << " skipping UDF '" << md5 << "' with name '" << udf.GetName() << "'";
                    break;
                }
                if (UnsafeNativeUdfDir.empty()) {
                    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                        << "TUdfStoreService: EnableUnsafeNativeUdf is set but UnsafeNativeUdfDir is empty,"
                        << " skipping UDF '" << md5 << "' with name '" << udf.GetName() << "'";
                    break;
                }
                if (!LoadedUdfs.contains(md5)) {
                    FetchRetryCounts.erase(md5);
                }
                EnqueueUdfIfNeeded(md5, udf.GetSize());
                break;
            case EUdfType::WASM:
                //TODO implement me
                break;
        }
    }

    // Log removed UDFs
    if (CurrentSnapshot) {
        for (const auto& [md5, udf] : CurrentSnapshot->GetUdfs()) {
            if (!snapshot->GetUdfByMd5(md5)) {
                ALS_INFO(NKikimrServices::METADATA_PROVIDER)
                    << "TUdfStoreService: UDF removed"
                    << ": md5=" << md5
                    << ", type=" << udf.GetType()
                    << ", name=" << udf.GetName();
                LoadedUdfs.erase(md5);
                FetchRetryCounts.erase(md5);
            }
            //TODO unregister removed UDFs
        }
    }

    CurrentSnapshot = snapshot;

    if (!FetchInProgress) {
        FetchNextBody();
    }
}

void TUdfStoreService::FetchNextBody() {
    if (PendingUdfs.empty()) {
        FetchInProgress = false;
        return;
    }

    FetchInProgress = true;
    const auto& pending = PendingUdfs.front();

    // Pass Md5 as the KV key; TKvBodyReadActor writes the binary to OutputDir/<md5>.
    Register(new TKvBodyReadActor(
        SelfId(),
        pending.Md5,
        KvVolumePath,
        UnsafeNativeUdfDir,
        FunctionRegistry,
        pending.ExpectedSize));
}

void TUdfStoreService::Handle(TEvKvBodyStore::TEvReadBodyResponse::TPtr& ev) {
    if (PendingUdfs.empty()) {
        ALS_WARN(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: received unexpected TEvReadBodyResponse for UDF '"
            << ev->Get()->Name << "' with no pending fetches";
        return;
    }

    auto pending = std::move(PendingUdfs.front());
    PendingUdfs.pop_front();

    if (ev->Get()->Success) {
        LoadedUdfs.insert(pending.Md5);
        FetchRetryCounts.erase(pending.Md5);
        ALS_INFO(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreService: native UDF '" << pending.Md5
            << "' saved to " << UnsafeNativeUdfDir;
    } else {
        ui32& retryCount = FetchRetryCounts[pending.Md5];
        if (retryCount < MaxFetchRetries) {
            ++retryCount;
            EnqueueUdfIfNeeded(pending.Md5, pending.ExpectedSize);
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: failed to save native UDF '" << pending.Md5
                << "' (retry " << retryCount << "/" << MaxFetchRetries
                << "): " << ev->Get()->ErrorMessage;
        } else {
            ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
                << "TUdfStoreService: giving up on native UDF '" << pending.Md5
                << "' after " << MaxFetchRetries << " retries: " << ev->Get()->ErrorMessage;
        }
    }

    FetchNextBody();
}

NActors::TActorId MakeServiceId(ui32 nodeId) {
    return NActors::TActorId(nodeId, "SrvcUdfStore");
}

NActors::IActor* CreateService(const NKikimrConfig::TUdfStoreConfig& serviceConfig, TIntrusivePtr<NMiniKQL::IMutableFunctionRegistry> functionRegistry) {
    if (!serviceConfig.GetEnabled()) {
        return nullptr;
    }
    return new TUdfStoreService(serviceConfig, std::move(functionRegistry));
}


} // namespace NKikimr::NUdfStore
