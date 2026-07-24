#include "store_initializer.h"
#include "metadata_subscription/udf_meta.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/base/appdata.h>

namespace NKikimr::NUdfStore {

void TUdfStoreInitializer::Bootstrap() {
    Become(&TUdfStoreInitializer::StateFunc);
    const auto& path = NKikimr::SplitPath(TUdfMeta::GetBehaviour()->GetStorageTablePath());
    auto it = cbegin(path);
    while (it != path.end()  && *it != NMetadata::NProvider::TServiceOperator::GetPath()) {
        ++it;
    }
    AFL_VERIFY(it != cend(path));
    Register(CreateTableCreator(
        {it, cend(path)},
        TUdfMeta::GetColumnDescription(),
        TUdfMeta::GetPk(),
        NKikimrServices::METADATA_PROVIDER,
        Nothing(),
        {},
        /* isSystemUser */ true
    ));
}

void TUdfStoreInitializer::HandleTableCreated(TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
    if (!ev->Get()->Success) {
        const TString errorMessage = TStringBuilder()
            << "failed to create meta table: " << ev->Get()->Issues.ToString();
        ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
            << "TUdfStoreInitializer: " << errorMessage;
        Send(ParentId, new TEvStoreInitFailed(errorMessage));
        PassAway();
        return;
    }


    auto tablePath = SplitPath(TUdfMeta::GetBehaviour()->GetStorageTablePath());
    AFL_VERIFY(!tablePath.empty());
    tablePath.pop_back();
    tablePath.push_back("binaries");
    KvVolumePath = NKikimr::CombinePath(cbegin(tablePath), cend(tablePath));

    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreInitializer: meta table created, creating KV volume at " << KvVolumePath;

    NACLib::TUserToken userToken("metadata@system", {});

    Ydb::KeyValue::CreateVolumeRequest kvRequest;
    kvRequest.set_path(KvVolumePath);
    kvRequest.set_partition_count(1);

    auto* storageConfig = kvRequest.mutable_storage_config();
    for (int i = 0; i < 3; ++i) {
        storageConfig->add_channel()->set_media(KvStorageMedia);
    }

    auto controller = std::make_shared<NMetadata::NRequest::TNaiveExternalController<NMetadata::NRequest::TDialogCreateKvVolume>>(SelfId());
    NMetadata::NRequest::TYDBOneRequestSender<NMetadata::NRequest::TDialogCreateKvVolume> sender(kvRequest, userToken, controller);
    sender.Start();
}

void TUdfStoreInitializer::HandleKvVolumeCreated(
    NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogCreateKvVolume>::TPtr& /*ev*/)
{
    ALS_INFO(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreInitializer: KV volume '" << KvVolumePath << "' created successfully";
    Send(ParentId, new TEvStoreInitialized{KvVolumePath});
    PassAway();
}

void TUdfStoreInitializer::HandleRequestFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
    const TString errorMessage = TStringBuilder()
        << "failed to create KV volume: " << ev->Get()->GetErrorMessage();
    ALS_ERROR(NKikimrServices::METADATA_PROVIDER)
        << "TUdfStoreInitializer: " << errorMessage;
    Send(ParentId, new TEvStoreInitFailed(errorMessage));
    PassAway();
}

} // namespace NKikimr::NUdfStore
