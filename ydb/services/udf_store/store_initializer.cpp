#include "store_initializer.h"
#include "blob_chunks.h"
#include "metadata_subscription/udf_module.h"
#include "metadata_subscription/storage_paths.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/core/base/appdata.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::METADATA_PROVIDER

namespace NKikimr::NUdfStore {

namespace {

TVector<TString> GetTablePathSuffix(const TString& internalPath) {
    const auto& path = NKikimr::SplitPath(internalPath);
    auto it = cbegin(path);
    while (it != path.end() && *it != NMetadata::NProvider::TServiceOperator::GetPath()) {
        ++it;
    }
    AFL_VERIFY(it != cend(path));
    return {it, cend(path)};
}

} // namespace

void TUdfStoreInitializer::Bootstrap() {
    Become(&TUdfStoreInitializer::StateFunc);
    CreateNextTable();
}

void TUdfStoreInitializer::CreateNextTable() {
    switch (InitStep_) {
        case EInitStep::Modules: {
            Register(CreateTableCreator(
                GetTablePathSuffix(TUdfModule::GetBehaviour()->GetStorageTablePath()),
                TUdfModule::GetColumnDescription(),
                TUdfModule::GetPk(),
                NKikimrServices::METADATA_PROVIDER,
                Nothing(),
                {},
                /* isSystemUser */ true
            ));
            break;
        }
        case EInitStep::ModuleChunks: {
            Register(CreateTableCreator(
                GetTablePathSuffix(GetModuleChunksTablePath()),
                TSourceChunkSchema::GetColumnDescription(),
                TSourceChunkSchema::GetPk(),
                NKikimrServices::METADATA_PROVIDER,
                Nothing(),
                {},
                /* isSystemUser */ true
            ));
            break;
        }
        case EInitStep::KvVolume: {
            auto tablePath = SplitPath(TUdfModule::GetBehaviour()->GetStorageTablePath());
            AFL_VERIFY(!tablePath.empty());
            tablePath.pop_back();
            tablePath.push_back("binaries");
            KvVolumePath = NKikimr::CombinePath(cbegin(tablePath), cend(tablePath));

            YDB_LOG_INFO("TUdfStoreInitializer: creating KV volume",
                {"kvVolumePath", KvVolumePath});

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
            break;
        }
        case EInitStep::Done:
            Y_ABORT("unexpected init step");
    }
}

void TUdfStoreInitializer::AdvanceInitStep() {
    switch (InitStep_) {
        case EInitStep::Modules:
            InitStep_ = EInitStep::ModuleChunks;
            break;
        case EInitStep::ModuleChunks:
            InitStep_ = EInitStep::KvVolume;
            break;
        case EInitStep::KvVolume:
            InitStep_ = EInitStep::Done;
            break;
        case EInitStep::Done:
            break;
    }
}

void TUdfStoreInitializer::HandleTableCreated(TEvTableCreator::TEvCreateTableResponse::TPtr& ev) {
    if (!ev->Get()->Success) {
        const TString errorMessage = TStringBuilder()
            << "failed to create UDF store table at step "
            << static_cast<int>(InitStep_)
            << ": " << ev->Get()->Issues.ToString();
        YDB_LOG_ERROR("TUdfStoreInitializer",
            {"errorMessage", errorMessage});
        Send(ParentId, new TEvStoreInitFailed(errorMessage));
        PassAway();
        return;
    }

    YDB_LOG_INFO("TUdfStoreInitializer: table created at step",
        {"initStep", static_cast<int>(InitStep_)});

    AdvanceInitStep();
    if (InitStep_ == EInitStep::Done) {
        Send(ParentId, new TEvStoreInitialized{KvVolumePath});
        PassAway();
        return;
    }
    CreateNextTable();
}

void TUdfStoreInitializer::HandleKvVolumeCreated(
    NMetadata::NRequest::TEvRequestResult<NMetadata::NRequest::TDialogCreateKvVolume>::TPtr& /*ev*/)
{
    YDB_LOG_INFO("TUdfStoreInitializer: KV volume created successfully",
        {"kvVolumePath", KvVolumePath});
    AdvanceInitStep();
    Send(ParentId, new TEvStoreInitialized{KvVolumePath});
    PassAway();
}

void TUdfStoreInitializer::HandleRequestFailed(NMetadata::NRequest::TEvRequestFailed::TPtr& ev) {
    const TString errorMessage = TStringBuilder()
        << "failed to create KV volume: " << ev->Get()->GetErrorMessage();
    YDB_LOG_ERROR("TUdfStoreInitializer",
        {"errorMessage", errorMessage});
    Send(ParentId, new TEvStoreInitFailed(errorMessage));
    PassAway();
}

} // namespace NKikimr::NUdfStore
