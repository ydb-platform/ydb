#include "schemeshard_export_metadata_uploader.h"
#include "schemeshard_private.h"

#include <library/cpp/json/json_writer.h>
#include <ydb/core/wrappers/s3_storage_config.h>
#include <ydb/core/wrappers/s3_wrapper.h>
#include <ydb/public/api/protos/ydb_export.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <google/protobuf/text_format.h>

#include <util/string/subst.h>

namespace NKikimr::NSchemeShard {

using namespace NWrappers::NExternalStorage;
using namespace NWrappers;

using namespace Aws::Auth;
using namespace Aws::Client;
using namespace Aws::S3;
using namespace Aws;

namespace NBackupRestore {

void TMetadata::AddFullBackup(TFullBackupMetadata::TPtr fb) {
    FullBackups.emplace(fb->SnapshotVts, fb);
}

TString TMetadata::Serialize() const {
    NJson::TJsonMap m;
    m["version"] = 0;
    NJson::TJsonArray fullBackups;
    for (auto &[tp, _] : FullBackups) {
        NJson::TJsonMap backupMap;
        NJson::TJsonArray vts;
        vts.AppendValue(tp.Step);
        vts.AppendValue(tp.TxId);
        backupMap["snapshot_vts"] = std::move(vts);
        fullBackups.AppendValue(std::move(backupMap));
    }
    m["full_backups"] = fullBackups;
    return NJson::WriteJson(&m, false);
}

} // namespace NBackupRestore

class TMetadataUploader: public TActorBootstrapped<TMetadataUploader> {
    static TString MetadataKeyFromSettings(const Ydb::Export::ExportToS3Settings& settings, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < (ui32)settings.items_size());
        return TStringBuilder() << settings.items(itemIdx).destination_prefix() << "/metadata.json";
    }

    void Bootstrap() {
        Client = this->RegisterWithSameMailbox(NWrappers::CreateS3Wrapper(ExternalStorageConfig->ConstructStorageOperator()));
        UploadMetadata();
    }

    void Handle(TEvExternalStorage::TEvPutObjectResponse::TPtr& ev) {
        if (ev->Get()->IsSuccess()) {
            Send(ReplyTo, new TEvPrivate::TEvExportMetadataUploaded(ExportInfo->Id, ItemIdx, true, ""));
        } else {
            const auto error = TStringBuilder() << "S3 error: " << ev->Get()->GetError().GetMessage().c_str();
            Send(ReplyTo, new TEvPrivate::TEvExportMetadataUploaded(ExportInfo->Id, ItemIdx, false, error));
        }
        PassAway();
    }

    void PassAway() override {
        Send(Client, new TEvents::TEvPoisonPill());
        TActor::PassAway();
    }

public:
    explicit TMetadataUploader(const TActorId& replyTo,
                               TExportInfo::TPtr exportInfo,
                               Ydb::Export::ExportToS3Settings settings,
                               ui32 itemIdx,
                               TString metadata)
        : ReplyTo(replyTo)
        , ExportInfo(exportInfo)
        , Settings(std::move(settings))
        , ItemIdx(itemIdx)
        , Metadata(std::move(metadata))
        , MetadataKey(MetadataKeyFromSettings(Settings, itemIdx))
        , ExternalStorageConfig(new TS3ExternalStorageConfig(Settings))
    {}

    void UploadMetadata() {
        auto request = Aws::S3::Model::PutObjectRequest()
            .WithKey(MetadataKey)
            .WithStorageClass(TS3ExternalStorageConfig::ConvertStorageClass(Settings.storage_class()));
        this->Send(Client, new TEvExternalStorage::TEvPutObjectRequest(request, std::move(Metadata)));
        this->Become(&TThis::StateWork);
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExternalStorage::TEvPutObjectResponse, Handle);
            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }
    
private:
    const TActorId ReplyTo;
    TExportInfo::TPtr ExportInfo;
    const Ydb::Export::ExportToS3Settings Settings;
    const ui32 ItemIdx;

    TString Metadata;
    const TString MetadataKey;

    TActorId Client;
    IExternalStorageConfig::TPtr ExternalStorageConfig;
}; // TMetadataUploader

IActor* CreateMetadataUploader(const TActorId& replyTo, TExportInfo::TPtr exportInfo, ui32 itemIdx) {
    Ydb::Export::ExportToS3Settings settings;
    Y_ABORT_UNLESS(settings.ParseFromString(exportInfo->Settings));
    
    NBackupRestore::TMetadata metadata;

    NBackupRestore::TFullBackupMetadata::TPtr backup = new NBackupRestore::TFullBackupMetadata{
        .SnapshotVts = NBackupRestore::TVirtualTimestamp(
            exportInfo->SnapshotStep,
            exportInfo->SnapshotTxId)
    };
    metadata.AddFullBackup(backup);

    return new TMetadataUploader(replyTo, exportInfo, std::move(settings), itemIdx, metadata.Serialize());
}

} // namespace NKikimr::NSchemeShard
