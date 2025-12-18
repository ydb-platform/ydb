#ifndef KIKIMR_DISABLE_EXPORT_OPS

#include "datashard.h"
#include "export.h"
#include "export_common.h"
#include "export_scan.h"
#include "extstorage_usage_config.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/wrappers/events/common.h>
#include <ydb/core/wrappers/storage_wrapper.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <google/protobuf/text_format.h>

#include <ydb/core/protos/config.pb.h>

namespace NKikimr {
namespace NDataShard {

using namespace NBackup;
using namespace NBackupRestoreTraits;

struct TChangefeedExportDescriptions {
    const Ydb::Table::ChangefeedDescription ChangefeedDescription;
    const Ydb::Topic::DescribeTopicResult Topic;
    TString Name;
    TString Prefix;
};

using namespace NWrappers::NExternalStorage;

class TStorageUploader: public TActorBootstrapped<TStorageUploader> {
    using TEvBuffer = TEvExportScan::TEvBuffer<TBuffer>;

    void Restart() {
        Last = false;

        if (Attempt) {
            this->Send(std::exchange(Client, TActorId()), new TEvents::TEvPoisonPill());
        }

        Client = this->RegisterWithSameMailbox(NWrappers::CreateStorageWrapper(ExternalStorageConfig->ConstructStorageOperator()));

        if (!MetadataUploaded) {
            UploadMetadata();
        } else if (EnablePermissions && !PermissionsUploaded) {
            UploadPermissions();
        } else if (!SchemeUploaded) {
            UploadScheme();
        } else if (!ChangefeedsUploaded) {
            UploadChangefeed();
        } else {
            this->Become(&TThis::StateUploadData);

            if (Attempt) {
                this->Send(std::exchange(Scanner, TActorId()), new TEvExportScan::TEvReset());
            } else if (Scanner) {
                this->Send(Scanner, new TEvExportScan::TEvFeed());
            }
        }
    }

    template <typename T>
    void PutData(TString&& data, const TString& key, T stateFunc) {
        this->Send(Client, new TEvPutDataRequest(key, std::move(data)));
        this->Become(stateFunc);
    }

    template <typename T>
    void PutDataWithChecksum(TString&& data, const TString& key, TString& checksum, T stateFunc, TMaybe<TEncryptionIV> iv) {
        if (EnableChecksums) {
            checksum = ComputeChecksum(data);
        }
        if (iv) {
            try {
                TBuffer encryptedData = TEncryptedFileSerializer::EncryptFullFile(Settings.EncryptionSettings.EncryptionAlgorithm, *Settings.EncryptionSettings.Key, *iv, data);
                data = TString(encryptedData.Data(), encryptedData.Size());
            } catch (const std::exception& ex) {
                Finish(false, TStringBuilder() << "Failed to encrypt " << key << ": " << ex.what());
                return;
            }
        }
        PutData(std::move(data), key, stateFunc);
    }

    template <typename T>
    void PutMessage(const google::protobuf::Message& message, const TString& key, TString& checksum, T stateFunc, TMaybe<TEncryptionIV> iv) {
        google::protobuf::TextFormat::PrintToString(message, &Buffer);
        PutDataWithChecksum(std::move(Buffer), key, checksum, stateFunc, iv);
    }

    void PutScheme(const Ydb::Table::CreateTableRequest& scheme) {
        PutMessage(scheme, Settings.GetSchemeKey(), SchemeChecksum, &TThis::StateUploadScheme, Settings.EncryptionSettings.GetSchemeIV());
    }

    void UploadScheme() {
        Y_ENSURE(!SchemeUploaded);

        if (!Scheme) {
            return Finish(false, "Cannot infer scheme");
        }
        PutScheme(Scheme.GetRef());
    }

    void PutPermissions(const Ydb::Scheme::ModifyPermissionsRequest& permissions) {
        PutMessage(permissions, Settings.GetPermissionsKey(), PermissionsChecksum, &TThis::StateUploadPermissions, Settings.EncryptionSettings.GetPermissionsIV());
    }

    void UploadPermissions() {
        Y_ENSURE(EnablePermissions && !PermissionsUploaded);

        if (!Permissions) {
            return Finish(false, "Cannot infer permissions");
        }
        PutPermissions(Permissions.GetRef());
    }

    void PutChangefeedDescription(ui64 changefeedIndex) {
        const auto& desc = Changefeeds[changefeedIndex];
        PutMessage(
            desc.ChangefeedDescription,
            Settings.GetChangefeedKey(desc.Prefix),
            ChangefeedChecksum,
            &TThis::StateUploadChangefeed,
            Settings.EncryptionSettings.GetChangefeedIV(static_cast<ui32>(changefeedIndex))
        );
    }

    void UploadChangefeed() {
        Y_ENSURE(!ChangefeedsUploaded);
        if (IndexExportedChangefeed == Changefeeds.size()) {
            ChangefeedsUploaded = true;
            if (Scanner) {
                this->Send(Scanner, new TEvExportScan::TEvFeed());
            }
            this->Become(&TThis::StateUploadData);
            return;
        }
        PutChangefeedDescription(IndexExportedChangefeed);
    }

    void PutTopicDescription(ui64 changefeedIndex) {
        const auto& desc = Changefeeds[changefeedIndex];
        PutMessage(
            desc.Topic,
            Settings.GetTopicKey(desc.Prefix),
            TopicChecksum,
            &TThis::StateUploadTopic,
            Settings.EncryptionSettings.GetChangefeedTopicIV(static_cast<ui32>(changefeedIndex))
        );
    }

    void UploadTopic() {
        PutTopicDescription(IndexExportedChangefeed);
    }

    void UploadMetadata() {
        Y_ENSURE(!MetadataUploaded);

        Buffer = std::move(Metadata);
        PutDataWithChecksum(std::move(Buffer), Settings.GetMetadataKey(), MetadataChecksum, &TThis::StateUploadMetadata, Settings.EncryptionSettings.GetMetadataIV());
    }

    void UploadChecksum(TString&& checksum, const TString& checksumKey, const TString& objectKeySuffix,
        std::function<void()> checksumUploadedCallback)
    {
        // make checksum verifiable using sha256sum CLI
        checksum += ' ' + objectKeySuffix;
        PutData(std::move(checksum), checksumKey, &TThis::StateUploadChecksum);
        ChecksumUploadedCallback = checksumUploadedCallback;
    }

    void HandleScheme(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandleScheme TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        auto nextStep = [this]() {
            SchemeUploaded = true;
            UploadChangefeed();
        };

        if (EnableChecksums) {
            TString checksumKey = ChecksumKey(Settings.GetSchemeKey());
            UploadChecksum(std::move(SchemeChecksum), checksumKey, SchemeKeySuffix(false), nextStep);
        } else {
            nextStep();
        }
    }

    void HandlePermissions(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandlePermissions TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        auto nextStep = [this]() {
            PermissionsUploaded = true;
            UploadScheme();
        };

        if (EnableChecksums) {
            TString checksumKey = ChecksumKey(Settings.GetPermissionsKey());
            UploadChecksum(std::move(PermissionsChecksum), checksumKey, PermissionsKeySuffix(false), nextStep);
        } else {
            nextStep();
        }
    }

    void HandleChangefeed(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandleChangefeed TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        auto nextStep = [this]() {
            UploadTopic();
        };
        if (EnableChecksums) {
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            TString checksumKey = ChecksumKey(Settings.GetChangefeedKey(desc.Prefix));
            UploadChecksum(std::move(ChangefeedChecksum), checksumKey, ChangefeedKeySuffix(false), nextStep);
        } else {
            nextStep();
        }
    }

    void HandleTopic(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandleTopic TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        auto nextStep = [this]() {
            ++IndexExportedChangefeed;
            UploadChangefeed();
        };
        if (EnableChecksums) {
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            TString checksumKey = ChecksumKey(Settings.GetTopicKey(desc.Prefix));
            UploadChecksum(std::move(TopicChecksum), checksumKey, TopicKeySuffix(false), nextStep);
        } else {
            nextStep();
        }
    }

    void HandleMetadata(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandleMetadata TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        auto nextStep = [this]() {
            MetadataUploaded = true;
            if (EnablePermissions) {
                UploadPermissions();
            } else {
                UploadScheme();
            }
        };

        if (EnableChecksums) {
            TString checksumKey = ChecksumKey(Settings.GetMetadataKey());
            UploadChecksum(std::move(MetadataChecksum), checksumKey, MetadataKeySuffix(false), nextStep);
        } else {
            nextStep();
        }
    }

    void HandleChecksum(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandleChecksum TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        ChecksumUploadedCallback();
    }

    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvReady"
            << ": self# " << this->SelfId()
            << ", sender# " << ev->Sender);

        Scanner = ev->Sender;

        if (Error) {
            return PassAway();
        }

        const bool permissionsDone = !EnablePermissions || PermissionsUploaded;
        if (SchemeUploaded && MetadataUploaded && permissionsDone && ChangefeedsUploaded) {
            this->Send(Scanner, new TEvExportScan::TEvFeed());
        }
    }

    void Handle(TEvBuffer::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvBuffer"
            << ": self# " << this->SelfId()
            << ", sender# " << ev->Sender
            << ", msg# " << ev->Get()->ToString());

        if (ev->Sender != Scanner) {
            EXPORT_LOG_W("Received buffer from unknown scanner"
                << ": self# " << this->SelfId()
                << ", sender# " << ev->Sender
                << ", scanner# " << Scanner);
            return;
        }

        Last = ev->Get()->Last;
        ev->Get()->Buffer.AsString(Buffer);
        DataChecksum = std::move(ev->Get()->Checksum);

        UploadData();
    }

    void UploadData() {
        this->Send(Client, new NWrappers::NExternalStorage::TEvPutDataRequest(Settings.GetDataKey(DataFormat, CompressionCodec), std::move(Buffer)));
    }

    void HandleData(TEvPutDataResponse::TPtr& ev) {
        const auto& response = ev->Get();

        EXPORT_LOG_D("HandleData TEvPutDataResponse"
            << ": self# " << this->SelfId()
            << ", response# " << response->ToString());

        if (!response->IsSuccess()) {
            return Finish(false, response->GetError());
        }

        if (Last) {
            auto nextStep = [this]() {
                Finish();
            };

            if (EnableChecksums) {
                // checksum is always calculated before compression
                TString checksumKey = ChecksumKey(Settings.GetDataKey(DataFormat, ECompressionCodec::None));
                TString dataKeySuffix = DataKeySuffix(ShardNum, DataFormat, ECompressionCodec::None, false);
                UploadChecksum(std::move(DataChecksum), checksumKey, dataKeySuffix, nextStep);
            } else {
                nextStep();
            }
        }
        this->Send(Scanner, new TEvExportScan::TEvFeed());
    }

    void Finish(bool success = true, const TString& error = TString()) {
        EXPORT_LOG_I("Finish"
            << ": self# " << this->SelfId()
            << ", success# " << success
            << ", error# " << error);

        if (!success) {
            Error = error;
        }
        PassAway();
    }

    void PassAway() override {
        if (Scanner) {
            this->Send(Scanner, new TEvExportScan::TEvFinish(Error.Empty(), Error.GetOrElse(TString())));
        }

        this->Send(Client, new TEvents::TEvPoisonPill());

        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_STORAGE_UPLOADER_ACTOR;
    }

    static constexpr TStringBuf LogPrefix() {
        return "uploader"sv;
    }

    template <typename TSettings>
    explicit TStorageUploader(
            const TActorId& dataShard, ui64 txId,
            const NKikimrSchemeOp::TBackupTask& task,
            const TSettings& settings,
            TMaybe<Ydb::Table::CreateTableRequest>&& scheme,
            TVector<TChangefeedExportDescriptions> changefeeds,
            TMaybe<Ydb::Scheme::ModifyPermissionsRequest>&& permissions,
            TString&& metadata)
        : ExternalStorageConfig(IExternalStorageConfig::Construct(settings))
        , Settings(TStorageSettings::FromBackupTask<TSettings>(task))
        , DataFormat(EDataFormat::Csv)
        , CompressionCodec(CodecFromTask(task))
        , ShardNum(task.GetShardNum())
        , DataShard(dataShard)
        , TxId(txId)
        , Scheme(std::move(scheme))
        , Changefeeds(std::move(changefeeds))
        , Metadata(std::move(metadata))
        , Permissions(std::move(permissions))
        , Retries(task.GetNumberOfRetries())
        , Attempt(0)
        , Delay(TDuration::Minutes(1))
        , SchemeUploaded(ShardNum == 0 ? false : true)
        , ChangefeedsUploaded(ShardNum == 0 ? false : true)
        , MetadataUploaded(ShardNum == 0 ? false : true)
        , PermissionsUploaded(ShardNum == 0 ? false : true)
        , EnableChecksums(task.GetEnableChecksums())
        , EnablePermissions(task.GetEnablePermissions())
    {
    }

    void Bootstrap() {
        EXPORT_LOG_D("Bootstrap"
            << ": self# " << this->SelfId()
            << ", attempt# " << Attempt);

        Restart();
    }

    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReady, Handle);

            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    STATEFN(StateUploadScheme) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NWrappers::NExternalStorage::TEvPutDataResponse, HandleScheme);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadPermissions) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutDataResponse, HandlePermissions);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadChangefeed) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutDataResponse, HandleChangefeed);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadTopic) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutDataResponse, HandleTopic);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadMetadata) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutDataResponse, HandleMetadata);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadChecksum) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvPutDataResponse, HandleChecksum);
        default:
            return StateBase(ev);
        }
    }

    STATEFN(StateUploadData) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBuffer, Handle);
            hFunc(TEvPutDataResponse, HandleData);
        default:
            return StateBase(ev);
        }
    }

private:
    NWrappers::IExternalStorageConfig::TPtr ExternalStorageConfig;
    TS3Settings Settings;
    const EDataFormat DataFormat;
    const ECompressionCodec CompressionCodec;
    const ui32 ShardNum;

    const TActorId DataShard;
    const ui64 TxId;
    const TMaybe<Ydb::Table::CreateTableRequest> Scheme;
    const TVector<TChangefeedExportDescriptions> Changefeeds;
    const TString Metadata;
    const TMaybe<Ydb::Scheme::ModifyPermissionsRequest> Permissions;

    const ui32 Retries;
    ui32 Attempt;
    ui64 IndexExportedChangefeed = 0;

    TDuration Delay;
    static constexpr TDuration MaxDelay = TDuration::Minutes(10);

    TActorId Client;
    bool SchemeUploaded;
    bool ChangefeedsUploaded;
    bool MetadataUploaded;
    bool PermissionsUploaded;
    bool Last;

    TActorId Scanner;
    TString Buffer;

    TMaybe<TString> UploadId;
    TMaybe<TString> Error;

    bool EnableChecksums;
    bool EnablePermissions;

    TString DataChecksum;
    TString MetadataChecksum;
    TString ChangefeedChecksum;
    TString TopicChecksum;
    TString SchemeChecksum;
    TString PermissionsChecksum;
    std::function<void()> ChecksumUploadedCallback;

}; // TStorageUploader

IActor* TExport::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();

    const bool encrypted = Task.HasEncryptionSettings();

    TMetadata metadata;
    metadata.SetVersion(Task.GetEnableChecksums() ? 1 : 0);
    metadata.SetEnablePermissions(Task.GetEnablePermissions());

    TVector<TChangefeedExportDescriptions> changefeeds;
    if (AppData()->FeatureFlags.GetEnableChangefeedsExport()) {
        const auto& persQueues = Task.GetChangefeedUnderlyingTopics();
        const auto& cdcStreams = Task.GetTable().GetTable().GetCdcStreams();
        Y_ASSERT(persQueues.size() == cdcStreams.size());

        const int changefeedsCount = cdcStreams.size();
        changefeeds.reserve(changefeedsCount);

        for (int i = 0; i < changefeedsCount; ++i) {
            Ydb::Table::ChangefeedDescription changefeed;
            const auto& cdcStream = cdcStreams.at(i);
            FillChangefeedDescription(changefeed, cdcStream);

            Ydb::Topic::DescribeTopicResult topic;
            const auto& pq = persQueues.at(i);
            Ydb::StatusIds::StatusCode status;
            TString error;
            FillTopicDescription(topic, pq.GetPersQueueGroup(), pq.GetSelf(), cdcStream.GetName(), status, error);
            // Unnecessary fields
            topic.clear_self();
            topic.clear_topic_stats();

            auto& descr = changefeeds.emplace_back(changefeed, topic);
            descr.Name = descr.ChangefeedDescription.name();
            if (encrypted) {
                // Anonymize changefeed name in export
                std::stringstream prefix;
                prefix << std::setfill('0') << std::setw(3) << std::right << (i + 1);
                descr.Prefix = prefix.str();
            } else {
                descr.Prefix = descr.Name;
            }

            metadata.AddChangefeed(TChangefeedMetadata{
                .ExportPrefix = descr.Prefix,
                .Name = descr.Name,
            });
        }
    }

    if (scheme) {
        int idx = changefeeds.size() + 1;
        for (const auto& index : scheme->indexes()) {
            const auto indexType = NTableIndex::ConvertIndexType(index.type_case());
            const TVector<TString> indexColumns(index.index_columns().begin(), index.index_columns().end());
            std::optional<Ydb::Table::FulltextIndexSettings::Layout> layout;
            if (indexType == NKikimrSchemeOp::EIndexTypeGlobalFulltext) {
                const auto& settings = index.global_fulltext_index().fulltext_settings();
                layout = settings.has_layout() ? settings.layout() : Ydb::Table::FulltextIndexSettings::LAYOUT_UNSPECIFIED;
            }

            for (const auto& implTable : NTableIndex::GetImplTables(indexType, indexColumns, layout)) {
                const TString implTablePrefix = TStringBuilder() << index.name() << "/" << implTable;
                TString exportPrefix;
                if (encrypted) {
                    std::stringstream prefix;
                    prefix << std::setfill('0') << std::setw(3) << std::right << idx++;
                    exportPrefix = prefix.str();
                } else {
                    exportPrefix = implTablePrefix;
                }

                metadata.AddIndex(TIndexMetadata{
                    .ExportPrefix = exportPrefix,
                    .ImplTablePrefix = implTablePrefix,
                });
            }
        }
    }

    auto permissions = (Task.GetEnablePermissions() && Task.GetShardNum() == 0)
        ? GenYdbPermissions(Task.GetTable())
        : Nothing();

    TFullBackupMetadata::TPtr backup = new TFullBackupMetadata{
        .SnapshotVts = TVirtualTimestamp(
            Task.GetSnapshotStep(),
            Task.GetSnapshotTxId())
    };
    metadata.AddFullBackup(backup);

    Y_ENSURE(Task.HasS3Settings(), "Unsupported storage settings");
    return new TStorageUploader(
        dataShard, txId, Task, Task.GetS3Settings(), std::move(scheme), std::move(changefeeds), std::move(permissions), metadata.Serialize());
}

} // NDataShard
} // NKikimr

#endif // KIKIMR_DISABLE_EXPORT_OPS
