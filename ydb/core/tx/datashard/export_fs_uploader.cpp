#include "export_common.h"
#include "export_fs.h"
#include "export_s3_buffer.h"
#include "backup_restore_traits.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/fs_settings.pb.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/backup/common/checksum.h>
#include <ydb/core/backup/common/metadata.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/folder/path.h>
#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/file.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NDataShard {

using namespace NBackup;
using namespace NBackupRestoreTraits;

class TFsSettings {
public:
    const TString BasePath;      // Base path on filesystem (e.g., /mnt/exports)
    const TString RelativePath;  // Relative path for this export item
    const ui32 Shard;

    explicit TFsSettings(const NKikimrSchemeOp::TFSSettings& settings, ui32 shard)
        : BasePath(settings.GetBasePath())
        , RelativePath(settings.GetPath())
        , Shard(shard)
    {
    }

    static TFsSettings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task) {
        return TFsSettings(task.GetFSSettings(), task.GetShardNum());
    }

    TString GetFullPath() const {
        return TFsPath(BasePath) / RelativePath;
    }

    TString GetPermissionsKey() const {
        return TFsPath(GetFullPath()) / PermissionsKeySuffix(false);
    }

    TString GetMetadataKey() const {
        return TFsPath(GetFullPath()) / MetadataKeySuffix(false);
    }

    TString GetSchemeKey() const {
        return TFsPath(GetFullPath()) / SchemeKeySuffix(false);
    }

    TString GetDataKey(EDataFormat format, ECompressionCodec codec) const {
        return TFsPath(GetFullPath()) / DataKeySuffix(Shard, format, codec, false);
    }

    TString GetChangefeedKey(const TString& changefeedPrefix) const {
        return TFsPath(GetFullPath()) / changefeedPrefix / ChangefeedKeySuffix(false);
    }

    TString GetTopicKey(const TString& changefeedPrefix) const {
        return TFsPath(GetFullPath()) / changefeedPrefix / TopicKeySuffix(false);
    }
};

struct TChangefeedExportDescriptions {
    const Ydb::Table::ChangefeedDescription ChangefeedDescription;
    const Ydb::Topic::DescribeTopicResult Topic;
    TString Name;
    TString Prefix;
};

class TFsUploader: public TActorBootstrapped<TFsUploader> {
    using TEvBuffer = TEvExportScan::TEvBuffer<TBuffer>;

    bool WriteFile(const TString& path, const TString& data, TString& error) {
        try {
            TFsPath fsPath(path);
            fsPath.Parent().MkDirs();
            
            TFile file(path, CreateAlways | WrOnly);
            file.Write(data.data(), data.size());
            file.Close();
            
            EXPORT_LOG_D("WriteFile succeeded"
                << ": self# " << SelfId()
                << ", path# " << path
                << ", size# " << data.size());
            
            return true;
        } catch (const std::exception& ex) {
            error = TStringBuilder() << "Failed to write file " << path << ": " << ex.what();
            EXPORT_LOG_E("WriteFile failed"
                << ": self# " << SelfId()
                << ", path# " << path
                << ", error# " << error);
            return false;
        }
    }

    bool WriteMessage(const google::protobuf::Message& message, const TString& path, TString& error) {
        TString data;
        google::protobuf::TextFormat::PrintToString(message, &data);
        return WriteFile(path, data, error);
    }

    bool WriteFileWithChecksum(const TString& path, const TString& data, TString& error) {
        if (!WriteFile(path, data, error)) {
            return false;
        }

        if (EnableChecksums) {
            TString checksum = ComputeChecksum(data);
            // Extract filename for checksum file format
            TFsPath fsPath(path);
            TString filename = fsPath.GetName();
            checksum += ' ' + filename;
            
            TString checksumPath = ChecksumKey(path);
            if (!WriteFile(checksumPath, checksum, error)) {
                return false;
            }
        }

        return true;
    }

    bool WriteMessageWithChecksum(const google::protobuf::Message& message, const TString& path, TString& error) {
        TString data;
        google::protobuf::TextFormat::PrintToString(message, &data);
        return WriteFileWithChecksum(path, data, error);
    }

    void UploadMetadata() {
        EXPORT_LOG_I("UploadMetadata started"
            << ": self# " << SelfId()
            << ", path# " << Settings.GetMetadataKey()
            << ", metadataSize# " << Metadata.size());

        TString error;
        if (!WriteFileWithChecksum(Settings.GetMetadataKey(), Metadata, error)) {
            EXPORT_LOG_E("UploadMetadata failed"
                << ": self# " << SelfId()
                << ", error# " << error);
            return Finish(false, error);
        }

        MetadataUploaded = true;
        EXPORT_LOG_I("UploadMetadata completed"
            << ": self# " << SelfId()
            << ", enablePermissions# " << EnablePermissions);
        
        if (EnablePermissions) {
            UploadPermissions();
        } else {
            UploadScheme();
        }
    }

    void UploadPermissions() {
        EXPORT_LOG_I("UploadPermissions started"
            << ": self# " << SelfId()
            << ", path# " << Settings.GetPermissionsKey()
            << ", hasPermissions# " << Permissions.Defined());

        if (!Permissions) {
            EXPORT_LOG_E("UploadPermissions failed - no permissions"
                << ": self# " << SelfId());
            return Finish(false, "Cannot infer permissions");
        }

        TString error;
        if (!WriteMessageWithChecksum(Permissions.GetRef(), Settings.GetPermissionsKey(), error)) {
            EXPORT_LOG_E("UploadPermissions failed"
                << ": self# " << SelfId()
                << ", error# " << error);
            return Finish(false, error);
        }

        PermissionsUploaded = true;
        EXPORT_LOG_I("UploadPermissions completed"
            << ": self# " << SelfId());
        UploadScheme();
    }

    void UploadScheme() {
        EXPORT_LOG_I("UploadScheme started"
            << ": self# " << SelfId()
            << ", path# " << Settings.GetSchemeKey()
            << ", hasScheme# " << Scheme.Defined());

        if (!Scheme) {
            EXPORT_LOG_E("UploadScheme failed - no scheme"
                << ": self# " << SelfId());
            return Finish(false, "Cannot infer scheme");
        }

        TString error;
        if (!WriteMessageWithChecksum(Scheme.GetRef(), Settings.GetSchemeKey(), error)) {
            EXPORT_LOG_E("UploadScheme failed"
                << ": self# " << SelfId()
                << ", error# " << error);
            return Finish(false, error);
        }

        SchemeUploaded = true;
        EXPORT_LOG_I("UploadScheme completed"
            << ": self# " << SelfId());
        UploadChangefeeds();
    }

    void UploadChangefeeds() {
        EXPORT_LOG_I("UploadChangefeeds started"
            << ": self# " << SelfId()
            << ", index# " << IndexExportedChangefeed
            << ", total# " << Changefeeds.size());

        while (IndexExportedChangefeed < Changefeeds.size()) {
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            
            EXPORT_LOG_I("UploadChangefeeds processing changefeed"
                << ": self# " << SelfId()
                << ", index# " << IndexExportedChangefeed
                << ", name# " << desc.Name
                << ", prefix# " << desc.Prefix);
            
            TString error;
            
            if (!WriteMessageWithChecksum(desc.ChangefeedDescription, Settings.GetChangefeedKey(desc.Prefix), error)) {
                EXPORT_LOG_E("UploadChangefeeds failed to write changefeed"
                    << ": self# " << SelfId()
                    << ", error# " << error);
                return Finish(false, error);
            }
            
            if (!WriteMessageWithChecksum(desc.Topic, Settings.GetTopicKey(desc.Prefix), error)) {
                EXPORT_LOG_E("UploadChangefeeds failed to write topic"
                    << ": self# " << SelfId()
                    << ", error# " << error);
                return Finish(false, error);
            }
            
            ++IndexExportedChangefeed;
        }

        ChangefeedsUploaded = true;
        EXPORT_LOG_I("UploadChangefeeds completed"
            << ": self# " << SelfId()
            << ", scanner# " << Scanner);
        
        if (Scanner) {
            EXPORT_LOG_I("Scanner already ready, finishing export"
                << ": self# " << SelfId());
            // Tell scanner we're done (skip data export for now)
            Finish(true);
        } else {
            EXPORT_LOG_I("Waiting for scanner to become ready"
                << ": self# " << SelfId());
            // Wait for scanner to be ready
            Become(&TThis::StateWaitForScanner);
        }
    }

    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        EXPORT_LOG_I("Handle TEvExportScan::TEvReady"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender
            << ", metadataUploaded# " << MetadataUploaded
            << ", schemeUploaded# " << SchemeUploaded
            << ", permissionsUploaded# " << PermissionsUploaded
            << ", changefeedsUploaded# " << ChangefeedsUploaded
            << ", error# " << Error.GetOrElse("none"));

        Scanner = ev->Sender;

        if (Error) {
            EXPORT_LOG_I("Handle TEvReady - has error, passing away"
                << ": self# " << SelfId()
                << ", error# " << Error.GetOrElse("none"));
            return PassAway();
        }

        const bool permissionsDone = !EnablePermissions || PermissionsUploaded;
        EXPORT_LOG_I("Handle TEvReady - checking completion"
            << ": self# " << SelfId()
            << ", permissionsDone# " << permissionsDone
            << ", enablePermissions# " << EnablePermissions);
            
        if (SchemeUploaded && MetadataUploaded && permissionsDone && ChangefeedsUploaded) {
            EXPORT_LOG_I("Handle TEvReady - all uploads done, finishing"
                << ": self# " << SelfId());
            Finish(true);
        } else {
            EXPORT_LOG_I("Handle TEvReady - waiting for uploads to complete"
                << ": self# " << SelfId());
        }
    }

    void Handle(TEvBuffer::TPtr& ev) {
        EXPORT_LOG_I("Handle TEvExportScan::TEvBuffer"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender
            << ", isScanner# " << (ev->Sender == Scanner)
            << ", last# " << ev->Get()->Last
            << ", msg# " << ev->Get()->ToString());

        if (ev->Sender == Scanner) {
            if (ev->Get()->Last) {
                EXPORT_LOG_I("Handle TEvBuffer - last buffer received, finishing"
                    << ": self# " << SelfId());
                Finish(true);
            } else {
                EXPORT_LOG_I("Handle TEvBuffer - requesting more data"
                    << ": self# " << SelfId());
                Send(Scanner, new TEvExportScan::TEvFeed());
            }
        }
    }

    void Finish(bool success = true, const TString& error = TString()) {
        EXPORT_LOG_I("Finish"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);

        if (!success) {
            Error = error;
        }

        if (Scanner) {
            Send(Scanner, new TEvExportScan::TEvFinish(success, error));
        }

        PassAway();
    }

    void PassAway() override {
        if (Scanner && Error) {
            Send(Scanner, new TEvExportScan::TEvFinish(false, Error.GetOrElse(TString())));
        }

        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR; // Reuse existing activity type
    }

    static constexpr TStringBuf LogPrefix() {
        return "fs"sv;
    }

    explicit TFsUploader(
            const TActorId& dataShard, ui64 txId,
            const NKikimrSchemeOp::TBackupTask& task,
            TMaybe<Ydb::Table::CreateTableRequest>&& scheme,
            TVector<TChangefeedExportDescriptions> changefeeds,
            TMaybe<Ydb::Scheme::ModifyPermissionsRequest>&& permissions,
            TString&& metadata)
        : Settings(TFsSettings::FromBackupTask(task))
        , DataShard(dataShard)
        , TxId(txId)
        , Scheme(std::move(scheme))
        , Changefeeds(std::move(changefeeds))
        , Metadata(std::move(metadata))
        , Permissions(std::move(permissions))
        , Retries(task.GetNumberOfRetries())
        , SchemeUploaded(task.GetShardNum() == 0 ? false : true)
        , ChangefeedsUploaded(task.GetShardNum() == 0 ? false : true)
        , MetadataUploaded(task.GetShardNum() == 0 ? false : true)
        , PermissionsUploaded(task.GetShardNum() == 0 ? false : true)
        , EnableChecksums(task.GetEnableChecksums())
        , EnablePermissions(task.GetEnablePermissions())
    {
        Y_UNUSED(DataShard);
        Y_UNUSED(TxId);
        Y_UNUSED(Retries);
    }

    void Bootstrap() {
        EXPORT_LOG_I("Bootstrap"
            << ": self# " << SelfId()
            << ", shardNum# " << Settings.Shard
            << ", basePath# " << Settings.BasePath
            << ", relativePath# " << Settings.RelativePath
            << ", metadataUploaded# " << MetadataUploaded
            << ", schemeUploaded# " << SchemeUploaded
            << ", permissionsUploaded# " << PermissionsUploaded
            << ", changefeedsUploaded# " << ChangefeedsUploaded);

        if (!MetadataUploaded) {
            EXPORT_LOG_I("Starting metadata upload (shard 0 path)"
                << ": self# " << SelfId());
            UploadMetadata();
        } else {
            EXPORT_LOG_I("Non-zero shard, waiting for scanner"
                << ": self# " << SelfId());
            Become(&TThis::StateWaitForScanner);
        }
    }

    STATEFN(StateBase) {
        EXPORT_LOG_D("StateBase received event"
            << ": self# " << SelfId()
            << ", type# " << ev->GetTypeRewrite());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReady, Handle);

            sFunc(TEvents::TEvWakeup, Bootstrap);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        default:
            EXPORT_LOG_W("StateBase unhandled event"
                << ": self# " << SelfId()
                << ", type# " << ev->GetTypeRewrite());
            break;
        }
    }

    STATEFN(StateWaitForScanner) {
        EXPORT_LOG_D("StateWaitForScanner received event"
            << ": self# " << SelfId()
            << ", type# " << ev->GetTypeRewrite());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReady, Handle);
            hFunc(TEvBuffer, Handle);

            sFunc(TEvents::TEvPoisonPill, PassAway);
        default:
            EXPORT_LOG_W("StateWaitForScanner unhandled event"
                << ": self# " << SelfId()
                << ", type# " << ev->GetTypeRewrite());
            break;
        }
    }

private:
    TFsSettings Settings;

    const TActorId DataShard;
    const ui64 TxId;
    const TMaybe<Ydb::Table::CreateTableRequest> Scheme;
    const TVector<TChangefeedExportDescriptions> Changefeeds;
    const TString Metadata;
    const TMaybe<Ydb::Scheme::ModifyPermissionsRequest> Permissions;

    const ui32 Retries;
    ui64 IndexExportedChangefeed = 0;

    TActorId Scanner;
    bool SchemeUploaded;
    bool ChangefeedsUploaded;
    bool MetadataUploaded;
    bool PermissionsUploaded;
    TMaybe<TString> Error;

    bool EnableChecksums;
    bool EnablePermissions;

}; // TFsUploader

IActor* TFsExport::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();

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
            descr.Prefix = descr.Name;

            metadata.AddChangefeed(TChangefeedMetadata{
                .ExportPrefix = descr.Prefix,
                .Name = descr.Name,
            });
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

    return new TFsUploader(
        dataShard, txId, Task, std::move(scheme), std::move(changefeeds), std::move(permissions), metadata.Serialize());
}

IExport::IBuffer* TFsExport::CreateBuffer() const {
    using namespace NBackupRestoreTraits;
    
    const auto& scanSettings = Task.GetScanSettings();
    const ui64 maxRows = scanSettings.GetRowsBatchSize() ? scanSettings.GetRowsBatchSize() : Max<ui64>();
    const ui64 maxBytes = scanSettings.GetBytesBatchSize();
    
    TS3ExportBufferSettings bufferSettings;
    bufferSettings
        .WithColumns(Columns)
        .WithMaxRows(maxRows)
        .WithMaxBytes(maxBytes)
        .WithMinBytes(0); // No minimum for filesystem
    
    if (Task.GetEnableChecksums()) {
        bufferSettings.WithChecksum(TS3ExportBufferSettings::Sha256Checksum());
    }

    switch (CodecFromTask(Task)) {
    case ECompressionCodec::None:
        break;
    case ECompressionCodec::Zstd:
        bufferSettings
            .WithCompression(TS3ExportBufferSettings::ZstdCompression(Task.GetCompression().GetLevel()));
        break;
    case ECompressionCodec::Invalid:
        Y_ENSURE(false, "unreachable");
    }

    return CreateS3ExportBuffer(std::move(bufferSettings));
}

} // NDataShard
} // NKikimr
