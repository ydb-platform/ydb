#include "export_common.h"
#include "export_fs.h"
#include "export_scan.h"
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
#include <util/stream/file.h>
#include <util/generic/buffer.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/string/builder.h>

#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NDataShard {

using namespace NBackup;

struct TChangefeedExportDescriptions {
    const Ydb::Table::ChangefeedDescription ChangefeedDescription;
    const Ydb::Topic::DescribeTopicResult Topic;
    TString Name;
    TString Prefix;
};

class TFsUploader: public TActorBootstrapped<TFsUploader> {
    struct TFsSettings {
        TString BasePath;
        TString Path;
        
        TString GetMetadataPath() const {
            return TFsPath(BasePath) / Path / "metadata";
        }
        
        TString GetSchemePath() const {
            return TFsPath(BasePath) / Path / "scheme";
        }
        
        TString GetPermissionsPath() const {
            return TFsPath(BasePath) / Path / "permissions";
        }
        
        TString GetChangefeedPath(const TString& prefix) const {
            return TFsPath(BasePath) / Path / (prefix + "_changefeed");
        }
        
        TString GetTopicPath(const TString& prefix) const {
            return TFsPath(BasePath) / Path / (prefix + "_topic");
        }
        
        TString GetChecksumPath(const TString& objectPath) const {
            return objectPath + ".checksum";
        }
        
        static TFsSettings FromBackupTask(const NKikimrSchemeOp::TBackupTask& task) {
            Y_ENSURE(task.HasFSSettings());
            const auto& fsSettings = task.GetFSSettings();
            
            TFsSettings result;
            result.BasePath = fsSettings.GetBasePath();
            result.Path = fsSettings.GetPath();
            return result;
        }
    };

    void WriteToFile(const TString& path, const TString& data) {
        EXPORT_LOG_D("WriteToFile"
            << ": self# " << SelfId()
            << ", path# " << path
            << ", size# " << data.size());
        
        try {
            // Ensure directory exists
            TFsPath filePath(path);
            TFsPath dirPath = filePath.Parent();
            dirPath.MkDirs();
            
            // Write file
            TFileOutput file(path);
            file.Write(data);
            file.Finish();
            
            EXPORT_LOG_I("Successfully wrote file"
                << ": self# " << SelfId()
                << ", path# " << path
                << ", size# " << data.size());
        } catch (const std::exception& ex) {
            Error = TStringBuilder() << "Failed to write file '" << path << "': " << ex.what();
            EXPORT_LOG_E("WriteToFile error"
                << ": self# " << SelfId()
                << ", path# " << path
                << ", error# " << Error);
            throw;
        }
    }
    
    void WriteMessage(const google::protobuf::Message& message, const TString& path, TString& checksum) {
        TString data;
        google::protobuf::TextFormat::PrintToString(message, &data);
        
        if (EnableChecksums) {
            checksum = ComputeChecksum(data);
        }
        
        WriteToFile(path, data);
        
        if (EnableChecksums) {
            WriteChecksum(checksum, Settings.GetChecksumPath(path), path);
        }
    }
    
    void WriteChecksum(const TString& checksum, const TString& checksumPath, const TString& objectPath) {
        // Format compatible with sha256sum CLI tool
        TString checksumData = checksum + " " + TFsPath(objectPath).GetName();
        WriteToFile(checksumPath, checksumData);
    }
    
    void UploadMetadata() {
        Y_ENSURE(!MetadataUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadMetadata"
            << ": self# " << SelfId());
        
        try {
            if (EnableChecksums) {
                MetadataChecksum = ComputeChecksum(Metadata);
            }
            
            WriteToFile(Settings.GetMetadataPath(), Metadata);
            
            if (EnableChecksums) {
                WriteChecksum(MetadataChecksum, Settings.GetChecksumPath(Settings.GetMetadataPath()), Settings.GetMetadataPath());
            }
            
            MetadataUploaded = true;
        } catch (...) {
            return Finish(false, Error.GetOrElse("Unknown error during metadata upload"));
        }
    }
    
    void UploadPermissions() {
        Y_ENSURE(EnablePermissions && !PermissionsUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadPermissions"
            << ": self# " << SelfId());
        
        if (!Permissions) {
            return Finish(false, "Cannot infer permissions");
        }
        
        try {
            WriteMessage(Permissions.GetRef(), Settings.GetPermissionsPath(), PermissionsChecksum);
            PermissionsUploaded = true;
        } catch (...) {
            return Finish(false, Error.GetOrElse("Unknown error during permissions upload"));
        }
    }
    
    void UploadScheme() {
        Y_ENSURE(!SchemeUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadScheme"
            << ": self# " << SelfId());
        
        if (!Scheme) {
            return Finish(false, "Cannot infer scheme");
        }
        
        try {
            WriteMessage(Scheme.GetRef(), Settings.GetSchemePath(), SchemeChecksum);
            SchemeUploaded = true;
        } catch (...) {
            return Finish(false, Error.GetOrElse("Unknown error during scheme upload"));
        }
    }
    
    void UploadChangefeed() {
        Y_ENSURE(!ChangefeedsUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadChangefeed"
            << ": self# " << SelfId()
            << ", index# " << IndexExportedChangefeed
            << ", total# " << Changefeeds.size());
        
        if (IndexExportedChangefeed == Changefeeds.size()) {
            ChangefeedsUploaded = true;
            return Finish();
        }
        
        try {
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            WriteMessage(desc.ChangefeedDescription, Settings.GetChangefeedPath(desc.Prefix), ChangefeedChecksum);
            UploadTopic();
        } catch (...) {
            return Finish(false, Error.GetOrElse("Unknown error during changefeed upload"));
        }
    }
    
    void UploadTopic() {
        Y_ENSURE(IndexExportedChangefeed < Changefeeds.size());
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadTopic"
            << ": self# " << SelfId()
            << ", index# " << IndexExportedChangefeed);
        
        try {
            const auto& desc = Changefeeds[IndexExportedChangefeed];
            WriteMessage(desc.Topic, Settings.GetTopicPath(desc.Prefix), TopicChecksum);
            
            ++IndexExportedChangefeed;
            UploadChangefeed();
        } catch (...) {
            return Finish(false, Error.GetOrElse("Unknown error during topic upload"));
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
        
        PassAway();
    }
    
    void PassAway() override {
        if (Scanner) {
            Send(Scanner, new TEvExportScan::TEvFinish(Error.Empty(), Error.GetOrElse(TString())));
        }
        
        IActor::PassAway();
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::EXPORT_S3_UPLOADER_ACTOR; // Reuse S3 activity type
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
        , ShardNum(task.GetShardNum())
        , SchemeUploaded(ShardNum == 0 ? false : true)
        , ChangefeedsUploaded(ShardNum == 0 ? false : true)
        , MetadataUploaded(ShardNum == 0 ? false : true)
        , PermissionsUploaded(ShardNum == 0 ? false : true)
        , EnableChecksums(task.GetEnableChecksums())
        , EnablePermissions(task.GetEnablePermissions())
    {
        Y_UNUSED(TxId);  // Reserved for future use
    }
    
    void Bootstrap() {
        EXPORT_LOG_D("Bootstrap"
            << ": self# " << SelfId()
            << ", shardNum# " << ShardNum);
        
        // For shard 0, upload metadata, permissions, scheme, changefeeds
        // For other shards, we would upload data (not implemented yet)
        if (ShardNum != 0) {
            // For now, just finish successfully for non-zero shards
            // Data export will be implemented later
            return Finish();
        }
        
        try {
            if (!MetadataUploaded) {
                UploadMetadata();
            }
            
            if (EnablePermissions && !PermissionsUploaded) {
                UploadPermissions();
            }
            
            if (!SchemeUploaded) {
                UploadScheme();
            }
            
            if (!ChangefeedsUploaded) {
                UploadChangefeed();
            } else {
                Finish();
            }
        } catch (...) {
            Finish(false, Error ? *Error : "Unknown error during bootstrap");
        }
    }
    
    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReady, Handle);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }
    
    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        EXPORT_LOG_D("Handle TEvExportScan::TEvReady"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender);
        
        Scanner = ev->Sender;
        
        // For schema-only export, we're already done
        if (Error) {
            return PassAway();
        }
        
        // Data export not implemented yet, so we finish here
        Finish();
    }

private:
    TFsSettings Settings;
    const TActorId DataShard;
    const ui64 TxId;
    const TMaybe<Ydb::Table::CreateTableRequest> Scheme;
    const TVector<TChangefeedExportDescriptions> Changefeeds;
    const TString Metadata;
    const TMaybe<Ydb::Scheme::ModifyPermissionsRequest> Permissions;
    
    const ui32 ShardNum;
    bool SchemeUploaded;
    bool ChangefeedsUploaded;
    bool MetadataUploaded;
    bool PermissionsUploaded;
    
    ui64 IndexExportedChangefeed = 0;
    
    TActorId Scanner;
    
    bool EnableChecksums;
    bool EnablePermissions;
    
    TString MetadataChecksum;
    TString ChangefeedChecksum;
    TString TopicChecksum;
    TString SchemeChecksum;
    TString PermissionsChecksum;
    
    TMaybe<TString> Error;
}; // TFsUploader

// Dummy buffer for schema-only export (no data scanning needed)
class TSchemaOnlyBuffer : public NExportScan::IBuffer {
public:
    void ColumnsOrder(const TVector<ui32>&) override {
        // No-op for schema-only export
    }

    bool Collect(const NTable::IScan::TRow&) override {
        // Should never be called for schema-only export
        return false;
    }

    IEventBase* PrepareEvent(bool, TStats&) override {
        // Should never be called for schema-only export
        return nullptr;
    }

    void Clear() override {
        // No-op for schema-only export
    }

    bool IsFilled() const override {
        // Never filled for schema-only export
        return false;
    }

    TString GetError() const override {
        return {};
    }
};

IActor* TFsExport::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();

    TMetadata metadata;
    metadata.SetVersion(Task.GetEnableChecksums() ? 1 : 0);
    metadata.SetEnablePermissions(Task.GetEnablePermissions());
    
    TVector<TChangefeedExportDescriptions> changefeeds;
    const bool enableChangefeedsExport = AppData() && AppData()->FeatureFlags.GetEnableChangefeedsExport();
    if (enableChangefeedsExport) {
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
            // For filesystem, use actual names (no anonymization for now)
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
    // For schema-only export, return a dummy buffer
    // Data export will be implemented later
    return new TSchemaOnlyBuffer();
}

} // NDataShard
} // NKikimr

