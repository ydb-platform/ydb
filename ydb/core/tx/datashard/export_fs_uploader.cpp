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
        std::cerr << "FSEXPORT_DEBUG: WriteToFile START path=" << path << " size=" << data.size() << std::endl;
        EXPORT_LOG_D("WriteToFile"
            << ": self# " << SelfId()
            << ", path# " << path
            << ", size# " << data.size());
        
        try {
            std::cerr << "FSEXPORT_DEBUG: WriteToFile creating TFsPath" << std::endl;
            TFsPath filePath(path);
            TFsPath dirPath = filePath.Parent();
            std::cerr << "FSEXPORT_DEBUG: WriteToFile calling MkDirs for " << dirPath.GetPath() << std::endl;
            dirPath.MkDirs();
            std::cerr << "FSEXPORT_DEBUG: WriteToFile MkDirs done" << std::endl;
            
            // Write file
            std::cerr << "FSEXPORT_DEBUG: WriteToFile opening file" << std::endl;
            TFileOutput file(path);
            std::cerr << "FSEXPORT_DEBUG: WriteToFile writing data" << std::endl;
            file.Write(data);
            std::cerr << "FSEXPORT_DEBUG: WriteToFile finishing file" << std::endl;
            file.Finish();
            std::cerr << "FSEXPORT_DEBUG: WriteToFile SUCCESS" << std::endl;
            
            EXPORT_LOG_I("Successfully wrote file"
                << ": self# " << SelfId()
                << ", path# " << path
                << ", size# " << data.size());
        } catch (const std::exception& ex) {
            std::cerr << "FSEXPORT_DEBUG: WriteToFile EXCEPTION: " << ex.what() << std::endl;
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
        std::cerr << "FSEXPORT_DEBUG: UploadMetadata START" << std::endl;
        Y_ENSURE(!MetadataUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadMetadata"
            << ": self# " << SelfId());
        
        try {
            if (EnableChecksums) {
                std::cerr << "FSEXPORT_DEBUG: UploadMetadata computing checksum" << std::endl;
                MetadataChecksum = ComputeChecksum(Metadata);
                std::cerr << "FSEXPORT_DEBUG: UploadMetadata checksum=" << MetadataChecksum << std::endl;
            }
            
            std::cerr << "FSEXPORT_DEBUG: UploadMetadata calling WriteToFile" << std::endl;
            WriteToFile(Settings.GetMetadataPath(), Metadata);
            std::cerr << "FSEXPORT_DEBUG: UploadMetadata WriteToFile done" << std::endl;
            
            if (EnableChecksums) {
                std::cerr << "FSEXPORT_DEBUG: UploadMetadata writing checksum" << std::endl;
                WriteChecksum(MetadataChecksum, Settings.GetChecksumPath(Settings.GetMetadataPath()), Settings.GetMetadataPath());
            }
            
            MetadataUploaded = true;
            std::cerr << "FSEXPORT_DEBUG: UploadMetadata SUCCESS" << std::endl;
        } catch (...) {
            std::cerr << "FSEXPORT_DEBUG: UploadMetadata EXCEPTION" << std::endl;
            return Finish(false, Error.GetOrElse("Unknown error during metadata upload"));
        }
    }
    
    void UploadPermissions() {
        std::cerr << "FSEXPORT_DEBUG: UploadPermissions START" << std::endl;
        Y_ENSURE(EnablePermissions && !PermissionsUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadPermissions"
            << ": self# " << SelfId());
        
        if (!Permissions) {
            std::cerr << "FSEXPORT_DEBUG: UploadPermissions Permissions is empty" << std::endl;
            return Finish(false, "Cannot infer permissions");
        }
        
        std::cerr << "FSEXPORT_DEBUG: UploadPermissions has Permissions, calling WriteMessage" << std::endl;
        try {
            WriteMessage(Permissions.GetRef(), Settings.GetPermissionsPath(), PermissionsChecksum);
            PermissionsUploaded = true;
            std::cerr << "FSEXPORT_DEBUG: UploadPermissions SUCCESS" << std::endl;
        } catch (...) {
            std::cerr << "FSEXPORT_DEBUG: UploadPermissions EXCEPTION" << std::endl;
            return Finish(false, Error.GetOrElse("Unknown error during permissions upload"));
        }
    }
    
    void UploadScheme() {
        std::cerr << "FSEXPORT_DEBUG: UploadScheme START" << std::endl;
        Y_ENSURE(!SchemeUploaded);
        Y_ENSURE(ShardNum == 0);
        
        EXPORT_LOG_D("UploadScheme"
            << ": self# " << SelfId());
        
        if (!Scheme) {
            std::cerr << "FSEXPORT_DEBUG: UploadScheme Scheme is empty" << std::endl;
            return Finish(false, "Cannot infer scheme");
        }
        
        std::cerr << "FSEXPORT_DEBUG: UploadScheme has Scheme, calling WriteMessage" << std::endl;
        try {
            WriteMessage(Scheme.GetRef(), Settings.GetSchemePath(), SchemeChecksum);
            SchemeUploaded = true;
            std::cerr << "FSEXPORT_DEBUG: UploadScheme SUCCESS" << std::endl;
        } catch (...) {
            std::cerr << "FSEXPORT_DEBUG: UploadScheme EXCEPTION" << std::endl;
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
        std::cerr << "FSEXPORT_DEBUG: Finish called success=" << success << " error=" << error << std::endl;
        EXPORT_LOG_I("Finish"
            << ": self# " << SelfId()
            << ", success# " << success
            << ", error# " << error);
        
        if (!success) {
            Error = error;
            std::cerr << "FSEXPORT_DEBUG: Finish setting Error=" << error << std::endl;
        }
        
        std::cerr << "FSEXPORT_DEBUG: Finish calling PassAway" << std::endl;
        PassAway();
    }
    
    void PassAway() override {
        std::cerr << "FSEXPORT_DEBUG: PassAway called Scanner=" << (void*)&Scanner << std::endl;
        if (Scanner) {
            std::cerr << "FSEXPORT_DEBUG: PassAway sending TEvFinish to Scanner" << std::endl;
            Send(Scanner, new TEvExportScan::TEvFinish(Error.Empty(), Error.GetOrElse(TString())));
            std::cerr << "FSEXPORT_DEBUG: PassAway TEvFinish sent" << std::endl;
        } else {
            std::cerr << "FSEXPORT_DEBUG: PassAway Scanner is NULL" << std::endl;
        }
        
        std::cerr << "FSEXPORT_DEBUG: PassAway calling IActor::PassAway" << std::endl;
        IActor::PassAway();
        std::cerr << "FSEXPORT_DEBUG: PassAway done" << std::endl;
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
        Y_UNUSED(TxId);
        std::cerr << "FSEXPORT_DEBUG: TFsUploader constructor shardNum=" << ShardNum 
                  << " hasScheme=" << Scheme.Defined()
                  << " hasPermissions=" << Permissions.Defined()
                  << " changefeeds.size=" << Changefeeds.size()
                  << " EnableChecksums=" << EnableChecksums
                  << " EnablePermissions=" << EnablePermissions << std::endl;
    }
    
    void Bootstrap() {
        std::cerr << "FSEXPORT_DEBUG: TFsUploader::Bootstrap START shardNum=" << ShardNum << std::endl;
        EXPORT_LOG_D("Bootstrap"
            << ": self# " << SelfId()
            << ", shardNum# " << ShardNum);
        
        Become(&TThis::StateBase);
        std::cerr << "FSEXPORT_DEBUG: TFsUploader::Bootstrap END (waiting for TEvReady)" << std::endl;
    }
    
    void DoWork() {
        std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork START shardNum=" << ShardNum << std::endl;
        
        if (ShardNum != 0) {
            std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork shardNum!=0, finishing" << std::endl;
            return Finish();
        }
        
        try {
            std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork MetadataUploaded=" << MetadataUploaded << std::endl;
            if (!MetadataUploaded) {
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork calling UploadMetadata" << std::endl;
                UploadMetadata();
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork UploadMetadata done" << std::endl;
            }
            
            std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork EnablePermissions=" << EnablePermissions 
                      << " PermissionsUploaded=" << PermissionsUploaded << std::endl;
            if (EnablePermissions && !PermissionsUploaded) {
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork calling UploadPermissions" << std::endl;
                UploadPermissions();
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork UploadPermissions done" << std::endl;
            }
            
            std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork SchemeUploaded=" << SchemeUploaded << std::endl;
            if (!SchemeUploaded) {
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork calling UploadScheme" << std::endl;
                UploadScheme();
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork UploadScheme done" << std::endl;
            }
            
            std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork ChangefeedsUploaded=" << ChangefeedsUploaded << std::endl;
            if (!ChangefeedsUploaded) {
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork calling UploadChangefeed" << std::endl;
                UploadChangefeed();
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork UploadChangefeed done" << std::endl;
            } else {
                std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork no changefeeds, calling Finish" << std::endl;
                Finish();
            }
        } catch (...) {
            std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork EXCEPTION caught" << std::endl;
            Finish(false, Error ? *Error : "Unknown error during work");
        }
        std::cerr << "FSEXPORT_DEBUG: TFsUploader::DoWork END" << std::endl;
    }
    
    STATEFN(StateBase) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvExportScan::TEvReady, Handle);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }
    
    void Handle(TEvExportScan::TEvReady::TPtr& ev) {
        std::cerr << "FSEXPORT_DEBUG: Handle TEvExportScan::TEvReady START sender=" << ev->Sender.ToString() << std::endl;
        EXPORT_LOG_D("Handle TEvExportScan::TEvReady"
            << ": self# " << SelfId()
            << ", sender# " << ev->Sender);
        
        Scanner = ev->Sender;
        std::cerr << "FSEXPORT_DEBUG: Handle TEvExportScan::TEvReady Scanner set, calling DoWork" << std::endl;
        
        if (Error) {
            std::cerr << "FSEXPORT_DEBUG: Handle TEvExportScan::TEvReady Error is set, calling PassAway" << std::endl;
            return PassAway();
        }
        
        DoWork();
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

class TSchemaOnlyBuffer : public NExportScan::IBuffer {
public:
    TSchemaOnlyBuffer() {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer constructor" << std::endl;
    }
    
    void ColumnsOrder(const TVector<ui32>&) override {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer::ColumnsOrder called" << std::endl;
    }

    bool Collect(const NTable::IScan::TRow&) override {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer::Collect called - returning false" << std::endl;
        return false;
    }

    IEventBase* PrepareEvent(bool, TStats&) override {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer::PrepareEvent called - returning nullptr" << std::endl;
        return nullptr;
    }

    void Clear() override {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer::Clear called" << std::endl;
    }

    bool IsFilled() const override {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer::IsFilled called - returning false" << std::endl;
        return false;
    }

    TString GetError() const override {
        std::cerr << "FSEXPORT_DEBUG: TSchemaOnlyBuffer::GetError called" << std::endl;
        return {};
    }
};

IActor* TFsExport::CreateUploader(const TActorId& dataShard, ui64 txId) const {
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader START shardNum=" << Task.GetShardNum() << std::endl;
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader dataShard=" << dataShard.ToString() << " txId=" << txId << std::endl;
    
    auto scheme = (Task.GetShardNum() == 0)
        ? GenYdbScheme(Columns, Task.GetTable())
        : Nothing();
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader GenYdbScheme done, scheme.Defined()=" << scheme.Defined() << std::endl;

    TMetadata metadata;
    metadata.SetVersion(Task.GetEnableChecksums() ? 1 : 0);
    metadata.SetEnablePermissions(Task.GetEnablePermissions());
    
    TVector<TChangefeedExportDescriptions> changefeeds;
    const bool enableChangefeedsExport = AppData() && AppData()->FeatureFlags.GetEnableChangefeedsExport();
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader enableChangefeedsExport=" << enableChangefeedsExport << std::endl;
    if (enableChangefeedsExport) {
        std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader processing changefeeds" << std::endl;
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
    
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader before GenYdbPermissions" << std::endl;
    auto permissions = (Task.GetEnablePermissions() && Task.GetShardNum() == 0)
        ? GenYdbPermissions(Task.GetTable())
        : Nothing();
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader GenYdbPermissions done, permissions.Defined()=" << permissions.Defined() << std::endl;
    
    TFullBackupMetadata::TPtr backup = new TFullBackupMetadata{
        .SnapshotVts = TVirtualTimestamp(
            Task.GetSnapshotStep(),
            Task.GetSnapshotTxId())
    };
    metadata.AddFullBackup(backup);
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader metadata ready, changefeeds.size=" << changefeeds.size() << std::endl;
    
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader creating TFsUploader" << std::endl;
    auto* uploader = new TFsUploader(
        dataShard, txId, Task, std::move(scheme), std::move(changefeeds), std::move(permissions), metadata.Serialize());
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateUploader TFsUploader created=" << (void*)uploader << std::endl;
    return uploader;
}

IExport::IBuffer* TFsExport::CreateBuffer() const {
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateBuffer START" << std::endl;
    // For schema-only export, return a dummy buffer
    // Data export will be implemented later
    auto* buffer = new TSchemaOnlyBuffer();
    std::cerr << "FSEXPORT_DEBUG: TFsExport::CreateBuffer returning buffer=" << (void*)buffer << std::endl;
    return buffer;
}

} // NDataShard
} // NKikimr

