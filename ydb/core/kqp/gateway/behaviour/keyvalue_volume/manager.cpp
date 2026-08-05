#include "manager.h"
#include "s3_channels.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/kqp/gateway/utils/metadata_helpers.h>
#include <ydb/core/kqp/gateway/utils/scheme_helpers.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>

#include <util/string/builder.h>
#include <util/string/cast.h>

namespace NKikimr::NKqp {

namespace {

using TYqlConclusionStatus = TKeyValueVolumeManager::TYqlConclusionStatus;
using TAsyncStatus = TKeyValueVolumeManager::TAsyncStatus;

template <class TValue>
using TYqlConclusion = TConclusionImpl<TYqlConclusionStatus, TValue>;

// Only KeyValue volumes exist so far, but TYPE is mandatory so that the same statement can serve other volume
// flavours later on.
constexpr TStringBuf VolumeTypeKeyValue = "KEY_VALUE";

// The KeyValue tablet keeps its system data in channel 0 and its log in channel 1, both of which have to live on
// physical groups; user data starts at channel 2.
constexpr ui32 FirstDataChannel = 2;
constexpr ui32 MinChannelCount = FirstDataChannel + 1;

// Mirrors VolumeChannelKey() in the SQL translation layer: object features are a flat string map, so a channel list
// travels as channel_count plus channel_<index>_<setting> entries.
TString ChannelKey(ui32 index, TStringBuf setting) {
    return TStringBuilder() << "channel_" << index << '_' << setting;
}

struct TVolumeChannel {
    ui32 Index = 0;
    TString Media;
    TString DataSourcePath;
    TString ObjectPrefix;
    TString StoragePoolKind;
    TString BlobDepotMedia;
    bool AsyncMode = false;

    bool IsS3() const {
        return !DataSourcePath.empty();
    }
};

struct TVolumeSettings {
    ui64 PartitionCount = 0;
    std::vector<TVolumeChannel> Channels;
};

TYqlConclusionStatus BadRequest(const TString& message) {
    return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_BAD_REQUEST, message);
}

TYqlConclusionStatus ValidateObjectPrefix(const TVolumeChannel& channel) {
    const TString& prefix = channel.ObjectPrefix;
    const auto fail = [&](TStringBuf reason) {
        return BadRequest(TStringBuilder() << "OBJECT_PREFIX of channel " << channel.Index << " " << reason
                                           << ", got: " << prefix);
    };
    if (prefix.empty()) {
        return BadRequest(TStringBuilder() << "OBJECT_PREFIX is required for channel " << channel.Index);
    }
    if (prefix.StartsWith('/')) {
        return fail("must not start with '/'");
    }
    if (!prefix.EndsWith('/')) {
        return fail("must end with '/'");
    }
    if (prefix.Contains("..")) {
        return fail("must not contain '..'");
    }
    return TYqlConclusionStatus::Success();
}

TYqlConclusionStatus ValidateChannel(const TVolumeChannel& channel) {
    if (channel.Media.empty() == channel.DataSourcePath.empty()) {
        return BadRequest(TStringBuilder() << "Channel " << channel.Index
                                           << " must set exactly one of MEDIA and DATA_SOURCE");
    }

    if (!channel.IsS3()) {
        if (channel.ObjectPrefix || channel.StoragePoolKind || channel.BlobDepotMedia) {
            return BadRequest(TStringBuilder() << "OBJECT_PREFIX, STORAGE_POOL and BLOB_DEPOT_MEDIA are only allowed for"
                                                  " a channel with a DATA_SOURCE, channel " << channel.Index);
        }
        return TYqlConclusionStatus::Success();
    }

    if (channel.Index < FirstDataChannel) {
        return BadRequest(TStringBuilder() << "Channel " << channel.Index << " keeps the tablet's own data and cannot be"
                                              " backed by a DATA_SOURCE, use MEDIA instead");
    }
    if (channel.StoragePoolKind.empty()) {
        return BadRequest(TStringBuilder() << "STORAGE_POOL is required for channel " << channel.Index
                                           << " because it is backed by a DATA_SOURCE");
    }
    return ValidateObjectPrefix(channel);
}

TYqlConclusion<TVolumeSettings> ParseVolumeSettings(NYql::TFeaturesExtractor& features) {
    const auto type = features.Extract("type");
    if (!type) {
        return BadRequest("TYPE is required for a volume");
    }
    if (to_upper(*type) != VolumeTypeKeyValue) {
        return BadRequest(TStringBuilder() << "Unknown volume TYPE: " << *type << ", only " << VolumeTypeKeyValue
                                           << " is supported");
    }

    TVolumeSettings result;

    if (const auto value = features.Extract("partition_count")) {
        if (!TryFromString(*value, result.PartitionCount) || !result.PartitionCount) {
            return BadRequest(TStringBuilder() << "PARTITION_COUNT must be a positive integer, got: " << *value);
        }
    } else {
        return BadRequest("PARTITION_COUNT is required for a volume");
    }

    // channel_count is produced by the translation layer, a user can only write CHANNELS.
    const auto channelCount = features.Extract<ui32>("channel_count");
    if (!channelCount) {
        return BadRequest("CHANNELS is required for a volume");
    }
    if (*channelCount < MinChannelCount) {
        return BadRequest(TStringBuilder() << "CHANNELS must list at least " << MinChannelCount
                                           << " channels: system, log and at least one data channel");
    }

    result.Channels.resize(*channelCount);
    for (ui32 index = 0; index < *channelCount; ++index) {
        auto& channel = result.Channels[index];
        channel.Index = index;
        channel.Media = features.Extract(ChannelKey(index, "media")).value_or("");
        channel.DataSourcePath = features.Extract(ChannelKey(index, "data_source")).value_or("");
        channel.ObjectPrefix = features.Extract(ChannelKey(index, "object_prefix")).value_or("");
        channel.StoragePoolKind = features.Extract(ChannelKey(index, "storage_pool")).value_or("");
        channel.BlobDepotMedia = features.Extract(ChannelKey(index, "blob_depot_media")).value_or("");

        const auto syncMode = to_upper(features.Extract(ChannelKey(index, "sync_mode")).value_or(TString("SYNC")));
        if (syncMode == "ASYNC") {
            channel.AsyncMode = true;
        } else if (syncMode != "SYNC") {
            return BadRequest(TStringBuilder() << "SYNC_MODE of channel " << index
                                               << " must be either SYNC or ASYNC, got: " << syncMode);
        }

        if (auto status = ValidateChannel(channel); status.IsFail()) {
            return status;
        }
    }

    if (!features.IsFinished()) {
        return BadRequest(TStringBuilder() << "Unknown property: " << features.GetRemainedParamsString());
    }

    // A depot stores its own log and data next to the volume's system channel unless told otherwise.
    for (auto& channel : result.Channels) {
        if (channel.IsS3() && channel.BlobDepotMedia.empty()) {
            channel.BlobDepotMedia = result.Channels[0].Media;
        }
    }
    return result;
}

TYqlConclusion<std::pair<TString, TString>> SplitVolumePath(const TString& objectId) {
    std::pair<TString, TString> pathPair;
    TString error;
    if (!NSchemeHelpers::TrySplitTablePath(objectId, pathPair, error)) {
        return BadRequest(TStringBuilder() << "Invalid volume path: " << error);
    }
    return pathPair;
}

TYqlConclusionStatus CheckFeatureFlag(const TKeyValueVolumeManager::TInternalModificationContext& context) {
    auto* actorSystem = context.GetExternalData().GetActorSystem();
    if (!actorSystem) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            "Internal error. VOLUME operations need an actor system. Please contact internal support");
    }
    if (!AppData(actorSystem)->FeatureFlags.GetEnableKeyValueVolumeDdl()) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNSUPPORTED,
            "CREATE VOLUME and DROP VOLUME are disabled. Please contact your system administrator to enable it");
    }
    return TYqlConclusionStatus::Success();
}

TYqlConclusionStatus PrepareCreate(NKqpProto::TKqpCreateKeyValueVolume& operation,
                                   const NYql::TCreateObjectSettings& settings,
                                   const TKeyValueVolumeManager::TInternalModificationContext& context) {
    if (auto status = CheckFeatureFlag(context); status.IsFail()) {
        return status;
    }

    auto pathPair = SplitVolumePath(settings.GetObjectId());
    if (pathPair.IsFail()) {
        return pathPair;
    }
    const auto& [workingDir, name] = *pathPair;

    auto volume = ParseVolumeSettings(settings.GetFeaturesExtractor());
    if (volume.IsFail()) {
        return volume;
    }

    auto& schemeTx = *operation.MutableSchemeTx();
    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateSolomonVolume);
    schemeTx.SetFailedOnAlreadyExists(!settings.GetExistingOk());

    // The very same transaction the KeyValue gRPC API issues, which is what makes a volume created by DDL
    // indistinguishable from one created by kvtool - and therefore safe to keep using after a downgrade.
    auto& volumeDesc = *schemeTx.MutableCreateSolomonVolume();
    volumeDesc.SetName(name);
    volumeDesc.SetPartitionCount(volume->PartitionCount);

    auto& storageConfig = *volumeDesc.MutableStorageConfig();
    for (const auto& channel : volume->Channels) {
        // An S3-backed channel binds to the pool holding its BlobDepot's virtual group; a local one to its media.
        storageConfig.AddChannel()->SetPreferredPoolKind(channel.IsS3() ? channel.StoragePoolKind : channel.Media);

        if (!channel.IsS3()) {
            continue;
        }
        auto& s3Channel = *operation.AddS3Channels();
        s3Channel.SetChannelIndex(channel.Index);
        s3Channel.SetDataSourcePath(channel.DataSourcePath);
        s3Channel.SetObjectPrefix(channel.ObjectPrefix);
        s3Channel.SetStoragePoolKind(channel.StoragePoolKind);
        s3Channel.SetBlobDepotMedia(channel.BlobDepotMedia);
        s3Channel.SetAsyncMode(channel.AsyncMode);
        // Deriving the name from the volume path makes allocation idempotent across retries of the same statement.
        s3Channel.SetVirtualGroupName(TStringBuilder() << workingDir << '/' << name << ":ch" << channel.Index);
    }

    return TYqlConclusionStatus::Success();
}

TYqlConclusionStatus PrepareDrop(NKikimrSchemeOp::TModifyScheme& schemeTx,
                                 const NYql::TDropObjectSettings& settings,
                                 const TKeyValueVolumeManager::TInternalModificationContext& context) {
    if (auto status = CheckFeatureFlag(context); status.IsFail()) {
        return status;
    }

    auto pathPair = SplitVolumePath(settings.GetObjectId());
    if (pathPair.IsFail()) {
        return pathPair;
    }
    const auto& [workingDir, name] = *pathPair;

    schemeTx.SetWorkingDir(workingDir);
    schemeTx.SetOperationType(NKikimrSchemeOp::ESchemeOpDropSolomonVolume);
    schemeTx.SetSuccessOnNotExist(settings.GetMissingOk());
    schemeTx.MutableDrop()->SetName(name);

    // The BlobDepots serving S3 channels outlive the volume on purpose: they still hold the data, and cancelling a
    // virtual group stays an administrative operation (`dstool group virtual cancel`).
    return TYqlConclusionStatus::Success();
}

TAsyncStatus ExecuteCreate(const NKqpProto::TKqpCreateKeyValueVolume& operation,
                           const TKeyValueVolumeManager::TExternalModificationContext& context) {
    if (!operation.S3ChannelsSize()) {
        return SendSchemeRequest(operation.GetSchemeTx(), context);
    }
    // The depot has to be up before the volume binds a channel to its pool.
    return ChainFeatures(AllocateS3Channels(operation, context), [operation, context] {
        return SendSchemeRequest(operation.GetSchemeTx(), context);
    });
}

TYqlConclusionStatus ErrorFromActivityType(TKeyValueVolumeManager::EActivityType activityType) {
    using EActivityType = TKeyValueVolumeManager::EActivityType;

    switch (activityType) {
        case EActivityType::Undefined:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                "Internal error. Undefined operation for a VOLUME object");
        case EActivityType::Alter:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNIMPLEMENTED,
                "Alter operation for VOLUME objects is not implemented");
        case EActivityType::Upsert:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_UNIMPLEMENTED,
                "Upsert operation for VOLUME objects is not implemented");
        default:
            return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                TStringBuilder() << "Internal error. Unexpected operation for a VOLUME object: " << activityType);
    }
}

}   // anonymous namespace

TAsyncStatus TKeyValueVolumeManager::DoModify(const NYql::TObjectSettingsImpl& settings, const ui32 nodeId,
                                              const NMetadata::IClassBehaviour::TPtr& manager,
                                              TInternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager);

    try {
        NKqpProto::TKqpSchemeOperation schemeOperation;
        if (auto status = DoPrepare(schemeOperation, settings, manager, context); status.IsFail()) {
            return NThreading::MakeFuture(status);
        }
        return ExecutePrepared(schemeOperation, nodeId, manager, context.GetExternalData());
    } catch (...) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            TStringBuilder() << "Internal error. Got unexpected exception during VOLUME modification operation: "
                             << CurrentExceptionMessage()));
    }
}

TYqlConclusionStatus TKeyValueVolumeManager::DoPrepare(NKqpProto::TKqpSchemeOperation& schemeOperation,
                                                       const NYql::TObjectSettingsImpl& settings,
                                                       const NMetadata::IClassBehaviour::TPtr& manager,
                                                       TInternalModificationContext& context) const {
    Y_UNUSED(manager);

    try {
        switch (context.GetActivityType()) {
            case EActivityType::Create:
                return PrepareCreate(*schemeOperation.MutableCreateKeyValueVolume(), settings, context);
            case EActivityType::Drop:
                return PrepareDrop(*schemeOperation.MutableDropKeyValueVolume(), settings, context);
            default:
                return ErrorFromActivityType(context.GetActivityType());
        }
    } catch (...) {
        return TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            TStringBuilder() << "Internal error. Got unexpected exception during preparation of a VOLUME modification"
                                " operation: " << CurrentExceptionMessage());
    }
}

TAsyncStatus TKeyValueVolumeManager::ExecutePrepared(const NKqpProto::TKqpSchemeOperation& schemeOperation,
                                                     const ui32 nodeId,
                                                     const NMetadata::IClassBehaviour::TPtr& manager,
                                                     const TExternalModificationContext& context) const {
    Y_UNUSED(nodeId, manager);

    try {
        switch (schemeOperation.GetOperationCase()) {
            case NKqpProto::TKqpSchemeOperation::kCreateKeyValueVolume:
                return ExecuteCreate(schemeOperation.GetCreateKeyValueVolume(), context);
            case NKqpProto::TKqpSchemeOperation::kDropKeyValueVolume:
                return SendSchemeRequest(schemeOperation.GetDropKeyValueVolume(), context);
            default:
                return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
                    TStringBuilder() << "Execution of a prepared operation for a VOLUME object: unsupported operation: "
                                     << static_cast<i32>(schemeOperation.GetOperationCase())));
        }
    } catch (...) {
        return NThreading::MakeFuture(TYqlConclusionStatus::Fail(NYql::TIssuesIds::KIKIMR_INTERNAL_ERROR,
            TStringBuilder() << "Internal error. Got unexpected exception during execution of a VOLUME modification"
                                " operation: " << CurrentExceptionMessage()));
    }
}

}   // namespace NKikimr::NKqp
