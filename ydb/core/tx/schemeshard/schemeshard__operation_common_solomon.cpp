#include "schemeshard__operation_common_external_data_source.h"

#include <ydb/core/mind/hive/hive.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NSchemeShard {

namespace {

TPathId ResolveExternalStorage(TSchemeShard* ss, const NKikimrSchemeOp::TKeyValueStorageConfig::TChannel& channel,
        const TPathId domainId) {
    if (channel.HasExternalDataSource()) {
        const auto& path = channel.GetExternalDataSource();
        if (!path.StartsWith('/')) {
            return {};
        }

        const auto source = TPath::Resolve(path, ss);
        if (source.IsResolved() && source->IsExternalDataSource() && source.GetPathIdForDomain() == domainId) {
            return source->PathId;
        }

        return {};
    }

    if (!channel.GetStoragePool().empty()) {
        for (const auto& [id, source] : ss->ExternalDataSources) {
            if (NExternalDataSource::GetStoragePoolAlias(source->Properties) == channel.GetStoragePool()) {
                const auto path = TPath::Init(id, ss);
                if (!path.IsDeleted() && path.GetPathIdForDomain() == domainId) {
                    return id;
                }
            }
        }
    }

    return {};
}

} // namespace

TShardIdx TSolomonVolumeInfo::GetFirstPartitionShardIdx() const {
    Y_ABORT_UNLESS(!Partitions.empty());
    return MinElementBy(Partitions, [](const auto& partition) {
        return partition.second->PartitionId;
    })->first;
}

bool TSchemeShard::ResolveSolomonVolumeChannels(const NKikimrSchemeOp::TKeyValueStorageConfig& config,
        const TPath& volume, const NACLib::TUserToken* userToken, TChannelsBindings& bindings,
        TEvSchemeShard::TEvModifySchemeTransactionResult& result) {
    if (ui32(config.ChannelSize()) > NHive::MAX_TABLET_CHANNELS) {
        result.SetError(NKikimrScheme::StatusInvalidParameter, "Too many KV storage channels");
        return false;
    }

    const auto domainId = volume.GetPathIdForDomain();
    const auto& pools = SubDomains.at(domainId)->EffectiveStoragePools();
    if (pools.empty()) {
        result.SetError(NKikimrScheme::StatusInvalidParameter, "No storage pools available in domain");
        return false;
    }

    for (const auto& channel : config.GetChannel()) {
        const auto index = bindings.size();
        auto& binding = bindings.emplace_back();
        if (!channel.HasExternalDataSource() && !channel.HasStoragePool()) {
            if (!BindChannelByPoolKind(binding, pools, channel.GetPreferredPoolKind())) {
                result.SetError(NKikimrScheme::StatusInvalidParameter, "Unable to construct channel binding with the storage pool");
                return false;
            }

            continue;
        }

        if (index < 2 || channel.HasPreferredPoolKind()) {
            result.SetError(NKikimrScheme::StatusInvalidParameter,
                "External storage can only select a data channel and cannot be combined with media");
            return false;
        }

        const auto sourceId = ResolveExternalStorage(this, channel, domainId);
        const auto* sourceInfo = ExternalDataSources.FindPtr(sourceId);
        if (!sourceInfo || NExternalDataSource::GetStoragePoolAlias((*sourceInfo)->Properties).empty()) {
            result.SetError(NKikimrScheme::StatusInvalidParameter, "External data source with a storage pool was not found in this database");
            return false;
        }

        const auto source = TPath::Init(sourceId, this);
        if (const auto checks = source.Check().IsAtLocalSchemeShard().NotDeleted().NotUnderOperation(); !checks) {
            result.SetError(checks.GetStatus(), checks.GetError());
            return false;
        }

        if (!AppData()->FeatureFlags.GetEnableExternalDataSourceStoragePools()) {
            result.SetError(NKikimrScheme::StatusPreconditionFailed, "External data source storage pools are disabled");
            return false;
        }

        if (userToken && !TSecurityObject(source->Owner, source.GetEffectiveACL(), false).CheckAccess(
                NACLib::SelectRow | NACLib::UpdateRow | NACLib::EraseRow, *userToken)) {
            result.SetError(NKikimrScheme::StatusAccessDenied, "Access denied for external data source storage pool");
            return false;
        }

        if ((*sourceInfo)->Properties.HasStoragePoolError()) {
            result.SetError(NKikimrScheme::StatusPreconditionFailed, TStringBuilder()
                << "External data source storage pool is unavailable: " << (*sourceInfo)->Properties.GetStoragePoolError());
            return false;
        }

        binding.SetStoragePoolName(NExternalDataSource::GetStoragePoolName(source.GetDomainPathString(), sourceId));
        sourceId.ToProto(binding.MutableExternalDataSourceId());
        binding.SetExternalDataSourcePath(source.PathString());
    }

    return true;
}

void TSchemeShard::PersistSolomonExternalDataSourceReferences(NIceDb::TNiceDb& db, const TPath& volume,
        const TChannelsBindings& bindings) {
    for (const auto& binding : bindings) {
        if (binding.HasExternalDataSourceId()) {
            const auto sourceId = TPathId::FromProto(binding.GetExternalDataSourceId());
            if (SolomonExternalDataSources[volume->PathId].insert(sourceId).second) {
                PersistExternalDataSourceReference(db, sourceId, volume);
                ClearDescribePathCaches(PathsById.at(sourceId));
            }
        }
    }
}

void TSchemeShard::ReleaseSolomonExternalDataSourceReferences(NIceDb::TNiceDb& db, TPathId volumeId) {
    const auto it = SolomonExternalDataSources.find(volumeId);
    if (it == SolomonExternalDataSources.end()) {
        return;
    }

    for (const auto sourceId : it->second) {
        if (ExternalDataSources.contains(sourceId)) {
            PersistRemoveExternalDataSourceReference(db, sourceId, volumeId);
            ClearDescribePathCaches(PathsById.at(sourceId));
        }
    }

    SolomonExternalDataSources.erase(it);
}

} // namespace NKikimr::NSchemeShard
