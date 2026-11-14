#include "schemeshard.h"
#include <ydb/library/ydb_issue/issue_helpers.h>

#include "schemeshard_impl.h"
#include "schemeshard_types.h"

namespace NKikimr {
namespace NSchemeShard {
    TEvSchemeShard::TEvInitTenantSchemeShard::TEvInitTenantSchemeShard(
        ui64 selfTabletId,
        ui64 pathId, TString tenantRootPath,
        TString owner, TString effectiveRootACL, ui64 effectiveRootACLVersion,
        const NKikimrSubDomains::TProcessingParams &processingParams, const TStoragePools &storagePools,
        const TMap<TString, TString> userAttrData, ui64 UserAttrsVersion, const NSchemeShard::TSchemeLimits &limits,
        ui64 sharedHive, const TPathId& resourcesDomainId)
    {
        Record.SetDomainSchemeShard(selfTabletId);
        Record.SetDomainPathId(pathId);
        Record.SetRootPath(tenantRootPath);
        Record.SetOwner(owner);

        Record.SetEffectiveACL(effectiveRootACL);
        Record.SetEffectiveACLVersion(effectiveRootACLVersion);

        Record.MutableProcessingParams()->CopyFrom(processingParams);
        for (auto& x: storagePools) {
            *Record.AddStoragePools() = x;
        }

        for (auto& x: userAttrData) {
            auto item = Record.AddUserAttributes();
            item->SetKey(x.first);
            item->SetValue(x.second);
        }
        Record.SetUserAttributesVersion(UserAttrsVersion);

        *Record.MutableSchemeLimits() = limits.AsProto();

        if (sharedHive != ui64(InvalidTabletId)) {
            Record.SetSharedHive(sharedHive);
        }

        if (resourcesDomainId) {
            Record.SetResourcesDomainOwnerId(resourcesDomainId.OwnerId);
            Record.SetResourcesDomainPathId(resourcesDomainId.LocalPathId);
        }
    }
}

IActor* CreateFlatTxSchemeShard(const TActorId &tablet, TTabletStorageInfo *info) {
    return new NSchemeShard::TSchemeShard(tablet, info);
}

bool PartitionConfigHasExternalBlobsEnabled(const NKikimrSchemeOp::TPartitionConfig &partitionConfig) {
    for (auto &family : partitionConfig.GetColumnFamilies()) {
        if (family.GetId() != 0) {
            // We don't currently support per-family settings for external
            // blobs or legacy storage mapping, so we may safely ignore
            // non-primary column families.
            continue;
        }
        if (family.HasStorageConfig()) {
            const ui32 externalThreshold = family.GetStorageConfig().GetExternalThreshold();
            if (externalThreshold != 0 && externalThreshold != Max<ui32>())
                return true;
        }
        if (family.HasStorage()) {
            switch (family.GetStorage()) {
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage2:
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Ext1:
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Ext2:
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext1:
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Ext2:
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage1Med2Ext2:
            case NKikimrSchemeOp::EColumnStorage::ColumnStorage2Med2Ext2:
                return true;
            default:
                break;
            }
        }
    }
    return false;
}

}

namespace NKikimr::NSchemeShard::TEvSchemeShard {

TEvProcessingRequest::TEvProcessingRequest(const ISSDataProcessor& processor) {
    Record.SetClassName(processor.GetClassName());
    Record.SetData(processor.SerializeToString());
}

ISSDataProcessor::TPtr TEvProcessingRequest::RestoreProcessor() const {
    auto result = ISSDataProcessor::TFactory::MakeHolder(TBase::Record.GetClassName());
    if (!result) {
        return nullptr;
    } else if (!result->DeserializeFromString(TBase::Record.GetData())) {
        return nullptr;
    } else {
        return std::shared_ptr<ISSDataProcessor>(result.Release());
    }
}

TEvModifySchemeTransaction::TEvModifySchemeTransaction(ui64 txid, ui64 tabletId)
{
    Record.SetTxId(txid);
    Record.SetTabletId(tabletId);
}

TString TEvModifySchemeTransaction::ToString() const {
    TStringStream str;
    str << "{TEvModifySchemeTransaction";
    if (Record.HasTxId()) {
        str << " txid# " << Record.GetTxId();
    }
    if (Record.HasTabletId()) {
        str << " TabletId# " << Record.GetTabletId();
    }
    str << "}";
    return str.Str();
}

TEvCancelTxResult::TEvCancelTxResult(ui64 targetTxId, ui64 txId) {
    Record.SetTargetTxId(targetTxId);
    Record.SetTxId(txId);
}

TEvModifySchemeTransactionResult::TEvModifySchemeTransactionResult(TTxId txid, TTabletId schemeshardId) {
    Record.SetTxId(ui64(txid));
    Record.SetSchemeshardId(ui64(schemeshardId));
}

TEvModifySchemeTransactionResult::TEvModifySchemeTransactionResult(EStatus status, ui64 txid, ui64 schemeshardId, const TStringBuf& reason)
    : TEvModifySchemeTransactionResult(TTxId(txid), TTabletId(schemeshardId))
{
    Record.SetStatus(status);
    if (reason.size() > 0) {
        Record.SetReason(reason.data(), reason.size());
    }
}

bool TEvModifySchemeTransactionResult::IsConditionalAccepted() const {
    //happens on retries, we answer like StatusAccepted with error message and do nothing in operation
    return !Record.GetReason().empty() && (Record.GetStatus() == EStatus::StatusAccepted);
}

void TEvModifySchemeTransactionResult::SetError(EStatus status, const TString& errStr) {
    Record.SetStatus(status);
    Record.SetReason(errStr);
}

void TEvModifySchemeTransactionResult::AddWarning(const TString& text) {
    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::WARNING, text);
    NYql::IssueToMessage(issue, Record.AddIssues());
}

void TEvModifySchemeTransactionResult::AddNotice(const TString& text) {
    auto issue = MakeIssue(NKikimrIssues::TIssuesIds::INFO, text);
    NYql::IssueToMessage(issue, Record.AddIssues());
}

TString TEvModifySchemeTransactionResult::ToString() const {
    TStringStream str;
    str << "{TEvModifySchemeTransactionResult";
    if (Record.HasStatus()) {
        str << " Status# " << Record.GetStatus();
    }
    if (Record.HasTxId()) {
        str << " txid# " << Record.GetTxId();
    }
    if (Record.HasReason()) {
        str << " Reason# " << Record.GetReason();
    }
    str << "}";
    return str.Str();
}

TEvInitRootShard::TEvInitRootShard(const TActorId& source, ui32 rootTag, const TString& rootTagName)
{
    ActorIdToProto(source, Record.MutableSource());
    Record.SetRootTag(rootTag);
    Record.SetRootTagName(rootTagName);
}

TEvInitRootShardResult::TEvInitRootShardResult(ui64 origin, EStatus status)
{
    Record.SetOrigin(origin);
    Record.SetStatus(status);
}

TEvDescribeScheme::TEvDescribeScheme(ui64 tabletId, ui64 pathId)
{
    Record.SetSchemeshardId(tabletId);
    Record.SetPathId(pathId);
}

TEvDescribeScheme::TEvDescribeScheme(TTableId tableId)
{
    Record.SetSchemeshardId(tableId.PathId.OwnerId);
    Record.SetPathId(tableId.PathId.LocalPathId);
}

TEvDescribeScheme::TEvDescribeScheme(NKikimr::TPathId pathId)
{
    Record.SetSchemeshardId(pathId.OwnerId);
    Record.SetPathId(pathId.LocalPathId);
}

TEvDescribeSchemeResult::TEvDescribeSchemeResult(const TString& path, TPathId pathId)
{
    Record.SetPath(path);
    Record.SetPathId(pathId.LocalPathId);
    Record.SetPathOwnerId(pathId.OwnerId);
}

TString TEvDescribeSchemeResult::ToString() const {
    TStringStream str;
    str << ToStringHeader()
        << " PreSerializedData size# " << PreSerializedData.size()
        << " Record# " << Record.ShortDebugString()
    ;
    return str.Str();
}

TEvInitTenantSchemeShardResult::TEvInitTenantSchemeShardResult(ui64 tenantSchemeShard, EStatus status) {
    Record.SetTenantSchemeShard(tenantSchemeShard);
    Record.SetStatus(status);
}

TPathId TEvMigrateSchemeShard::GetPathId() const {
    if (!Record.GetPath().HasPathId()) {
        return NKikimr::TPathId();
    }

    auto& pathId = Record.GetPath().GetPathId();
    return NKikimr::TPathId(pathId.GetOwnerId(), pathId.GetLocalId());
}

TPathId TEvMigrateSchemeShardResult::GetPathId() const {
    if (!Record.HasPathId()) {
        return NKikimr::TPathId();
    }

    auto& pathId = Record.GetPathId();
    return NKikimr::TPathId(pathId.GetOwnerId(), pathId.GetLocalId());
}

TEvPublishTenantAsReadOnlyResult::TEvPublishTenantAsReadOnlyResult(ui64 tenantSchemeShard, EStatus status) {
    Record.SetTenantSchemeShard(tenantSchemeShard);
    Record.SetStatus(status);
}

TEvSyncTenantSchemeShard::TEvSyncTenantSchemeShard(const TEvSyncTenantSchemeShardInitializer& _)
{
    Record.SetDomainSchemeShard(_.DomainKey.OwnerId);
    Record.SetDomainPathId(_.DomainKey.LocalPathId);

    Record.SetTabletID(_.TabletId);
    Record.SetGeneration(_.Generation);

    Record.SetEffectiveACLVersion(_.EffectiveACLVersion);
    Record.SetSubdomainVersion(_.SubdomainVersion);
    Record.SetUserAttributesVersion(_.UserAttrsVersion);

    Record.SetTenantHive(_.TenantHive);
    Record.SetTenantSysViewProcessor(_.TenantSysViewProcessor);
    Record.SetTenantStatisticsAggregator(_.TenantStatisticsAggregator);
    Record.SetTenantGraphShard(_.TenantGraphShard);
    Record.SetTenantWasmCompileController(_.TenantWasmCompileController);

    Record.SetTenantRootACL(_.RootACL);
}

TEvUpdateTenantSchemeShard::TEvUpdateTenantSchemeShard(ui64 tabletId, ui64 generation) {
    Record.SetTabletId(tabletId);
    Record.SetGeneration(generation);
}

void TEvUpdateTenantSchemeShard::SetEffectiveACL(const TString& owner, const TString& effectiveACL, ui64 version) {
    Record.SetOwner(owner);
    Record.SetEffectiveACL(effectiveACL);
    Record.SetEffectiveACLVersion(version);
}

void TEvUpdateTenantSchemeShard::SetStoragePools(const TStoragePools& storagePools, ui64 version) {
    for (auto& x: storagePools) {
        *Record.AddStoragePools() = x;
    }
    Record.SetSubdomainVersion(version);
}

void TEvUpdateTenantSchemeShard::SetUserAttrs(const TMap<TString, TString>& userAttrData, ui64 version) {
    for (auto& x: userAttrData) {
        auto item = Record.AddUserAttributes();
        item->SetKey(x.first);
        item->SetValue(x.second);
    }
    Record.SetUserAttributesVersion(version);
}

TEvFindTabletSubDomainPathIdResult::TEvFindTabletSubDomainPathIdResult(ui64 tabletId, EStatus status) {
    Record.SetStatus(status);
    Record.SetTabletId(tabletId);
}

TEvFindTabletSubDomainPathIdResult::TEvFindTabletSubDomainPathIdResult(ui64 tabletId, ui64 schemeShardId, ui64 subDomainPathId) {
    Record.SetStatus(NKikimrScheme::TEvFindTabletSubDomainPathIdResult::SUCCESS);
    Record.SetTabletId(tabletId);
    Record.SetSchemeShardId(schemeShardId);
    Record.SetSubDomainPathId(subDomainPathId);
}

TEvTenantShredResponse::TEvTenantShredResponse(const TPathId& pathId, ui64 generation, const NKikimrScheme::TEvTenantShredResponse::EStatus& status) {
    Record.MutablePathId()->SetOwnerId(pathId.OwnerId);
    Record.MutablePathId()->SetLocalId(pathId.LocalPathId);
    Record.SetGeneration(generation);
    Record.SetStatus(status);
}

TEvShredInfoResponse::TEvShredInfoResponse(ui64 generation, const NKikimrScheme::TEvShredInfoResponse::EStatus& status) {
    Record.SetGeneration(generation);
    Record.SetStatus(status);
}

} // namespace NKikimr::NSchemeShard::TEvSchemeShard
