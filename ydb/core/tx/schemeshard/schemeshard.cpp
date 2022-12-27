#include "schemeshard.h"
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
