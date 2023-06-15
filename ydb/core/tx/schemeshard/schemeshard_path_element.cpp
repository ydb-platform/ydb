#include "schemeshard_path_element.h"

namespace NKikimr {
namespace NSchemeShard {

TPathElement::TPathElement(TPathId pathId, TPathId parentPathId, TPathId domainPathId, const TString& name, const TString& owner)
    : PathId(pathId)
    , ParentPathId(parentPathId)
    , DomainPathId(domainPathId)
    , Name(name)
    , Owner(owner)
    , UserAttrs(new TUserAttributes(1))
{}

ui64 TPathElement::GetAliveChildren() const {
    return AliveChildrenCount;
}

void TPathElement::SetAliveChildren(ui64 val) {
    AliveChildrenCount = val;
}

ui64 TPathElement::GetBackupChildren() const {
    return BackupChildrenCount;
}

void TPathElement::IncAliveChildren(ui64 delta, bool isBackup) {
    Y_VERIFY(Max<ui64>() - AliveChildrenCount >= delta);
    AliveChildrenCount += delta;

    if (isBackup) {
        Y_VERIFY(Max<ui64>() - BackupChildrenCount >= delta);
        BackupChildrenCount += delta;
    }
}

void TPathElement::DecAliveChildren(ui64 delta, bool isBackup) {
    Y_VERIFY(AliveChildrenCount >= delta);
    AliveChildrenCount -= delta;

    if (isBackup) {
        Y_VERIFY(BackupChildrenCount >= delta);
        BackupChildrenCount -= delta;
    }
}

ui64 TPathElement::GetShardsInside() const {
    return ShardsInsideCount;
}

void TPathElement::SetShardsInside(ui64 val) {
    ShardsInsideCount = val;
}

void TPathElement::IncShardsInside(ui64 delta) {
    Y_VERIFY(Max<ui64>() - ShardsInsideCount >= delta);
    ShardsInsideCount += delta;
}

void TPathElement::DecShardsInside(ui64 delta) {
    Y_VERIFY(ShardsInsideCount >= delta);
    ShardsInsideCount -= delta;
}

bool TPathElement::IsRoot() const {
    return PathId.LocalPathId == RootPathId;
}

bool TPathElement::IsDirectory() const {
    return PathType == EPathType::EPathTypeDir;
}

bool TPathElement::IsTableIndex() const {
    return PathType == EPathType::EPathTypeTableIndex;
}

bool TPathElement::IsCdcStream() const {
    return PathType == EPathType::EPathTypeCdcStream;
}

bool TPathElement::IsTable() const {
    return PathType == EPathType::EPathTypeTable;
}

bool TPathElement::IsSolomon() const {
    return PathType == EPathType::EPathTypeSolomonVolume;
}

bool TPathElement::IsPQGroup() const {
    return PathType == EPathType::EPathTypePersQueueGroup;
}

bool TPathElement::IsDomainRoot() const {
    return IsSubDomainRoot() || IsExternalSubDomainRoot();
}

bool TPathElement::IsSubDomainRoot() const {
    return PathType == EPathType::EPathTypeSubDomain || IsRoot();
}

bool TPathElement::IsExternalSubDomainRoot() const {
    return PathType == EPathType::EPathTypeExtSubDomain;
}

bool TPathElement::IsRtmrVolume() const {
    return PathType == EPathType::EPathTypeRtmrVolume;
}

bool TPathElement::IsBlockStoreVolume() const {
    return PathType == EPathType::EPathTypeBlockStoreVolume;
}

bool TPathElement::IsFileStore() const {
    return PathType == EPathType::EPathTypeFileStore;
}

bool TPathElement::IsKesus() const {
    return PathType == EPathType::EPathTypeKesus;
}

bool TPathElement::IsOlapStore() const {
    return PathType == EPathType::EPathTypeColumnStore;
}

bool TPathElement::IsColumnTable() const {
    return PathType == EPathType::EPathTypeColumnTable;
}

bool TPathElement::IsSequence() const {
    return PathType == EPathType::EPathTypeSequence;
}

bool TPathElement::IsReplication() const {
    return PathType == EPathType::EPathTypeReplication;
}

bool TPathElement::IsBlobDepot() const {
    return PathType == EPathType::EPathTypeBlobDepot;
}

bool TPathElement::IsContainer() const {
    return PathType == EPathType::EPathTypeDir || PathType == EPathType::EPathTypeSubDomain
        || PathType == EPathType::EPathTypeColumnStore;
}

bool TPathElement::IsLikeDirectory() const {
    return IsDirectory() || IsDomainRoot() || IsOlapStore();
}

bool TPathElement::HasActiveChanges() const {
    // there are old clusters where Root node has CreateTxId == 0
    return (!IsRoot() && !CreateTxId) || (PathState != EPathState::EPathStateNoChanges);
}

bool TPathElement::IsCreateFinished() const {
    return (IsRoot() && CreateTxId) || StepCreated;
}

TGlobalTimestamp TPathElement::GetCreateTS() const {
    return TGlobalTimestamp(StepCreated, CreateTxId);
}

TGlobalTimestamp TPathElement::GetDropTS() const {
    return TGlobalTimestamp(StepDropped, DropTxId);
}

void TPathElement::SetDropped(TStepId step, TTxId txId) {
    PathState = EPathState::EPathStateNotExist;
    StepDropped = step;
    DropTxId = txId;
}

bool TPathElement::NormalState() const {
    return PathState == EPathState::EPathStateNoChanges;
}

bool TPathElement::Dropped() const {
    if (StepDropped) {
        Y_VERIFY_DEBUG_S(PathState == EPathState::EPathStateNotExist,
                            "Non consistent PathState and StepDropped."
                                << " PathState: " << NKikimrSchemeOp::EPathState_Name(PathState)
                                << ", StepDropped: " << StepDropped
                                << ", PathId: " << PathId);
    }
    return bool(StepDropped);
}

bool TPathElement::IsMigrated() const {
    return PathState == EPathState::EPathStateMigrated;
}

bool TPathElement::IsUnderMoving() const {
    return PathState == EPathState::EPathStateMoving;
}

bool TPathElement::IsUnderCreating() const {
    return PathState == EPathState::EPathStateCreate;
}

bool TPathElement::PlannedToCreate() const {
    return PathState == EPathState::EPathStateCreate;
}

bool TPathElement::PlannedToDrop() const {
    return PathState == EPathState::EPathStateDrop;
}

bool TPathElement::AddChild(const TString& name, TPathId pathId, bool replace) {
    TPathId* ptr = FindChild(name);
    if (ptr && !replace)
        return false;
    Children[name] = pathId;
    return true;
}

bool TPathElement::RemoveChild(const TString& name, TPathId pathId) {
    auto it = Children.find(name);
    if (it != Children.end() && it->second == pathId) {
        Children.erase(it);
        return true;
    }
    return false;
}

TPathId* TPathElement::FindChild(const TString& name) {
    return Children.FindPtr(name);
}

const TPathElement::TChildrenCont& TPathElement::GetChildren() const {
    return Children;
}

void TPathElement::SwapChildren(TChildrenCont& container) {
    container.swap(Children);
}

void TPathElement::ApplyACL(const TString& acl) {
    NACLib::TACL secObj(ACL);
    NACLib::TDiffACL diffACL(acl);
    secObj.ApplyDiff(diffACL);
    ACL = secObj.SerializeAsString();
}

void TPathElement::ApplySpecialAttributes() {
    VolumeSpaceRaw.Limit = Max<ui64>();
    VolumeSpaceSSD.Limit = Max<ui64>();
    VolumeSpaceHDD.Limit = Max<ui64>();
    VolumeSpaceSSDNonrepl.Limit = Max<ui64>();
    VolumeSpaceSSDSystem.Limit = Max<ui64>();
    ExtraPathSymbolsAllowed = TString();
    for (const auto& item : UserAttrs->Attrs) {
        switch (TUserAttributes::ParseName(item.first)) {
            case EAttribute::VOLUME_SPACE_LIMIT:
                HandleAttributeValue(item.second, VolumeSpaceRaw.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_SSD:
                HandleAttributeValue(item.second, VolumeSpaceSSD.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_HDD:
                HandleAttributeValue(item.second, VolumeSpaceHDD.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_NONREPL:
                HandleAttributeValue(item.second, VolumeSpaceSSDNonrepl.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_SYSTEM:
                HandleAttributeValue(item.second, VolumeSpaceSSDSystem.Limit);
                break;
            case EAttribute::EXTRA_PATH_SYMBOLS_ALLOWED:
                HandleAttributeValue(item.second, ExtraPathSymbolsAllowed);
                break;
            case EAttribute::DOCUMENT_API_VERSION:
                HandleAttributeValue(item.second, DocumentApiVersion);
                break;
            case EAttribute::ASYNC_REPLICATION:
                HandleAttributeValue(item.second, AsyncReplication);
                break;
            default:
                break;
        }
    }
}

void TPathElement::HandleAttributeValue(const TString& value, TString& target) {
    target = value;
}

void TPathElement::HandleAttributeValue(const TString& value, ui64& target) {
    ui64 parsed;
    if (TryFromString(value, parsed)) {
        target = parsed;
    }
}

void TPathElement::HandleAttributeValue(const TString& value, NJson::TJsonValue& target) {
    NJson::TJsonValue parsed;
    if (NJson::ReadJsonTree(value, &parsed)) {
        target = std::move(parsed);
    }
}

void TPathElement::ChangeVolumeSpaceBegin(TVolumeSpace newSpace, TVolumeSpace oldSpace) {
    auto update = [](TVolumeSpaceLimits& limits, ui64 newValue, ui64 oldValue) {
        if (newValue > oldValue) {
            // Volume space increase is handled at tx begin
            limits.Allocated += newValue - oldValue;
        }
    };
    update(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw);
    update(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD);
    update(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD);
    update(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl);
    update(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem);
}

void TPathElement::ChangeVolumeSpaceCommit(TVolumeSpace newSpace, TVolumeSpace oldSpace) {
    auto update = [](TVolumeSpaceLimits& limits, ui64 newValue, ui64 oldValue) {
        if (newValue < oldValue) {
            // Volume space decrease is handled at tx commit
            ui64 diff = oldValue - newValue;
            Y_VERIFY(limits.Allocated >= diff);
            limits.Allocated -= diff;
        }
    };
    update(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw);
    update(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD);
    update(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD);
    update(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl);
    update(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem);
}

bool TPathElement::CheckVolumeSpaceChange(TVolumeSpace newSpace, TVolumeSpace oldSpace, TString& errStr) {
    auto check = [&errStr](const TVolumeSpaceLimits& limits, ui64 newValue, ui64 oldValue, const char* suffix) -> bool {
        if (newValue > oldValue) {
            ui64 newAllocated = limits.Allocated + newValue - oldValue;
            if (newAllocated > limits.Limit) {
                errStr = TStringBuilder()
                    << "New volume space is over a limit" << suffix
                    << ": " << newAllocated << " > " << limits.Limit;
                return false;
            }
        }
        return true;
    };
    return (check(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw, "") &&
            check(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD, " (ssd)") &&
            check(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD, " (hdd)") &&
            check(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl, " (ssd_nonrepl)") &&
            check(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem, " (ssd_system)"));
}

bool TPathElement::HasRuntimeAttrs() const {
    return (VolumeSpaceRaw.Allocated > 0 ||
            VolumeSpaceSSD.Allocated > 0 ||
            VolumeSpaceHDD.Allocated > 0 ||
            VolumeSpaceSSDNonrepl.Allocated > 0 ||
            VolumeSpaceSSDSystem.Allocated > 0);
}

void TPathElement::SerializeRuntimeAttrs(
        google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TUserAttribute>* userAttrs) const
{
    auto process = [userAttrs](const TVolumeSpaceLimits& limits, const char* name) {
        if (limits.Allocated > 0) {
            auto* attr = userAttrs->Add();
            attr->SetKey(name);
            attr->SetValue(TStringBuilder() << limits.Allocated);
        }
    };
    process(VolumeSpaceRaw, "__volume_space_allocated");
    process(VolumeSpaceSSD, "__volume_space_allocated_ssd");
    process(VolumeSpaceHDD, "__volume_space_allocated_hdd");
    process(VolumeSpaceSSDNonrepl, "__volume_space_allocated_ssd_nonrepl");
    process(VolumeSpaceSSDSystem, "__volume_space_allocated_ssd_system");
}
}
}
