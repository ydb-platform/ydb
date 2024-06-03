#include "schemeshard_path_element.h"

#include <library/cpp/json/json_reader.h>

namespace NKikimr::NSchemeShard {

namespace {

void UpdateSpaceBegin(TSpaceLimits& limits, ui64 newValue, ui64 oldValue) {
    if (newValue <= oldValue) {
        return;
    }

    // Space increase is handled at tx begin
    limits.Allocated += newValue - oldValue;
}

void UpdateSpaceCommit(TSpaceLimits& limits, ui64 newValue, ui64 oldValue) {
    if (newValue >= oldValue) {
        return;
    }

    // Space decrease is handled at tx commit
    const ui64 diff = oldValue - newValue;
    Y_ABORT_UNLESS(limits.Allocated >= diff);
    limits.Allocated -= diff;
}

bool CheckSpaceChanged(const TSpaceLimits& limits, ui64 newValue, ui64 oldValue,
        TString& errStr, const char* tabletType, const char* suffix) {
    if (newValue <= oldValue) {
        return true;
    }

    const ui64 diff = newValue - oldValue;
    const ui64 newAllocated = limits.Allocated + diff;
    if (newAllocated <= limits.Limit) {
        return true;
    }

    errStr = TStringBuilder()
        << "New " << tabletType << " space is over a limit" << suffix
        << ": " << newAllocated << " > " << limits.Limit;
    return false;
}

}

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
    Y_ABORT_UNLESS(Max<ui64>() - AliveChildrenCount >= delta);
    AliveChildrenCount += delta;

    if (isBackup) {
        Y_ABORT_UNLESS(Max<ui64>() - BackupChildrenCount >= delta);
        BackupChildrenCount += delta;
    }
}

void TPathElement::DecAliveChildren(ui64 delta, bool isBackup) {
    Y_ABORT_UNLESS(AliveChildrenCount >= delta);
    AliveChildrenCount -= delta;

    if (isBackup) {
        Y_ABORT_UNLESS(BackupChildrenCount >= delta);
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
    Y_ABORT_UNLESS(Max<ui64>() - ShardsInsideCount >= delta);
    ShardsInsideCount += delta;
}

void TPathElement::DecShardsInside(ui64 delta) {
    Y_ABORT_UNLESS(ShardsInsideCount >= delta);
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
    return IsDirectory() || IsDomainRoot() || IsOlapStore() || IsTableIndex();
}

bool TPathElement::HasActiveChanges() const {
    // there are old clusters where Root node has CreateTxId == 0
    return (!IsRoot() && !CreateTxId) || (PathState != EPathState::EPathStateNoChanges);
}

bool TPathElement::IsCreateFinished() const {
    return (IsRoot() && CreateTxId) || StepCreated;
}

TVirtualTimestamp TPathElement::GetCreateTS() const {
    return TVirtualTimestamp(StepCreated, CreateTxId);
}

TVirtualTimestamp TPathElement::GetDropTS() const {
    return TVirtualTimestamp(StepDropped, DropTxId);
}

bool TPathElement::IsExternalTable() const {
    return PathType == EPathType::EPathTypeExternalTable;
}

bool TPathElement::IsExternalDataSource() const {
    return PathType == EPathType::EPathTypeExternalDataSource;
}

bool TPathElement::IsView() const {
    return PathType == EPathType::EPathTypeView;
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
    FileStoreSpaceSSD.Limit = Max<ui64>();
    FileStoreSpaceHDD.Limit = Max<ui64>();
    ExtraPathSymbolsAllowed = TString();
    DocumentApiVersion = 0;
    AsyncReplication = NJson::TJsonValue();

    for (const auto& [key, value] : UserAttrs->Attrs) {
        switch (TUserAttributes::ParseName(key)) {
            case EAttribute::VOLUME_SPACE_LIMIT:
                HandleAttributeValue(value, VolumeSpaceRaw.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_SSD:
                HandleAttributeValue(value, VolumeSpaceSSD.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_HDD:
                HandleAttributeValue(value, VolumeSpaceHDD.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_NONREPL:
                HandleAttributeValue(value, VolumeSpaceSSDNonrepl.Limit);
                break;
            case EAttribute::VOLUME_SPACE_LIMIT_SSD_SYSTEM:
                HandleAttributeValue(value, VolumeSpaceSSDSystem.Limit);
                break;
            case EAttribute::FILESTORE_SPACE_LIMIT_SSD:
                HandleAttributeValue(value, FileStoreSpaceSSD.Limit);
                break;
            case EAttribute::FILESTORE_SPACE_LIMIT_HDD:
                HandleAttributeValue(value, FileStoreSpaceHDD.Limit);
                break;
            case EAttribute::EXTRA_PATH_SYMBOLS_ALLOWED:
                HandleAttributeValue(value, ExtraPathSymbolsAllowed);
                break;
            case EAttribute::DOCUMENT_API_VERSION:
                HandleAttributeValue(value, DocumentApiVersion);
                break;
            case EAttribute::ASYNC_REPLICATION:
                HandleAttributeValue(value, AsyncReplication);
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
    UpdateSpaceBegin(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw);
    UpdateSpaceBegin(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD);
    UpdateSpaceBegin(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD);
    UpdateSpaceBegin(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl);
    UpdateSpaceBegin(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem);
}

void TPathElement::ChangeVolumeSpaceCommit(TVolumeSpace newSpace, TVolumeSpace oldSpace) {
    UpdateSpaceCommit(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw);
    UpdateSpaceCommit(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD);
    UpdateSpaceCommit(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD);
    UpdateSpaceCommit(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl);
    UpdateSpaceCommit(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem);
}

bool TPathElement::CheckVolumeSpaceChange(TVolumeSpace newSpace, TVolumeSpace oldSpace, TString& errStr) {
    return (CheckSpaceChanged(VolumeSpaceRaw, newSpace.Raw, oldSpace.Raw, errStr, "volume", "") &&
            CheckSpaceChanged(VolumeSpaceSSD, newSpace.SSD, oldSpace.SSD, errStr, "volume", " (ssd)") &&
            CheckSpaceChanged(VolumeSpaceHDD, newSpace.HDD, oldSpace.HDD, errStr, "volume", " (hdd)") &&
            CheckSpaceChanged(VolumeSpaceSSDNonrepl, newSpace.SSDNonrepl, oldSpace.SSDNonrepl, errStr, "volume", " (ssd_nonrepl)") &&
            CheckSpaceChanged(VolumeSpaceSSDSystem, newSpace.SSDSystem, oldSpace.SSDSystem, errStr, "volume", " (ssd_system)"));
}

void TPathElement::ChangeFileStoreSpaceBegin(TFileStoreSpace newSpace, TFileStoreSpace oldSpace) {
    UpdateSpaceBegin(FileStoreSpaceSSD, newSpace.SSD, oldSpace.SSD);
    UpdateSpaceBegin(FileStoreSpaceHDD, newSpace.HDD, oldSpace.HDD);
}

void TPathElement::ChangeFileStoreSpaceCommit(TFileStoreSpace newSpace, TFileStoreSpace oldSpace) {
    UpdateSpaceCommit(FileStoreSpaceSSD, newSpace.SSD, oldSpace.SSD);
    UpdateSpaceCommit(FileStoreSpaceHDD, newSpace.HDD, oldSpace.HDD);
}

bool TPathElement::CheckFileStoreSpaceChange(TFileStoreSpace newSpace, TFileStoreSpace oldSpace, TString& errStr) {
    return (CheckSpaceChanged(FileStoreSpaceSSD, newSpace.SSD, oldSpace.SSD, errStr, "filestore", " (ssd)") &&
            CheckSpaceChanged(FileStoreSpaceHDD, newSpace.HDD, oldSpace.HDD, errStr, "filestore", " (hdd)"));
}

void TPathElement::SetAsyncReplica() {
    IsAsyncReplica = true;
}

bool TPathElement::HasRuntimeAttrs() const {
    return (VolumeSpaceRaw.Allocated > 0 ||
            VolumeSpaceSSD.Allocated > 0 ||
            VolumeSpaceHDD.Allocated > 0 ||
            VolumeSpaceSSDNonrepl.Allocated > 0 ||
            VolumeSpaceSSDSystem.Allocated > 0 ||
            FileStoreSpaceSSD.Allocated > 0 ||
            FileStoreSpaceHDD.Allocated > 0 ||
            IsAsyncReplica);
}

void TPathElement::SerializeRuntimeAttrs(
        google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TUserAttribute>* userAttrs) const
{
    auto process = [userAttrs](const TSpaceLimits& limits, const char* name) {
        if (limits.Allocated > 0) {
            auto* attr = userAttrs->Add();
            attr->SetKey(name);
            attr->SetValue(TStringBuilder() << limits.Allocated);
        }
    };

    // blockstore volume
    process(VolumeSpaceRaw, "__volume_space_allocated");
    process(VolumeSpaceSSD, "__volume_space_allocated_ssd");
    process(VolumeSpaceHDD, "__volume_space_allocated_hdd");
    process(VolumeSpaceSSDNonrepl, "__volume_space_allocated_ssd_nonrepl");
    process(VolumeSpaceSSDSystem, "__volume_space_allocated_ssd_system");

    // filestore
    process(FileStoreSpaceSSD, "__filestore_space_allocated_ssd");
    process(FileStoreSpaceHDD, "__filestore_space_allocated_hdd");

    if (IsAsyncReplica) {
        auto* attr = userAttrs->Add();
        attr->SetKey(ToString(ATTR_ASYNC_REPLICA));
        attr->SetValue("true");
    }
}

}
