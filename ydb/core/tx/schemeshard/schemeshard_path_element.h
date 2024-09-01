#pragma once

#include "schemeshard_types.h"
#include "schemeshard_effective_acl.h"
#include "user_attributes.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actorid.h>

#include <library/cpp/json/json_value.h>

#include <util/generic/map.h>
#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard {

struct TVolumeSpace {
    ui64 Raw = 0;
    ui64 SSD = 0;
    ui64 HDD = 0;
    ui64 SSDNonrepl = 0;
    ui64 SSDSystem = 0;
};

struct TFileStoreSpace {
    ui64 SSD = 0;
    ui64 HDD = 0;
};

struct TSpaceLimits {
    ui64 Allocated = 0;
    ui64 Limit = Max<ui64>();
};

struct TPathElement : TSimpleRefCount<TPathElement> {
    using TPtr = TIntrusivePtr<TPathElement>;
    using TChildrenCont = TMap<TString, TPathId>;
    using EPathType = NKikimrSchemeOp::EPathType;
    using EPathSubType = NKikimrSchemeOp::EPathSubType;
    using EPathState = NKikimrSchemeOp::EPathState;

    static constexpr TLocalPathId RootPathId = 1;

    TPathId PathId = InvalidPathId;
    TPathId ParentPathId = InvalidPathId;
    TPathId DomainPathId = InvalidPathId;

    TString Name;
    TString Owner;
    TString ACL;

    EPathType PathType = EPathType::EPathTypeDir;
    EPathState PathState = EPathState::EPathStateNotExist;

    TStepId StepCreated = InvalidStepId;
    TTxId CreateTxId = InvalidTxId;
    TStepId StepDropped = InvalidStepId;
    TTxId DropTxId = InvalidTxId;
    TTxId LastTxId = InvalidTxId;

    ui64 DirAlterVersion = 0;
    ui64 ACLVersion = 0;

    TUserAttributes::TPtr UserAttrs;

    TString PreSerializedChildrenListing;

    TEffectiveACL CachedEffectiveACL;
    ui64 CachedEffectiveACLVersion = 0;

    TString ExtraPathSymbolsAllowed; // it's better to move it in TSubDomainInfo like SchemeLimits

    TSpaceLimits VolumeSpaceRaw;
    TSpaceLimits VolumeSpaceSSD;
    TSpaceLimits VolumeSpaceHDD;
    TSpaceLimits VolumeSpaceSSDNonrepl;
    TSpaceLimits VolumeSpaceSSDSystem;
    TSpaceLimits FileStoreSpaceSSD;
    TSpaceLimits FileStoreSpaceHDD;
    ui64 DocumentApiVersion = 0;
    NJson::TJsonValue AsyncReplication;
    bool IsAsyncReplica = false;
    bool IsRestoreTable = false;

    // Number of references to this path element in the database
    size_t DbRefCount = 0;
    size_t AllChildrenCount = 0;

    TActorId TempDirOwnerActorId; // Only for EPathType::EPathTypeDir.
                                  // Not empty if dir must be deleted after loosing connection with TempDirOwnerActorId actor.
                                  // See schemeshard__background_cleaning.cpp.

private:
    ui64 AliveChildrenCount = 0;
    ui64 BackupChildrenCount = 0;
    ui64 ShardsInsideCount = 0;
    TChildrenCont Children;
public:
    TPathElement(TPathId pathId, TPathId parentPathId, TPathId domainPathId, const TString& name, const TString& owner);
    ui64 GetAliveChildren() const;
    void SetAliveChildren(ui64 val);
    ui64 GetBackupChildren() const;
    void IncAliveChildren(ui64 delta = 1, bool isBackup = false);
    void DecAliveChildren(ui64 delta = 1, bool isBackup = false);
    ui64 GetShardsInside() const;
    void SetShardsInside(ui64 val);
    void IncShardsInside(ui64 delta = 1);
    void DecShardsInside(ui64 delta = 1);
    bool IsRoot() const;
    bool IsDirectory() const;
    bool IsTableIndex() const;
    bool IsCdcStream() const;
    bool IsTable() const;
    bool IsSolomon() const;
    bool IsPQGroup() const;
    bool IsDomainRoot() const;
    bool IsSubDomainRoot() const;
    bool IsExternalSubDomainRoot() const;
    bool IsRtmrVolume() const;
    bool IsBlockStoreVolume() const;
    bool IsFileStore() const;
    bool IsKesus() const;
    bool IsOlapStore() const;
    bool IsColumnTable() const;
    bool IsSequence() const;
    bool IsReplication() const;
    bool IsBlobDepot() const;
    bool IsContainer() const;
    bool IsLikeDirectory() const;
    bool HasActiveChanges() const;
    bool IsCreateFinished() const;
    bool IsExternalTable() const;
    bool IsExternalDataSource() const;
    bool IsIncrementalBackupTable() const;
    bool IsView() const;
    bool IsTemporary() const;
    bool IsResourcePool() const;
    TVirtualTimestamp GetCreateTS() const;
    TVirtualTimestamp GetDropTS() const;
    void SetDropped(TStepId step, TTxId txId);
    bool NormalState() const;
    bool Dropped() const;
    bool IsMigrated() const;
    bool IsUnderMoving() const;
    bool IsUnderCreating() const;
    bool PlannedToCreate() const;
    bool PlannedToDrop() const;
    bool AddChild(const TString& name, TPathId pathId, bool replace = false);
    bool RemoveChild(const TString& name, TPathId pathId);
    TPathId* FindChild(const TString& name);
    const TChildrenCont& GetChildren() const;
    void SwapChildren(TChildrenCont& container);
    void ApplyACL(const TString& acl);
    void ApplySpecialAttributes();
    void HandleAttributeValue(const TString& value, TString& target);
    void HandleAttributeValue(const TString& value, ui64& target);
    void HandleAttributeValue(const TString& value, NJson::TJsonValue& target);
    void ChangeVolumeSpaceBegin(TVolumeSpace newSpace, TVolumeSpace oldSpace);
    void ChangeVolumeSpaceCommit(TVolumeSpace newSpace, TVolumeSpace oldSpace);
    bool CheckVolumeSpaceChange(TVolumeSpace newSpace, TVolumeSpace oldSpace, TString& errStr);
    void ChangeFileStoreSpaceBegin(TFileStoreSpace newSpace, TFileStoreSpace oldSpace);
    void ChangeFileStoreSpaceCommit(TFileStoreSpace newSpace, TFileStoreSpace oldSpace);
    bool CheckFileStoreSpaceChange(TFileStoreSpace newSpace, TFileStoreSpace oldSpace, TString& errStr);
    void SetAsyncReplica();
    void SetRestoreTable();
    bool HasRuntimeAttrs() const;
    void SerializeRuntimeAttrs(google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TUserAttribute>* userAttrs) const;
};

}
