#pragma once

#include "schemeshard_path_element.h"
#include "schemeshard_info_types.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <util/generic/maybe.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TPath {
    TSchemeShard* SS;
    TVector<TString> NameParts;
    TVector<TPathElement::TPtr> Elements;

public:
    class TChecker {
        using EStatus = NKikimrScheme::EStatus;

        const TPath& Path;
        mutable bool Failed;
        mutable EStatus Status;
        mutable TString Error;

    private:
        TString BasicPathInfo(TPathElement::TPtr element) const;
        const TChecker& Fail(EStatus status, const TString& error) const;

    public:
        explicit TChecker(const TPath& path);

        explicit operator bool() const;
        EStatus GetStatus() const;
        const TString& GetError() const;

        const TChecker& IsResolved(EStatus status = EStatus::StatusPathDoesNotExist) const;
        const TChecker& HasResolvedPrefix(EStatus status = EStatus::StatusSchemeError) const;
        const TChecker& NotEmpty(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& NotRoot(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& NotResolved(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& NotDeleted(EStatus status = EStatus::StatusPathDoesNotExist) const;
        const TChecker& NotUnderDeleting(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& NotUnderDomainUpgrade(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& IsDeleted(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& IsUnderDeleting(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& IsUnderMoving(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& NotUnderOperation(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& IsUnderCreating(EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& IsUnderOperation(EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& IsUnderTheSameOperation(TTxId txId, EStatus status = EStatus::StatusMultipleModifications) const;
        const TChecker& NotUnderTheSameOperation(TTxId txId, EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& NoOlapStore(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& HasOlapStore(EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& IsOlapStore(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsColumnTable(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsSequence(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsReplication(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsCommonSensePath(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsInsideTableIndexPath(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsInsideCdcStreamPath(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsTable(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& NotBackupTable(EStatus status = EStatus::StatusSchemeError) const;
        const TChecker& NotAsyncReplicaTable(EStatus status = EStatus::StatusSchemeError) const;
        const TChecker& IsBlockStoreVolume(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsFileStore(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsKesus(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsPQGroup(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsSubDomain(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsExternalSubDomain(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsAtLocalSchemeShard(EStatus status = EStatus::StatusRedirectDomain) const;
        const TChecker& IsSolomon(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsTableIndex(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsCdcStream(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsLikeDirectory(EStatus status = EStatus::StatusPathIsNotDirectory) const;
        const TChecker& IsDirectory(EStatus status = EStatus::StatusPathIsNotDirectory) const;
        const TChecker& IsTheSameDomain(const TPath& another, EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& FailOnWrongType(const TSet<TPathElement::EPathType>& expectedTypes) const;
        const TChecker& FailOnWrongType(TPathElement::EPathType expectedType) const;
        const TChecker& FailOnExist(const TSet<TPathElement::EPathType>& expectedTypes, bool acceptAlreadyExist) const;
        const TChecker& FailOnExist(TPathElement::EPathType expectedType, bool acceptAlreadyExist) const;
        const TChecker& IsValidLeafName(EStatus status = EStatus::StatusSchemeError) const;
        const TChecker& DepthLimit(ui64 delta = 0, EStatus status = EStatus::StatusSchemeError) const;
        const TChecker& PathsLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& DirChildrenLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& ShardsLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& PathShardsLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& NotChildren(EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& CanBackupTable(EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& IsValidACL(const TString& acl, EStatus status = EStatus::StatusInvalidParameter) const;
        const TChecker& PQPartitionsLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& PQReservedStorageLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& ExportsLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& ImportsLimit(ui64 delta = 1, EStatus status = EStatus::StatusResourceExhausted) const;
        const TChecker& IsExternalTable(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsExternalDataSource(EStatus status = EStatus::StatusNameConflict) const;
        // Check there are no uncles or cousins with same name
        const TChecker& IsNameUniqGrandParentLevel(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& IsView(EStatus status = EStatus::StatusNameConflict) const;
        const TChecker& FailOnRestrictedCreateInTempZone(bool allowCreateInTemporaryDir = false, EStatus status = EStatus::StatusPreconditionFailed) const;
        const TChecker& IsResourcePool(EStatus status = EStatus::StatusNameConflict) const;
    };

public:
    explicit TPath(TSchemeShard* ss);
    TPath(TVector<TPathElement::TPtr>&& elements, TSchemeShard* ss);

    TPath(const TPath& path) = default;
    TPath(TPath&& path) = default;

    TPath& operator = (TPath&& another) = default;

    bool operator ==(const TPath& another) const;
    bool operator !=(const TPath& another) const;

    explicit operator bool() const;

    static TPath Resolve(const TString path, TSchemeShard* ss);
    static TPath Resolve(const TPath& prefix, TVector<TString>&& pathParts);
    static TPath ResolveWithInactive(TOperationId opId, const TString path, TSchemeShard* ss);

    static TPath Init(const TPathId pathId, TSchemeShard* ss);

    TChecker Check() const;
    bool IsEmpty() const;
    bool IsResolved() const;

    static TPath Root(TSchemeShard* ss);
    TString PathString() const;
    TPath& Rise();
    TPath Parent() const;
    TPath& RiseUntilFirstResolvedParent();
    TPath FirstResoledParent() const;
    TPath& RiseUntilExisted();
    TPath FirstExistedParent() const;
    TString GetDomainPathString() const;
    TSubDomainInfo::TPtr DomainInfo() const;
    TPathId GetPathIdForDomain() const;
    TPathId GetDomainKey() const;
    bool IsDomain() const;
    TPath& Dive(const TString& name);
    TPath Child(const TString& name) const;
    TPathElement::TPtr Base() const;
    TPathElement* operator->() const;
    bool IsDeleted() const;
    bool IsUnderOperation() const;
    TTxId ActiveOperation() const;
    bool IsUnderCreating() const;
    bool IsUnderAltering() const;
    bool IsUnderDomainUpgrade() const;
    bool IsUnderCopying() const;
    bool IsUnderBackingUp() const;
    bool IsUnderRestoring() const;
    bool IsUnderDeleting() const;
    bool IsUnderMoving() const;
    TPath& RiseUntilOlapStore();
    TPath FindOlapStore() const;
    bool IsCommonSensePath() const;
    bool AtLocalSchemeShardPath() const;
    bool IsInsideTableIndexPath() const;
    bool IsInsideCdcStreamPath() const;
    bool IsTableIndex(const TMaybe<NKikimrSchemeOp::EIndexType>& type = {}) const;
    bool IsBackupTable() const;
    bool IsAsyncReplicaTable() const;
    bool IsCdcStream() const;
    bool IsSequence() const;
    bool IsReplication() const;
    ui32 Depth() const;
    ui64 Shards() const;
    const TString& LeafName() const;
    bool IsValidLeafName(TString& explain) const;
    TString GetEffectiveACL() const;
    ui64 GetEffectiveACLVersion() const;
    TTxId LockedBy() const;

    bool IsActive() const;
    void Activate();

    void MaterializeLeaf(const TString& owner);
    void MaterializeLeaf(const TString& owner, const TPathId& newPathId, bool allowInactivePath = false);

private:
    EAttachChildResult MaterializeImpl(const TString& owner, const TPathId& newPathId);
    TPath& DiveByPathId(const TPathId& pathId);
    TPathId GetPathIdSafe() const;
};

}
