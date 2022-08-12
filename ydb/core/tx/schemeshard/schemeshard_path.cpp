#include "schemeshard_path.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>

#include <ydb/core/sys_view/common/path.h>

namespace NKikimr {
namespace NSchemeShard {

static constexpr ui64 MaxPQStorage = Max<ui64>() / 2;

TPath::TChecker::TChecker(const TPath &path)
    : Path(path)
    , Failed(false)
    , Status(EStatus::StatusSuccess)
{}

NKikimr::NSchemeShard::TPath::TChecker::operator bool() const {
    return !Failed;
}

TPath::TChecker::EStatus TPath::TChecker::GetStatus(TString *explain) const {
    Y_VERIFY(Failed);

    if (explain) {
        if (!explain->empty()) {
            explain->append(": ");
        }
        explain->append(Explain);
    }
    return Status;
}

const TPath::TChecker&  TPath::TChecker::IsResolved(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsResolved()) {
        return *this;
    }

    Failed = true;
    Status = status;
    TPath nearestParent = Path.FirstResoledParent();
    Explain << "path hasn't been resolved"
            << ", nearest resolved path is: " << nearestParent.PathString()
            << ", with pathId: " << (nearestParent.IsResolved() ? nearestParent.Base()->PathId : InvalidPathId);

    return *this;
}

const TPath::TChecker& TPath::TChecker::NotEmpty(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsEmpty()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is empty";
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotRoot(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.Base()->IsRoot()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is root";
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotResolved(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsResolved()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path has been resolved"
            << ", pathId: " << Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotUnderDeleting(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsUnderDeleting()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is being deleted right now"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState)
            << ", msg for compatibility KIKIMR-6499 Another drop in progress";
    return *this;
}

const TPath::TChecker &TPath::TChecker::NotUnderDomainUpgrade(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsUnderDomainUpgrade()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is being upgraded as part of subdomain right now"
            << ", domainId: " << Path.GetPathIdForDomain();
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsDeleted(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsDeleted()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path hasn't been deleted yet"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsUnderDeleting(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderDeleting()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path isn't under deletion right now"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsUnderMoving(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderMoving()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path isn't under moving right now"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}


const TPath::TChecker& TPath::TChecker::NotUnderOperation(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsUnderOperation()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is under operation"
            << ", pathId: " << Path.Base()->PathId
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsUnderCreating(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderCreating()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path isn't under creating right now"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsUnderOperation(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderOperation()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not under operation at all"
            << ", pathId: " << Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsUnderTheSameOperation(TTxId txId, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TTxId activeTxId = Path.ActiveOperation();
    if (activeTxId == txId) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not under the same operation"
            << ", pathId: " << Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState)
            << ", active txId: " << activeTxId
            << ", expected txId: " << txId;
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotUnderTheSameOperation(TTxId txId, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TTxId activeTxId = Path.ActiveOperation();
    if (activeTxId != txId) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is under the same operation"
            << ", pathId: " << Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState)
            << ", txId: " << txId;
    return *this;
}

const TPath::TChecker& TPath::TChecker::NoOlapStore(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.FindOlapStore()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "there is another olap store in the given path";

    return *this;
}

const TPath::TChecker& TPath::TChecker::HasOlapStore(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.FindOlapStore()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "no olap store found anywhere in the given path";

    return *this;
}

const TPath::TChecker& TPath::TChecker::IsOlapStore(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsOlapStore()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not an olap store"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsColumnTable(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsColumnTable()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not an olap table"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsSequence(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsSequence()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a sequence"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsReplication(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsReplication()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a replication"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsCommonSensePath(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsCommonSensePath()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a simple path which goes through directories"
            << ", it might be a table index or private index table"
            << ", pathId " << Path.Base()->PathId;

    return *this;
}

const TPath::TChecker& TPath::TChecker::IsInsideTableIndexPath(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsInsideTableIndexPath()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path doesn't go through directories towards table index"
            << ", it might be a table index or private index table"
            << ", pathId " << Path.Base()->PathId;
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsInsideCdcStreamPath(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsInsideCdcStreamPath()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path doesn't go through directories towards cdc stream"
            << ", it might be a cdc stream or private topic"
            << ", pathId " << Path.Base()->PathId;
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsTable(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsTable()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a table"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotBackupTable(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.Base()->IsTable()) {
        return *this;
    }

    if (!Path.IsBackupTable()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is a backup table, scheme operation is limited with it"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsBlockStoreVolume(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsBlockStoreVolume()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a block store volume"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsFileStore(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsFileStore()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a FileStore"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsKesus(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsKesus()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a kesus"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsPQGroup(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsPQGroup()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a pq group"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsSubDomain(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsSubDomainRoot()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a sub domain"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsExternalSubDomain(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsExternalSubDomainRoot()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not an external domain"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsAtLocalSchemeShard(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.AtLocalSchemeShardPath()) {
        return *this;
    }

    Failed = true;
    Status = status;
    TPath nearestParent = Path.FirstResoledParent();
    Explain << "path is an external domain, the redirection is needed"
            << ", external domain is: " << nearestParent.PathString()
            << ", with pathId: " << (nearestParent.IsResolved() ? nearestParent.Base()->PathId : InvalidPathId);

    return *this;
}

const TPath::TChecker& TPath::TChecker::IsSolomon(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsSolomon()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a solomon"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsTableIndex(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsTableIndex()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a table index"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsCdcStream(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsCdcStream()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a cdc stream"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsLikeDirectory(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsLikeDirectory()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a directory"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsDirectory(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsDirectory()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path is not a directory"
            << ", pathId: " <<  Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsTheSameDomain(const TPath &another, TPath::TChecker::EStatus status) const {

    if (Failed) {
        return *this;
    }

    if (Path.GetPathIdForDomain() == another.GetPathIdForDomain()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "only paths to single domain are allowed"
            << ", detected paths from different domains"
            << " for example one path: " << Path.PathString()
            << " another path: " << another.PathString();
    return *this;
}

const TPath::TChecker& TPath::TChecker::FailOnExist(TSet<TPathElement::EPathType> expectedTypes, bool acceptAlreadyExist) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsResolved()) {
        return *this;
    }

    if (Path.IsDeleted()) {
        return *this;
    }

    Failed = true;

    if (!expectedTypes.contains(Path.Base()->PathType)) {
        Status = EStatus::StatusNameConflict;
        Explain << "unexpected path type for path"
                << ", expected type: ";
        for (auto& type: expectedTypes) {
            Explain << NKikimrSchemeOp::EPathType_Name(type) << ", ";
        }
        Explain << "pathId: " << Path.Base()->PathId
                << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
                << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
        return *this;
    }

    if (!Path.Base()->IsCreateFinished()) {
        Status = EStatus::StatusMultipleModifications;
        Explain << "Muliple modifications: path exist but creating right now"
                << ", pathId: " << Path.Base()->PathId
                << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
                << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
        return *this;
    }

    if (acceptAlreadyExist) {
        Status = EStatus::StatusAlreadyExists;
        Explain << "path exist, request accepts it"
                << ", pathId: " << Path.Base()->PathId
                << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
                << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
        return *this;
    }

    Status = EStatus::StatusSchemeError;
    Explain << "path exist, request doesn't accept it"
            << ", pathId: " << Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::FailOnExist(TPathElement::EPathType expectedType, bool acceptAlreadyExist) const {
    return FailOnExist(TSet<TPathElement::EPathType>{expectedType}, acceptAlreadyExist);
}

const TPath::TChecker& TPath::TChecker::IsValidLeafName(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsValidLeafName(Explain)) {
        return *this;
    }

    Failed = true;
    Status = status;

    return *this;
}

const TPath::TChecker& TPath::TChecker::DepthLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    if (Path.Depth() + delta <= domainInfo->GetSchemeLimits().MaxDepth) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path depth has reached maximum value in the domain"
            << ", max path depth: " << domainInfo->GetSchemeLimits().MaxDepth
            << ", reached depth: " << Path.Depth()
            << ", aditional delta was: " << delta;
    return *this;
}

const TPath::TChecker& TPath::TChecker::PathsLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();

    if (!delta || domainInfo->GetPathsInside() + delta <= domainInfo->GetSchemeLimits().MaxPaths) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "paths count has reached maximum value in the domain"
            << ", paths limit for domain: " << domainInfo->GetSchemeLimits().MaxPaths
            << ", paths count inside domain: " << domainInfo->GetPathsInside()
            << ", intention to create new paths: " << delta;
    return *this;
}

const TPath::TChecker& TPath::TChecker::DirChildrenLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();

    auto parent = Path.Parent();
    ui64 aliveChildren = parent.Base()->GetAliveChildren();

    if (!delta || aliveChildren + delta <= domainInfo->GetSchemeLimits().MaxChildrenInDir) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "children count has reached maximum value in the dir"
            << ", children limit for domain dir: " << domainInfo->GetSchemeLimits().MaxChildrenInDir
            << ", children count inside dir: " << aliveChildren
            << ", intention to create new children: " << delta;
    return *this;
}

const TPath::TChecker& TPath::TChecker::ShardsLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();

    if (!delta || domainInfo->GetShardsInside() + delta <= domainInfo->GetSchemeLimits().MaxShards) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "shards count has reached maximum value in the domain"
            << ", shards limit for domain: " << domainInfo->GetSchemeLimits().MaxShards
            << ", shards count inside domain: " << domainInfo->GetShardsInside()
            << ", intention to create new shards: " << delta;
    return *this;
}

const TPath::TChecker& TPath::TChecker::PQPartitionsLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();

    if (!delta || domainInfo->GetPQPartitionsInside() + delta <= domainInfo->GetSchemeLimits().MaxPQPartitions && (!domainInfo->GetDatabaseQuotas()
        || !domainInfo->GetDatabaseQuotas()->data_stream_shards_quota()
        || domainInfo->GetPQPartitionsInside() + delta <= domainInfo->GetDatabaseQuotas()->data_stream_shards_quota())) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "data stream shards count has reached maximum value in the domain"
            << ", data stream shards limit for domain: " << (domainInfo->GetDatabaseQuotas() ? domainInfo->GetDatabaseQuotas()->data_stream_shards_quota() : 0) << "(" << domainInfo->GetSchemeLimits().MaxPQPartitions << ")"
            << ", data stream shards count inside domain: " << domainInfo->GetPQPartitionsInside()
            << ", intention to create new data stream shards: " << delta;
    return *this;
}

const TPath::TChecker& TPath::TChecker::PQReservedStorageLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();

    if (!delta || !domainInfo->GetDatabaseQuotas()
        || !domainInfo->GetDatabaseQuotas()->data_stream_reserved_storage_quota()
        || domainInfo->GetPQReservedStorage() + delta <= domainInfo->GetDatabaseQuotas()->data_stream_reserved_storage_quota()) {

        if (domainInfo->GetPQReservedStorage() + delta <= MaxPQStorage) {
            return *this;
        }
    }

    Failed = true;
    Status = status;
    Explain << "data stream reserved storage size has reached maximum value in the domain"
            << ", data stream reserved storage size limit for domain: "
            << (domainInfo->GetDatabaseQuotas() && domainInfo->GetDatabaseQuotas()->data_stream_reserved_storage_quota()
                             ? domainInfo->GetDatabaseQuotas()->data_stream_reserved_storage_quota()
                             : MaxPQStorage) << " bytes"
            << ", data stream reserved storage size inside domain: " << domainInfo->GetPQReservedStorage() << " bytes"
            << ", intention to reserve more storage for : " << delta << " bytes";
    return *this;
}



const TPath::TChecker& TPath::TChecker::PathShardsLimit(ui64 delta, TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    const ui64 shardInPath = Path.Shards();

    if (Path.IsResolved() && !Path.IsDeleted()) {
        Y_VERIFY_DEBUG_S(Path.SS->CollectAllShards({Path.Base()->PathId}).size() == shardInPath, "pedantic check:"
                         << " CollectAllShards " << Path.SS->CollectAllShards({Path.Base()->PathId}).size()
                         << " !="
                         << " Path.GetShardsInside " << shardInPath
                         << " for path " << Path.PathString());
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();

    if (!delta || shardInPath + delta <= domainInfo->GetSchemeLimits().MaxShardsInPath) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "shards count has reached maximum value in the path"
            << ", shards limit for path: " << domainInfo->GetSchemeLimits().MaxShardsInPath
            << ", shards count inside path: " << shardInPath
            << ", intention to create new shards: " << delta;
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotChildren(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    ui64 childrenCount = Path.Base()->GetAliveChildren();

    if (0 == childrenCount) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path has children, request doesn't accept it"
            << ", pathId: " << Path.Base()->PathId
            << ", path type: " << NKikimrSchemeOp::EPathType_Name(Path.Base()->PathType)
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState)
            << ", alive children: " << childrenCount;
    return *this;
}

const TPath::TChecker& TPath::TChecker::NotDeleted(TPath::TChecker::EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsDeleted()) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path has been deleted"
            << ", pathId: " <<  Path.Base()->PathId
            << ", deleted in stepId: " << Path.Base()->StepDropped
            << ", txId: " << Path.Base()->DropTxId
            << ", path state: " << NKikimrSchemeOp::EPathState_Name(Path.Base()->PathState);
    return *this;
}

const TPath::TChecker& TPath::TChecker::IsValidACL(const TString& acl, EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (acl.empty()) {
        return *this;
    }

    auto secObj = MakeHolder<NACLib::TACL>();
    if (Path.IsResolved()) {
        secObj.Reset(new NACLib::TACL(Path.Base()->ACL));
    }

    NACLib::TDiffACL diffACL(acl);
    secObj->ApplyDiff(diffACL);
    const ui64 bytesSize = secObj->SerializeAsString().size();

    if (bytesSize <= Path.DomainInfo()->GetSchemeLimits().MaxAclBytesSize) {
        return *this;
    }

    Failed = true;
    Status = status;
    Explain << "path's ACL is too long"
            << ", path: " <<  Path.PathString()
            << ", calculated ACL size: " << bytesSize
            << ". Limit: " << Path.DomainInfo()->GetSchemeLimits().MaxAclBytesSize;
    return *this;
}

TPath::TPath(TSchemeShard* ss)
    : SS(ss)
{
    Y_VERIFY(SS);
    Y_VERIFY(IsEmpty() && !IsResolved());
}

TPath::TPath(TVector<TPathElement::TPtr>&& elements, TSchemeShard* ss)
    : SS(ss)
{
    Y_VERIFY(SS);
    Y_VERIFY(elements);
    Elements = std::move(elements);
    for (auto& item: Elements) {
        NameParts.push_back(item->Name);
    }
    Y_VERIFY(!IsEmpty());
    Y_VERIFY(IsResolved());
}

TPath::TChecker TPath::Check() const {
    return TChecker(*this);
}

bool TPath::IsEmpty() const {
    return NameParts.empty();
}

bool TPath::IsResolved() const {
    return !IsEmpty() && NameParts.size() == Elements.size();
}

NKikimr::NSchemeShard::TPath::operator bool() const {
    return IsResolved();
}

bool TPath::operator ==(const TPath& another) const { // likely O(1) complexity, but might be O(path length)
    if (Y_LIKELY(IsResolved() && another.IsResolved())) {
        return Base()->PathId == another.Base()->PathId; // check pathids is the only right way to compare
    }                                                    // PathStrings could be false equeal due operation Init form deleted path element
    if (IsResolved() || another.IsResolved()) {
        return false;
    }
    //both are not resolved

    if (IsEmpty() && another.IsEmpty()) {
        return true;
    }

    if (IsEmpty() || another.IsEmpty()) {
        return false;
    }
    //both are not empty

    if (Depth() != another.Depth()) {
        return false;
    }

    // we could have checked parents and unresolved tails
    // but better let's keep it simple
    return PathString() == another.PathString();
}

bool TPath::operator !=(const TPath& another) const {
    return !(*this == another);
}

TPath TPath::Root(TSchemeShard* ss) {
    Y_VERIFY(ss);

    auto result = TPath::Init(ss->RootPathId(), ss);
    return result;
}

TString TPath::PathString() const { //O(1) if resolved, in other case O(result length) complexity
    if (!NameParts) {
        return TString();
    }

    TStringBuilder result;

    for (auto& part: NameParts) {
        result <<  '/' << part;
    }

    return result;
}

TPath& TPath::Rise() {
    if (!NameParts) {
        Y_VERIFY(!Elements);
        return *this;
    }

    if (Elements.size() == NameParts.size()) {
        if (Base()->IsRoot()) {
            return *this;
        }

        Elements.pop_back();
    }
    NameParts.pop_back();
    return *this;
}

TPath TPath::Parent() const {
    TPath result = *this;
    result.Rise();
    return result;
}

TPath& TPath::RiseUntilFirstResolvedParent() {
    Rise();
    while (!IsEmpty() && !IsResolved()) {
        Rise();
    }
    return *this;
}

TPath TPath::FirstResoledParent() const {
    TPath result = *this;
    result.RiseUntilFirstResolvedParent();
    return result;
}

TPath& TPath::RiseUntilExisted() {
    if (!IsResolved()) {
        RiseUntilFirstResolvedParent();
    }
    while (!IsEmpty() && IsDeleted()) {
        Rise();
    }
    return *this;
}

TPath TPath::FirstExistedParent() const {
    TPath result = *this;
    result.RiseUntilExisted();
    return result;
}

TString TPath::GetDomainPathString() const {
    // TODO: not effective because of creating vectors in Init() method. should keep subdomain path somethere in struct TSubDomainInfo
    return Init(GetPathIdForDomain(), SS).PathString();
}

TSubDomainInfo::TPtr TPath::DomainInfo() const {
    Y_VERIFY(!IsEmpty());
    Y_VERIFY(Elements.size());

    return SS->ResolveDomainInfo(Elements.back());
}

TPathId TPath::GetPathIdForDomain() const {
    Y_VERIFY(!IsEmpty());
    Y_VERIFY(Elements.size());

    return SS->ResolvePathIdForDomain(Elements.back());
}

TPathId TPath::GetDomainKey() const {
    Y_VERIFY(IsResolved());

    return SS->GetDomainKey(Elements.back());
}

bool TPath::IsDomain() const {
    Y_VERIFY(IsResolved());

    return Base()->IsDomainRoot();
}

TPath& TPath::Dive(const TString& name) {
    if (!SS->IsShemeShardConfigured()) {
        NameParts.push_back(name);
        return *this;
    }

    if (Elements.empty() && NameParts.size() < SS->RootPathElements.size()) {
        NameParts.push_back(name);

        if (NameParts.size() == SS->RootPathElements.size()
                && NameParts == SS->RootPathElements)
        {
            Elements = {SS->PathsById.at(SS->RootPathId())};
            NameParts = {Elements.front()->Name};
        }

        return *this;
    }

    if (Elements.size() != NameParts.size()) {
        NameParts.push_back(name);
        return *this;
    }

    NameParts.push_back(name);
    TPathElement::TPtr last = Elements.back();

    TPathId* childId = last->FindChild(name);

    if (nullptr == childId) {
        return *this;
    }

    Elements.push_back(SS->PathsById.at(*childId));
    return *this;
}


TPath TPath::Child(const TString& name) const {
    TPath result = *this;
    result.Dive(name);
    return result;
}

TPath TPath::Resolve(const TString path, TSchemeShard* ss) {
    Y_VERIFY(ss);

    TPath nullPrefix{ss};
    return Resolve(nullPrefix, SplitPath(path));
}

TPath TPath::Resolve(const TPath& prefix, TVector<TString>&& pathParts) {
    TPath result = prefix;

    if (pathParts.empty()) {
        return result;
    }

    for (auto& part: pathParts) {
        result.Dive(part);
    }

    return result;
}

TPath TPath::ResolveWithInactive(TOperationId opId, const TString path, TSchemeShard* ss) {
    TPath nullPrefix{ss};
    auto pathParts = SplitPath(path);

    int headSubTxId = opId.GetSubTxId() - 1;
    while (headSubTxId >= 0) {
        auto headOpId = TOperationId(opId.GetTxId(), headSubTxId);
        TTxState* txState = ss->FindTx(headOpId);
        if (!txState) {
            break;
        }

        TPath headOpPath = Init(txState->TargetPathId, ss);

        auto headPathNameParts = ss->RootPathElements;
        headPathNameParts.insert(headPathNameParts.end(), std::next(headOpPath.NameParts.begin()), headOpPath.NameParts.end());

        if (headPathNameParts.size() + 1 == pathParts.size()
                && std::equal(headPathNameParts.begin(), headPathNameParts.end(),
                              pathParts.begin()))
        {
            // headOpPath is a prefix of the path
            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                         "ResolveWithInactive: attach to the TargetPath of head operation"
                         << " path: " << path
                         << " opId: " << opId
                         << " head opId: " << headOpId
                         << " headOpPath: " << headOpPath.PathString()
                         << " headOpPath id: " << headOpPath->PathId);

            return headOpPath.Child(pathParts.back());
        }

        --headSubTxId;
    }

    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::FLAT_TX_SCHEMESHARD,
                 "ResolveWithInactive: NO attach to the TargetPath of head operation"
                 << " path: " << path
                 << " opId: " << opId);

    return Resolve(nullPrefix, std::move(pathParts));
}


TPath TPath::Init(const TPathId pathId, TSchemeShard* ss) {
    Y_VERIFY(ss);

    if (!ss->PathsById.contains(pathId)) {
        return TPath(ss);
    }

    TVector<TPathElement::TPtr> parts;
    TPathElement::TPtr cur = ss->PathsById.at(pathId);

    while (!cur->IsRoot()) {
        parts.push_back(cur);
        Y_VERIFY(ss->PathsById.contains(cur->ParentPathId));
        cur = ss->PathsById.at(cur->ParentPathId);
    }

    parts.push_back(cur); //add root
    std::reverse(parts.begin(), parts.end());

    return TPath(std::move(parts), ss);
}

TPathElement::TPtr TPath::Base() const {
    Y_VERIFY_S(IsResolved(), "not resolved path " << PathString()
               << " NameParts: " << NameParts.size()
               << " Elements: " << Elements.size());

    return Elements.back();
}

TPathElement* TPath::operator->() const {
    Y_VERIFY_S(IsResolved(), "not resolved path " << PathString());

    return Elements.back().Get();
}

bool TPath::IsDeleted() const {
    Y_VERIFY(IsResolved());

    return Base()->Dropped();
}

bool TPath::IsUnderOperation() const {
    Y_VERIFY(IsResolved());

    if (Base()->Dropped()) {
        return false;
    }

    bool result = Base()->PathState != NKikimrSchemeOp::EPathState::EPathStateNoChanges;
    if (result) {
        ui32 summ = (ui32)IsUnderCreating()
            + (ui32)IsUnderAltering()
            + (ui32)IsUnderCopying()
            + (ui32)IsUnderBackuping()
            + (ui32)IsUnderRestoring()
            + (ui32)IsUnderDeleting()
            + (ui32)IsUnderDomainUpgrade()
            + (ui32)IsUnderMoving();
        Y_VERIFY_S(summ == 1,
                   "only one operation at the time"
                       << " pathId: " << Base()->PathId
                       << " path state: " << NKikimrSchemeOp::EPathState_Name(Base()->PathState)
                       << " path: " << PathString()
                       << " sum is: " << summ);
    }
    return result;
}

TTxId TPath::ActiveOperation() const {
    Y_VERIFY(IsResolved());

    if (!IsUnderOperation()) {
        return InvalidTxId;
    }

    TTxId txId = InvalidTxId;
    if (IsUnderCreating()) {
        txId = Base()->CreateTxId;
    } else if (IsUnderDeleting()) {
        txId = Base()->DropTxId;
    } else {
        txId = Base()->LastTxId;
    }

    Y_VERIFY(txId != InvalidTxId);
    Y_VERIFY_S(SS->Operations.contains(txId),
               "no operation,"
                   << " txId: " << txId
                   << " pathId: " << Base()->PathId
                   << " path state: " << NKikimrSchemeOp::EPathState_Name(Base()->PathState)
                   << " path " << PathString());

    return txId;
}

bool TPath::IsUnderCreating() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateCreate;
}

bool TPath::IsUnderAltering() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateAlter;
}

bool TPath::IsUnderDomainUpgrade() const {
    if (Elements.empty()) {
        return false;
    }

    return SS->PathsById.at(GetPathIdForDomain())->PathState == NKikimrSchemeOp::EPathState::EPathStateUpgrade;
}

bool TPath::IsUnderCopying() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateCopying;
}

bool TPath::IsUnderBackuping() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateBackup;
}

bool TPath::IsUnderRestoring() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateRestore;
}

bool TPath::IsUnderDeleting() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateDrop;
}

bool TPath::IsUnderMoving() const {
    Y_VERIFY(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateMoving;
}

TPath& TPath::RiseUntilOlapStore() {
    size_t end = Elements.size();
    while (end > 0) {
        auto& current = Elements[end-1];
        if (current->IsOlapStore()) {
            break;
        }
        --end;
    }
    Elements.resize(end);
    NameParts.resize(end);
    return *this;
}

TPath TPath::FindOlapStore() const {
    TPath result = *this;
    result.RiseUntilOlapStore();
    return result;
}

bool TPath::IsCommonSensePath() const {
    Y_VERIFY(IsResolved());

    auto item = ++Elements.rbegin(); //do not check the Base
    for (; item != Elements.rend(); ++item) {
        // Directories and domain roots are always ok as intermediaries
        bool ok = (*item)->IsDirectory() || (*item)->IsDomainRoot();
        // Temporarily olap stores are treated like directories
        ok = ok || (*item)->IsOlapStore();
        if (!ok) {
            return false;
        }
    }

    return true;
}

bool TPath::AtLocalSchemeShardPath() const {
    if (Elements.empty()) {
        return true;
    }

    auto it = Elements.rbegin();

    if ((*it)->IsExternalSubDomainRoot() && !(*it)->Dropped()) {
        return IsResolved();
    }

    return !(*it)->IsMigrated();
}

bool TPath::IsInsideTableIndexPath() const {
    Y_VERIFY(IsResolved());

    // expected /<root>/.../<table>/<table_index>/<private_tables>
    if (Depth() < 3) {
        return false;
    }

    auto item = Elements.rbegin();

    //skip private_table
    if ((*item)->IsTable()) {
        ++item;
    }

    if (!(*item)->IsTableIndex()) {
        return false;
    }

    ++item;
    if (!(*item)->IsTable()) {
        return false;
    }

    ++item;
    for (; item != Elements.rend(); ++item) {
        if (!(*item)->IsDirectory() && !(*item)->IsSubDomainRoot()) {
            return false;
        }
    }

    return true;
}

bool TPath::IsInsideCdcStreamPath() const {
    Y_VERIFY(IsResolved());

    // expected /<root>/.../<table>/<cdc_stream>/<private_topic>
    if (Depth() < 3) {
        return false;
    }

    auto item = Elements.rbegin();

    //skip private_topic
    if ((*item)->IsPQGroup()) {
        ++item;
    }

    if (!(*item)->IsCdcStream()) {
        return false;
    }

    ++item;
    if (!(*item)->IsTable()) {
        return false;
    }

    ++item;
    for (; item != Elements.rend(); ++item) {
        if (!(*item)->IsDirectory() && !(*item)->IsSubDomainRoot()) {
            return false;
        }
    }

    return true;
}

bool TPath::IsTableIndex() const {
    Y_VERIFY(IsResolved());

    return Base()->IsTableIndex();
}

bool TPath::IsBackupTable() const {
    Y_VERIFY(IsResolved());

    if (!Base()->IsTable() || !SS->Tables.contains(Base()->PathId)) {
        return false;
    }

    TTableInfo::TCPtr tableInfo = SS->Tables.at(Base()->PathId);

    return tableInfo->IsBackup;
}

bool TPath::IsCdcStream() const {
    Y_VERIFY(IsResolved());

    return Base()->IsCdcStream();
}

bool TPath::IsSequence() const {
    Y_VERIFY(IsResolved());

    return Base()->IsSequence();
}

bool TPath::IsReplication() const {
    Y_VERIFY(IsResolved());

    return Base()->IsReplication();
}

ui32 TPath::Depth() const {
    return NameParts.size();
}

ui64 TPath::Shards() const {
    if (IsEmpty() || !IsResolved() || IsDeleted()) {
        return 0;
    }

    return Base()->GetShardsInside();
}

const TString &TPath::LeafName() const {
    Y_VERIFY(!IsEmpty());
    return NameParts.back();
}

bool TPath::IsValidLeafName(TString& explain) const {
    Y_VERIFY(!IsEmpty());

    const auto& leaf = NameParts.back();
    if (leaf.empty()) {
        explain += "path part shouldn't be empty";
        return false;
    }

    const auto& schemeLimits = DomainInfo()->GetSchemeLimits();

    if (leaf.size() > schemeLimits.MaxPathElementLength) {
        explain += "path part is too long";
        return false;
    }

    if (!SS->IsShemeShardConfigured()) {
        explain += "cluster don't have inited root jet";
        return false;
    }

    if (AppData()->FeatureFlags.GetEnableSystemViews() && leaf == NSysView::SysPathName) {
        explain += TStringBuilder()
            << "path part '" << NSysView::SysPathName << "' is reserved by the system";
        return false;
    }

    if (IsPathPartContainsOnlyDots(leaf)) {
        explain += TStringBuilder()
            << "is not allowed path part contains only dots '" << leaf << "'";
        return false;
    }

    auto brokenAt = PathPartBrokenAt(leaf, schemeLimits.ExtraPathSymbolsAllowed);
    if (brokenAt != leaf.end()) {
        explain += TStringBuilder()
            << "symbol '" << *brokenAt << "'"
            << " is not allowed in the path part '" << leaf << "'";
        return false;
    }

    return true;
}

TString TPath::GetEffectiveACL() const {
    Y_VERIFY(IsResolved());

    ui64 version = 0;

    if (!SS->IsDomainSchemeShard) {
        version += SS->ParentDomainEffectiveACLVersion;
    }

    //actualize CachedEffectiveACL in each element if needed
    for (auto elementIt = Elements.begin(); elementIt != Elements.end(); ++elementIt) {
        TPathElement::TPtr element = *elementIt;
        version += element->ACLVersion;

        if (element->CachedEffectiveACLVersion != version || !element->CachedEffectiveACL) {  //path needs actualizing
            if (elementIt == Elements.begin()) { // it is root
                if (!SS->IsDomainSchemeShard) {
                    element->CachedEffectiveACL.Update(SS->ParentDomainCachedEffectiveACL, element->ACL, element->IsContainer());
                } else {
                    element->CachedEffectiveACL.Init(element->ACL);
                }
            } else { // path element in the middle
                auto prevIt = std::prev(elementIt);
                const auto& prevElement = *prevIt;
                element->CachedEffectiveACL.Update(prevElement->CachedEffectiveACL, element->ACL, element->IsContainer());
            }
            element->CachedEffectiveACLVersion = version;
        }
    }

    return Elements.back()->CachedEffectiveACL.GetForSelf();
}

ui64 TPath::GetEffectiveACLVersion() const {
    Y_VERIFY(IsResolved());

    ui64 version = 0;

    if (!SS->IsDomainSchemeShard) {
        version += SS->ParentDomainEffectiveACLVersion;
    }

    for (auto elementIt = Elements.begin(); elementIt != Elements.end(); ++elementIt) {
        version += (*elementIt)->ACLVersion;
    }

    return version;
}

TTxId TPath::LockedBy() const {
    auto it = SS->LockedPaths.find(Base()->PathId);
    if (it != SS->LockedPaths.end()) {
        return it->second;
    }

    return InvalidTxId;
}

bool TPath::IsActive() const {
    return SS->PathIsActive(Base()->PathId);
}

void TPath::Activate() {
    if (IsActive()) {
        return;
    }

    Y_VERIFY(Base()->IsCreateFinished());

    auto result = SS->AttachChild(Base());
    Y_VERIFY_S(result == EAttachChildResult::AttachedAsNewerActual, "result is: " << result);
}

void TPath::MaterializeLeaf(const TString& owner) {
    return MaterializeLeaf(owner, SS->AllocatePathId(), /*allowInactivePath*/ false);
}

void TPath::MaterializeLeaf(const TString &owner, const TPathId &newPathId, bool allowInactivePath) {
    auto result = MaterializeImpl(owner, newPathId);
    switch (result) {
    case EAttachChildResult::Undefined:
        Y_FAIL("unexpected result: Undefined");
        break;

    case EAttachChildResult::AttachedAsOnlyOne:
    case EAttachChildResult::AttachedAsActual:
        Y_VERIFY(SS->PathIsActive(newPathId));
        break;

    case EAttachChildResult::AttachedAsCreatedActual:
    case EAttachChildResult::AttachedAsOlderUnCreated:
    case EAttachChildResult::AttachedAsNewerDeleted:
    case EAttachChildResult::AttachedAsNewerActual:
        Y_FAIL_S("strange result for materialization: " << result);
        break;

    case EAttachChildResult::RejectAsInactve:
        if (allowInactivePath) {
            Y_VERIFY(!SS->PathIsActive(newPathId));
        } else {
            Y_FAIL_S("MaterializeLeaf do not accept shadow pathes, use allowInactivePath = true");
        }
        break;

    case EAttachChildResult::RejectAsOlderDeleted:
    case EAttachChildResult::RejectAsDeleted:
    case EAttachChildResult::RejectAsOlderActual:
    case EAttachChildResult::RejectAsNewerUnCreated:
        Y_FAIL_S("MaterializeLeaf do not accept rejection: " << result);
        break;
    };
}

EAttachChildResult TPath::MaterializeImpl(const TString& owner, const TPathId& newPathId) {
    const TString leafName = NameParts.back();
    Rise();
    Y_VERIFY(IsResolved());

    TPathId domainId = Base()->IsDomainRoot() ? Base()->PathId : Base()->DomainPathId;
    TPathElement::TPtr newPath = new TPathElement(newPathId, Base()->PathId, domainId, leafName, owner);

    auto attachResult = SS->AttachChild(newPath);

    Base()->DbRefCount++;
    Base()->AllChildrenCount++;

    Y_VERIFY_S(!SS->PathsById.contains(newPathId), "There's another path with PathId: " << newPathId);
    SS->PathsById[newPathId] = newPath;

    DiveByPathId(newPathId);
    Y_VERIFY_S(IsResolved(),
               "not resolved,"
                   << " path: " << PathString()
                   << ", Elements size: " << Elements.size()
                   << ", NameParts size: " << NameParts.size());

    return attachResult;
}

TPath& TPath::DiveByPathId(const TPathId& pathId) {
    Y_VERIFY(IsResolved());

    TPathElement::TPtr nextElem = SS->PathsById.at(pathId);
    Y_VERIFY(nextElem->ParentPathId == Elements.back()->PathId);

    Elements.push_back(nextElem);
    NameParts.push_back(nextElem->Name);

    return *this;
}


}}
