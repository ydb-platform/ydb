#include "schemeshard_path.h"
#include "schemeshard_impl.h"

#include <ydb/core/base/path.h>
#include <ydb/core/sys_view/common/path.h>

#include <util/string/join.h>

namespace NKikimr::NSchemeShard {

TPath::TChecker::TChecker(const TPath& path)
    : Path(path)
    , Failed(false)
    , Status(EStatus::StatusSuccess)
{
}

TPath::TChecker::operator bool() const {
    return !Failed;
}

TPath::TChecker::EStatus TPath::TChecker::GetStatus() const {
    return Status;
}

const TString& TPath::TChecker::GetError() const {
    return Error;
}

const TPath::TChecker& TPath::TChecker::Fail(EStatus status, const TString& error) const {
    Failed = true;
    Status = status;
    Error = TStringBuilder() << "Check failed"
        << ": path: '" << Path.PathString() << "'"
        << ", error: " << error;

    return *this;
}

const TPath::TChecker& TPath::TChecker::IsResolved(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsResolved()) {
        return *this;
    }

    const auto nearest = Path.FirstResoledParent();
    return Fail(status, TStringBuilder() << "path hasn't been resolved, nearest resolved path"
        << ": '" << nearest.PathString() << "' (id: " << nearest.GetPathIdSafe() << ")");
}

const TPath::TChecker& TPath::TChecker::HasResolvedPrefix(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.Elements.empty()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "root not found");
}

const TPath::TChecker& TPath::TChecker::NotEmpty(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsEmpty()) {
        return *this;
    }

    return Fail(status, "path is empty");
}

const TPath::TChecker& TPath::TChecker::NotRoot(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.Base()->IsRoot()) {
        return *this;
    }

    return Fail(status, "path is root");
}

const TPath::TChecker& TPath::TChecker::NotResolved(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsResolved()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path has been resolved"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::NotUnderDeleting(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsUnderDeleting()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is being deleted right now"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::NotUnderDomainUpgrade(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsUnderDomainUpgrade()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is being upgraded as part of subdomain right now"
        << " (domain id: " << Path.GetPathIdForDomain() << ")");
}

const TPath::TChecker& TPath::TChecker::IsDeleted(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsDeleted()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path hasn't been deleted yet"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsUnderDeleting(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderDeleting()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path isn't under deletion right now"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsUnderMoving(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderMoving()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path isn't under moving right now"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::NotUnderOperation(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsUnderOperation()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is under operation"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsUnderCreating(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderCreating()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path isn't under creating right now"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsUnderOperation(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsUnderOperation()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not under operation at all"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsUnderTheSameOperation(TTxId txId, EStatus status) const {
    if (Failed) {
        return *this;
    }

    const auto activeTxId = Path.ActiveOperation();
    if (activeTxId == txId) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not under the same operation"
        << ", active txId: " << activeTxId
        << ", expected txId: " << txId);
}

const TPath::TChecker& TPath::TChecker::NotUnderTheSameOperation(TTxId txId, EStatus status) const {
    if (Failed) {
        return *this;
    }

    const auto activeTxId = Path.ActiveOperation();
    if (activeTxId != txId) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is under the same operation"
        << ", txId: " << txId);
}

const TPath::TChecker& TPath::TChecker::NoOlapStore(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.FindOlapStore()) {
        return *this;
    }

    return Fail(status, "there is another olap store in the given path");
}

const TPath::TChecker& TPath::TChecker::HasOlapStore(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.FindOlapStore()) {
        return *this;
    }

    return Fail(status, "no olap store found anywhere in the given path");
}

const TPath::TChecker& TPath::TChecker::IsOlapStore(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsOlapStore()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not an olap store"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsColumnTable(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsColumnTable()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not an olap table"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsSequence(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsSequence()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a sequence"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsReplication(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsReplication()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a replication"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsCommonSensePath(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsCommonSensePath()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a common path"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsInsideTableIndexPath(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsInsideTableIndexPath()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path doesn't go through directories towards table index"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsInsideCdcStreamPath(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.IsInsideCdcStreamPath()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path doesn't go through directories towards cdc stream"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsTable(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsTable()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a table"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::NotBackupTable(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.Base()->IsTable()) {
        return *this;
    }

    if (!Path.IsBackupTable()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is a backup table"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::NotAsyncReplicaTable(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.Base()->IsTable()) {
        return *this;
    }

    if (!Path.IsAsyncReplicaTable()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is an async replica table"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsBlockStoreVolume(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsBlockStoreVolume()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a block store volume"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsFileStore(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsFileStore()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a FileStore"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsKesus(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsKesus()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a kesus"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsPQGroup(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsPQGroup()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a topic"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsSubDomain(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsSubDomainRoot()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a subdomain"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsExternalSubDomain(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsExternalSubDomainRoot()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not an external subdomain"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsAtLocalSchemeShard(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.AtLocalSchemeShardPath()) {
        return *this;
    }

    const auto nearest = Path.FirstResoledParent();
    return Fail(status, TStringBuilder() << "path is an external domain, the redirection is needed"
        << ": '" << nearest.PathString() << "' (id: " << nearest.GetPathIdSafe() << ")");
}

const TPath::TChecker& TPath::TChecker::IsSolomon(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsSolomon()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a solomon"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsTableIndex(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsTableIndex()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a table index"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsCdcStream(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsCdcStream()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a cdc stream"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsLikeDirectory(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsLikeDirectory()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a directory"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsDirectory(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsDirectory()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a directory"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsTheSameDomain(const TPath& another, EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.GetPathIdForDomain() == another.GetPathIdForDomain()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "only paths to a single subdomain are allowed"
        << ", another path: " << another.PathString());
}

const TPath::TChecker& TPath::TChecker::FailOnWrongType(const TSet<TPathElement::EPathType>& expectedTypes) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsResolved()) {
        return *this;
    }

    if (Path.IsDeleted()) {
        return *this;
    }

    if (!expectedTypes.contains(Path.Base()->PathType)) {
        return Fail(EStatus::StatusNameConflict, TStringBuilder() << "unexpected path type"
            << " (" << BasicPathInfo(Path.Base()) << ")"
            << ", expected types: " << JoinSeq(", ", expectedTypes));
    }

    if (!Path.Base()->IsCreateFinished()) {
        return Fail(EStatus::StatusMultipleModifications, TStringBuilder() << "path exists but creating right now"
            << " (" << BasicPathInfo(Path.Base()) << ")");
    }

    return *this;
}

const TPath::TChecker& TPath::TChecker::FailOnWrongType(TPathElement::EPathType expectedType) const {
    return FailOnWrongType(TSet<TPathElement::EPathType>{expectedType});
}

const TPath::TChecker& TPath::TChecker::FailOnExist(const TSet<TPathElement::EPathType>& expectedTypes, bool acceptAlreadyExist) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsResolved()) {
        return *this;
    }

    if (Path.IsDeleted()) {
        return *this;
    }

    if (!expectedTypes.contains(Path.Base()->PathType)) {
        return Fail(EStatus::StatusNameConflict, TStringBuilder() << "unexpected path type"
            << " (" << BasicPathInfo(Path.Base()) << ")"
            << ", expected types: " << JoinSeq(", ", expectedTypes));
    }

    if (!Path.Base()->IsCreateFinished()) {
        return Fail(EStatus::StatusMultipleModifications, TStringBuilder() << "path exists but creating right now"
            << " (" << BasicPathInfo(Path.Base()) << ")");
    }

    if (acceptAlreadyExist) {
        return Fail(EStatus::StatusAlreadyExists, TStringBuilder() << "path exist, request accepts it"
            << " (" << BasicPathInfo(Path.Base()) << ")");
    }

    return Fail(EStatus::StatusSchemeError, TStringBuilder() << "path exist, request doesn't accept it"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::FailOnExist(TPathElement::EPathType expectedType, bool acceptAlreadyExist) const {
    return FailOnExist(TSet<TPathElement::EPathType>{expectedType}, acceptAlreadyExist);
}

const TPath::TChecker& TPath::TChecker::IsValidLeafName(EStatus status) const {
    if (Failed) {
        return *this;
    }

    TString error;
    if (Path.IsValidLeafName(error)) {
        return *this;
    }

    return Fail(status, error);
}

const TPath::TChecker& TPath::TChecker::DepthLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    if (Path.Depth() + delta <= domainInfo->GetSchemeLimits().MaxDepth) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "paths depth limit exceeded"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxDepth
        << ", depth: " << Path.Depth()
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::PathsLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    const auto pathsTotal = domainInfo->GetPathsInside();
    const auto backupPaths = domainInfo->GetBackupPaths();

    Y_VERIFY_S(pathsTotal >= backupPaths, "Constraint violation"
        << ": path: " << Path.PathString()
        << ", paths total: " << pathsTotal
        << ", backup paths: " << backupPaths);

    if (!delta || (pathsTotal - backupPaths) + delta <= domainInfo->GetSchemeLimits().MaxPaths) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "paths count limit exceeded"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxPaths
        << ", paths: " << (pathsTotal - backupPaths)
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::DirChildrenLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    auto parent = Path.Parent();
    const auto aliveChildren = parent.Base()->GetAliveChildren();
    const auto backupChildren = parent.Base()->GetBackupChildren();

    Y_VERIFY_S(aliveChildren >= backupChildren, "Constraint violation"
        << ": path: " << parent.PathString()
        << ", alive children: " << aliveChildren
        << ", backup children: " << backupChildren);

    if (!delta || (aliveChildren - backupChildren) + delta <= domainInfo->GetSchemeLimits().MaxChildrenInDir) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "children count limit exceeded"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxChildrenInDir
        << ", children: " << (aliveChildren - backupChildren)
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::ShardsLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    const auto shardsTotal = domainInfo->GetShardsInside();
    const auto backupShards = domainInfo->GetBackupShards();

    Y_VERIFY_S(shardsTotal >= backupShards, "Constraint violation"
        << ": path: " << Path.PathString()
        << ", shards total: " << shardsTotal
        << ", backup shards: " << backupShards);

    if (!delta || (shardsTotal - backupShards) + delta <= domainInfo->GetSchemeLimits().MaxShards) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "shards count limit exceeded (in subdomain)"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxShards
        << ", shards: " << (shardsTotal - backupShards)
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::PQPartitionsLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!delta) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    const auto pqPartitions = domainInfo->GetPQPartitionsInside();

    if (pqPartitions + delta > domainInfo->GetSchemeLimits().MaxPQPartitions) {
        return Fail(status, TStringBuilder() << "data stream shards limit exceeded"
            << ", limit: " << domainInfo->GetSchemeLimits().MaxPQPartitions
            << ", data stream shards: " << pqPartitions
            << ", delta: " << delta);
    }

    if (const auto& quotas = domainInfo->GetDatabaseQuotas()) {
        if (const auto limit = quotas->data_stream_shards_quota()) {
            if (pqPartitions + delta > limit) {
                return Fail(status, TStringBuilder() << "data stream shards limit exceeded"
                    << ", limit: " << limit
                    << ", data stream shards: " << pqPartitions
                    << ", delta: " << delta);
            }
        }
    }

    return *this;
}

const TPath::TChecker& TPath::TChecker::PQReservedStorageLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!delta) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    const auto pqReservedStorage = domainInfo->GetPQReservedStorage();
    static constexpr ui64 MaxPQStorage = Max<ui64>() / 2;

    if (pqReservedStorage + delta > MaxPQStorage) {
        return Fail(status, TStringBuilder() << "data stream reserved storage size limit exceeded"
            << ", limit: " << MaxPQStorage << " bytes"
            << ", data stream reserved storage size: " << pqReservedStorage << " bytes"
            << ", delta: " << delta << " bytes");
    }

    if (const auto& quotas = domainInfo->GetDatabaseQuotas()) {
        if (const auto limit = quotas->data_stream_reserved_storage_quota()) {
            if (pqReservedStorage + delta > limit) {
                return Fail(status, TStringBuilder() << "data stream reserved storage size limit exceeded"
                    << ", limit: " << limit << " bytes"
                    << ", data stream reserved storage size: " << pqReservedStorage << " bytes"
                    << ", delta: " << delta << " bytes");
            }
        }
    }

    ui64 quotasAvailable = domainInfo->DiskSpaceQuotasAvailable();
    if (quotasAvailable < delta && AppData()->FeatureFlags.GetEnableTopicDiskSubDomainQuota()) {
        return Fail(status, TStringBuilder() << "database size limit exceeded"
            << ", limit: " << domainInfo->GetDiskSpaceQuotas().HardQuota << " bytes"
            << ", available: " << quotasAvailable << " bytes"
            << ", delta: " << delta << " bytes");
    }

    return *this;
}

const TPath::TChecker& TPath::TChecker::IsExternalTable(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsExternalTable()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a external table"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsExternalDataSource(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsExternalDataSource()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a external data source"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::IsView(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsView()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a view"
        << " (" << BasicPathInfo(Path.Base()) << ")"
    );
}

const TPath::TChecker& TPath::TChecker::FailOnRestrictedCreateInTempZone(bool allowCreateInTemporaryDir, EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (allowCreateInTemporaryDir) {
        return *this;
    }

    for (const auto& element : Path.Elements) {
        if (element->IsTemporary()) {
            return Fail(status, TStringBuilder() << "path is temporary"
                << " (" << BasicPathInfo(Path.Base()) << ")"
            );
        }
    }

    return *this;
}

const TPath::TChecker& TPath::TChecker::IsResourcePool(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Base()->IsResourcePool()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path is not a resource pool"
        << " (" << BasicPathInfo(Path.Base()) << ")");
}

const TPath::TChecker& TPath::TChecker::PathShardsLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    const ui64 shardInPath = Path.Shards();

    if (Path.IsResolved() && !Path.IsDeleted()) {
        const auto allShards = Path.SS->CollectAllShards({Path.Base()->PathId});
        Y_VERIFY_DEBUG_S(allShards.size() == shardInPath, "pedantic check"
            << ": CollectAllShards(): " << allShards.size()
            << ", Path.Shards(): " << shardInPath
            << ", path: " << Path.PathString());
    }

    if (!delta || shardInPath + delta <= domainInfo->GetSchemeLimits().MaxShardsInPath) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "shards count limit exceeded (in dir)"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxShardsInPath
        << ", shards: " << shardInPath
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::ExportsLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    if (Path.SS->Exports.size() + delta <= domainInfo->GetSchemeLimits().MaxExports) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "exports count limit exceeded"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxExports
        << ", exports: " << Path.SS->Exports.size()
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::ImportsLimit(ui64 delta, EStatus status) const {
    if (Failed) {
        return *this;
    }

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    if (Path.SS->Imports.size() + delta <= domainInfo->GetSchemeLimits().MaxImports) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "imports count limit exceeded"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxImports
        << ", exports: " << Path.SS->Imports.size()
        << ", delta: " << delta);
}

const TPath::TChecker& TPath::TChecker::NotChildren(EStatus status) const {
    if (Failed) {
        return *this;
    }

    const auto childrenCount = Path.Base()->GetAliveChildren();
    if (!childrenCount) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path has children, request doesn't accept it"
        << ", children: " << childrenCount);
}

const TPath::TChecker& TPath::TChecker::CanBackupTable(EStatus status) const {
    if (Failed) {
        return *this;
    }

    for (const auto& child: Path.Base()->GetChildren()) {
        auto name = child.first;

        TPath childPath = Path.Child(name);
        if (childPath->IsTableIndex()) {
            return Fail(status, TStringBuilder() << "path has indexes, request doesn't accept it");
        }
    }

    return *this;
}

const TPath::TChecker& TPath::TChecker::NotDeleted(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (!Path.IsDeleted()) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "path has been deleted"
        << " (" << BasicPathInfo(Path.Base()) << ")"
        << ", drop stepId: " << Path.Base()->StepDropped
        << ", drop txId: " << Path.Base()->DropTxId);
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

    TSubDomainInfo::TPtr domainInfo = Path.DomainInfo();
    if (bytesSize <= domainInfo->GetSchemeLimits().MaxAclBytesSize) {
        return *this;
    }

    return Fail(status, TStringBuilder() << "ACL size limit exceeded"
        << ", limit: " << domainInfo->GetSchemeLimits().MaxAclBytesSize
        << ", new ACL size: " << bytesSize);
}

const TPath::TChecker& TPath::TChecker::IsNameUniqGrandParentLevel(EStatus status) const {
    if (Failed) {
        return *this;
    }

    if (Path.Elements.size() < 2) {
        return *this;
    }

    // must be a copy here
    auto path = Path.Parent();
    if (!path.IsResolved()) {
        return *this;
    }

    auto parentPathId = path->PathId;

    path.Rise();
    if (!path.IsResolved()) {
        return *this;
    }

    const auto& myName = Path.NameParts.back();

    auto raiseErr = [this](EStatus st, const TString& myName, const TString& conflict) -> const TPath::TChecker& {
        return Fail(st, TStringBuilder() << "name " << myName
            << " is not uniq. Found in: " << conflict);
    };

    if (path.Elements.back()->FindChild(myName)) {
        return raiseErr(status, myName, path.PathString());
    }

    TVector<TPathId> uncles;
    uncles.reserve(path.Elements.back()->GetChildren().size());
    for (const auto& [x, unclePathId] : path.Elements.back()->GetChildren()) {
        if (unclePathId != parentPathId) {
            uncles.emplace_back(unclePathId);
        }
    }

    for (const auto& pathid : uncles) {
        auto& uncle = path.DiveByPathId(pathid);
        if (uncle->FindChild(myName)) {
            return raiseErr(status, myName, uncle.PathString());
        }
        uncle.Rise();
    }

    return *this;
}

TString TPath::TChecker::BasicPathInfo(TPathElement::TPtr element) const {
    return TStringBuilder()
        << "id: " << element->PathId << ", "
        << "type: " << element->PathType << ", "
        << "state: " << element->PathState;
}

TPath::TPath(TSchemeShard* ss)
    : SS(ss)
{
    Y_ABORT_UNLESS(SS);
    Y_ABORT_UNLESS(IsEmpty() && !IsResolved());
}

TPath::TPath(TVector<TPathElement::TPtr>&& elements, TSchemeShard* ss)
    : SS(ss)
    , Elements(std::move(elements))
{
    Y_ABORT_UNLESS(SS);
    Y_ABORT_UNLESS(Elements);

    NameParts.reserve(Elements.size());
    for (const auto& item : Elements) {
        NameParts.push_back(item->Name);
    }

    Y_ABORT_UNLESS(!IsEmpty());
    Y_ABORT_UNLESS(IsResolved());
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

TPath::operator bool() const {
    return IsResolved();
}

bool TPath::operator ==(const TPath& another) const { // likely O(1) complexity, but might be O(path length)
    if (Y_LIKELY(IsResolved() && another.IsResolved())) {
        return Base()->PathId == another.Base()->PathId; // check path ids is the only right way to compare
    }                                                    // PathStrings could be false equal due operation Init form deleted path element
    if (IsResolved() || another.IsResolved()) {
        return false;
    }
    // both are not resolved

    if (IsEmpty() && another.IsEmpty()) {
        return true;
    }

    if (IsEmpty() || another.IsEmpty()) {
        return false;
    }
    // both are not empty

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
    Y_ABORT_UNLESS(ss);
    return TPath::Init(ss->RootPathId(), ss);
}

TString TPath::PathString() const {
    return CanonizePath(NameParts);
}

TPath& TPath::Rise() {
    if (!NameParts) {
        Y_ABORT_UNLESS(!Elements);
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
    // TODO: not effective because of creating vectors in Init() method. should keep subdomain path somewhere in struct TSubDomainInfo
    return Init(GetPathIdForDomain(), SS).PathString();
}

TSubDomainInfo::TPtr TPath::DomainInfo() const {
    Y_ABORT_UNLESS(!IsEmpty());
    Y_ABORT_UNLESS(Elements.size());

    return SS->ResolveDomainInfo(Elements.back());
}

TPathId TPath::GetPathIdForDomain() const {
    Y_ABORT_UNLESS(!IsEmpty());
    Y_ABORT_UNLESS(Elements.size());

    return SS->ResolvePathIdForDomain(Elements.back());
}

TPathId TPath::GetDomainKey() const {
    Y_ABORT_UNLESS(IsResolved());

    return SS->GetDomainKey(Elements.back());
}

bool TPath::IsDomain() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->IsDomainRoot();
}

TPath& TPath::Dive(const TString& name) {
    if (!SS->IsSchemeShardConfigured()) {
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
    Y_ABORT_UNLESS(ss);

    TPath nullPrefix{ss};
    return Resolve(nullPrefix, SplitPath(path));
}

TPath TPath::Resolve(const TPath& prefix, TVector<TString>&& pathParts) {
    TPath result = prefix;

    for (const auto& part : pathParts) {
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
    Y_ABORT_UNLESS(ss);

    if (!ss->PathsById.contains(pathId)) {
        return TPath(ss);
    }

    TVector<TPathElement::TPtr> parts;
    TPathElement::TPtr cur = ss->PathsById.at(pathId);

    while (!cur->IsRoot()) {
        parts.push_back(cur);
        Y_ABORT_UNLESS(ss->PathsById.contains(cur->ParentPathId));
        cur = ss->PathsById.at(cur->ParentPathId);
    }

    parts.push_back(cur); // add root
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
    Y_ABORT_UNLESS(IsResolved());

    return Base()->Dropped();
}

bool TPath::IsUnderOperation() const {
    Y_ABORT_UNLESS(IsResolved());

    if (Base()->Dropped()) {
        return false;
    }

    bool result = Base()->PathState != NKikimrSchemeOp::EPathState::EPathStateNoChanges;
    if (result) {
        ui32 sum = (ui32)IsUnderCreating()
            + (ui32)IsUnderAltering()
            + (ui32)IsUnderCopying()
            + (ui32)IsUnderBackingUp()
            + (ui32)IsUnderRestoring()
            + (ui32)IsUnderDeleting()
            + (ui32)IsUnderDomainUpgrade()
            + (ui32)IsUnderMoving();
        Y_VERIFY_S(sum == 1,
                   "only one operation at the time"
                       << " pathId: " << Base()->PathId
                       << " path state: " << NKikimrSchemeOp::EPathState_Name(Base()->PathState)
                       << " path: " << PathString()
                       << " sum is: " << sum);
    }
    return result;
}

TTxId TPath::ActiveOperation() const {
    Y_ABORT_UNLESS(IsResolved());

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

    Y_ABORT_UNLESS(txId != InvalidTxId);
    Y_VERIFY_S(SS->Operations.contains(txId),
               "no operation,"
                   << " txId: " << txId
                   << " pathId: " << Base()->PathId
                   << " path state: " << NKikimrSchemeOp::EPathState_Name(Base()->PathState)
                   << " path " << PathString());

    return txId;
}

bool TPath::IsUnderCreating() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateCreate;
}

bool TPath::IsUnderAltering() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateAlter;
}

bool TPath::IsUnderDomainUpgrade() const {
    if (Elements.empty()) {
        return false;
    }

    return SS->PathsById.at(GetPathIdForDomain())->PathState == NKikimrSchemeOp::EPathState::EPathStateUpgrade;
}

bool TPath::IsUnderCopying() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateCopying;
}

bool TPath::IsUnderBackingUp() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateBackup;
}

bool TPath::IsUnderRestoring() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateRestore;
}

bool TPath::IsUnderDeleting() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->PathState == NKikimrSchemeOp::EPathState::EPathStateDrop;
}

bool TPath::IsUnderMoving() const {
    Y_ABORT_UNLESS(IsResolved());

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
    Y_ABORT_UNLESS(IsResolved());

    for (auto item = ++Elements.rbegin(); item != Elements.rend(); ++item) {
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
    Y_ABORT_UNLESS(IsResolved());

    // expected /<root>/.../<table>/<table_index>/<private_tables>
    if (Depth() < 3) {
        return false;
    }

    auto item = Elements.rbegin();

    // skip private_table
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
    Y_ABORT_UNLESS(IsResolved());

    // expected /<root>/.../<table>/<cdc_stream>/<private_topic>
    if (Depth() < 3) {
        return false;
    }

    auto item = Elements.rbegin();

    // skip private_topic
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

    return true;
}

bool TPath::IsTableIndex(const TMaybe<NKikimrSchemeOp::EIndexType>& type) const {
    Y_ABORT_UNLESS(IsResolved());

    if (!Base()->IsTableIndex()) {
        return false;
    }

    if (!type.Defined()) {
        return true;
    }

    Y_ABORT_UNLESS(SS->Indexes.contains(Base()->PathId));
    return SS->Indexes.at(Base()->PathId)->Type == *type;
}

bool TPath::IsBackupTable() const {
    Y_ABORT_UNLESS(IsResolved());

    if (!Base()->IsTable() || !SS->Tables.contains(Base()->PathId)) {
        return false;
    }

    TTableInfo::TCPtr tableInfo = SS->Tables.at(Base()->PathId);

    return tableInfo->IsBackup;
}

bool TPath::IsAsyncReplicaTable() const {
    Y_ABORT_UNLESS(IsResolved());

    if (!Base()->IsTable() || !SS->Tables.contains(Base()->PathId)) {
        return false;
    }

    TTableInfo::TCPtr tableInfo = SS->Tables.at(Base()->PathId);

    return tableInfo->IsAsyncReplica();
}

bool TPath::IsCdcStream() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->IsCdcStream();
}

bool TPath::IsSequence() const {
    Y_ABORT_UNLESS(IsResolved());

    return Base()->IsSequence();
}

bool TPath::IsReplication() const {
    Y_ABORT_UNLESS(IsResolved());

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

const TString& TPath::LeafName() const {
    Y_ABORT_UNLESS(!IsEmpty());
    return NameParts.back();
}

bool TPath::IsValidLeafName(TString& explain) const {
    Y_ABORT_UNLESS(!IsEmpty());

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

    if (!SS->IsSchemeShardConfigured()) {
        explain += "cluster don't have initialized root yet";
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
    Y_ABORT_UNLESS(IsResolved());

    ui64 version = 0;

    if (!SS->IsDomainSchemeShard) {
        version += SS->ParentDomainEffectiveACLVersion;
    }

    // actualize CachedEffectiveACL in each element if needed
    for (auto item = Elements.begin(); item != Elements.end(); ++item) {
        TPathElement::TPtr element = *item;
        version += element->ACLVersion;

        if (element->CachedEffectiveACLVersion != version || !element->CachedEffectiveACL) {  // path needs actualizing
            if (item == Elements.begin()) { // it is root
                if (!SS->IsDomainSchemeShard) {
                    element->CachedEffectiveACL.Update(SS->ParentDomainCachedEffectiveACL, element->ACL, element->IsContainer());
                } else {
                    element->CachedEffectiveACL.Init(element->ACL);
                }
            } else { // path element in the middle
                auto prevIt = std::prev(item);
                const auto& prevElement = *prevIt;
                element->CachedEffectiveACL.Update(prevElement->CachedEffectiveACL, element->ACL, element->IsContainer());
            }
            element->CachedEffectiveACLVersion = version;
        }
    }

    return Elements.back()->CachedEffectiveACL.GetForSelf();
}

ui64 TPath::GetEffectiveACLVersion() const {
    Y_ABORT_UNLESS(IsResolved());

    ui64 version = 0;

    if (!SS->IsDomainSchemeShard) {
        version += SS->ParentDomainEffectiveACLVersion;
    }

    for (auto item = Elements.begin(); item != Elements.end(); ++item) {
        version += (*item)->ACLVersion;
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

    Y_ABORT_UNLESS(Base()->IsCreateFinished());

    auto result = SS->AttachChild(Base());
    Y_VERIFY_S(result == EAttachChildResult::AttachedAsNewerActual, "result is: " << result);
}

void TPath::MaterializeLeaf(const TString& owner) {
    return MaterializeLeaf(owner, SS->AllocatePathId(), /*allowInactivePath*/ false);
}

void TPath::MaterializeLeaf(const TString& owner, const TPathId& newPathId, bool allowInactivePath) {
    auto result = MaterializeImpl(owner, newPathId);
    switch (result) {
    case EAttachChildResult::Undefined:
        Y_ABORT("unexpected result: Undefined");
        break;

    case EAttachChildResult::AttachedAsOnlyOne:
    case EAttachChildResult::AttachedAsActual:
        Y_ABORT_UNLESS(SS->PathIsActive(newPathId));
        break;

    case EAttachChildResult::AttachedAsCreatedActual:
    case EAttachChildResult::AttachedAsOlderUnCreated:
    case EAttachChildResult::AttachedAsNewerDeleted:
    case EAttachChildResult::AttachedAsNewerActual:
        Y_FAIL_S("strange result for materialization: " << result);
        break;

    case EAttachChildResult::RejectAsInactive:
        if (allowInactivePath) {
            Y_ABORT_UNLESS(!SS->PathIsActive(newPathId));
        } else {
            Y_FAIL_S("MaterializeLeaf do not accept shadow paths, use allowInactivePath = true");
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
    Y_ABORT_UNLESS(IsResolved());

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
    Y_ABORT_UNLESS(IsResolved());

    TPathElement::TPtr nextElem = SS->PathsById.at(pathId);
    Y_ABORT_UNLESS(nextElem->ParentPathId == Elements.back()->PathId);

    Elements.push_back(nextElem);
    NameParts.push_back(nextElem->Name);

    return *this;
}

TPathId TPath::GetPathIdSafe() const {
    return IsResolved()
        ? Base()->PathId
        : InvalidPathId;
}

}
