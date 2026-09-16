#include "path_aliasing.h"

#include <ydb/core/base/path.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/msgbus.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/public/api/protos/ydb_cms.pb.h>

namespace NKikimr::NMsgBusProxy {
    namespace {

        TString* PrimaryName(NKikimrSchemeOp::TModifyScheme& scheme) {
            using namespace NKikimrSchemeOp;
            switch (scheme.GetOperationType()) {
#define SCHEMA_NAME(Operation, Body, Field)                          \
    case ESchemeOp##Operation:                                       \
        return scheme.Has##Body() && scheme.Get##Body().Has##Field() \
                   ? scheme.Mutable##Body()->Mutable##Field()        \
                   : nullptr;
                SCHEMA_NAME(MkDir, MkDir, Name)
                SCHEMA_NAME(CreateTable, CreateTable, Name)
                SCHEMA_NAME(AlterTable, AlterTable, Name)
                SCHEMA_NAME(CreatePersQueueGroup, CreatePersQueueGroup, Name)
                SCHEMA_NAME(AlterPersQueueGroup, AlterPersQueueGroup, Name)
                SCHEMA_NAME(ModifyACL, ModifyACL, Name)
                SCHEMA_NAME(Backup, Backup, TableName)
                SCHEMA_NAME(CreateSubDomain, SubDomain, Name)
                SCHEMA_NAME(AlterSubDomain, SubDomain, Name)
                SCHEMA_NAME(CreateExtSubDomain, SubDomain, Name)
                SCHEMA_NAME(AlterExtSubDomain, SubDomain, Name)
                SCHEMA_NAME(CreateRtmrVolume, CreateRtmrVolume, Name)
                SCHEMA_NAME(CreateBlockStoreVolume, CreateBlockStoreVolume, Name)
                SCHEMA_NAME(AlterBlockStoreVolume, AlterBlockStoreVolume, Name)
                SCHEMA_NAME(AssignBlockStoreVolume, AssignBlockStoreVolume, Name)
                SCHEMA_NAME(CreateSolomonVolume, CreateSolomonVolume, Name)
                SCHEMA_NAME(AlterSolomonVolume, AlterSolomonVolume, Name)
                SCHEMA_NAME(CreateFileStore, CreateFileStore, Name)
                SCHEMA_NAME(AlterFileStore, AlterFileStore, Name)
                SCHEMA_NAME(CreateColumnStore, CreateColumnStore, Name)
                SCHEMA_NAME(AlterColumnStore, AlterColumnStore, Name)
                SCHEMA_NAME(CreateColumnTable, CreateColumnTable, Name)
                SCHEMA_NAME(AlterColumnTable, AlterColumnTable, Name)
                SCHEMA_NAME(CreateKesus, Kesus, Name)
                SCHEMA_NAME(AlterKesus, Kesus, Name)
                SCHEMA_NAME(AlterUserAttributes, AlterUserAttributes, PathName)
                SCHEMA_NAME(UpgradeSubDomain, UpgradeSubDomain, Name)
                SCHEMA_NAME(UpgradeSubDomainDecision, UpgradeSubDomain, Name)
                SCHEMA_NAME(DropIndex, DropIndex, TableName)
                SCHEMA_NAME(Restore, Restore, TableName)
                SCHEMA_NAME(CreateCdcStream, CreateCdcStream, TableName)
                SCHEMA_NAME(AlterCdcStream, AlterCdcStream, TableName)
                SCHEMA_NAME(DropCdcStream, DropCdcStream, TableName)
                SCHEMA_NAME(RotateCdcStream, RotateCdcStream, TableName)
                SCHEMA_NAME(CreateContinuousBackup, CreateContinuousBackup, TableName)
                SCHEMA_NAME(AlterContinuousBackup, AlterContinuousBackup, TableName)
                SCHEMA_NAME(DropContinuousBackup, DropContinuousBackup, TableName)
                SCHEMA_NAME(CreateSequence, Sequence, Name)
                SCHEMA_NAME(AlterSequence, Sequence, Name)
                SCHEMA_NAME(CreateReplication, Replication, Name)
                SCHEMA_NAME(AlterReplication, Replication, Name)
                SCHEMA_NAME(CreateTransfer, Replication, Name)
                SCHEMA_NAME(AlterTransfer, Replication, Name)
                SCHEMA_NAME(CreateBlobDepot, BlobDepot, Name)
                SCHEMA_NAME(AlterBlobDepot, BlobDepot, Name)
                SCHEMA_NAME(CreateExternalTable, CreateExternalTable, Name)
                SCHEMA_NAME(CreateExternalDataSource, CreateExternalDataSource, Name)
                SCHEMA_NAME(CreateView, CreateView, Name)
                SCHEMA_NAME(CreateSecret, CreateSecret, Name)
                SCHEMA_NAME(AlterSecret, AlterSecret, Name)
                SCHEMA_NAME(CreateResourcePool, CreateResourcePool, Name)
                SCHEMA_NAME(CreateBackupCollection, CreateBackupCollection, Name)
                SCHEMA_NAME(AlterBackupCollection, AlterBackupCollection, Name)
                SCHEMA_NAME(DropBackupCollection, DropBackupCollection, Name)
                SCHEMA_NAME(BackupBackupCollection, BackupBackupCollection, Name)
                SCHEMA_NAME(BackupIncrementalBackupCollection, BackupIncrementalBackupCollection, Name)
                SCHEMA_NAME(RestoreBackupCollection, RestoreBackupCollection, Name)
                SCHEMA_NAME(CreateSysView, CreateSysView, Name)
                SCHEMA_NAME(CreateTestShardSet, CreateTestShardSet, Name)
                SCHEMA_NAME(AlterResourcePool, CreateResourcePool, Name)
                SCHEMA_NAME(ChangePathState, ChangePathState, Path)
                SCHEMA_NAME(CreateStreamingQuery, CreateStreamingQuery, Name)
                SCHEMA_NAME(AlterStreamingQuery, CreateStreamingQuery, Name)
                SCHEMA_NAME(TruncateTable, TruncateTable, TableName)
#undef SCHEMA_NAME
                case ESchemeOpCreateIndexedTable:
                    return scheme.GetCreateIndexedTable().GetTableDescription().HasName()
                               ? scheme.MutableCreateIndexedTable()->MutableTableDescription()->MutableName()
                               : nullptr;
                case ESchemeOpDropTable:
                case ESchemeOpDropPersQueueGroup:
                case ESchemeOpRmDir:
                case ESchemeOpDropSubDomain:
                case ESchemeOpDropBlockStoreVolume:
                case ESchemeOpDropKesus:
                case ESchemeOpForceDropSubDomain:
                case ESchemeOpDropSolomonVolume:
                case ESchemeOpForceDropUnsafe:
                case ESchemeOpForceDropExtSubDomain:
                case ESchemeOpDropFileStore:
                case ESchemeOpDropColumnStore:
                case ESchemeOpDropColumnTable:
                case ESchemeOpDropSequence:
                case ESchemeOpDropReplication:
                case ESchemeOpDropReplicationCascade:
                case ESchemeOpDropTransfer:
                case ESchemeOpDropTransferCascade:
                case ESchemeOpDropBlobDepot:
                case ESchemeOpDropExternalTable:
                case ESchemeOpDropExternalDataSource:
                case ESchemeOpDropView:
                case ESchemeOpDropResourcePool:
                case ESchemeOpDropSysView:
                case ESchemeOpDropSecret:
                case ESchemeOpDropStreamingQuery:
                case ESchemeOpDropTestShardSet:
                    return scheme.GetDrop().HasName() ? scheme.MutableDrop()->MutableName() : nullptr;
                default:
                    // Internal and unsupported operation kinds retain their owner checks.
                    return nullptr;
            }
        }

        TString Compose(const TString& directory, const TString& name) {
            auto parts = SplitPath(directory);
            const auto tail = SplitPath(name);
            parts.insert(parts.end(), tail.begin(), tail.end());
            return CanonizePath(parts);
        }

        class TSchemaPaths {
        public:
            explicit TSchemaPaths(const NPathAliasing::TPathContext& context)
                : Context(context)
            {
            }

            TConclusionStatus Root(TString& path) const {
                if (path.empty()) {
                    return TConclusionStatus::Success();
                }
                TString candidate = CanonizePath(path);
                if (candidate.empty()) {
                    candidate = "/";
                }
                auto resolved = Context.NormalizePath(candidate);
                if (resolved.IsFail()) {
                    return resolved;
                }
                if (resolved->Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten) {
                    path = std::move(resolved.DetachResult().Path);
                }
                return TConclusionStatus::Success();
            }

            TConclusionStatus Relative(TString& path, const TString& parent) const {
                if (path.empty() || path.StartsWith('/')) {
                    return Root(path);
                }
                auto resolved = Context.NormalizePath(Compose(parent, path));
                if (resolved.IsFail()) {
                    return resolved;
                }
                // A miss is still the complete logical candidate, not a new relative
                // operand to be joined under an already rewritten table/database.
                if (PrimaryChanged || resolved->Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten) {
                    path = std::move(resolved.DetachResult().Path);
                }
                return TConclusionStatus::Success();
            }

            TConclusionStatus Primary(NKikimrSchemeOp::TModifyScheme& scheme, TString& logicalParent) {
                auto* name = PrimaryName(scheme);
                if (!name || name->empty()) {
                    logicalParent = scheme.GetWorkingDir();
                    if (scheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpAlterLogin ||
                        scheme.GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateFullBackupOp) {
                        return Root(*scheme.MutableWorkingDir());
                    }
                    return TConclusionStatus::Success();
                }
                const TString candidate = Compose(scheme.GetWorkingDir(), *name);
                const auto logicalSlash = candidate.rfind('/');
                logicalParent = logicalSlash == TString::npos ? TString{} : candidate.substr(0, logicalSlash);
                auto resolved = Context.NormalizePath(candidate);
                if (resolved.IsFail()) {
                    return resolved;
                }
                if (resolved->Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten) {
                    const TString& path = resolved->Path;
                    const auto slash = path.rfind('/');
                    if (slash == TString::npos || slash + 1 == path.size()) {
                        return TConclusionStatus::Fail("Rewritten schema object path has no name");
                    }
                    scheme.SetWorkingDir(slash ? path.substr(0, slash) : TString("/"));
                    *name = path.substr(slash + 1);
                    PrimaryChanged = true;
                }
                return TConclusionStatus::Success();
            }

            template <class TTtlSettings>
            TConclusionStatus Ttl(TTtlSettings& settings) const {
                for (auto& tier : *settings.MutableTiers()) {
                    if (tier.HasEvictToExternalStorage() && tier.GetEvictToExternalStorage().HasStorage()) {
                        if (auto status = Root(*tier.MutableEvictToExternalStorage()->MutableStorage()); status.IsFail()) {
                            return status;
                        }
                    }
                }
                return TConclusionStatus::Success();
            }

            TConclusionStatus Table(NKikimrSchemeOp::TTableDescription& table, const TString& logicalParent,
                                    const google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TSequenceDescription>* indexedSequences = nullptr) const {
                if (table.HasCopyFromTable()) {
                    if (auto status = Root(*table.MutableCopyFromTable()); status.IsFail()) {
                        return status;
                    }
                }
                if (table.HasTTLSettings() && table.GetTTLSettings().HasEnabled()) {
                    if (auto status = Ttl(*table.MutableTTLSettings()->MutableEnabled()); status.IsFail()) {
                        return status;
                    }
                }
                for (auto& column : *table.MutableColumns()) {
                    if (!column.HasDefaultFromSequence()) {
                        continue;
                    }
                    bool declared = false;
                    for (const auto& sequence : table.GetSequences()) {
                        declared |= column.GetDefaultFromSequence() == sequence.GetName();
                    }
                    if (indexedSequences) {
                        for (const auto& sequence : *indexedSequences) {
                            declared |= column.GetDefaultFromSequence() == sequence.GetName();
                        }
                    }
                    if (!declared) {
                        if (auto status = Relative(*column.MutableDefaultFromSequence(), logicalParent); status.IsFail()) {
                            return status;
                        }
                    }
                }
                return TConclusionStatus::Success();
            }

            TConclusionStatus Collection(NKikimrSchemeOp::TBackupCollectionDescription& collection) const {
                if (collection.HasExplicitEntryList()) {
                    for (auto& entry : *collection.MutableExplicitEntryList()->MutableEntries()) {
                        if (entry.HasPath()) {
                            if (auto status = Root(*entry.MutablePath()); status.IsFail()) {
                                return status;
                            }
                        }
                    }
                }
                if (collection.HasPrefix()) {
                    return Root(*collection.MutablePrefix());
                }
                return TConclusionStatus::Success();
            }

            template <class TMove>
            TConclusionStatus Move(TMove& move) const {
                if (move.HasSrcPath()) {
                    if (auto status = Root(*move.MutableSrcPath()); status.IsFail()) {
                        return status;
                    }
                }
                if (move.HasDstPath()) {
                    return Root(*move.MutableDstPath());
                }
                return TConclusionStatus::Success();
            }

        private:
            const NPathAliasing::TPathContext& Context;
            bool PrimaryChanged = false;
        };

    } // namespace

    TConclusionStatus NormalizeMessageBusDatabasePaths(
        NKikimrClient::TConsoleRequest& request, const NPathAliasing::TPathContext& context)
    {
        if (context.Empty()) {
            return TConclusionStatus::Success();
        }
        using TRequest = NKikimrClient::TConsoleRequest;
        switch (request.GetRequestCase()) {
            case TRequest::kCreateTenantRequest:
            case TRequest::kGetTenantStatusRequest:
            case TRequest::kAlterTenantRequest:
            case TRequest::kRemoveTenantRequest:
                break;
            default:
                return TConclusionStatus::Success();
        }
        TSchemaPaths paths(context);
        auto resolved = request;
        const auto normalize = [&paths](auto& envelope) -> TConclusionStatus {
            if (!envelope.HasRequest() || envelope.GetRequest().path().empty()) {
                return TConclusionStatus::Success();
            }
            TString path = envelope.GetRequest().path();
            if (auto status = paths.Root(path); status.IsFail()) {
                return status;
            }
            envelope.MutableRequest()->set_path(path);
            return TConclusionStatus::Success();
        };
        TConclusionStatus status = TConclusionStatus::Success();
        switch (resolved.GetRequestCase()) {
            case TRequest::kCreateTenantRequest: {
                auto& envelope = *resolved.MutableCreateTenantRequest();
                status = normalize(envelope);
                if (status.IsSuccess() && envelope.GetRequest().has_serverless_resources()) {
                    auto* resources = envelope.MutableRequest()->mutable_serverless_resources();
                    TString sharedDatabase = resources->shared_database_path();
                    status = paths.Root(sharedDatabase);
                    if (status.IsSuccess()) {
                        resources->set_shared_database_path(sharedDatabase);
                    }
                }
                break;
            }
            case TRequest::kGetTenantStatusRequest:
                status = normalize(*resolved.MutableGetTenantStatusRequest());
                break;
            case TRequest::kAlterTenantRequest:
                status = normalize(*resolved.MutableAlterTenantRequest());
                break;
            case TRequest::kRemoveTenantRequest:
                status = normalize(*resolved.MutableRemoveTenantRequest());
                break;
            default:
                break;
        }
        if (status.IsSuccess()) {
            request.Swap(&resolved);
        }
        return status;
    }

    TConclusionStatus NormalizeMessageBusMaintenancePaths(
        NKikimrClient::TCmsRequest& request, const NPathAliasing::TPathContext& context)
    {
        if (context.Empty() || !request.HasPermissionRequest()) {
            return TConclusionStatus::Success();
        }
        auto resolved = request.GetPermissionRequest();
        for (auto& action : *resolved.MutableActions()) {
            if (!action.HasTenant() || action.GetTenant().empty()) {
                continue;
            }
            // CMS matches this selector exactly against physical tenant paths.
            // Host, device, service and maintenance IDs are separate namespaces.
            auto path = context.NormalizePath(action.GetTenant());
            if (path.IsFail()) {
                return path;
            }
            if (path->Outcome == NPathAliasing::EPathRewriteOutcome::Rewritten) {
                action.SetTenant(path->Path);
            }
        }
        request.MutablePermissionRequest()->Swap(&resolved);
        return TConclusionStatus::Success();
    }

    TConclusionStatus NormalizeMessageBusSchemaPaths(
        NKikimrSchemeOp::TModifyScheme& scheme, const NPathAliasing::TPathContext& context)
    {
        if (context.Empty()) {
            return TConclusionStatus::Success();
        }
        TSchemaPaths paths(context);
        TString logicalParent;
        if (auto status = paths.Primary(scheme, logicalParent); status.IsFail()) {
            return status;
        }
        using namespace NKikimrSchemeOp;
        switch (scheme.GetOperationType()) {
            case ESchemeOpCreateTable:
                return scheme.HasCreateTable() ? paths.Table(*scheme.MutableCreateTable(), logicalParent) : TConclusionStatus::Success();
            case ESchemeOpAlterTable:
                return scheme.HasAlterTable() ? paths.Table(*scheme.MutableAlterTable(), logicalParent) : TConclusionStatus::Success();
            case ESchemeOpCreateIndexedTable:
                if (scheme.GetCreateIndexedTable().HasTableDescription()) {
                    return paths.Table(*scheme.MutableCreateIndexedTable()->MutableTableDescription(), logicalParent,
                                       &scheme.GetCreateIndexedTable().GetSequenceDescription());
                }
                break;
            case ESchemeOpCreateColumnTable:
                if (scheme.GetCreateColumnTable().HasTtlSettings() && scheme.GetCreateColumnTable().GetTtlSettings().HasEnabled()) {
                    return paths.Ttl(*scheme.MutableCreateColumnTable()->MutableTtlSettings()->MutableEnabled());
                }
                break;
            case ESchemeOpAlterColumnTable:
                if (scheme.GetAlterColumnTable().HasAlterTtlSettings() && scheme.GetAlterColumnTable().GetAlterTtlSettings().HasEnabled()) {
                    return paths.Ttl(*scheme.MutableAlterColumnTable()->MutableAlterTtlSettings()->MutableEnabled());
                }
                break;
            case ESchemeOpCreateConsistentCopyTables:
                if (!scheme.HasCreateConsistentCopyTables()) {
                    break;
                }
                for (auto& table : *scheme.MutableCreateConsistentCopyTables()->MutableCopyTableDescriptions()) {
                    if (auto status = paths.Move(table); status.IsFail()) {
                        return status;
                    }
                }
                break;
            case ESchemeOpMoveTable:
                return scheme.HasMoveTable() ? paths.Move(*scheme.MutableMoveTable()) : TConclusionStatus::Success();
            case ESchemeOpMoveSequence:
                return scheme.HasMoveSequence() ? paths.Move(*scheme.MutableMoveSequence()) : TConclusionStatus::Success();
            case ESchemeOpMoveIndex:
                // SrcPath/DstPath here are index-local names, not schema operands.
                if (scheme.GetMoveIndex().HasTablePath()) {
                    return paths.Root(*scheme.MutableMoveIndex()->MutableTablePath());
                }
                break;
            case ESchemeOpSplitMergeTablePartitions:
                if (scheme.GetSplitMergeTablePartitions().HasTablePath()) {
                    return paths.Root(*scheme.MutableSplitMergeTablePartitions()->MutableTablePath());
                }
                break;
            case ESchemeOpCreateExternalTable:
                if (scheme.GetCreateExternalTable().HasDataSourcePath()) {
                    return paths.Root(*scheme.MutableCreateExternalTable()->MutableDataSourcePath());
                }
                break;
            case ESchemeOpCreateReplication:
            case ESchemeOpAlterReplication:
            case ESchemeOpCreateTransfer:
            case ESchemeOpAlterTransfer: {
                if (!scheme.GetReplication().HasConfig()) {
                    break;
                }
                auto* config = scheme.MutableReplication()->MutableConfig();
                if (config->HasSpecific()) {
                    for (auto& target : *config->MutableSpecific()->MutableTargets()) {
                        if (target.HasDstPath()) {
                            if (auto status = paths.Root(*target.MutableDstPath()); status.IsFail()) {
                                return status;
                            }
                        }
                    }
                }
                if (config->HasTransferSpecific() && config->GetTransferSpecific().HasTarget()) {
                    auto* target = config->MutableTransferSpecific()->MutableTarget();
                    if (target->HasDstPath()) {
                        if (auto status = paths.Root(*target->MutableDstPath()); status.IsFail()) {
                            return status;
                        }
                    }
                    if (target->HasDirectoryPath()) {
                        return paths.Root(*target->MutableDirectoryPath());
                    }
                }
                break;
            }
            case ESchemeOpRestoreMultipleIncrementalBackups: {
                if (!scheme.HasRestoreMultipleIncrementalBackups()) {
                    break;
                }
                auto* restore = scheme.MutableRestoreMultipleIncrementalBackups();
                for (auto& source : *restore->MutableSrcTablePaths()) {
                    if (auto status = paths.Root(source); status.IsFail()) {
                        return status;
                    }
                }
                if (restore->HasDstTablePath()) {
                    return paths.Root(*restore->MutableDstTablePath());
                }
                break;
            }
            case ESchemeOpCreateBackupCollection:
                return scheme.HasCreateBackupCollection()
                           ? paths.Collection(*scheme.MutableCreateBackupCollection())
                           : TConclusionStatus::Success();
            case ESchemeOpAlterBackupCollection:
                return scheme.HasAlterBackupCollection()
                           ? paths.Collection(*scheme.MutableAlterBackupCollection())
                           : TConclusionStatus::Success();
            default:
                break;
        }
        return TConclusionStatus::Success();
    }

} // namespace NKikimr::NMsgBusProxy
