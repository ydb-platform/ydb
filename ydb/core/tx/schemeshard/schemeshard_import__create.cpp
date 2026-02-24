#include "schemeshard_audit_log.h"
#include "schemeshard_impl.h"
#include "schemeshard_index_build_info.h"
#include "schemeshard_import.h"
#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_import_getters.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_import_scheme_query_executor.h"
#include "schemeshard_xxport__helpers.h"
#include "schemeshard_xxport__tx_base.h"

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/lib/ydb_cli/dump/files/files.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_data_source_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/external_table_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/replication_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/view_utils.h>
#include <ydb/public/lib/ydb_cli/dump/util/util.h>

#include <ydb/core/tx/schemeshard/schemeshard_path_describer.h>
#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

void TSchemeShard::EraseEncryptionKey(NIceDb::TNiceDb& db, TImportInfo& importInfo) {
    if (importInfo.EraseEncryptionKey()) {
        PersistImportSettings(db, importInfo);
    }
}

namespace {

using TItem = TImportInfo::TItem;
using EState = TImportInfo::EState;

THashMap<EState, int> CountItemsByState(const TVector<TItem>& items) {
    THashMap<EState, int> counter;
    for (const auto& item : items) {
        counter[item.State]++;
    }
    return counter;
}

bool AllDone(const THashMap<EState, int>& stateCounts) {
    return AllOf(stateCounts, [](const auto& stateCount) { return stateCount.first == EState::Done; });
}

bool AllDoneOrWaiting(const THashMap<EState, int>& stateCounts) {
    return AllOf(stateCounts, [](const auto& stateCount) {
        return stateCount.first == EState::Done
            || stateCount.first == EState::Waiting;
    });
}

// the item is to be created by query, i.e. it is not a table
bool IsCreatedByQuery(const TItem& item) {
    return !item.CreationQuery.empty();
}

bool IsCreateViewQuery(const TString& query) {
    return query.Contains("CREATE VIEW");
}

bool IsCreateReplicationQuery(const TString& query) {
    return query.Contains("CREATE ASYNC REPLICATION");
}

bool IsCreateTransferQuery(const TString& query) {
    return query.Contains("CREATE TRANSFER");
}

bool IsCreateExternalDataSourceQuery(const TString& query) {
    return query.Contains("CREATE EXTERNAL DATA SOURCE");
}

bool IsCreateExternalTableQuery(const TString& query) {
    return query.Contains("CREATE EXTERNAL TABLE");
}

bool RewriteCreateQuery(
    TString& query,
    const TString& dbRestoreRoot,
    const TString& dbPath,
    NYql::TIssues& issues)
{
    if (IsCreateViewQuery(query)) {
        return NYdb::NDump::RewriteCreateViewQuery(query, dbRestoreRoot, true, dbPath, issues);
    } else if (IsCreateReplicationQuery(query)) {
        return NYdb::NDump::RewriteCreateAsyncReplicationQuery(query, dbRestoreRoot, dbPath, issues);
    } else if (IsCreateTransferQuery(query)) {
        return NYdb::NDump::RewriteCreateTransferQuery(query, dbRestoreRoot, dbPath, issues);
    } else if (IsCreateExternalDataSourceQuery(query)) {
        return NYdb::NDump::RewriteCreateExternalDataSourceQuery(query, dbRestoreRoot, dbPath, issues);
    } else if (IsCreateExternalTableQuery(query)) {
        return NYdb::NDump::RewriteCreateExternalTableQuery(query, dbRestoreRoot, dbPath, issues);
    }

    issues.AddIssue(TStringBuilder() << "unsupported create query: " << query);
    return false;
}

TString GetDatabase(TSchemeShard& ss) {
    return CanonizePath(ss.RootPathElements);
}

bool ShouldDelayQueryExecutionOnError(
    const NKikimrSchemeOp::TModifyScheme& preparedQuery,
    NKikimrScheme::EStatus status)
{
    switch (preparedQuery.GetOperationType()) {
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateTransfer:
            return status == NKikimrScheme::StatusNotAvailable;
        case NKikimrSchemeOp::EOperationType::ESchemeOpCreateExternalTable:
            return IsIn({
                NKikimrScheme::StatusPathDoesNotExist,
                NKikimrScheme::StatusSchemeError,
            }, status);
        default:
            return false;
    }
}

}

bool ValidateImportDstPath(const TString& dstPath, TSchemeShard* ss, TString& explain) {
    const TPath path = TPath::Resolve(dstPath, ss);
    TPath::TChecker checks = path.Check();
    checks
        .IsAtLocalSchemeShard()
        .HasResolvedPrefix()
        .FailOnRestrictedCreateInTempZone();

    if (path.IsResolved()) {
        if (path->IsSysView()) {
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting();
        } else {
            checks
                .IsResolved()
                .IsDeleted();
        }
    } else {
        checks
            .NotEmpty()
            .NotResolved();
    }

    if (checks) {
        checks
            //NOTE: using TSystemUsers::Metadata() here allows restoration of paths with system-reserved names/prefixes.
            // Reason is, that these names aren't being created anew but were legitimate elsewhere or
            // at some other point in time, blocking them now would create unnecessary problems.
            .IsValidLeafName(&NACLib::TSystemUsers::Metadata())
            .DepthLimit()
            .PathsLimit();

        if (path.Parent().IsResolved()) {
            checks.DirChildrenLimit();
        }
    }

    if (!checks) {
        explain = checks.GetError();
        return false;
    }
    return true;
}

struct TSchemeShard::TImport::TTxCreate: public TSchemeShard::TXxport::TTxBase {
    TEvImport::TEvCreateImportRequest::TPtr Request;
    bool Progress;

    explicit TTxCreate(TSelf* self, TEvImport::TEvCreateImportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
        , Progress(false)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_IMPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        LOG_D("TImport::TTxCreate: DoExecute");
        LOG_T("Message:\n" << request.ShortDebugString());

        auto response = MakeHolder<TEvImport::TEvCreateImportResponse>(request.GetTxId());

        const ui64 id = request.GetTxId();
        if (Self->Imports.contains(id)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::ALREADY_EXISTS,
                TStringBuilder() << "Import with id '" << id << "' already exists"
            );
        }

        const TString& uid = GetUid(request.GetRequest().GetOperationParams());
        if (uid) {
            if (auto it = Self->ImportsByUid.find(uid); it != Self->ImportsByUid.end()) {
                if (IsSameDomain(it->second, request.GetDatabaseName())) {
                    Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), *it->second);
                    return Reply(std::move(response));
                } else {
                    return Reply(
                        std::move(response),
                        Ydb::StatusIds::ALREADY_EXISTS,
                        TStringBuilder() << "Import with uid '" << uid << "' already exists"
                    );
                }
            }
        }

        const TPath domainPath = TPath::Resolve(request.GetDatabaseName(), Self);
        {
            TPath::TChecker checks = domainPath.Check();
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsCommonSensePath()
                .IsLikeDirectory();

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, checks.GetError());
            }

            if (!request.HasUserSID() || !Self->SystemBackupSIDs.contains(request.GetUserSID())) {
                checks.ImportsLimit();
            }

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::PRECONDITION_FAILED, checks.GetError());
            }
        }

        TImportInfo::TPtr importInfo = nullptr;
        TImportInfo::EState initialState = TImportInfo::EState::Waiting;

        switch (request.GetRequest().GetSettingsCase()) {
        case NKikimrImport::TCreateImportRequest::kImportFromS3Settings:
            {
                auto settings = request.GetRequest().GetImportFromS3Settings();
                if (!settings.scheme()) {
                    settings.set_scheme(Ydb::Import::ImportFromS3Settings::HTTPS);
                }

                if (!settings.source_prefix().empty() && AppData()->FeatureFlags.GetEnableEncryptedExport()) {
                    initialState = TImportInfo::EState::DownloadExportMetadata;
                }

                if (!AppData()->FeatureFlags.GetEnableIndexMaterialization()) {
                    switch (settings.index_population_mode()) {
                    case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_IMPORT:
                    case Ydb::Import::ImportFromS3Settings::INDEX_POPULATION_MODE_AUTO:
                        return Reply(
                            std::move(response),
                            Ydb::StatusIds::PRECONDITION_FAILED,
                            "Index materialization is not enabled"
                        );
                    default:
                        break;
                    }
                }

                importInfo = new TImportInfo(id, uid, TImportInfo::EKind::S3, settings, domainPath.Base()->PathId, request.GetPeerName());

                if (request.HasUserSID()) {
                    importInfo->UserSID = request.GetUserSID();
                }

                TString explain;
                if (!FillItems(*importInfo, settings, explain)) {
                    return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, explain);
                }
            }
            break;

        case NKikimrImport::TCreateImportRequest::kImportFromFsSettings:
            {
                if (!AppData()->FeatureFlags.GetEnableFsBackups()) {
                    return Reply(std::move(response), Ydb::StatusIds::UNSUPPORTED, "The feature flag \"EnableFsBackups\" is disabled. The operation cannot be performed.");
                }

                if (AppData()->FeatureFlags.GetEnableEncryptedExport()) {
                    initialState = TImportInfo::EState::DownloadExportMetadata;
                }

                const auto& settings = request.GetRequest().GetImportFromFsSettings();

                importInfo = new TImportInfo(id, uid, TImportInfo::EKind::FS, settings, domainPath.Base()->PathId, request.GetPeerName());

                if (request.HasUserSID()) {
                    importInfo->UserSID = request.GetUserSID();
                }

                TString explain;
                if (!FillItems(*importInfo, settings, explain)) {
                    return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, explain);
                }
            }
            break;

        default:
            Y_DEBUG_ABORT("Unknown import kind");
        }

        Y_ABORT_UNLESS(importInfo != nullptr);

        importInfo->SanitizedToken = request.GetSanitizedToken();

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistCreateImport(db, *importInfo);

        importInfo->State = initialState;
        importInfo->StartTime = TAppData::TimeProvider->Now();
        Self->PersistImportState(db, *importInfo);

        Self->AddImport(importInfo);
        Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), *importInfo);

        Progress = true;
        return Reply(std::move(response));
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_D("TImport::TTxCreate: DoComplete");

        if (Progress) {
            const ui64 id = Request->Get()->Record.GetTxId();
            Self->Execute(Self->CreateTxProgressImport(id), ctx);
        }
    }

private:
    bool Reply(
        THolder<TEvImport::TEvCreateImportResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString()
    ) {
        LOG_D("TImport::TTxCreate: Reply"
            << ": status# " << status
            << ", error# " << errorMessage);
        LOG_T("Message:\n" << response->Record.ShortDebugString());

        auto& entry = *response->Record.MutableResponse()->MutableEntry();
        entry.SetStatus(status);
        if (errorMessage) {
            AddIssue(entry, errorMessage);
        }

        AuditLogImportStart(Request->Get()->Record, response->Record, Self);

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    // Common helper to validate destination path
    bool ValidateAndAddDestinationPath(const TString& dstPath, THashSet<TString>& dstPaths, TString& explain) {
        if (dstPath) {
            if (!dstPaths.insert(NBackup::NormalizeItemPath(dstPath)).second) {
                explain = TStringBuilder() << "Duplicate destination_path: " << dstPath;
                return false;
            }

            if (!ValidateImportDstPath(dstPath, Self, explain)) {
                return false;
            }
        }
        return true;
    }

    // S3-FS-specific FillItems
    template <typename TSettings>
    bool FillItems(TImportInfo& importInfo, const TSettings& settings, TString& explain) {
        THashSet<TString> dstPaths;

        if (!importInfo.CompileExcludeRegexps(explain)) {
            return false;
        }

        importInfo.Items.reserve(settings.items().size());
        for (ui32 itemIdx : xrange(settings.items().size())) {
            const TString& dstPath = settings.items(itemIdx).destination_path();

            if (!ValidateAndAddDestinationPath(dstPath, dstPaths, explain)) {
                return false;
            }

            if (!dstPath && importInfo.GetSource().empty()) {
                // Can not take path from schema mapping
                explain = "No common source prefix and item destination path set";
                return false;
            }

            if (!importInfo.IsExcludedFromImport(dstPath)) {
                auto& item = importInfo.Items.emplace_back(dstPath);
                item.SrcPrefix = NBackup::NormalizeExportPrefix(GetItemSource(settings, itemIdx));
                item.SrcPath = NBackup::NormalizeItemPath(settings.items(itemIdx).source_path());
            }
        }

        if (settings.items().size() && importInfo.Items.empty()) {
            explain = TStringBuilder() << "no items to import";
            return false;
        }

        return true;
    }

}; // TTxCreate

struct TSchemeShard::TImport::TTxProgress: public TSchemeShard::TXxport::TTxBase {
    using EState = TImportInfo::EState;
    using ESubState = TImportInfo::TItem::ESubState;

    static constexpr ui32 IssuesSizeLimit = 2 * 1024;

    ui64 Id;
    TMaybe<ui32> ItemIdx;
    TEvPrivate::TEvImportSchemeReady::TPtr SchemeResult = nullptr;
    TEvPrivate::TEvImportSchemaMappingReady::TPtr SchemaMappingResult = nullptr;
    TEvPrivate::TEvImportSchemeQueryResult::TPtr SchemeQueryResult = nullptr;
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult = nullptr;
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult = nullptr;
    TEvIndexBuilder::TEvCreateResponse::TPtr CreateIndexResult = nullptr;
    TTxId CompletedTxId = InvalidTxId;

    explicit TTxProgress(TSelf* self, ui64 id, const TMaybe<ui32>& itemIdx)
        : TXxport::TTxBase(self)
        , Id(id)
        , ItemIdx(itemIdx)
    {
    }

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvImportSchemeReady::TPtr& ev)
        : TXxport::TTxBase(self)
        , SchemeResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvImportSchemaMappingReady::TPtr& ev)
        : TXxport::TTxBase(self)
        , Id(ev->Get()->ImportId)
        , SchemaMappingResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvPrivate::TEvImportSchemeQueryResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , SchemeQueryResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , AllocateResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev)
        : TXxport::TTxBase(self)
        , ModifyResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TEvIndexBuilder::TEvCreateResponse::TPtr& ev)
        : TXxport::TTxBase(self)
        , CreateIndexResult(ev)
    {
    }

    explicit TTxProgress(TSelf* self, TTxId completedTxId)
        : TXxport::TTxBase(self)
        , CompletedTxId(completedTxId)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_IMPORT_PROGRESS;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_D("TImport::TTxProgress: DoExecute");

        if (SchemeResult) {
            OnSchemeResult(txc, ctx);
        } else if (SchemaMappingResult) {
            OnSchemaMappingResult(txc, ctx);
        } else if (SchemeQueryResult) {
            OnSchemeQueryPreparation(txc, ctx);
        } else if (AllocateResult) {
            OnAllocateResult(txc, ctx);
        } else if (ModifyResult) {
            OnModifyResult(txc, ctx);
        } else if (CreateIndexResult) {
            OnCreateIndexResult(txc, ctx);
        } else if (CompletedTxId) {
            OnNotifyResult(txc, ctx);
        } else {
            Resume(txc, ctx);
        }

        return true;
    }

    void DoComplete(const TActorContext&) override {
        LOG_D("TImport::TTxProgress: DoComplete");
    }

private:
    void GetScheme(TImportInfo::TPtr importInfo, ui32 itemIdx, const TActorContext& ctx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        LOG_I("TImport::TTxProgress: Get scheme"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        item.SchemeGetter = ctx.RegisterWithSameMailbox(CreateSchemeGetter(Self->SelfId(), importInfo, itemIdx, item.ExportItemIV));
        Self->RunningImportSchemeGetters.emplace(item.SchemeGetter);
    }

    void GetSchemaMapping(TImportInfo::TPtr importInfo, const TActorContext& ctx) {
        LOG_I("TImport::TTxProgress: Download schema mapping"
            << ": info# " << importInfo->ToString());

        importInfo->SchemaMappingGetter = ctx.RegisterWithSameMailbox(CreateSchemaMappingGetter(Self->SelfId(), importInfo));
        Self->RunningImportSchemeGetters.emplace(importInfo->SchemaMappingGetter);
    }

    void CreateTable(TImportInfo& importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        Y_ABORT_UNLESS(item.Table);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: CreateTable propose"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateTablePropose(Self, txId, importInfo, itemIdx);
        Y_ABORT_UNLESS(propose);

        Send(Self->SelfId(), std::move(propose));
    }

    void ProcessSysViewRestore(TTransactionContext& txc, TImportInfo::TPtr importInfo, ui32 itemIdx, const TActorContext& ctx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);
        Y_ABORT_UNLESS(item.SysView);

        NIceDb::TNiceDb db(txc.DB);
        NYql::TIssues issues;

        LOG_I("TImport::TTxProgress: ProcessSysViewRestore"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
        );

        const auto ev = DescribePath(Self, ctx, item.DstPathName);
        const auto& describeResult = ev->GetRecord();
        const auto status = describeResult.GetStatus();

        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            LOG_I("TImport::TTxProgress: ProcessSysViewRestore"
                << ", item# " << item.ToString(itemIdx)
                << ": system view does not exist"
            );

            item.State = EState::Done;
            return;
        } else if (status == NKikimrScheme::StatusSuccess) {
            Ydb::Table::DescribeSystemViewResult describeSysViewResult;
            Ydb::StatusIds_StatusCode status;
            TString error;

            const auto& pathDescription = describeResult.GetPathDescription();
            if (!FillSysViewDescription(describeSysViewResult, pathDescription, status, error)) {
                LOG_I("TImport::TTxProgress: ProcessSysViewRestore"
                    << ", item# " << item.ToString(itemIdx)
                    << ": path is not a system view"
                );

                item.State = EState::Done;
                return;
            }

            const auto compatibilityStatus = NYdb::NDump::CheckSysViewCompatibility(*item.SysView, describeSysViewResult);
            if (!compatibilityStatus.IsSuccess()) {
                LOG_E("TImport::TTxProgress: ProcessSysViewRestore"
                    << ", item# " << item.ToString(itemIdx)
                    << ": system view compatibility check failed"
                );

                return CancelAndPersist(db, importInfo, itemIdx, compatibilityStatus.GetIssues().ToString(),
                    "sysview compatibility check failed");
            } else {
                if (item.Permissions.Empty()) {
                    item.State = EState::Done;
                    return;
                } else {
                    LOG_I("TImport::TTxProgress: ProcessSysViewRestore"
                        << ", item# " << item.ToString(itemIdx)
                        << ": needs to restore ACL"
                    );

                    AllocateTxId(*importInfo, itemIdx);
                    return;
                }
            }
        } else {
            issues.AddIssue(TStringBuilder() << "can't get path'" << item.DstPathName << "' description");
            return CancelAndPersist(db, importInfo, itemIdx, issues.ToString(), "invalid describe path status");
        }
    }

    bool ReplaceSysViewACL(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        Y_ABORT_UNLESS(item.SysView);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: RestoreSysViewPermissions propose"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto path = TPath::Resolve(item.DstPathName, Self);
        Y_ABORT_UNLESS(path);

        // Only restore permissions, don't create the system view itself
        auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), Self->TabletID());
        auto& record = propose->Record;

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme.SetWorkingDir(path.Parent().PathString());
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpModifyACL);

        auto& op = *modifyScheme.MutableModifyACL();
        op.SetName(path.Base()->Name);

        if (!FillACL(modifyScheme, item.Permissions, error)) {
            return false;
        }

        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    bool CreateTopic(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: CreateTopic propose"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateTopicPropose(Self, txId, importInfo, itemIdx, error);
        if (!propose) {
            return false;
        }

        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    void ExecutePreparedQuery(TTransactionContext& txc, TImportInfo::TPtr importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items[itemIdx];

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: ExecutePreparedQuery"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId
        );

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(txId), Self->TabletID());
        auto& record = propose->Record;

        auto& modifyScheme = *record.AddTransaction();
        modifyScheme = *item.PreparedCreationQuery;
        modifyScheme.SetInternal(true);

        if (importInfo->UserSID) {
            record.SetOwner(*importInfo->UserSID);
        }
        FillOwner(record, item.Permissions);

        if (TString error; !FillACL(modifyScheme, item.Permissions, error)) {
            NIceDb::TNiceDb db(txc.DB);
            return CancelAndPersist(db, importInfo, itemIdx, error, "cannot parse permissions");
        }

        Send(Self->SelfId(), std::move(propose));
    }

    void DelayObjectCreation(
        TImportInfo::TPtr importInfo,
        ui32 itemIdx,
        NIceDb::TNiceDb& db,
        const TString& error,
        const TActorContext& ctx)
    {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        LOG_D("TImport::TTxProgress: delay scheme object query execution"
            << ": id# " << importInfo->Id
            << ", delayed item# " << itemIdx);

        item.State = EState::Waiting;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDoneOrWaiting(stateCounts)) {
            if (stateCounts.at(EState::Waiting) == importInfo->WaitingSchemeObjects) {
                // No progress has been made since the last scheme object creation retry.
                return CancelAndPersist(db, importInfo, itemIdx, error, "creation query failed, no progress");
            }
            RetrySchemeObjectsQueryExecution(*importInfo, db, ctx);
        }
    }

    void RetrySchemeObjectsQueryExecution(TImportInfo& importInfo, NIceDb::TNiceDb& db, const TActorContext& ctx) {
        const auto database = GetDatabase(*Self);
        TVector<ui32> retriedItems;
        for (ui32 itemIdx : xrange(importInfo.Items.size())) {
            auto& item = importInfo.Items[itemIdx];
            if (item.State != EState::Waiting || !IsCreatedByQuery(item)) {
                continue;
            }
            if (item.PreparedCreationQuery) {
                AllocateTxId(importInfo, itemIdx);
            } else {
                item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
                    Self->SelfId(), importInfo.Id, itemIdx, item.CreationQuery, database
                ));
                Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
            }

            item.State = EState::CreateSchemeObject;
            Self->PersistImportItemState(db, importInfo, itemIdx);

            retriedItems.emplace_back(itemIdx);
        }
        if (!retriedItems.empty()) {
            importInfo.WaitingSchemeObjects = std::ssize(retriedItems);
            LOG_D("TImport::TTxProgress: retry scheme object query execution"
                << ": id# " << importInfo.Id
                << ", retried items# " << JoinSeq(", ", retriedItems)
            );
        }
    }

    void TransferData(TImportInfo& importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: Restore propose"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->SelfId(), RestoreTableDataPropose(Self, txId, importInfo, itemIdx));
    }

    bool CancelTransferring(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                importInfo.State = EState::Cancellation;
            }

            return false;
        }

        importInfo.State = EState::Cancellation;

        LOG_I("TImport::TTxProgress: cancel restore's tx"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx));

        Send(Self->SelfId(), CancelRestoreTableDataPropose(importInfo, item.WaitTxId), 0, importInfo.Id);
        return true;
    }

    void BuildIndex(TImportInfo& importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: build index"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->SelfId(), BuildIndexPropose(Self, txId, importInfo, itemIdx, MakeIndexBuildUid(importInfo, itemIdx)));
    }

    bool CancelIndexBuilding(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                importInfo.State = EState::Cancellation;
            }

            return false;
        }

        importInfo.State = EState::Cancellation;

        LOG_I("TImport::TTxProgress: cancel index building"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx));

        Send(Self->SelfId(), CancelIndexBuildPropose(Self, importInfo, item.WaitTxId), 0, importInfo.Id);
        return true;
    }

    bool CreateChangefeed(TImportInfo& importInfo, ui32 itemIdx, TTxId txId, TString& error) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: CreateChangefeed propose"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateChangefeedPropose(Self, txId, importInfo, item, error);
        if (!propose) {
            return false;
        }

        Send(Self->SelfId(), std::move(propose));
        return true;
    }

    void CreateConsumers(TImportInfo& importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);
        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: CreateConsumers propose"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        Send(Self->SelfId(), CreateConsumersPropose(Self, txId, importInfo, item));
    }

    void AllocateTxId(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::AllocateTxId;

        LOG_I("TImport::TTxProgress: Allocate txId"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx));

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(), 0, importInfo.Id);
    }

    void SubscribeTx(TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        item.SubState = ESubState::Subscribed;

        LOG_I("TImport::TTxProgress: Wait for completion"
            << ": info# " << importInfo.ToString()
            << ", item# " << item.ToString(itemIdx));

        Y_ABORT_UNLESS(item.WaitTxId != InvalidTxId);
        Send(Self->SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(ui64(item.WaitTxId)));
    }

    TTxId GetActiveRestoreTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::Transferring);
        Y_ABORT_UNLESS(item.DstPathId);

        if (!Self->PathsById.contains(item.DstPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.DstPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateRestore) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    TTxId GetActiveBuildIndexId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::BuildIndexes);

        const auto uid = MakeIndexBuildUid(importInfo, itemIdx);
        const auto* infoPtr = Self->IndexBuildsByUid.FindPtr(uid);
        if (!infoPtr) {
            return InvalidTxId;
        }

        return TTxId(ui64((*infoPtr)->Id));
    }

    TTxId GetActiveCreateChangefeedTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::CreateChangefeed);
        Y_ABORT_UNLESS(item.DstPathId);

        if (!Self->PathsById.contains(item.DstPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.DstPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateAlter) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    TTxId GetActiveCreateConsumerTxId(const TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        const auto& item = importInfo.Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::CreateChangefeed);
        Y_ABORT_UNLESS(item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateConsumers);
        Y_ABORT_UNLESS(item.StreamImplPathId);

        if (!Self->PathsById.contains(item.StreamImplPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.StreamImplPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateAlter) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    void KillChildActors(TImportInfo::TItem& item) {
        if (auto schemeGetter = std::exchange(item.SchemeGetter, {})) {
            Send(schemeGetter, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeGetters.erase(schemeGetter);
        }
        if (auto schemeQueryExecutor = std::exchange(item.SchemeQueryExecutor, {})) {
            Send(schemeQueryExecutor, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeQueryExecutors.erase(schemeQueryExecutor);
        }
    }

    void Cancel(TImportInfo& importInfo, ui32 itemIdx, TStringBuf marker) {
        const TItem* item = nullptr;
        if (itemIdx != ui32(-1)) {
            Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
            item = &importInfo.Items.at(itemIdx);
        }

        TStringBuilder itemLogStr;
        if (item) {
            itemLogStr << ", item# " << item->ToString(itemIdx);
        }
        LOG_N("TImport::TTxProgress: " << marker << ", cancelling"
            << ", info# " << importInfo.ToString()
            << itemLogStr);

        importInfo.State = EState::Cancelled;

        if (auto schemaMappingGetter = std::exchange(importInfo.SchemaMappingGetter, {})) {
            Send(schemaMappingGetter, new TEvents::TEvPoisonPill());
            Self->RunningImportSchemeGetters.erase(schemaMappingGetter);
        }

        for (ui32 i : xrange(importInfo.Items.size())) {
            KillChildActors(importInfo.Items[i]);
            if (i == itemIdx) {
                continue;
            }

            switch (importInfo.Items.at(i).State) {
            case EState::Transferring:
                CancelTransferring(importInfo, i);
                break;

            case EState::BuildIndexes:
                CancelIndexBuilding(importInfo, i);
                break;

            default:
                break;
            }
        }

        if (importInfo.State == EState::Cancelled) {
            importInfo.EndTime = TAppData::TimeProvider->Now();
        }
    }

    void CancelAndPersist(NIceDb::TNiceDb& db, TImportInfo::TPtr importInfo, ui32 itemIdx, TStringBuf itemIssue, TStringBuf marker) {
        if (itemIdx != ui32(-1)) {
            Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
            auto& item = importInfo->Items[itemIdx];

            item.Issue = itemIssue;
            PersistImportItemState(db, *importInfo, itemIdx);

            if (importInfo->State != EState::Waiting) {
                return;
            }
        }

        Cancel(*importInfo, itemIdx, marker);
        PersistImportState(db, *importInfo);
        EraseEncryptionKey(db, *importInfo);

        SendNotificationsIfFinished(importInfo);
    }

    TMaybe<TString> GetIssues(const TImportInfo::TItem& item, TTxId restoreTxId) {
        if (item.Table->store_type() == Ydb::Table::STORE_TYPE_COLUMN) {
            Y_ABORT_UNLESS(Self->ColumnTables.contains(item.DstPathId));
            TColumnTableInfo::TPtr table = Self->ColumnTables.at(item.DstPathId).GetPtr();
            return GetIssues(table, restoreTxId);
        } else {
            Y_ABORT_UNLESS(Self->Tables.contains(item.DstPathId));
            TTableInfo::TPtr table = Self->Tables.at(item.DstPathId);
            return GetIssues(table, restoreTxId);
        }
    }

    template <typename TTable>
    TMaybe<TString> GetIssues(const TTable& table, TTxId restoreTxId) {
        Y_ABORT_UNLESS(table->RestoreHistory.contains(restoreTxId));
        const auto& result = table->RestoreHistory.at(restoreTxId);

        if (result.TotalShardCount == result.SuccessShardCount) {
            return Nothing();
        }

        TStringBuilder output;
        bool first = true;

        for (const auto& [shardId, status] : result.ShardStatuses) {
            if (status.Success) {
                continue;
            }

            if (output.size() > IssuesSizeLimit) {
                output << "... <truncated>";
                break;
            }

            if (!first) {
                output << ", ";
            }
            first = false;

            output << "shard: " << shardId << ", error: " << status.Error;
        }

        return output;
    }

    TMaybe<TString> GetIssues(TIndexBuildId indexBuildId) {
        const auto* indexInfoPtr = Self->IndexBuilds.FindPtr(indexBuildId);
        Y_ABORT_UNLESS(indexInfoPtr);
        const auto& indexInfo = *indexInfoPtr->get();

        if (indexInfo.IsDone()) {
            return Nothing();
        }

        return indexInfo.GetIssue();
    }

    TString GetIssues(const NKikimrIndexBuilder::TEvCreateResponse& proto) {
        TStringBuilder output;

        for (const auto& issue : proto.GetIssues()) {
            output << issue.message() << Endl;
        }

        return output;
    }

    void Resume(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(Self->Imports.contains(Id));
        TImportInfo::TPtr importInfo = Self->Imports.at(Id);

        LOG_D("TImport::TTxProgress: Resume"
            << ": id# " << Id
            << ", itemIdx# " << ItemIdx);

        switch (importInfo->State) {
            case EState::DownloadExportMetadata: {
                GetSchemaMapping(importInfo, ctx);
                break;
            }
            default: {
                if (ItemIdx) {
                    Resume(importInfo, *ItemIdx, txc, ctx);
                } else {
                    for (ui32 itemIdx : xrange(importInfo->Items.size())) {
                        Resume(importInfo, itemIdx, txc, ctx);
                    }
                }
                break;
            }
        }
    }

    void Resume(TImportInfo::TPtr importInfo, ui32 itemIdx, TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        LOG_D("TImport::TTxProgress: Resume"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        NIceDb::TNiceDb db(txc.DB);

        switch (importInfo->State) {
            case EState::Waiting: {
                switch (item.State) {
                case EState::GetScheme:
                    if (!Self->TableProfilesLoaded) {
                        Self->WaitForTableProfiles(Id, itemIdx);
                    } else {
                        GetScheme(importInfo, itemIdx, ctx);
                    }
                    break;

                case EState::CreateSchemeObject:
                case EState::Transferring:
                case EState::BuildIndexes:
                case EState::CreateChangefeed:
                    if (item.WaitTxId == InvalidTxId) {
                        if (!IsCreatedByQuery(item) || item.PreparedCreationQuery) {
                            AllocateTxId(*importInfo, itemIdx);
                        } else {
                            const auto database = GetDatabase(*Self);
                            item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
                                Self->SelfId(), importInfo->Id, itemIdx, item.CreationQuery, database
                            ));
                            Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
                        }
                    } else {
                        SubscribeTx(*importInfo, itemIdx);
                    }
                    break;

                default:
                    break;
                }
            }
            break;

            case EState::Cancellation: {
                TTxId txId = InvalidTxId;

                switch (item.State) {
                case EState::Transferring:
                    if (!CancelTransferring(*importInfo, itemIdx)) {
                        txId = GetActiveRestoreTxId(*importInfo, itemIdx);
                    }
                    break;

                case EState::BuildIndexes:
                    if (!CancelIndexBuilding(*importInfo, itemIdx)) {
                        txId = GetActiveBuildIndexId(*importInfo, itemIdx);
                    }
                    break;

                default:
                    break;
                }

                if (txId != InvalidTxId) {
                    item.WaitTxId = txId;
                    Self->PersistImportItemState(db, *importInfo, itemIdx);

                    switch (item.State) {
                    case EState::Transferring:
                        CancelTransferring(*importInfo, itemIdx);
                        break;

                    case EState::BuildIndexes:
                        CancelIndexBuilding(*importInfo, itemIdx);
                        break;

                    default:
                        break;
                    }
                }
            }
            break;

        default:
            break;
        }
    }

    void OnSchemeResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(SchemeResult);

        const auto& msg = *SchemeResult->Get();

        LOG_D("TImport::TTxProgress: OnSchemeResult"
            << ": id# " << msg.ImportId
            << ", itemIdx# " << msg.ItemIdx
            << ", success# " << msg.Success
        );

        if (!Self->Imports.contains(msg.ImportId)) {
            LOG_E("TImport::TTxProgress: OnSchemeResult received unknown id"
                << ": id# " << msg.ImportId);
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(msg.ImportId);
        if (msg.ItemIdx >= importInfo->Items.size()) {
            LOG_E("TImport::TTxProgress: OnSchemeResult received unknown item"
                << ": id# " << msg.ImportId
                << ", item# " << msg.ItemIdx);
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto& item = importInfo->Items.at(msg.ItemIdx);
        Self->RunningImportSchemeGetters.erase(std::exchange(item.SchemeGetter, {}));

        if (!msg.Success) {
            return CancelAndPersist(db, importInfo, msg.ItemIdx, msg.Error, "cannot get scheme");
        }

        if (IsCreatedByQuery(item)) {
            // Send the creation query to KQP to prepare.
            const auto database = GetDatabase(*Self);
            const TString source = TStringBuilder() << item.SrcPath;

            NYql::TIssues issues;
            if (!RewriteCreateQuery(item.CreationQuery, database, item.DstPathName, issues)) {
                issues.AddIssue(TStringBuilder() << "path: " << source);
                return CancelAndPersist(db, importInfo, msg.ItemIdx, issues.ToString(), "invalid creation query");
            }

            item.SchemeQueryExecutor = ctx.Register(CreateSchemeQueryExecutor(
                Self->SelfId(), msg.ImportId, msg.ItemIdx, item.CreationQuery, database
            ));
            Self->RunningImportSchemeQueryExecutors.emplace(item.SchemeQueryExecutor);
        } else if (item.Table) {
            TString error;
            if (!CreateTablePropose(Self, TTxId(), *importInfo, msg.ItemIdx, error)) {
                return CancelAndPersist(db, importInfo, msg.ItemIdx, error, "invalid table scheme");
            }
        }

        if (!IsCreatedByQuery(item)) {
            if (item.SysView) {
                ProcessSysViewRestore(txc, importInfo, msg.ItemIdx, ctx);
            } else {
                AllocateTxId(*importInfo, msg.ItemIdx);
            }
        }

        Self->PersistImportItemScheme(db, *importInfo, msg.ItemIdx);

        if (item.State != EState::Done) {
            item.State = EState::CreateSchemeObject;
        }

        Self->PersistImportItemState(db, *importInfo, msg.ItemIdx);

        const TString parentSrc = importInfo->GetItemSrcPrefix(msg.ItemIdx);
        const TString parentDst = item.DstPathName;
        auto materializedIndexes = std::move(item.MaterializedIndexes);
        item.ChildItems.reserve(materializedIndexes.size());
        importInfo->Items.reserve(importInfo->Items.size() + materializedIndexes.size());

        for (auto& [indexMetadata, scheme] : materializedIndexes) {
            auto src = TStringBuilder() << parentSrc << "/" << indexMetadata.ExportPrefix;
            auto dst = TStringBuilder() << parentDst << "/" << indexMetadata.ImplTablePrefix;

            auto& childItem = importInfo->Items.emplace_back(std::move(dst));
            const ui32 childIdx = importInfo->Items.size() - 1;

            importInfo->Items.at(msg.ItemIdx).ChildItems.push_back(childIdx);

            childItem.SrcPrefix = std::move(src);
            childItem.State = EState::Waiting;
            childItem.ParentIdx = msg.ItemIdx;
            childItem.Table = std::move(scheme);

            Self->PersistNewImportItem(db, *importInfo, childIdx);
            Self->PersistImportItemScheme(db, *importInfo, childIdx);
        }

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDone(stateCounts)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        }

        Self->PersistImportState(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }
    }

    void OnSchemaMappingResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(SchemaMappingResult);

        const auto& msg = *SchemaMappingResult->Get();

        LOG_D("TImport::TTxProgress: OnSchemaMappingResult"
            << ": id# " << msg.ImportId
            << ", success# " << msg.Success
        );

        if (!Self->Imports.contains(msg.ImportId)) {
            LOG_E("TImport::TTxProgress: OnSchemaMappingResult received unknown id"
                << ": id# " << msg.ImportId);
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(msg.ImportId);

        NIceDb::TNiceDb db(txc.DB);

        Self->RunningImportSchemeGetters.erase(std::exchange(importInfo->SchemaMappingGetter, {}));

        if (!msg.Success) {
            return CancelAndPersist(db, importInfo, -1, {}, TStringBuilder() << "cannot get schema mapping: " << msg.Error);
        }

        if (!importInfo->SchemaMapping->Items.empty()) {
            if (importInfo->GetEncryptedBackup() != importInfo->SchemaMapping->Items[0].IV.Defined()) {
                return CancelAndPersist(db, importInfo, -1, {}, "incorrect schema mapping");
            }
        }

        const TImportInfo::TFillItemsFromSchemaMappingResult fillResult = importInfo->FillItemsFromSchemaMapping(Self);
        if (!fillResult.Success) {
            return CancelAndPersist(db, importInfo, -1, {}, fillResult.ErrorMessage);
        }

        importInfo->State = EState::Waiting;
        PersistImportState(db, *importInfo);
        PersistSchemaMappingImportFields(db, *importInfo);
        Resume(txc, ctx);
    }

    void OnSchemeQueryPreparation(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(SchemeQueryResult);
        const auto& message = *SchemeQueryResult.Get()->Get();
        const TString error = std::holds_alternative<TString>(message.Result) ? std::get<TString>(message.Result) : "";

        LOG_D("TImport::TTxProgress: OnSchemeQueryPreparation"
            << ": id# " << message.ImportId
            << ", itemIdx# " << message.ItemIdx
            << ", status# " << message.Status
            << ", error# " << error
        );

        auto importInfo = Self->Imports.Value(message.ImportId, nullptr);
        if (!importInfo) {
            LOG_E("TImport::TTxProgress: OnSchemeQueryPreparation received unknown import id"
                << ": id# " << message.ImportId
            );
            return;
        }
        if (message.ItemIdx >= importInfo->Items.size()) {
            LOG_E("TImport::TTxProgress: OnSchemeQueryPreparation item index out of range"
                << ": id# " << message.ImportId
                << ", item index# " << message.ItemIdx
                << ", number of items# " << importInfo->Items.size()
            );
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        auto& item = importInfo->Items[message.ItemIdx];
        Self->RunningImportSchemeQueryExecutors.erase(std::exchange(item.SchemeQueryExecutor, {}));

        if (message.Status == Ydb::StatusIds::SCHEME_ERROR) {
            // Scheme error happens when the creation query depends on other objects that are not yet imported.
            return DelayObjectCreation(importInfo, message.ItemIdx, db, error, ctx);
        }

        if (message.Status != Ydb::StatusIds::SUCCESS || !error.empty()) {
            return CancelAndPersist(db, importInfo, message.ItemIdx, error, "creation query failed");
        }

        if (item.State == EState::CreateSchemeObject) {
            item.PreparedCreationQuery = std::get<NKikimrSchemeOp::TModifyScheme>(message.Result);
            PersistImportItemPreparedCreationQuery(db, *importInfo, message.ItemIdx);
            if (item.PreparedCreationQuery->HasReplication()) {
                // In case async replication or transfer is created, it is essential to execute modify scheme
                // After all other objects, as they do not fail in case their dependencies are not imported yet
                return DelayObjectCreation(importInfo, message.ItemIdx, db, error, ctx);
            }
            AllocateTxId(*importInfo, message.ItemIdx);
        }
    }

    void OnAllocateResult(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(AllocateResult);

        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());
        const ui64 id = AllocateResult->Cookie;

        LOG_D("TImport::TTxProgress: OnAllocateResult"
            << ": txId# " << txId
            << ", id# " << id);

        if (!Self->Imports.contains(id)) {
            LOG_E("TImport::TTxProgress: OnAllocateResult received unknown id"
                << ": id# " << id);
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        if (importInfo->State != EState::Waiting) {
            return;
        }

        TMaybe<ui32> itemIdx;
        for (ui32 i : xrange(importInfo->Items.size())) {
            const auto& item = importInfo->Items.at(i);

            if (item.SubState != ESubState::AllocateTxId) {
                continue;
            }

            switch (item.State) {
            case EState::CreateSchemeObject:
                if (item.PreparedCreationQuery) {
                    ExecutePreparedQuery(txc, importInfo, i, txId);
                    itemIdx = i;
                    break;
                }
                if (item.Topic) {
                    TString error;
                    if (!CreateTopic(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(db, importInfo, i, error, "creation topic failed");
                    }
                    itemIdx = i;
                    break;
                }
                if (item.SysView) {
                    TString error;
                    if (!ReplaceSysViewACL(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(db, importInfo, i, error, "restore sysview permissions failed");
                        return;
                    }
                    itemIdx = i;
                    break;
                }
                if (IsCreatedByQuery(item)) {
                    // We only need a txId for modify scheme transactions.
                    // If an object’s CreationQuery has not been prepared yet, it does not need a txId at this point.
                    break;
                }
                if (!Self->TableProfilesLoaded) {
                    Self->WaitForTableProfiles(id, i);
                } else if (item.Table) {
                    CreateTable(*importInfo, i, txId);
                    itemIdx = i;
                }
                break;

            case EState::Transferring:
                TransferData(*importInfo, i, txId);
                itemIdx = i;
                break;

            case EState::BuildIndexes:
                BuildIndex(*importInfo, i, txId);
                itemIdx = i;
                break;

            case EState::CreateChangefeed:
                if (item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateChangefeed) {
                    TString error;
                    if (!CreateChangefeed(*importInfo, i, txId, error)) {
                        NIceDb::TNiceDb db(txc.DB);
                        CancelAndPersist(db, importInfo, i, error, "creation changefeed failed");
                    }
                } else {
                    CreateConsumers(*importInfo, i, txId);
                }
                itemIdx = i;
                break;

            default:
                break;
            }

            if (itemIdx) {
                break;
            }
        }

        if (!itemIdx) {
            return;
        }

        Y_ABORT_UNLESS(!Self->TxIdToImport.contains(txId));
        Self->TxIdToImport[txId] = {importInfo->Id, *itemIdx};
    }

    void OnModifyResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ModifyResult);
        const auto& record = ModifyResult->Get()->Record;

        LOG_D("TImport::TTxProgress: OnModifyResult"
            << ": txId# " << record.GetTxId()
            << ", status# " << record.GetStatus());
        LOG_T("Message:\n" << record.ShortDebugString());

        auto txId = TTxId(record.GetTxId());
        if (!Self->TxIdToImport.contains(txId)) {
            LOG_E("TImport::TTxProgress: OnModifyResult received unknown txId"
                << ": txId# " << txId);
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToImport.at(txId);
        if (!Self->Imports.contains(id)) {
            LOG_E("TImport::TTxProgress: OnModifyResult received unknown id"
                << ": id# " << id);
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        if (IsCreatedByQuery(item)) {
            // As for created by query objects query must be compiled to execute
            Y_ABORT_UNLESS(item.PreparedCreationQuery);

            if (ShouldDelayQueryExecutionOnError(*item.PreparedCreationQuery, record.GetStatus())) {
                return DelayObjectCreation(importInfo, itemIdx, db, record.GetReason(), ctx);
            }
        }

        if (record.GetStatus() == NKikimrScheme::StatusSuccess) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;
            item.State = EState::Done;
        } else if (record.GetStatus() != NKikimrScheme::StatusAccepted) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;

            if (IsIn({ NKikimrScheme::StatusMultipleModifications, NKikimrScheme::StatusAlreadyExists },
                record.GetStatus()
            )) {
                if (record.GetPathCreateTxId()) {
                    txId = TTxId(record.GetPathCreateTxId());
                } else if (item.State == EState::CreateSchemeObject) {
                    // In case dependency object is being created
                    return DelayObjectCreation(importInfo, itemIdx, db, record.GetReason(), ctx);
                } else if (item.State == EState::Transferring) {
                    txId = GetActiveRestoreTxId(*importInfo, itemIdx);
                } else if (item.State == EState::CreateChangefeed) {
                    if (item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateChangefeed) {
                        txId = GetActiveCreateChangefeedTxId(*importInfo, itemIdx);
                    } else {
                        txId = GetActiveCreateConsumerTxId(*importInfo, itemIdx);
                    }
                }
            }

            if (txId == InvalidTxId) {
                if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists && item.State == EState::CreateChangefeed) {
                    if (item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateChangefeed) {
                        item.ChangefeedState = TImportInfo::TItem::EChangefeedState::CreateConsumers;
                        AllocateTxId(*importInfo, itemIdx);
                    } else if (++item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size()) {
                        item.ChangefeedState = TImportInfo::TItem::EChangefeedState::CreateChangefeed;
                        AllocateTxId(*importInfo, itemIdx);
                    } else {
                        item.State = EState::Done;
                    }
                    return;
                }

                return CancelAndPersist(db, importInfo, itemIdx, record.GetReason(), "unhappy propose");
            }

            Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
        }

        item.WaitTxId = txId;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        if (importInfo->State != EState::Waiting && item.State == EState::Transferring) {
            CancelTransferring(*importInfo, itemIdx);
            return;
        }

        if (item.State == EState::Done || item.State == EState::CreateSchemeObject) {
            UpdateItemDstPathId(db, *importInfo, itemIdx);
            for (auto childIdx : item.ChildItems) {
                UpdateItemDstPathId(db, *importInfo, childIdx);
            }
        }

        if (txId != InvalidTxId) {
            SubscribeTx(*importInfo, itemIdx);
        }

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDone(stateCounts)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        }

        Self->PersistImportState(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }
    }

    void UpdateItemDstPathId(NIceDb::TNiceDb& db, TImportInfo& importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo.Items.size());
        auto& item = importInfo.Items.at(itemIdx);

        auto path = TPath::Resolve(item.DstPathName, Self);
        Y_ABORT_UNLESS(path);

        item.DstPathId = path.Base()->PathId;
        Self->PersistImportItemDstPathId(db, importInfo, itemIdx);
    }

    void OnCreateIndexResult(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(CreateIndexResult);
        const auto& record = CreateIndexResult->Get()->Record;

        LOG_D("TImport::TTxProgress: OnCreateIndexResult"
            << ": txId# " << record.GetTxId()
            << ", status# " << record.GetStatus());
        LOG_T("Message:\n" << record.ShortDebugString());

        auto txId = TTxId(record.GetTxId());
        if (!Self->TxIdToImport.contains(txId)) {
            LOG_E("TImport::TTxProgress: OnCreateIndexResult received unknown txId"
                << ": txId# " << txId);
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToImport.at(txId);
        if (!Self->Imports.contains(id)) {
            LOG_E("TImport::TTxProgress: OnCreateIndexResult received unknown id"
                << ": id# " << id);
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        if (record.GetStatus() != Ydb::StatusIds::SUCCESS) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;

            if (record.GetStatus() == Ydb::StatusIds::ALREADY_EXISTS) {
                if (item.State == EState::BuildIndexes) {
                    txId = GetActiveBuildIndexId(*importInfo, itemIdx);
                }
            }

            if (txId == InvalidTxId) {
                item.Issue = GetIssues(record);
                Self->PersistImportItemState(db, *importInfo, itemIdx);

                if (importInfo->State != EState::Waiting) {
                    return;
                }

                Cancel(*importInfo, itemIdx, "unhappy propose");
                Self->PersistImportState(db, *importInfo);
                Self->EraseEncryptionKey(db, *importInfo);

                return SendNotificationsIfFinished(importInfo);
            }

            Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
        }

        item.WaitTxId = txId;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        if (importInfo->State != EState::Waiting) {
            CancelIndexBuilding(*importInfo, itemIdx);
            return;
        }

        SubscribeTx(*importInfo, itemIdx);
    }

    void OnNotifyResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(CompletedTxId);
        LOG_D("TImport::TTxProgress: OnNotifyResult"
            << ": txId# " << CompletedTxId);

        const auto txId = CompletedTxId;
        if (!Self->TxIdToImport.contains(txId)) {
            LOG_E("TImport::TTxProgress: OnNotifyResult received unknown txId"
                << ": txId# " << txId);
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToImport.at(txId);
        if (!Self->Imports.contains(id)) {
            LOG_E("TImport::TTxProgress: OnNotifyResult received unknown id"
                << ": id# " << id);
            return;
        }

        TImportInfo::TPtr importInfo = Self->Imports.at(id);

        if (importInfo->State != EState::Waiting) {
            return;
        }

        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.WaitTxId = InvalidTxId;
        Self->PersistImportItemState(db, *importInfo, itemIdx);

        Self->TxIdToImport.erase(txId);

        switch (item.State) {
        case EState::CreateSchemeObject:
            if (IsCreatedByQuery(item)) {
                item.State = EState::Done;
                break;
            } else if (item.Topic) {
                item.State = EState::Done;
                break;
            }
            if (item.Table) {
                for (auto childIdx : item.ChildItems) {
                    Y_ABORT_UNLESS(childIdx < importInfo->Items.size());
                    auto& childItem = importInfo->Items.at(childIdx);

                    childItem.State = EState::Transferring;
                    Self->PersistImportItemState(db, *importInfo, childIdx);
                    AllocateTxId(*importInfo, childIdx);
                }
            } else {
                Y_ABORT("Create Scheme Object: schema objects are empty");
            }
            item.State = EState::Transferring;
            AllocateTxId(*importInfo, itemIdx);
            break;

        case EState::Transferring:
            if (const auto issue = GetIssues(item, txId)) {
                item.Issue = *issue;
                Cancel(*importInfo, itemIdx, "issues during restore " + *issue);
                Self->EraseEncryptionKey(db, *importInfo);
            } else {
                const auto needToBuildIndexes = NeedToBuildIndexes(*importInfo, itemIdx);
                if (needToBuildIndexes && item.Table && item.NextIndexIdx < item.Table->indexes_size()) {
                    item.State = EState::BuildIndexes;
                    AllocateTxId(*importInfo, itemIdx);
                } else if (item.NextChangefeedIdx < item.Changefeeds.changefeeds_size() &&
                           AppData()->FeatureFlags.GetEnableChangefeedsImport()) {
                    item.State = EState::CreateChangefeed;
                    AllocateTxId(*importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                }
            }
            break;

        case EState::BuildIndexes:
            if (const auto issue = GetIssues(TIndexBuildId(ui64(txId)))) {
                item.Issue = *issue;
                Cancel(*importInfo, itemIdx, "issues during index building");
                Self->EraseEncryptionKey(db, *importInfo);
            } else {
                if (item.Table && ++item.NextIndexIdx < item.Table->indexes_size()) {
                    AllocateTxId(*importInfo, itemIdx);
                } else if (item.NextChangefeedIdx < item.Changefeeds.changefeeds_size() &&
                           AppData()->FeatureFlags.GetEnableChangefeedsImport()) {
                    item.State = EState::CreateChangefeed;
                    AllocateTxId(*importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                }
            }
            break;

        case EState::CreateChangefeed:
            if (item.ChangefeedState == TImportInfo::TItem::EChangefeedState::CreateChangefeed) {
                item.ChangefeedState = TImportInfo::TItem::EChangefeedState::CreateConsumers;
                AllocateTxId(*importInfo, itemIdx);
            } else if (++item.NextChangefeedIdx < item.Changefeeds.GetChangefeeds().size()) {
                item.ChangefeedState = TImportInfo::TItem::EChangefeedState::CreateChangefeed;
                AllocateTxId(*importInfo, itemIdx);
            } else {
                item.State = EState::Done;
            }
            break;

        default:
            return SendNotificationsIfFinished(importInfo);
        }

        const auto stateCounts = CountItemsByState(importInfo->Items);
        if (AllDone(stateCounts)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        } else if (AllDoneOrWaiting(stateCounts)) {
            RetrySchemeObjectsQueryExecution(*importInfo, db, ctx);
        }

        Self->PersistImportItemState(db, *importInfo, itemIdx);
        Self->PersistImportState(db, *importInfo);

        SendNotificationsIfFinished(importInfo);

        if (importInfo->IsFinished()) {
            AuditLogImportEnd(*importInfo.Get(), Self);
        }
    }

}; // TTxProgress

ITransaction* TSchemeShard::CreateTxCreateImport(TEvImport::TEvCreateImportRequest::TPtr& ev) {
    return new TImport::TTxCreate(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(ui64 id, const TMaybe<ui32>& itemIdx) {
    return new TImport::TTxProgress(this, id, itemIdx);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvPrivate::TEvImportSchemeReady::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvPrivate::TEvImportSchemaMappingReady::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvPrivate::TEvImportSchemeQueryResult::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TEvIndexBuilder::TEvCreateResponse::TPtr& ev) {
    return new TImport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressImport(TTxId completedTxId) {
    return new TImport::TTxProgress(this, completedTxId);
}

} // NSchemeShard
} // NKikimr
