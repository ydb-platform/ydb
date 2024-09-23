#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_export_flow_proposals.h"
#include "schemeshard_export_helpers.h"
#include "schemeshard_export.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_export.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TExport::TTxCreate: public TSchemeShard::TXxport::TTxBase {
    TEvExport::TEvCreateExportRequest::TPtr Request;
    bool Progress;

    explicit TTxCreate(TSelf* self, TEvExport::TEvCreateExportRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
        , Progress(false)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_EXPORT;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        LOG_D("TExport::TTxCreate: DoExecute");
        LOG_T("Message:\n" << request.ShortDebugString());

        auto response = MakeHolder<TEvExport::TEvCreateExportResponse>(request.GetTxId());

        const ui64 id = request.GetTxId();
        if (Self->Exports.contains(id)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::ALREADY_EXISTS,
                TStringBuilder() << "Export with id '" << id << "' already exists"
            );
        }

        const TString& uid = GetUid(request.GetRequest().GetOperationParams().labels());
        if (uid) {
            if (auto it = Self->ExportsByUid.find(uid); it != Self->ExportsByUid.end()) {
                if (IsSameDomain(it->second, request.GetDatabaseName())) {
                    Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), it->second);
                    return Reply(std::move(response));
                } else {
                    return Reply(
                        std::move(response),
                        Ydb::StatusIds::ALREADY_EXISTS,
                        TStringBuilder() << "Export with uid '" << uid << "' already exists"
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
                checks.ExportsLimit();
            }

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::PRECONDITION_FAILED, checks.GetError());
            }
        }

        TExportInfo::TPtr exportInfo = nullptr;

        switch (request.GetRequest().GetSettingsCase()) {
        case NKikimrExport::TCreateExportRequest::kExportToYtSettings:
            {
                const auto& settings = request.GetRequest().GetExportToYtSettings();
                exportInfo = new TExportInfo(id, uid, TExportInfo::EKind::YT, settings, domainPath.Base()->PathId);

                TString explain;
                if (!FillItems(exportInfo, settings, explain)) {
                    return Reply(
                        std::move(response),
                        Ydb::StatusIds::BAD_REQUEST,
                        TStringBuilder() << "Failed item check: " << explain
                    );
                }
            }
            break;

        case NKikimrExport::TCreateExportRequest::kExportToS3Settings:
            {
                auto settings = request.GetRequest().GetExportToS3Settings();
                if (!settings.scheme()) {
                    settings.set_scheme(Ydb::Export::ExportToS3Settings::HTTPS);
                }

                exportInfo = new TExportInfo(id, uid, TExportInfo::EKind::S3, settings, domainPath.Base()->PathId);

                TString explain;
                if (!FillItems(exportInfo, settings, explain)) {
                    return Reply(
                        std::move(response),
                        Ydb::StatusIds::BAD_REQUEST,
                        TStringBuilder() << "Failed item check: " << explain
                    );
                }
            }
            break;

        default:
            Y_DEBUG_ABORT("Unknown export kind");
        }

        Y_ABORT_UNLESS(exportInfo != nullptr);

        if (request.HasUserSID()) {
            exportInfo->UserSID = request.GetUserSID();
        }

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistCreateExport(db, exportInfo);

        exportInfo->State = TExportInfo::EState::CreateExportDir;
        exportInfo->StartTime = TAppData::TimeProvider->Now();
        Self->PersistExportState(db, exportInfo);

        Self->Exports[id] = exportInfo;
        if (uid) {
            Self->ExportsByUid[uid] = exportInfo;
        }

        Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), exportInfo);

        Progress = true;
        return Reply(std::move(response));
    }

    void DoComplete(const TActorContext& ctx) override {
        LOG_D("TExport::TTxCreate: DoComplete");

        if (Progress) {
            const ui64 id = Request->Get()->Record.GetTxId();
            Self->Execute(Self->CreateTxProgressExport(id), ctx);
        }
    }

private:
    static TString GetUid(const google::protobuf::Map<TString, TString>& labels) {
        auto it = labels.find("uid");
        if (it == labels.end()) {
            return TString();
        }

        return it->second;
    }

    bool Reply(
        THolder<TEvExport::TEvCreateExportResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString()
    ) {
        LOG_D("TExport::TTxCreate: Reply"
            << ": status# " << status
            << ", error# " << errorMessage);
        LOG_T("Message:\n" << response->Record.ShortDebugString());

        auto& exprt = *response->Record.MutableResponse()->MutableEntry();
        exprt.SetStatus(status);
        if (errorMessage) {
            AddIssue(exprt, errorMessage);
        }

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    template <typename TSettings>
    bool FillItems(TExportInfo::TPtr exportInfo, const TSettings& settings, TString& explain) {
        exportInfo->Items.reserve(settings.items().size());
        for (ui32 itemIdx : xrange(settings.items().size())) {
            const auto& item = settings.items(itemIdx);
            const TPath path = TPath::Resolve(item.source_path(), Self);
            {
                TPath::TChecker checks = path.Check();
                checks
                    .IsResolved()
                    .NotDeleted()
                    .NotUnderDeleting()
                    .IsTable()
                    .FailOnRestrictedCreateInTempZone();

                if (!checks) {
                    explain = checks.GetError();
                    return false;
                }
            }

            exportInfo->Items.emplace_back(item.source_path(), path.Base()->PathId);
            exportInfo->PendingItems.push_back(itemIdx);
        }

        return true;
    }

}; // TTxCreate

struct TSchemeShard::TExport::TTxProgress: public TSchemeShard::TXxport::TTxBase {
    using EState = TExportInfo::EState;
    using ESubState = TExportInfo::TItem::ESubState;

    static constexpr ui32 IssuesSizeLimit = 2 * 1024;

    ui64 Id;
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult = nullptr;
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult = nullptr;
    TTxId CompletedTxId = InvalidTxId;

    explicit TTxProgress(TSelf* self, ui64 id)
        : TXxport::TTxBase(self)
        , Id(id)
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

    explicit TTxProgress(TSelf* self, TTxId completedTxId)
        : TXxport::TTxBase(self)
        , CompletedTxId(completedTxId)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_EXPORT_PROGRESS;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_D("TExport::TTxProgress: DoExecute");

        if (AllocateResult) {
            OnAllocateResult(txc, ctx);
        } else if (ModifyResult) {
            OnModifyResult(txc, ctx);
        } else if (CompletedTxId) {
            OnNotifyResult(txc, ctx);
        } else {
            Resume(txc, ctx);
        }

        return true;
    }

    void DoComplete(const TActorContext&) override {
        LOG_D("TExport::TTxProgress: DoComplete");
    }

private:
    void MkDir(TExportInfo::TPtr exportInfo, TTxId txId) {
        LOG_I("TExport::TTxProgress: MkDir propose"
            << ": info# " << exportInfo->ToString()
            << ", txId# " << txId);

        Y_ABORT_UNLESS(exportInfo->WaitTxId == InvalidTxId);
        Send(Self->SelfId(), MkDirPropose(Self, txId, exportInfo));
    }

    void CopyTables(TExportInfo::TPtr exportInfo, TTxId txId) {
        LOG_I("TExport::TTxProgress: CopyTables propose"
            << ": info# " << exportInfo->ToString()
            << ", txId# " << txId);

        Y_ABORT_UNLESS(exportInfo->WaitTxId == InvalidTxId);
        Send(Self->SelfId(), CopyTablesPropose(Self, txId, exportInfo));
    }

    void TransferData(TExportInfo::TPtr exportInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        auto& item = exportInfo->Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TExport::TTxProgress: Backup propose"
            << ": info# " << exportInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->SelfId(), BackupPropose(Self, txId, exportInfo, itemIdx));
    }

    bool CancelTransferring(TExportInfo::TPtr exportInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        const auto& item = exportInfo->Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                exportInfo->State = EState::Cancellation;
            }

            return false;
        }

        exportInfo->State = EState::Cancellation;

        LOG_I("TExport::TTxProgress: cancel backup's tx"
            << ": info# " << exportInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Send(Self->SelfId(), CancelPropose(exportInfo, item.WaitTxId), 0, exportInfo->Id);
        return true;
    }

    void DropTable(TExportInfo::TPtr exportInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        const auto& item = exportInfo->Items.at(itemIdx);

        LOG_I("TExport::TTxProgress: Drop propose"
            << ": info# " << exportInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->SelfId(), DropPropose(Self, txId, exportInfo, itemIdx));
    }

    void DropDir(TExportInfo::TPtr exportInfo, TTxId txId) {
        LOG_I("TExport::TTxProgress: Drop propose"
            << ": info# " << exportInfo->ToString()
            << ", txId# " << txId);

        Y_ABORT_UNLESS(exportInfo->WaitTxId == InvalidTxId);
        Send(Self->SelfId(), DropPropose(Self, txId, exportInfo));
    }

    void AllocateTxId(TExportInfo::TPtr exportInfo) {
        LOG_I("TExport::TTxProgress: Allocate txId"
            << ": info# " << exportInfo->ToString());

        Y_ABORT_UNLESS(exportInfo->WaitTxId == InvalidTxId);
        Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(), 0, exportInfo->Id);
    }

    void AllocateTxId(TExportInfo::TPtr exportInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        auto& item = exportInfo->Items.at(itemIdx);

        item.SubState = ESubState::AllocateTxId;

        LOG_I("TExport::TTxProgress: Allocate txId"
            << ": info# " << exportInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(), 0, exportInfo->Id);
    }

    void SubscribeTx(TTxId txId) {
        Send(Self->SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(ui64(txId)));
    }

    void SubscribeTx(TExportInfo::TPtr exportInfo) {
        LOG_I("TExport::TTxProgress: Wait for completion"
            << ": info# " << exportInfo->ToString());

        Y_ABORT_UNLESS(exportInfo->WaitTxId != InvalidTxId);
        SubscribeTx(exportInfo->WaitTxId);
    }

    void SubscribeTx(TExportInfo::TPtr exportInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        auto& item = exportInfo->Items.at(itemIdx);

        item.SubState = ESubState::Subscribed;

        LOG_I("TExport::TTxProgress: Wait for completion"
            << ": info# " << exportInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Y_ABORT_UNLESS(item.WaitTxId != InvalidTxId);
        SubscribeTx(item.WaitTxId);
    }

    static TPathId ItemPathId(TSchemeShard* ss, TExportInfo::TPtr exportInfo, ui32 itemIdx) {
        const TPath itemPath = TPath::Resolve(ExportItemPathName(ss, exportInfo, itemIdx), ss);

        if (!itemPath.IsResolved()) {
            return {};
        }

        return itemPath.Base()->PathId;
    }

    TTxId GetActiveCopyingTxId(TExportInfo::TPtr exportInfo) {
        if (exportInfo->Items.size() < 1) {
            return InvalidTxId;
        }

        const auto& item = exportInfo->Items.at(0);
        if (!Self->PathsById.contains(item.SourcePathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(item.SourcePathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateCopying) {
            return InvalidTxId;
        }

        if (!ItemPathId(Self, exportInfo, 0)) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    TTxId GetActiveBackupTxId(TExportInfo::TPtr exportInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        const auto& item = exportInfo->Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::Transferring);

        const auto itemPathId = ItemPathId(Self, exportInfo, itemIdx);
        if (!itemPathId) {
            return InvalidTxId;
        }

        if (!Self->PathsById.contains(itemPathId)) {
            return InvalidTxId;
        }

        auto path = Self->PathsById.at(itemPathId);
        if (path->PathState != NKikimrSchemeOp::EPathStateBackup) {
            return InvalidTxId;
        }

        return path->LastTxId;
    }

    void Cancel(TExportInfo::TPtr exportInfo, ui32 itemIdx, TStringBuf marker) {
        Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
        const auto& item = exportInfo->Items.at(itemIdx);

        LOG_N("TExport::TTxProgress: " << marker << ", cancelling"
            << ", info# " << exportInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        exportInfo->State = EState::Cancelled;

        for (ui32 i : xrange(exportInfo->Items.size())) {
            if (i == itemIdx) {
                continue;
            }

            if (exportInfo->Items.at(i).State != EState::Transferring) {
                continue;
            }

            CancelTransferring(exportInfo, i);
        }

        if (exportInfo->State == EState::Cancelled) {
            exportInfo->EndTime = TAppData::TimeProvider->Now();
        }
    }

    TMaybe<TString> GetIssues(const TPathId& itemPathId, TTxId backupTxId) {
        if (!Self->Tables.contains(itemPathId)) {
            return TStringBuilder() << "Cannot find table: " << itemPathId;
        }

        TTableInfo::TPtr table = Self->Tables.at(itemPathId);
        if (!table->BackupHistory.contains(backupTxId)) {
            return TStringBuilder() << "Cannot find backup: " << backupTxId << " for table: " << itemPathId;
        }

        const auto& result = table->BackupHistory.at(backupTxId);
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

    void Resume(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(Self->Exports.contains(Id));
        TExportInfo::TPtr exportInfo = Self->Exports.at(Id);

        LOG_D("TExport::TTxProgress: Resume"
            << ": id# " << Id);

        NIceDb::TNiceDb db(txc.DB);

        switch (exportInfo->State) {
        case EState::CreateExportDir:
        case EState::CopyTables:
            if (exportInfo->WaitTxId == InvalidTxId) {
                AllocateTxId(exportInfo);
            } else {
                SubscribeTx(exportInfo);
            }
            break;

        case EState::Transferring:
            for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
                const auto& item = exportInfo->Items.at(itemIdx);

                if (item.WaitTxId == InvalidTxId) {
                    AllocateTxId(exportInfo, itemIdx);
                } else {
                    SubscribeTx(exportInfo, itemIdx);
                }
            }
            break;

        case EState::Cancellation:
            exportInfo->State = EState::Cancelled;
            // will be switched back to Cancellation if there is any active backup tx
            for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
                auto& item = exportInfo->Items.at(itemIdx);

                if (item.State != EState::Transferring) {
                    continue;
                }

                if (!CancelTransferring(exportInfo, itemIdx)) {
                    const TTxId txId = GetActiveBackupTxId(exportInfo, itemIdx);

                    if (txId == InvalidTxId) {
                        item.State = EState::Cancelled;
                    } else {
                        item.WaitTxId = txId;
                        CancelTransferring(exportInfo, itemIdx);
                    }

                    Self->PersistExportItemState(db, exportInfo, itemIdx);
                }
            }

            if (exportInfo->State == EState::Cancelled) {
                exportInfo->EndTime = TAppData::TimeProvider->Now();
            }

            Self->PersistExportState(db, exportInfo);
            SendNotificationsIfFinished(exportInfo);
            break;

        case EState::Dropping:
            if (!exportInfo->AllItemsAreDropped()) {
                for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
                    const auto& item = exportInfo->Items.at(itemIdx);

                    if (item.State != EState::Dropping) {
                        continue;
                    }

                    if (item.WaitTxId == InvalidTxId) {
                        exportInfo->PendingDropItems.push_back(itemIdx);
                        AllocateTxId(exportInfo, itemIdx);
                    } else {
                        SubscribeTx(exportInfo, itemIdx);
                    }
                }
            } else {
                if (exportInfo->WaitTxId == InvalidTxId) {
                    AllocateTxId(exportInfo);
                } else {
                    SubscribeTx(exportInfo);
                }
            }
            break;

        default:
            break;
        }
    }

    void OnAllocateResult(TTransactionContext&, const TActorContext&) {
        Y_ABORT_UNLESS(AllocateResult);

        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());
        const ui64 id = AllocateResult->Cookie;

        LOG_D("TExport::TTxProgress: OnAllocateResult"
            << ": txId# " << txId
            << ", id# " << id);

        if (!Self->Exports.contains(id)) {
            LOG_E("TExport::TTxProgress: OnAllocateResult received unknown id"
                << ": id# " << id);
            return;
        }

        TExportInfo::TPtr exportInfo = Self->Exports.at(id);
        ui32 itemIdx = Max<ui32>();

        auto popPendingItemIdx = [](TDeque<ui32>& pendingItems) {
            const ui32 itemIdx = pendingItems.front();
            pendingItems.pop_front();
            return itemIdx;
        };

        switch (exportInfo->State) {
        case EState::CreateExportDir:
            MkDir(exportInfo, txId);
            break;

        case EState::CopyTables:
            CopyTables(exportInfo, txId);
            break;

        case EState::Transferring:
            if (exportInfo->PendingItems) {
                itemIdx = popPendingItemIdx(exportInfo->PendingItems);
                TransferData(exportInfo, itemIdx, txId);
            } else {
                return;
            }
            break;

        case EState::Dropping:
            if (exportInfo->PendingDropItems) {
                itemIdx = popPendingItemIdx(exportInfo->PendingDropItems);
                DropTable(exportInfo, itemIdx, txId);
            } else {
                DropDir(exportInfo, txId);
            }
            break;

        default:
            return;
        }

        Y_ABORT_UNLESS(!Self->TxIdToExport.contains(txId));
        Self->TxIdToExport[txId] = {exportInfo->Id, itemIdx};
    }

    void OnModifyResult(TTransactionContext& txc, const TActorContext& ctx) {
        Y_ABORT_UNLESS(ModifyResult);
        const auto& record = ModifyResult->Get()->Record;

        LOG_D("TExport::TTxProgress: OnModifyResult"
            << ": txId# " << record.GetTxId()
            << ", status# " << record.GetStatus());
        LOG_T("Message:\n" << record.ShortDebugString());

        auto txId = TTxId(record.GetTxId());
        if (!Self->TxIdToExport.contains(txId)) {
            LOG_E("TExport::TTxProgress: OnModifyResult received unknown txId"
                << ": txId# " << txId);
            return;
        }

        ui64 id;
        ui32 itemIdx;
        std::tie(id, itemIdx) = Self->TxIdToExport.at(txId);
        if (!Self->Exports.contains(id)) {
            LOG_E("TExport::TTxProgress: OnModifyResult received unknown id"
                << ": id# " << id);
            return;
        }

        TExportInfo::TPtr exportInfo = Self->Exports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        if (record.GetStatus() != NKikimrScheme::StatusAccepted) {
            Self->TxIdToExport.erase(txId);
            txId = InvalidTxId;

            const auto status = record.GetStatus();
            const bool isMultipleMods = status == NKikimrScheme::StatusMultipleModifications;
            const bool isAlreadyExists = status == NKikimrScheme::StatusAlreadyExists;
            const bool isNotExist = status == NKikimrScheme::StatusPathDoesNotExist;

            switch (exportInfo->State) {
            case EState::CreateExportDir:
            case EState::CopyTables:
                if (isMultipleMods || isAlreadyExists) {
                    if (record.GetPathCreateTxId()) {
                        txId = TTxId(record.GetPathCreateTxId());
                    } else {
                        txId = GetActiveCopyingTxId(exportInfo);
                    }
                }
                break;

            case EState::Transferring:
                if (isMultipleMods) {
                    txId = GetActiveBackupTxId(exportInfo, itemIdx);
                }
                break;

            case EState::Dropping:
                if (isMultipleMods || isNotExist) {
                    if (record.GetPathDropTxId()) {
                        // We may need to wait for an earlier tx
                        txId = TTxId(record.GetPathDropTxId());
                    } else if (isNotExist) {
                        // Already dropped and fully forgotten
                        txId = TTxId(record.GetTxId());
                    } else {
                        // We need to wait and retry the operation
                        txId = TTxId(record.GetTxId());
                        THolder<IEventBase> ev;

                        if (itemIdx < exportInfo->Items.size()) {
                            ev.Reset(DropPropose(Self, txId, exportInfo, itemIdx).Release());
                        } else {
                            ev.Reset(DropPropose(Self, txId, exportInfo).Release());
                        }

                        ctx.TActivationContext::Schedule(TDuration::Seconds(10),
                            new IEventHandle(Self->SelfId(), Self->SelfId(), ev.Release()));
                        Self->TxIdToExport[txId] = {exportInfo->Id, itemIdx};

                        return;
                    }
                }
                break;

            default:
                break;
            }

            if (txId == InvalidTxId) {
                if (itemIdx < exportInfo->Items.size()) {
                    auto& item = exportInfo->Items.at(itemIdx);

                    item.State = EState::Cancelled;
                    item.Issue = record.GetReason();
                    Self->PersistExportItemState(db, exportInfo, itemIdx);

                    if (!exportInfo->IsInProgress()) {
                        return;
                    }

                    Cancel(exportInfo, itemIdx, "unhappy propose");
                } else {
                    if (!exportInfo->IsInProgress()) {
                        return;
                    }

                    if (exportInfo->State == EState::CopyTables && isMultipleMods) {
                        for (const auto& item : exportInfo->Items) {
                            if (!Self->PathsById.contains(item.SourcePathId)) {
                                exportInfo->DependencyTxIds.clear();
                                break;
                            }

                            auto path = Self->PathsById.at(item.SourcePathId);
                            if (path->PathState != NKikimrSchemeOp::EPathStateNoChanges) {
                                exportInfo->DependencyTxIds.insert(path->LastTxId);
                                SubscribeTx(path->LastTxId);

                                Y_DEBUG_ABORT_UNLESS(itemIdx == Max<ui32>());
                                Self->TxIdToDependentExport[path->LastTxId].insert(exportInfo->Id);
                            }
                        }

                        if (!exportInfo->DependencyTxIds.empty()) {
                            return;
                        }
                    }

                    exportInfo->Issue = record.GetReason();
                    exportInfo->State = EState::Cancelled;
                    exportInfo->EndTime = TAppData::TimeProvider->Now();
                }

                Self->PersistExportState(db, exportInfo);
                return SendNotificationsIfFinished(exportInfo);
            }

            Self->TxIdToExport[txId] = {exportInfo->Id, itemIdx};
        }

        switch (exportInfo->State) {
        case EState::CreateExportDir:
            exportInfo->ExportPathId = Self->MakeLocalId(TLocalPathId(record.GetPathId()));
            Self->PersistExportPathId(db, exportInfo);

            exportInfo->WaitTxId = txId;
            Self->PersistExportState(db, exportInfo);
            break;

        case EState::CopyTables:
            exportInfo->WaitTxId = txId;
            Self->PersistExportState(db, exportInfo);
            break;

        case EState::Transferring:
            Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
            exportInfo->Items.at(itemIdx).WaitTxId = txId;
            Self->PersistExportItemState(db, exportInfo, itemIdx);
            break;

        case EState::Dropping:
            if (!exportInfo->AllItemsAreDropped()) {
                Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
                exportInfo->Items.at(itemIdx).WaitTxId = txId;
                Self->PersistExportItemState(db, exportInfo, itemIdx);
            } else {
                exportInfo->WaitTxId = txId;
                Self->PersistExportState(db, exportInfo);
            }
            break;

        case EState::Cancellation:
            if (itemIdx < exportInfo->Items.size()) {
                exportInfo->Items.at(itemIdx).WaitTxId = txId;
                Self->PersistExportItemState(db, exportInfo, itemIdx);

                CancelTransferring(exportInfo, itemIdx);
            }
            return;

        default:
            return; // no need to wait notification
        }

        LOG_I("TExport::TTxProgress: Wait for completion"
            << ": info# " << exportInfo->ToString()
            << ", itemIdx# " << itemIdx
            << ", txId# " << txId);
        SubscribeTx(txId);
    }

    void OnNotifyResult(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(CompletedTxId);
        LOG_D("TExport::TTxProgress: OnNotifyResult"
            << ": txId# " << CompletedTxId);

        const auto txId = CompletedTxId;
        if (!Self->TxIdToExport.contains(txId) && !Self->TxIdToDependentExport.contains(txId)) {
            LOG_E("TExport::TTxProgress: OnNotifyResult received unknown txId"
                << ": txId# " << txId);
            return;
        }

        if (Self->TxIdToExport.contains(txId)) {
            ui64 id;
            ui32 itemIdx;
            std::tie(id, itemIdx) = Self->TxIdToExport.at(txId);

            OnNotifyResult(txId, id, itemIdx, txc);
            Self->TxIdToExport.erase(txId);
        }
        
        if (Self->TxIdToDependentExport.contains(txId)) {
            for (const auto id : Self->TxIdToDependentExport.at(txId)) {
                OnNotifyResult(txId, id, Max<ui32>(), txc);
            }

            Self->TxIdToDependentExport.erase(txId);
        }
    }

    void OnNotifyResult(TTxId txId, ui64 id, ui32 itemIdx, TTransactionContext& txc) {
        LOG_D("TExport::TTxProgress: OnNotifyResult"
            << ": txId# " << txId
            << ", id# " << id
            << ", itemIdx# " << itemIdx);

        if (!Self->Exports.contains(id)) {
            LOG_E("TExport::TTxProgress: OnNotifyResult received unknown id"
                << ": id# " << id);
            return;
        }

        TExportInfo::TPtr exportInfo = Self->Exports.at(id);
        NIceDb::TNiceDb db(txc.DB);

        switch (exportInfo->State) {
        case EState::CreateExportDir:
            exportInfo->State = EState::CopyTables;
            exportInfo->WaitTxId = InvalidTxId;
            AllocateTxId(exportInfo);
            break;

        case EState::CopyTables:
            if (exportInfo->DependencyTxIds.contains(txId)) {
                exportInfo->DependencyTxIds.erase(txId);
                if (exportInfo->DependencyTxIds.empty()) {
                    AllocateTxId(exportInfo);
                }
                return;
            }

            exportInfo->State = EState::Transferring;
            exportInfo->WaitTxId = InvalidTxId;
            for (ui32 itemIdx : xrange(exportInfo->Items.size())) {
                exportInfo->Items.at(itemIdx).State = EState::Transferring;
                Self->PersistExportItemState(db, exportInfo, itemIdx);

                AllocateTxId(exportInfo, itemIdx);
            }
            break;

        case EState::Transferring: {
            Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
            auto& item = exportInfo->Items.at(itemIdx);

            item.State = EState::Done;
            item.WaitTxId = InvalidTxId;

            if (const auto issue = GetIssues(ItemPathId(Self, exportInfo, itemIdx), txId)) {
                item.Issue = *issue;
                Cancel(exportInfo, itemIdx, "issues during backing up");
            } else {
                if (AllOf(exportInfo->Items, &TExportInfo::TItem::IsDone)) {
                    exportInfo->State = EState::Done;
                    exportInfo->EndTime = TAppData::TimeProvider->Now();
                }
            }

            Self->PersistExportItemState(db, exportInfo, itemIdx);
            break;
        }

        case EState::Dropping:
            if (!exportInfo->AllItemsAreDropped()) {
                Y_ABORT_UNLESS(itemIdx < exportInfo->Items.size());
                auto& item = exportInfo->Items.at(itemIdx);

                item.State = EState::Dropped;
                item.WaitTxId = InvalidTxId;
                Self->PersistExportItemState(db, exportInfo, itemIdx);

                if (exportInfo->AllItemsAreDropped()) {
                    AllocateTxId(exportInfo);
                }
            } else {
                SendNotificationsIfFinished(exportInfo, true); // for tests

                if (exportInfo->Uid) {
                    Self->ExportsByUid.erase(exportInfo->Uid);
                }

                Self->Exports.erase(exportInfo->Id);
                Self->PersistRemoveExport(db, exportInfo);
            }
            return;

        default:
            return SendNotificationsIfFinished(exportInfo);
        }

        Self->PersistExportState(db, exportInfo);
        SendNotificationsIfFinished(exportInfo);
    }

}; // TTxProgress

ITransaction* TSchemeShard::CreateTxCreateExport(TEvExport::TEvCreateExportRequest::TPtr& ev) {
    return new TExport::TTxCreate(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressExport(ui64 id) {
    return new TExport::TTxProgress(this, id);
}

ITransaction* TSchemeShard::CreateTxProgressExport(TEvTxAllocatorClient::TEvAllocateResult::TPtr& ev) {
    return new TExport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressExport(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& ev) {
    return new TExport::TTxProgress(this, ev);
}

ITransaction* TSchemeShard::CreateTxProgressExport(TTxId completedTxId) {
    return new TExport::TTxProgress(this, completedTxId);
}

} // NSchemeShard
} // NKikimr
