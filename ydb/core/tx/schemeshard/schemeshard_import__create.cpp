#include "schemeshard_xxport__tx_base.h"
#include "schemeshard_import_flow_proposals.h"
#include "schemeshard_import_scheme_getter.h"
#include "schemeshard_import_helpers.h"
#include "schemeshard_import.h"
#include "schemeshard_impl.h"

#include <ydb/public/api/protos/ydb_import.pb.h>
#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/algorithm.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/xrange.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

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

        const TString& uid = GetUid(request.GetRequest().GetOperationParams().labels());
        if (uid) {
            if (auto it = Self->ImportsByUid.find(uid); it != Self->ImportsByUid.end()) {
                if (IsSameDomain(it->second, request.GetDatabaseName())) {
                    Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), it->second);
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

        switch (request.GetRequest().GetSettingsCase()) {
        case NKikimrImport::TCreateImportRequest::kImportFromS3Settings:
            {
                auto settings = request.GetRequest().GetImportFromS3Settings();
                if (!settings.scheme()) {
                    settings.set_scheme(Ydb::Import::ImportFromS3Settings::HTTPS);
                }

                importInfo = new TImportInfo(id, uid, TImportInfo::EKind::S3, settings, domainPath.Base()->PathId);

                if (request.HasUserSID()) {
                    importInfo->UserSID = request.GetUserSID();
                }

                TString explain;
                if (!FillItems(importInfo, settings, explain)) {
                    return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, explain);
                }
            }
            break;

        default:
            Y_DEBUG_ABORT("Unknown import kind");
        }

        Y_ABORT_UNLESS(importInfo != nullptr);

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistCreateImport(db, importInfo);

        importInfo->State = TImportInfo::EState::Waiting;
        importInfo->StartTime = TAppData::TimeProvider->Now();
        Self->PersistImportState(db, importInfo);

        Self->Imports[id] = importInfo;
        if (uid) {
            Self->ImportsByUid[uid] = importInfo;
        }

        Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), importInfo);

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
    static TString GetUid(const google::protobuf::Map<TString, TString>& labels) {
        auto it = labels.find("uid");
        if (it == labels.end()) {
            return TString();
        }

        return it->second;
    }

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

        Send(Request->Sender, std::move(response), 0, Request->Cookie);

        return true;
    }

    template <typename TSettings>
    bool FillItems(TImportInfo::TPtr importInfo, const TSettings& settings, TString& explain) {
        THashSet<TString> dstPaths;

        importInfo->Items.reserve(settings.items().size());
        for (ui32 itemIdx : xrange(settings.items().size())) {
            const auto& dstPath = settings.items(itemIdx).destination_path();
            if (!dstPaths.insert(dstPath).second) {
                explain = TStringBuilder() << "Duplicate destination_path: " << dstPath;
                return false;
            }

            const TPath path = TPath::Resolve(dstPath, Self);
            {
                TPath::TChecker checks = path.Check();
                checks
                    .IsAtLocalSchemeShard()
                    .HasResolvedPrefix()
                    .FailOnRestrictedCreateInTempZone();

                if (path.IsResolved()) {
                    checks
                        .IsResolved()
                        .IsDeleted();
                } else {
                    checks
                        .NotEmpty()
                        .NotResolved();
                }

                if (checks) {
                    checks
                        .IsValidLeafName()
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
            }

            importInfo->Items.emplace_back(dstPath);
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
        const auto& item = importInfo->Items.at(itemIdx);

        LOG_I("TImport::TTxProgress: Get scheme"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        ctx.RegisterWithSameMailbox(CreateSchemeGetter(Self->SelfId(), importInfo, itemIdx));
    }

    void CreateTable(TImportInfo::TPtr importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: CreateTable propose"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);

        auto propose = CreateTablePropose(Self, txId, importInfo, itemIdx);
        Y_ABORT_UNLESS(propose);

        Send(Self->SelfId(), std::move(propose));
    }

    void TransferData(TImportInfo::TPtr importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: Restore propose"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->SelfId(), RestorePropose(Self, txId, importInfo, itemIdx));
    }

    bool CancelTransferring(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        const auto& item = importInfo->Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                importInfo->State = EState::Cancellation;
            }

            return false;
        }

        importInfo->State = EState::Cancellation;

        LOG_I("TImport::TTxProgress: cancel restore's tx"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Send(Self->SelfId(), CancelRestorePropose(importInfo, item.WaitTxId), 0, importInfo->Id);
        return true;
    }

    void BuildIndex(TImportInfo::TPtr importInfo, ui32 itemIdx, TTxId txId) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.SubState = ESubState::Proposed;

        LOG_I("TImport::TTxProgress: build index"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx)
            << ", txId# " << txId);

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->SelfId(), BuildIndexPropose(Self, txId, importInfo, itemIdx, MakeIndexBuildUid(importInfo, itemIdx)));
    }

    bool CancelIndexBuilding(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        const auto& item = importInfo->Items.at(itemIdx);

        if (item.WaitTxId == InvalidTxId) {
            if (item.SubState == ESubState::Proposed) {
                importInfo->State = EState::Cancellation;
            }

            return false;
        }

        importInfo->State = EState::Cancellation;

        LOG_I("TImport::TTxProgress: cancel index building"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Send(Self->SelfId(), CancelIndexBuildPropose(Self, importInfo, item.WaitTxId), 0, importInfo->Id);
        return true;
    }

    void AllocateTxId(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.SubState = ESubState::AllocateTxId;

        LOG_I("TImport::TTxProgress: Allocate txId"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Y_ABORT_UNLESS(item.WaitTxId == InvalidTxId);
        Send(Self->TxAllocatorClient, new TEvTxAllocatorClient::TEvAllocate(), 0, importInfo->Id);
    }

    void SubscribeTx(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.SubState = ESubState::Subscribed;

        LOG_I("TImport::TTxProgress: Wait for completion"
            << ": info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        Y_ABORT_UNLESS(item.WaitTxId != InvalidTxId);
        Send(Self->SelfId(), new TEvSchemeShard::TEvNotifyTxCompletion(ui64(item.WaitTxId)));
    }

    TTxId GetActiveRestoreTxId(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        const auto& item = importInfo->Items.at(itemIdx);

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

    TTxId GetActiveBuildIndexId(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        const auto& item = importInfo->Items.at(itemIdx);

        Y_ABORT_UNLESS(item.State == EState::BuildIndexes);

        const auto uid = MakeIndexBuildUid(importInfo, itemIdx);
        const auto* infoPtr = Self->IndexBuildsByUid.FindPtr(uid);
        if (!infoPtr) {
            return InvalidTxId;
        }

        return TTxId(ui64((*infoPtr)->Id));
    }

    static TString MakeIndexBuildUid(TImportInfo::TPtr importInfo, ui32 itemIdx) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        const auto& item = importInfo->Items.at(itemIdx);

        return TStringBuilder() << importInfo->Id << "-" << itemIdx << "-" << item.NextIndexIdx;
    }

    void Cancel(TImportInfo::TPtr importInfo, ui32 itemIdx, TStringBuf marker) {
        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        const auto& item = importInfo->Items.at(itemIdx);

        LOG_N("TImport::TTxProgress: " << marker << ", cancelling"
            << ", info# " << importInfo->ToString()
            << ", item# " << item.ToString(itemIdx));

        importInfo->State = EState::Cancelled;

        for (ui32 i : xrange(importInfo->Items.size())) {
            if (i == itemIdx) {
                continue;
            }

            switch (importInfo->Items.at(i).State) {
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

        if (importInfo->State == EState::Cancelled) {
            importInfo->EndTime = TAppData::TimeProvider->Now();
        }
    }

    TMaybe<TString> GetIssues(const TPathId& dstPathId, TTxId restoreTxId) {
        Y_ABORT_UNLESS(Self->Tables.contains(dstPathId));
        TTableInfo::TPtr table = Self->Tables.at(dstPathId);

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
        const auto& indexInfo = *indexInfoPtr->Get();

        if (indexInfo.IsDone()) {
            return Nothing();
        }

        return indexInfo.Issue;
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

        if (ItemIdx) {
            Resume(importInfo, *ItemIdx, txc, ctx);
        } else {
            for (ui32 itemIdx : xrange(importInfo->Items.size())) {
                Resume(importInfo, itemIdx, txc, ctx);
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

                case EState::CreateTable:
                case EState::Transferring:
                case EState::BuildIndexes:
                    if (item.WaitTxId == InvalidTxId) {
                        AllocateTxId(importInfo, itemIdx);
                    } else {
                        SubscribeTx(importInfo, itemIdx);
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
                    if (!CancelTransferring(importInfo, itemIdx)) {
                        txId = GetActiveRestoreTxId(importInfo, itemIdx);
                    }
                    break;

                case EState::BuildIndexes:
                    if (!CancelIndexBuilding(importInfo, itemIdx)) {
                        txId = GetActiveBuildIndexId(importInfo, itemIdx);
                    }
                    break;

                default:
                    break;
                }

                if (txId != InvalidTxId) {
                    item.WaitTxId = txId;
                    Self->PersistImportItemState(db, importInfo, itemIdx);

                    switch (item.State) {
                    case EState::Transferring:
                        CancelTransferring(importInfo, itemIdx);
                        break;

                    case EState::BuildIndexes:
                        CancelIndexBuilding(importInfo, itemIdx);
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

    void OnSchemeResult(TTransactionContext& txc, const TActorContext&) {
        Y_ABORT_UNLESS(SchemeResult);

        const auto& msg = *SchemeResult->Get();

        LOG_D("TImport::TTxProgress: OnSchemeResult"
            << ": id# " << msg.ImportId);

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

        auto& item = importInfo->Items.at(msg.ItemIdx);
        NIceDb::TNiceDb db(txc.DB);

        TString error;
        if (!msg.Success || !CreateTablePropose(Self, TTxId(), importInfo, msg.ItemIdx, error)) {
            item.Issue = msg.Success ? error : msg.Error;
            Self->PersistImportItemState(db, importInfo, msg.ItemIdx);

            if (importInfo->State != EState::Waiting) {
                return;
            }

            Cancel(importInfo, msg.ItemIdx, "cannot get/invalid scheme");
            Self->PersistImportState(db, importInfo);

            return SendNotificationsIfFinished(importInfo);
        }

        Self->PersistImportItemScheme(db, importInfo, msg.ItemIdx);

        item.State = EState::CreateTable;
        Self->PersistImportItemState(db, importInfo, msg.ItemIdx);
        AllocateTxId(importInfo, msg.ItemIdx);
    }

    void OnAllocateResult(TTransactionContext&, const TActorContext&) {
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
            case EState::CreateTable:
                if (!Self->TableProfilesLoaded) {
                    Self->WaitForTableProfiles(id, i);
                } else {
                    CreateTable(importInfo, i, txId);
                    itemIdx = i;
                }
                break;

            case EState::Transferring:
                TransferData(importInfo, i, txId);
                itemIdx = i;
                break;

            case EState::BuildIndexes:
                BuildIndex(importInfo, i, txId);
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

    void OnModifyResult(TTransactionContext& txc, const TActorContext&) {
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

        if (record.GetStatus() != NKikimrScheme::StatusAccepted) {
            Self->TxIdToImport.erase(txId);
            txId = InvalidTxId;

            if (record.GetStatus() == NKikimrScheme::StatusMultipleModifications) {
                if (record.GetPathCreateTxId()) {
                    txId = TTxId(record.GetPathCreateTxId());
                } else if (item.State == EState::Transferring) {
                    txId = GetActiveRestoreTxId(importInfo, itemIdx);
                }
            }

            if (txId == InvalidTxId) {
                item.Issue = record.GetReason();
                Self->PersistImportItemState(db, importInfo, itemIdx);

                if (importInfo->State != EState::Waiting) {
                    return;
                }

                Cancel(importInfo, itemIdx, "unhappy propose");
                Self->PersistImportState(db, importInfo);

                return SendNotificationsIfFinished(importInfo);
            }

            Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
        }

        item.WaitTxId = txId;
        Self->PersistImportItemState(db, importInfo, itemIdx);

        if (importInfo->State != EState::Waiting && item.State == EState::Transferring) {
            CancelTransferring(importInfo, itemIdx);
            return;
        }

        if (item.State == EState::CreateTable) {
            auto createPath = TPath::Resolve(item.DstPathName, Self);
            Y_ABORT_UNLESS(createPath);

            item.DstPathId = createPath.Base()->PathId;
            Self->PersistImportItemDstPathId(db, importInfo, itemIdx);
        }

        SubscribeTx(importInfo, itemIdx);
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
                    txId = GetActiveBuildIndexId(importInfo, itemIdx);
                }
            }

            if (txId == InvalidTxId) {
                item.Issue = GetIssues(record);
                Self->PersistImportItemState(db, importInfo, itemIdx);

                if (importInfo->State != EState::Waiting) {
                    return;
                }

                Cancel(importInfo, itemIdx, "unhappy propose");
                Self->PersistImportState(db, importInfo);

                return SendNotificationsIfFinished(importInfo);
            }

            Self->TxIdToImport[txId] = {importInfo->Id, itemIdx};
        }

        item.WaitTxId = txId;
        Self->PersistImportItemState(db, importInfo, itemIdx);

        if (importInfo->State != EState::Waiting) {
            CancelIndexBuilding(importInfo, itemIdx);
            return;
        }

        SubscribeTx(importInfo, itemIdx);
    }

    void OnNotifyResult(TTransactionContext& txc, const TActorContext&) {
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
        NIceDb::TNiceDb db(txc.DB);

        Y_ABORT_UNLESS(itemIdx < importInfo->Items.size());
        auto& item = importInfo->Items.at(itemIdx);

        item.WaitTxId = InvalidTxId;
        Self->PersistImportItemState(db, importInfo, itemIdx);

        Self->TxIdToImport.erase(txId);

        if (importInfo->State != EState::Waiting) {
            return;
        }

        switch (item.State) {
        case EState::CreateTable:
            item.State = EState::Transferring;
            AllocateTxId(importInfo, itemIdx);
            break;

        case EState::Transferring:
            if (const auto issue = GetIssues(item.DstPathId, txId)) {
                item.Issue = *issue;
                Cancel(importInfo, itemIdx, "issues during restore");
            } else {
                if (item.NextIndexIdx < item.Scheme.indexes_size()) {
                    item.State = EState::BuildIndexes;
                    AllocateTxId(importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                }
            }
            break;

        case EState::BuildIndexes:
            if (const auto issue = GetIssues(TIndexBuildId(ui64(txId)))) {
                item.Issue = *issue;
                Cancel(importInfo, itemIdx, "issues during index building");
            } else {
                if (++item.NextIndexIdx < item.Scheme.indexes_size()) {
                    AllocateTxId(importInfo, itemIdx);
                } else {
                    item.State = EState::Done;
                }
            }
            break;

        default:
            return SendNotificationsIfFinished(importInfo);
        }

        if (AllOf(importInfo->Items, &TImportInfo::TItem::IsDone)) {
            importInfo->State = EState::Done;
            importInfo->EndTime = TAppData::TimeProvider->Now();
        }

        Self->PersistImportItemState(db, importInfo, itemIdx);
        Self->PersistImportState(db, importInfo);

        SendNotificationsIfFinished(importInfo);
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
