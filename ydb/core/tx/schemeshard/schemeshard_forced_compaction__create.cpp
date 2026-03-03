#include "schemeshard_forced_compaction.h"
#include "schemeshard_impl.h"

#define LOG_N(stream) LOG_NOTICE_S(ctx, NKikimrServices::FLAT_TX_SCHEMESHARD, "[" << Self->SelfTabletId() << "][ForcedCompaction] " << stream)

namespace NKikimr::NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxCreate: public TRwTxBase {
    explicit TTxCreate(TSelf* self, TEvForcedCompaction::TEvCreateRequest::TPtr& ev)
        : TRwTxBase(self)
        , Request(ev)
    {}

    void DoExecute(TTransactionContext &txc, const TActorContext &ctx) override {
        const auto& request = Request->Get()->Record;
        const auto& settings = request.GetSettings();
        LOG_N("TForcedCompaction::TTxCreate DoExecute " << request.ShortDebugString());

        auto response = MakeHolder<TEvForcedCompaction::TEvCreateResponse>(Request->Get()->Record.GetTxId());

        if (Self->IsServerlessDomain(TPath::Init(Self->RootPathId(), Self))) {
            return Reply(std::move(response), Ydb::StatusIds::PRECONDITION_FAILED, TStringBuilder()
                << "Forced compaction not allowed for serverless");
        }

        ui64 id = request.GetTxId();

        if (Self->ForcedCompactions.contains(id)) {
            return Reply(std::move(response), Ydb::StatusIds::ALREADY_EXISTS, TStringBuilder()
                << "Forced compaction with id '" << id << "' already exists");
        }

        const auto subdomainPath = TPath::Resolve(request.GetDatabaseName(), Self);
        {
            const auto checks = subdomainPath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsSubDomain()
                .NotUnderDomainUpgrade();

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, checks.GetError());
            }
        }

        const auto tablePath = TPath::Resolve(settings.source_path(), Self);
        {
            const auto checks = tablePath.Check();
            checks
                .IsAtLocalSchemeShard()
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTable()
                .IsCommonSensePath()
                .IsTheSameDomain(subdomainPath);

            if (!checks) {
                return Reply(std::move(response), Ydb::StatusIds::BAD_REQUEST, checks.GetError());
            }
        }

        auto info = MakeIntrusive<TForcedCompactionInfo>();
        info->Id = id;
        info->State = TForcedCompactionInfo::EState::InProgress;
        info->TablePathId = tablePath.Base()->PathId;
        info->SubdomainPathId = subdomainPath.Base()->PathId;
        info->Cascade = settings.cascade();
        info->MaxShardsInFlight = settings.max_shards_in_flight();
        info->StartTime = ctx.Now();
        if (request.HasUserSID()) {
            info->UserSID = request.GetUserSID();
        }

        if (info->Cascade) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::BAD_REQUEST,
                "'cascade = true' is not supported yet");
        }
        if (Self->InProgressForcedCompactionsByTable.contains(info->TablePathId)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                TStringBuilder() << "Forced compaction already in progress for table " << tablePath.PathString());
        }

        auto tableInfo = Self->Tables.at(info->TablePathId);
        if (tableInfo->GetPartitions().empty()) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::PRECONDITION_FAILED,
                "The table has no shards");
        }

        TVector<TShardIdx> shardsToCompact;
        shardsToCompact.reserve(tableInfo->GetPartitions().size());
        for (const auto& shardInfo : tableInfo->GetPartitions()) {
            shardsToCompact.push_back(shardInfo.ShardIdx);
        }
        info->TotalShardCount = shardsToCompact.size();

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistForcedCompactionState(db, *info);
        Self->PersistForcedCompactionShards(db, *info, shardsToCompact);

        Self->AddForcedCompaction(info);
        for (const auto& shardIdx : shardsToCompact) {
            Self->AddForcedCompactionShard(shardIdx, info);
        }
        Self->FromForcedCompactionInfo(*response->Record.MutableForcedCompaction(), *info);

        Reply(std::move(response));

        SideEffects.ApplyOnExecute(Self, txc, ctx);
    }

    void DoComplete(const TActorContext &ctx) override {
        LOG_N("TForcedCompaction::TTxCreate DoComplete");
        Self->ProcessForcedCompactionQueues();
        SideEffects.ApplyOnComplete(Self, ctx);
    }

private:
    void Reply(
        THolder<TEvForcedCompaction::TEvCreateResponse> response,
        const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
        const TString& errorMessage = TString())
    {
        auto& record = response->Record;
        record.SetStatus(status);
        if (errorMessage) {
            auto& issue = *record.MutableIssues()->Add();
            issue.set_severity(NYql::TSeverityIds::S_ERROR);
            issue.set_message(errorMessage);

        }

        SideEffects.Send(Request->Sender, std::move(response), 0, Request->Cookie);
    }

private:
    TSideEffects SideEffects;
    TEvForcedCompaction::TEvCreateRequest::TPtr Request;
};

ITransaction* TSchemeShard::CreateTxCreateForcedCompaction(TEvForcedCompaction::TEvCreateRequest::TPtr& ev) {
    return new TForcedCompaction::TTxCreate(this, ev);
}

} // namespace NKikimr::NSchemeShard
