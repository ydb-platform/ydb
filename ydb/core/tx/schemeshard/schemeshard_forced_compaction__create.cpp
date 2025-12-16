#include "schemeshard_impl.h"
#include "schemeshard_forced_compaction.h"

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

namespace NKikimr {
namespace NSchemeShard {

using namespace NTabletFlatExecutor;

struct TSchemeShard::TForcedCompaction::TTxCreate: public TSchemeShard::TXxport::TTxBase {
    TEvForcedCompaction::TEvCreateForcedCompactionRequest::TPtr Request;
    
    explicit TTxCreate(TSelf* self, TEvForcedCompaction::TEvCreateForcedCompactionRequest::TPtr& ev)
        : TXxport::TTxBase(self)
        , Request(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_FORCED_COMPACTION;
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext&) override {
        const auto& request = Request->Get()->Record;

        LOG_D("TForcedCompaction::TTxCreate: DoExecute");
        LOG_T("Message:\n" << request.ShortDebugString());

        auto response = MakeHolder<TEvForcedCompaction::TEvCreateForcedCompactionResponse>(request.GetTxId());

        const ui64 id = request.GetTxId();
        if (Self->ForcedCompactions.contains(id)) {
            return Reply(
                std::move(response),
                Ydb::StatusIds::ALREADY_EXISTS,
                TStringBuilder() << "Forced compaction with id '" << id << "' already exists"
            );
        }

        const TString& uid = GetUid(request.GetRequest().GetOperationParams());
        if (uid) {
            if (auto it = Self->ForcedCompactionsByUid.find(uid); it != Self->ForcedCompactionsByUid.end()) {
                if (IsSameDomain(it->second, request.GetDatabaseName())) {
                    Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), *it->second);
                    return Reply(std::move(response));
                } else {
                    return Reply(
                        std::move(response),
                        Ydb::StatusIds::ALREADY_EXISTS,
                        TStringBuilder() << "Forced compaction with uid '" << uid << "' already exists"
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
                return Reply(std::move(response), checks.GetStatus(), checks.GetError());
            }
        }

        const TPathId domainPathId = domainPath.Base()->PathId;
        const TOwnerId ownerPathId = request.GetRequest().GetOwnerPathId();
        const TLocalPathId localPathId = request.GetRequest().GetLocalPathId();
        const TPathId tablePathId(ownerPathId, localPathId);

        const TPath tablePath = TPath::Init(tablePathId, Self);
        {
            TPath::TChecker checks = tablePath.Check();
            checks
                .IsResolved()
                .NotDeleted()
                .NotUnderDeleting()
                .IsTable();

            if (!checks) {
                return Reply(std::move(response), checks.GetStatus(), checks.GetError());
            }
        }

        const TString& peerName = request.GetPeerName();
        TForcedCompactionInfo::TPtr compactionInfo = new TForcedCompactionInfo(id, uid, tablePathId, domainPathId, peerName);
        compactionInfo->State = TForcedCompactionInfo::EState::Waiting;
        compactionInfo->StartTime = TAppData::TimeProvider->Now();

        if (request.HasUserSID()) {
            compactionInfo->UserSID = request.GetUserSID();
        }

        // Get all shards of the table
        TTableInfo::TPtr tableInfo = Self->Tables.at(tablePathId);
        compactionInfo->TotalShards = tableInfo->GetPartitions().size();

        NIceDb::TNiceDb db(txc.DB);
        Self->PersistCreateForcedCompaction(db, *compactionInfo);

        Self->ForcedCompactions[id] = compactionInfo;
        if (uid) {
            Self->ForcedCompactionsByUid[uid] = compactionInfo;
        }

        // Enqueue all shards for compaction
        for (const auto& partition : tableInfo->GetPartitions()) {
            const TShardIdx& shardIdx = partition.ShardIdx;
            compactionInfo->QueuedShards.insert(shardIdx);
            Self->EnqueueForcedCompaction(shardIdx);
        }

        Self->FromXxportInfo(*response->Record.MutableResponse()->MutableEntry(), *compactionInfo);

        return Reply(std::move(response));
    }

    bool Reply(THolder<TEvForcedCompaction::TEvCreateForcedCompactionResponse> response, Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS, const TString& errorMessage = {}) {
        LOG_I("TTxCreate: Reply"
              << ", status: " << status
              << ", error: " << errorMessage);
        LOG_D("Message:\n" << response->Record.ShortDebugString());

        auto& entry = *response->Record.MutableResponse()->MutableEntry();
        entry.SetStatus(status);
        if (errorMessage) {
            AddIssue(entry.MutableIssues(), errorMessage);
        }

        Send(Request->Sender, std::move(response), 0, Request->Cookie);
        return true;
    }

}; // TTxCreate

ITransaction* TSchemeShard::CreateTxCreateForcedCompaction(TEvForcedCompaction::TEvCreateForcedCompactionRequest::TPtr& ev) {
    return new TForcedCompaction::TTxCreate(this, ev);
}

} // NSchemeShard
} // NKikimr
