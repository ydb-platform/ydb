#include "resource_pool_classifiers.h"

#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/fetcher.h>
#include <ydb/core/kqp/gateway/behaviour/resource_pool_classifier/snapshot.h>
#include <ydb/core/kqp/workload_service/actors/actors.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/services/metadata/service.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr {
namespace NSysView {

using namespace NActors;
using namespace NNodeWhiteboard;

class TResourcePoolClassifiersScan : public TScanActorBase<TResourcePoolClassifiersScan> {
public:
    using TBase  = TScanActorBase<TResourcePoolClassifiersScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TResourcePoolClassifiersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, 
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& database, bool reverse)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
        , UserToken(std::move(userToken))
        , Database(database)
        , Reverse(reverse)
    {
        const auto& cellsFrom = TableRange.From.GetCells();
        if (cellsFrom.size() == 1 && !cellsFrom[0].IsNull()) {
            From = TString{cellsFrom[0].Data(), cellsFrom[0].Size()};
        }

        const auto& cellsTo = TableRange.To.GetCells();
        if (cellsTo.size() == 1 && !cellsTo[0].IsNull()) {
            To = TString{cellsTo[0].Data(), cellsTo[0].Size()};
        }
    }

    STFUNC(StateScan) {
        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
                hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle)
                hFunc(NKqp::NWorkload::TEvFetchDatabaseResponse, Handle);
                hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
                cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
                cFunc(TEvents::TEvPoison::EventType, PassAway);
                default:
                    LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                        "NSysView::TResourcePoolClassifiersScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
            }
        } catch (...) {
            LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                "NSysView::TResourcePoolClassifiersScan: with exception %s", CurrentExceptionMessage().c_str());
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, CurrentExceptionMessage());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TResourcePoolClassifiersScan::StateScan);
        if (AckReceived) {
            StartScan();
        }
    }

    void StartScan() {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            ReplyEmptyAndDie();
        }
        Register(NKqp::NWorkload::CreateDatabaseFetcherActor(SelfId(), Database, UserToken, NACLib::EAccessRights::GenericFull));
    }

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

    void Handle(NKqp::NWorkload::TEvFetchDatabaseResponse::TPtr& ev) {
        auto& event = *ev->Get();
        if (event.Status != Ydb::StatusIds::SUCCESS) {
            ReplyErrorAndDie(event.Status, event.Issues.ToOneLineString());
            return;
        }
        Database = event.DatabaseId;
        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NKqp::TResourcePoolClassifierSnapshotsFetcher>()));
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        using TExtractor = std::function<TCell(const NKqp::TResourcePoolClassifierConfig&, TVector<TBuffer>&)>;
        using TSchema = Schema::ResourcePoolClassifiers;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::Name::ColumnId, [] (const NKqp::TResourcePoolClassifierConfig& config, TVector<TBuffer>&) {
                    return TCell(config.GetName().data(), config.GetName().size());
                }});
                insert({TSchema::Rank::ColumnId, [] (const NKqp::TResourcePoolClassifierConfig& config, TVector<TBuffer>&) {
                    return TCell::Make<i64>(config.GetRank());
                }});
                insert({TSchema::Config::ColumnId, [] (const NKqp::TResourcePoolClassifierConfig& config, TVector<TBuffer>& holder) {
                    TStringStream str;
                    NJson::WriteJson(&str, &config.GetConfigJson(), NJson::TJsonWriterConfig{});
                    const auto maybeBinaryJson = NBinaryJson::SerializeToBinaryJson(str.Str());
                    if (std::holds_alternative<TString>(maybeBinaryJson)) {
                        ythrow yexception() << "Can't serialize binary json value: " << std::get<TString>(maybeBinaryJson);
                    }
                    holder.emplace_back(std::move(std::get<NBinaryJson::TBinaryJson>(maybeBinaryJson)));
                    return TCell(holder.back().Data(), holder.back().Size());
                }});
            }
        };
        static TExtractorsMap extractors;

        const auto& snapshot = ev->Get()->GetSnapshotAs<NKqp::TResourcePoolClassifierSnapshot>();        
        const auto& config = snapshot->GetResourcePoolClassifierConfigs();
        auto resourcePoolsIt = config.find(Database);
        if (resourcePoolsIt == config.end()) {
            ReplyEmptyAndDie();
            return;
        }

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;
        // It's a mandatory condition to keep sorted PK here
        for (const auto& [name, config] : std::map(resourcePoolsIt->second.begin(), resourcePoolsIt->second.end())) {
            if (!IsInRange(name)) {
                continue;
            }
            TVector<TCell> cells;
            TVector<TBuffer> holder;
            for (auto column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(config, holder));
                }
            }
            TArrayRef<const TCell> ref(cells);
            batch->Rows.emplace_back(TOwnedCellVec::Make(ref));
        }
        if (Reverse) {
            std::reverse(batch->Rows.begin(), batch->Rows.end());
        }
        SendBatch(std::move(batch));
    }

    bool IsInRange(const TString& name) const {
        if ((From && name < From) || (!TableRange.FromInclusive && From && name == From)) {
            return false;
        }
        if ((To && To < name) || (!TableRange.ToInclusive && To && name == To)) {
            return false;
        }
        return true;
    }

private:
    TMaybe<TString> From;
    TMaybe<TString> To;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    TString Database;
    const bool Reverse;
};

THolder<NActors::IActor> CreateResourcePoolClassifiersScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& database, bool reverse)
{
    return MakeHolder<TResourcePoolClassifiersScan>(ownerId, scanId, tableId, tableRange, columns, std::move(userToken), database, reverse);
}

} // NSysView
} // NKikimr
