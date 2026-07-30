#include "resource_pool_classifiers.h"

#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/fetcher.h>
#include <ydb/services/workload_manager/metadata_subscription/resource_pool_classifier/snapshot.h>
#include <ydb/services/workload_manager/actors/actors.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/registry.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
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

class TResourcePoolClassifiersScan : public TScanActorWithoutBackPressure<TResourcePoolClassifiersScan> {
public:
    using TBase = TScanActorWithoutBackPressure<TResourcePoolClassifiersScan>;

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TResourcePoolClassifiersScan(const NActors::TActorId& ownerId, ui32 scanId,
        const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse)
        : TBase(ownerId, scanId, database, sysViewInfo, tableRange, columns)
        , UserToken(std::move(userToken))
        , Reverse(reverse)
    {}

    STFUNC(StateScan) {
        try {
            switch (ev->GetTypeRewrite()) {
                sFunc(NKqp::TEvKqpCompute::TEvScanDataAck, HandleAck);
                hFunc(NMetadata::NProvider::TEvRefreshSubscriberData, Handle)
                hFunc(NWorkloadManager::TEvFetchDatabaseResponse, Handle);
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
    void StartScan() final {
        if (!NMetadata::NProvider::TServiceOperator::IsEnabled()) {
            ReplyEmptyAndDie();
        }
        Register(NWorkloadManager::CreateDatabaseFetcherActor(SelfId(), TenantName, UserToken, NACLib::EAccessRights::GenericUse));
    }

    void Handle(NWorkloadManager::TEvFetchDatabaseResponse::TPtr& ev) {
        auto& event = *ev->Get();
        if (event.Status != Ydb::StatusIds::SUCCESS) {
            ReplyErrorAndDie(event.Status, event.Issues.ToOneLineString());
            return;
        }
        DatabaseId = event.DatabaseId;
        Send(NMetadata::NProvider::MakeServiceId(SelfId().NodeId()), new NMetadata::NProvider::TEvAskSnapshot(std::make_shared<NWorkloadManager::TResourcePoolClassifierSnapshotsFetcher>()));
    }

    void Handle(NMetadata::NProvider::TEvRefreshSubscriberData::TPtr& ev) {
        using TExtractor = std::function<TCell(const NWorkloadManager::TResourcePoolClassifierConfig&)>;
        using TSchema = Schema::ResourcePoolClassifiers;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::Name::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    return TCell(config.GetName().data(), config.GetName().size());
                }});
                insert({TSchema::Rank::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    return TCell::Make<i64>(config.GetRank());
                }});
                insert({TSchema::MemberName::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.MemberName.has_value()) {
                        const auto& memberName = settings.MemberName.value();
                        return TCell(memberName.data(), memberName.size());
                    }
                    return TCell();
                }});
                insert({TSchema::ResourcePool::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.ResourcePool.has_value()) {
                        const auto& resourcePool = settings.ResourcePool.value();
                        return TCell(resourcePool.data(), resourcePool.size());
                    }
                    return TCell();
                }});
                insert({TSchema::HasAppName::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.HasAppName.has_value()) {
                        const auto& hasAppName = settings.HasAppName.value();
                        return TCell(hasAppName.data(), hasAppName.size());
                    }
                    return TCell();
                }});
                insert({TSchema::Action::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.Action.has_value()
                        && *settings.Action == NResourcePool::EClassifierAction::Reject)
                    {
                        return TCell(ToString(NResourcePool::EClassifierAction::Reject));
                    }
                    return TCell();
                }});
                insert({TSchema::HasFullScan::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.HasFullScan.has_value()) {
                        const auto& hasFullScan = settings.HasFullScan->Pattern;
                        return TCell(hasFullScan.data(), hasFullScan.size());
                    }
                    return TCell();
                }});
                insert({TSchema::HasPath::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.HasPath.has_value()) {
                        const auto& hasPath = settings.HasPath->Pattern;
                        return TCell(hasPath.data(), hasPath.size());
                    }
                    return TCell();
                }});
                insert({TSchema::HasStream::ColumnId, [] (const NWorkloadManager::TResourcePoolClassifierConfig& config) {
                    const auto& settings = config.GetClassifierSettings();
                    if (settings.HasStream.has_value()) {
                        return TCell::Make<bool>(settings.HasStream.value());
                    } else {
                        return TCell();
                    }
                }});
            }
        };
        static TExtractorsMap extractors;

        const auto& snapshot = ev->Get()->GetSnapshotAs<NWorkloadManager::TResourcePoolClassifierSnapshot>();
        const auto& config = snapshot->GetResourcePoolClassifierConfigs();
        auto resourcePoolsIt = config.find(DatabaseId);
        if (resourcePoolsIt == config.end()) {
            ReplyEmptyAndDie();
            return;
        }

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;
        // It's a mandatory condition to keep sorted PK here
        for (const auto& [name, config] : std::map(resourcePoolsIt->second.ByName.begin(), resourcePoolsIt->second.ByName.end())) {
            if (!StringKeyIsInTableRange({name})) {
                continue;
            }
            TVector<TCell> cells;
            for (auto column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(config));
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

private:
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const bool Reverse;
    TString DatabaseId;
};

THolder<NActors::IActor> CreateResourcePoolClassifiersScan(const NActors::TActorId& ownerId, ui32 scanId,
    const TString& database, const NKikimrSysView::TSysViewDescription& sysViewInfo,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
    TIntrusiveConstPtr<NACLib::TUserToken> userToken, bool reverse)
{
    return MakeHolder<TResourcePoolClassifiersScan>(ownerId, scanId, database, sysViewInfo, tableRange, columns,
        std::move(userToken), reverse);
}

} // NSysView
} // NKikimr
