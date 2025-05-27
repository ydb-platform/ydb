#include "resource_pools.h"

#include <ydb/core/sys_view/common/common.h>
#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/sys_view/common/scan_actor_base_impl.h>
#include <ydb/core/sys_view/common/schema.h>
#include <ydb/library/table_creator/table_creator.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/json/json_writer.h>

#include <yql/essentials/types/binary_json/read.h>
#include <yql/essentials/types/binary_json/write.h>

namespace NKikimr {
namespace NSysView {

using namespace NActors;

class TResourcePoolsScan : public TScanActorBase<TResourcePoolsScan> {
public:
    using TBase  = TScanActorBase<TResourcePoolsScan>;

    enum class EState {
        LIST_RESOURCE_POOLS,
        DESCRIBE_RESOURCE_POOLS
    };

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::KQP_SYSTEM_VIEW_SCAN;
    }

    TResourcePoolsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
        const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns,
        TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& database, bool reverse)
        : TBase(ownerId, scanId, tableId, tableRange, columns)
        , UserToken(std::move(userToken))
        , Database(database)
        , Reverse(reverse)
    {
    }

    STFUNC(StateScan) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqpCompute::TEvScanDataAck, Handle);
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleAbortExecution);
            cFunc(TEvents::TEvWakeup::EventType, HandleTimeout);
            cFunc(TEvents::TEvPoison::EventType, PassAway);
            default:
                LOG_CRIT(*TlsActivationContext, NKikimrServices::SYSTEM_VIEWS,
                    "NSysView::TResourcePoolsScan: unexpected event 0x%08" PRIx32, ev->GetTypeRewrite());
        }
    }

private:
    void ProceedToScan() override {
        Become(&TResourcePoolsScan::StateScan);
        if (AckReceived) {
            StartScan();
        }
    }

    void StartScan() {
        SendRequestToSchemeCache({{".metadata/workload_manager", "pools"}},  NSchemeCache::TSchemeCacheNavigate::OpList);
    }

    void SendRequestToSchemeCache(const TVector<TVector<TString>>& pathsComponents, NSchemeCache::TSchemeCacheNavigate::EOp operation) {
        auto event = NTableCreator::BuildSchemeCacheNavigateRequest(
            pathsComponents,
            Database,
            UserToken
        );
        for (auto& resultSet : event->ResultSet) {
            resultSet.Access |= NACLib::SelectRow;
            resultSet.Operation = operation;
        }
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(event.Release()), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        switch (State) {
            case EState::LIST_RESOURCE_POOLS:
                ProcessListResourcePools(ev);
                return;
            case EState::DESCRIBE_RESOURCE_POOLS:
                ProcessDescribeResourcePools(ev);
                return;
        }
    }

    void ProcessListResourcePools(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        if (results.size() != 1) {
            ReplyErrorAndDie(Ydb::StatusIds::INTERNAL_ERROR, "Unexpected scheme cache response");
            return;
        }

        const auto& result = results[0];
        switch (result.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid path to resource pools");
                return;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for listings resource pools");
                return;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                ReplyEmptyAndDie();
                return;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                return;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                State = EState::DESCRIBE_RESOURCE_POOLS;
                TVector<TVector<TString>> pathsComponents;
                for (const auto& child : result.ListNodeEntry->Children) {
                    pathsComponents.push_back({".metadata/workload_manager/pools", child.Name});
                }
                SendRequestToSchemeCache(pathsComponents, NSchemeCache::TSchemeCacheNavigate::EOp::OpPath);
                return;
        }
    }

    void ProcessDescribeResourcePools(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& results = ev->Get()->Request->ResultSet;
        TMap<TString, TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>> resourcePools;
        for (const auto& result : results) {
            switch (result.Status) {
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                    ReplyErrorAndDie(Ydb::StatusIds::BAD_REQUEST, TStringBuilder() << "Invalid resource pool id");
                    return;
                case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                    ReplyErrorAndDie(Ydb::StatusIds::UNAUTHORIZED, TStringBuilder() << "You don't have access permissions for describe resource pools");
                    return;
                case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                    ReplyErrorAndDie(Ydb::StatusIds::NOT_FOUND, TStringBuilder() << "Resource pools not found or you don't have access permissions");
                    return;
                case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
                case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
                    ReplyErrorAndDie(Ydb::StatusIds::UNAVAILABLE, TStringBuilder() << "Retry limit exceeded on scheme error: " << result.Status);
                    return;
                case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                    const auto& name = result.ResourcePoolInfo->Description.GetName();
                    resourcePools[name] = result.ResourcePoolInfo;                    
            }
        }

        SendResponse(resourcePools);
    }

    void SendResponse(const TMap<TString, TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>>& resourcePools) {
        using TExtractor = std::function<TCell(const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>&)>;
        using TSchema = Schema::ResourcePools;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::Name::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& name = resourcePool->Description.GetName();
                    return TCell(name.data(), name.size());
                }});
                insert({TSchema::ConcurrentQueryLimit::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("concurrent_query_limit");
                    return it == properties.end() ? TCell::Make<i32>(-1) : TCell::Make<i32>(std::stoi(it->second));
                }});
                insert({TSchema::QueueSize::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("queue_size");
                    return it == properties.end() ? TCell::Make<i32>(-1) : TCell::Make<i32>(std::stoi(it->second));
                }});
                insert({TSchema::DatabaseLoadCpuThreshold::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("database_load_cpu_threshold");
                    return it == properties.end() ? TCell::Make<double>(-1) : TCell::Make<double>(std::stod(it->second));
                }});
                insert({TSchema::ResourceWeight::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("resource_weight");
                    return it == properties.end() ? TCell::Make<double>(-1) : TCell::Make<double>(std::stod(it->second));
                }});
                insert({TSchema::TotalCpuLimitPercentPerNode::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("total_cpu_limit_percent_per_node");
                    return it == properties.end() ? TCell::Make<double>(-1) : TCell::Make<double>(std::stod(it->second));
                }});
                insert({TSchema::QueryCpuLimitPercentPerNode::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("query_cpu_limit_percent_per_node");
                    return it == properties.end() ? TCell::Make<double>(-1) : TCell::Make<double>(std::stod(it->second));
                }});
                insert({TSchema::QueryMemoryLimitPercentPerNode::ColumnId, [] (const TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo>& resourcePool) {
                    const auto& properties = resourcePool->Description.GetProperties().GetProperties();
                    auto it = properties.find("query_memory_limit_percent_per_node");
                    return it == properties.end() ? TCell::Make<double>(-1) : TCell::Make<double>(std::stod(it->second));
                }});
            }
        };
        static TExtractorsMap extractors;

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;
        // It's a mandatory condition to keep sorted PK here
        for (const auto& [name, info] : resourcePools) {
            if (!StringKeyIsInTableRange({name})) {
                continue;
            }
            TVector<TCell> cells;
            for (auto column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(info));
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

    void Handle(NKqp::TEvKqpCompute::TEvScanDataAck::TPtr&) {
        StartScan();
    }

private:
    EState State = EState::LIST_RESOURCE_POOLS;
    const TIntrusiveConstPtr<NACLib::TUserToken> UserToken;
    const TString Database;
    const bool Reverse;
};

THolder<NActors::IActor> CreateResourcePoolsScan(const NActors::TActorId& ownerId, ui32 scanId, const TTableId& tableId,
    const TTableRange& tableRange, const TArrayRef<NMiniKQL::TKqpComputeContextBase::TColumn>& columns, TIntrusiveConstPtr<NACLib::TUserToken> userToken, const TString& database, bool reverse)
{
    return MakeHolder<TResourcePoolsScan>(ownerId, scanId, tableId, tableRange, columns, std::move(userToken), database, reverse);
}

} // NSysView
} // NKikimr
