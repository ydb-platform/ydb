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

    struct TResourcePoolDescription {
        TIntrusiveConstPtr<NSchemeCache::TSchemeCacheNavigate::TResourcePoolInfo> Info;
        TString Owner;
        TVector<std::pair<TString, TString>> Permissions;
        TVector<std::pair<TString, TString>> EffectivePermissions;
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
        TMap<TString, TResourcePoolDescription> resourcePools;
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
                    resourcePools[name] = { result.ResourcePoolInfo, result.SecurityObject->GetOwnerSID(), ConvertACLToString(result.SecurityObject->GetACL(), false), ConvertACLToString(result.SecurityObject->GetACL(), true)};                    
            }
        }

        SendResponse(resourcePools);
    }

    void SendResponse(const TMap<TString, TResourcePoolDescription>& resourcePools) {
        using TExtractor = std::function<TCell(const std::pair<TString, TResourcePoolDescription>&, TVector<TBuffer>&)>;
        using TSchema = Schema::ResourcePools;

        struct TExtractorsMap : public THashMap<NTable::TTag, TExtractor> {
            TExtractorsMap() {
                insert({TSchema::Name::ColumnId, [] (const std::pair<TString, TResourcePoolDescription>& resourcePool, TVector<TBuffer>&) {
                    return TCell(resourcePool.first.data(), resourcePool.first.size());
                }});
                insert({TSchema::Config::ColumnId, [] (const std::pair<TString, TResourcePoolDescription>& resourcePool, TVector<TBuffer>& holder) {
                    NJson::TJsonValue jsonMap;
                    for (const auto& [key, value] : resourcePool.second.Info->Description.GetProperties().GetProperties()) {
                        TString upperKey = key;
                        upperKey.to_upper();
                        jsonMap[upperKey] = value;
                    }
                    CreateJsonColumn(jsonMap, holder);
                    return TCell(holder.back().Data(), holder.back().Size());
                }});
                insert({TSchema::Owner::ColumnId, [] (const std::pair<TString, TResourcePoolDescription>& resourcePool, TVector<TBuffer>&) {
                    return TCell(resourcePool.second.Owner.data(), resourcePool.second.Owner.size());
                }});
                insert({TSchema::Permissions::ColumnId, [] (const std::pair<TString, TResourcePoolDescription>& resourcePool, TVector<TBuffer>& holder) {
                    CreatePermissionsColumn(resourcePool.second.Permissions, holder);
                    return TCell(holder.back().Data(), holder.back().Size());
                }});
                insert({TSchema::EffectivePermissions::ColumnId, [] (const std::pair<TString, TResourcePoolDescription>& resourcePool, TVector<TBuffer>& holder) {
                    CreatePermissionsColumn(resourcePool.second.EffectivePermissions, holder);
                    return TCell(holder.back().Data(), holder.back().Size());
                }});
            }
        };
        static TExtractorsMap extractors;

        auto batch = MakeHolder<NKqp::TEvKqpCompute::TEvScanData>(ScanId);
        batch->Finished = true;
        // It's a mandatory condition to keep sorted PK here
        for (const auto& p : resourcePools) {
            if (!StringKeyIsInTableRange({p.first})) {
                continue;
            }
            TVector<TCell> cells;
            TVector<TBuffer> holder;
            for (auto column : Columns) {
                auto extractor = extractors.find(column.Tag);
                if (extractor == extractors.end()) {
                    cells.push_back(TCell());
                } else {
                    cells.push_back(extractor->second(p, holder));
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

    static void CreateJsonColumn(const NJson::TJsonValue& jsonValue, TVector<TBuffer>& holder) {
        TStringStream str;
        NJson::WriteJson(&str, &jsonValue, NJson::TJsonWriterConfig{});
        const auto maybeBinaryJson = NBinaryJson::SerializeToBinaryJson(str.Str());
        if (std::holds_alternative<TString>(maybeBinaryJson)) {
            ythrow yexception() << "Can't serialize binary json value: " << std::get<TString>(maybeBinaryJson);
        }
        holder.emplace_back(std::move(std::get<NBinaryJson::TBinaryJson>(maybeBinaryJson)));
    }

    static void CreatePermissionsColumn(const TVector<std::pair<TString, TString>>& permissions, TVector<TBuffer>& holder) {
        NJson::TJsonValue jsonArray;
        jsonArray.SetType(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& [sid, permission] : permissions) {
            NJson::TJsonValue json;
            json["SID"] = sid;
            json["Permission"] = permission;
            jsonArray.AppendValue(json);
        }
        CreateJsonColumn(jsonArray, holder);
    }

private:
    EState State = EState::LIST_RESOURCE_POOLS;
    TMaybe<TString> From;
    TMaybe<TString> To;
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
