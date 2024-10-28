#include "database.h"

#include <ydb/core/statistics/events.h>
#include <ydb/core/statistics/common.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>


namespace NKikimr::NStat {

constexpr ui32 StatisticsTableDataColumn = 5;

class TLoadStatisticsActor : public TActorBootstrapped<TLoadStatisticsActor> {
private:
    const ui64 QueryId;
    const NActors::TActorId ReplyActorId;
    const TString Database;
    const TPathId PathId;
    const ui64 StatType;
    const ui32 ColumnTag;
    const ui64 Cookie;

    using TNavigate = NSchemeCache::TSchemeCacheNavigate;

public:
    TLoadStatisticsActor(ui64 queryId, const NActors::TActorId& replyActorId, const TString& database,
        const TPathId& pathId, ui64 statType, ui32 columnTag, ui64 cookie)
        : QueryId(queryId)
        , ReplyActorId(replyActorId)
        , Database(database)
        , PathId(pathId)
        , StatType(statType)
        , ColumnTag(columnTag)
        , Cookie(cookie)
    {}

    void Bootstrap() {
        auto path = SplitPath(Database);
        path.push_back(".metadata");
        path.push_back("_statistics");

        auto navigateRequest = std::make_unique<TNavigate>();
        auto& entry = navigateRequest->ResultSet.emplace_back();
        entry.Path = path;
        entry.Operation = TNavigate::EOp::OpTable;
        entry.RequestType = TNavigate::TEntry::ERequestType::ByPath;
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(navigateRequest.release()));

        Become(&TLoadStatisticsActor::StateWork);
    }

    STFUNC(StateWork) {
        switch(ev->GetTypeRewrite()) {
                hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
                hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
                hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
                hFunc(TEvDataShard::TEvReadResult, Handle);
            default:
                SA_LOG_CRIT("[TLoadStatisticsActor] QueryId["
                    << QueryId << "] Unexpected event# " << ev->GetTypeRewrite() << " " << ev->ToString());
        }
    }

    void OnError() {
        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

        auto response = std::make_unique<TEvStatistics::TEvLoadStatisticsQueryResponse>();
        response->Success = false;
        response->Cookie = Cookie;

        Send(ReplyActorId, response.release());
        PassAway();        
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        SA_LOG_D("[TLoadStatisticsActor] TEvTxProxySchemeCache::TEvNavigateKeySetResult QueryId[ "
            << QueryId << " ]");

        std::unique_ptr<TNavigate> navigate(ev->Get()->Request.Release());
        Y_ABORT_UNLESS(navigate->ResultSet.size() == 1);

        const auto& entry = navigate->ResultSet.front();
        if (entry.Status != TNavigate::EStatus::Ok) {
            SA_LOG_E("[TLoadStatisticsActor] TEvTxProxySchemeCache::TEvNavigateKeySetResult QueryId[ "
                << QueryId << " ] ErrorStatus[ " << static_cast<ui64>(entry.Status) << " ]");

            OnError();
            return;
        }

        auto resolveRequest = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        resolveRequest->ResultSet.emplace_back(MakeHolder<TKeyDesc>(
            TTableId(entry.TableId.PathId),
            TTableRange({}),
            TKeyDesc::ERowOperation::Unknown,
            TVector<NScheme::TTypeInfo>(), TVector<TKeyDesc::TColumnOp>()
        ));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(resolveRequest.release()));
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        SA_LOG_D("[TLoadStatisticsActor] TEvTxProxySchemeCache::TEvResolveKeySetResult QueryId[ "
            << QueryId << " ]");

        const auto request = ev->Get()->Request.Get();
        Y_ABORT_UNLESS(request->ResultSet.size() == 1);

        const auto& entry = request->ResultSet[0];

        if (entry.Status != NSchemeCache::TSchemeCacheRequest::EStatus::OkData) {
            SA_LOG_E("[TLoadStatisticsActor] TEvTxProxySchemeCache::TEvResolveKeySetResult QueryId[ "
                << QueryId << " ], ErrorStatus[ " << static_cast<ui64>(entry.Status) << " ]");

            OnError();
            return;
        }

        const auto recipient = MakePipePerNodeCacheID(false);
        const auto& partitions = entry.KeyDescription->GetPartitions();

        SA_LOG_D("[TLoadStatisticsActor] TEvTxProxySchemeCache::TEvResolveKeySetResult QueryId[ "
                << QueryId << " ], PartitionsSize[ " << partitions.size() << " ] " );

        if (partitions.empty()) {
            SA_LOG_E("[TLoadStatisticsActor] TEvTxProxySchemeCache::TEvResolveKeySetResult QueryId[ "
                << QueryId << " ] partitions collection is empty");

            OnError();
            return;           
        }

        TVector<TCell> key;
        key.emplace_back(TCell::Make<ui64>(PathId.OwnerId));
        key.emplace_back(TCell::Make<ui64>(PathId.LocalPathId));
        key.emplace_back(TCell::Make<ui32>(StatType));
        key.emplace_back(TCell::Make<ui32>(ColumnTag));

        ui64 shardId = partitions[0].ShardId;

        if (partitions.size() > 1) {
            const auto& keyColumnTypes = entry.KeyDescription->KeyColumnTypes;
            auto it = std::lower_bound(partitions.begin(), partitions.end(), key,
                [&keyColumnTypes](const auto& partition, const auto& key) {
                    const auto& range = *partition.Range;
                    const int cmp = CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                        range.IsInclusive || range.IsPoint, true, keyColumnTypes);
                    return (cmp < 0);
                }
            );

            if (it == partitions.end()) {
                SA_LOG_E("[TLoadStatisticsActor] QueryId[ "
                    << QueryId << " ], PathId[ " << PathId << " ], StatType[ "
                    << StatType << " ], ColumnTag[ " << ColumnTag << " ] partition not found");

                OnError();
                return;
            }

            shardId = it->ShardId;
        }
        
        SA_LOG_D("[TLoadStatisticsActor] QueryId[ "
            << QueryId << " ] TabletId[ " << shardId << " ] send TEvRead");

        const auto& pathId = entry.KeyDescription->TableId.PathId;
        auto evRead = std::make_unique<TEvDataShard::TEvRead>();
        evRead->Keys.emplace_back(std::move(key));

        auto& record = evRead->Record;
        record.SetReadId(1);
        record.MutableTableId()->SetOwnerId(pathId.OwnerId);
        record.MutableTableId()->SetTableId(pathId.LocalPathId);
        record.AddColumns(StatisticsTableDataColumn);
        record.SetResultFormat(NKikimrDataEvents::FORMAT_CELLVEC);

        Send(recipient, new TEvPipeCache::TEvForward(evRead.release(), shardId, true), IEventHandle::FlagTrackDelivery);
    }

    void Handle(TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto readResult = ev->Get();
        const auto& record = readResult->Record;
        SA_LOG_D("[TLoadStatisticsActor] TEvDataShard::TEvReadResult QueryId[ " << QueryId << " ]");

        if (record.HasStatus() 
                && record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            SA_LOG_E("[TLoadStatisticsActor] TEvDataShard::TEvReadResult QueryId[ "
                << QueryId << " ] ErrorStatus[ " << static_cast<ui64>(record.GetStatus().GetCode()) << " ]");

            OnError();
            return;
        }

        const auto rowsCount = readResult->GetRowsCount();
        if (rowsCount != 1) {
            SA_LOG_E("[TLoadStatisticsActor] QueryId[ "
                << QueryId << " ] RowsCount[ " << rowsCount << " ] incorrect number of rows");

            OnError();
            return;
        }

        const auto& row = readResult->GetCells(0);
        if (row.empty()) {
            SA_LOG_E("[TLoadStatisticsActor] QueryId[ " << QueryId << " ] result row is empty");

            OnError();
            return;            
        }

        Send(MakePipePerNodeCacheID(false), new TEvPipeCache::TEvUnlink(0));

        auto response = std::make_unique<TEvStatistics::TEvLoadStatisticsQueryResponse>();
        response->Success = true;
        response->Status = Ydb::StatusIds::SUCCESS;
        response->Data = row[0].AsBuf();
        response->Cookie = Cookie;

        Send(ReplyActorId, response.release());
        PassAway();
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        const auto tabletId = ev->Get()->TabletId;

        SA_LOG_D("[TLoadStatisticsActor] TEvPipeCache::TEvDeliveryProblem QueryId[ "
            << QueryId << " ] TabletId[ " << tabletId << " ]");
        
        OnError();
    }
};

NActors::IActor* CreateLoadStatisticsActor(ui64 queryId, const NActors::TActorId& replyActorId, const TString& database,
    const TPathId& pathId, ui64 statType, ui32 columnTag, ui64 cookie)
{
    return new TLoadStatisticsActor(queryId, replyActorId, database, pathId, statType, columnTag, cookie);
}

} // NKikimr::NStat
