#include "grpc_request_proxy.h"
#include "rpc_calls.h"
#include "rpc_common/rpc_common.h"
#include "rpc_request_base.h"

#include <ydb/public/api/protos/ydb_import.pb.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/io_formats/ydb_dump/csv_ydb_dump.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/public/udf/udf_types.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/vector.h>
#include <util/memory/pool.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace NKikimrIssues;
using namespace Ydb;

using TEvImportDataRequest = TGrpcRequestOperationCall<Ydb::Import::ImportDataRequest,
    Ydb::Import::ImportDataResponse>;

class TImportDataRPC: public TRpcRequestActor<TImportDataRPC, TEvImportDataRequest, true> {
    using TNavigate = NSchemeCache::TSchemeCacheNavigate;
    using TEvNavigate = TEvTxProxySchemeCache::TEvNavigateKeySet;
    using TResolve = NSchemeCache::TSchemeCacheRequest;
    using TEvResolve = TEvTxProxySchemeCache::TEvResolveKeySet;

    static constexpr TDuration MAX_TIMEOUT = TDuration::Minutes(5);

    static TVector<NScheme::TTypeInfo> MakeKeyColumnTypes(const TNavigate::TEntry& entry) {
        TVector<NScheme::TTypeInfo> result;

        for (const auto& [_, column] : entry.Columns) {
            if (column.KeyOrder < 0) {
                continue;
            }

            if (result.size() <= static_cast<ui32>(column.KeyOrder)) {
                result.resize(column.KeyOrder + 1);
            }

            result[column.KeyOrder] = column.PType;
        }

        return result;
    }

    static TSerializedTableRange GetFullRange(ui32 keyColumnsCount) {
        TVector<TCell> fromValues(keyColumnsCount);
        TVector<TCell> toValues;
        return TSerializedTableRange(fromValues, true, toValues, false);
    }

    static THolder<TKeyDesc> MakeKeyDesc(const TNavigate::TEntry& entry) {
        const TVector<NScheme::TTypeInfo> keyColumnTypes = MakeKeyColumnTypes(entry);
        return MakeHolder<TKeyDesc>(
            entry.TableId,
            GetFullRange(keyColumnTypes.size()).ToTableRange(),
            TKeyDesc::ERowOperation::Update,
            keyColumnTypes,
            TVector<TKeyDesc::TColumnOp>()
        );
    }

    static ui64 GetShardId(const TTableRange& range, const TKeyDesc* keyDesc) {
        Y_ABORT_UNLESS(range.Point);
        Y_ABORT_UNLESS(!keyDesc->GetPartitions().empty());

        TVector<TKeyDesc::TPartitionInfo>::const_iterator it = LowerBound(
            keyDesc->GetPartitions().begin(), keyDesc->GetPartitions().end(), true,
            [&](const TKeyDesc::TPartitionInfo& partition, bool) {
                const int cmp = CompareBorders<true, false>(
                    partition.Range->EndKeyPrefix.GetCells(), range.From,
                    partition.Range->IsInclusive || partition.Range->IsPoint,
                    range.InclusiveFrom || range.Point, keyDesc->KeyColumnTypes
                );

                return (cmp < 0);
            }
        );

        Y_ABORT_UNLESS(it != keyDesc->GetPartitions().end());
        return it->ShardId;
    }

    /// Get proxy services

    void GetProxyServices() {
        Send(MakeTxProxyID(), new TEvTxUserProxy::TEvGetProxyServicesRequest);
        Become(&TThis::StateGetProxyServices);
    }

    STATEFN(StateGetProxyServices) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvGetProxyServicesResponse, Handle);
        }
    }

    void Handle(TEvTxUserProxy::TEvGetProxyServicesResponse::TPtr& ev) {
        LeaderPipeCache = ev->Get()->Services.LeaderPipeCache;
        ResolvePath();
    }

    /// Resolve path

    void ResolvePath() {
        auto request = MakeHolder<TNavigate>();
        request->DatabaseName = NKikimr::CanonizePath(GetDatabaseName());

        auto& entry = request->ResultSet.emplace_back();
        entry.Operation = TNavigate::OpTable;
        entry.Path = NKikimr::SplitPath(GetProtoRequest()->path());

        Send(MakeSchemeCacheID(), new TEvNavigate(request.Release()));
        Become(&TThis::StateResolvePath);
    }

    STATEFN(StateResolvePath) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        if (request->ResultSet.empty()) {
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        const auto& entry = request->ResultSet.front();

        switch (entry.Status) {
        case TNavigate::EStatus::Ok:
            break;
        case TNavigate::EStatus::RootUnknown:
        case TNavigate::EStatus::PathErrorUnknown:
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::PATH_NOT_EXIST);
        case TNavigate::EStatus::LookupError:
        case TNavigate::EStatus::RedirectLookupError:
            return Reply(StatusIds::UNAVAILABLE, TIssuesIds::RESOLVE_LOOKUP_ERROR);
        default:
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        if (!CheckAccess(CanonizePath(entry.Path), entry.SecurityObject, NACLib::UpdateRow)) {
            return;
        }

        if (entry.Indexes) {
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::DEFAULT_ERROR,
                "Table should not have global secondary indexes");
        }

        for (const auto& [_, column] : entry.Columns) {
            Y_ABORT_UNLESS(Columns.emplace(column.Name, column).second);
        }

        KeyDesc = MakeKeyDesc(entry);
        ResolveKeys();
    }

    /// Resolve keys

    void ResolveKeys() {
        auto request = MakeHolder<TResolve>();
        request->DatabaseName = NKikimr::CanonizePath(GetDatabaseName());

        request->ResultSet.emplace_back(std::move(KeyDesc));
        request->ResultSet.back().Access = NACLib::UpdateRow;

        Send(MakeSchemeCacheID(), new TEvResolve(request.Release()));
        Become(&TThis::StateResolveKeys);
    }

    STATEFN(StateResolveKeys) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
        }
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr& ev) {
        const auto& request = ev->Get()->Request;

        if (request->ResultSet.empty()) {
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        auto& entry = request->ResultSet.front();
        KeyDesc = std::move(entry.KeyDescription);

        switch (entry.Status) {
        case TResolve::EStatus::OkData:
            break;
        case TResolve::EStatus::NotMaterialized:
        case TResolve::EStatus::PathErrorNotExist:
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::PATH_NOT_EXIST);
        case TResolve::EStatus::LookupError:
            return Reply(StatusIds::UNAVAILABLE, TIssuesIds::RESOLVE_LOOKUP_ERROR);
        default:
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        if (KeyDesc->GetPartitions().empty()) {
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::GENERIC_RESOLVE_ERROR);
        }

        ProcessData();
    }

    /// Process data

    void ProcessData() {
        const auto& request = *GetProtoRequest();

        if (request.data().empty()) {
            return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Empty data");
        }

        auto ev = MakeHolder<TEvDataShard::TEvUploadRowsRequest>();
        ev->Record.SetTableId(KeyDesc->TableId.PathId.LocalPathId);

        const auto timeout = request.operation_params().has_operation_timeout()
            ? Min(GetDuration(request.operation_params().operation_timeout()), MAX_TIMEOUT)
            : MAX_TIMEOUT;
        ev->Record.SetCancelDeadlineMs((AppData()->TimeProvider->Now() + timeout).MilliSeconds());

        TMaybe<ui64> shardId;
        switch (request.format_case()) {
        case Import::ImportDataRequest::kYdbDump:
            if (!FillColumnIds(request.ydb_dump().columns(), ev->Record)) {
                return;
            }
            shardId = ProcessData(request.ydb_dump(), request.data(), ev->Record);
            break;
        default:
            Y_ABORT("unreachable");
        }

        if (!shardId) {
            return;
        }

        Send(LeaderPipeCache, new TEvPipeCache::TEvForward(ev.Release(), *shardId, true), IEventHandle::FlagTrackDelivery);
        Become(&TThis::StateProcessData);
    }

    template <typename TCont>
    bool FillColumnIds(const TCont& columnNames, NKikimrTxDataShard::TEvUploadRowsRequest& request) {
        auto& rowScheme = *request.MutableRowScheme();

        for (const auto& column : columnNames) {
            const auto* info = Columns.FindPtr(column);
            if (!info) {
                Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown column: " << column);
                return false;
            }

            if (info->KeyOrder != -1) {
                auto& ids = *rowScheme.MutableKeyColumnIds();

                if (ids.size() < (info->KeyOrder + 1)) {
                    ids.Resize(info->KeyOrder + 1, 0);
                }

                ids[info->KeyOrder] = info->Id;
            } else {
                rowScheme.AddValueColumnIds(info->Id);
            }
        }

        return true;
    }

    TMaybe<ui64> ProcessData(const Import::YdbDumpFormat& format, TStringBuf data, NKikimrTxDataShard::TEvUploadRowsRequest& request) {
        TMaybe<ui64> shardId;

        TMaybe<TOwnedCellVec> prevKey;
        TMemoryPool pool(256);

        std::vector<std::pair<i32, NScheme::TTypeInfo>> columnOrderTypes; // {keyOrder, PType}
        columnOrderTypes.reserve(format.columns().size());
        for (const auto& column : format.columns()) {
            const auto* info = Columns.FindPtr(column);
            if (!info) {
                Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unknown column: " << column);
                return Nothing();
            }
            columnOrderTypes.emplace_back(info->KeyOrder, info->PType);
        }

        while (data) {
            pool.Clear();

            TStringBuf line = data.NextTok('\n');
            const TStringBuf origLine = line;

            if (!line) {
                if (data) {
                    continue;
                }

                break;
            }

            TVector<TCell> keys;
            TVector<TCell> values;
            TString strError;
            ui64 numBytes = 0;

            if (!NFormats::TYdbDump::ParseLine(line, columnOrderTypes, pool, keys, values, strError, numBytes)) {
                TString message = TStringBuilder() << strError << " on line: " << origLine;
                Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, message);
                return Nothing();
            }

            Y_ABORT_UNLESS(!keys.empty());

            // sorting constraint
            if (!CheckSorted(prevKey, keys)) {
                return Nothing();
            }

            prevKey = TOwnedCellVec::Make(keys);

            // same partition constraint
            if (!shardId) {
                shardId = GetShardId(TTableRange(keys), KeyDesc.Get());
            }

            if (!CheckSameShard(*shardId, keys)) {
                return Nothing();
            }

            // ok
            auto& row = *request.AddRows();
            row.SetKeyColumns(TSerializedCellVec::Serialize(keys));
            row.SetValueColumns(TSerializedCellVec::Serialize(values));
        }

        if (!shardId) {
            Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, "Empty shard id");
        }

        return shardId;
    }

    bool CheckSorted(const TMaybe<TOwnedCellVec>& prevKey, const TVector<TCell>& key) {
        if (!prevKey) {
            return true;
        }

        const int cmp = CompareTypedCellVectors(prevKey->data(), key.data(),
            KeyDesc->KeyColumnTypes.data(), prevKey->size(), key.size());
        if (cmp <= 0) {
            return true;
        }

        Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Keys must be sorted");
        return false;
    }

    bool CheckSameShard(ui64 prevShardId, const TVector<TCell>& key) {
        const ui64 shardId = GetShardId(TTableRange(key), KeyDesc.Get());
        if (prevShardId == shardId) {
            return true;
        }

        Reply(StatusIds::PRECONDITION_FAILED, TIssuesIds::DEFAULT_ERROR, "All keys must be from the same partition");
        return false;
    }

    STATEFN(StateProcessData) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDataShard::TEvUploadRowsResponse, Handle);
            hFunc(TEvPipeCache::TEvDeliveryProblem, Handle);
            hFunc(TEvents::TEvUndelivered, Handle);
        }
    }

    void Handle(TEvDataShard::TEvUploadRowsResponse::TPtr& ev) {
        const auto& record = ev->Get()->Record;

        switch (record.GetStatus()) {
        case NKikimrTxDataShard::TError::OK:
            return Reply(MakeOperation(StatusIds::SUCCESS));
        case NKikimrTxDataShard::TError::SCHEME_ERROR:
            return Reply(StatusIds::SCHEME_ERROR, TIssuesIds::DEFAULT_ERROR, record.GetErrorDescription());
        case NKikimrTxDataShard::TError::BAD_ARGUMENT:
            return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, record.GetErrorDescription());
        case NKikimrTxDataShard::TError::EXECUTION_CANCELLED:
            return Reply(StatusIds::TIMEOUT, TIssuesIds::DEFAULT_ERROR, record.GetErrorDescription());
        case NKikimrTxDataShard::TError::WRONG_SHARD_STATE:
            return Reply(StatusIds::OVERLOADED, TIssuesIds::DEFAULT_ERROR, record.GetErrorDescription());
        default:
            return Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, record.GetErrorDescription());
        };
    }

    void Handle(TEvPipeCache::TEvDeliveryProblem::TPtr& ev) {
        Reply(StatusIds::UNAVAILABLE, TIssuesIds::SHARD_NOT_AVAILABLE,
            TStringBuilder() << "Failed to connect to shard: " << ev->Get()->TabletId);
    }

    void Handle(TEvents::TEvUndelivered::TPtr&) {
        Reply(StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, "Pipe cache is not available");
    }

public:
    using TRpcRequestActor<TImportDataRPC, TEvImportDataRequest, true>::TRpcRequestActor;

    void Bootstrap() {
        switch (GetProtoRequest()->format_case()) {
        case Import::ImportDataRequest::kYdbDump:
            break;
        default:
            return Reply(StatusIds::BAD_REQUEST, TIssuesIds::DEFAULT_ERROR, "Unknown format");
        }

        GetProxyServices();
    }

private:
    TActorId LeaderPipeCache;
    THashMap<TString, TSysTables::TTableColumnInfo> Columns;
    THolder<TKeyDesc> KeyDesc;

}; // TImportDataRPC

void DoImportDataRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TImportDataRPC(p.release()));
}

} // namespace NGRpcService
} // namespace NKikimr
