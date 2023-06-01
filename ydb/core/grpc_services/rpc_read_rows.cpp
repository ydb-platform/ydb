#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common.h"
#include "service_table.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/public/udf/udf_types.h>
#include <ydb/library/yql/minikql/dom/yson.h>
#include <ydb/library/yql/minikql/dom/json.h>
#include <ydb/library/yql/utils/utf8.h>
#include <ydb/library/yql/public/decimal/yql_decimal.h>

#include <ydb/library/binary_json/write.h>
#include <ydb/library/dynumber/dynumber.h>

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/string/vector.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NGRpcService {

using namespace NActors;
using namespace Ydb;

namespace {

TVector<std::pair<TString, Ydb::Type>> GetRequestColumns(const Ydb::Table::ReadRowsRequest* proto) {
    const auto& type = proto->Getkeys().Gettype();
    const auto& rowFields = type.Getlist_type().Getitem().Getstruct_type().Getmembers();

    TVector<std::pair<TString, Ydb::Type>> result;
    result.reserve(rowFields.size());
    for (i32 pos = 0; pos < rowFields.size(); ++pos) {
        const auto& name = rowFields[pos].Getname();
        const auto& typeInProto = rowFields[pos].type().has_optional_type() ?
                    rowFields[pos].type().optional_type().item() : rowFields[pos].type();

        result.emplace_back(name, typeInProto);
    }
    return result;
}

}

using TEvReadRowsRequest = TGrpcRequestNoOperationCall<Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>;

class TReadRowsRPC : public TActorBootstrapped<TReadRowsRPC> {
    using TThis = TReadRowsRPC;
    using TBase = TActorBootstrapped<TReadRowsRPC>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    static constexpr TDuration DEFAULT_TIMEOUT = TDuration::Seconds(60);
public:
    explicit TReadRowsRPC(IRequestNoOpCtx* request)
        : Request(request)
        , PipeCache(MakePipePeNodeCacheID(true))
    {}

    bool BuildSchema(NSchemeCache::TSchemeCacheNavigate* resolveNamesResult, TString& errorMessage) {
        Y_VERIFY(resolveNamesResult);

        auto& entry = resolveNamesResult->ResultSet.front();

        if (entry.Indexes.size()) {
            errorMessage = "EvReadResults is not supported for tables with indexes";
            return false;
        }

        THashSet<TString> keyColumnsLeft;
        THashMap<TString, ui32> columnByName;

        for (const auto& [_, colInfo] : entry.Columns) {
            ui32 id = colInfo.Id;
            auto& name = colInfo.Name;
            auto& type = colInfo.PType;
            ColumnsMeta.emplace_back(TColumnMeta{name, type, id});

            columnByName[name] = id;
            i32 keyOrder = colInfo.KeyOrder;
            if (keyOrder != -1) {
                Y_VERIFY(keyOrder >= 0);
                KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), keyOrder + 1));
                KeyColumnTypes[keyOrder] = type;
                keyColumnsLeft.insert(name);
            }
            if (colInfo.PType.GetTypeId() == NScheme::NTypeIds::Pg) {
                errorMessage = "Pg types are not supported yet";
                return false;
            }
        }

        KeyColumnPositions.resize(KeyColumnTypes.size());

        auto reqColumns = GetRequestColumns(GetProto());

        for (size_t pos = 0; pos < reqColumns.size(); ++pos) {
            auto& name = reqColumns[pos].first;
            const auto* cp = columnByName.FindPtr(name);
            if (!cp) {
                errorMessage = Sprintf("Unknown column: %s", name.c_str());
                return false;
            }
            i32 typmod = -1;
            ui32 colId = *cp;
            auto& ci = *entry.Columns.FindPtr(colId);

            const auto& typeInProto = reqColumns[pos].second;

            if (typeInProto.type_id()) {
                // TODO check Arrow types
            } else if (typeInProto.has_decimal_type() && ci.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
                int precision = typeInProto.decimal_type().precision();
                int scale = typeInProto.decimal_type().scale();
                if (precision != NScheme::DECIMAL_PRECISION || scale != NScheme::DECIMAL_SCALE) {
                    errorMessage = Sprintf("Unsupported Decimal(%d,%d) for column %s: expected Decimal(%d,%d)",
                                           precision, scale,
                                           name.c_str(),
                                           NScheme::DECIMAL_PRECISION, NScheme::DECIMAL_SCALE);

                    return false;
                }
            } else if (typeInProto.has_pg_type()) {
                const auto& typeName = typeInProto.pg_type().type_name();
                auto* typeDesc = NPg::TypeDescFromPgTypeName(typeName);
                if (!typeDesc) {
                    errorMessage = Sprintf("Unknown pg type for column %s: %s",
                                           name.c_str(), typeName.c_str());
                    return false;
                }
                auto typeInRequest = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
                if (typeInRequest != ci.PType) {
                    errorMessage = Sprintf("Type mismatch for column %s: expected %s, got %s",
                                           name.c_str(), NScheme::TypeName(ci.PType).c_str(),
                                           NScheme::TypeName(typeInRequest).c_str());
                    return false;
                }
                if (!ci.PTypeMod.empty() && NPg::TypeDescNeedsCoercion(typeDesc)) {
                    auto result = NPg::BinaryTypeModFromTextTypeMod(ci.PTypeMod, typeDesc);
                    if (result.Error) {
                        errorMessage = Sprintf("Invalid typemod for column %s: type %s, error %s",
                            name.c_str(), NScheme::TypeName(ci.PType, ci.PTypeMod).c_str(),
                            result.Error->c_str());
                        return false;
                    }
                    typmod = result.Typmod;
                }
            } else {
                errorMessage = Sprintf("Unexpected type for column %s: expected %s",
                                       name.c_str(), NScheme::TypeName(ci.PType).c_str());
                return false;
            }

            bool notNull = entry.NotNullColumns.contains(ci.Name);

            if (ci.KeyOrder != -1) {
                KeyColumnPositions[ci.KeyOrder] = NTxProxy::TFieldDescription{ci.Id, ci.Name, (ui32)pos, ci.PType, typmod, notNull};
                keyColumnsLeft.erase(ci.Name);
            }
        }

        if (!keyColumnsLeft.empty()) {
            errorMessage = Sprintf("Missing key columns: %s", JoinSeq(", ", keyColumnsLeft).c_str());
            return false;
        }

        return true;
    }

    bool CreateKeysToRead() {
        TMemoryPool valueDataPool(256);
        const auto& keys = GetProto()->Getkeys().Getvalue().Getitems();

        for (const auto& r : keys) {
            valueDataPool.Clear();

            TVector<TCell> keyCells;
            TString errorMessage;
            if (!FillCellsFromProto(keyCells, KeyColumnPositions, r, errorMessage, valueDataPool)) {
                ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "can't read values from proto " + errorMessage);
                return false;
            }

            TConstArrayRef<TCell> keyVec(keyCells);
            KeysToRead.emplace_back(keyVec);
        }

        return true;
    }

    const Ydb::Table::ReadRowsRequest* GetProto() {
        return TEvReadRowsRequest::GetProtoRequest(Request.get());
    }

    Ydb::Table::ReadRowsResponse* CreateResponse() {
        return google::protobuf::Arena::CreateMessage<Ydb::Table::ReadRowsResponse>(Request->GetArena());
    }

    TString GetDatabase() {
        return Request->GetDatabaseName().GetOrElse(DatabaseFromDomain(AppData()));
    }

    const TString& GetTable() {
        return GetProto()->path();
    }

    bool CheckAccess(NSchemeCache::TSchemeCacheNavigate* resolveNamesResult, TString& errorMessage) {
        if (Request->GetSerializedToken().empty())
            return true;

        NACLib::TUserToken userToken(Request->GetSerializedToken());
        const ui32 access = NACLib::EAccessRights::SelectRow;
        if (!resolveNamesResult) {
            TStringStream explanation;
            explanation << "Access denied for " << userToken.GetUserSID()
                        << " path '" << GetProto()->path()
                        << "' has not been resolved yet";

            errorMessage = explanation.Str();
            return false;
        }
        for (const NSchemeCache::TSchemeCacheNavigate::TEntry& entry : resolveNamesResult->ResultSet) {
            if (entry.Status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok
                && entry.SecurityObject != nullptr
                && !entry.SecurityObject->CheckAccess(access, userToken))
            {
                TStringStream explanation;
                explanation << "Access denied for " << userToken.GetUserSID()
                            << " with access " << NACLib::AccessRightsToString(access)
                            << " to path '" << GetProto()->path() << "'";

                errorMessage = explanation.Str();
                return false;
            }
        }
        return true;
    }

    void FindMinMaxKeys() {
        for (const auto& key : KeysToRead) {
            if (MinKey.empty()) {
                // Only for the first key
                MinKey = key;
                MaxKey = key;
            } else {
                // For all next keys
                if (CompareTypedCellVectors(key.data(), MinKey.data(),
                                            KeyColumnTypes.data(),
                                            key.size(), MinKey.size()) < 0)
                {
                    MinKey = key;
                } else if (CompareTypedCellVectors(key.data(), MaxKey.data(),
                                                   KeyColumnTypes.data(),
                                                   key.size(), MaxKey.size()) > 0)
                {
                    MaxKey = key;
                }
            }
        }
    }

    void Bootstrap(const NActors::TActorContext& ctx) {
        StartTime = TAppData::TimeProvider->Now();
        if (!ResolveTable()) {
            return;
        }

        auto clientTimeout = Request->GetDeadline() - ctx.Now();
        TimeoutTimerActorId = CreateLongTimer(ctx, std::min(clientTimeout, DEFAULT_TIMEOUT), new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup()));
        Become(&TThis::MainState);

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC bootstraped ");
    }

    bool ResolveTable() {
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = ::NKikimr::SplitPath(GetTable());
        if (entry.Path.empty()) {
            ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, "Invalid table path specified");
            return false;
        }
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        entry.SyncVersion = false;
        entry.ShowPrivatePath = false;
        auto request = std::make_unique<NSchemeCache::TSchemeCacheNavigate>();
        request->ResultSet.emplace_back(entry);
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()));
        return true;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = *ev->Get()->Request;

        Y_VERIFY(request.ResultSet.size() == 1);
        const auto& entry = request.ResultSet.front();

        OwnerId = entry.Self->Info.GetSchemeshardId();
        TableId = entry.Self->Info.GetPathId();

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TEvNavigateKeySetResult, " << " OwnerId: " << OwnerId << " TableId: " << TableId);
        switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                return ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Sprintf("Table '%s' unavaliable", GetTable().c_str()));
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotTable:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathNotPath:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::TableCreationNotComplete:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::PathErrorUnknown:
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown table '%s'", GetTable().c_str()));
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RootUnknown:
                return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown database for table '%s'", GetTable().c_str()));
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Unknown:
                return ReplyWithError(Ydb::StatusIds::GENERIC_ERROR, Sprintf("Unknown error on table '%s'", GetTable().c_str()));
        }

        if (entry.TableId.IsSystemView()) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                Sprintf("Table '%s' is a system view. ReadRows is not supported.", GetTable().c_str()));
        }

        auto* resolveNamesResult = ev->Get()->Request.Release();

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST,
            "TReadRowsRPC going to create keys to read from proto: " << GetProto()->DebugString());

        TString errorMessage;
        if (!CheckAccess(resolveNamesResult, errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, errorMessage);
        }

        if (!BuildSchema(resolveNamesResult, errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, errorMessage);
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                Sprintf("Table '%s': ReadRows is not supported for this table kind.", GetTable().c_str()));
        }

        if (!CreateKeysToRead()) {
            return;
        }

        ResolveShards(resolveNamesResult);
    }

    void ResolveShards(NSchemeCache::TSchemeCacheNavigate* resolveNamesResult) {
        auto& entry = resolveNamesResult->ResultSet.front();

        // We are going to request only key columns
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& [_, ci] : entry.Columns) {
            if (ci.KeyOrder != -1) {
                TKeyDesc::TColumnOp op = { ci.Id, TKeyDesc::EColumnOperation::Set, ci.PType, 0, 0 };
                columns.push_back(op);
            }
        }
        FindMinMaxKeys();
        TTableRange range(MinKey, true, MaxKey, true, false);
        auto keyRange = MakeHolder<TKeyDesc>(entry.TableId, range, TKeyDesc::ERowOperation::Read, KeyColumnTypes, columns);

        auto request = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.emplace_back(std::move(keyRange));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.release()));
    }

    std::map<ui64, std::vector<TOwnedCellVec>> CreateShardToKeyMapping(TKeyDesc* keyRange) {
        std::map<ui64, std::vector<TOwnedCellVec>> shardToKey;
        auto &partitions = keyRange->GetPartitions();
        for (auto& key : KeysToRead) {
            auto it = std::lower_bound(partitions.begin(), partitions.end(), key,
                [&](const auto& partition, const auto& key) {
                        const auto& range = *partition.Range;
                        const int cmp = CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                                range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
                        return (cmp < 0);
                });
            Y_VERIFY(it != partitions.end());
            shardToKey[it->ShardId].emplace_back(std::move(key));
        }

        return shardToKey;
    }

    void Handle(TEvTxProxySchemeCache::TEvResolveKeySetResult::TPtr &ev) {
        TEvTxProxySchemeCache::TEvResolveKeySetResult *msg = ev->Get();
        auto& resolvePartitionsResult = msg->Request;

        if (resolvePartitionsResult->ErrorCount > 0) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("Unknown table '%s'", GetTable().c_str()));
        }
        if (resolvePartitionsResult->ResultSet.size() != 1) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, Sprintf("unexpected ResultSet.size() != 1 for table '%s'", GetTable().c_str()));
        }
        auto keyRange = resolvePartitionsResult->ResultSet[0].KeyDescription.Get();

        for (const auto& [shardId, keys] : CreateShardToKeyMapping(keyRange)) {
            SendRead(shardId, keys);
        }
    }

    void SendRead(ui64 shardId, const std::vector<TOwnedCellVec>& keys) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        record.SetReadId(0);
        record.MutableTableId()->SetOwnerId(OwnerId);
        record.MutableTableId()->SetTableId(TableId);

        for (const auto& meta: ColumnsMeta) {
            record.AddColumns(meta.Id);
        }

        record.SetResultFormat(::NKikimrTxDataShard::EScanDataFormat::CELLVEC);

        for (auto& key : keys) {
            request->Keys.emplace_back(TSerializedCellVec::Serialize(key));
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC send TEvRead shardId : " << shardId << " keys.size(): " << keys.size());
        Send(PipeCache, new TEvPipeCache::TEvForward(request.release(), shardId, true), IEventHandle::FlagTrackDelivery);
        ++ReadsInFlight;
    }

    void Handle(const TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto* msg = ev->Get();

        if (msg->Record.HasStatus() && msg->Record.GetStatus().GetCode() != Ydb::StatusIds::SUCCESS) {
            TStringStream ss;
            ss << "Failed to read from ds# " << ShardId << ", code# " << msg->Record.GetStatus().GetCode();
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, ss.Str());
        }
        Y_VERIFY(msg->Record.HasFinished() && msg->Record.GetFinished());
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC TEvReadResult RowsCount: " << msg->GetRowsCount());

        EvReadResults.emplace_back(ev->Release().Release());

        --ReadsInFlight;
        if (ReadsInFlight == 0) {
            SendResult(Ydb::StatusIds::SUCCESS, "");
        }
    }

    void FillResultRows(Ydb::Table::ReadRowsResponse* response) {
        auto *resultSet = response->Mutableresult_set();

        NKqp::TProgressStatEntry stats;
        auto& ioStats = stats.ReadIOStat;

        for (const auto& colMeta : ColumnsMeta) {
            auto type = NYdb::TTypeBuilder().Primitive((NYdb::EPrimitiveType)colMeta.Type.GetTypeId()).Build();
            auto* col = resultSet->Addcolumns();
            *col->mutable_type() = NYdb::TProtoAccessor::GetProto(type);
            *col->mutable_name() = colMeta.Name;
        }

        for (auto& result : EvReadResults) {
            for (size_t i = 0; i < result->GetRowsCount(); ++i) {
                const auto& row = result->GetCells(i);
                NYdb::TValueBuilder vb;
                vb.BeginStruct();
                ui64 sz = 0;
                for (const auto& colMeta : ColumnsMeta) {
                    auto type = NYdb::TTypeBuilder().Primitive((NYdb::EPrimitiveType)colMeta.Type.GetTypeId()).Build();
                    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC "
                        << " name: " << colMeta.Name
                    );
                    const auto& cell = row[colMeta.Id - 1];
                    vb.AddMember(colMeta.Name);
                    ProtoValueFromCell(vb, colMeta.Type, cell);
                    sz += cell.Size();
                }
                vb.EndStruct();
                auto proto = NYdb::TProtoAccessor::GetProto(vb.Build());
                ioStats.Rows++;
                ioStats.Bytes += sz;
                *resultSet->add_rows() = std::move(proto);
            }
        }

        RuCost = NKqp::NRuCalc::CalcRequestUnit(stats);
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC created ReadRowsResponse " << response->DebugString());
    }

    void SendResult(const Ydb::StatusIds::StatusCode& status, const TString& errorMsg) {
        auto* resp = CreateResponse();


        if (status == Ydb::StatusIds::SUCCESS) {
            Request->SetRuHeader(RuCost);

            FillResultRows(resp);
        }

        if (errorMsg) {
            Request->RaiseIssue(NYql::TIssue(errorMsg));
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC sent result");
        Request->Reply(resp, status);
        PassAway();
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr&) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, TStringBuilder() << "ReadRows from table " << GetTable()
            << " timed out, duration: " << (TAppData::TimeProvider->Now() - StartTime).Seconds() << " sec");
    }

    void ReplyWithError(const Ydb::StatusIds::StatusCode& status, const TString& errorMsg) {
        LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC ReplyWithError: " << errorMsg);
        SendResult(status, errorMsg);
    }

    void PassAway() override {
        Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
        if (TimeoutTimerActorId) {
            Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }
        TBase::PassAway();
    }

    STFUNC(MainState) {
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC got: " << ev->GetTypeName());
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            hFunc(TEvTxProxySchemeCache::TEvResolveKeySetResult, Handle);
            hFunc(TEvDataShard::TEvReadResult, Handle);

            hFunc(TEvents::TEvWakeup, HandleTimeout);
        }
    }

private:
    std::unique_ptr<IRequestNoOpCtx> Request;
    TInstant StartTime;
    TActorId TimeoutTimerActorId;
    TActorId PipeCache;
    std::vector<TOwnedCellVec> KeysToRead;
    TOwnedCellVec MinKey;
    TOwnedCellVec MaxKey;
    ui64 RuCost = 0;

    // Scheme
    TVector<NTxProxy::TFieldDescription> KeyColumnPositions;
    TVector<NScheme::TTypeInfo> KeyColumnTypes;
    struct TColumnMeta {
        TString Name;
        NScheme::TTypeInfo Type;
        ui32 Id;
    };
    TVector<TColumnMeta> ColumnsMeta;

    std::vector<std::unique_ptr<TEvDataShard::TEvReadResult>> EvReadResults;
    // TEvRead interface
    ui64 ReadsInFlight = 0;
    ui64 OwnerId = 0;
    ui64 TableId = 0;
    ui64 ShardId = 0;
};

void DoReadRowsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TReadRowsRPC(p.release()));
}

} // namespace NKikimr::NGRpcService
