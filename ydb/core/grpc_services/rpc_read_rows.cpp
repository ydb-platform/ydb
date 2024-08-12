#include <ydb/core/grpc_services/base/base.h>

#include "rpc_common/rpc_common.h"
#include "service_table.h"

#include <ydb/core/kqp/common/kqp_ru_calc.h>
#include <ydb/core/kqp/executer_actor/kqp_executer_stats.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/upload_rows_common_impl.h>
#include <ydb/core/ydb_convert/ydb_convert.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/type_desc.h>
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

struct RequestedKeyColumn {
    TString Name;
    Ydb::Type Type;
};

}

namespace {

auto GetRequestedKeyColumns(const Ydb::Table::ReadRowsRequest& proto) {
    const auto& type = proto.Getkeys().Gettype();
    const auto& rowFields = type.Getlist_type().Getitem().Getstruct_type().Getmembers();

    std::vector<RequestedKeyColumn> result;
    result.reserve(rowFields.size());
    for (const auto& rowField: rowFields) {
        const auto& name = rowField.Getname();
        const auto& typeInProto = rowField.type().has_optional_type() ?
                    rowField.type().optional_type().item() : rowField.type();

        result.push_back({name, typeInProto});
    }
    return result;
}

auto GetRequestedResultColumns(const Ydb::Table::ReadRowsRequest& proto) {
    const auto& columns = proto.Getcolumns();

    std::vector<TString> result;
    result.reserve(columns.size());
    for (const auto& column: columns) {
        result.emplace_back(column);
    }
    return result;
}

} // namespace

using TEvReadRowsRequest = TGrpcRequestNoOperationCall<Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>;

class TReadRowsRPC : public TActorBootstrapped<TReadRowsRPC> {
    using TThis = TReadRowsRPC;
    using TBase = TActorBootstrapped<TReadRowsRPC>;

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_REQ;
    }

    static constexpr TDuration DEFAULT_TIMEOUT = TDuration::Seconds(60);
public:
    explicit TReadRowsRPC(std::unique_ptr<IRequestNoOpCtx> request)
        : Request(std::move(request))
        , PipeCache(MakePipePerNodeCacheID(true))
        , Span(TWilsonGrpc::RequestActor, Request->GetWilsonTraceId(), "ReadRowsRpc")
    {}

    bool BuildSchema(NSchemeCache::TSchemeCacheNavigate* resolveNamesResult, TString& errorMessage) {
        Y_ABORT_UNLESS(resolveNamesResult);

        const auto& entry = resolveNamesResult->ResultSet.front();

        if (entry.Indexes.size()) {
            errorMessage = "EvReadResults is not supported for tables with indexes";
            return false;
        }

        const auto requestedResultColumnNames = GetRequestedResultColumns(*GetProto());

        THashSet<TString> keyColumnNamesInSchema;
        THashMap<TString, const NKikimr::TSysTables::TTableColumnInfo*> nameToColumn;
        for (const auto& [colId, colInfo] : entry.Columns) {
            nameToColumn[colInfo.Name] = &colInfo;

            const i32 keyOrder = colInfo.KeyOrder;
            if (keyOrder != -1) {
                Y_ABORT_UNLESS(keyOrder >= 0);
                keyColumnNamesInSchema.insert(colInfo.Name);
            }
        }

        if (!PrepareRequestedKeyColumns(keyColumnNamesInSchema,
                                        nameToColumn,
                                        entry.NotNullColumns,
                                        errorMessage))
        {
            return false;
        }

        if (!PrepareRequestedResultColumns(nameToColumn,
                                           errorMessage))
        {
            return false;
        }

        return true;
    }

    bool PrepareRequestedKeyColumns(const THashSet<TString>& keyColumnNamesInSchema,
                                    const THashMap<TString, const NKikimr::TSysTables::TTableColumnInfo*>& nameToColumn,
                                    const THashSet<TString>& notNullColumns,
                                    TString& errorMessage)
    {
        auto keyColumnsLeft = keyColumnNamesInSchema;
        KeyColumnPositions.resize(keyColumnNamesInSchema.size());

        const auto requestedColumns = GetRequestedKeyColumns(*GetProto());

        for (size_t pos = 0; pos < requestedColumns.size(); ++pos) {
            const auto& [name, typeInProto] = requestedColumns[pos];
            const auto* colInfoPtr = nameToColumn.Value(name, nullptr);
            if (!colInfoPtr) {
                errorMessage = Sprintf("Unknown key column: %s", name.c_str());
                return false;
            }
            const auto& colInfo = *colInfoPtr;

            i32 typmod = -1;
            if (typeInProto.type_id()) {
                // TODO check Arrow types
            } else if (typeInProto.has_decimal_type() && colInfo.PType.GetTypeId() == NScheme::NTypeIds::Decimal) {
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

                const auto typeInRequest = NScheme::TTypeInfo(NScheme::NTypeIds::Pg, typeDesc);
                if (typeInRequest != colInfo.PType) {
                    errorMessage = Sprintf("Type mismatch for column %s: expected %s, got %s",
                                           name.c_str(), NScheme::TypeName(colInfo.PType).c_str(),
                                           NScheme::TypeName(typeInRequest).c_str());
                    return false;
                }

                if (!colInfo.PTypeMod.empty() && NPg::TypeDescNeedsCoercion(typeDesc)) {
                    const auto result = NPg::BinaryTypeModFromTextTypeMod(colInfo.PTypeMod, typeDesc);
                    if (result.Error) {
                        errorMessage = Sprintf("Invalid typemod for column %s: type %s, error %s",
                            name.c_str(), NScheme::TypeName(colInfo.PType, colInfo.PTypeMod).c_str(),
                            result.Error->c_str());
                        return false;
                    }
                    typmod = result.Typmod;
                }
            } else {
                errorMessage = Sprintf("Unexpected type for column %s: expected %s",
                                       name.c_str(), NScheme::TypeName(colInfo.PType).c_str());
                return false;
            }

            KeyColumnTypes.resize(Max<size_t>(KeyColumnTypes.size(), colInfo.KeyOrder + 1));
            KeyColumnTypes[colInfo.KeyOrder] = colInfo.PType;

            bool notNull = notNullColumns.contains(colInfo.Name);
            KeyColumnPositions[colInfo.KeyOrder] = NTxProxy::TFieldDescription{colInfo.Id, colInfo.Name, static_cast<ui32>(pos), colInfo.PType, typmod, notNull};
            keyColumnsLeft.erase(colInfo.Name);
        }

        if (!keyColumnsLeft.empty()) {
            errorMessage = Sprintf("Missing key columns: %s", JoinSeq(", ", keyColumnsLeft).c_str());
            return false;
        }

        return true;
    }

    bool PrepareRequestedResultColumns(const THashMap<TString, const NKikimr::TSysTables::TTableColumnInfo*>& nameToColumn,
                                       TString& errorMessage)
    {
        const auto requestedColumnNames = GetRequestedResultColumns(*GetProto());
        if (requestedColumnNames.empty())
        {
            for (const auto& [name, colInfoPtr] : nameToColumn) {
                RequestedColumnsMeta.emplace_back(*colInfoPtr);
            }
            // sort per schema column order
            std::sort(RequestedColumnsMeta.begin(), RequestedColumnsMeta.end(), [](const auto& a, const auto& b) { return a.Id < b.Id; });
        }
        else
        {
            for (const auto& name : requestedColumnNames) {
                const auto* colInfoPtr = nameToColumn.Value(name, nullptr);
                if (!colInfoPtr) {
                    errorMessage = Sprintf("Unknown result column: %s", name.c_str());
                    return false;
                }

                RequestedColumnsMeta.emplace_back(*colInfoPtr);
            }
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
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvNavigateKeySet(request.release()), 0, 0, Span.GetTraceId());
        return true;
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev) {
        const auto& request = *ev->Get()->Request;

        Y_ABORT_UNLESS(request.ResultSet.size() == 1);
        const auto& entry = request.ResultSet.front();

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TEvNavigateKeySetResult, " << " OwnerId: " << OwnerId << " TableId: " << TableId);
        switch (entry.Status) {
            case NSchemeCache::TSchemeCacheNavigate::EStatus::Ok:
                break;
            case NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError:
            case NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError:
                return ReplyWithError(Ydb::StatusIds::UNAVAILABLE, Sprintf("Table '%s' unavaliable", GetTable().c_str()));
            case NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied:
                return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, Sprintf("Access denied to table '%s'", GetTable().c_str()));
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

        OwnerId = entry.Self->Info.GetSchemeshardId();
        TableId = entry.Self->Info.GetPathId();

        if (entry.TableId.IsSystemView()) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                Sprintf("Table '%s' is a system view. ReadRows is not supported.", GetTable().c_str()));
        }

        auto& resolveNamesResult = ev->Get()->Request;

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST,
            "TReadRowsRPC going to create keys to read from proto: " << GetProto()->DebugString());

        TString errorMessage;
        if (!CheckAccess(resolveNamesResult.Get(), errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::UNAUTHORIZED, errorMessage);
        }
        const auto& keys = GetProto()->Getkeys().Getvalue().Getitems();
        if (keys.empty()) {
            return ReplyWithError(Ydb::StatusIds::BAD_REQUEST, "no keys are found in request's proto");
        }

        if (!BuildSchema(resolveNamesResult.Get(), errorMessage)) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR, errorMessage);
        }
        if (entry.Kind != NSchemeCache::TSchemeCacheNavigate::KindTable) {
            return ReplyWithError(Ydb::StatusIds::SCHEME_ERROR,
                Sprintf("Table '%s': ReadRows is not supported for this table kind.", GetTable().c_str()));
        }

        if (!CreateKeysToRead()) {
            return;
        }

        ResolveShards(resolveNamesResult.Get());
    }

    void ResolveShards(NSchemeCache::TSchemeCacheNavigate* resolveNamesResult) {
        auto& entry = resolveNamesResult->ResultSet.front();

        // We are going to request only key columns
        TVector<TKeyDesc::TColumnOp> columns;
        for (const auto& [colId, colInfo] : entry.Columns) {
            if (colInfo.KeyOrder != -1) {
                TKeyDesc::TColumnOp op = { colInfo.Id, TKeyDesc::EColumnOperation::Set, colInfo.PType, 0, 0 };
                columns.push_back(op);
            }
        }
        FindMinMaxKeys();
        TTableRange range(MinKey, true, MaxKey, true, false);
        auto keyRange = MakeHolder<TKeyDesc>(entry.TableId, range, TKeyDesc::ERowOperation::Read, KeyColumnTypes, columns);

        auto request = std::make_unique<NSchemeCache::TSchemeCacheRequest>();
        request->ResultSet.emplace_back(std::move(keyRange));
        Send(MakeSchemeCacheID(), new TEvTxProxySchemeCache::TEvResolveKeySet(request.release()), 0, 0, Span.GetTraceId());
    }

    void CreateShardToKeysMapping(TKeyDesc* keyRange) {
        auto &partitions = keyRange->GetPartitions();
        for (auto& key : KeysToRead) {
            auto it = std::lower_bound(partitions.begin(), partitions.end(), key,
                [&](const auto& partition, const auto& key) {
                        const auto& range = *partition.Range;
                        const int cmp = CompareBorders<true, false>(range.EndKeyPrefix.GetCells(), key,
                                range.IsInclusive || range.IsPoint, true, KeyColumnTypes);
                        return (cmp < 0);
                });
            Y_ABORT_UNLESS(it != partitions.end());
            ShardIdToKeys[it->ShardId].emplace_back(std::move(key));
        }
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

        CreateShardToKeysMapping(keyRange);
        for (const auto& [shardId, keys] : ShardIdToKeys) {
            SendRead(shardId, keys);
        }
    }

    void SendRead(ui64 shardId, const std::vector<TOwnedCellVec>& keys) {
        auto request = std::make_unique<TEvDataShard::TEvRead>();
        auto& record = request->Record;

        // the ReadId field is used as a cookie to distinguish responses from different datashards
        record.SetReadId(shardId);
        record.MutableTableId()->SetOwnerId(OwnerId);
        record.MutableTableId()->SetTableId(TableId);

        for (const auto& meta: RequestedColumnsMeta) {
            record.AddColumns(meta.Id);
        }

        record.SetResultFormat(::NKikimrDataEvents::FORMAT_CELLVEC);

        for (auto& key : keys) {
            request->Keys.emplace_back(TSerializedCellVec::Serialize(key));
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC send TEvRead shardId : " << shardId << " keys.size(): " << keys.size());
        Send(PipeCache, new TEvPipeCache::TEvForward(request.release(), shardId, true), IEventHandle::FlagTrackDelivery, 0, Span.GetTraceId());
        ++ReadsInFlight;
    }

    void Handle(const TEvDataShard::TEvReadResult::TPtr& ev) {
        const auto* msg = ev->Get();

        --ReadsInFlight;

        if (msg->Record.HasStatus()) {
            // ReadRows can reply with the following statuses:
            // * SUCCESS
            // * INTERNAL_ERROR -- only if MaxRetries is reached
            // * OVERLOADED -- client will retrie it with backoff
            // * ABORTED -- code is used for all other DataShard errors

            const auto& status = msg->Record.GetStatus();
            auto statusCode = status.GetCode();
            const auto issues = status.GetIssues();

            ui64 shardId = msg->Record.GetReadId();

            switch (statusCode) {
            case Ydb::StatusIds::SUCCESS:
                break;
            case Ydb::StatusIds::INTERNAL_ERROR: {
                auto it = ShardIdToKeys.find(shardId);
                ++Retries;
                if (it == ShardIdToKeys.end()) {
                    TStringStream ss;
                    ss << "Got unknown shardId from TEvReadResult# " << shardId << ", status# " << statusCode;
                    ReplyWithError(statusCode, ss.Str(), &issues);
                } else if (Retries < MaxTotalRetries) {
                    TStringStream ss;
                    ss << "Reached MaxRetries count for DataShard# " << shardId << ", status# " << statusCode;
                    ReplyWithError(statusCode, ss.Str(), &issues);
                } else {
                    SendRead(shardId, it->second);
                }
                return;
            }
            case Ydb::StatusIds::OVERLOADED:
                [[fallthrough]];
            default: {
                TStringStream ss;
                ss << "Failed to read from ds# " << shardId << ", status# " << statusCode;
                if (statusCode != Ydb::StatusIds::OVERLOADED) {
                    statusCode = Ydb::StatusIds::ABORTED;
                }
                ReplyWithError(statusCode, ss.Str(), &issues);
                return;
            }
            }
        }
        Y_ABORT_UNLESS(msg->Record.HasFinished() && msg->Record.GetFinished());
        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC TEvReadResult RowsCount: " << msg->GetRowsCount());

        EvReadResults.emplace_back(ev->Release().Release());

        if (ReadsInFlight == 0) {
            SendResult(Ydb::StatusIds::SUCCESS, "");
        }
    }

    void FillResultRows(Ydb::Table::ReadRowsResponse* response) {
        auto *resultSet = response->Mutableresult_set();

        NKqp::TProgressStatEntry stats;
        auto& ioStats = stats.ReadIOStat;

        const auto getPgTypeFromColMeta = [](const auto &colMeta) {
            return NYdb::TPgType(NPg::PgTypeNameFromTypeDesc(colMeta.Type.GetTypeDesc()),
                                 colMeta.PTypeMod);
        };

        const auto getTypeFromColMeta = [&](const auto &colMeta) {
            if (colMeta.Type.GetTypeId() == NScheme::NTypeIds::Pg) {
                return NYdb::TTypeBuilder().Pg(getPgTypeFromColMeta(colMeta)).Build();
            } else {
                return NYdb::TTypeBuilder()
                    .Primitive((NYdb::EPrimitiveType)colMeta.Type.GetTypeId())
                    .Build();
            }
        };

        for (const auto& colMeta : RequestedColumnsMeta) {
            const auto type = getTypeFromColMeta(colMeta);
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
                for (size_t i = 0; i < RequestedColumnsMeta.size(); ++i) {
                    const auto& colMeta = RequestedColumnsMeta[i];
                    const auto type = getTypeFromColMeta(colMeta);
                    LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC "
                        << " name: " << colMeta.Name
                    );
                    const auto& cell = row[i];
                    vb.AddMember(colMeta.Name);
                    if (colMeta.Type.GetTypeId() == NScheme::NTypeIds::Pg)
                    {
                        const NPg::TConvertResult& pgResult = NPg::PgNativeTextFromNativeBinary(cell.AsBuf(), colMeta.Type.GetTypeDesc());
                        if (pgResult.Error) {
                            LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "PgNativeTextFromNativeBinary error " << *pgResult.Error);
                        }
                        const NYdb::TPgValue pgValue{cell.IsNull() ? NYdb::TPgValue::VK_NULL : NYdb::TPgValue::VK_TEXT, pgResult.Str, getPgTypeFromColMeta(colMeta)};
                        vb.Pg(pgValue);
                    }
                    else
                    {
                        ProtoValueFromCell(vb, colMeta.Type, cell);
                    }
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

    void SendResult(const Ydb::StatusIds::StatusCode& status, const TString& errorMsg,
        const ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues = nullptr)
    {
        auto* resp = CreateResponse();
        resp->set_status(status);
        if (!errorMsg.Empty() || issues) {
            const NYql::TIssue& issue = MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR, errorMsg);
            auto* protoIssue = resp->add_issues();
            NYql::IssueToMessage(issue, protoIssue);
            if (issues) {
                for (auto& i : *issues) {
                    *resp->add_issues() = i;
                }
            }
        }

        if (status == Ydb::StatusIds::SUCCESS) {
            Request->SetRuHeader(RuCost);

            FillResultRows(resp);
        }

        LOG_DEBUG_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC sent result");
        Request->Reply(resp, status);
        PassAway();
    }

    void HandleTimeout(TEvents::TEvWakeup::TPtr&) {
        return ReplyWithError(Ydb::StatusIds::TIMEOUT, TStringBuilder() << "ReadRows from table " << GetTable()
            << " timed out, duration: " << (TAppData::TimeProvider->Now() - StartTime).Seconds() << " sec");
    }

    void ReplyWithError(const Ydb::StatusIds::StatusCode& status, const TString& errorMsg,
        const ::google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* issues = nullptr)
    {
        LOG_ERROR_S(TlsActivationContext->AsActorContext(), NKikimrServices::RPC_REQUEST, "TReadRowsRPC ReplyWithError: " << errorMsg);
        SendResult(status, errorMsg, issues);
    }

    void PassAway() override {
        Send(PipeCache, new TEvPipeCache::TEvUnlink(0));
        if (TimeoutTimerActorId) {
            Send(TimeoutTimerActorId, new TEvents::TEvPoisonPill());
        }
        Span.EndOk();
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
        TColumnMeta(const TSysTables::TTableColumnInfo& colInfo)
            : Id(colInfo.Id)
            , Name(colInfo.Name)
            , Type(colInfo.PType)
            , PTypeMod(colInfo.PTypeMod)
        {
        }

        ui32 Id;
        TString Name;
        NScheme::TTypeInfo Type;
        TString PTypeMod;
    };
    TVector<TColumnMeta> RequestedColumnsMeta;

    std::map<ui64, std::vector<TOwnedCellVec>> ShardIdToKeys;
    std::vector<std::unique_ptr<TEvDataShard::TEvReadResult>> EvReadResults;
    // TEvRead interface
    ui64 ReadsInFlight = 0;
    ui64 OwnerId = 0;
    ui64 TableId = 0;

    ui64 Retries = 0;
    const ui64 MaxTotalRetries = 5;

    NWilson::TSpan Span;
};

void DoReadRowsRequest(std::unique_ptr<IRequestNoOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TReadRowsRPC(std::move(p)));
}

} // namespace NKikimr::NGRpcService
