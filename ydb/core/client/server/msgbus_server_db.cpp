#include "msgbus_server.h"
#include "msgbus_server_proxy.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/path.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/ticket_parser.h>
#include <ydb/core/engine/kikimr_program_builder.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/protobuf/json/json2proto.h>


namespace NKikimr {
namespace NMsgBusProxy {

namespace {
    constexpr TDuration Timeout = TDuration::Seconds(30);

    struct TEvPrivate {
        enum EEv {
            EvReply = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

        struct TEvReply : public TEventLocal<TEvReply, EvReply> {
            EResponseStatus Status;
            TEvTxUserProxy::TResultStatus::EStatus ProxyStatus;
            TString Message;
            NKikimrTxUserProxy::TEvProposeTransactionStatus Result;

            TEvReply(EResponseStatus status, TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message)
                : Status(status)
                , ProxyStatus(proxyStatus)
                , Message(message)
            {}

            TEvReply(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus& result)
                : Status(status)
                , ProxyStatus(TEvTxUserProxy::TResultStatus::EStatus::Unknown)
                , Result(result)
            {}
        };
    };
}

template <typename RequestType>
class TMessageBusInterface : public TMessageBusSessionIdentHolder {
private:
    TAutoPtr<RequestType> Request;
    NKikimrClient::TJsonSettings JsonSettings;

public:
    TMessageBusInterface(TBusMessageContext& msg)
        : TMessageBusSessionIdentHolder(msg)
        , Request(static_cast<RequestType*>(msg.ReleaseMessage()))
    {}

    void ReplyWithErrorToInterface(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext&) {
        TAutoPtr<TBusResponse> response(new TBusResponseStatus(status, message));
        if (proxyStatus != TEvTxUserProxy::TResultStatus::Unknown)
            response->Record.SetProxyErrorCode(proxyStatus);
        SendReplyAutoPtr(response);
    }

    void ReplyWithResultToInterface(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus& result, const TActorContext&) {
        if (status == MSTATUS_OK && result.HasExecutionEngineEvaluatedResponse()) {
            const NKikimrMiniKQL::TResult& pbResult(result.GetExecutionEngineEvaluatedResponse());
            if (pbResult.HasType() && pbResult.HasValue()) {
                TAutoPtr<TBusDbResponse> response(new TBusDbResponse);
                NClient::TValue value(NClient::TValue::Create(pbResult.GetValue(), pbResult.GetType()));
                NClient::TValue resultValue(value["Result"]);
                if (resultValue.HaveValue()) {
                    response->Record.SetJSON(resultValue.GetValueText<NClient::TFormatJSON>({JsonSettings.GetUI64AsString()}));
                    SendReplyAutoPtr(response);
                } else {
                    response->Record.SetJSON("{}");
                    SendReplyAutoPtr(response);
                }
            }
        } else {
            TAutoPtr<TBusResponse> response(ProposeTransactionStatusToResponse(status, result));
            if (result.HasExecutionEngineEvaluatedResponse()) {
                response->Record.MutableExecutionEngineEvaluatedResponse()->CopyFrom(result.GetExecutionEngineEvaluatedResponse());
            }
            SendReplyAutoPtr(response);
        }
    }

    void ReplyWithResultToInterface(EResponseStatus, const google::protobuf::RepeatedPtrField<NKikimrMiniKQL::TResult>& result, const TActorContext&) {
        TStringStream json;
        if (!result.empty()) {
            if (result.size() == 1) {
                const NKikimrMiniKQL::TResult& pbResult = *result.begin();
                if (pbResult.HasType() && pbResult.HasValue()) {
                    NClient::TValue value(NClient::TValue::Create(pbResult.GetValue(), pbResult.GetType()));
                    NClient::TValue resultValue(value["Data"]);
                    if (resultValue.HaveValue()) {
                        json << resultValue.GetValueText<NClient::TFormatJSON>({JsonSettings.GetUI64AsString()});
                    } else {
                        json << "[]";
                    }
                }
            } else {
                json << '[';
                for (auto it = result.begin(); it != result.end(); ++it) {
                    const NKikimrMiniKQL::TResult& pbResult = *it;
                    if (it != result.begin()) {
                        json << ',';
                    }
                    if (pbResult.HasType() && pbResult.HasValue()) {
                        NClient::TValue value(NClient::TValue::Create(pbResult.GetValue(), pbResult.GetType()));
                        NClient::TValue resultValue(value["Data"]);
                        if (resultValue.HaveValue()) {
                            json << resultValue.GetValueText<NClient::TFormatJSON>({JsonSettings.GetUI64AsString()});
                        } else {
                            json << "[]";
                        }
                    } else {
                        json << "[]";
                    }
                }
                json << ']';
            }
            TAutoPtr<TBusDbResponse> response(new TBusDbResponse);
            response->Record.SetJSON(json.Str());
            SendReplyAutoPtr(response);
        }
    }

    bool BootstrapJSON(NJson::TJsonValue* jsonValue, TString& securityToken) {
        static NJson::TJsonReaderConfig readerConfig;
        TMemoryInput in(Request->Record.GetJSON());
        bool result = NJson::ReadJsonTree(&in, &readerConfig, jsonValue, false);
        if (result) {
            NJson::TJsonValue* jsonSettings;
            if (jsonValue->GetValuePointer("JsonSettings", &jsonSettings)) {
                try {
                    NProtobufJson::Json2Proto(*jsonSettings, JsonSettings);
                } catch(const yexception&) {
                }
            }
        }
        if (Request->Record.HasSecurityToken()) {
            securityToken = Request->Record.GetSecurityToken();
        }
        if (Request->Record.HasJsonSettings()) {
            JsonSettings.MergeFrom(Request->Record.GetJsonSettings());
        }
        Request.Destroy();
        return result;
    }
};

class TActorInterface {
private:
    TActorId HostActor;
public:
    TActorInterface(const TActorId& hostActor)
        : HostActor(hostActor)
    {}

    void ReplyWithErrorToInterface(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext& ctx) {
        ctx.Send(HostActor, new TEvPrivate::TEvReply(status, proxyStatus, message));
    }

    void ReplyWithResultToInterface(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus& result, const TActorContext& ctx) {
        ctx.Send(HostActor, new TEvPrivate::TEvReply(status, result));
    }

    bool BootstrapJSON(NJson::TJsonValue*, TString&) { return true; }
};

template <typename InterfaceBase>
class TServerDbOperation : public TActorBootstrapped<TServerDbOperation<InterfaceBase>>, public InterfaceBase {
protected:
    using TThis = TServerDbOperation<InterfaceBase>;
    using TBase = TActorBootstrapped<TServerDbOperation<InterfaceBase>>;
    using TTabletId = ui64;

    TActorId TxProxyId;
    TActorId SchemeCache;
    TIntrusivePtr<TMessageBusDbOpsCounters> DbOperationsCounters;

    NMonitoring::THistogramPtr OperationHistogram;
    TAutoPtr<NSchemeCache::TSchemeCacheNavigate> CacheNavigate;
    NJson::TJsonValue JSON;
    THPTimer StartTime;
    TString Table;
    TString SecurityToken; // raw input
    TString UserToken; // built and serialized
    ui32 Requests = 0;
    ui32 Responses = 0;

    void CompleteRequest(const TActorContext& ctx) {
        TDuration duration(TDuration::MicroSeconds(StartTime.Passed() * 1000000/*us*/));
        if (OperationHistogram) {
            OperationHistogram->Collect(duration.MilliSeconds());
        }
        DbOperationsCounters->RequestTotalTimeHistogram->Collect(duration.MilliSeconds());
        Die(ctx);
    }

    void ReplyWithError(EResponseStatus status, TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext& ctx) {
        InterfaceBase::ReplyWithErrorToInterface(status, proxyStatus, message, ctx);
        CompleteRequest(ctx);
    }

    void ReplyWithError(EResponseStatus status, const TString& message, const TActorContext& ctx) {
        ReplyWithError(status, TEvTxUserProxy::TResultStatus::Unknown, message, ctx);
    }

    void ReplyWithError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TActorContext& ctx) {
        ReplyWithError(status, proxyStatus, TEvTxUserProxy::TResultStatus::Str(proxyStatus), ctx);
    }

    void ReplyWithResult(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus& result, const TActorContext& ctx) {
        InterfaceBase::ReplyWithResultToInterface(status, result, ctx);
        CompleteRequest(ctx);
    }

    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySetResult::TPtr& ev, const TActorContext& ctx) {
        const NSchemeCache::TSchemeCacheNavigate& request = *ev->Get()->Request;
        Y_ABORT_UNLESS(request.ResultSet.size() == 1);
        if (request.ResultSet.front().Status != NSchemeCache::TSchemeCacheNavigate::EStatus::Ok) {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TResultStatus::ResolveError, ToString(request.ResultSet.front().Status), ctx);
        }
        CacheNavigate = ev->Get()->Request;
        if (++Responses == Requests) {
            BuildAndRunProgram(ctx);
        }
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr &ev, const TActorContext &ctx) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        if (!result.Error.empty()) {
            //return TBase::HandleError(EResponseStatus::MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, result.Error, ctx);
        } else {
            UserToken = result.Token->GetSerializedToken();
        }
        if (++Responses == Requests) {
            BuildAndRunProgram(ctx);
        }
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        const TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
        switch (status) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyAccepted:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyPrepared:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorPlanned:
        // transitional statuses
            return;
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready:
        // completion
            return ReplyWithResult(MSTATUS_OK, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout:
            return ReplyWithResult(MSTATUS_INPROGRESS, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyNotReady:
            return ReplyWithError(MSTATUS_NOTREADY, status, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted:
            return ReplyWithError(MSTATUS_ABORTED, status, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::EmptyAffectedSet:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::DomainLocalityError:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest:
            return ReplyWithResult(MSTATUS_ERROR, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded:
            return ReplyWithResult(MSTATUS_REJECTED, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown:
            return ReplyWithResult(MSTATUS_TIMEOUT, msg->Record, ctx);
        default:
            return ReplyWithResult(MSTATUS_INTERNALERROR, msg->Record, ctx);
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::MSGBUS_COMMON; }

    TServerDbOperation(TBusMessageContext& msg, TActorId txProxyId, const TActorId& schemeCache, const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters);

    TServerDbOperation(
            const TActorId& hostActor,
            NJson::TJsonValue&& jsonValue,
            const TString& SecurityToken,
            TActorId txProxyId,
            const TActorId& schemeCache,
            const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters
            );

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(MSTATUS_TIMEOUT, "Request timed out", ctx);
    }

    void StateWaitResolve(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxProxySchemeCache::TEvNavigateKeySetResult, Handle);
            HFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void StateWaitExecute(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void ResolveTable(const TString& table, const NActors::TActorContext& ctx) {
        TAutoPtr<NSchemeCache::TSchemeCacheNavigate> request(new NSchemeCache::TSchemeCacheNavigate());
        NSchemeCache::TSchemeCacheNavigate::TEntry entry;
        entry.Path = SplitPath(table);
        if (entry.Path.empty()) {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "Invalid table path specified", ctx);
        }
        entry.Operation = NSchemeCache::TSchemeCacheNavigate::OpTable;
        request->ResultSet.emplace_back(entry);
        ctx.Send(SchemeCache, new TEvTxProxySchemeCache::TEvNavigateKeySet(request));
        ++Requests;
        if (!SecurityToken.empty()) {
            ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket(SecurityToken));
            ++Requests;
        }
        TBase::Become(&TThis::StateWaitResolve, ctx, Timeout, new TEvents::TEvWakeup());
    }

    static NMiniKQL::TRuntimeNode NewDataLiteral(NMiniKQL::TKikimrProgramBuilder& pgmBuilder, const NJson::TJsonValue& jsonValue, NScheme::TTypeId typeId) {
        // TODO
        auto& builder = static_cast<NMiniKQL::TProgramBuilder&>(pgmBuilder);
        switch (typeId) {
        case NScheme::NTypeIds::Bool:
            return builder.NewDataLiteral<bool>(jsonValue.GetBoolean());
        case NScheme::NTypeIds::Float:
            return builder.NewDataLiteral<float>(jsonValue.GetDouble());
        case NScheme::NTypeIds::Double:
            return builder.NewDataLiteral(jsonValue.GetDouble());
        case NScheme::NTypeIds::Int32:
            return builder.NewDataLiteral<i32>(jsonValue.GetInteger());
        case NScheme::NTypeIds::Int64:
            return builder.NewDataLiteral<i64>(jsonValue.GetInteger());
        case NScheme::NTypeIds::Uint32:
            return builder.NewDataLiteral<ui32>(jsonValue.GetUInteger());
        case NScheme::NTypeIds::Uint64:
            return builder.NewDataLiteral<ui64>(jsonValue.GetUInteger());
        case NScheme::NTypeIds::Utf8:
            return builder.NewDataLiteral<NUdf::EDataSlot::Utf8>(jsonValue.GetString());
        case NScheme::NTypeIds::String:
            return builder.NewDataLiteral<NUdf::EDataSlot::String>(jsonValue.GetString());
        case NScheme::NTypeIds::Yson:
            return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Yson>(jsonValue.GetString());
        case NScheme::NTypeIds::Json:
            return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::Json>(jsonValue.GetString());
        case NScheme::NTypeIds::JsonDocument:
            return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::JsonDocument>(jsonValue.GetString());
        case NScheme::NTypeIds::DyNumber:
            return pgmBuilder.NewDataLiteral<NUdf::EDataSlot::DyNumber>(jsonValue.GetString());
        default:
            // still better than VERIFY
            return pgmBuilder.NewEmptyOptionalDataLiteral(typeId);
        }
    }

    void BuildProgram(const NJson::TJsonValue& json, NMiniKQL::TRuntimeNode& pgmReturn, NMiniKQL::TKikimrProgramBuilder& pgmBuilder, const NSchemeCache::TSchemeCacheNavigate::TEntry& tableInfo,
                      const TVector<const NTxProxy::TTableColumnInfo*>& keys, const THashMap<TString, const NTxProxy::TTableColumnInfo*>& columnByName, THashSet<TString> notNullColumns,
                      TVector<NMiniKQL::TRuntimeNode>& result) {
        TVector<NMiniKQL::TRuntimeNode> keyColumns;
        TVector<NScheme::TTypeInfo> keyTypes;
        keyTypes.reserve(keys.size());
        for (const NTxProxy::TTableColumnInfo* key : keys) {
            Y_ABORT_UNLESS(key != nullptr);
            keyTypes.push_back(key->PType);
        }
        NJson::TJsonValue jsonWhere;
        if (json.GetValue("Where", &jsonWhere)) {
            keyColumns.reserve(keys.size());
            for (const NTxProxy::TTableColumnInfo* key : keys) {
                NJson::TJsonValue jsonKey;

                if (key->PType.GetTypeId() == NScheme::NTypeIds::Pg)
                    throw yexception() << "pg types are not supported";

                if (jsonWhere.GetValue(key->Name, &jsonKey)) {
                    keyColumns.emplace_back(NewDataLiteral(pgmBuilder, jsonKey, key->PType.GetTypeId()));
                } else {
                    throw yexception() << "Key \"" << key->Name << "\" was not specified";
                }
            }
        }

        TReadTarget readTarget;
        NJson::TJsonValue jsonReadTarget;
        if (json.GetValue("ReadTarget", &jsonReadTarget)) {
            const auto& readTargetValue = jsonReadTarget.GetString();
            if (readTargetValue == "Online") {
                readTarget = TReadTarget::Online();
            } else
            if (readTargetValue == "Head") {
                readTarget = TReadTarget::Head();
            } else
            if (readTargetValue == "Follower") {
                readTarget = TReadTarget::Follower();
            }
        }

        NJson::TJsonValue jsonSelect;
        if (json.GetValue("Select", &jsonSelect)) {
            TVector<NMiniKQL::TSelectColumn> columnsToRead;
            if (jsonSelect.IsArray()) {
                const NJson::TJsonValue::TArray& array = jsonSelect.GetArray();
                columnsToRead.reserve(array.size());
                for (const NJson::TJsonValue& value : array) {
                    const TString& column = value.GetString();
                    auto itCol = columnByName.find(column);
                    if (itCol != columnByName.end()) {
                        auto nullConstraint = notNullColumns.contains(column) ? EColumnTypeConstraint::NotNull : EColumnTypeConstraint::Nullable;
                        columnsToRead.emplace_back(itCol->second->Name, itCol->second->Id, itCol->second->PType, nullConstraint);
                    } else if (column == "*") {
                        for (const auto& pr : columnByName) {
                            auto nullConstraint = notNullColumns.contains(pr.first) ? EColumnTypeConstraint::NotNull : EColumnTypeConstraint::Nullable;
                            columnsToRead.emplace_back(pr.second->Name, pr.second->Id, pr.second->PType, nullConstraint);
                        }
                    } else {
                        throw yexception() << "Column \"" << value.GetString() << "\" not found";
                    }
                }
            } else if (jsonSelect.IsString() && jsonSelect.GetString() == "*") {
                for (const auto& pr : columnByName) {
                    auto nullConstraint = notNullColumns.contains(pr.first) ? EColumnTypeConstraint::NotNull : EColumnTypeConstraint::Nullable;
                    columnsToRead.emplace_back(pr.second->Name, pr.second->Id, pr.second->PType, nullConstraint);
                }
            }
            if (keyColumns.size() == keyTypes.size()) {
                result.emplace_back(pgmBuilder.SelectRow(tableInfo.TableId, keyTypes, columnsToRead, keyColumns, readTarget));
            } else {
                NMiniKQL::TTableRangeOptions tableRangeOptions = pgmBuilder.GetDefaultTableRangeOptions();
                TVector<NMiniKQL::TRuntimeNode> keyFromColumns = keyColumns;
                for (size_t i = keyColumns.size(); i < keyTypes.size(); ++i) {
                    if (keyTypes[i].GetTypeId() == NScheme::NTypeIds::Pg)
                        throw yexception() << "pg types are not supported";

                    keyFromColumns.emplace_back(pgmBuilder.NewEmptyOptionalDataLiteral(keyTypes[i].GetTypeId()));
                }
                tableRangeOptions.ToColumns = keyColumns;
                tableRangeOptions.FromColumns = keyFromColumns;
                result.emplace_back(pgmBuilder.SelectRange(tableInfo.TableId, keyTypes, columnsToRead, tableRangeOptions, readTarget));
            }
            OperationHistogram = DbOperationsCounters->RequestSelectTimeHistogram;
        }

        NJson::TJsonValue jsonUpdate;
        if (json.GetValue("Update", &jsonUpdate)) {
            const NJson::TJsonValue::TMapType& jsonMap = jsonUpdate.GetMap();
            for (auto itVal = jsonMap.begin(); itVal != jsonMap.end(); ++itVal) {
                auto itCol = columnByName.find(itVal->first);
                if (itCol != columnByName.end()) {
                    if (itCol->second->PType.GetTypeId() == NScheme::NTypeIds::Pg)
                        throw yexception() << "pg types are not supported";

                    auto update = pgmBuilder.GetUpdateRowBuilder();
                    update.SetColumn(itCol->second->Id, itCol->second->PType, pgmBuilder.NewOptional(NewDataLiteral(pgmBuilder, itVal->second, itCol->second->PType.GetTypeId())));
                    pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.UpdateRow(tableInfo.TableId, keyTypes, keyColumns, update));
                } else {
                    throw yexception() << "Column \"" << itVal->first << "\" not found";
                }
            }
            OperationHistogram = DbOperationsCounters->RequestUpdateTimeHistogram;
        }

        NJson::TJsonValue jsonDelete;
        if (json.GetValue("Delete", &jsonDelete)) {
            pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.EraseRow(tableInfo.TableId, keyTypes, keyColumns));
            OperationHistogram = DbOperationsCounters->RequestUpdateTimeHistogram;
        }

        NJson::TJsonValue jsonBatch;
        if (json.GetValue("Batch", &jsonBatch)) {
            const NJson::TJsonValue::TArray& array = jsonBatch.GetArray();
            for (const NJson::TJsonValue& value : array) {
                BuildProgram(value, pgmReturn, pgmBuilder, tableInfo, keys, columnByName, notNullColumns, result);
            }
            OperationHistogram = DbOperationsCounters->RequestBatchTimeHistogram;
        }
    }

    void BuildAndRunProgram(const NActors::TActorContext& ctx) {
        try {
            const NMiniKQL::IFunctionRegistry& functionRegistry = *AppData(ctx)->FunctionRegistry;
            TAlignedPagePoolCounters counters(AppData(ctx)->Counters, "build");
            NMiniKQL::TScopedAlloc alloc(__LOCATION__, counters, functionRegistry.SupportsSizedAllocators());
            NMiniKQL::TTypeEnvironment env(alloc);
            NMiniKQL::TKikimrProgramBuilder pgmBuilder(env, functionRegistry);
            NMiniKQL::TRuntimeNode pgmReturn = pgmBuilder.NewEmptyListOfVoid();
            const NSchemeCache::TSchemeCacheNavigate::TEntry& tableInfo = CacheNavigate->ResultSet.front();
            TVector<const NTxProxy::TTableColumnInfo*> keys;
            THashMap<TString, const NTxProxy::TTableColumnInfo*> columnByName;
            TVector<NMiniKQL::TRuntimeNode> result;

            // reordering key columns
            for (auto itCol = tableInfo.Columns.begin(); itCol != tableInfo.Columns.end(); ++itCol) {
                if (itCol->second.KeyOrder != -1/*key column*/) {
                    if (keys.size() <= (std::size_t)itCol->second.KeyOrder)
                        keys.resize(itCol->second.KeyOrder + 1);
                    keys[itCol->second.KeyOrder] = &itCol->second;
                }
                columnByName[itCol->second.Name] = &itCol->second;
            }

            BuildProgram(JSON, pgmReturn, pgmBuilder, tableInfo, keys, columnByName, tableInfo.NotNullColumns, result);
            if (JSON.Has("Batch")) {
                pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("Result", pgmBuilder.NewTuple(result)));
            } else {
                if (!result.empty()) {
                    pgmReturn = pgmBuilder.Append(pgmReturn, pgmBuilder.SetResult("Result", result.front()));
                }
            }

            NMiniKQL::TRuntimeNode node = pgmBuilder.Build(pgmReturn);
            TString bin = NMiniKQL::SerializeRuntimeNode(node, env);

            DbOperationsCounters->RequestPrepareTimeHistogram->Collect(StartTime.Passed() * 1000/*ms*/);

            TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal(new TEvTxUserProxy::TEvProposeTransaction());
            NKikimrTxUserProxy::TEvProposeTransaction &record = Proposal->Record;
            //record.SetExecTimeoutPeriod(TDuration::Seconds(60));
            record.MutableTransaction()->MutableMiniKQLTransaction()->MutableProgram()->SetBin(bin);
            if (!UserToken.empty()) {
                record.SetUserToken(UserToken);
            }
            ctx.Send(TxProxyId, Proposal.Release());
            TBase::Become(&TThis::StateWaitExecute);
        }
        catch (yexception& e) {
            ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, e.what(), ctx);
        }
    }

    void Die(const TActorContext& ctx) override {
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!InterfaceBase::BootstrapJSON(&JSON, SecurityToken) || !JSON.IsDefined()) {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "Failed to parse JSON", ctx);
        }
        NJson::TJsonValue jsonTable;
        if (JSON.GetValue("Table", &jsonTable) && jsonTable.IsString()) {
            Table = jsonTable.GetString();
        } else {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "Table should be specified", ctx);
        }
        ResolveTable(Table, ctx);
    }
};

template <>
TServerDbOperation<TMessageBusInterface<TBusDbOperation>>::TServerDbOperation(
        TBusMessageContext& msg, TActorId txProxyId, const TActorId& schemeCache, const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters)
    : TMessageBusInterface<TBusDbOperation>(msg)
    , TxProxyId(txProxyId)
    , SchemeCache(schemeCache)
    , DbOperationsCounters(dbOperationsCounters)
{}

template <>
TServerDbOperation<TActorInterface>::TServerDbOperation(
        const TActorId& hostActor,
        NJson::TJsonValue&& jsonValue,
        const TString& securityToken,
        TActorId txProxyId,
        const TActorId& schemeCache,
        const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters
        )
    : TActorInterface(hostActor)
    , TxProxyId(txProxyId)
    , SchemeCache(schemeCache)
    , DbOperationsCounters(dbOperationsCounters)
    , JSON(jsonValue)
    , SecurityToken(securityToken)
{}

template <typename InterfaceBase>
class TServerDbSchema : public TActorBootstrapped<TServerDbSchema<InterfaceBase>>, public InterfaceBase {
protected:
    using TThis = TServerDbSchema<InterfaceBase>;
    using TBase = TActorBootstrapped<TServerDbSchema<InterfaceBase>>;
    using TTabletId = ui64;

    TActorId TxProxyId;
    TIntrusivePtr<TMessageBusDbOpsCounters> DbOperationsCounters;
    NJson::TJsonValue JSON;
    THashMap<TTabletId, TActorId> Pipes;
    TDeque<TAutoPtr<TEvTxUserProxy::TEvProposeTransaction>> Requests;
    THPTimer StartTime;
    TString SecurityToken;
    TString UserToken; // built and serialized

    void CompleteRequest(const TActorContext& ctx) {
        TDuration duration(TDuration::MicroSeconds(StartTime.Passed() * 1000000/*us*/));
        DbOperationsCounters->RequestSchemaTimeHistogram->Collect(duration.MilliSeconds());
        DbOperationsCounters->RequestTotalTimeHistogram->Collect(duration.MilliSeconds());
        Die(ctx);
    }

    void ReplyWithError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext& ctx) {
        InterfaceBase::ReplyWithErrorToInterface(status, proxyStatus, message, ctx);
        CompleteRequest(ctx);
    }

    void ReplyWithError(EResponseStatus status, const TString& message, const TActorContext& ctx) {
        ReplyWithError(status, TEvTxUserProxy::TResultStatus::Unknown, message, ctx);
    }

    void ReplyWithError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TActorContext& ctx) {
        ReplyWithError(status, proxyStatus, TEvTxUserProxy::TResultStatus::Str(proxyStatus), ctx);
    }

    void ReplyWithResult(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus& result, const TActorContext& ctx) {
        InterfaceBase::ReplyWithResultToInterface(status, result, ctx);
        CompleteRequest(ctx);
    }

    void ProcessRequests(const TActorContext& ctx) {
        if (Requests.empty()) {
            CompleteRequest(ctx);
        } else {
            SendNextRequest(ctx);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered::TPtr&, const TActorContext&) {
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult::TPtr&, const TActorContext& ctx) {
        ProcessRequests(ctx);
    }

    void SendNextRequest(const TActorContext& ctx) {
        LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_PROXY,
                    "SendNextRequest proposal" <<
                    " to proxy#" << TxProxyId.ToString());

        TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> proposal = Requests.front();
        Requests.pop_front();
        ctx.Send(TxProxyId, proposal.Release());
    }

    void Handle(TEvTxUserProxy::TEvProposeTransactionStatus::TPtr& ev, const TActorContext& ctx) {
        TEvTxUserProxy::TEvProposeTransactionStatus* msg = ev->Get();
        const TEvTxUserProxy::TEvProposeTransactionStatus::EStatus status = static_cast<TEvTxUserProxy::TEvProposeTransactionStatus::EStatus>(msg->Record.GetStatus());
        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecInProgress) {
            TTabletId schemeShardId = msg->Record.GetSchemeShardTabletId();
            TActorId pipe;
            {
                auto itPipe = Pipes.find(schemeShardId);
                if (itPipe == Pipes.end()) {
                    static NTabletPipe::TClientConfig clientConfig;
                    pipe = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, schemeShardId, clientConfig));
                    Pipes.emplace(schemeShardId, pipe);
                } else {
                    pipe = itPipe->second;
                }
            }
            TAutoPtr<NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion> request(new NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletion());
            request->Record.SetTxId(msg->Record.GetTxId());
            LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_PROXY,
                        "HANDLE TEvProposeTransactionStatus" <<
                        " send to schemeShard tabletid#" << schemeShardId);
            NTabletPipe::SendData(ctx, pipe, request.Release());
            return;
        }

        if (status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecComplete
                || status == TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAlready) {
            if (Requests.empty()) {
                ReplyWithResult(MSTATUS_OK, msg->Record, ctx);
            } else {
                SendNextRequest(ctx);
            }
            return;
        }

        switch (status) {
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyAccepted:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyResolved:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyPrepared:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::CoordinatorPlanned:
        // transitional statuses
            return;
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecTimeout:
            return ReplyWithResult(MSTATUS_INPROGRESS, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyNotReady:
            return ReplyWithError(MSTATUS_NOTREADY, status, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecAborted:
            return ReplyWithError(MSTATUS_ABORTED, status, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::EmptyAffectedSet:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ResolveError:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ExecError:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest:
            return ReplyWithResult(MSTATUS_ERROR, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardNotAvailable:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardTryLater:
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardOverloaded:
            return ReplyWithResult(MSTATUS_REJECTED, msg->Record, ctx);
        case TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::ProxyShardUnknown:
            return ReplyWithResult(MSTATUS_TIMEOUT, msg->Record, ctx);
        default:
            return ReplyWithResult(MSTATUS_INTERNALERROR, msg->Record, ctx);
        }
    }

    void Handle(TEvTicketParser::TEvAuthorizeTicketResult::TPtr &ev, const TActorContext &ctx) {
        const TEvTicketParser::TEvAuthorizeTicketResult& result(*ev->Get());
        if (!result.Error.empty()) {
            //return TBase::HandleError(EResponseStatus::MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::AccessDenied, result.Error, ctx);
        } else {
            UserToken = result.Token->GetSerializedToken();
        }
        BuildRequests(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TServerDbSchema(TBusMessageContext &msg, TActorId txProxyId, const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters);

    TServerDbSchema(
            const TActorId& hostActor,
            NJson::TJsonValue&& jsonValue,
            const TString& securityToken,
            TActorId txProxyId,
            const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters
            );

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvProposeTransactionStatus, Handle);
            HFunc(TEvTicketParser::TEvAuthorizeTicketResult, Handle)
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionRegistered, Handle);
            HFunc(NSchemeShard::TEvSchemeShard::TEvNotifyTxCompletionResult, Handle);
        }
    }

    void Die(const TActorContext& ctx) override {
        for (auto it = Pipes.begin(); it != Pipes.end(); ++it) {
            NTabletPipe::CloseClient(ctx, it->second);
        }
        TBase::Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!InterfaceBase::BootstrapJSON(&JSON, SecurityToken) || !JSON.IsDefined()) {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "Failed to parse JSON", ctx);
        }
        if (!SecurityToken.empty()) {
            ctx.Send(MakeTicketParserID(), new TEvTicketParser::TEvAuthorizeTicket(SecurityToken));
        } else {
            BuildRequests(ctx);
        }
        TBase::Become(&TThis::StateWork, ctx, Timeout, new TEvents::TEvWakeup());
    }

    void BuildRequests(const TActorContext& ctx) {
        TString path;
        NJson::TJsonValue jsonPath;
        if (JSON.GetValue("Path", &jsonPath)) {
            path = jsonPath.GetString();
        } else {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "Path not found", ctx);
        }

        NJson::TJsonValue jsonDropTable;
        if (JSON.GetValue("DropTable", &jsonDropTable)) {
            if (jsonDropTable.IsArray()) {
                const NJson::TJsonValue::TArray& array = jsonDropTable.GetArray();
                for (const NJson::TJsonValue& value : array) {
                    TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal(new TEvTxUserProxy::TEvProposeTransaction());
                    NKikimrSchemeOp::TModifyScheme& modifyScheme(*Proposal->Record.MutableTransaction()->MutableModifyScheme());
                    modifyScheme.SetWorkingDir(path);
                    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                    modifyScheme.MutableDrop()->SetName(value.GetString());
                    Requests.emplace_back(Proposal.Release());
                }
            } else {
                TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal(new TEvTxUserProxy::TEvProposeTransaction());
                NKikimrSchemeOp::TModifyScheme& modifyScheme(*Proposal->Record.MutableTransaction()->MutableModifyScheme());
                modifyScheme.SetWorkingDir(path);
                modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpDropTable);
                modifyScheme.MutableDrop()->SetName(jsonDropTable.GetString());
                Requests.emplace_back(Proposal.Release());
            }
        }

        NJson::TJsonValue jsonMkDir;
        if (JSON.GetValue("MkDir", &jsonMkDir)) {
            if (jsonMkDir.IsArray()) {
                const NJson::TJsonValue::TArray& array = jsonMkDir.GetArray();
                for (const NJson::TJsonValue& value : array) {
                    TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal(new TEvTxUserProxy::TEvProposeTransaction());
                    NKikimrSchemeOp::TModifyScheme& modifyScheme(*Proposal->Record.MutableTransaction()->MutableModifyScheme());
                    modifyScheme.SetWorkingDir(path);
                    modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
                    modifyScheme.MutableMkDir()->SetName(value.GetString());
                    Requests.emplace_back(Proposal.Release());
                }
            } else {
                TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal(new TEvTxUserProxy::TEvProposeTransaction());
                NKikimrSchemeOp::TModifyScheme& modifyScheme(*Proposal->Record.MutableTransaction()->MutableModifyScheme());
                modifyScheme.SetWorkingDir(path);
                modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpMkDir);
                modifyScheme.MutableMkDir()->SetName(jsonMkDir.GetString());
                Requests.emplace_back(Proposal.Release());
            }
        }

        NJson::TJsonValue jsonCreateTable;
        if (JSON.GetValue("CreateTable", &jsonCreateTable)) {
            const NJson::TJsonValue::TMapType& jsonTables = jsonCreateTable.GetMap();
            for (auto itTable = jsonTables.begin(); itTable != jsonTables.end(); ++itTable) {
                TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> Proposal(new TEvTxUserProxy::TEvProposeTransaction());
                NKikimrSchemeOp::TModifyScheme& modifyScheme(*Proposal->Record.MutableTransaction()->MutableModifyScheme());
                modifyScheme.SetWorkingDir(path);
                modifyScheme.SetOperationType(NKikimrSchemeOp::EOperationType::ESchemeOpCreateTable);
                NKikimrSchemeOp::TTableDescription& createTable(*modifyScheme.MutableCreateTable());
                createTable.SetName(itTable->first);
                // parsing CreateTable/<name>/Columns
                NJson::TJsonValue jsonColumn;
                if (itTable->second.GetValue("Columns", &jsonColumn)) {
                    const NJson::TJsonValue::TMapType& jsonColumns = jsonColumn.GetMap();
                    for (auto itColumn = jsonColumns.begin(); itColumn != jsonColumns.end(); ++itColumn) {
                        NKikimrSchemeOp::TColumnDescription& column(*createTable.AddColumns());
                        column.SetName(itColumn->first);
                        column.SetType(itColumn->second.GetString());
                    }
                }
                // parsing CreateTable/<name>/Key
                NJson::TJsonValue jsonKey;
                if (itTable->second.GetValue("Key", &jsonKey)) {
                    if (jsonKey.IsArray()) {
                        const NJson::TJsonValue::TArray& array = jsonKey.GetArray();
                        for (const NJson::TJsonValue& value : array) {
                            createTable.AddKeyColumnNames(value.GetString());
                        }
                    } else {
                        createTable.AddKeyColumnNames(jsonKey.GetString());
                    }
                }
                // parsing CreateTable/<name>/Partitions
                NJson::TJsonValue jsonPartitions;
                if (itTable->second.GetValue("Partitions", &jsonPartitions)) {
                    createTable.SetUniformPartitionsCount(jsonPartitions.GetUInteger());
                }
                // parsing CreateTable/<name>/PartitionConfig
                try {
                    NJson::TJsonValue jsonPartitionConfig;
                    if (itTable->second.GetValue("PartitionConfig", &jsonPartitionConfig)) {
                        NProtobufJson::Json2Proto(jsonPartitionConfig, *createTable.MutablePartitionConfig());
                    }
                    Requests.emplace_back(Proposal.Release());
                } catch(const yexception&) {
                    // Json2Proto throws exception on invalid json
                }
            }
        }

        DbOperationsCounters->RequestPrepareTimeHistogram->Collect(StartTime.Passed() * 1000/*ms*/);
        if (Requests.empty()) {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "No valid operations were found", ctx);
        }
        ProcessRequests(ctx);
    }
};

template <>
TServerDbSchema<TMessageBusInterface<TBusDbSchema>>::TServerDbSchema(
        TBusMessageContext &msg, TActorId txProxyId, const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters)
    : TMessageBusInterface<TBusDbSchema>(msg)
    , TxProxyId(txProxyId)
    , DbOperationsCounters(dbOperationsCounters)
{}

template <>
TServerDbSchema<TActorInterface>::TServerDbSchema(
        const TActorId& hostActor,
        NJson::TJsonValue&& jsonValue,
        const TString& securityToken,
        TActorId txProxyId,
        const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters
        )
    : TActorInterface(hostActor)
    , TxProxyId(txProxyId)
    , DbOperationsCounters(dbOperationsCounters)
    , JSON(jsonValue)
    , SecurityToken(securityToken)
{}

template <typename InterfaceBase>
class TServerDbBatch : public TActorBootstrapped<TServerDbBatch<InterfaceBase>>, public InterfaceBase {
protected:
    using TThis = TServerDbBatch<InterfaceBase>;
    using TBase = TActorBootstrapped<TServerDbBatch<InterfaceBase>>;
    using TTabletId = ui64;

    TActorId TxProxyId;
    TActorId SchemeCache;
    TIntrusivePtr<TMessageBusDbOpsCounters> DbOperationsCounters;
    NJson::TJsonValue JSON;
    TDeque<TAutoPtr<IActor>> Operations;
    TDeque<TAutoPtr<TEvPrivate::TEvReply>> Replies;
    int MaxInFlight;
    int CurrentInFlight;
    TString SecurityToken;

    void CompleteRequest(const TActorContext& ctx) {
        TBase::Die(ctx);
    }

    void ReplyWithError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TString& message, const TActorContext& ctx) {
        InterfaceBase::ReplyWithErrorToInterface(status, proxyStatus, message, ctx);
        CompleteRequest(ctx);
    }

    void ReplyWithError(EResponseStatus status, const TString& message, const TActorContext& ctx) {
        ReplyWithError(status, TEvTxUserProxy::TResultStatus::Unknown, message, ctx);
    }

    void ReplyWithError(EResponseStatus status,  TEvTxUserProxy::TResultStatus::EStatus proxyStatus, const TActorContext& ctx) {
        ReplyWithError(status, proxyStatus, TEvTxUserProxy::TResultStatus::Str(proxyStatus), ctx);
    }

    void ReplyWithResult(EResponseStatus status, const NKikimrTxUserProxy::TEvProposeTransactionStatus& result, const TActorContext& ctx) {
        InterfaceBase::ReplyWithResultToInterface(status, result, ctx);
        CompleteRequest(ctx);
    }

    void ProcessOperations(const TActorContext& ctx) {
        if (!Operations.empty()) {
            RunNextOperation(ctx);
        }
    }

    void RunNextOperation(const TActorContext& ctx) {
        if (CurrentInFlight < MaxInFlight) {
            TAutoPtr<IActor> operation = Operations.front();
            Operations.pop_front();
            ctx.Register(operation.Release());
            ++CurrentInFlight;
        }
    }

    void Handle(TEvPrivate::TEvReply::TPtr& ev, const TActorContext& ctx) {
        Replies.emplace_back(ev->Release());
        --CurrentInFlight;
        ProcessOperations(ctx);
        if (CurrentInFlight == 0) {
            NKikimrTxUserProxy::TEvProposeTransactionStatus mergedResult;
            EResponseStatus status = MSTATUS_OK;
            TString message;
            for (const TAutoPtr<TEvPrivate::TEvReply>& reply : Replies) {
                if (status == MSTATUS_OK && reply->Status != MSTATUS_OK) {
                    status = reply->Status;
                    message = reply->Message;
                }
                mergedResult.MergeFrom(reply->Result);
            }
            if (status != MSTATUS_OK) {
                ReplyWithError(status, TEvTxUserProxy::TResultStatus::Unknown, message, ctx);
            } else {
                ReplyWithResult(status, mergedResult, ctx);
            }
        }
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::MSGBUS_COMMON;
    }

    TServerDbBatch(TBusMessageContext &msg, const TActorId txProxyId, const TActorId& schemeCache, const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters);

    void HandleTimeout(const TActorContext& ctx) {
        return ReplyWithError(MSTATUS_TIMEOUT, "Request timed out", ctx);
    }

    void StateWork(TAutoPtr<NActors::IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvReply, Handle);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        if (!InterfaceBase::BootstrapJSON(&JSON, SecurityToken) || !JSON.IsDefined()) {
            return ReplyWithError(MSTATUS_ERROR, TEvTxUserProxy::TEvProposeTransactionStatus::EStatus::WrongRequest, "Failed to parse JSON", ctx);
        }
        NJson::TJsonValue jsonOperations;
        if (JSON.GetValue("Operations", &jsonOperations)) {
            NJson::TJsonValue::TArray& array = jsonOperations.GetArraySafe();
            for (NJson::TJsonValue& value : array) {
                if (value.IsMap()) {
                    if (value.Has("Path")) {
                        Operations.emplace_back(new TServerDbSchema<TActorInterface>(
                                                    ctx.SelfID,
                                                    std::move(value),
                                                    SecurityToken,
                                                    TxProxyId,
                                                    DbOperationsCounters
                                                    ));
                    } else {
                        Operations.emplace_back(new TServerDbOperation<TActorInterface>(
                                                    ctx.SelfID,
                                                    std::move(value),
                                                    SecurityToken,
                                                    TxProxyId,
                                                    SchemeCache,
                                                    DbOperationsCounters
                                                    ));
                    }
                }
            }
        }
        TBase::Become(&TThis::StateWork);
        ProcessOperations(ctx);
    }
};

template <>
TServerDbBatch<TMessageBusInterface<TBusDbBatch>>::TServerDbBatch(
        TBusMessageContext &msg, TActorId txProxyId, const TActorId& schemeCache, const TIntrusivePtr<TMessageBusDbOpsCounters>& dbOperationsCounters)
    : TMessageBusInterface<TBusDbBatch>(msg)
    , TxProxyId(txProxyId)
    , SchemeCache(schemeCache)
    , DbOperationsCounters(dbOperationsCounters)
    , MaxInFlight(1)
    , CurrentInFlight(0)
{}

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvDbSchema::TPtr& ev, const TActorContext& ctx) {
    TEvBusProxy::TEvDbSchema* msg = ev->Get();
    LOG_DEBUG_S(ctx, NKikimrServices::MSGBUS_PROXY,
                " actor# "<< ctx.SelfID.ToString() <<
                " HANDLE TEvDbSchema");
    auto* requestActor = new TServerDbSchema<TMessageBusInterface<TBusDbSchema>>(msg->MsgContext, TxProxy, DbOperationsCounters);
    ctx.Register(requestActor);
}

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvDbOperation::TPtr& ev, const TActorContext& ctx) {
    TEvBusProxy::TEvDbOperation* msg = ev->Get();
    auto* requestActor = new TServerDbOperation<TMessageBusInterface<TBusDbOperation>>(msg->MsgContext, TxProxy, SchemeCache, DbOperationsCounters);
    ctx.Register(requestActor);
}

void TMessageBusServerProxy::Handle(TEvBusProxy::TEvDbBatch::TPtr& ev, const TActorContext& ctx) {
    TEvBusProxy::TEvDbBatch* msg = ev->Get();
    auto* requestActor = new TServerDbBatch<TMessageBusInterface<TBusDbBatch>>(msg->MsgContext, TxProxy, SchemeCache, DbOperationsCounters);
    ctx.Register(requestActor);
}

}
}
