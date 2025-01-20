#include "etcd_impl.h"
#include "events.h"
#include "appdata.h"

#include <ydb/apps/etcd_proxy/proto/rpc.grpc.pb.h>

#include <ydb/core/base/path.h>
#include <ydb/core/grpc_services/rpc_scheme_base.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/mind/local.h>
#include <ydb/core/protos/local.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_query/tx.h>


namespace NKikimr::NGRpcService {

using TEvRangeKVRequest = TGrpcRequestOperationCall<etcdserverpb::RangeRequest, etcdserverpb::RangeResponse>;
using TEvPutKVRequest = TGrpcRequestOperationCall<etcdserverpb::PutRequest, etcdserverpb::PutResponse>;
using TEvDeleteRangeKVRequest = TGrpcRequestOperationCall<etcdserverpb::DeleteRangeRequest, etcdserverpb::DeleteRangeResponse>;
using TEvTxnKVRequest = TGrpcRequestOperationCall<etcdserverpb::TxnRequest, etcdserverpb::TxnResponse>;
using TEvCompactKVRequest = TGrpcRequestOperationCall<etcdserverpb::CompactionRequest, etcdserverpb::CompactionResponse>;

using namespace NActors;
using namespace Ydb;

namespace {

TString DecrementKey(TString key) {
    for (auto i = key.size(); i > 0u;) {
        if (const auto k = key[--i]) {
            key[i] = k - '\x01';
            return key;
        } else {
            key[i] = '\xFF';
        }
    }
    return TString();
}

void MakeSimplePredicate(const TString& key, const TString& rangeEnd, TStringBuilder& sql, NYdb::TParamsBuilder& params, size_t& paramsCounter) {
    const auto keyParamName = TString("$Key") += ToString(paramsCounter++);
    params.AddParam(keyParamName).String(key).Build();

    sql << "where ";
    if (rangeEnd.empty())
        sql << "`key` = " << keyParamName;
    else if (rangeEnd == key)
        sql << "startswith(`key`, " << keyParamName << ')';
    else {
        const auto rangeEndParamName = TString("$RangeEnd") += ToString(paramsCounter++);
        params.AddParam(rangeEndParamName).String(rangeEnd).Build();
        sql << "`key` between $Key and " << rangeEndParamName;
    }
}

struct TOperation {
    size_t ResultIndex = 0ULL;
};


struct TRange : public TOperation {
    TString Key, RangeEnd;
    bool KeysOnly, CountOnly;
    ui64 Limit;
    i64 KeyRevision;
    i64 MinCreateRevision, MaxCreateRevision;
    i64 MinModificateRevision, MaxModificateRevision;
    std::optional<bool> SortOrder;
    size_t SortTarget;

    bool Parse(const etcdserverpb::RangeRequest& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        KeysOnly = rec.keys_only();
        CountOnly = rec.count_only();
        Limit = rec.limit();
        KeyRevision = rec.revision();
        MinCreateRevision = rec.min_create_revision();
        MaxCreateRevision = rec.max_create_revision();
        MinModificateRevision = rec.min_mod_revision();
        MaxModificateRevision = rec.max_mod_revision();
        SortTarget = rec.sort_target();
        switch (rec.sort_order()) {
            case etcdserverpb::RangeRequest_SortOrder_ASCEND: SortOrder = true; break;
            case etcdserverpb::RangeRequest_SortOrder_DESCEND: SortOrder = false; break;
            default: break;
        }
        return !Key.empty() && (RangeEnd.empty() || Key <= RangeEnd);
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params, size_t& paramsCounter, size_t& resultsCounter) {
        ++resultsCounter;
        sql << "select ";
        if (CountOnly)
            sql << "count(*)";
        else if (KeysOnly)
            sql << "`key`";
        else
            sql << "`key`,`value`,`created`,`modified`,`version`,`lease`";
        sql << Endl << "from ";
        const bool fromHistory = KeyRevision || MinCreateRevision || MaxCreateRevision || MinModificateRevision || MaxModificateRevision;
        sql << '`' << (fromHistory ? "verhaal" : "huidig") << '`' << Endl;

        MakeSimplePredicate(Key, RangeEnd, sql, params, paramsCounter);
        if (KeyRevision) {
            const auto paramName = TString("$Revision") += ToString(paramsCounter++);
            sql << Endl << '\t' << "and `modified` = " << paramName;
            params.AddParam(paramName).Int64(KeyRevision).Build();
        }

        if (MinCreateRevision) {
            const auto paramName = TString("$MinCreateRevision") += ToString(paramsCounter++);
            sql << Endl << '\t' << "and `created` >= " << paramName;
            params.AddParam(paramName).Int64(MinCreateRevision).Build();
        }

        if (MaxCreateRevision) {
            const auto paramName = TString("$MaxCreateRevision") += ToString(paramsCounter++);
            sql << Endl << '\t' << "and `created` <= " << paramName;
            params.AddParam(paramName).Int64(MaxCreateRevision).Build();
        }

        if (MinModificateRevision) {
            const auto paramName = TString("$MinModificateRevision") += ToString(paramsCounter++);
            sql << Endl << '\t' << "and `modified` >= " << paramName;
            params.AddParam(paramName).Int64(MinModificateRevision).Build();
        }

        if (MaxModificateRevision) {
            const auto paramName = TString("$MaxModificateRevision") += ToString(paramsCounter++);
            sql << Endl << '\t' << "and `modified` <= " << paramName;
            params.AddParam(paramName).Int64(MaxModificateRevision).Build();
        }

        if (SortOrder) {
            static constexpr std::string_view Fields[] = {"key"sv, "version"sv, "created"sv, "modified"sv, "value"sv};
            sql << Endl << "order by `" << Fields[SortTarget] << "` " << (*SortOrder ? "asc" : "desc");
        }

        if (Limit) {
            const auto paramName = TString("$Limit") += ToString(paramsCounter++);
            sql << Endl << "LIMIT " << paramName;
            params.AddParam(paramName).Uint64(Limit).Build();
        }

        sql << ';' << Endl;
    }

    etcdserverpb::RangeResponse MakeResponse(const NYdb::TResultSets& results) {
        etcdserverpb::RangeResponse response;
        if (!results.empty()) {
            auto parser = NYdb::TResultSetParser(results[ResultIndex]);
            if (CountOnly) {
                if (parser.TryNextRow()) {
                    response.set_count(NYdb::TValueParser(parser.GetValue(0)).GetUint64());
                }
            } else if (KeysOnly) {
                while (parser.TryNextRow()) {
                    response.add_kvs()->set_key(NYdb::TValueParser(parser.GetValue(0)).GetString());
                }
            } else {
                while (parser.TryNextRow()) {
                    const auto kvs = response.add_kvs();
                    kvs->set_key(NYdb::TValueParser(parser.GetValue("key")).GetString());
                    kvs->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                    kvs->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                    kvs->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                    kvs->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                    kvs->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
                }
            }
        }
        return response;
    }
};

struct TPut : public TOperation {
    TString Key, Value;
    i64 Lease = 0LL;
    bool GetPrevious = false;
    bool IgnoreValue = false;
    bool IgnoreLease = false;

    bool Parse(const etcdserverpb::PutRequest& rec) {
        Key = rec.key();
        Value = rec.value();
        Lease = rec.lease();
        GetPrevious = rec.prev_kv();
        IgnoreValue = rec.ignore_value();
        IgnoreLease = rec.ignore_lease();
        return !Key.empty();
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params, size_t& paramsCounter, size_t& resultsCounter) {
        const bool update = IgnoreValue || IgnoreLease;
        sql << Endl << NResource::Find(update ? "update.sql" : "upsert.sql") << Endl;

        const auto keyParamName = TString("$Key") += ToString(paramsCounter++);
        const auto valueParamName = IgnoreValue ? TString("NULL") : TString("$Value") += ToString(paramsCounter++);
        const auto leaseParamName = IgnoreValue ? TString("NULL") : TString("$Lease") += ToString(paramsCounter++);
        params.AddParam(keyParamName).String(Key).Build();
        if (!IgnoreValue)
            params.AddParam(valueParamName).String(Value).Build();
        if (!IgnoreLease)
            params.AddParam(leaseParamName).Int64(Lease).Build();

        if (GetPrevious) {
            ResultIndex = resultsCounter++;
            sql << "select `value`, `created`, `modified`, `version`,`lease` from `huidig` where `key` = " << keyParamName << ';' << Endl;
        }

        sql << Endl << "do $up" << (update ? "date" : "sert") << "($Revision," << keyParamName << ',' << valueParamName << ',' << leaseParamName << ");" << Endl;
    }

    etcdserverpb::PutResponse MakeResponse(const NYdb::TResultSets& results) {
        etcdserverpb::PutResponse response;
        if (GetPrevious) {
            if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow() && 5ULL == parser.ColumnsCount()) {
                const auto prev = response.mutable_prev_kv();
                prev->set_key(Key);
                prev->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                prev->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                prev->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                prev->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                prev->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
            }
        }
        return response;
    }
};

struct TDeleteRange : public TOperation {
    TString Key, RangeEnd;
    bool GetPrevious = false;

    bool Parse(const etcdserverpb::DeleteRangeRequest& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        GetPrevious = rec.prev_kv();
        return !Key.empty() && (RangeEnd.empty() || Key <= RangeEnd);
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params, size_t& paramsCounter, size_t& resultsCounter) {
        TStringBuilder where;
        MakeSimplePredicate(Key, RangeEnd, where, params, paramsCounter);
        ResultIndex = resultsCounter++;
        sql << "select count(*) from `huidig` " << where << ';' << Endl;
        if (GetPrevious) {
            ++resultsCounter;
            sql << "select `key`,`value`, `created`, `modified`, `version`,`lease` from `huidig` " << where << ';' << Endl;
        }
        sql << "delete from `huidig` " << where << ';' << Endl;
    }

    etcdserverpb::DeleteRangeResponse MakeResponse(const NYdb::TResultSets& results) {
        etcdserverpb::DeleteRangeResponse response;
        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            response.set_deleted(NYdb::TValueParser(parser.GetValue(0)).GetUint64());
        }

        if (GetPrevious) {
            auto parser = NYdb::TResultSetParser(results[ResultIndex + 1U]);
            while (parser.TryNextRow()) {
                const auto kvs = response.add_prev_kvs();
                kvs->set_key(NYdb::TValueParser(parser.GetValue("key")).GetString());
                kvs->set_value(NYdb::TValueParser(parser.GetValue("value")).GetString());
                kvs->set_mod_revision(NYdb::TValueParser(parser.GetValue("modified")).GetInt64());
                kvs->set_create_revision(NYdb::TValueParser(parser.GetValue("created")).GetInt64());
                kvs->set_version(NYdb::TValueParser(parser.GetValue("version")).GetInt64());
                kvs->set_lease(NYdb::TValueParser(parser.GetValue("lease")).GetInt64());
            }
        }
        return response;
    }
};

struct TCompare {
    TString Key, RangeEnd;

    std::variant<i64, TString> Value;

    size_t Result, Target;

    bool Parse(const etcdserverpb::Compare& rec) {
        Key = rec.key();
        RangeEnd = DecrementKey(rec.range_end());
        Result = rec.result();
        Target = rec.target();
        switch (rec.target()) {
            case etcdserverpb::Compare_CompareTarget_VERSION:
                Value = rec.version();
                break;
            case etcdserverpb::Compare_CompareTarget_CREATE:
                Value = rec.create_revision();
                break;
            case etcdserverpb::Compare_CompareTarget_MOD:
                Value = rec.mod_revision();
                break;
            case etcdserverpb::Compare_CompareTarget_VALUE:
                Value = rec.value();
                break;
            case etcdserverpb::Compare_CompareTarget_LEASE:
                Value = rec.lease();
                break;
            default:
                break;
        }

        return !Key.empty() && (RangeEnd.empty() || Key <= RangeEnd);
    }

};

struct TTxn : public TOperation {
    using TRequestOp = std::variant<TRange, TPut, TDeleteRange, TTxn>;

    std::vector<TCompare> Compares;
    std::vector<TRequestOp> Success, Failure;

    template<class TOperation, class TSrc>
    static bool Parse(std::vector<TRequestOp>& operations, const TSrc& src) {
        TOperation op;
        if (!op.Parse(src))
            return false;
        operations.emplace_back(std::move(op));
        return true;
    }

    bool Parse(const etcdserverpb::TxnRequest& rec) {
        for (const auto& comp : rec.compare()) {
            Compares.emplace_back();
            if (!Compares.back().Parse(comp))
                return false;
        }

        const auto fill = [](std::vector<TRequestOp>& operations, const auto& fields) {
            for (const auto& op : fields) {
                switch (op.request_case()) {
                    case etcdserverpb::RequestOp::RequestCase::kRequestRange: {
                        if (!Parse<TRange>(operations, op.request_range()))
                            return false;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestPut: {
                        if (!Parse<TPut>(operations, op.request_put()))
                            return false;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestDeleteRange: {
                        if (!Parse<TDeleteRange>(operations, op.request_delete_range()))
                            return false;
                        break;
                    }
                    case etcdserverpb::RequestOp::RequestCase::kRequestTxn: {
                        if (!Parse<TTxn>(operations, op.request_txn()))
                            return false;
                        break;
                    }
                    default:
                        return false;
                }
            }
            return true;
        };

        return !Compares.empty() && fill(Success, rec.success()) && fill(Failure, rec.failure());
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params, size_t& paramsCounter, size_t& resultsCounter) {
        static constexpr std::string_view Fields[] = {"version"sv, "created"sv, "modified"sv, "value"sv, "lease"sv};
        static constexpr std::string_view Comparator[] = {"="sv, ">"sv, "<"sv, "!="sv};

        ResultIndex = resultsCounter++;
        sql << "select bool_and(`cmp`) ?? false from (" << Endl;
        for (auto i = 0U; i < Compares.size(); ++i) {
            if (i)
                sql << Endl << "union all" << Endl;
            const auto argParamName = TString("$Arg") += ToString(paramsCounter++);
            sql << "select bool_and(`" << Fields[Compares[i].Target] << "` " << Comparator[Compares[i].Result] << ' ' << argParamName << ") ?? false as `cmp` from `huidig` ";
            MakeSimplePredicate(Compares[i].Key, Compares[i].RangeEnd, sql, params, paramsCounter);
            if (const auto val = std::get_if<i64>(&Compares[i].Value))
                params.AddParam(argParamName).Int64(*val).Build();
            if (const auto val = std::get_if<TString>(&Compares[i].Value))
                params.AddParam(argParamName).String(*val).Build();
        }

        sql << Endl << ");" << Endl;
    }

    etcdserverpb::TxnResponse MakeResponse(const NYdb::TResultSets& results) {
        etcdserverpb::TxnResponse response;
        if (auto parser = NYdb::TResultSetParser(results[ResultIndex]); parser.TryNextRow()) {
            response.set_succeeded(NYdb::TValueParser(parser.GetValue(0)).GetBool());
        }
        return response;
    }
};

template <typename TDerived>
class TBaseEtcdRequest {
protected:
    virtual bool ParseGrpcRequest() = 0;
    virtual void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params) = 0;
    virtual void ReplyWith(const NYdb::TResultSets& results) = 0;

    i64 Revision;
};

template <typename TDerived, typename TRequest, bool IsOperation>
class TEtcdRequestWithOperationParamsActor : public TActorBootstrapped<TDerived> {
private:
    typedef TActorBootstrapped<TDerived> TBase;
    typedef typename std::conditional<IsOperation, IRequestOpCtx, IRequestNoOpCtx>::type TRequestBase;
public:
    enum EWakeupTag {
        WakeupTagTimeout = 10,
        WakeupTagCancel = 11,
        WakeupTagGetConfig = 21,
        WakeupTagClientLost = 22,
    };
public:
    TEtcdRequestWithOperationParamsActor(TRequestBase* request)
        : Request_(request)
    {
    }

    const typename TRequest::TRequest* GetProtoRequest() const {
        return TRequest::GetProtoRequest(Request_);
    }

    Ydb::Operations::OperationParams::OperationMode GetOperationMode() const {
        return GetProtoRequest()->operation_params().operation_mode();
    }

    void Bootstrap(const TActorContext &ctx) {
        HasCancel_ = static_cast<TDerived*>(this)->HasCancelOperation();

        if (OperationTimeout_) {
            OperationTimeoutTimer = CreateLongTimer(ctx, OperationTimeout_,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(WakeupTagTimeout)),
                AppData(ctx)->UserPoolId);
        }

        if (HasCancel_ && CancelAfter_) {
            CancelAfterTimer = CreateLongTimer(ctx, CancelAfter_,
                new IEventHandle(ctx.SelfID, ctx.SelfID, new TEvents::TEvWakeup(WakeupTagCancel)),
                AppData(ctx)->UserPoolId);
        }

        auto selfId = ctx.SelfID;
        auto* actorSystem = ctx.ExecutorThread.ActorSystem;
        auto clientLostCb = [selfId, actorSystem]() {
            actorSystem->Send(selfId, new TRpcServices::TEvForgetOperation());
        };

        Request_->SetFinishAction(std::move(clientLostCb));
    }

    bool HasCancelOperation() {
        return false;
    }

    TRequestBase& Request() const {
        return *Request_;
    }

protected:
    TDuration GetOperationTimeout() {
        return OperationTimeout_;
    }

    TDuration GetCancelAfter() {
        return CancelAfter_;
    }

    void DestroyTimers() {
        auto& ctx = TlsActivationContext->AsActorContext();
        if (OperationTimeoutTimer) {
            ctx.Send(OperationTimeoutTimer, new TEvents::TEvPoisonPill);
        }
        if (CancelAfterTimer) {
            ctx.Send(CancelAfterTimer, new TEvents::TEvPoisonPill);
        }
    }

    void PassAway() override {
        DestroyTimers();
        TBase::PassAway();
    }

    TRequest* RequestPtr() {
        return static_cast<TRequest*>(Request_.get());
    }

protected:
    std::shared_ptr<TRequestBase> Request_;

    TActorId OperationTimeoutTimer;
    TActorId CancelAfterTimer;
    TDuration OperationTimeout_;
    TDuration CancelAfter_;
    bool HasCancel_ = false;
    bool ReportCostInfo_ = false;
};

template <typename TDerived, typename TRequest>
class TEtcdOperationRequestActor : public TEtcdRequestWithOperationParamsActor<TDerived, TRequest, true> {
private:
    typedef TEtcdRequestWithOperationParamsActor<TDerived, TRequest, true> TBase;

public:

    TEtcdOperationRequestActor(IRequestOpCtx* request)
        : TBase(request)
        , Span_(TWilsonGrpc::RequestActor, request->GetWilsonTraceId(),
                "RequestProxy.RpcOperationRequestActor", NWilson::EFlags::AUTO_END)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DEFERRABLE_RPC;
    }

    void OnCancelOperation(const TActorContext& ctx) {
        Y_UNUSED(ctx);
    }

    void OnForgetOperation(const TActorContext& ctx) {
        // No client is waiting for the reply, but we have to issue fake reply
        // anyway before dying to make Grpc happy.
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Closing Grpc request, client should not see this message."));
        Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, ctx);
    }

    void OnOperationTimeout(const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
            "Operation timeout."));
        Reply(Ydb::StatusIds::TIMEOUT, issues, ctx);
    }

protected:
    void StateFuncBase(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvWakeup, HandleWakeup);
            HFunc(TRpcServices::TEvForgetOperation, HandleForget);
            hFunc(TEvSubscribeGrpcCancel, HandleSubscribeiGrpcCancel);
            default: {
                NYql::TIssues issues;
                issues.AddIssue(MakeIssue(NKikimrIssues::TIssuesIds::DEFAULT_ERROR,
                    TStringBuilder() << "Unexpected event received in TEtcdOperationRequestActor::StateWork: "
                        << ev->GetTypeRewrite()));
                return this->Reply(Ydb::StatusIds::INTERNAL_ERROR, issues, TActivationContext::AsActorContext());
            }
        }
    }

protected:
    using TBase::Request_;

    void Reply(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message, const TActorContext& ctx)
    {
        NYql::TIssues issues;
        IssuesFromMessage(message, issues);
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues, const TActorContext& ctx) {
        Request_->RaiseIssues(issues);
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TString& message, NKikimrIssues::TIssuesIds::EIssueCode issueCode, const TActorContext& ctx) {
        NYql::TIssues issues;
        issues.AddIssue(MakeIssue(issueCode, message));
        Reply(status, issues, ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, const TActorContext& ctx) {
        Request_->ReplyWithYdbStatus(status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void Reply(Ydb::StatusIds::StatusCode status, typename TRequest::TResponse& resp, const TActorContext& ctx) {
        Request_->Reply(&resp);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    template<typename TResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status,
        const google::protobuf::RepeatedPtrField<TYdbIssueMessageType>& message,
        const TResult& result,
        const TActorContext& ctx)
    {
        Request_->SendResult(result, status, message);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    template<typename TResult>
    void ReplyWithResult(Ydb::StatusIds::StatusCode status,
                         const TResult& result,
                         const TActorContext& ctx) {
        Request_->SendResult(result, status);
        NWilson::EndSpanWithStatus(Span_, status);
        this->Die(ctx);
    }

    void ReplyOperation(Ydb::Operations::Operation& operation)
    {
        Request_->SendOperation(operation);
        NWilson::EndSpanWithStatus(Span_, operation.status());
        this->PassAway();
    }

    void SetCost(ui64 ru) {
        Request_->SetRuHeader(ru);
        if (TBase::ReportCostInfo_) {
            Request_->SetCostInfo(ru);
        }
    }

protected:
    void HandleWakeup(TEvents::TEvWakeup::TPtr &ev, const TActorContext &ctx) {
        switch (ev->Get()->Tag) {
            case TBase::WakeupTagTimeout:
                static_cast<TDerived*>(this)->OnOperationTimeout(ctx);
                break;
            case TBase::WakeupTagCancel:
                static_cast<TDerived*>(this)->OnCancelOperation(ctx);
                break;
            default:
                break;
        }
    }

    void HandleForget(TRpcServices::TEvForgetOperation::TPtr&, const TActorContext &ctx) {
        static_cast<TDerived*>(this)->OnForgetOperation(ctx);
    }
private:
    void HandleSubscribeiGrpcCancel(TEvSubscribeGrpcCancel::TPtr& ev) {
        auto as = TActivationContext::ActorSystem();
        PassSubscription(ev->Get(), Request_.get(), as);
    }

protected:
    NWilson::TSpan Span_;
};

template <typename TDerived, typename TRequest>
class TEtcdRequestGrpc
    : public TEtcdOperationRequestActor<TDerived, TRequest>
    , public TBaseEtcdRequest<TEtcdRequestGrpc<TDerived, TRequest>>
{
public:
    using TBase = TEtcdOperationRequestActor<TDerived, TRequest>;
    using TBase::TBase;

    friend class TBaseEtcdRequest<TEtcdRequestGrpc<TDerived, TRequest>>;

    void Bootstrap(const TActorContext& ctx) {
        TBase::Bootstrap(ctx);
        this->ParseGrpcRequest();
        this->Become(&TEtcdRequestGrpc::StateFunc);
        SendDatabaseRequest();
    }
private:
    void SendDatabaseRequest() {
        TStringBuilder sql;
        NYdb::TParamsBuilder params;
        this->MakeQueryWithParams(sql, params);
        Cerr << Endl << sql << Endl;
        const auto my = this->SelfId();
        const auto ass = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
        NEtcd::AppData()->Client->ExecuteQuery(sql, NYdb::NQuery::TTxControl::NoTx(), params.Build()).Subscribe([my, ass](const auto& future) {
            if (const auto res = future.GetValueSync(); res.IsSuccess())
                ass->Send(my, new NEtcd::TEvQueryResult(res.GetResultSets()));
            else
                ass->Send(my, new NEtcd::TEvQueryError(res.GetIssues()));
        });
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NEtcd::TEvQueryResult, Handle);
            hFunc(NEtcd::TEvQueryError, Handle);
        default:
            return TBase::StateFuncBase(ev);
        }
    }

    void Handle(NEtcd::TEvQueryResult::TPtr &ev) {
        this->ReplyWith(ev->Get()->Results);
    }

    void Handle(NEtcd::TEvQueryError::TPtr &ev) {
        Cerr << __func__ << ' ' << ev->Get()->Issues.ToString() << Endl;
    }
};

class TRangeRequest
    : public TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TRangeRequest, TEvRangeKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = NEtcd::AppData()->Revision.load();
        return Range.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params) final {
        size_t resultsCounter = 0U, paramsCounter = 0U;
        return Range.MakeQueryWithParams(sql, params, resultsCounter, paramsCounter);
    }

    void ReplyWith(const NYdb::TResultSets& results) final {
        auto response = Range.MakeResponse(results);
        const auto header = response.mutable_header();
        header->set_revision(Revision);
        header->set_cluster_id(0ULL);
        header->set_member_id(0ULL);
        header->set_raft_term(0ULL);
        return this->Reply(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
    }

    TRange Range;
};

class TPutRequest
    : public TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TPutRequest, TEvPutKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = NEtcd::AppData()->Revision.fetch_add(1L);
        return Put.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params) final {
        params.AddParam("$Revision").Int64(Revision).Build();
        size_t resultsCounter = 0U, paramsCounter = 0U;
        return Put.MakeQueryWithParams(sql, params, resultsCounter, paramsCounter);
    }

    void ReplyWith(const NYdb::TResultSets& results) final {
        auto response = Put.MakeResponse(results);
        const auto header = response.mutable_header();
        header->set_revision(Revision);
        header->set_cluster_id(0ULL);
        header->set_member_id(0ULL);
        header->set_raft_term(0ULL);
        return this->Reply(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
    }

    TPut Put;
};

class TDeleteRangeRequest
    : public TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TDeleteRangeRequest, TEvDeleteRangeKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = NEtcd::AppData()->Revision.fetch_add(1L);
        return DeleteRange.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params) final {
        size_t resultsCounter = 0U, paramsCounter = 0U;
        return DeleteRange.MakeQueryWithParams(sql, params, resultsCounter, paramsCounter);
    }

    void ReplyWith(const NYdb::TResultSets& results) final {
        auto response = DeleteRange.MakeResponse(results);
        const auto header = response.mutable_header();
        header->set_revision(Revision);
        header->set_cluster_id(0ULL);
        header->set_member_id(0ULL);
        header->set_raft_term(0ULL);
        return this->Reply(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
    }

    TDeleteRange DeleteRange;
};

class TTxnRequest
    : public TEtcdRequestGrpc<TTxnRequest, TEvTxnKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TTxnRequest, TEvTxnKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = NEtcd::AppData()->Revision.fetch_add(1L);
        return Txn.Parse(*GetProtoRequest());
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params) final {
        params.AddParam("$Revision").Int64(Revision).Build();
        size_t resultsCounter = 0U, paramsCounter = 0U;
        return Txn.MakeQueryWithParams(sql, params, resultsCounter, paramsCounter);
    }

    void ReplyWith(const NYdb::TResultSets& results) final {
        auto response = Txn.MakeResponse(results);
        const auto header = response.mutable_header();
        header->set_revision(Revision);
        header->set_cluster_id(0ULL);
        header->set_member_id(0ULL);
        header->set_raft_term(0ULL);
        return this->Reply(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
    }

    TTxn Txn;
};

class TCompactRequest
    : public TEtcdRequestGrpc<TCompactRequest, TEvCompactKVRequest> {
public:
    using TBase = TEtcdRequestGrpc<TCompactRequest, TEvCompactKVRequest>;
    using TBase::TBase;
private:
    bool ParseGrpcRequest() final {
        Revision = NEtcd::AppData()->Revision.load();

        const auto &rec = *GetProtoRequest();
        KeyRevision = rec.revision();
        return KeyRevision > 0 && KeyRevision < Revision;
    }

    void MakeQueryWithParams(TStringBuilder& sql, NYdb::TParamsBuilder& params) final {
        sql << "delete from `verhaal` where `modified` < $Revision;" << Endl;
        params.AddParam("$Revision").Int64(KeyRevision).Build();
    }

    void ReplyWith(const NYdb::TResultSets&) final {
        etcdserverpb::CompactionResponse response;
        const auto header = response.mutable_header();
        header->set_revision(Revision);
        header->set_cluster_id(0ULL);
        header->set_member_id(0ULL);
        header->set_raft_term(0ULL);
        this->Reply(Ydb::StatusIds::SUCCESS, response, TActivationContext::AsActorContext());
    }

    i64 KeyRevision;
};

}

NActors::IActor* MakeRange(IRequestOpCtx* p) {
    return new TRangeRequest(p);
}

NActors::IActor* MakePut(IRequestOpCtx* p) {
    return new TPutRequest(p);
}

NActors::IActor* MakeDeleteRange(IRequestOpCtx* p) {
    return new TDeleteRangeRequest(p);
}

NActors::IActor* MakeTxn(IRequestOpCtx* p) {
    return new TTxnRequest(p);
}

NActors::IActor* MakeCompact(IRequestOpCtx* p) {
    return new TCompactRequest(p);
}

} // namespace NKikimr::NGRpcService
