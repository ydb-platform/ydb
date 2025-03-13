#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/draft/ydb_scripting.h>

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/table_helpers/helpers.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_scripting.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>

namespace NYdb::inline Dev {
namespace NScripting {

using namespace NThreading;

TExecuteYqlResult::TExecuteYqlResult(TStatus&& status, std::vector<TResultSet>&& resultSets,
    const std::optional<NTable::TQueryStats>& queryStats)
    : TStatus(std::move(status))
    , ResultSets_(std::move(resultSets))
    , QueryStats_(queryStats) {}

const std::vector<TResultSet>& TExecuteYqlResult::GetResultSets() const {
    return ResultSets_;
}

TResultSet TExecuteYqlResult::GetResultSet(size_t resultIndex) const {
    if (resultIndex >= ResultSets_.size()) {
        RaiseError(std::string("Requested index out of range\n"));
    }

    return ResultSets_[resultIndex];
}

TResultSetParser TExecuteYqlResult::GetResultSetParser(size_t resultIndex) const {
    return TResultSetParser(GetResultSet(resultIndex));
}

const std::optional<NTable::TQueryStats>& TExecuteYqlResult::GetStats() const {
    return QueryStats_;
}

////////////////////////////////////////////////////////////////////////////////

class TYqlResultPartIterator::TReaderImpl {
public:
    using TSelf = TYqlResultPartIterator::TReaderImpl;
    using TResponse = Ydb::Scripting::ExecuteYqlPartialResponse;
    using TStreamProcessorPtr = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NYdbGrpc::TGrpcStatus;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const std::string& endpoint)
        : StreamProcessor_(streamProcessor)
        , Finished_(false)
        , Endpoint_(endpoint)
    {}

    ~TReaderImpl() {
        StreamProcessor_->Cancel();
    }

    bool IsFinished() const {
        return Finished_;
    }

    TAsyncYqlResultPart ReadNext(std::shared_ptr<TSelf> self) {
        auto promise = NThreading::NewPromise<TYqlResultPart>();
        // Capture self - guarantee no dtor call during the read
        auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
            if (!grpcStatus.Ok()) {
                self->Finished_ = true;
                promise.SetValue({ TStatus(TPlainStatus(grpcStatus, self->Endpoint_)) });
            } else {
                NYdb::NIssue::TIssues issues;
                NYdb::NIssue::IssuesFromMessage(self->Response_.issues(), issues);
                EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
                TPlainStatus plainStatus{ clientStatus, std::move(issues), self->Endpoint_, {} };
                TStatus status{ std::move(plainStatus) };
                std::optional<NTable::TQueryStats> queryStats;

                if (self->Response_.result().has_query_stats()) {
                    queryStats = NTable::TQueryStats(self->Response_.result().query_stats());
                }
                if (self->Response_.result().has_result_set()) {
                    promise.SetValue(
                        {
                            std::move(status),
                            TYqlPartialResult(
                                self->Response_.result().result_set_index(),
                                TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set()))
                            ),
                            queryStats
                        }
                    );
                } else {
                    promise.SetValue({ std::move(status), queryStats });
                }
            }
        };
        StreamProcessor_->Read(&Response_, readCb);
        return promise.GetFuture();
    }
private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    std::string Endpoint_;
};

TYqlResultPartIterator::TYqlResultPartIterator(
    std::shared_ptr<TReaderImpl> impl,
    TPlainStatus&& status)
    : TStatus(std::move(status))
    , ReaderImpl_(impl)
{}

TAsyncYqlResultPart TYqlResultPartIterator::ReadNext() {
    if (ReaderImpl_->IsFinished())
        RaiseError("Attempt to perform read on invalid or finished stream");
    return ReaderImpl_->ReadNext(ReaderImpl_);
}

////////////////////////////////////////////////////////////////////////////////

TExplainYqlResult::TExplainYqlResult(TStatus&& status, const ::google::protobuf::Map<TStringType, Ydb::Type>&& types, std::string&& plan)
    : TStatus(std::move(status))
    , ParameterTypes_(std::move(types))
    , Plan_(plan) {}

std::map<std::string, TType> TExplainYqlResult::GetParameterTypes() const {
    std::map<std::string, TType> typesMap;
    for (const auto& param : ParameterTypes_) {
        typesMap.emplace(param.first, TType(param.second));
    }
    return typesMap;
}

const std::string& TExplainYqlResult::GetPlan() const {
    return Plan_;
}

////////////////////////////////////////////////////////////////////////////////

class TScriptingClient::TImpl : public TClientImplCommon<TScriptingClient::TImpl> {
public:
    using TYqlScriptProcessorPtr = TYqlResultPartIterator::TReaderImpl::TStreamProcessorPtr;

    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings) {}

    template<typename TParamsType>
    TAsyncExecuteYqlResult ExecuteYqlScript(const std::string& script, TParamsType params,
        const TExecuteYqlRequestSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Scripting::ExecuteYqlRequest>(settings);
        request.set_script(TStringType{script});
        SetParams(params, &request);
        request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));
        request.set_syntax(settings.Syntax_);

        auto promise = NewPromise<TExecuteYqlResult>();

        auto extractor = [promise]
            (google::protobuf::Any* any, TPlainStatus status) mutable {
                std::vector<TResultSet> res;
                std::optional<NTable::TQueryStats> queryStats;
                if (any) {
                    Ydb::Scripting::ExecuteYqlResult result;
                    any->UnpackTo(&result);

                    for (size_t i = 0; i < static_cast<size_t>(result.result_sets_size()); i++) {
                        res.push_back(TResultSet(*result.mutable_result_sets(i)));
                    }

                    if (result.has_query_stats()) {
                        queryStats = NTable::TQueryStats(result.query_stats());
                    }
                }

                TExecuteYqlResult executeResult(TStatus(std::move(status)), std::move(res),
                    queryStats);
                promise.SetValue(std::move(executeResult));
            };

        Connections_->RunDeferred<Ydb::Scripting::V1::ScriptingService, Ydb::Scripting::ExecuteYqlRequest,
            Ydb::Scripting::ExecuteYqlResponse>(
                std::move(request),
                extractor,
                &Ydb::Scripting::V1::ScriptingService::Stub::AsyncExecuteYql,
                DbDriverState_,
                INITIAL_DEFERRED_CALL_DELAY,
                TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    template<typename TParamsType>
    TFuture<std::pair<TPlainStatus, TYqlScriptProcessorPtr>> StreamExecuteYqlScriptInternal(const std::string& script,
        TParamsType params, const TExecuteYqlRequestSettings& settings)
    {
        auto request = MakeOperationRequest<Ydb::Scripting::ExecuteYqlRequest>(settings);
        request.set_script(TStringType{script});
        SetParams(params, &request);
        request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));
        request.set_syntax(settings.Syntax_);

        auto promise = NewPromise<std::pair<TPlainStatus, TYqlScriptProcessorPtr>>();

        Connections_->StartReadStream<
            Ydb::Scripting::V1::ScriptingService,
            Ydb::Scripting::ExecuteYqlRequest,
            Ydb::Scripting::ExecuteYqlPartialResponse>
        (
            std::move(request),
            [promise](TPlainStatus status, TYqlScriptProcessorPtr processor) mutable {
                promise.SetValue(std::make_pair(status, processor));
            },
            &Ydb::Scripting::V1::ScriptingService::Stub::AsyncStreamExecuteYql,
            DbDriverState_,
            TRpcRequestSettings::Make(settings)
        );

        return promise.GetFuture();
    }

    template<typename TParamsType>
    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const std::string& query, TParamsType params,
        const TExecuteYqlRequestSettings& settings)
    {
        auto promise = NewPromise<TYqlResultPartIterator>();

        auto iteratorCallback = [promise](TFuture<std::pair<TPlainStatus,
            TScriptingClient::TImpl::TYqlScriptProcessorPtr>> future) mutable
        {
            Y_ASSERT(future.HasValue());
            auto pair = future.ExtractValue();
            promise.SetValue(TYqlResultPartIterator(
                pair.second
                ? std::make_shared<TYqlResultPartIterator::TReaderImpl>(pair.second, pair.first.Endpoint)
                : nullptr,
                std::move(pair.first)));
        };

        StreamExecuteYqlScriptInternal(query, params, settings).Subscribe(iteratorCallback);
        return promise.GetFuture();
    }

    TAsyncExplainYqlResult ExplainYqlScript(const std::string& script,
        const TExplainYqlRequestSettings& settings)
    {
            auto request = MakeOperationRequest<Ydb::Scripting::ExplainYqlRequest>(settings);
            request.set_script(TStringType{script});

            switch (settings.Mode_) {
                // KIKIMR-10990
                //case ExplainYqlRequestMode::Parse:
                //    request.set_mode(::Ydb::Scripting::ExplainYqlRequest_Mode::ExplainYqlRequest_Mode_PARSE);
                //    break;
                case ExplainYqlRequestMode::Validate:
                    request.set_mode(::Ydb::Scripting::ExplainYqlRequest_Mode::ExplainYqlRequest_Mode_VALIDATE);
                    break;
                case ExplainYqlRequestMode::Plan:
                    request.set_mode(::Ydb::Scripting::ExplainYqlRequest_Mode::ExplainYqlRequest_Mode_PLAN);
                    break;
            }

            auto promise = NewPromise<TExplainYqlResult>();

            auto extractor = [promise]
                (google::protobuf::Any* any, TPlainStatus status) mutable {
                    std::string plan;
                    ::google::protobuf::Map<TStringType, Ydb::Type> types;
                    if (any) {
                        Ydb::Scripting::ExplainYqlResult result;
                        any->UnpackTo(&result);

                        plan = result.plan();
                        types = result.parameters_types();
                    }

                    TExplainYqlResult explainResult(TStatus(std::move(status)),
                        std::move(types), std::move(plan));
                    promise.SetValue(std::move(explainResult));
                };

            Connections_->RunDeferred<Ydb::Scripting::V1::ScriptingService, Ydb::Scripting::ExplainYqlRequest,
                Ydb::Scripting::ExplainYqlResponse>(
                    std::move(request),
                    extractor,
                    &Ydb::Scripting::V1::ScriptingService::Stub::AsyncExplainYql,
                    DbDriverState_,
                    INITIAL_DEFERRED_CALL_DELAY,
                    TRpcRequestSettings::Make(settings));

            return promise.GetFuture();
    }

private:
    template<typename TRequest>
    static void SetParams(::google::protobuf::Map<TStringType, Ydb::TypedValue>* params, TRequest* request) {
        if (params) {
            request->mutable_parameters()->swap(*params);
        }
    }

    template<typename TRequest>
    static void SetParams(const ::google::protobuf::Map<TStringType, Ydb::TypedValue>& params, TRequest* request) {
        *request->mutable_parameters() = params;
    }

};

TScriptingClient::TScriptingClient(const TDriver& driver, const TCommonClientSettings &settings)
    : Impl_(new TImpl(CreateInternalInterface(driver), settings))
{}

TParamsBuilder TScriptingClient::GetParamsBuilder() {
    return TParamsBuilder();
}

TAsyncExecuteYqlResult TScriptingClient::ExecuteYqlScript(const std::string &query, NYdb::TParams&& params,
    const TExecuteYqlRequestSettings &settings)
{
    auto paramsPtr = params.Empty() ? nullptr : params.GetProtoMapPtr();
    return Impl_->ExecuteYqlScript(query, paramsPtr, settings);
}

TAsyncExecuteYqlResult TScriptingClient::ExecuteYqlScript(const std::string &query, const NYdb::TParams& params,
    const TExecuteYqlRequestSettings &settings)
{
    if (params.Empty()) {
        return Impl_->ExecuteYqlScript(
            query,
            nullptr,
            settings);
    } else {
        using TProtoParamsType = const ::google::protobuf::Map<TStringType, Ydb::TypedValue>;
        return Impl_->ExecuteYqlScript<TProtoParamsType&>(
            query,
            params.GetProtoMap(),
            settings);
    }
}

TAsyncExecuteYqlResult TScriptingClient::ExecuteYqlScript(const std::string &script,
    const TExecuteYqlRequestSettings &settings)
{
    return Impl_->ExecuteYqlScript(script, nullptr, settings);
}

TAsyncYqlResultPartIterator TScriptingClient::StreamExecuteYqlScript(const std::string& script,
    const TExecuteYqlRequestSettings& settings)
{
    return Impl_->StreamExecuteYqlScript(script, nullptr, settings);
}

TAsyncYqlResultPartIterator TScriptingClient::StreamExecuteYqlScript(const std::string& script, const TParams& params,
    const TExecuteYqlRequestSettings& settings)
{
    if (params.Empty()) {
        return Impl_->StreamExecuteYqlScript(script, nullptr, settings);
    } else {
        using TProtoParamsType = const ::google::protobuf::Map<TStringType, Ydb::TypedValue>;
        return Impl_->StreamExecuteYqlScript<TProtoParamsType&>(script, params.GetProtoMap(), settings);
    }
}

TAsyncYqlResultPartIterator TScriptingClient::StreamExecuteYqlScript(const std::string& script, TParams&& params,
    const TExecuteYqlRequestSettings& settings)
{
    auto paramsPtr = params.Empty() ? nullptr : params.GetProtoMapPtr();
    return Impl_->StreamExecuteYqlScript(script, paramsPtr, settings);
}

TAsyncExplainYqlResult TScriptingClient::ExplainYqlScript(const std::string& script,
    const TExplainYqlRequestSettings& settings)
{
    return Impl_->ExplainYqlScript(script, settings);
}

} // namespace NScheme
} // namespace NYdb
