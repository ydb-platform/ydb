#define INCLUDE_YDB_INTERNAL_H
#include "exec_query.h"

#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

namespace NYdb::NQuery {

using namespace NThreading;

static void SetTxSettings(const TTxSettings& txSettings, Ydb::Query::TransactionSettings* proto) {
    switch (txSettings.GetMode()) {
        case TTxSettings::TS_SERIALIZABLE_RW:
            proto->mutable_serializable_read_write();
            break;
        case TTxSettings::TS_ONLINE_RO:
            proto->mutable_online_read_only()->set_allow_inconsistent_reads(
                txSettings.OnlineSettings_.AllowInconsistentReads_);
            break;
        case TTxSettings::TS_STALE_RO:
            proto->mutable_stale_read_only();
            break;
        case TTxSettings::TS_SNAPSHOT_RO:
            proto->mutable_snapshot_read_only();
            break;
        default:
            throw TContractViolation("Unexpected transaction mode.");
    }
}

class TExecuteQueryIterator::TReaderImpl {
public:
    using TSelf = TExecuteQueryIterator::TReaderImpl;
    using TResponse = Ydb::Query::ExecuteQueryResponsePart;
    using TStreamProcessorPtr = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NYdbGrpc::TGrpcStatus;
    using TBatchReadResult = std::pair<TResponse, TGRpcStatus>;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint, const TMaybe<TSession>& session)
        : StreamProcessor_(streamProcessor)
        , Finished_(false)
        , Endpoint_(endpoint)
        , Session_(session)
    {}

    ~TReaderImpl() {
        StreamProcessor_->Cancel();
    }

    bool IsFinished() const {
        return Finished_;
    }

    TAsyncExecuteQueryPart ReadNext(std::shared_ptr<TSelf> self) {
        auto promise = NThreading::NewPromise<TExecuteQueryPart>();
        // Capture self - guarantee no dtor call during the read
        auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
            if (!grpcStatus.Ok()) {
                self->Finished_ = true;
                promise.SetValue({TStatus(TPlainStatus(grpcStatus, self->Endpoint_)), {}, {}});
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(self->Response_.issues(), issues);
                EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
                TPlainStatus plainStatus{clientStatus, std::move(issues), self->Endpoint_, {}};
                TStatus status{std::move(plainStatus)};

                TMaybe<TExecStats> stats;
                TMaybe<TTransaction> tx;
                if (self->Response_.has_exec_stats()) {
                    stats = TExecStats(std::move(*self->Response_.mutable_exec_stats()));
                }

                if (self->Response_.has_tx_meta() && self->Response_.tx_meta().id() && self->Session_.Defined()) {
                    tx = TTransaction(self->Session_.GetRef(), self->Response_.tx_meta().id());
                }

                if (self->Response_.has_result_set()) {
                    promise.SetValue({
                        std::move(status),
                        TResultSet(std::move(*self->Response_.mutable_result_set())),
                        self->Response_.result_set_index(),
                        std::move(stats),
                        std::move(tx)
                    });
                } else {
                    promise.SetValue({std::move(status), std::move(stats), std::move(tx)});
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
    TString Endpoint_;
    TMaybe<TSession> Session_;
};

TAsyncExecuteQueryPart TExecuteQueryIterator::ReadNext() {
    if (ReaderImpl_->IsFinished()) {
        RaiseError("Attempt to perform read on invalid or finished stream");
    }

    return ReaderImpl_->ReadNext(ReaderImpl_);
}

using TExecuteQueryProcessorPtr = TExecuteQueryIterator::TReaderImpl::TStreamProcessorPtr;

struct TExecuteQueryBuffer : public TThrRefBase, TNonCopyable {
    using TPtr = TIntrusivePtr<TExecuteQueryBuffer>;

    TExecuteQueryBuffer(TExecuteQueryIterator&& iterator)
        : Promise_(NewPromise<TExecuteQueryResult>())
        , Iterator_(std::move(iterator)) {}

    TPromise<TExecuteQueryResult> Promise_;
    TExecuteQueryIterator Iterator_;
    TVector<NYql::TIssue> Issues_;
    TVector<Ydb::ResultSet> ResultSets_;
    TMaybe<TExecStats> Stats_;
    TMaybe<TTransaction> Tx_;

    void Next() {
        TPtr self(this);

        Iterator_.ReadNext().Subscribe([self](TAsyncExecuteQueryPart partFuture) mutable {
            auto part = partFuture.ExtractValue();

            if (const auto& st = part.GetStats()) {
                self->Stats_ = st;
            }

            if (!part.IsSuccess()) {
                TMaybe<TExecStats> stats;
                std::swap(self->Stats_, stats);

                if (part.EOS()) {
                    TVector<NYql::TIssue> issues;
                    TVector<Ydb::ResultSet> resultProtos;
                    TMaybe<TTransaction> tx;

                    std::swap(self->Issues_, issues);
                    std::swap(self->ResultSets_, resultProtos);
                    std::swap(self->Tx_, tx);

                    TVector<TResultSet> resultSets;
                    for (auto& proto : resultProtos) {
                        resultSets.emplace_back(std::move(proto));
                    }

                    self->Promise_.SetValue(TExecuteQueryResult(
                        TStatus(EStatus::SUCCESS, NYql::TIssues(std::move(issues))),
                        std::move(resultSets),
                        std::move(stats),
                        std::move(tx)
                    ));
                } else {
                    self->Promise_.SetValue(TExecuteQueryResult(std::move(part), {}, std::move(stats), {}));
                }

                return;
            }

            self->Issues_.insert(self->Issues_.end(), part.GetIssues().begin(), part.GetIssues().end());

            if (part.HasResultSet()) {
                auto inRs = part.ExtractResultSet();
                auto& inRsProto = TProtoAccessor::GetProto(inRs);

                // TODO: Use result sets metadata
                if (self->ResultSets_.size() <= part.GetResultSetIndex()) {
                    self->ResultSets_.resize(part.GetResultSetIndex() + 1);
                }

                auto& resultSet = self->ResultSets_[part.GetResultSetIndex()];
                if (resultSet.columns().empty()) {
                    resultSet.mutable_columns()->CopyFrom(inRsProto.columns());
                }

                resultSet.mutable_rows()->Add(inRsProto.rows().begin(), inRsProto.rows().end());
            }

            if (const auto& tx = part.GetTransaction()) {
                self->Tx_ = tx;
            }

            self->Next();
        });
    }
};

TFuture<std::pair<TPlainStatus, TExecuteQueryProcessorPtr>> StreamExecuteQueryImpl(
    const std::shared_ptr<TGRpcConnectionsImpl>& connections, const TDbDriverStatePtr& driverState,
    const TString& query, const TTxControl& txControl, const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
    const TExecuteQuerySettings& settings, const TMaybe<TSession>& session)
{
    auto request = MakeRequest<Ydb::Query::ExecuteQueryRequest>();
    request.set_exec_mode(::Ydb::Query::ExecMode(settings.ExecMode_));
    request.set_stats_mode(::Ydb::Query::StatsMode(settings.StatsMode_));
    request.mutable_query_content()->set_text(query);
    request.mutable_query_content()->set_syntax(::Ydb::Query::Syntax(settings.Syntax_));
    if (session.Defined()) {
        request.set_session_id(session->GetId());
    } else if ((txControl.TxSettings_.Defined() && !txControl.CommitTx_) || txControl.TxId_.Defined()) {
        throw TContractViolation("Interactive tx must use explisit session");
    }

    if (settings.ConcurrentResultSets_) {
        request.set_concurrent_result_sets(*settings.ConcurrentResultSets_);
    }

    if (settings.OutputChunkMaxSize_) {
        request.set_response_part_limit_bytes(*settings.OutputChunkMaxSize_);
    }

    if (txControl.HasTx()) {
        auto requestTxControl = request.mutable_tx_control();
        requestTxControl->set_commit_tx(txControl.CommitTx_);
        if (txControl.TxId_) {
            requestTxControl->set_tx_id(*txControl.TxId_);
        } else {
            Y_ASSERT(txControl.TxSettings_);
            SetTxSettings(*txControl.TxSettings_, requestTxControl->mutable_begin_tx());
        }
    } else {
        Y_ASSERT(!txControl.CommitTx_);
    }

    if (params) {
        *request.mutable_parameters() = *params;
    }

    auto promise = NewPromise<std::pair<TPlainStatus, TExecuteQueryProcessorPtr>>();

    auto rpcSettings = TRpcRequestSettings::Make(settings);
    if (session.Defined()) {
        rpcSettings.PreferredEndpoint = TEndpointKey(GetNodeIdFromSession(session->GetId()));
    }

    connections->StartReadStream<
        Ydb::Query::V1::QueryService,
        Ydb::Query::ExecuteQueryRequest,
        Ydb::Query::ExecuteQueryResponsePart>
    (
        std::move(request),
        [promise] (TPlainStatus status, TExecuteQueryProcessorPtr processor) mutable {
            promise.SetValue(std::make_pair(status, processor));
        },
        &Ydb::Query::V1::QueryService::Stub::AsyncExecuteQuery,
        driverState,
        rpcSettings
    );

    return promise.GetFuture();
}

TAsyncExecuteQueryIterator TExecQueryImpl::StreamExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
    const TDbDriverStatePtr& driverState, const TString& query, const TTxControl& txControl,
    const TMaybe<TParams>& params, const TExecuteQuerySettings& settings, const TMaybe<TSession>& session)
{
    auto promise = NewPromise<TExecuteQueryIterator>();

    auto iteratorCallback = [promise, session](TFuture<std::pair<TPlainStatus, TExecuteQueryProcessorPtr>> future) mutable {
        Y_ASSERT(future.HasValue());
        auto pair = future.ExtractValue();
        promise.SetValue(TExecuteQueryIterator(
            pair.second
                ? std::make_shared<TExecuteQueryIterator::TReaderImpl>(pair.second, pair.first.Endpoint, session)
                : nullptr,
            std::move(pair.first))
        );
    };

    auto paramsProto = params
        ? &params->GetProtoMap()
        : nullptr;

    StreamExecuteQueryImpl(connections, driverState, query, txControl, paramsProto, settings, session)
        .Subscribe(iteratorCallback);
    return promise.GetFuture();
}

TAsyncExecuteQueryResult TExecQueryImpl::ExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
    const TDbDriverStatePtr& driverState, const TString& query, const TTxControl& txControl,
    const TMaybe<TParams>& params, const TExecuteQuerySettings& settings, const TMaybe<TSession>& session)
{
    auto syncSettings = settings;
    syncSettings.ConcurrentResultSets(true);

    return StreamExecuteQuery(connections, driverState, query, txControl, params, syncSettings, session)
        .Apply([](TAsyncExecuteQueryIterator itFuture){
            auto it = itFuture.ExtractValue();

            if (!it.IsSuccess()) {
                return MakeFuture<TExecuteQueryResult>(std::move(it));
            }

            auto buffer = MakeIntrusive<TExecuteQueryBuffer>(std::move(it));
            buffer->Next();

            return buffer->Promise_.GetFuture();
        });
}

} // namespace NYdb::NQuery
