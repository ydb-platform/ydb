#define INCLUDE_YDB_INTERNAL_H
#include "exec_query.h"
#include "client_session.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/kqp_session_common/kqp_session_common.h>
#include <ydb/public/sdk/cpp/src/client/impl/ydb_internal/session_pool/session_pool.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

#include <library/cpp/threading/future/core/coroutine_traits.h>

namespace NYdb::inline Dev::NQuery {

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
        case TTxSettings::TS_SNAPSHOT_RW:
            proto->mutable_snapshot_read_write();
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

    TReaderImpl(TStreamProcessorPtr streamProcessor, const std::string& endpoint, const std::optional<TSession>& session)
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

    TAsyncExecuteQueryPart DoReadNext(std::shared_ptr<TSelf> self) {
        auto promise = NThreading::NewPromise<TExecuteQueryPart>();
        // Capture self - guarantee no dtor call during the read
        auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
            if (!grpcStatus.Ok()) {
                self->Finished_ = true;
                promise.SetValue({TStatus(TPlainStatus(grpcStatus, self->Endpoint_)), {}, {}});
            } else {
                NYdb::NIssue::TIssues issues;
                NYdb::NIssue::IssuesFromMessage(self->Response_.issues(), issues);
                EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
                TPlainStatus plainStatus{clientStatus, std::move(issues), self->Endpoint_, {}};
                TStatus status{std::move(plainStatus)};

                std::optional<TExecStats> stats;
                std::optional<TTransaction> tx;
                if (self->Response_.has_exec_stats()) {
                    stats = TExecStats(std::move(*self->Response_.mutable_exec_stats()));
                }

                if (self->Response_.has_tx_meta() && !self->Response_.tx_meta().id().empty() && self->Session_.has_value()) {
                    tx = TTransaction(self->Session_.value(), self->Response_.tx_meta().id());
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

    TAsyncExecuteQueryPart ReadNext(std::shared_ptr<TSelf> self) {
        if (!Session_)
            return DoReadNext(std::move(self));

        return NSessionPool::InjectSessionStatusInterception(
            Session_->SessionImpl_,
            DoReadNext(std::move(self)),
            false, // no need to ping stream session
            TDuration::Zero());
    }

private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    std::string Endpoint_;
    std::optional<TSession> Session_;
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
    std::vector<NYdb::NIssue::TIssue> Issues_;
    std::vector<Ydb::ResultSet> ResultSets_;
    std::optional<TExecStats> Stats_;
    std::optional<TTransaction> Tx_;
    std::vector<std::string> ArrowSchemas_;
    std::vector<std::vector<std::string>> BytesData_;

    void Next() {
        TPtr self(this);

        Iterator_.ReadNext().Subscribe([self](TAsyncExecuteQueryPart partFuture) mutable {
            auto part = partFuture.ExtractValue();

            if (const auto& st = part.GetStats()) {
                self->Stats_ = st;
            }

            if (!part.IsSuccess()) {
                std::optional<TExecStats> stats;
                std::swap(self->Stats_, stats);

                if (part.EOS()) {
                    std::vector<NYdb::NIssue::TIssue> issues;
                    std::vector<Ydb::ResultSet> resultProtos;
                    std::optional<TTransaction> tx;
                    std::vector<std::string> arrowSchemas;
                    std::vector<std::vector<std::string>> bytesData;

                    std::swap(self->Issues_, issues);
                    std::swap(self->ResultSets_, resultProtos);
                    std::swap(self->Tx_, tx);
                    std::swap(self->ArrowSchemas_, arrowSchemas);
                    std::swap(self->BytesData_, bytesData);

                    std::vector<TResultSet> resultSets;
                    for (size_t i = 0; i < resultProtos.size(); ++i) {
                        resultSets.emplace_back(std::move(resultProtos[i]), std::move(arrowSchemas[i]), std::move(bytesData[i]));
                    }

                    self->Promise_.SetValue(TExecuteQueryResult(
                        TStatus(EStatus::SUCCESS, NYdb::NIssue::TIssues(std::move(issues))),
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
                    self->ArrowSchemas_.resize(part.GetResultSetIndex() + 1);
                    self->BytesData_.resize(part.GetResultSetIndex() + 1);
                }

                auto& resultSet = self->ResultSets_[part.GetResultSetIndex()];
                if (resultSet.columns().empty()) {
                    resultSet.mutable_columns()->CopyFrom(inRsProto.columns());
                }

                resultSet.set_format(inRsProto.format());

                switch (resultSet.format()) {
                    case Ydb::ResultSet::FORMAT_VALUE: {
                        resultSet.mutable_rows()->Reserve(resultSet.mutable_rows()->size() + inRsProto.rows_size());
                        for (const auto& row : inRsProto.rows()) {
                            *resultSet.mutable_rows()->Add() = row;
                        }
                        break;
                    }
                    case Ydb::ResultSet::FORMAT_ARROW: {
                        if (self->ArrowSchemas_.size() <= part.GetResultSetIndex()) {
                            self->ArrowSchemas_.resize(part.GetResultSetIndex() + 1);
                            self->BytesData_.resize(part.GetResultSetIndex() + 1);
                        }

                        auto& arrowSchema = self->ArrowSchemas_[part.GetResultSetIndex()];
                        auto& bytesData = self->BytesData_[part.GetResultSetIndex()];

                        auto& mutableProto = inRs.MutableProto();
                        if (arrowSchema.empty()) {
                            arrowSchema = std::move(*mutableProto.mutable_arrow_format_meta()->mutable_schema());
                        }

                        if (auto* data = mutableProto.mutable_data(); data && !data->empty()) {
                            bytesData.emplace_back(std::move(*data));
                        }
                        break;
                    }
                    default:
                        break;
                }
            }

            if (const auto& tx = part.GetTransaction()) {
                self->Tx_ = tx;
            }

            self->Next();
        });
    }
};

class TExecQueryInternal {
public:
    static TFuture<std::pair<TPlainStatus, TExecuteQueryProcessorPtr>> ExecuteQueryCommon(
        const std::shared_ptr<TGRpcConnectionsImpl>& connections, const TDbDriverStatePtr& driverState,
        const std::string& query, const TTxControl& txControl, const ::google::protobuf::Map<TStringType, Ydb::TypedValue>* params,
        const TExecuteQuerySettings& settings, const std::optional<TSession>& session)
    {
        auto request = MakeRequest<Ydb::Query::ExecuteQueryRequest>();
        request.set_exec_mode(::Ydb::Query::ExecMode(settings.ExecMode_));
        request.set_stats_mode(::Ydb::Query::StatsMode(settings.StatsMode_));
        request.set_pool_id(TStringType{settings.ResourcePool_});
        request.mutable_query_content()->set_text(TStringType{query});
        request.mutable_query_content()->set_syntax(::Ydb::Query::Syntax(settings.Syntax_));
        request.set_schema_inclusion_mode(::Ydb::Query::SchemaInclusionMode(settings.SchemaInclusionMode_));
        request.set_result_set_format(::Ydb::ResultSet::Format(settings.Format_));
        if (session.has_value()) {
            request.set_session_id(TStringType{session->GetId()});
        } else if ((std::holds_alternative<TTxSettings>(txControl.Tx_) && !txControl.CommitTx_) ||
                    std::holds_alternative<TTransaction>(txControl.Tx_) ||
                    std::holds_alternative<std::string>(txControl.Tx_)) {
            throw TContractViolation("Interactive tx must use explisit session");
        }

        if (settings.ConcurrentResultSets_) {
            request.set_concurrent_result_sets(*settings.ConcurrentResultSets_);
        }

        if (settings.OutputChunkMaxSize_) {
            request.set_response_part_limit_bytes(*settings.OutputChunkMaxSize_);
        }

        if (settings.StatsCollectPeriod_) {
            request.set_stats_period_ms(settings.StatsCollectPeriod_->count());
        }

        if (settings.ArrowFormatSettings_) {
            auto formatSettings = request.mutable_arrow_format_settings();
            if (settings.ArrowFormatSettings_->CompressionCodec_) {
                auto codec = formatSettings->mutable_compression_codec();
                auto type = settings.ArrowFormatSettings_->CompressionCodec_->Type_;
                codec->set_type(::Ydb::Formats::ArrowFormatSettings::CompressionCodec::Type(type));

                if (settings.ArrowFormatSettings_->CompressionCodec_->Level_) {
                    codec->set_level(*settings.ArrowFormatSettings_->CompressionCodec_->Level_);
                }
            }
        }

        if (txControl.HasTx()) {
            auto requestTxControl = request.mutable_tx_control();
            requestTxControl->set_commit_tx(txControl.CommitTx_);

            if (auto* tx = std::get_if<TTransaction>(&txControl.Tx_)) {
                requestTxControl->set_tx_id(TStringType{tx->GetId()});
            } else if (auto* txId = std::get_if<std::string>(&txControl.Tx_)) {
                requestTxControl->set_tx_id(TStringType{*txId});
            } else if (auto* txSettings = std::get_if<TTxSettings>(&txControl.Tx_)) {
                SetTxSettings(*txSettings, requestTxControl->mutable_begin_tx());
            } else {
                Y_DEBUG_ABORT("Unexpected tx control type");
            }
        } else {
            Y_ASSERT(!txControl.CommitTx_);
        }

        if (params) {
            *request.mutable_parameters() = *params;
        }

        auto promise = NewPromise<std::pair<TPlainStatus, TExecuteQueryProcessorPtr>>();

        auto rpcSettings = TRpcRequestSettings::Make(settings);
        if (session.has_value()) {
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

};

TAsyncExecuteQueryIterator TExecQueryImpl::StreamExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
    const TDbDriverStatePtr& driverState, const std::string& query, const TTxControl& txControl,
    const std::optional<TParams>& params, const TExecuteQuerySettings& settings, const std::optional<TSession>& session)
{
    TPlainStatus plainStatus;
    TExecuteQueryProcessorPtr processor;

    auto sessionCopy = session;

    if (auto* txPtr = std::get_if<TTransaction>(&txControl.Tx_); txPtr && txControl.CommitTx_) {
        auto queryCopy = query;
        auto txControlCopy = txControl;
        auto paramsCopy = params;
        auto settingsCopy = settings;

        auto tx = *txPtr;
        auto precommitStatus = co_await tx.Precommit();

        if (!precommitStatus.IsSuccess()) {
            co_return TExecuteQueryIterator(nullptr, std::move(precommitStatus));
        }

        std::tie(plainStatus, processor) = co_await TExecQueryInternal::ExecuteQueryCommon(
            connections, driverState, queryCopy, txControlCopy, paramsCopy ? &paramsCopy->GetProtoMap() : nullptr, settingsCopy, sessionCopy);

        if (!plainStatus.Ok()) {
            co_await tx.ProcessFailure();

            co_return TExecuteQueryIterator(nullptr, std::move(plainStatus));
        }
    } else {
        std::tie(plainStatus, processor) = co_await AsExtractingAwaitable(TExecQueryInternal::ExecuteQueryCommon(
            connections, driverState, query, txControl, params ? &params->GetProtoMap() : nullptr, settings, sessionCopy));
    }

    co_return TExecuteQueryIterator(
        processor
            ? std::make_shared<TExecuteQueryIterator::TReaderImpl>(processor, plainStatus.Endpoint, sessionCopy)
            : nullptr,
        std::move(plainStatus)
    );
}

TAsyncExecuteQueryResult TExecQueryImpl::ExecuteQuery(const std::shared_ptr<TGRpcConnectionsImpl>& connections,
    const TDbDriverStatePtr& driverState, const std::string& query, const TTxControl& txControl,
    const std::optional<TParams>& params, const TExecuteQuerySettings& settings, const std::optional<TSession>& session)
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
