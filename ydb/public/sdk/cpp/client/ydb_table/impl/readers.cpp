#include "readers.h"

#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>


namespace NYdb {
namespace NTable {

using namespace NThreading;


TTablePartIterator::TReaderImpl::TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint)
    : StreamProcessor_(streamProcessor)
    , Finished_(false)
    , Endpoint_(endpoint)
{}

TTablePartIterator::TReaderImpl::~TReaderImpl() {
    StreamProcessor_->Cancel();
}

bool TTablePartIterator::TReaderImpl::IsFinished() {
    return Finished_;
}

TAsyncSimpleStreamPart<TResultSet> TTablePartIterator::TReaderImpl::ReadNext(std::shared_ptr<TSelf> self) {
    auto promise = NThreading::NewPromise<TSimpleStreamPart<TResultSet>>();
    // Capture self - guarantee no dtor call during the read
    auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
        std::optional<TReadTableSnapshot> snapshot;
        if (self->Response_.has_snapshot()) {
            snapshot.emplace(
                self->Response_.snapshot().plan_step(),
                self->Response_.snapshot().tx_id());
        }
        if (!grpcStatus.Ok()) {
            self->Finished_ = true;
            promise.SetValue({TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())),
                            TStatus(TPlainStatus(grpcStatus, self->Endpoint_)),
                            snapshot});
        } else {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(self->Response_.issues(), issues);
            EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
            promise.SetValue({TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())),
                            TStatus(clientStatus, std::move(issues)),
                            snapshot});
        }
    };
    StreamProcessor_->Read(&Response_, readCb);
    return promise.GetFuture();
}



TScanQueryPartIterator::TReaderImpl::TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint)
    : StreamProcessor_(streamProcessor)
    , Finished_(false)
    , Endpoint_(endpoint)
{}

TScanQueryPartIterator::TReaderImpl::~TReaderImpl() {
    StreamProcessor_->Cancel();
}

bool TScanQueryPartIterator::TReaderImpl::IsFinished() const {
    return Finished_;
}

TAsyncScanQueryPart TScanQueryPartIterator::TReaderImpl::ReadNext(std::shared_ptr<TSelf> self) {
    auto promise = NThreading::NewPromise<TScanQueryPart>();
    // Capture self - guarantee no dtor call during the read
    auto readCb = [self, promise](TGRpcStatus&& grpcStatus) mutable {
        if (!grpcStatus.Ok()) {
            self->Finished_ = true;
            promise.SetValue({TStatus(TPlainStatus(grpcStatus, self->Endpoint_))});
        } else {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(self->Response_.issues(), issues);
            EStatus clientStatus = static_cast<EStatus>(self->Response_.status());
            // TODO: Add headers for streaming calls.
            TPlainStatus plainStatus{clientStatus, std::move(issues), self->Endpoint_, {}};
            TStatus status{std::move(plainStatus)};
            TMaybe<TQueryStats> queryStats;
            TMaybe<TString> diagnostics;

            if (self->Response_.result().has_query_stats()) {
                queryStats = TQueryStats(self->Response_.result().query_stats());
            }

            diagnostics = self->Response_.result().query_full_diagnostics();

            std::optional<TVirtualTimestamp> vt;

            if (self->Response_.result().has_snapshot()) {
                const auto& snap = self->Response_.result().snapshot();
                vt = TVirtualTimestamp(snap.plan_step(), snap.tx_id());
            }

            if (self->Response_.result().has_result_set()) {
                promise.SetValue({std::move(status),
                    TResultSet(std::move(*self->Response_.mutable_result()->mutable_result_set())), queryStats, diagnostics, std::move(vt)});
            } else {
                promise.SetValue({std::move(status), queryStats, diagnostics});
            }
        }
    };
    StreamProcessor_->Read(&Response_, readCb);
    return promise.GetFuture();
}

}
}
