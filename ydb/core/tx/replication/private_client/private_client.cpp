#include "private_client.h"

#include <ydb/public/api/grpc/draft/ydb_replication_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/common_client/impl/client.h>
#include <ydb/public/sdk/cpp/src/library/issue/yql_issue_message.h>

#include <google/protobuf/util/time_util.h>

#include <atomic>

namespace NKikimr::NReplication {

using namespace NYdb;

class TChecksumTableIterator::TReader: public std::enable_shared_from_this<TReader> {
public:
    using TResponse = Ydb::Replication::StreamChecksumTablesInternalResponse;
    using TProcessor = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;

    TReader(TProcessor processor, std::string endpoint)
        : Processor(std::move(processor))
        , Endpoint(std::move(endpoint))
    {}

    ~TReader() {
        Cancel();
    }

    void Cancel() {
        Finished = true;
        Processor->Cancel();
    }

    NThreading::TFuture<TChecksumStreamPart> ReadNext() {
        auto promise = NThreading::NewPromise<TChecksumStreamPart>();
        if (Reading.exchange(true)) {
            promise.SetValue(TChecksumStreamPart(TStatus(EStatus::CLIENT_INTERNAL_ERROR,
                {NIssue::TIssue("Concurrent checksum stream reads are not supported")})));
            return promise.GetFuture();
        }
        if (Finished) {
            Reading = false;
            promise.SetValue(TChecksumStreamPart(TStatus(TPlainStatus(EStatus::CLIENT_OUT_OF_RANGE,
                NIssue::TIssues{}, Endpoint, {}))));
            return promise.GetFuture();
        }
        Response.Clear();
        Processor->Read(&Response, [self = shared_from_this(), promise](NYdbGrpc::TGrpcStatus&& status) mutable {
            if (!status.Ok()) {
                self->Finished = true;
                self->Reading = false;
                promise.SetValue(TChecksumStreamPart(TStatus(TPlainStatus(status, self->Endpoint))));
                return;
            }
            auto& response = self->Response;
            NIssue::TIssues issues;
            NIssue::IssuesFromMessage(response.issues(), issues);
            TChecksumStreamPart part(TStatus(TPlainStatus(static_cast<EStatus>(response.status()),
                std::move(issues), self->Endpoint, {})));
            part.Record = std::move(response);
            if (part.Record.complete() || !part.IsSuccess()) {
                self->Finished = true;
            }
            self->Reading = false;
            promise.SetValue(std::move(part));
        });
        return promise.GetFuture();
    }

private:
    TProcessor Processor;
    const std::string Endpoint;
    TResponse Response;
    std::atomic<bool> Reading = false;
    std::atomic<bool> Finished = false;
};

TChecksumTableIterator::TChecksumTableIterator(TStatus status, std::shared_ptr<TReader> reader)
    : TStatus(std::move(status))
    , Reader(std::move(reader))
{}

NThreading::TFuture<TChecksumStreamPart> TChecksumTableIterator::ReadNext() {
    if (!Reader) {
        return NThreading::MakeFuture(TChecksumStreamPart(TStatus(*this)));
    }
    return Reader->ReadNext();
}

void TChecksumTableIterator::Cancel() {
    if (Reader) {
        Reader->Cancel();
    }
}

class TPrivateClient::TImpl: public TClientImplCommon<TPrivateClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TCommonClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
    {}

    NThreading::TFuture<TChecksumTableIterator> StreamChecksumTablesInternal(
        Ydb::Replication::StreamChecksumTablesInternalRequest request, TDuration timeout)
    {
        using namespace Ydb::Replication;

        *request.mutable_timeout() = google::protobuf::util::TimeUtil::MicrosecondsToDuration(timeout.MicroSeconds());
        auto promise = NThreading::NewPromise<TChecksumTableIterator>();
        TRpcRequestSettings rpcSettings;
        rpcSettings.Deadline = NYdb::TDeadline::AfterDuration(timeout);
        Connections_->StartReadStream<V1::ReplicationService, StreamChecksumTablesInternalRequest, StreamChecksumTablesInternalResponse>(
            std::move(request), [promise](TPlainStatus status, TChecksumTableIterator::TReader::TProcessor processor) mutable {
                std::shared_ptr<TChecksumTableIterator::TReader> reader;
                if (processor) {
                    reader = std::make_shared<TChecksumTableIterator::TReader>(std::move(processor), status.Endpoint);
                }
                promise.SetValue(TChecksumTableIterator(TStatus(std::move(status)), std::move(reader)));
            }, &V1::ReplicationService::Stub::AsyncStreamChecksumTablesInternal, DbDriverState_, rpcSettings);
        return promise.GetFuture();
    }
};

TPrivateClient::TPrivateClient(const TDriver& driver, const TCommonClientSettings& settings)
    : Impl(std::make_shared<TImpl>(CreateInternalInterface(driver), settings))
{}

NThreading::TFuture<TChecksumTableIterator> TPrivateClient::StreamChecksumTablesInternal(
    Ydb::Replication::StreamChecksumTablesInternalRequest request, TDuration timeout)
{
    return Impl->StreamChecksumTablesInternal(std::move(request), timeout);
}

} // namespace NKikimr::NReplication
