#include "client.h"
#include "error.h"
#include "utils.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/generic/connector/api/service/connector.grpc.pb.h>

#define REQUEST_START() ({ YQL_LOG(INFO) << __func__ << ": request handling started"; })

#define REQUEST_END(status, err)                                        \
    ({                                                                  \
        if (!status.ok()) {                                             \
            YQL_LOG(ERROR) << __func__ << ": request handling failed: " \
                           << "code=" << int(status.error_code())       \
                           << ", msg=" << status.error_message();       \
            err = ErrorFromGRPCStatus(status);                          \
        } else {                                                        \
            YQL_LOG(INFO) << __func__ << ": request handling finished"; \
        }                                                               \
    })

namespace NYql::NConnector {
    TClientGRPC::TClientGRPC(const NYql::TGenericConnectorConfig& cfg) {
        std::shared_ptr<grpc::ChannelCredentials> credentials;
        auto networkEndpoint = cfg.GetEndpoint().host() + ":" + std::to_string(cfg.GetEndpoint().port());

        if (cfg.GetUseTLS()) {
            // Hopefully GRPC will find appropriate CA cert in system folders
            credentials = grpc::SslCredentials(grpc::SslCredentialsOptions());
        } else {
            credentials = grpc::InsecureChannelCredentials();
        }

        auto channel = grpc::CreateChannel(networkEndpoint, credentials);
        Stub_ = NApi::Connector::NewStub(channel);
    };

    std::shared_ptr<TDescribeTableResult> TClientGRPC::DescribeTable(const NApi::TDescribeTableRequest& request) {
        grpc::ClientContext ctx;
        NApi::TDescribeTableResponse response;
        auto out = std::make_shared<TDescribeTableResult>();

        REQUEST_START();
        grpc::Status status = Stub_->DescribeTable(&ctx, request, &response);
        out->Error = response.error();
        REQUEST_END(status, out->Error);

        out->Schema = response.schema();

        return out;
    };

    std::shared_ptr<TListSplitsResult> TClientGRPC::ListSplits(const NApi::TListSplitsRequest& request) {
        REQUEST_START();
        grpc::ClientContext ctx;
        std::unique_ptr<grpc::ClientReader<NApi::TListSplitsResponse>> reader(Stub_->ListSplits(&ctx, request));

        NApi::TListSplitsResponse response;
        auto out = std::make_shared<TListSplitsResult>();
        out->Error.Clear();

        while (reader->Read(&response)) {
            // preserve server error
            out->Error = response.error();

            for (const auto& split : response.splits()) {
                out->Splits.push_back(split);
            }
        }

        YQL_LOG(DEBUG) << __func__ << ": total splits: " << out->Splits.size();

        auto status = reader->Finish();
        REQUEST_END(status, out->Error);

        return out;
    };

    std::shared_ptr<TReadSplitsResult> TClientGRPC::ReadSplits(const NApi::TReadSplitsRequest& request) {
        REQUEST_START();
        grpc::ClientContext ctx;
        std::unique_ptr<grpc::ClientReader<NApi::TReadSplitsResponse>> reader(Stub_->ReadSplits(&ctx, request));

        NApi::TReadSplitsResponse response;
        auto out = std::make_shared<TReadSplitsResult>();
        out->Error.Clear();

        while (reader->Read(&response)) {
            // preserve server error
            out->Error = response.error();

            if (!ErrorIsSuccess(out->Error)) {
                break;
            }

            // convert our own columnar format into arrow batch
            out->RecordBatches.push_back(APIReadSplitsResponseToArrowRecordBatch(response));
        }

        auto status = reader->Finish();
        REQUEST_END(status, out->Error);

        return out;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& cfg) {
        std::shared_ptr<IClient> out = std::make_shared<TClientGRPC>(cfg);
        return out;
    }
}
