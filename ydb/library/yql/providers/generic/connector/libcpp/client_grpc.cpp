#include "client.h"
#include "error.h"
#include "utils.h"

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/library/yql/providers/generic/connector/api/service/connector.grpc.pb.h>

#define REQUEST_START() ({ YQL_LOG(INFO) << __func__ << ": request handling started"; })

#define REQUEST_END(status, err)                                                                           \
    ({                                                                                                     \
        if (!status.ok()) {                                                                                \
            YQL_LOG(ERROR) << __func__ << ": request handling failed: " << int(status.error_code()) << " " \
                           << status.error_message();                                                      \
            /* do not overwrite logical error with transport error */                                      \
            if (ErrorIsSuccess(err)) {                                                                     \
                err = ErrorFromGRPCStatus(status);                                                         \
            }                                                                                              \
        } else {                                                                                           \
            YQL_LOG(INFO) << __func__ << ": request handling finished";                                    \
        }                                                                                                  \
    })

namespace NYql::Connector {
    ClientGRPC::ClientGRPC(const NYql::TGenericConnectorConfig& cfg) {
        std::shared_ptr<grpc::ChannelCredentials> credentials;
        auto networkEndpoint = cfg.GetEndpoint().host() + ":" + std::to_string(cfg.GetEndpoint().port());

        if (cfg.GetUseTLS()) {
            // Hopefully GRPC will find appropriate CA cert in system folders
            credentials = grpc::SslCredentials(grpc::SslCredentialsOptions());
        } else {
            credentials = grpc::InsecureChannelCredentials();
        }

        auto channel = grpc::CreateChannel(networkEndpoint, credentials);
        Stub_ = API::Connector::NewStub(channel);
    };

    std::shared_ptr<DescribeTableResult> ClientGRPC::DescribeTable(const API::DescribeTableRequest& request) {
        grpc::ClientContext ctx;
        API::DescribeTableResponse response;
        auto out = std::make_shared<Connector::DescribeTableResult>();

        REQUEST_START();
        grpc::Status status = Stub_->DescribeTable(&ctx, request, &response);
        REQUEST_END(status, out->Error);

        out->Schema = response.schema();
        out->Error = response.error();

        return out;
    };

    std::shared_ptr<ListSplitsResult> ClientGRPC::ListSplits(const API::ListSplitsRequest& request) {
        REQUEST_START();
        grpc::ClientContext ctx;
        std::unique_ptr<grpc::ClientReader<API::ListSplitsResponse>> reader(Stub_->ListSplits(&ctx, request));

        API::ListSplitsResponse response;
        auto out = std::make_shared<ListSplitsResult>();
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

    std::shared_ptr<ReadSplitsResult> ClientGRPC::ReadSplits(const API::ReadSplitsRequest& request) {
        REQUEST_START();
        grpc::ClientContext ctx;
        std::unique_ptr<grpc::ClientReader<API::ReadSplitsResponse>> reader(Stub_->ReadSplits(&ctx, request));

        API::ReadSplitsResponse response;
        auto out = std::make_shared<ReadSplitsResult>();
        out->Error.Clear();

        while (reader->Read(&response)) {
            // preserve server error
            out->Error = response.error();

            // convert our own columnar format into arrow batch
            out->RecordBatches.push_back(APIReadSplitsResponseToArrowRecordBatch(response));
        }

        auto status = reader->Finish();
        REQUEST_END(status, out->Error);

        return out;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& cfg) {
        std::shared_ptr<IClient> out = std::make_shared<ClientGRPC>(cfg);
        return out;
    }
}
