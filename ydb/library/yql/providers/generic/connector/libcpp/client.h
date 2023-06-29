#pragma once

#include <arrow/api.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/connector.grpc.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

namespace NYql::Connector {
    struct DescribeTableResult {
        API::Schema Schema;
        API::Error Error;

        using TPtr = std::shared_ptr<DescribeTableResult>;
    };

    struct ListSplitsResult {
        std::vector<API::Split> Splits;
        API::Error Error;

        using TPtr = std::shared_ptr<ListSplitsResult>;
    };

    struct ReadSplitsResult {
        std::vector<std::shared_ptr<arrow::RecordBatch>> RecordBatches;
        API::Error Error;

        using TPtr = std::shared_ptr<ReadSplitsResult>;
    };

    // IClient is an abstraction that hides some parts of GRPC interface.
    // For now we completely ignore the streaming nature of the Connector and buffer all the results.
    class IClient {
    public:
        using TPtr = std::shared_ptr<IClient>;

        virtual DescribeTableResult::TPtr DescribeTable(const API::DescribeTableRequest& request) = 0;
        virtual ListSplitsResult::TPtr ListSplits(const API::ListSplitsRequest& request) = 0;
        virtual ReadSplitsResult::TPtr ReadSplits(const API::ReadSplitsRequest& request) = 0;
        virtual ~IClient() {
        }
    };

    // ClientGRPC - client interacting with Connector server via network
    class ClientGRPC: public IClient {
    public:
        ClientGRPC() = delete;
        ClientGRPC(const TGenericConnectorConfig& config);
        virtual DescribeTableResult::TPtr DescribeTable(const API::DescribeTableRequest& request) override;
        virtual ListSplitsResult::TPtr ListSplits(const API::ListSplitsRequest& request) override;
        virtual ReadSplitsResult::TPtr ReadSplits(const API::ReadSplitsRequest& request) override;
        ~ClientGRPC() {
        }

    private:
        std::unique_ptr<API::Connector::Stub> Stub_;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& config);

    // ClientMock is a stub client that returns predefined data.
    class ClientMock: public IClient {
    public:
        virtual DescribeTableResult::TPtr DescribeTable(const API::DescribeTableRequest& request) override;
        virtual ListSplitsResult::TPtr ListSplits(const API::ListSplitsRequest& request) override;
        virtual ReadSplitsResult::TPtr ReadSplits(const API::ReadSplitsRequest& request) override;
        ~ClientMock() {
        }

    private:
        arrow::Status PrepareRecordBatch(std::shared_ptr<arrow::RecordBatch>& table);
    };

    IClient::TPtr MakeClientMock();

} // namespace NYql::Connector
