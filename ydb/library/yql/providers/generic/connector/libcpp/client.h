#pragma once

#include <arrow/api.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/connector.grpc.pb.h>
#include <ydb/library/yql/providers/generic/connector/api/service/protos/connector.pb.h>

namespace NYql::NConnector {
    struct TDescribeTableResult {
        NApi::TSchema Schema;
        NApi::TError Error;

        using TPtr = std::shared_ptr<TDescribeTableResult>;
    };

    struct TListSplitsResult {
        std::vector<NApi::TSplit> Splits;
        NApi::TError Error;

        using TPtr = std::shared_ptr<TListSplitsResult>;
    };

    struct TReadSplitsResult {
        std::vector<std::shared_ptr<arrow::RecordBatch>> RecordBatches;
        NApi::TError Error;

        using TPtr = std::shared_ptr<TReadSplitsResult>;
    };

    // IClient is an abstraction that hides some parts of GRPC interface.
    // For now we completely ignore the streaming nature of the Connector and buffer all the results.
    class IClient {
    public:
        using TPtr = std::shared_ptr<IClient>;

        virtual TDescribeTableResult::TPtr DescribeTable(const NApi::TDescribeTableRequest& request) = 0;
        virtual TListSplitsResult::TPtr ListSplits(const NApi::TListSplitsRequest& request) = 0;
        virtual TReadSplitsResult::TPtr ReadSplits(const NApi::TReadSplitsRequest& request) = 0;
        virtual ~IClient() {
        }
    };

    // ClientGRPC - client interacting with Connector server via network
    class TClientGRPC: public IClient {
    public:
        TClientGRPC() = delete;
        TClientGRPC(const TGenericConnectorConfig& config);
        virtual TDescribeTableResult::TPtr DescribeTable(const NApi::TDescribeTableRequest& request) override;
        virtual TListSplitsResult::TPtr ListSplits(const NApi::TListSplitsRequest& request) override;
        virtual TReadSplitsResult::TPtr ReadSplits(const NApi::TReadSplitsRequest& request) override;
        ~TClientGRPC() {
        }

    private:
        std::unique_ptr<NApi::Connector::Stub> Stub_;
    };

    IClient::TPtr MakeClientGRPC(const NYql::TGenericConnectorConfig& config);

    // ClientMock is a stub client that returns predefined data.
    class TClientMock: public IClient {
    public:
        virtual TDescribeTableResult::TPtr DescribeTable(const NApi::TDescribeTableRequest& request) override;
        virtual TListSplitsResult::TPtr ListSplits(const NApi::TListSplitsRequest& request) override;
        virtual TReadSplitsResult::TPtr ReadSplits(const NApi::TReadSplitsRequest& request) override;
        ~TClientMock() {
        }

    private:
        arrow::Status PrepareRecordBatch(std::shared_ptr<arrow::RecordBatch>& table);
    };

    IClient::TPtr MakeClientMock();

} // namespace NYql::NConnector
