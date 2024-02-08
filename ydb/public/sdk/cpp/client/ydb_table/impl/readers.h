#pragma once

#include <ydb/public/sdk/cpp/client/resources/ydb_resources.h>

#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>

#include <util/random/random.h>

#include "client_session.h"
#include "data_query.h"
#include "request_migrator.h"


namespace NYdb {
namespace NTable {

using namespace NThreading;


class TTablePartIterator::TReaderImpl {
public:
    using TSelf = TTablePartIterator::TReaderImpl;
    using TResponse = Ydb::Table::ReadTableResponse;
    using TStreamProcessorPtr = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NYdbGrpc::TGrpcStatus;
    using TBatchReadResult = std::pair<TResponse, TGRpcStatus>;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint);
    ~TReaderImpl();
    bool IsFinished();
    TAsyncSimpleStreamPart<TResultSet> ReadNext(std::shared_ptr<TSelf> self);

private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    TString Endpoint_;
};


class TScanQueryPartIterator::TReaderImpl {
public:
    using TSelf = TScanQueryPartIterator::TReaderImpl;
    using TResponse = Ydb::Table::ExecuteScanQueryPartialResponse;
    using TStreamProcessorPtr = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TPtr;
    using TReadCallback = NYdbGrpc::IStreamRequestReadProcessor<TResponse>::TReadCallback;
    using TGRpcStatus = NYdbGrpc::TGrpcStatus;
    using TBatchReadResult = std::pair<TResponse, TGRpcStatus>;

    TReaderImpl(TStreamProcessorPtr streamProcessor, const TString& endpoint);
    ~TReaderImpl();
    bool IsFinished() const;
    TAsyncScanQueryPart ReadNext(std::shared_ptr<TSelf> self);

private:
    TStreamProcessorPtr StreamProcessor_;
    TResponse Response_;
    bool Finished_;
    TString Endpoint_;
};


}
}
