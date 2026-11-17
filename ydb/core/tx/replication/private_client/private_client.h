#pragma once

#include <ydb/public/api/protos/draft/ydb_replication.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <library/cpp/threading/future/future.h>
#include <util/datetime/base.h>

#include <memory>
#include <utility>

namespace NKikimr::NReplication {

class TChecksumStreamPart: public NYdb::TStatus {
public:
    explicit TChecksumStreamPart(NYdb::TStatus status)
        : TStatus(std::move(status))
    {}

    Ydb::Replication::StreamChecksumTablesInternalResponse Record;
};

class TChecksumTableIterator: public NYdb::TStatus {
public:
    class TReader;

    TChecksumTableIterator(NYdb::TStatus status, std::shared_ptr<TReader> reader);
    NThreading::TFuture<TChecksumStreamPart> ReadNext();
    void Cancel();

private:
    std::shared_ptr<TReader> Reader;
};

class TPrivateClient {
    class TImpl;

public:
    TPrivateClient(const NYdb::TDriver& driver, const NYdb::TCommonClientSettings& settings = NYdb::TCommonClientSettings());

    NThreading::TFuture<TChecksumTableIterator> StreamChecksumTablesInternal(
        Ydb::Replication::StreamChecksumTablesInternalRequest request, TDuration timeout);

private:
    std::shared_ptr<TImpl> Impl;
};

} // namespace NKikimr::NReplication
