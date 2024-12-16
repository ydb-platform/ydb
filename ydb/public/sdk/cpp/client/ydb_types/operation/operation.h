#pragma once

#include <ydb/public/lib/operation_id/operation_id.h>

#include <library/cpp/threading/future/future.h>

#include <google/protobuf/stubs/status.h>
#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/util/json_util.h>

namespace Ydb {
namespace Operations {

class Operation;

} // namespace Operations
} // namespace Ydb

namespace NYdb {

class TStatus;

class TOperation {
public:
    using TOperationId = NKikimr::NOperationId::TOperationId;

public:
    TOperation(TStatus&& status);
    TOperation(TStatus&& status, Ydb::Operations::Operation&& operation);
    virtual ~TOperation() = default;

    const TOperationId& Id() const;
    bool Ready() const;
    const TStatus& Status() const;
    TInstant CreateTime() const;
    TInstant EndTime() const;
    const TString& CreatedBy() const;

    TString ToString() const;
    TString ToJsonString() const;
    void Out(IOutputStream& o) const;

protected:
    const Ydb::Operations::Operation& GetProto() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

using TAsyncOperation = NThreading::TFuture<TOperation>;

TInstant ProtoTimestampToInstant(const NProtoBuf::Timestamp& timestamp);

} // namespace NYdb
