#include "operation.h"

#include <ydb/public/api/protos/ydb_operation.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_types/status/status.h>

namespace NYdb {


class TOperation::TImpl {
public:
    TImpl(TStatus&& status)
        : Status_(std::move(status))
        , Ready_(true)
    { }

    TImpl(TStatus&& status, Ydb::Operations::Operation&& operation)
        : Id_(operation.id(), true /* allowEmpty */)
        , Status_(std::move(status))
        , Ready_(operation.ready())
        , CreateTime_(ProtoTimestampToInstant(operation.create_time()))
        , EndTime_(ProtoTimestampToInstant(operation.end_time()))
        , CreatedBy_(operation.created_by())
        , Operation_(std::move(operation))
    {
    }

    const TOperationId& Id() const {
        return Id_;
    }

    bool Ready() const {
        return Ready_;
    }

    const TStatus& Status() const {
        return Status_;
    }

    TInstant CreateTime() const {
        return CreateTime_;
    }

    TInstant EndTime() const {
        return EndTime_;
    }

    const TString& CreatedBy() const {
        return CreatedBy_;
    }

    const Ydb::Operations::Operation& GetProto() const {
        return Operation_;
    }

private:
    const TOperationId Id_;
    const TStatus Status_;
    const bool Ready_;
    const TInstant CreateTime_;
    const TInstant EndTime_;
    const TString CreatedBy_;
    const Ydb::Operations::Operation Operation_;
};

TOperation::TOperation(TStatus&& status)
    : Impl_(std::make_shared<TImpl>(std::move(status)))
{ }

TOperation::TOperation(TStatus&& status, Ydb::Operations::Operation&& operation)
    : Impl_(std::make_shared<TImpl>(std::move(status), std::move(operation)))
{ }

const TOperation::TOperationId& TOperation::Id() const {
    return Impl_->Id();
}

bool TOperation::Ready() const {
    return Impl_->Ready();
}

const TStatus& TOperation::Status() const {
    return Impl_->Status();
}

TInstant TOperation::CreateTime() const {
    return Impl_->CreateTime();
}

TInstant TOperation::EndTime() const {
    return Impl_->EndTime();
}

const TString& TOperation::CreatedBy() const {
    return Impl_->CreatedBy();
}

TString TOperation::ToString() const {
    TString result;
    TStringOutput out(result);
    Out(out);
    return result;
}

void TOperation::Out(IOutputStream& o) const {
    o << GetProto().DebugString();
}

TString TOperation::ToJsonString() const {
    using namespace google::protobuf::util;

    TString json;
    auto status = MessageToJsonString(GetProto(), &json, JsonPrintOptions());
    Y_ABORT_UNLESS(status.ok());
    return json;
}

const Ydb::Operations::Operation& TOperation::GetProto() const {
    return Impl_->GetProto();
}

TInstant ProtoTimestampToInstant(const NProtoBuf::Timestamp& timestamp) {
    ui64 us = timestamp.seconds() * 1000000;
    us += timestamp.nanos() / 1000;
    return TInstant::MicroSeconds(us);
}

} // namespace NYdb
