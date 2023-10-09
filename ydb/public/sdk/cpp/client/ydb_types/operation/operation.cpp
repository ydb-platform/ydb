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

    const Ydb::Operations::Operation& GetProto() const {
        return Operation_;
    }

private:
    const TOperationId Id_;
    const TStatus Status_;
    const bool Ready_;
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

} // namespace NYdb
