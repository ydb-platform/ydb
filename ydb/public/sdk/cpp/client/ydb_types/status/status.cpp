#include "status.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/plain_status/status.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/sdk/cpp/client/ydb_types/exceptions/exceptions.h>

namespace NYdb {

class TStatus::TImpl {
public:
    const TPlainStatus Status;

    TImpl(TPlainStatus&& status)
        : Status(std::move(status))
    { }

    void CheckStatusOk(const TStringType& str) const {
        if (!Status.Ok()) {
            ThrowFatalError(TStringType("Attempt to use result with not successfull status. ") + str + "\n");
        }
    }

    void RaiseError(const TStringType& str) const {
        ythrow TContractViolation(str);
    }
};

TStatus::TStatus(EStatus statusCode, NYql::TIssues&& issues)
    : Impl_(std::make_shared<TImpl>(TPlainStatus{statusCode, std::move(issues)}))
{ }

TStatus::TStatus(TPlainStatus&& plain)
    : Impl_(std::make_shared<TImpl>(std::move(plain)))
{ }

const NYql::TIssues& TStatus::GetIssues() const {
    return Impl_->Status.Issues;
}

EStatus TStatus::GetStatus() const {
    return Impl_->Status.Status;
}

bool TStatus::IsSuccess() const {
    return Impl_->Status.Status == EStatus::SUCCESS;
}

bool TStatus::IsTransportError() const {
    return static_cast<size_t>(Impl_->Status.Status) >= TRANSPORT_STATUSES_FIRST
        && static_cast<size_t>(Impl_->Status.Status) <= TRANSPORT_STATUSES_LAST;
}

void TStatus::CheckStatusOk(const TStringType& str) const {
    Impl_->CheckStatusOk(str);
}

void TStatus::RaiseError(const TStringType& str) const {
    Impl_->RaiseError(str);
}

const TStringType& TStatus::GetEndpoint() const {
    return Impl_->Status.Endpoint;
}

const std::multimap<TStringType, TStringType>& TStatus::GetResponseMetadata() const {
    return Impl_->Status.Metadata;
}

float TStatus::GetConsumedRu() const {
    return Impl_->Status.ConstInfo.consumed_units();
}

void TStatus::Out(IOutputStream& out) const {
    out << "{ status: " << GetStatus()
        << ", issues: " << GetIssues().ToOneLineString()
        << " }";
}

IOutputStream& operator<<(IOutputStream& out, const TStatus& st) {
    out << "Status: " << st.GetStatus() << Endl;
    if (st.GetIssues()) {
        out << "Issues: " << Endl;
        st.GetIssues().PrintTo(out);
    }
    return out;
}

////////////////////////////////////////////////////////////////////////////////

TStreamPartStatus::TStreamPartStatus(TStatus&& status)
    : TStatus(std::move(status))
{}

bool TStreamPartStatus::EOS() const {
    return GetStatus() == EStatus::CLIENT_OUT_OF_RANGE;
}

} // namespace NYdb
