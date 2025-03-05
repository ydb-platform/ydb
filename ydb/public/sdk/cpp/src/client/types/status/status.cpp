#include <ydb-cpp-sdk/client/types/status/status.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_internal/plain_status/status.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb-cpp-sdk/client/types/exceptions/exceptions.h>

#include <util/string/cast.h>

namespace NYdb::inline V3 {

class TStatus::TImpl {
public:
    const TPlainStatus Status;

    TImpl(TPlainStatus&& status)
        : Status(std::move(status))
    { }

    void CheckStatusOk(const std::string& str) const {
        if (!Status.Ok()) {
            ThrowFatalError(std::string("Attempt to use result with not successfull status. ") + str + "\n");
        }
    }

    void RaiseError(const std::string& str) const {
        ythrow TContractViolation(str);
    }
};

TStatus::TStatus(EStatus statusCode, NYdb::NIssue::TIssues&& issues)
    : Impl_(std::make_shared<TImpl>(TPlainStatus{statusCode, std::move(issues)}))
{ }

TStatus::TStatus(TPlainStatus&& plain)
    : Impl_(std::make_shared<TImpl>(std::move(plain)))
{ }

const NYdb::NIssue::TIssues& TStatus::GetIssues() const {
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

void TStatus::CheckStatusOk(const std::string& str) const {
    Impl_->CheckStatusOk(str);
}

void TStatus::RaiseError(const std::string& str) const {
    Impl_->RaiseError(str);
}

const std::string& TStatus::GetEndpoint() const {
    return Impl_->Status.Endpoint;
}

const std::multimap<std::string, std::string>& TStatus::GetResponseMetadata() const {
    return Impl_->Status.Metadata;
}

float TStatus::GetConsumedRu() const {
    return Impl_->Status.CostInfo.consumed_units();
}

void TStatus::Out(IOutputStream& out) const {
    out << "{ status: " << GetStatus()
        << ", issues: " << GetIssues().ToOneLineString()
        << " }";
}

IOutputStream& operator<<(IOutputStream& out, const TStatus& st) {
    out << "Status: " << ToString(st.GetStatus()) << Endl;
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

////////////////////////////////////////////////////////////////////////////////

namespace NStatusHelpers {

void ThrowOnError(TStatus status, std::function<void(TStatus)> onSuccess) {
    if (!status.IsSuccess()) {
        throw TYdbErrorException(status) << status;
    } else {
        onSuccess(status);
    }
}

void ThrowOnErrorOrPrintIssues(TStatus status) {
    ThrowOnError(status, [](TStatus status) {
        if (status.GetIssues()) {
            std::cerr << ToString(status);
        }
    });
}

}

} // namespace NYdb
