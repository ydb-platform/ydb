#pragma once

#include "generic_status.h"

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NKikimr {

template <class TStatus, TStatus StatusOk, TStatus DefaultError>
class TConclusionStatusImpl: public TConclusionStatusGenericImpl<TConclusionStatusImpl<TStatus, StatusOk, DefaultError>, TString, TStatus, StatusOk, DefaultError> {
protected:
    using TSelf = TConclusionStatusImpl<TStatus, StatusOk, DefaultError>;
    using TBase = TConclusionStatusGenericImpl<TSelf, TString, TStatus, StatusOk, DefaultError>;
    using TBase::TBase;

    friend class TConclusionStatusGenericImpl<TSelf, TString, TStatus, StatusOk, DefaultError>;

    TConclusionStatusImpl() = default;

    TConclusionStatusImpl(const char* errorMessage, TStatus status = DefaultError)
        : TBase(TString(errorMessage), status) {
    }

    TConclusionStatusImpl(const std::string& errorMessage, TStatus status = DefaultError)
        : TBase(TString(errorMessage), status) {
    }

public:
    void Validate(const TString& processInfo = Default<TString>()) const {
        if (processInfo) {
            Y_ABORT_UNLESS(TBase::Ok(), "error=%s, processInfo=%s", GetErrorMessage().c_str(), processInfo.c_str());
        } else {
            Y_ABORT_UNLESS(TBase::Ok(), "error=%s", GetErrorMessage().c_str());
        }
    }

    void Ensure(const TString& processInfo = Default<TString>()) const {
        if (processInfo) {
            Y_ENSURE(TBase::Ok(), "error=" + GetErrorMessage() + ", processInfo=" + processInfo);
        } else {
            Y_ENSURE(TBase::Ok(), "error=" + GetErrorMessage());
        }
    }

    [[nodiscard]] TString GetErrorMessage() const {
        Y_ABORT_UNLESS(TBase::IsFail());
        return TBase::GetErrorDescription();
    }
};

}   // namespace NKikimr
