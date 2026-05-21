#pragma once

namespace NKikimr {
namespace NHive {

TString GetLogPrefix();

template <typename T>
class TTransactionBase : public NKikimr::NTabletFlatExecutor::TTransactionBase<T> {
protected:
    using TSelf = T;
    using TBase = TTransactionBase<T>;

public:
    TTransactionBase(T* self)
        : NKikimr::NTabletFlatExecutor::TTransactionBase<T>(self)
    {}

    TString GetLogPrefix() const {
        return NKikimr::NTabletFlatExecutor::TTransactionBase<T>::Self->GetLogPrefix();
    }
};

}
}

#define Y_ENSURE_LOG(cond, stream) if (!(cond)) { LOG_ERROR_S(*TlsActivationContext, NKikimrServices::HIVE, GetLogPrefix() << "Failed condition \"" << #cond << "\" " << stream); }
