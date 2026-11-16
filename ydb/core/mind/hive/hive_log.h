#pragma once

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <library/cpp/json/json_value.h>

namespace NKikimr {
namespace NHive {

class THive;

class TLoggedTransaction {
    ui64 Index;
    TString User;
    TString LogPrefix;

protected:
    TLoggedTransaction(TString user, THive* self);

    void WriteOperation(NIceDb::TNiceDb& db, const NJson::TJsonValue& op, TStringBuf description);

    TString GetUserForLog() const {
        return User.empty() ? TString("anonymous") : User;
    }
};

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
