#pragma once

#include <library/cpp/threading/future/core/future.h>

namespace NYql::NFmr {

class ITableDataService: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITableDataService>;

    virtual ~ITableDataService() = default;

    virtual NThreading::TFuture<void> Put(const TString& id, const TString& data) = 0;

    virtual NThreading::TFuture<TMaybe<TString>> Get(const TString& id) = 0;

    virtual NThreading::TFuture<void> Delete(const TString& id) = 0;
};

} // namespace NYql::NFmr
