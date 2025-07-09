#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

class ITableDataService: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITableDataService>;

    virtual ~ITableDataService() = default;

    virtual NThreading::TFuture<void> Put(const TString& key, const TString& value) = 0;

    virtual NThreading::TFuture<TMaybe<TString>> Get(const TString& key) const = 0;

    virtual NThreading::TFuture<void> Delete(const TString& key) = 0;

    virtual NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& groupsToDelete) = 0;
};

} // namespace NYql::NFmr
