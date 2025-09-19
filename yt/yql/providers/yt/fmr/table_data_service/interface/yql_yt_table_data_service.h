#pragma once

#include <library/cpp/threading/future/core/future.h>
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

class ITableDataService: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ITableDataService>;

    virtual ~ITableDataService() = default;

    // Table data service key should consist of group (prefix) and chunkId.

    virtual NThreading::TFuture<void> Put(const TString& group, const TString& chunkId, const TString& value) = 0;

    virtual NThreading::TFuture<TMaybe<TString>> Get(const TString& group, const TString& chunkId) const = 0;

    virtual NThreading::TFuture<void> Delete(const TString& group, const TString& chunkId) = 0;

    virtual NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& groupsToDelete) = 0;

    virtual NThreading::TFuture<void> Clear() = 0;
};

} // namespace NYql::NFmr
