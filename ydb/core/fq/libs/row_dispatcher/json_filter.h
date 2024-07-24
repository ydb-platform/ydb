
#pragma once

namespace NFq {

#include <ydb/library/yql/public/udf/udf_data_type.h>
#include <ydb/library/yql/public/udf/udf_value.h>

class TJsonFilter {
public:
    using TCallback = std::function<void(const TString&)>;
    
public:
    TJsonFilter(
        const TVector<TString>& columns, 
        const TVector<TString>& types,
        const TString& whereFilter,
        TCallback callback);
    ~TJsonFilter();
    void Push(const NYql::NUdf::TUnboxedValue* value);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonFilter> NewJsonFilter(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    const TString& whereFilter,
    TJsonFilter::TCallback callback);

} // namespace NFq
