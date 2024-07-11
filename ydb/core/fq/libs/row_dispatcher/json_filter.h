
#pragma once

namespace NFq {

class TJsonFilter {
public:
    using TCallback = std::function<void(const TString&)>;
    
public:
    TJsonFilter(const TVector<TString>& columns, const TString& whereFilter, TCallback callback);
    ~TJsonFilter();
    void Push(const NYql::NUdf::TUnboxedValue* value);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonFilter> NewJsonFilter(
    const TVector<TString>& columns,
    const TString& whereFilter,
    TJsonFilter::TCallback callback);

} // namespace NFq
