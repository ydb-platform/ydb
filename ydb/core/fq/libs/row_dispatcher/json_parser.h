
#pragma once

namespace NFq {

class TJsonParser {
public:
    using TCallback = std::function<void(const NYql::NUdf::TUnboxedValue*)>;
    
public:
    TJsonParser(const TVector<TString>& columns, TCallback callback);
    ~TJsonParser();
    void Push(const TString& value);

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonParser> NewJsonParser(
    const TVector<TString>& columns,
    TJsonParser::TCallback callback);

} // namespace NFq
