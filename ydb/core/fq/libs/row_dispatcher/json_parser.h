#pragma once

#include <functional>

#include <util/generic/string.h>

namespace NFq {

class TJsonParser {
public:
    using TCallback = std::function<void(ui64, TList<TString>&&)>;
    
public:
    TJsonParser(
        const TString& udfDir,
        const TVector<TString>& columns,
        TCallback callback);
    ~TJsonParser();
    void Push(ui64 offset, const TString& value);
    TString GetSql();

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonParser> NewJsonParser(
    const TString& udfDir,
    const TVector<TString>& columns,
    TJsonParser::TCallback callback);

} // namespace NFq
