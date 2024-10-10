#pragma once

#include <util/string/builder.h>

#include <ydb/library/accessor/accessor.h>

namespace NFq {

class TJsonParserBuffer {
public:
    TJsonParserBuffer();

    void Reserve(size_t size);

    void AddValue(const TString& value);
    std::string_view AddHolder(std::string_view value);

    std::pair<const char*, size_t> Finish();
    void Clear();

private:
    YDB_READONLY_DEF(size_t, NumberValues);
    YDB_READONLY_DEF(bool, Finished)

    TStringBuilder Values;
};

class TJsonParser {
public:
    TJsonParser(const TVector<TString>& columns, const TVector<TString>& types);
    ~TJsonParser();

    TJsonParserBuffer& GetBuffer();
    const TVector<TVector<std::string_view>>& Parse();

    TString GetDescription() const;
    TString GetDebugString(const TVector<TVector<std::string_view>>& parsedValues) const;

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types);

} // namespace NFq
