#pragma once

#include <util/generic/list.h>
#include <util/string/builder.h>

#include <ydb/library/accessor/accessor.h>

namespace NFq {

class TJsonParserBuffer {
public:
    using TPtr = TList<TJsonParserBuffer>::iterator;

public:
    TJsonParserBuffer();

    void Reserve(size_t size);

    void AddValue(const TString& value);
    std::string_view AddHolder(std::string_view value);

    std::pair<const char*, size_t> Finish();
    void Clear();

private:
    YDB_ACCESSOR_DEF(ui64, Offset);
    YDB_READONLY_DEF(size_t, NumberValues);
    YDB_READONLY_DEF(bool, Finished)

    TStringBuilder Values;
};

class TJsonParser {
public:
    using TCallback = std::function<void(TVector<TVector<std::string_view>>&&, TJsonParserBuffer::TPtr)>;

public:
    TJsonParser(
        const TVector<TString>& columns,
        const TVector<TString>& types,
        TCallback callback);

    ~TJsonParser();

    TJsonParserBuffer& GetBuffer(ui64 offset);
    void ReleaseBuffer(TJsonParserBuffer::TPtr buffer);

    void Parse();

    TString GetDescription() const;

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonParser> NewJsonParser(
    const TVector<TString>& columns,
    const TVector<TString>& types,
    TJsonParser::TCallback callback);

} // namespace NFq
