#pragma once

#include <ydb/library/yql/public/udf/udf_value.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/include/read_events.h>

namespace NFq {

class TJsonParserError: public yexception {
public:
    TJsonParserError() = default;
    TJsonParserError(const std::string& fieldName)
        : FieldName(fieldName)
    {}

    TMaybe<TString> GetField() const noexcept {
        return FieldName;
    }

private:
    TMaybe<TString> FieldName;
};


class TJsonParser {
public:
    using TCallback = std::function<void(ui64 rowsOffset, ui64 numberRows, const TVector<TVector<NYql::NUdf::TUnboxedValue>>& parsedValues)>;

public:
    TJsonParser(const TVector<TString>& columns, const TVector<TString>& types, TCallback parseCallback, ui64 batchSize, TDuration batchCreationTimeout, ui64 bufferCellCount);
    ~TJsonParser();

    bool IsReady() const;
    TInstant GetCreationDeadline() const;
    size_t GetNumberValues() const;
    const TVector<ui64>& GetOffsets() const;

    void AddMessages(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages);
    void Parse();

    TString GetDescription() const;

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types, TJsonParser::TCallback parseCallback, ui64 batchSize, TDuration batchCreationTimeout, ui64 bufferCellCount);

} // namespace NFq
