#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

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
    const TMaybe<TString> FieldName;
};


class TJsonParser {
public:
    TJsonParser(const TVector<TString>& columns, const TVector<TString>& types, ui64 batchSize, TDuration batchCreationTimeout);
    ~TJsonParser();

    bool IsReady() const;
    TInstant GetCreationDeadline() const;
    size_t GetNumberValues() const;
    const TVector<ui64>& GetOffsets() const;

    void AddMessages(const TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage>& messages);
    const TVector<NKikimr::NMiniKQL::TUnboxedValueVector>& Parse();

    TString GetDescription() const;

private:
    class TImpl;
    const std::unique_ptr<TImpl> Impl;
};

std::unique_ptr<TJsonParser> NewJsonParser(const TVector<TString>& columns, const TVector<TString>& types, ui64 batchSize, TDuration batchCreationTimeout);

} // namespace NFq
