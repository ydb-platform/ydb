#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <library/cpp/string_utils/csv/csv.h>

namespace NYdb {
namespace NConsoleClient {

class TCsvParseException : public yexception {};

class TCsvParser {
public:
    struct TParseMetadata {
        std::optional<uint64_t> Line;
        std::optional<TString> Filename;
    };

    TCsvParser() = default;

    TCsvParser(const TCsvParser&) = delete;
    TCsvParser(TCsvParser&&) = default;
    TCsvParser& operator=(const TCsvParser&) = delete;
    TCsvParser& operator=(TCsvParser&&) = default;
    ~TCsvParser() = default;

    TCsvParser(TString&& headerRow, const char delimeter, const std::optional<TString>& nullValue,
               const std::map<TString, TType>* destinationTypes = nullptr,
               const std::map<TString, TString>* paramSources = nullptr);
    TCsvParser(TVector<TString>&& header, const char delimeter, const std::optional<TString>& nullValue,
               const std::map<TString, TType>* destinationTypes = nullptr,
               const std::map<TString, TString>* paramSources = nullptr);

    void BuildParams(TString& data, TParamsBuilder& builder, const TParseMetadata& meta) const;
    void BuildValue(TString& data, TValueBuilder& builder, const TType& type, const TParseMetadata& meta) const;
    TValue BuildList(std::vector<TString>& lines, const TString& filename,
                     std::optional<ui64> row = std::nullopt) const;
    void BuildLineType();

private:
    TVector<TString> Header;
    TString HeaderRow;
    char Delimeter;
    std::optional<TString> NullValue;
    // Types of destination table or query parameters
    // Column name -> column type
    const std::map<TString, TType>* DestinationTypes;
    const std::map<TString, TString>* ParamSources;
    // Type of a single row in resulting TValue.
    // Column order according to the header, though can have less elements than the Header
    std::optional<TType> ResultLineType = std::nullopt;
    std::optional<TType> ResultListType = std::nullopt;
    // If a value is true (header column is absent in dstTypes), skip corresponding value in input csv row
    std::vector<bool> SkipBitMap;
    // Count of columns in each struct in resulting TValue
    size_t ResultColumnCount;
    // Types of columns in a single row in resulting TValue.
    // Column order according to the header, though can have less elements than the Header
    std::vector<const TType*> ResultLineTypesSorted;
};

}
}