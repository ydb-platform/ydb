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
               const std::map<TString, TType>* paramTypes = nullptr,
               const std::map<TString, TString>* paramSources = nullptr);
    TCsvParser(TVector<TString>&& header, const char delimeter, const std::optional<TString>& nullValue,
               const std::map<TString, TType>* paramTypes = nullptr,
               const std::map<TString, TString>* paramSources = nullptr);

    void GetParams(TString&& data, TParamsBuilder& builder, const TParseMetadata& meta) const;
    void GetValue(TString&& data, TValueBuilder& builder, const TType& type, const TParseMetadata& meta) const;
    TType GetColumnsType() const;

private:
    TVector<TString> Header;
    TString HeaderRow;
    char Delimeter;
    std::optional<TString> NullValue;
    const std::map<TString, TType>* ParamTypes;
    const std::map<TString, TString>* ParamSources;
};

}
}