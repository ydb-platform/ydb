#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <library/cpp/string_utils/csv/csv.h>

namespace NYdb {
namespace NConsoleClient {

class TCsvParser {
public:
    TCsvParser(TString&& headerRow, const char delimeter,
               const std::map<TString, TType>& paramTypes, const std::map<TString, TString>* paramSources = nullptr);

    void GetParams(TString&& data, TParamsBuilder& builder);
    void GetValue(TString&& data, TValueBuilder& builder, const TType& type);
    TType GetColumnsType();

private:
    TValue FieldToValue(TTypeParser& parser, TStringBuf token);

    TVector<TString> Header;
    TString HeaderRow;
    const char Delimeter;
    const std::map<TString, TType>& ParamTypes;
    const std::map<TString, TString>* ParamSources;
};

}
}