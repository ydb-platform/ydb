#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NBackup {

class TQueryBuilder {
    std::vector<TColumn> Columns;
    const TString Query;
    TValueBuilder Value;

    TString BuildQuery(const TString& path);
    void AddMemberFromString(TTypeParser& type, const TString& name, TStringBuf ss);
    void AddPrimitiveMember(EPrimitiveType type, TStringBuf buf);
    static void CheckNull(const TString& name, TStringBuf buf);
    static void BuildType(TTypeParser& typeParser, TTypeBuilder& typeBuilder, const TString& name);
    static TType GetType(TTypeParser& typeParser, const TString& name);

public:
    TQueryBuilder(const TString& path, std::vector<TColumn> columns)
        : Columns(std::move(columns))
        , Query(BuildQuery(path))
    {}

    void Begin();
    void AddLine(TStringBuf line);
    TValue EndAndGetResultingValue();
    TParams EndAndGetResultingParams();
    TString GetQueryString() const;
};

} // NYdb::NBackup
