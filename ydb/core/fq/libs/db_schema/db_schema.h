#pragma once

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <util/string/builder.h>
#include <util/generic/hash_set.h>

namespace NFq {

class TSqlQueryBuilder {
public:
    TSqlQueryBuilder(const TString& tablePrefix, const TString& queryName = "Unknown query name");

    TSqlQueryBuilder& PushPk();
    TSqlQueryBuilder& PopPk();

    TSqlQueryBuilder& AddPk(const TString& name, const NYdb::TValue& value);

    TSqlQueryBuilder& AddPk(const TString& name, const TString& value) {
        return AddPk(name, NYdb::TValueBuilder().String(value).Build());
    }

    TSqlQueryBuilder& AddValue(const TString& name, const NYdb::TValue& value);

    TSqlQueryBuilder& AddString(const TString& name, const TString& value) {
        return AddValue(name, NYdb::TValueBuilder().String(value).Build());
    }

    TSqlQueryBuilder& AddUint32(const TString& name, ui32 value) {
        return AddValue(name, NYdb::TValueBuilder().Uint32(value).Build());
    }

    TSqlQueryBuilder& AddInt32(const TString& name, i32 value) {
        return AddValue(name, NYdb::TValueBuilder().Int32(value).Build());
    }

    TSqlQueryBuilder& AddUint64(const TString& name, ui64 value) {
        return AddValue(name, NYdb::TValueBuilder().Uint64(value).Build());
    }

    TSqlQueryBuilder& AddInt64(const TString& name, i64 value) {
        return AddValue(name, NYdb::TValueBuilder().Int64(value).Build());
    }

    TSqlQueryBuilder& AddTimestamp(const TString& name, TInstant value) {
        return AddValue(name, NYdb::TValueBuilder().Timestamp(value).Build());
    }

    TSqlQueryBuilder& AddBool(const TString& name, bool value) {
        return AddValue(name, NYdb::TValueBuilder().Bool(value).Build());
    }

    TSqlQueryBuilder& AddDouble(const TString& name, double value) {
        return AddValue(name, NYdb::TValueBuilder().Double(value).Build());
    }

    TSqlQueryBuilder& AddValue(const TString& name, const NYdb::TValue& value, const TString& pred);

    TSqlQueryBuilder& AddValue(const TString& name, const TString& value, const TString& pred) {
        return AddValue(name, NYdb::TValueBuilder().String(value).Build(), pred);
    }

    TSqlQueryBuilder& AddValue(const TString& name, ui32 value, const TString& pred) {
        return AddValue(name, NYdb::TValueBuilder().Uint32(value).Build(), pred);
    }

    TSqlQueryBuilder& AddValue(const TString& name, ui64 value, const TString& pred) {
        return AddValue(name, NYdb::TValueBuilder().Uint64(value).Build(), pred);
    }

    TSqlQueryBuilder& AddValue(const TString& name, TInstant value, const TString& pred) {
        return AddValue(name, NYdb::TValueBuilder().Timestamp(value).Build(), pred);
    }

    TSqlQueryBuilder& MaybeAddValue(const TString& name, const TMaybe<NYdb::TValue>& value);

    template<typename T>
    TSqlQueryBuilder& MaybeAddValue(const TString& name, const TMaybe<T>& value) {
        if (value) {
            AddValue(name, *value);
        }
        return *this;
    }

    TSqlQueryBuilder& AddColNames(TVector<TString>&& colNames);

    TSqlQueryBuilder& AddText(const TString& sql, const TString& tableName = "");
    TSqlQueryBuilder& AddBeforeQuery(const TString& sql, const TString& tableName = "");

    TSqlQueryBuilder& Upsert(const TString& tableName);
    TSqlQueryBuilder& Update(const TString& tableName);
    TSqlQueryBuilder& Insert(const TString& tableName);
    TSqlQueryBuilder& Delete(const TString& tableName, int limit = 0);
    TSqlQueryBuilder& Get(const TString& tableName, int limit = 0);
    TSqlQueryBuilder& GetCount(const TString& tableName);
    TSqlQueryBuilder& GetLastDeleted();
    TSqlQueryBuilder& End();

    struct TResult {
        TString Sql;
        NYdb::TParams Params;
    };

    TResult Build();

    const TString& Prefix() const {
        return TablePrefix;
    }

private:
    void ConstructWhereFilter(TStringBuilder& text);

    const TString TablePrefix;
    const TString QueryName;
    TString Table;
    int Limit = 0;
    TString Op;

    TVector<TVector<TString>> PkStack;
    TVector<TString> Pk;

    TVector<std::tuple<TString, TString, TString>> Fields;
    TVector<std::pair<TString, TString>> ParametersOrdered;
    THashMap<TString, TString> Parameters;
    NYdb::TParamsBuilder ParamsBuilder;
    TStringBuilder BeforeQuery;
    TStringBuilder Text;
    int Counter = 0;

    TVector<TString> ColNames;
};

} // namespace NFq
