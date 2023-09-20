#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/public/api/protos/ydb_query.pb.h>

namespace NYdb {
namespace NScripting {

class TExecuteYqlResult : public TStatus {
public:
    TExecuteYqlResult(TStatus&& status, TVector<TResultSet>&& resultSets,
        const TMaybe<NTable::TQueryStats>& queryStats);

    const TVector<TResultSet>& GetResultSets() const;
    TResultSet GetResultSet(size_t resultIndex) const;

    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    const TMaybe<NTable::TQueryStats>& GetStats() const;

private:
    TVector<TResultSet> ResultSets_;
    TMaybe<NTable::TQueryStats> QueryStats_;
};

class TYqlPartialResult {
public:
    TYqlPartialResult(ui32 resultSetIndex, TResultSet&& resultSet)
        : ResultSetIndex_(resultSetIndex)
        , ResultSet_(std::move(resultSet))
    {}

    ui32 GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return ResultSet_; }

private:
    ui32 ResultSetIndex_;
    TResultSet ResultSet_;
};

class TYqlResultPart : public TStreamPartStatus {
public:
    bool HasPartialResult() const { return PartialResult_.Defined(); }
    const TYqlPartialResult& GetPartialResult() const { return *PartialResult_; }

    bool HasQueryStats() const { return QueryStats_.Defined(); }
    const NTable::TQueryStats& GetQueryStats() const { return *QueryStats_; }
    NTable::TQueryStats ExtractQueryStats() { return std::move(*QueryStats_); }

    TYqlResultPart(TStatus&& status)
        : TStreamPartStatus(std::move(status))
    {}

    TYqlResultPart(TStatus&& status, const TMaybe<NTable::TQueryStats> &queryStats)
        : TStreamPartStatus(std::move(status))
        , QueryStats_(queryStats)
    {}

    TYqlResultPart(TStatus&& status, TYqlPartialResult&& partialResult, const TMaybe<NTable::TQueryStats> &queryStats)
        : TStreamPartStatus(std::move(status))
        , PartialResult_(std::move(partialResult))
        , QueryStats_(queryStats)
    {}

private:
    TMaybe<TYqlPartialResult> PartialResult_;
    TMaybe<NTable::TQueryStats> QueryStats_;
};

using TAsyncYqlResultPart = NThreading::TFuture<TYqlResultPart>;

class TYqlResultPartIterator : public TStatus {
    friend class TScriptingClient;
public:
    TAsyncYqlResultPart ReadNext();
    class TReaderImpl;
private:
    TYqlResultPartIterator(
        std::shared_ptr<TReaderImpl> impl,
        TPlainStatus&& status
    );
    std::shared_ptr<TReaderImpl> ReaderImpl_;
};

class TExplainYqlResult : public TStatus {
public:
    TExplainYqlResult(TStatus&& status, const ::google::protobuf::Map<TString, Ydb::Type>&& types, TString&& plan);

    std::map<TString, TType> GetParameterTypes() const;
    const TString& GetPlan() const;

private:
    ::google::protobuf::Map<TString, Ydb::Type> ParameterTypes_;
    TString Plan_;
};

using TAsyncExecuteYqlResult = NThreading::TFuture<TExecuteYqlResult>;
using TAsyncYqlResultPartIterator = NThreading::TFuture<TYqlResultPartIterator>;
using TAsyncExplainYqlResult = NThreading::TFuture<TExplainYqlResult>;

////////////////////////////////////////////////////////////////////////////////

struct TExecuteYqlRequestSettings : public TOperationRequestSettings<TExecuteYqlRequestSettings> {
    FLUENT_SETTING_DEFAULT(Ydb::Query::Syntax, Syntax, Ydb::Query::SYNTAX_YQL_V1);
    FLUENT_SETTING_DEFAULT(NTable::ECollectQueryStatsMode, CollectQueryStats, NTable::ECollectQueryStatsMode::None);
};

enum class ExplainYqlRequestMode {
    // Parse = 1,
    Validate = 2,
    Plan = 3,
};

struct TExplainYqlRequestSettings : public TOperationRequestSettings<TExplainYqlRequestSettings> {
    FLUENT_SETTING_DEFAULT(ExplainYqlRequestMode, Mode, ExplainYqlRequestMode::Validate);
};

////////////////////////////////////////////////////////////////////////////////

class TScriptingClient {
    class TImpl;

public:
    TScriptingClient(const TDriver& driver, const TCommonClientSettings& settings = TCommonClientSettings());

    //! Returns new params builder
    TParamsBuilder GetParamsBuilder();

    TAsyncExecuteYqlResult ExecuteYqlScript(const TString& script,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncExecuteYqlResult ExecuteYqlScript(const TString& script, const TParams& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncExecuteYqlResult ExecuteYqlScript(const TString& script, TParams&& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const TString& script,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const TString& script, const TParams& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncYqlResultPartIterator StreamExecuteYqlScript(const TString& script, TParams&& params,
        const TExecuteYqlRequestSettings& settings = TExecuteYqlRequestSettings());

    TAsyncExplainYqlResult ExplainYqlScript(const TString& script,
        const TExplainYqlRequestSettings& settings = TExplainYqlRequestSettings());

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NScripting
} // namespace NYdb
