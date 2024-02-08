#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <library/cpp/getopt/last_getopt.h>

#include <list>
#include <map>
#include <string>

namespace NYdbWorkload {

struct TQueryInfo {
    TQueryInfo()
        : Query("")
        , Params(NYdb::TParamsBuilder().Build())
    {}

    TQueryInfo(const std::string& query, const NYdb::TParams&& params, bool useBulk = false)
        : Query(query)
        , Params(std::move(params))
        , UseReadRows(useBulk)
    {}

    std::string Query;
    NYdb::TParams Params;
    bool UseReadRows = false;
    TString TablePath;
    std::optional<NYdb::TValue> KeyToRead;
    std::optional<NYdb::NTable::TAlterTableSettings> AlterTable;

    std::optional<std::function<void(NYdb::NTable::TReadRowsResult)>> ReadRowsResultCallback;
    std::optional<std::function<void(NYdb::NTable::TDataQueryResult)>> DataQueryResultCallback;
    std::optional<std::function<void(NYdb::NQuery::TExecuteQueryResult)>> GenericQueryResultCallback;
};

using TQueryInfoList = std::list<TQueryInfo>;

class IBulkDataGenerator {
public:
    using TPtr = std::shared_ptr<IBulkDataGenerator>;
    virtual ~IBulkDataGenerator() = default;
    IBulkDataGenerator(const std::string& table)
        : Table(table)
    {}

    std::string GetTable() const {
        return Table;
    }

    virtual TMaybe<NYdb::TValue> GenerateDataPortion() = 0;

private:
    std::string Table;
};

using TBulkDataGeneratorList = std::list<std::shared_ptr<IBulkDataGenerator>>;

class IWorkloadQueryGenerator {
public:
    struct TWorkloadType {
        explicit TWorkloadType(int type, const TString& commandName, const TString& description)
            : Type(type)
            , CommandName(commandName)
            , Description(description)
        {}
        int Type = 0;
        TString CommandName;
        TString Description;
    };
public:
    virtual ~IWorkloadQueryGenerator() = default;
    virtual std::string GetDDLQueries() const = 0;
    virtual TQueryInfoList GetInitialData() = 0;
    virtual TBulkDataGeneratorList GetBulkInitialData() const {
        return {};
    }
    virtual TVector<std::string> GetCleanPaths() const = 0;
    virtual TQueryInfoList GetWorkload(int type) = 0;
    virtual TVector<TWorkloadType> GetSupportedWorkloadTypes() const = 0;
    std::string GetCleanDDLQueries() const {
        std::string cleanQuery;
        for (const auto& table : GetCleanPaths()) {
            cleanQuery += "DROP TABLE `" + table + "`;";
        }
        return cleanQuery;
    };
};

class TWorkloadParams {
public:
    enum class ECommandType {
        Init /* "init" */,
        Run /* "run" */,
        Clean  /* "clean" */
    };
    virtual ~TWorkloadParams() = default;
    virtual void ConfigureOpts(NLastGetopt::TOpts& /*opts*/, const ECommandType /*commandType*/, int /*workloadType*/) {
    };
    virtual THolder<IWorkloadQueryGenerator> CreateGenerator() const = 0;
    virtual TString GetWorkloadName() const = 0;

public:
    std::string DbPath;
};

template<class TP>
class TWorkloadQueryGeneratorBase: public IWorkloadQueryGenerator {
public:
    using TParams = TP;
    TWorkloadQueryGeneratorBase(const TParams* params)
        : Params(*params)
    {}
protected:
    const TParams& Params;
};

} // namespace NYdbWorkload

