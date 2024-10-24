#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_query/client.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>
#include <ydb/library/accessor/accessor.h>
#include <library/cpp/getopt/last_getopt.h>

#include <list>
#include <string>
#include <vector>

#define WORKLOAD_QUERY_GENERATOR_INTERFACE_VERSION 3

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
    std::string ExpectedResult;
    NYdb::TParams Params;
    bool UseReadRows = false;
    bool UseStaleRO = false;
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
    IBulkDataGenerator(const std::string& name, ui64 size)
        : Name(name)
        , Size(size)
    {}

    class TDataPortion: public TAtomicRefCount<TDataPortion>, TMoveOnly {
    public:
        struct TCsv {
            TCsv(TString&& data, const TString& formatString = TString())
                : Data(std::move(data))
                , FormatString(formatString)
            {}
            TString Data;
            TString FormatString;
        };

        struct TArrow {
            TArrow(TString&& data, TString&& schema)
                : Data(std::move(data))
                , Schema(schema)
            {}
            TString Data;
            TString Schema;
        };

        using TDataType = std::variant<NYdb::TValue, TCsv, TArrow>;

        template<class T>
        TDataPortion(const TString& table, T&& data, ui64 size)
            : Table(table)
            , Size(size)
            , Data(std::move(data))
        {}

        virtual ~TDataPortion() = default;
        virtual void SetSendResult(const NYdb::TStatus& status) {
            Y_UNUSED(status);
        }
        TDataType& MutableData() {
            return Data;
        }
        YDB_READONLY_DEF(TString, Table);
        YDB_READONLY(ui64, Size, 0);
    private:
        TDataType Data;
    };

    using TDataPortionPtr = TIntrusivePtr<TDataPortion>;
    using TDataPortions = TVector<TDataPortionPtr>;

    virtual TDataPortions GenerateDataPortion() = 0;
    YDB_READONLY_DEF(std::string, Name);
    YDB_READONLY(ui64, Size, 0);
};

using TBulkDataGeneratorList = std::vector<std::shared_ptr<IBulkDataGenerator>>;

class TWorkloadDataInitializer {
public:
    using TPtr = std::shared_ptr<TWorkloadDataInitializer>;
    using TList = std::vector<TPtr>;

    TWorkloadDataInitializer(const TString& name, const TString& description)
        : Name(name)
        , Description(description)
    {}

    virtual ~TWorkloadDataInitializer() = default;

    virtual void ConfigureOpts(NLastGetopt::TOpts& opts) = 0;
    virtual TBulkDataGeneratorList GetBulkInitialData() = 0;
    YDB_READONLY_DEF(TString, Name);
    YDB_READONLY_DEF(TString, Description);
};

class IWorkloadQueryGenerator {
public:
    struct TWorkloadType {
        enum class EKind {
            Workload,
            Benchmark
        };
        explicit TWorkloadType(int type, const TString& commandName, const TString& description, EKind kind = EKind::Workload)
            : Type(type)
            , CommandName(commandName)
            , Description(description)
            , Kind(kind)
        {}
        int Type = 0;
        TString CommandName;
        TString Description;
        EKind Kind;
    };
public:
    virtual ~IWorkloadQueryGenerator() = default;
    virtual std::string GetDDLQueries() const = 0;
    virtual TQueryInfoList GetInitialData() = 0;
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
        Clean  /* "clean" */,
        Root  /* "root" */,
        Import /* "import"*/
    };
    virtual ~TWorkloadParams() = default;
    virtual void ConfigureOpts(NLastGetopt::TOpts& /*opts*/, const ECommandType /*commandType*/, int /*workloadType*/) {
    };
    virtual THolder<IWorkloadQueryGenerator> CreateGenerator() const = 0;
    virtual TWorkloadDataInitializer::TList CreateDataInitializers() const {
        return {};
    }
    virtual TString GetWorkloadName() const = 0;

public:
    ui64 BulkSize = 10000;
    std::string DbPath;
};

template<class TP>
class TWorkloadQueryGeneratorBase: public IWorkloadQueryGenerator {
public:
    using TParams = TP;
    TWorkloadQueryGeneratorBase(const TParams* params)
        : Params(*params)
    {}

    const TParams& GetParams() const {
        return Params;
    }

protected:
    const TParams& Params;
};

} // namespace NYdbWorkload

