#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_value/value.h>

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
};

using TQueryInfoList = std::list<TQueryInfo>;

struct TWorkloadParams {
    std::string DbPath;
};

class IWorkloadQueryGenerator {
public:
    virtual ~IWorkloadQueryGenerator() {}

    virtual std::string GetDDLQueries() const = 0;
    virtual TQueryInfoList GetInitialData() = 0;
    virtual std::string GetCleanDDLQueries() const = 0;

    virtual TQueryInfoList GetWorkload(int type) = 0;

    virtual TWorkloadParams* GetParams() = 0;
};

} // namespace NYdbWorkload

