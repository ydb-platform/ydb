#pragma once

#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <list>
#include <map>
#include <string>

namespace NYdbWorkload {

struct TQueryInfo {
    TQueryInfo()
        : Query("")
        , Params(NYdb::TParamsBuilder().Build())
    {}

    TQueryInfo(const std::string& query, const NYdb::TParams&& params)
        : Query(query)
        , Params(std::move(params))
    {}

    std::string Query;
    NYdb::TParams Params;
};

using TQueryInfoList = std::list<TQueryInfo>;

class IWorkloadQueryGenerator {
public:
    virtual ~IWorkloadQueryGenerator() {}

    virtual std::string GetDDLQueries() const = 0;
    virtual TQueryInfoList GetInitialData() = 0;
    virtual std::string GetCleanDDLQueries() const = 0;

    virtual TQueryInfoList GetWorkload(int type) = 0;
};

struct TWorkloadParams {
    std::string DbPath;
};

} // namespace NYdbWorkload

