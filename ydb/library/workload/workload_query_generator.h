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

protected:
    static bool validateDbPath(const std::string& path) {
        for (size_t i = 0; i < path.size(); ++i) {
            char c = path[i];
            if (!std::isalnum(c) && c != '/' && c != '_' && c != '-') {
                return false;
            }
        }
        return true;
    }

};

} // namespace NYdbWorkload

