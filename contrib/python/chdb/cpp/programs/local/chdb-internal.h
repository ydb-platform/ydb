#pragma once

#include "chdb.h"
#include "QueryResult.h"

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <boost/iostreams/detail/select.hpp>

namespace DB_CHDB
{
    class LocalServer;
}

namespace CHDB
{

struct QueryRequestBase
{
    virtual ~QueryRequestBase() = default;
    virtual bool isStreaming() const = 0;
    virtual bool isIteration() const { return false; }
};

struct MaterializedQueryRequest : QueryRequestBase
{
    std::string query;
    std::string format;

    bool isStreaming() const override { return false; }
};

struct StreamingInitRequest : QueryRequestBase
{
    std::string query;
    std::string format;

    bool isStreaming() const override { return true; }
};

struct StreamingIterateRequest : QueryRequestBase
{
    void * streaming_result = nullptr;
    bool is_canceled = false;

    bool isStreaming() const override { return true; }
    bool isIteration() const override { return true; }
};

enum class QueryType : uint8_t
{
    TYPE_MATERIALIZED = 0,
    TYPE_STREAMING_INIT = 1,
    TYPE_STREAMING_ITER = 2
};

struct QueryQueue
{
    std::mutex mutex;
    std::condition_variable query_cv; // For query submission
    std::condition_variable result_cv; // For query result retrieval
    std::unique_ptr<QueryRequestBase> current_query;
    QueryResultPtr current_result;
    bool has_result = false;
    bool has_query = false;
    bool has_streaming_query = false;
    bool shutdown = false;
    bool cleanup_done = false;
};

std::unique_ptr<MaterializedQueryResult> pyEntryClickHouseLocal(int argc, char ** argv);

void chdbCleanupConnection();

void cancelStreamQuery(DB_CHDB::LocalServer * server, void * stream_result);

}
