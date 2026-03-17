#pragma once

#include <memory> 
#include <vector>

#include <base/types.h>
#include <utility>

namespace CHDB {

enum class QueryResultType : uint8_t
{
    RESULT_TYPE_MATERIALIZED = 0,
    RESULT_TYPE_STREAMING = 1,
    RESULT_TYPE_NONE = 2
};

class QueryResult {
public:
    explicit QueryResult(QueryResultType type, String error_message_ = "")
        : result_type(type), error_message(std::move(error_message_))
    {}

    virtual ~QueryResult() = default;

    QueryResultType getType() const { return result_type; }
    const String & getError() const { return error_message; }

protected:
    QueryResultType result_type;
    String error_message;
};

class StreamQueryResult : public QueryResult {
public:
    explicit StreamQueryResult(String error_message_ = "")
        : QueryResult(QueryResultType::RESULT_TYPE_STREAMING, std::move(error_message_))
    {}
};

using ResultBuffer = std::unique_ptr<std::vector<char>>;

class MaterializedQueryResult : public QueryResult {
public:
    explicit MaterializedQueryResult(
        ResultBuffer result_buffer_,
        double elapsed_,
        uint64_t rows_read_,
        uint64_t bytes_read_,
        uint64_t storage_rows_read_,
        uint64_t storage_bytes_read_)
        : QueryResult(QueryResultType::RESULT_TYPE_MATERIALIZED),
        result_buffer(std::move(result_buffer_)),
        elapsed(elapsed_),
        rows_read(rows_read_),
        bytes_read(bytes_read_),
        storage_rows_read(storage_rows_read_),
        storage_bytes_read(storage_bytes_read_)
    {}

    explicit MaterializedQueryResult(String error_message_)
        : QueryResult(QueryResultType::RESULT_TYPE_MATERIALIZED, std::move(error_message_))
    {}

    String string()
    {
        return String(result_buffer->begin(), result_buffer->end());
    }

public:
    ResultBuffer result_buffer;
    double elapsed;
    uint64_t rows_read;
    uint64_t bytes_read;
    uint64_t storage_rows_read;
    uint64_t storage_bytes_read;
};

using QueryResultPtr = std::unique_ptr<QueryResult>;
using MaterializedQueryResultPtr = std::unique_ptr<MaterializedQueryResult>;
using StreamQueryResultPtr = std::unique_ptr<StreamQueryResult>;

} // namespace CHDB
