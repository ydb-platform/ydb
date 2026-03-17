#pragma once

#include "chdb.h"
#include "PybindWrapper.h"
#include "clickhouse_config.h"

#include <filesystem>

namespace py = pybind11;

class __attribute__((visibility("default"))) local_result_wrapper;
class __attribute__((visibility("default"))) connection_wrapper;
class __attribute__((visibility("default"))) cursor_wrapper;
class __attribute__((visibility("default"))) memoryview_wrapper;
class __attribute__((visibility("default"))) query_result;
class __attribute__((visibility("default"))) streaming_query_result;

class connection_wrapper
{
private:
    chdb_connection * conn;
    std::string db_path;
    bool is_memory_db;
    bool is_readonly;

public:
    explicit connection_wrapper(const std::string & conn_str);
    chdb_connection get_conn() { return *conn; }
    ~connection_wrapper();
    cursor_wrapper * cursor();
    void commit();
    void close();
    query_result * query(const std::string & query_str, const std::string & format = "CSV");
    streaming_query_result * send_query(const std::string & query_str, const std::string & format = "CSV");
    query_result * streaming_fetch_result(streaming_query_result * streaming_result);
    void streaming_cancel_query(streaming_query_result * streaming_result);

    // Move the private methods declarations here
    std::pair<std::string, std::map<std::string, std::string>> parse_connection_string(const std::string & conn_str);
    std::vector<std::string> build_clickhouse_args(const std::string & path, const std::map<std::string, std::string> & params);
};

class local_result_wrapper
{
private:
    chdb_result * result;
    bool keep_buf; // background server mode will handle buf in ClickHouse engine

public:
    local_result_wrapper(chdb_result * result) : result(result), keep_buf(false) { }
    local_result_wrapper(chdb_result * result, bool keep_buf) : result(result), keep_buf(keep_buf) { }
    ~local_result_wrapper()
    {
        /// keep_buf is always false
        chdb_destroy_query_result(result);
    }
    char * data()
    {
        return chdb_result_buffer(result);
    }
    size_t size()
    {
        return chdb_result_length(result);
    }
    py::bytes bytes()
    {
        if (result == nullptr)
        {
            return py::bytes();
        }
        return py::bytes(chdb_result_buffer(result), chdb_result_length(result));
    }
    py::str str()
    {
        if (result == nullptr)
        {
            return py::str();
        }
        return py::str(chdb_result_buffer(result), chdb_result_length(result));
    }
    // Query statistics
    size_t rows_read()
    {
        return chdb_result_rows_read(result);
    }
    size_t bytes_read()
    {
        return chdb_result_bytes_read(result);
    }
    size_t storage_rows_read()
    {
        return chdb_result_storage_rows_read(result);
    }
    size_t storage_bytes_read()
    {
        return chdb_result_storage_bytes_read(result);
    }
    double elapsed()
    {
        return chdb_result_elapsed(result);
    }
    bool has_error()
    {
        return chdb_result_error(result) != nullptr;
    }
    py::str error_message()
    {
        auto error_message = chdb_result_error(result);
        if (error_message)
        {
            return py::str(error_message);
        }
        return py::str();
    }
};

class query_result
{
private:
    std::shared_ptr<local_result_wrapper> result_wrapper;

public:
    query_result(chdb_result * result) : result_wrapper(std::make_shared<local_result_wrapper>(result)) { }
    query_result(chdb_result * result, bool keep_buf) : result_wrapper(std::make_shared<local_result_wrapper>(result, keep_buf)) { }
    ~query_result() = default;
    char * data() { return result_wrapper->data(); }
    py::bytes bytes() { return result_wrapper->bytes(); }
    py::str str() { return result_wrapper->str(); }
    size_t size() { return result_wrapper->size(); }
    size_t rows_read() { return result_wrapper->rows_read(); }
    size_t bytes_read() { return result_wrapper->bytes_read(); }
    size_t storage_rows_read() { return result_wrapper->storage_rows_read(); }
    size_t storage_bytes_read() { return result_wrapper->storage_bytes_read(); }
    double elapsed() { return result_wrapper->elapsed(); }
    bool has_error() { return result_wrapper->has_error(); }
    py::str error_message() { return result_wrapper->error_message(); }
    memoryview_wrapper * get_memview();
};

class streaming_query_result
{
private:
    chdb_result * result;

public:
    streaming_query_result(chdb_result * result_) : result(result_) {}
    ~streaming_query_result()
    {
        chdb_destroy_query_result(result);
    }
    bool has_error() { return chdb_result_error(result) != nullptr; }
    py::str error_message()
    {
        auto error_message = chdb_result_error(result);
        if (error_message)
        {
            return py::str(error_message);
        }
        return py::str();
    }
    chdb_result * get_result() { return result; }
};

class memoryview_wrapper
{
private:
    std::shared_ptr<local_result_wrapper> result_wrapper;

public:
    explicit memoryview_wrapper(std::shared_ptr<local_result_wrapper> result) : result_wrapper(result)
    {
        // std::cerr << "memoryview_wrapper::memoryview_wrapper" << this->result->bytes() << std::endl;
    }
    ~memoryview_wrapper() = default;

    size_t size()
    {
        if (result_wrapper == nullptr)
        {
            return 0;
        }
        return result_wrapper->size();
    }

    py::bytes bytes() { return result_wrapper->bytes(); }

    void release() { }

    py::memoryview view()
    {
        if (result_wrapper != nullptr)
        {
            return py::memoryview(py::memoryview::from_memory(result_wrapper->data(), result_wrapper->size(), true));
        }
        else
        {
            return py::memoryview(py::memoryview::from_memory(nullptr, 0, true));
        }
    }
};

class cursor_wrapper
{
private:
    connection_wrapper * conn;
    chdb_result * current_result;

    void release_result()
    {
        if (current_result)
        {
            chdb_destroy_query_result(current_result);

            current_result = nullptr;
        }
    }

public:
    explicit cursor_wrapper(connection_wrapper * connection) : conn(connection), current_result(nullptr) { }

    ~cursor_wrapper() { release_result(); }

    void execute(const std::string & query_str);

    void commit()
    {
        // do nothing
    }

    void close() { release_result(); }

    py::memoryview get_memview()
    {
        if (current_result == nullptr)
        {
            return py::memoryview(py::memoryview::from_memory(nullptr, 0, true));
        }
        return py::memoryview(py::memoryview::from_memory(chdb_result_buffer(current_result), chdb_result_length(current_result), true));
    }

    size_t data_size()
    {
        return chdb_result_length(current_result);
    }

    size_t rows_read()
    {
        return chdb_result_rows_read(current_result);
    }

    size_t bytes_read()
    {
        return chdb_result_bytes_read(current_result);
    }

    size_t storage_rows_read()
    {
        return chdb_result_storage_rows_read(current_result);
    }

    size_t storage_bytes_read()
    {
        return chdb_result_storage_bytes_read(current_result);
    }

    double elapsed()
    {
        return chdb_result_elapsed(current_result);
    }

    bool has_error()
    {
        return chdb_result_error(current_result) != nullptr;
    }

    py::str error_message()
    {
        auto error_message = chdb_result_error(current_result);
        if (error_message)
        {
            return py::str(error_message);
        }
        return py::str();
    }
};
