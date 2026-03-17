#include "chdb.h"
#include <cstddef>
#include "chdb-internal.h"
#include "LocalServer.h"
#include "QueryResult.h"

#if USE_PYTHON
#include "FormatHelper.h"
#include "PythonTableCache.h"
#endif

namespace CHDB
{

static std::shared_mutex global_connection_mutex;
static std::mutex CHDB_MUTEX;
chdb_conn * global_conn_ptr = nullptr;
std::string global_db_path;

static DB_CHDB::LocalServer * bgClickHouseLocal(int argc, char ** argv)
{
    DB_CHDB::LocalServer * app = nullptr;
    try
    {
        app = new DB_CHDB::LocalServer();
        app->setBackground(true);
        app->init(argc, argv);
        int ret = app->run();
        if (ret != 0)
        {
            auto err_msg = app->getErrorMsg();
            LOG_ERROR(&app->logger(), "Error running bgClickHouseLocal: {}", err_msg);
            delete app;
            app = nullptr;
            throw DB_CHDB::Exception(DB_CHDB::ErrorCodes::BAD_ARGUMENTS, "Error running bgClickHouseLocal: {}", err_msg);
        }
        return app;
    }
    catch (const DB_CHDB::Exception & e)
    {
        delete app;
        throw DB_CHDB::Exception(DB_CHDB::ErrorCodes::BAD_ARGUMENTS, "bgClickHouseLocal {}", DB_CHDB::getExceptionMessage(e, false));
    }
    catch (const CHDBPoco::Exception & e)
    {
        delete app;
        throw DB_CHDB::Exception(DB_CHDB::ErrorCodes::BAD_ARGUMENTS, "bgClickHouseLocal {}", e.displayText());
    }
    catch (const std::exception & e)
    {
        delete app;
        throw std::domain_error(e.what());
    }
    catch (...)
    {
        delete app;
        throw std::domain_error(DB_CHDB::getCurrentExceptionMessage(true));
    }
}

static local_result_v2 * convert2LocalResultV2(QueryResult * query_result)
{
    auto local_result = new local_result_v2();
    auto * materialized_query_result = static_cast<MaterializedQueryResult *>(query_result);

    if (!materialized_query_result)
    {
        String error = "Query processing failed";
        local_result->error_message = new char[error.size() + 1];
        std::memcpy(local_result->error_message, error.c_str(), error.size() + 1);
    }
    else if (!materialized_query_result->getError().empty())
    {
        const String & error = materialized_query_result->getError();
        local_result->error_message = new char[error.size() + 1];
        std::memcpy(local_result->error_message, error.c_str(), error.size() + 1);
    }
    else if (!materialized_query_result->result_buffer)
    {
        local_result->rows_read = materialized_query_result->rows_read;
        local_result->bytes_read = materialized_query_result->bytes_read;
        local_result->elapsed = materialized_query_result->elapsed;
    }
    else
    {
        local_result->len = materialized_query_result->result_buffer->size();
        local_result->buf = materialized_query_result->result_buffer->data();
        local_result->_vec = materialized_query_result->result_buffer.release();
        local_result->rows_read = materialized_query_result->rows_read;
        local_result->bytes_read = materialized_query_result->bytes_read;
        local_result->elapsed = materialized_query_result->elapsed;
    }

    return local_result;
}

static local_result_v2 * createErrorLocalResultV2(const String & error)
{
    auto local_result = new local_result_v2();
    local_result->error_message = new char[error.size() + 1];
    std::memcpy(local_result->error_message, error.c_str(), error.size() + 1);
    return local_result;
}

static QueryResultPtr createMaterializedLocalQueryResult(DB_CHDB::LocalServer * server, const CHDB::QueryRequestBase & req)
{
    QueryResultPtr query_result;
    const auto & materialized_request = static_cast<const CHDB::MaterializedQueryRequest &>(req);

    try
    {
        if (!server->parseQueryTextWithOutputFormat(materialized_request.query, materialized_request.format))
        {
            query_result = std::make_unique<MaterializedQueryResult>(server->getErrorMsg());
        }
        else
        {
            query_result = std::make_unique<MaterializedQueryResult>(
                ResultBuffer(server->stealQueryOutputVector()),
                server->getElapsedTime(),
                server->getProcessedRows(),
                server->getProcessedBytes(),
                server->getStorgaeRowsRead(),
                server->getStorageBytesRead());
        }
    }
    catch (const DB_CHDB::Exception & e)
    {
        query_result = std::make_unique<MaterializedQueryResult>(DB_CHDB::getExceptionMessage(e, false));
    }
    catch (...)
    {
        String error_message = "Unknown error occurred";
        query_result = std::make_unique<MaterializedQueryResult>(error_message);
    }

    return query_result;
}

static QueryResultPtr createStreamingQueryResult(DB_CHDB::LocalServer * server, const CHDB::QueryRequestBase & req)
{
    QueryResultPtr query_result;
    const auto & streaming_init_request = static_cast<const CHDB::StreamingInitRequest &>(req);

    try
    {
        if (!server->parseQueryTextWithOutputFormat(streaming_init_request.query, streaming_init_request.format))
            query_result = std::make_unique<StreamQueryResult>(server->getErrorMsg());
        else
            query_result = std::make_unique<StreamQueryResult>();
    }
    catch (const DB_CHDB::Exception& e)
    {
        query_result = std::make_unique<StreamQueryResult>(DB_CHDB::getExceptionMessage(e, false));
    }
    catch (...)
    {
        String error_message = "Unknown error occurred";
        query_result = std::make_unique<StreamQueryResult>(error_message);
    }

    return query_result;
}

static QueryResultPtr createStreamingIterateQueryResult(DB_CHDB::LocalServer * server, const CHDB::QueryRequestBase & req)
{
    QueryResultPtr query_result;
    const auto & streaming_iter_request = static_cast<const CHDB::StreamingIterateRequest &>(req);
    const auto old_processed_rows = server->getProcessedRows();
    const auto old_processed_bytes = server->getProcessedBytes();
    const auto old_storage_rows_read = server->getStorgaeRowsRead();
    const auto old_storage_bytes_read = server->getStorageBytesRead();
    const auto old_elapsed_time = server->getElapsedTime();

    try
    {
        if (!server->processStreamingQuery(streaming_iter_request.streaming_result, streaming_iter_request.is_canceled))
        {
            query_result = std::make_unique<MaterializedQueryResult>(server->getErrorMsg());
        }
        else
        {
            const auto processed_rows = server->getProcessedRows();
            const auto processed_bytes = server->getProcessedBytes();
            const auto storage_rows_read = server->getStorgaeRowsRead();
            const auto storage_bytes_read = server->getStorageBytesRead();
            const auto elapsed_time = server->getElapsedTime();
            if (processed_rows <= old_processed_rows)
                query_result =  std::make_unique<MaterializedQueryResult>(nullptr, 0.0, 0, 0, 0, 0);
            else
                query_result =  std::make_unique<MaterializedQueryResult>(
                    ResultBuffer(server->stealQueryOutputVector()),
                    elapsed_time - old_elapsed_time,
                    processed_rows - old_processed_rows,
                    processed_bytes - old_processed_bytes,
                    storage_rows_read - old_storage_rows_read,
                    storage_bytes_read - old_storage_bytes_read);
        }
    }
    catch (const DB_CHDB::Exception& e)
    {
        query_result = std::make_unique<MaterializedQueryResult>(DB_CHDB::getExceptionMessage(e, false));
    }
    catch (...)
    {
        String error_message = "Unknown error occurred";
        query_result = std::make_unique<MaterializedQueryResult>(error_message);
    }

    return query_result;
}

static std::pair<QueryResultPtr, bool> createQueryResult(DB_CHDB::LocalServer * server, const CHDB::QueryRequestBase & req)
{
    QueryResultPtr query_result;
    bool is_end = false;

    if (!req.isStreaming())
    {
        query_result = createMaterializedLocalQueryResult(server, req);
        is_end = true;
    }
    else if (!req.isIteration())
    {
        server->streaming_query_context = std::make_shared<DB_CHDB::StreamingQueryContext>();
        query_result = createStreamingQueryResult(server, req);
        is_end = !query_result->getError().empty();

        if (!is_end)
            server->streaming_query_context->streaming_result = query_result.get();
        else
            server->streaming_query_context.reset();
    }
    else
    {
        query_result = createStreamingIterateQueryResult(server, req);
        const auto & streaming_iter_request = static_cast<const CHDB::StreamingIterateRequest &>(req);
        auto materialized_query_result_ptr = static_cast<CHDB::MaterializedQueryResult *>(query_result.get());

        is_end = !materialized_query_result_ptr->getError().empty()
                    || materialized_query_result_ptr->rows_read == 0
                    || streaming_iter_request.is_canceled;
    }

    if (is_end)
    {
        server->streaming_query_context.reset();
#if USE_PYTHON
        if (auto * local_connection = static_cast<DB_CHDB::LocalConnection*>(server->connection.get()))
        {
            /// Must clean up Context objects whether the query succeeds or fails.
            /// During process exit, if LocalServer destructor triggers while cached PythonStorage
            /// objects still exist in Context, their destruction will attempt to acquire GIL.
            /// Acquiring GIL during process termination leads to immediate thread termination.
            local_connection->resetQueryContext();
        }

        CHDB::PythonTableCache::clear();
#endif
    }

    return std::make_pair(std::move(query_result), is_end);
}

static bool checkConnectionValidity(chdb_conn * conn)
{
    return conn && conn->connected && conn->queue;
}

static QueryResultPtr executeQueryRequest(
    CHDB::QueryQueue * queue,
    const char * query,
    const char * format,
    CHDB::QueryType query_type,
    void * streaming_result_ = nullptr,
    bool is_canceled = false)
{
    QueryResultPtr query_result;

    try
    {
        {
            std::unique_lock<std::mutex> lock(queue->mutex);
            // Wait until any ongoing query completes
            if (query_type == CHDB::QueryType::TYPE_STREAMING_ITER)
                queue->result_cv.wait(lock, [queue]() { return (!queue->has_query && !queue->has_result) || queue->shutdown; });
            else
                queue->result_cv.wait(lock, [queue]() { return (!queue->has_query && !queue->has_result && !queue->has_streaming_query) || queue->shutdown; });

            if (queue->shutdown)
            {
                String error_message = "connection is shutting down";
                if (query_type == CHDB::QueryType::TYPE_STREAMING_INIT)
                {
                    query_result.reset(new StreamQueryResult(error_message));
                }
                else
                {
                    query_result.reset(new MaterializedQueryResult(error_message));
                }
                return query_result;
            }

            if (query_type == CHDB::QueryType::TYPE_STREAMING_INIT)
            {
                auto streaming_req = std::make_unique<CHDB::StreamingInitRequest>();
                streaming_req->query = query;
                streaming_req->format = format;
                queue->current_query = std::move(streaming_req);
#if USE_PYTHON
                CHDB::SetCurrentFormat(format);
#endif
            }
            else if (query_type == CHDB::QueryType::TYPE_MATERIALIZED)
            {
                auto materialized_req = std::make_unique<CHDB::MaterializedQueryRequest>();
                materialized_req->query = query;
                materialized_req->format = format;
                queue->current_query = std::move(materialized_req);
#if USE_PYTHON
                CHDB::SetCurrentFormat(format);
#endif
            }
            else
            {
                auto streaming_iter_req = std::make_unique<CHDB::StreamingIterateRequest>();
                streaming_iter_req->streaming_result = streaming_result_;
                streaming_iter_req->is_canceled = is_canceled;
                queue->current_query = std::move(streaming_iter_req);
            }

            queue->has_query = true;
            queue->current_result.reset();
            queue->has_result = false;
        }
        queue->query_cv.notify_one();

        {
            std::unique_lock<std::mutex> lock(queue->mutex);
            queue->result_cv.wait(lock, [queue]() { return queue->has_result || queue->shutdown; });

            if (!queue->shutdown && queue->has_result)
            {
                query_result = std::move(queue->current_result);
                queue->has_result = false;
                queue->has_query = false;
            }
        }
        queue->result_cv.notify_all();
    }
    catch (...)
    {
        // Handle any exceptions during query processing
        String error_message = "Error occurred while processing query";
        if (query_type == CHDB::QueryType::TYPE_STREAMING_INIT)
            query_result.reset(new StreamQueryResult(error_message));
        else
            query_result.reset(new MaterializedQueryResult(error_message));
    }

    return query_result;
}

void chdbCleanupConnection()
{
    try
	{
        close_conn(&global_conn_ptr);
	}
	catch (...)
	{
	}
}

void cancelStreamQuery(DB_CHDB::LocalServer * server, void * stream_result)
{
    auto streaming_iter_req = std::make_unique<CHDB::StreamingIterateRequest>();
    streaming_iter_req->streaming_result = stream_result;
    streaming_iter_req->is_canceled = true;

    createStreamingIterateQueryResult(server, *streaming_iter_req);
}

std::unique_ptr<MaterializedQueryResult> pyEntryClickHouseLocal(int argc, char ** argv)
{
    try
    {
        std::lock_guard<std::mutex> lock(CHDB_MUTEX);
        DB_CHDB::LocalServer app;
        app.init(argc, argv);
        int ret = app.run();
        if (ret == 0)
        {
            return std::make_unique<MaterializedQueryResult>(
                ResultBuffer(app.stealQueryOutputVector()),
                app.getElapsedTime(),
                app.getProcessedRows(),
                app.getProcessedBytes(),
                0,
                0);
        } else {
            return std::make_unique<MaterializedQueryResult>(app.getErrorMsg());
        }
    }
    catch (const DB_CHDB::Exception & e)
    {
        // wrap the error message into a new std::exception
        throw std::domain_error(DB_CHDB::getExceptionMessage(e, false));
    }
    catch (const boost::program_options::error & e)
    {
        throw std::invalid_argument("Bad arguments: " + std::string(e.what()));
    }
    catch (...)
    {
        throw std::domain_error(DB_CHDB::getCurrentExceptionMessage(true));
    }
}

} // namespace CHDB

using namespace CHDB;

local_result * query_stable(int argc, char ** argv)
{
    auto query_result = pyEntryClickHouseLocal(argc, argv);
    if (!query_result->getError().empty() || query_result->result_buffer == nullptr)
        return nullptr;

    local_result * res = new local_result;
    res->len = query_result->result_buffer->size();
    res->buf = query_result->result_buffer->data();
    res->_vec = query_result->result_buffer.release();
    res->rows_read = query_result->rows_read;
    res->bytes_read = query_result->bytes_read;
    res->elapsed = query_result->elapsed;
    return res;
}

void free_result(local_result * result)
{
    if (!result)
    {
        return;
    }
    if (result->_vec)
    {
        std::vector<char> * vec = reinterpret_cast<std::vector<char> *>(result->_vec);
        delete vec;
        result->_vec = nullptr;
    }
    delete result;
}

local_result_v2 * query_stable_v2(int argc, char ** argv)
{
    // pyEntryClickHouseLocal may throw some serious exceptions, although it's not likely
    // to happen in the context of clickhouse-local. we catch them here and return an error
    local_result_v2 * res = nullptr;
    try
    {
        auto query_result = pyEntryClickHouseLocal(argc, argv);

        return convert2LocalResultV2(query_result.get());
    }
    catch (const std::exception & e)
    {
        res = new local_result_v2();
        res->error_message = new char[strlen(e.what()) + 1];
        std::strcpy(res->error_message, e.what());
    }
    catch (...)
    {
        res = new local_result_v2();
        const char * unknown_exception_msg = "Unknown exception";
        size_t len = std::strlen(unknown_exception_msg) + 1;
        res->error_message = new char[len];
        std::strcpy(res->error_message, unknown_exception_msg);
    }

    return res;
}

void free_result_v2(local_result_v2 * result)
{
    if (!result)
        return;

    delete reinterpret_cast<std::vector<char> *>(result->_vec);
    delete[] result->error_message;
    delete result;
}

chdb_conn ** connect_chdb(int argc, char ** argv)
{
    std::lock_guard<std::shared_mutex> global_lock(global_connection_mutex);

    std::string path = ":memory:"; // Default path
    for (int i = 1; i < argc; i++)
    {
        if (strncmp(argv[i], "--path=", 7) == 0)
        {
            path = argv[i] + 7;
            break;
        }
    }

    if (global_conn_ptr != nullptr)
    {
        if (path == global_db_path)
            return &global_conn_ptr;

        throw DB_CHDB::Exception(
            DB_CHDB::ErrorCodes::BAD_ARGUMENTS,
            "Another connection is already active with different path. Old path = {}, new path = {}, "
            "please close the existing connection first.",
            global_db_path,
            path);
    }

    auto * conn = new chdb_conn();
    auto * q_queue = new CHDB::QueryQueue();
    conn->queue = q_queue;

    std::mutex init_mutex;
    std::condition_variable init_cv;
    bool init_done = false;
    bool init_success = false;
    std::exception_ptr init_exception;

    // Start query processing thread
    std::thread(
        [&]()
        {
            auto * queue = static_cast<CHDB::QueryQueue *>(conn->queue);
            try
            {
                DB_CHDB::LocalServer * server = bgClickHouseLocal(argc, argv);
                conn->server = server;
                conn->connected = true;

                global_conn_ptr = conn;
                global_db_path = path;

                // Signal successful initialization
                {
                    std::lock_guard<std::mutex> init_lock(init_mutex);
                    init_success = true;
                    init_done = true;
                }
                init_cv.notify_one();

                while (true)
                {
                    {
                        std::unique_lock<std::mutex> lock(queue->mutex);
                        queue->query_cv.wait(lock, [queue]() { return queue->has_query || queue->shutdown; });

                        if (queue->shutdown)
                        {
                            try
                            {
                                server->cleanup();
                                delete server;
                            }
                            catch (...)
                            {
                                // Log error but continue shutdown
                                LOG_ERROR(&CHDBPoco::Logger::get("LocalServer"), "Error during server cleanup");
                            }
                            queue->cleanup_done = true;
                            queue->query_cv.notify_all();
                            break;
                        }

                    }

                    CHDB::QueryRequestBase & req = *(queue->current_query);
                    auto result = createQueryResult(server, req);
                    bool is_end = result.second;

                    {
                        std::lock_guard<std::mutex> lock(queue->mutex);
                        if (req.isStreaming() && !req.isIteration() && !is_end)
                            queue->has_streaming_query = true;

                        if (req.isStreaming() && req.isIteration() && is_end)
                            queue->has_streaming_query = false;

                        queue->current_result = std::move(result.first);
                        queue->has_result = true;
                        queue->has_query = false;
                    }
                    queue->result_cv.notify_all();
                }
            }
            catch (const DB_CHDB::Exception & e)
            {
                // Log the error
                LOG_ERROR(&CHDBPoco::Logger::get("LocalServer"), "Query thread terminated with error: {}", e.what());

                // Signal thread termination
                {
                    std::lock_guard<std::mutex> init_lock(init_mutex);
                    init_exception = std::current_exception();
                    init_done = true;
                    std::lock_guard<std::mutex> lock(queue->mutex);
                    queue->shutdown = true;
                    queue->cleanup_done = true;
                }
                init_cv.notify_one();
                queue->query_cv.notify_all();
                queue->result_cv.notify_all();
            }
            catch (...)
            {
                LOG_ERROR(&CHDBPoco::Logger::get("LocalServer"), "Query thread terminated with unknown error");

                {
                    std::lock_guard<std::mutex> init_lock(init_mutex);
                    init_exception = std::current_exception();
                    init_done = true;
                    std::lock_guard<std::mutex> lock(queue->mutex);
                    queue->shutdown = true;
                    queue->cleanup_done = true;
                }
                init_cv.notify_one();
                queue->query_cv.notify_all();
                queue->result_cv.notify_all();
            }
        })
        .detach();

    // Wait for initialization to complete
    {
        std::unique_lock<std::mutex> init_lock(init_mutex);
        init_cv.wait(init_lock, [&init_done]() { return init_done; });

        if (!init_success)
        {
            delete q_queue;
            delete conn;
            if (init_exception)
                std::rethrow_exception(init_exception);
            throw DB_CHDB::Exception(DB_CHDB::ErrorCodes::BAD_ARGUMENTS, "Failed to create connection");
        }
    }

    return &global_conn_ptr;
}

void close_conn(chdb_conn ** conn)
{
    std::lock_guard<std::shared_mutex> global_lock(global_connection_mutex);

    if (!conn || !*conn)
        return;

    if ((*conn)->connected)
    {
        if ((*conn)->queue)
        {
            auto * queue = static_cast<CHDB::QueryQueue *>((*conn)->queue);

            {
                std::unique_lock<std::mutex> queue_lock(queue->mutex);
                queue->shutdown = true;
                queue->query_cv.notify_all(); // Wake up query processing thread
                queue->result_cv.notify_all(); // Wake up any waiting result threads

                // Wait for server cleanup
                queue->query_cv.wait(queue_lock, [queue] { return queue->cleanup_done; });

                // Clean up current result if any
                queue->current_result.reset();
                queue->has_result = false;
            }

            delete queue;
            (*conn)->queue = nullptr;
        }

        // Mark as disconnected BEFORE deleting queue and nulling global pointer
        (*conn)->connected = false;
    }
    // Clear global pointer under lock before queue deletion
    if (*conn != global_conn_ptr)
    {
        LOG_ERROR(&CHDBPoco::Logger::get("LocalServer"), "Connection mismatch during close_conn");
    }

    delete *conn;
    *conn = nullptr;
}

struct local_result_v2 * query_conn(chdb_conn * conn, const char * query, const char * format)
{
    // Add connection validity check under global lock
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!checkConnectionValidity(conn))
        return createErrorLocalResultV2("Invalid or closed connection");

    auto * queue = static_cast<CHDB::QueryQueue *>(conn->queue);
    auto query_result = executeQueryRequest(queue, query, format, CHDB::QueryType::TYPE_MATERIALIZED);

    return convert2LocalResultV2(query_result.get());
}

chdb_streaming_result * query_conn_streaming(chdb_conn * conn, const char * query, const char * format)
{
    // Add connection validity check under global lock
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!checkConnectionValidity(conn))
    {
        auto * result = new StreamQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_streaming_result *>(result);
    }

    auto * queue = static_cast<CHDB::QueryQueue *>(conn->queue);
    auto query_result = executeQueryRequest(queue, query, format, CHDB::QueryType::TYPE_STREAMING_INIT);

    if (!query_result)
    {
        auto * result = new StreamQueryResult("Query processing failed");
        return reinterpret_cast<chdb_streaming_result *>(result);
    }

    return reinterpret_cast<chdb_streaming_result *>(query_result.release());
}

const char * chdb_streaming_result_error(chdb_streaming_result * result)
{
    if (!result)
	    return nullptr;

    auto stream_query_result = reinterpret_cast<StreamQueryResult *>(result);

    const auto & error_message = stream_query_result->getError();
    if (!error_message.empty())
        return error_message.c_str();

    return nullptr;
}

local_result_v2 * chdb_streaming_fetch_result(chdb_conn * conn, chdb_streaming_result * result)
{
    // Add connection validity check under global lock
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!checkConnectionValidity(conn))
        return createErrorLocalResultV2("Invalid or closed connection");

    auto * queue = static_cast<CHDB::QueryQueue *>(conn->queue);
    auto query_result = executeQueryRequest(queue, nullptr, nullptr, CHDB::QueryType::TYPE_STREAMING_ITER, result);

    return convert2LocalResultV2(query_result.get());
}

void chdb_streaming_cancel_query(chdb_conn * conn, chdb_streaming_result * result)
{
    // Add connection validity check under global lock
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!checkConnectionValidity(conn))
        return;

    auto * queue = static_cast<CHDB::QueryQueue *>(conn->queue);
    auto query_result = executeQueryRequest(queue, nullptr, nullptr, CHDB::QueryType::TYPE_STREAMING_ITER, result, true);
    query_result.reset();
}

void chdb_destroy_result(chdb_streaming_result * result)
{
    if (!result)
	    return;

    auto stream_query_result = reinterpret_cast<StreamQueryResult *>(result);
    delete stream_query_result;
}

// ============== New API Implementation ==============

chdb_connection * chdb_connect(int argc, char ** argv)
{
    auto connection = connect_chdb(argc, argv);

    return reinterpret_cast<chdb_connection *>(connection);
}

void chdb_close_conn(chdb_connection * conn)
{
    if (!conn || !*conn)
        return;

    auto connection = reinterpret_cast<chdb_conn **>(conn);

    close_conn(connection);
}

chdb_result * chdb_query(chdb_connection conn, const char * query, const char * format)
{
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!conn)
    {
        auto * result = new MaterializedQueryResult("Unexepected null connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * result = new MaterializedQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto * queue = static_cast<CHDB::QueryQueue *>(connection->queue);
    auto query_result = executeQueryRequest(queue, query, format, CHDB::QueryType::TYPE_MATERIALIZED);

    return reinterpret_cast<chdb_result *>(query_result.release());

}

chdb_result * chdb_query_cmdline(int argc, char ** argv)
{
    MaterializedQueryResult * result = nullptr;
    try
    {
        auto query_result = pyEntryClickHouseLocal(argc, argv);

        return reinterpret_cast<chdb_result *>(query_result.release());
    }
    catch (const std::exception & e)
    {
        result = new MaterializedQueryResult(e.what());
    }
    catch (...)
    {
        result = new MaterializedQueryResult("Unknown exception");
    }

    return reinterpret_cast<chdb_result *>(result);
}

chdb_result * chdb_stream_query(chdb_connection conn, const char * query, const char * format)
{
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!conn)
    {
        auto * result = new StreamQueryResult("Unexepected null connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * result = new StreamQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(result);
    }

    auto * queue = static_cast<CHDB::QueryQueue *>(connection->queue);
    auto query_result = executeQueryRequest(queue, query, format, CHDB::QueryType::TYPE_STREAMING_INIT);

    if (!query_result)
    {
        auto * result = new StreamQueryResult("Query processing failed");
        return reinterpret_cast<chdb_result *>(result);
    }

    return reinterpret_cast<chdb_result *>(query_result.release());
}

chdb_result * chdb_stream_fetch_result(chdb_connection conn, chdb_result * result)
{
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!conn)
    {
        auto * query_result = new MaterializedQueryResult("Unexepected null connection");
        return reinterpret_cast<chdb_result *>(query_result);
    }

    if (!result)
    {
        auto * query_result = new MaterializedQueryResult("Unexepected null result");
        return reinterpret_cast<chdb_result *>(query_result);
    }


    auto connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
    {
        auto * query_result = new MaterializedQueryResult("Invalid or closed connection");
        return reinterpret_cast<chdb_result *>(query_result);
    }

    auto * queue = static_cast<CHDB::QueryQueue *>(connection->queue);
    auto query_result = executeQueryRequest(queue, nullptr, nullptr, CHDB::QueryType::TYPE_STREAMING_ITER, result);

    return reinterpret_cast<chdb_result *>(query_result.release());
}

void chdb_stream_cancel_query(chdb_connection conn, chdb_result * result)
{
    std::shared_lock<std::shared_mutex> global_lock(global_connection_mutex);

    if (!result || !conn)
        return;

    auto connection = reinterpret_cast<chdb_conn *>(conn);
    if (!checkConnectionValidity(connection))
        return;

    auto * queue = static_cast<CHDB::QueryQueue *>(connection->queue);
    auto query_result = executeQueryRequest(queue, nullptr, nullptr, CHDB::QueryType::TYPE_STREAMING_ITER, result, true);
    query_result.reset();
}

void chdb_destroy_query_result(chdb_result * result)
{
    if (!result)
        return;

    auto query_result = reinterpret_cast<QueryResult *>(result);
    delete query_result;
}

char * chdb_result_buffer(chdb_result * result)
{
    if (!result)
        return nullptr;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->result_buffer ? materialized_result->result_buffer->data() : nullptr;
    }

    return nullptr;
}

size_t chdb_result_length(chdb_result * result)
{
    if (!result)
        return 0;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->result_buffer ? materialized_result->result_buffer->size() : 0;
    }

    return 0;
}

double chdb_result_elapsed(chdb_result * result)
{
    if (!result)
        return 0.0;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->elapsed;
    }

    return 0.0;
}

uint64_t chdb_result_rows_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->rows_read;
    }

    return 0;
}

uint64_t chdb_result_bytes_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->bytes_read;
    }

    return 0;
}

uint64_t chdb_result_storage_rows_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->storage_rows_read;
    }

    return 0;
}

uint64_t chdb_result_storage_bytes_read(chdb_result * result)
{
    if (!result)
        return 0;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getType() == QueryResultType::RESULT_TYPE_MATERIALIZED)
    {
        auto materialized_result = reinterpret_cast<MaterializedQueryResult *>(result);
        return materialized_result->storage_bytes_read;
    }

    return 0;
}

const char * chdb_result_error(chdb_result * result)
{
    if (!result)
        return nullptr;

    auto query_result = reinterpret_cast<QueryResult *>(result);

    if (query_result->getError().empty())
        return nullptr;

    return query_result->getError().c_str();
}
