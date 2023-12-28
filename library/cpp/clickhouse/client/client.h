#pragma once

#include "query.h"
#include "exceptions.h"

#include "columns/array.h"
#include "columns/date.h"
#include "columns/nullable.h"
#include "columns/numeric.h"
#include "columns/string.h"
#include "columns/tuple.h"

#include <library/cpp/openssl/io/stream.h>

#include <util/generic/string.h>

namespace NClickHouse {
    /// Метод сжатия
    enum class ECompressionMethod {
        None = -1,
        LZ4 = 1,
    };

    struct TClientOptions {
#define DECLARE_FIELD(name, type, default)                \
    type name{default};                                   \
    inline TClientOptions& Set##name(const type& value) { \
        name = value;                                     \
        return *this;                                     \
    }

        /// Hostname of the server.
        DECLARE_FIELD(Host, TString, TString());
        /// Service port.
        DECLARE_FIELD(Port, int, 9000);

        /// Default database.
        DECLARE_FIELD(DefaultDatabase, TString, "default");
        /// User name.
        DECLARE_FIELD(User, TString, "default");
        /// Access password.
        DECLARE_FIELD(Password, TString, TString());

        /// By default all exceptions received during query execution will be
        /// passed to OnException handler.  Set rethrow_exceptions to true to
        /// enable throwing exceptions with standard c++ exception mechanism.
        DECLARE_FIELD(RethrowExceptions, bool, true);

        /// Ping server every time before execute any query.
        DECLARE_FIELD(PingBeforeQuery, bool, false);
        /// Count of retry to send request to server.
        DECLARE_FIELD(SendRetries, int, 1);
        /// Amount of time to wait before next retry.
        DECLARE_FIELD(RetryTimeout, TDuration, TDuration::Seconds(5));
        /// Define timeout for establishing a connection to server.
        DECLARE_FIELD(ConnectTimeout, TDuration, TDuration::Seconds(5));
        /// Define timeout for any operations.
        DECLARE_FIELD(RequestTimeout, TDuration, TDuration::Zero());

        /// Compression method.
        DECLARE_FIELD(CompressionMethod, ECompressionMethod, ECompressionMethod::None);

        /// Use SSL encryption
        DECLARE_FIELD(UseSsl, bool, false);
        /// SSL Options
        DECLARE_FIELD(SslOptions, TOpenSslClientIO::TOptions, TOpenSslClientIO::TOptions());

#undef DECLARE_FIELD
    };

    /**
 *
 */
    class TClient {
    public:
        TClient(const TClientOptions& opts);
        ~TClient();

        /// Intends for execute arbitrary queries.
        void Execute(const TQuery& query);

        /// Intends for execute select queries.  Data will be returned with
        /// one or more call of \p cb.
        void Select(const TString& query, TSelectCallback cb, const TString& query_id = "");

        /// Alias for Execute.
        void Select(const TQuery& query);

        /// Intends for insert block of data into a table \p table_name.
        void Insert(const TString& table_name, const TBlock& block, const TString& query_id = "");

        /// Ping server for aliveness.
        void Ping();

        /// Reset connection with initial params.
        void ResetConnection();

    private:
        TClientOptions Options_;

        class TImpl;
        THolder<TImpl> Impl_;
    };

}
