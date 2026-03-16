#pragma once

#include "clickhouse_config.h"

#if USE_SSL

#include <string>
#include <filesystem>
#include <list>
#include <unordered_map>
#include <mutex>

#include <CHDBPoco/Logger.h>
#include <CHDBPoco/Util/AbstractConfiguration.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <CHDBPoco/Crypto/RSAKey.h>
#include <CHDBPoco/Crypto/X509Certificate.h>
#include <Common/MultiVersion.h>
#include <Common/Logger.h>


namespace DB_CHDB
{

/// The CertificateReloader singleton performs 2 functions:
/// 1. Dynamic reloading of TLS key-pair when requested by server:
///   Server config reloader notifies CertificateReloader when the config changes.
///   On changed config, CertificateReloader reloads certs from disk.
/// 2. Implement `SSL_CTX_set_cert_cb` to set certificate for a new connection:
///   OpenSSL invokes a callback to setup a connection.
class CertificateReloader
{
public:
    using stat_t = struct stat;

    struct Data
    {
        CHDBPoco::Crypto::X509Certificate::List certs_chain;
        CHDBPoco::Crypto::EVPPKey key;

        Data(std::string cert_path, std::string key_path, std::string pass_phrase);
    };

    struct File
    {
        const char * description;
        explicit File(const char * description_) : description(description_) {}

        std::string path;
        std::filesystem::file_time_type modification_time;

        bool changeIfModified(std::string new_path, LoggerPtr logger);
    };

    struct MultiData
    {
        SSL_CTX * ctx = nullptr;
        MultiVersion<Data> data;
        bool init_was_not_made = true;

        File cert_file{"certificate"};
        File key_file{"key"};

        explicit MultiData(SSL_CTX * ctx_) : ctx(ctx_) {}
    };

    /// Singleton
    CertificateReloader(CertificateReloader const &) = delete;
    void operator=(CertificateReloader const &) = delete;
    static CertificateReloader & instance()
    {
        static CertificateReloader instance;
        return instance;
    }

    /// Handle configuration reload for default path
    void tryLoad(const CHDBPoco::Util::AbstractConfiguration & config);

    /// Handle configuration reload
    void tryLoad(const CHDBPoco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix);

    /// Handle configuration reload for all contexts
    void tryReloadAll(const CHDBPoco::Util::AbstractConfiguration & config);

    /// A callback for OpenSSL
    int setCertificate(SSL * ssl, const MultiData * pdata);

private:
    CertificateReloader() = default;

    /// Initialize the callback and perform the initial cert loading
    void init(MultiData * pdata) TSA_REQUIRES(data_mutex);

    /// Unsafe implementation
    void tryLoadImpl(const CHDBPoco::Util::AbstractConfiguration & config, SSL_CTX * ctx, const std::string & prefix) TSA_REQUIRES(data_mutex);

    std::list<MultiData>::iterator findOrInsert(SSL_CTX * ctx, const std::string & prefix) TSA_REQUIRES(data_mutex);

    LoggerPtr log = getLogger("CertificateReloader");

    std::list<MultiData> data TSA_GUARDED_BY(data_mutex);
    std::unordered_map<std::string, std::list<MultiData>::iterator> data_index TSA_GUARDED_BY(data_mutex);
    mutable std::mutex data_mutex;
};

}

#endif
