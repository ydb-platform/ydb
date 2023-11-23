#include "config.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/misc/cache_config.h>

namespace NYT::NDriver {

////////////////////////////////////////////////////////////////////////////////

void TDriverConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_reader", &TThis::FileReader)
        .DefaultNew();
    registrar.Parameter("file_writer", &TThis::FileWriter)
        .DefaultNew();
    registrar.Parameter("table_reader", &TThis::TableReader)
        .DefaultNew();
    registrar.Parameter("table_writer", &TThis::TableWriter)
        .DefaultNew();
    registrar.Parameter("journal_reader", &TThis::JournalReader)
        .DefaultNew();
    registrar.Parameter("journal_writer", &TThis::JournalWriter)
        .DefaultNew();
    registrar.Parameter("fetcher", &TThis::Fetcher)
        .DefaultNew();
    registrar.Parameter("chunk_fragment_reader", &TThis::ChunkFragmentReader)
        .DefaultNew();

    registrar.Parameter("read_buffer_row_count", &TThis::ReadBufferRowCount)
        .Default(10'000);
    registrar.Parameter("read_buffer_size", &TThis::ReadBufferSize)
        .Default(1_MB);
    registrar.Parameter("write_buffer_size", &TThis::WriteBufferSize)
        .Default(1_MB);

    registrar.Parameter("client_cache", &TThis::ClientCache)
        .DefaultNew();

    registrar.Parameter("api_version", &TThis::ApiVersion)
        .Default(ApiVersion3)
        .GreaterThanOrEqual(ApiVersion3)
        .LessThanOrEqual(ApiVersion4);

    registrar.Parameter("token", &TThis::Token)
        .Optional();

    registrar.Parameter("proxy_discovery_cache", &TThis::ProxyDiscoveryCache)
        .DefaultNew();

    registrar.Parameter("enable_internal_commands", &TThis::EnableInternalCommands)
        .Default(false);

    registrar.Parameter("expect_structured_input_in_structured_batch_commands", &TThis::ExpectStructuredInputInStructuredBatchCommands)
        .Default(true);

    registrar.Preprocessor([] (TThis* config) {
        config->ClientCache->Capacity = 1024_KB;
        config->ProxyDiscoveryCache->RefreshTime = TDuration::Seconds(15);
        config->ProxyDiscoveryCache->ExpireAfterSuccessfulUpdateTime = TDuration::Seconds(15);
        config->ProxyDiscoveryCache->ExpireAfterFailedUpdateTime = TDuration::Seconds(15);
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->ApiVersion != ApiVersion3 && config->ApiVersion != ApiVersion4) {
            THROW_ERROR_EXCEPTION("Unsupported API version %v",
                config->ApiVersion);
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
