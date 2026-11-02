#include "command_memory_estimate.h"

#include "config.h"

#include <yt/yt/client/api/config.h>

#include <yt/yt/client/table_client/config.h>

#include <yt/yt/core/ytree/node.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NDriver {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TConfig>
TIntrusivePtr<TConfig> GetEffectiveConfig(
    const TIntrusivePtr<TConfig>& config,
    const IMapNodePtr& parameters,
    TStringBuf parameterName)
{
    auto patch = parameters ? parameters->FindChild(parameterName) : INodePtr();
    return patch ? UpdateYsonStruct(config, patch) : config;
}

} // namespace

std::optional<i64> EstimateHeavyCommandMemoryUsage(
    const std::string& commandName,
    const TDriverConfigPtr& config,
    const IMapNodePtr& parameters)
{
    if (commandName == "read_file" || commandName == "read_file_partition") {
        return GetEffectiveConfig(config->FileReader, parameters, "file_reader")->MaxBufferSize;
    }
    if (commandName == "read_table" || commandName == "read_blob_table") {
        return GetEffectiveConfig(config->TableReader, parameters, "table_reader")->MaxBufferSize;
    }
    if (commandName == "read_table_partition") {
        // The partition reader ignores the driver config and uses the default one.
        static const auto DefaultTableReaderConfig = New<NTableClient::TTableReaderConfig>();
        return DefaultTableReaderConfig->MaxBufferSize;
    }
    if (commandName == "write_file") {
        auto writerConfig = GetEffectiveConfig(config->FileWriter, parameters, "file_writer");
        return writerConfig->BlockSize + writerConfig->EncodeWindowSize;
    }
    if (commandName == "write_file_fragment") {
        // The file fragment writer ignores the driver config and uses the default one.
        static const auto DefaultFileWriterConfig = New<NApi::TFileWriterConfig>();
        return DefaultFileWriterConfig->BlockSize + DefaultFileWriterConfig->EncodeWindowSize;
    }
    if (commandName == "write_table" || commandName == "write_table_fragment") {
        auto writerConfig = GetEffectiveConfig(config->TableWriter, parameters, "table_writer");
        return writerConfig->MaxBufferSize + writerConfig->EncodeWindowSize;
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDriver
