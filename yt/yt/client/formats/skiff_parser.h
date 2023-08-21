#pragma once

#include "public.h"
#include "config.h"

#include <library/cpp/skiff/skiff.h>

namespace NYT::NFormats {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IParser> CreateParserForSkiff(
    std::shared_ptr<NSkiff::TSkiffSchema> skiffSchema,
    NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForSkiff(
    std::shared_ptr<NSkiff::TSkiffSchema> skiffSchema,
    const NTableClient::TTableSchemaPtr& tableSchema,
    NTableClient::IValueConsumer* consumer);

std::unique_ptr<IParser> CreateParserForSkiff(
    NTableClient::IValueConsumer* consumer,
    const std::vector<std::shared_ptr<NSkiff::TSkiffSchema>>& skiffSchemas,
    const TSkiffFormatConfigPtr& config,
    int tableIndex);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFormats
