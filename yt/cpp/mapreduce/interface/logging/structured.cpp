#include "structured.h"

#include <yt/yt/core/logging/config.h>
#include <yt/yt/core/logging/file_log_writer.h>

#include <yt/yt/core/misc/fs.h>

namespace NYT::NDetail {

using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

void InitializeStructuredLogging(
    TLogManagerConfigPtr logManagerConfig,
    const TString& structuredLogPath)
{
    auto ruleConfig = New<TRuleConfig>();
    ruleConfig->MinLevel = ELogLevel::Info;
    ruleConfig->Writers.push_back(StructuredLogTypeName);
    ruleConfig->IncludeCategories = {"Structured"};

    auto writerConfig = New<TFileLogWriterConfig>();
    writerConfig->Type = StructuredLogTypeName;
    writerConfig->FileName = NFS::NormalizePathSeparators(structuredLogPath);
    writerConfig->Format = ELogFormat::Json;

    logManagerConfig->Rules.emplace_back(std::move(ruleConfig));

    EmplaceOrCrash(
        logManagerConfig->Writers,
        StructuredLogTypeName,
        ConvertTo<NYTree::IMapNodePtr>(writerConfig));
}

void RegisterStructuredLogWriterFactory()
{
    TLogManager::Get()->RegisterWriterFactory(StructuredLogTypeName, GetFileLogWriterFactory());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDetail
