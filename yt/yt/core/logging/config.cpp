#include "config.h"
#include "private.h"
#include "file_log_writer.h"
#include "stream_log_writer.h"

#include <yt/yt/core/misc/fs.h>

#include <util/string/vector.h>
#include <util/system/env.h>

#include <library/cpp/iterator/functools.h>

namespace NYT::NLogging {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<ELogLevel> GetLogLevelFromEnv()
{
    auto logLevelStr = GetEnv("YT_LOG_LEVEL");
    if (logLevelStr.empty()) {
        return {};
    }

    // This handles most typical casings like "DEBUG", "debug", "Debug".
    logLevelStr.to_title();
    return TEnumTraits<ELogLevel>::FromString(logLevelStr);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

void TLogWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("type", &TThis::Type);
    registrar.Parameter("format", &TThis::Format)
        .Alias("accepted_message_format")
        .Default(ELogFormat::PlainText);
    registrar.Parameter("rate_limit", &TThis::RateLimit)
        .Default();
    registrar.Parameter("common_fields", &TThis::CommonFields)
        .Default();
    registrar.Parameter("enable_system_messages", &TThis::EnableSystemMessages)
        .Alias("enable_control_messages")
        .Default();
    registrar.Parameter("system_message_family", &TThis::SystemMessageFamily)
        .Default();
    registrar.Parameter("enable_source_location", &TThis::EnableSourceLocation)
        .Default(false);
    registrar.Parameter("enable_system_fields", &TThis::EnableSystemFields)
        .Alias("enable_instant")
        .Default(true);
    registrar.Parameter("enable_host_field", &TThis::EnableHostField)
        .Default(false);
    registrar.Parameter("json_format", &TThis::JsonFormat)
        .Default();

    registrar.Postprocessor([] (TThis* config) {
        // COMPAT(max42).
        if (config->Format == ELogFormat::Structured) {
            config->Format = ELogFormat::Json;
        }
    });
}

bool TLogWriterConfig::AreSystemMessagesEnabled() const
{
    return EnableSystemMessages.value_or(Format == ELogFormat::PlainText);
}

ELogFamily TLogWriterConfig::GetSystemMessageFamily() const
{
    return SystemMessageFamily.value_or(Format == ELogFormat::PlainText ? ELogFamily::PlainText : ELogFamily::Structured);
}

////////////////////////////////////////////////////////////////////////////////

void TRotationPolicyConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("max_total_size_to_keep", &TThis::MaxTotalSizeToKeep)
        .Default(std::numeric_limits<i64>::max())
        .GreaterThan(0);
    registrar.Parameter("max_segment_count_to_keep", &TThis::MaxSegmentCountToKeep)
        .Default(std::numeric_limits<i64>::max())
        .GreaterThan(0);
    registrar.Parameter("max_segment_size", &TThis::MaxSegmentSize)
        .Default(std::nullopt)
        .GreaterThan(0);
    registrar.Parameter("rotation_period", &TThis::RotationPeriod)
        .Default(std::nullopt)
        .GreaterThan(TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

void TFileLogWriterConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("file_name", &TThis::FileName);
    registrar.Parameter("use_timestamp_suffix", &TThis::UseTimestampSuffix)
        .Default(false);
    registrar.Parameter("enable_compression", &TThis::EnableCompression)
        .Default(false);
    registrar.Parameter("compression_method", &TThis::CompressionMethod)
        .Default(ECompressionMethod::Gzip);
    registrar.Parameter("compression_level", &TThis::CompressionLevel)
        .Default(6);
    registrar.Parameter("rotation_policy", &TThis::RotationPolicy)
        .DefaultNew();

    registrar.Preprocessor([] (TThis* config) {
        config->Type = WriterType;
    });

    registrar.Postprocessor([] (TThis* config) {
        if (config->CompressionMethod == ECompressionMethod::Gzip && (config->CompressionLevel < 0 || config->CompressionLevel > 9)) {
            THROW_ERROR_EXCEPTION("Invalid \"compression_level\" attribute for \"gzip\" compression method");
        } else if (config->CompressionMethod == ECompressionMethod::Zstd && config->CompressionLevel > 22) {
            THROW_ERROR_EXCEPTION("Invalid \"compression_level\" attribute for \"zstd\" compression method");
        }
    });
}

////////////////////////////////////////////////////////////////////////////////

void TStderrLogWriterConfig::Register(TRegistrar registrar)
{
    registrar.Preprocessor([] (TThis* config) {
        config->Type = WriterType;
    });
}

////////////////////////////////////////////////////////////////////////////////

void TRuleConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("include_categories", &TThis::IncludeCategories)
        .Default();
    registrar.Parameter("exclude_categories", &TThis::ExcludeCategories)
        .Default();
    registrar.Parameter("min_level", &TThis::MinLevel)
        .Default(ELogLevel::Minimum);
    registrar.Parameter("max_level", &TThis::MaxLevel)
        .Default(ELogLevel::Maximum);
    registrar.Parameter("family", &TThis::Family)
        .Alias("message_format")
        .Default();
    registrar.Parameter("writers", &TThis::Writers)
        .NonEmpty();
}

bool TRuleConfig::IsApplicable(TStringBuf category, ELogFamily family) const
{
    return
        (!Family || *Family == family) &&
        ExcludeCategories.find(category) == ExcludeCategories.end() &&
        (!IncludeCategories || IncludeCategories->find(category) != IncludeCategories->end());
}

bool TRuleConfig::IsApplicable(TStringBuf category, ELogLevel level, ELogFamily family) const
{
    return
        IsApplicable(category, family) &&
        MinLevel <= level && level <= MaxLevel;
}

////////////////////////////////////////////////////////////////////////////////

void TLogManagerConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("flush_period", &TThis::FlushPeriod)
        .Default();
    registrar.Parameter("watch_period", &TThis::WatchPeriod)
        .Default();
    registrar.Parameter("check_space_period", &TThis::CheckSpacePeriod)
        .Default();
    registrar.Parameter("rotation_check_period", &TThis::RotationCheckPeriod)
        .Default(TDuration::Seconds(5))
        .GreaterThanOrEqual(TDuration::Seconds(1));
    registrar.Parameter("min_disk_space", &TThis::MinDiskSpace)
        .GreaterThanOrEqual(0)
        .Default(5_GB);
    registrar.Parameter("high_backlog_watermark", &TThis::HighBacklogWatermark)
        .GreaterThanOrEqual(0)
        .Default(10'000'000);
    registrar.Parameter("low_backlog_watermark", &TThis::LowBacklogWatermark)
        .GreaterThanOrEqual(0)
        .Default(1'000'000);
    registrar.Parameter("shutdown_grace_timeout", &TThis::ShutdownGraceTimeout)
        .Default(TDuration::Seconds(1));
    registrar.Parameter("shutdown_busy_timeout", &TThis::ShutdownGraceTimeout)
        .Default(TDuration::Zero());

    registrar.Parameter("writers", &TThis::Writers);
    registrar.Parameter("rules", &TThis::Rules);
    registrar.Parameter("suppressed_messages", &TThis::SuppressedMessages)
        .Default();
    registrar.Parameter("category_rate_limits", &TThis::CategoryRateLimits)
        .Default();

    registrar.Parameter("request_suppression_timeout", &TThis::RequestSuppressionTimeout)
        .Alias("trace_suppression_timeout")
        .Default(TDuration::Zero());

    registrar.Parameter("enable_anchor_profiling", &TThis::EnableAnchorProfiling)
        .Default(false);
    registrar.Parameter("min_logged_message_rate_to_profile", &TThis::MinLoggedMessageRateToProfile)
        .Default(1.0);

    registrar.Parameter("abort_on_alert", &TThis::AbortOnAlert)
        .Default(false);

    registrar.Parameter("structured_validation_sampling_rate", &TThis::StructuredValidationSamplingRate)
        .Default(0.01)
        .InRange(0.0, 1.0);

    registrar.Parameter("compression_thread_count", &TThis::CompressionThreadCount)
        .Default(1);
}

TLogManagerConfigPtr TLogManagerConfig::ApplyDynamic(const TLogManagerDynamicConfigPtr& dynamicConfig) const
{
    auto mergedConfig = New<TLogManagerConfig>();
    mergedConfig->FlushPeriod = FlushPeriod;
    mergedConfig->WatchPeriod = WatchPeriod;
    mergedConfig->CheckSpacePeriod = CheckSpacePeriod;
    mergedConfig->MinDiskSpace = dynamicConfig->MinDiskSpace.value_or(MinDiskSpace);
    mergedConfig->HighBacklogWatermark = dynamicConfig->HighBacklogWatermark.value_or(HighBacklogWatermark);
    mergedConfig->LowBacklogWatermark = dynamicConfig->LowBacklogWatermark.value_or(LowBacklogWatermark);
    mergedConfig->ShutdownGraceTimeout = ShutdownGraceTimeout;
    mergedConfig->Rules = CloneYsonStructs(dynamicConfig->Rules.value_or(Rules));
    mergedConfig->Writers = CloneYsonStructs(Writers);
    mergedConfig->SuppressedMessages = dynamicConfig->SuppressedMessages.value_or(SuppressedMessages);
    mergedConfig->CategoryRateLimits = dynamicConfig->CategoryRateLimits.value_or(CategoryRateLimits);
    mergedConfig->RequestSuppressionTimeout = dynamicConfig->RequestSuppressionTimeout.value_or(RequestSuppressionTimeout);
    mergedConfig->EnableAnchorProfiling = dynamicConfig->EnableAnchorProfiling.value_or(EnableAnchorProfiling);
    mergedConfig->MinLoggedMessageRateToProfile = dynamicConfig->MinLoggedMessageRateToProfile.value_or(MinLoggedMessageRateToProfile);
    mergedConfig->AbortOnAlert = dynamicConfig->AbortOnAlert.value_or(AbortOnAlert);
    mergedConfig->StructuredValidationSamplingRate = dynamicConfig->StructuredValidationSamplingRate.value_or(StructuredValidationSamplingRate);
    mergedConfig->CompressionThreadCount = dynamicConfig->CompressionThreadCount.value_or(CompressionThreadCount);
    mergedConfig->Postprocess();
    return mergedConfig;
}

TLogManagerConfigPtr TLogManagerConfig::CreateLogFile(const TString& path)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = ELogLevel::Trace;
    rule->Writers.push_back(TString(DefaultFileWriterName));

    auto fileWriterConfig = New<TFileLogWriterConfig>();
    fileWriterConfig->FileName = NFS::NormalizePathSeparators(path);

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    EmplaceOrCrash(config->Writers, DefaultFileWriterName, ConvertTo<IMapNodePtr>(fileWriterConfig));
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100'000;
    config->LowBacklogWatermark = 100'000;

    config->Postprocess();
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateStderrLogger(ELogLevel logLevel)
{
    auto rule = New<TRuleConfig>();
    rule->MinLevel = logLevel;
    rule->Writers.push_back(TString(DefaultStderrWriterName));

    auto stderrWriterConfig = New<TStderrLogWriterConfig>();

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(rule);
    config->Writers.emplace(DefaultStderrWriterName, ConvertTo<IMapNodePtr>(stderrWriterConfig));
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 100'000;
    config->LowBacklogWatermark = 100'000;

    config->Postprocess();
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateDefault()
{
    return CreateStderrLogger(DefaultStderrMinLevel);
}

TLogManagerConfigPtr TLogManagerConfig::CreateQuiet()
{
    return CreateStderrLogger(DefaultStderrQuietLevel);
}

TLogManagerConfigPtr TLogManagerConfig::CreateSilent()
{
    auto config = New<TLogManagerConfig>();
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = 0;
    config->LowBacklogWatermark = 0;

    config->Postprocess();
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateYTServer(
    const TString& componentName,
    const TString& directory,
    const THashMap<TString, TString>& structuredCategoryToWriterName)
{
    auto config = New<TLogManagerConfig>();

    auto logLevel = GetLogLevelFromEnv().value_or(ELogLevel::Debug);

    static const std::vector logLevels{
        ELogLevel::Trace,
        ELogLevel::Debug,
        ELogLevel::Info,
        ELogLevel::Error
    };

    for (auto currentLogLevel : logLevels) {
        if (currentLogLevel < logLevel) {
            continue;
        }

        auto rule = New<TRuleConfig>();
        // Due to historical reasons, error logs usually contain warning messages.
        rule->MinLevel = currentLogLevel == ELogLevel::Error ? ELogLevel::Warning : currentLogLevel;
        rule->Writers.push_back(ToString(currentLogLevel));

        auto fileWriterConfig = New<TFileLogWriterConfig>();
        auto fileName = Format(
            "%v/%v%v.log",
            directory,
            componentName,
            currentLogLevel == ELogLevel::Info ? "" : "." + FormatEnum(currentLogLevel));
        fileWriterConfig->FileName = NFS::NormalizePathSeparators(fileName);

        config->Rules.push_back(rule);
        config->Writers.emplace(ToString(currentLogLevel), ConvertTo<IMapNodePtr>(fileWriterConfig));
    }

    for (const auto& [category, writerName] : structuredCategoryToWriterName) {
        auto rule = New<TRuleConfig>();
        rule->MinLevel = ELogLevel::Info;
        rule->Writers.emplace_back(writerName);
        rule->Family = ELogFamily::Structured;
        rule->IncludeCategories = {category};

        auto fileWriterConfig = New<TFileLogWriterConfig>();
        fileWriterConfig->Format = ELogFormat::Yson;
        auto fileName = Format(
            "%v/%v.yson.%v.log",
            directory,
            componentName,
            writerName);
        fileWriterConfig->FileName = NFS::NormalizePathSeparators(fileName);

        config->Rules.push_back(rule);
        config->Writers.emplace(writerName, ConvertTo<IMapNodePtr>(fileWriterConfig));
    }

    config->Postprocess();
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::CreateFromFile(const TString& file, const NYPath::TYPath& path)
{
    NYTree::INodePtr node;
    {
        TIFStream stream(file);
        node = NYTree::ConvertToNode(&stream);
    }
    return CreateFromNode(std::move(node), path);
}

TLogManagerConfigPtr TLogManagerConfig::CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path)
{
    auto config = New<TLogManagerConfig>();
    config->Load(node, true, true, path);
    return config;
}

TLogManagerConfigPtr TLogManagerConfig::TryCreateFromEnv()
{
    auto logLevel = GetLogLevelFromEnv();
    if (!logLevel) {
        return nullptr;
    }

    auto logExcludeCategoriesStr = GetEnv("YT_LOG_EXCLUDE_CATEGORIES");
    auto logIncludeCategoriesStr = GetEnv("YT_LOG_INCLUDE_CATEGORIES");

    auto rule = New<TRuleConfig>();
    rule->Writers.push_back(TString(DefaultStderrWriterName));
    rule->MinLevel = *logLevel;

    std::vector<TString> logExcludeCategories;
    if (logExcludeCategoriesStr) {
        logExcludeCategories = SplitString(logExcludeCategoriesStr, ",");
    }

    for (const auto& excludeCategory : logExcludeCategories) {
        rule->ExcludeCategories.insert(excludeCategory);
    }

    std::vector<TString> logIncludeCategories;
    if (logIncludeCategoriesStr) {
        logIncludeCategories = SplitString(logIncludeCategoriesStr, ",");
    }

    if (!logIncludeCategories.empty()) {
        rule->IncludeCategories.emplace();
        for (const auto& includeCategory : logIncludeCategories) {
            rule->IncludeCategories->insert(includeCategory);
        }
    }

    auto stderrWriterConfig = New<TStderrLogWriterConfig>();

    auto config = New<TLogManagerConfig>();
    config->Rules.push_back(std::move(rule));
    config->MinDiskSpace = 0;
    config->HighBacklogWatermark = std::numeric_limits<int>::max();
    config->LowBacklogWatermark = 0;
    EmplaceOrCrash(config->Writers, DefaultStderrWriterName, ConvertTo<IMapNodePtr>(stderrWriterConfig));

    config->Postprocess();
    return config;
}

void TLogManagerConfig::UpdateWriters(
    const std::function<IMapNodePtr(const IMapNodePtr&)> updater)
{
    THashMap<TString, IMapNodePtr> updatedWriters;
    for (const auto& [name, configNode] : Writers) {
        if (auto updatedConfigNode = updater(configNode)) {
            EmplaceOrCrash(updatedWriters, name, updatedConfigNode);
        }
    }
    Writers = std::move(updatedWriters);
}

////////////////////////////////////////////////////////////////////////////////

void TLogManagerDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("min_disk_space", &TThis::MinDiskSpace)
        .Optional();
    registrar.Parameter("high_backlog_watermark", &TThis::HighBacklogWatermark)
        .Optional();
    registrar.Parameter("low_backlog_watermark", &TThis::LowBacklogWatermark)
        .Optional();

    registrar.Parameter("rules", &TThis::Rules)
        .Optional();
    registrar.Parameter("suppressed_messages", &TThis::SuppressedMessages)
        .Optional();
    registrar.Parameter("category_rate_limits", &TThis::CategoryRateLimits)
        .Optional();

    registrar.Parameter("request_suppression_timeout", &TThis::RequestSuppressionTimeout)
        .Optional();

    registrar.Parameter("enable_anchor_profiling", &TThis::EnableAnchorProfiling)
        .Optional();
    registrar.Parameter("min_logged_message_rate_to_profile", &TThis::MinLoggedMessageRateToProfile)
        .Optional();

    registrar.Parameter("abort_on_alert", &TThis::AbortOnAlert)
        .Optional();
    registrar.Parameter("structured_validation_sampling_rate", &TThis::StructuredValidationSamplingRate)
        .Optional();

    registrar.Parameter("compression_thread_count", &TThis::CompressionThreadCount)
        .Optional();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
