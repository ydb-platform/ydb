#pragma once

#include "public.h"

#include <yt/yt/core/json/public.h>
#include <yt/yt/core/json/config.h>

#include <yt/yt/core/ytree/public.h>
#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TRotationPolicyConfig
    : public NYTree::TYsonStruct
{
public:
    //! Upper limit on the total size of rotated log files.
    i64 MaxTotalSizeToKeep;
    //! Upper limit on the number of rotated log files.
    i64 MaxSegmentCountToKeep;
    //! Limit on the size of current log file, which triggers rotation.
    std::optional<i64> MaxSegmentSize;
    //! Period of regular log file rotation.
    std::optional<TDuration> RotationPeriod;

    REGISTER_YSON_STRUCT(TRotationPolicyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRotationPolicyConfig)

////////////////////////////////////////////////////////////////////////////////

class TFileLogWriterConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr const TStringBuf Type = "file";

    TString FileName;
    bool UseTimestampSuffix;
    bool EnableCompression;
    ECompressionMethod CompressionMethod;
    int CompressionLevel;

    TRotationPolicyConfigPtr RotationPolicy;

    REGISTER_YSON_STRUCT(TFileLogWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TFileLogWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TStderrLogWriterConfig
    : public NYTree::TYsonStruct
{
public:
    static constexpr TStringBuf Type = "stderr";

    REGISTER_YSON_STRUCT(TStderrLogWriterConfig);

    static void Register(TRegistrar)
    { }
};

DEFINE_REFCOUNTED_TYPE(TStderrLogWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogWriterConfig
    : public NYTree::TYsonStruct
{
public:
    TString Type;

    ELogFormat Format;

    std::optional<i64> RateLimit;

    //! Common formatter options.
    std::optional<bool> EnableSystemMessages;

    //! Plain text formatter options.
    bool EnableSourceLocation;

    //! Structured formatter options.
    bool EnableSystemFields;
    THashMap<TString, NYTree::INodePtr> CommonFields;
    NJson::TJsonFormatConfigPtr JsonFormat;

    bool AreSystemMessagesEnabled() const;

    //! Constructs a full config by combining parameters from this one and #typedConfig.
    template <class TTypedConfigPtr>
    NYTree::IMapNodePtr BuildFullConfig(const TTypedConfigPtr& typedConfig);

    REGISTER_YSON_STRUCT(TLogWriterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogWriterConfig)

////////////////////////////////////////////////////////////////////////////////

class TRuleConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<THashSet<TString>> IncludeCategories;
    THashSet<TString> ExcludeCategories;

    ELogLevel MinLevel;
    ELogLevel MaxLevel;

    std::optional<ELogFamily> Family;

    std::vector<TString> Writers;

    bool IsApplicable(TStringBuf category, ELogFamily family) const;
    bool IsApplicable(TStringBuf category, ELogLevel level, ELogFamily family) const;

    REGISTER_YSON_STRUCT(TRuleConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRuleConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogManagerConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<TDuration> FlushPeriod;
    std::optional<TDuration> WatchPeriod;
    std::optional<TDuration> CheckSpacePeriod;
    TDuration RotationCheckPeriod;

    i64 MinDiskSpace;

    int HighBacklogWatermark;
    int LowBacklogWatermark;

    TDuration ShutdownGraceTimeout;

    std::vector<TRuleConfigPtr> Rules;
    THashMap<TString, NYTree::IMapNodePtr> Writers;
    std::vector<TString> SuppressedMessages;
    THashMap<TString, i64> CategoryRateLimits;

    TDuration RequestSuppressionTimeout;

    bool EnableAnchorProfiling;
    double MinLoggedMessageRateToProfile;

    bool AbortOnAlert;

    double StructuredValidationSamplingRate;

    int CompressionThreadCount;

    TLogManagerConfigPtr ApplyDynamic(const TLogManagerDynamicConfigPtr& dynamicConfig) const;

    static TLogManagerConfigPtr CreateStderrLogger(ELogLevel logLevel);
    static TLogManagerConfigPtr CreateLogFile(const TString& path);
    static TLogManagerConfigPtr CreateDefault();
    static TLogManagerConfigPtr CreateQuiet();
    static TLogManagerConfigPtr CreateSilent();
    //! Create logging config a-la YT server config: #directory/#componentName{,.debug,.error}.log.
    //! Also allows adding structured logs. For example, pair ("RpcProxyStructuredMain", "main") would
    //! make structured messages with RpcProxyStructuredMain category go to #directory/#componentName.yson.main.log.
    static TLogManagerConfigPtr CreateYTServer(
        const TString& componentName,
        const TString& directory = ".",
        const THashMap<TString, TString>& structuredCategoryToWriterName = {});
    static TLogManagerConfigPtr CreateFromFile(const TString& file, const NYPath::TYPath& path = "");
    static TLogManagerConfigPtr CreateFromNode(NYTree::INodePtr node, const NYPath::TYPath& path = "");
    static TLogManagerConfigPtr TryCreateFromEnv();

    //! Applies #updater to each writer config in #Writers.
    //! If null is returned then the writer is removed, otherwise it is replaced.
    void UpdateWriters(const std::function<NYTree::IMapNodePtr(const NYTree::IMapNodePtr&)> updater);

    REGISTER_YSON_STRUCT(TLogManagerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogManagerConfig)

////////////////////////////////////////////////////////////////////////////////

class TLogManagerDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    std::optional<i64> MinDiskSpace;

    std::optional<int> HighBacklogWatermark;
    std::optional<int> LowBacklogWatermark;

    std::optional<std::vector<TRuleConfigPtr>> Rules;
    std::optional<std::vector<TString>> SuppressedMessages;
    std::optional<THashMap<TString, i64>> CategoryRateLimits;

    std::optional<TDuration> RequestSuppressionTimeout;

    std::optional<bool> EnableAnchorProfiling;
    std::optional<double> MinLoggedMessageRateToProfile;

    std::optional<bool> AbortOnAlert;

    std::optional<double> StructuredValidationSamplingRate;

    std::optional<int> CompressionThreadCount;

    REGISTER_YSON_STRUCT(TLogManagerDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TLogManagerDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define CONFIG_INL_H_
#include "config-inl.h"
#undef CONFIG_INL_H_
