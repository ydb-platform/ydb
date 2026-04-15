#pragma once
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/utils/yql_panic.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/typetraits.h>
#include <util/generic/yexception.h>
#include <util/system/defaults.h>

#include <variant>

namespace {

[[noreturn]] void AbortUnimplemented(const char* methodName) {
    Y_ABORT(
        "Method %s is not implemented in IClient descendant",
        methodName);
}

} // anonymous namespace

namespace NPq::NConfigurationManager {

// Statuses as in ydb/public/api/protos/ydb_status_codes.proto
enum class EStatus {
    STATUS_CODE_UNSPECIFIED,
    SUCCESS,
    BAD_REQUEST,
    UNAUTHORIZED,
    INTERNAL_ERROR,
    ABORTED,
    UNAVAILABLE,
    OVERLOADED,
    SCHEME_ERROR,
    GENERIC_ERROR,
    TIMEOUT,
    BAD_SESSION,
    PRECONDITION_FAILED,
    ALREADY_EXISTS,
    NOT_FOUND,
    SESSION_EXPIRED,
    CANCELLED,
    UNDETERMINED,
    UNSUPPORTED,
    SESSION_BUSY
};

class TException : public yexception {
public:
    explicit TException(EStatus status)
        : Status(status)
    {
    }

    EStatus GetStatus() const {
        return Status;
    }

private:
    EStatus Status = EStatus::STATUS_CODE_UNSPECIFIED;
};

struct TObjectDescriptionBase {
    explicit TObjectDescriptionBase(const TString& path)
        : Path(path)
    {
    }

    TString Path;
    THashMap<TString, TString> Metadata; // Arbitrary key-value pairs.
};

struct TAccountDescription : public TObjectDescriptionBase {
    using TObjectDescriptionBase::TObjectDescriptionBase;

    // TODO: If you need other properties, add them.
};

struct TPathDescription : public TObjectDescriptionBase {
    using TObjectDescriptionBase::TObjectDescriptionBase;

    // TODO: If you need other properties, add them.
};

struct TTopicDescription : public TObjectDescriptionBase {
    using TObjectDescriptionBase::TObjectDescriptionBase;

    size_t PartitionsCount = 0;

    // TODO: If you need other properties, add them.
};

struct TConsumerDescription : public TObjectDescriptionBase {
    using TObjectDescriptionBase::TObjectDescriptionBase;

    // TODO: If you need other properties, add them.
};

struct TDescribePathResult {
    using TDescription = std::variant<TAccountDescription, TPathDescription, TTopicDescription, TConsumerDescription>;

    const TDescription& GetDescriptionVariant() const {
        return Description;
    }

    TDescription& GetDescriptionVariant() {
        return Description;
    }

    template<class T>
    const T& Get() const {
        return std::get<T>(Description);
    }

    const TAccountDescription& GetAccountDescription() const {
        return Get<TAccountDescription>();
    }

    const TPathDescription& GetPathDescription() const {
        return Get<TPathDescription>();
    }

    const TTopicDescription& GetTopicDescription() const {
        return Get<TTopicDescription>();
    }

    const TConsumerDescription& GetConsumerDescription() const {
        return Get<TConsumerDescription>();
    }

    bool IsAccount() const {
        return std::holds_alternative<TAccountDescription>(Description);
    }

    bool IsPath() const {
        return std::holds_alternative<TPathDescription>(Description);
    }

    bool IsTopic() const {
        return std::holds_alternative<TTopicDescription>(Description);
    }

    bool IsConsumer() const {
        return std::holds_alternative<TConsumerDescription>(Description);
    }

    template<class TResultType, class... T>
    static TDescribePathResult Make(T&&... params) {
        return {std::in_place_type_t<TResultType>(), std::forward<T>(params)...};
    }

private:
    template<class... T>
    TDescribePathResult(T&&... params)
        : Description(std::forward<T>(params)...)
    {
    }

private:
    TDescription Description;
};

using TAsyncDescribePathResult = NThreading::TFuture<TDescribePathResult>;

class TOperation {
public:
    TOperation(TString id, bool ready, EStatus status)
        : Id(std::move(id))
        , Ready(ready)
        , Status(status)
    {
    }

    TOperation(TString id, bool ready, EStatus status, NYql::TIssues issues)
        : Id(std::move(id))
        , Ready(ready)
        , Status(status)
        , Issues(std::move(issues))
    {
    }

    const TString& GetId() const {
        return Id;
    }

    bool IsReady() const {
        return Ready;
    }

    EStatus GetStatus() const {
        return Status;
    }

    const NYql::TIssues& GetIssues() const {
        return Issues;
    }

    template <typename... TArgs>
    void AddIssue(TArgs&&... args) {
        return Issues.AddIssue(std::forward<TArgs>(args)...);
    }

    void AddIssues(const NYql::TIssues& issues) {
        Issues.AddIssues(issues);
    }

private:
    // Identifier of the operation, empty value means no active operation object is present (it was forgotten or
    // not created in the first place, as in SYNC operation mode).
    TString Id;

    // true - this operation has been finished (doesn't matter successful or not),
    // so Status field has status code.
    // false - this operation still running. You can repeat request using operation Id.
    bool Ready;

    EStatus Status;
    NYql::TIssues Issues;
};

using TAsyncGetOperationResult = NThreading::TFuture<TOperation>;

using TAsyncCreateConsumerResult = NThreading::TFuture<TOperation>;

using TAsyncCreateDirectoryResult = NThreading::TFuture<TOperation>;

using TAsyncCreateTopicResult = NThreading::TFuture<TOperation>;

using TAsyncCreateReadRuleResult = NThreading::TFuture<TOperation>;

using TAsyncGrantPermissionsResult = NThreading::TFuture<TOperation>;

enum class ELimitsMode {
    WAIT /* "wait" */,
    NOTIFY /* "notify" */,
};

enum class EPermission {
    READ_TOPIC /* "ReadTopic" */,
    WRITE_TOPIC /* "WriteTopic" */,
    READ_AS_CONSUMER /* "ReadAsConsumer" */,
    MODIFY_PERMISSIONS /* "ModifyPermissions" */,
    CREATE_RESOURCES /* "CreateResources" */,
    MODIFY_RESOURCES /* "ModifyResources" */,
    LIST_RESOURCES /* "ListResources" */,
    CREATE_READ_RULES /* "CreateReadRules" */,
    DESCRIBE_RESOURCES /* "DescribeResources" */,
};

struct TExtraSettings {
    TExtraSettings& AddExtraSetting(const TString& name, const TString& value) {
        auto [iterator, emplaced] = Settings_.emplace(name, value);
        YQL_ENSURE(emplaced, "Duplicate setting name found: " << name
            << " (previous value: " << iterator->second << ", new value: " << value);

        return *this;
    }

    THashMap<TString, TString> Settings_;
};

template <typename TParent>
struct TExtraSettingsBuilder {
    TExtraSettingsBuilder(TParent& parent)
        : Parent_(parent)
    {
    }

    TExtraSettings& AddExtraSetting(const TString& name, const TString& value) {
        return Settings_.AddExtraSetting(name, value);
    }

    TParent& EndExtraSettings() {
        Parent_.ExtraSettings(Settings_);
        return Parent_;
    }

private:
    TExtraSettings Settings_;
    TParent& Parent_;
};

struct TCreateConsumerOptions {
    using TSelf = TCreateConsumerOptions;

    FLUENT_SETTING_OPTIONAL(bool, Important);
    FLUENT_SETTING_OPTIONAL(ui64, AvailabilityPeriodSec);
    FLUENT_SETTING_OPTIONAL(ELimitsMode, LimitsMode);
    FLUENT_SETTING_VECTOR(NYdb::NTopic::ECodec, SupportedCodecs);

    FLUENT_SETTING(TExtraSettings, ExtraSettings);

    TExtraSettingsBuilder<TSelf> BeginExtraSettings() {
        return TExtraSettingsBuilder(*this);
    }
};

struct TCreateDirectoryOptions {
    using TSelf = TCreateDirectoryOptions;

    FLUENT_SETTING(TExtraSettings, ExtraSettings);

    TExtraSettingsBuilder<TSelf> BeginExtraSettings() {
        return TExtraSettingsBuilder(*this);
    }
};

struct TCreateTopicOptions {
    using TSelf = TCreateTopicOptions;

    FLUENT_SETTING_OPTIONAL(ui64, PartitionsCount);
    FLUENT_SETTING_OPTIONAL(ui64, RetentionPeriodSec);

    FLUENT_SETTING_OPTIONAL(bool, AllowUnauthenticatedRead);
    FLUENT_SETTING_OPTIONAL(bool, AllowUnauthenticatedWrite);

    FLUENT_SETTING_VECTOR(NYdb::NTopic::ECodec, SupportedCodecs);
    FLUENT_SETTING_OPTIONAL(TString, FederationAccount);

    FLUENT_SETTING_OPTIONAL(ui64, MaxPartitionsCount);
    FLUENT_SETTING_OPTIONAL(ui64, AutoPartitioningStabilizationWindowSeconds);
    FLUENT_SETTING_OPTIONAL(ui64, AutoPartitioningUpUtilizationPercent);
    FLUENT_SETTING_OPTIONAL(ui64, AutoPartitioningDownUtilizationPercent);
    FLUENT_SETTING_OPTIONAL(TString, AutoPartitioningStrategy);

    FLUENT_SETTING_OPTIONAL(bool, PartitionMetricsEnabled);

    FLUENT_SETTING(TExtraSettings, ExtraSettings);

    TExtraSettingsBuilder<TSelf> BeginExtraSettings() {
        return TExtraSettingsBuilder(*this);
    }
};

struct TCreateReadRuleOptions {
    using TSelf = TCreateReadRuleOptions;

    // Read all data in this cluster or use 'all original' read rule type.
    FLUENT_SETTING(TString, MirrorToCluster);
};

struct TPermission {
    using TSelf = TPermission;

    FLUENT_SETTING_VECTOR(EPermission, PermissionNames);
    FLUENT_SETTING(TString, Subject);

    // Subject type and name used only in describe
    FLUENT_SETTING_OPTIONAL(TString, SubjectType);
    // Printable subject name
    FLUENT_SETTING_OPTIONAL(TString, SubjectName);
};

struct TGrantPermissionsOptions {
    using TSelf = TGrantPermissionsOptions;

    FLUENT_SETTING_VECTOR(TPermission, Permissions);
};

struct IClient : public TThrRefBase {
    using TPtr = TIntrusivePtr<IClient>;

    virtual TAsyncDescribePathResult DescribePath(const TString& /*path*/) const {
        AbortUnimplemented(__FUNCTION__);
    }

    virtual TAsyncCreateConsumerResult CreateConsumer(const TString& /*path*/, const TCreateConsumerOptions& /*options*/) const {
        AbortUnimplemented(__FUNCTION__);
    }

    virtual TAsyncCreateDirectoryResult CreateDirectory(const TString& /*path*/, const TCreateDirectoryOptions& /*options*/) const {
        AbortUnimplemented(__FUNCTION__);
    }

    virtual TAsyncCreateTopicResult CreateTopic(const TString& /*path*/, const TCreateTopicOptions& /*options*/) const {
        AbortUnimplemented(__FUNCTION__);
    }

    virtual TAsyncCreateReadRuleResult CreateReadRule(const TString& /*topicPath*/, const TString& /*consumerPath*/,
        const TCreateReadRuleOptions& /*options*/) const
    {
        AbortUnimplemented(__FUNCTION__);
    }

    virtual TAsyncGrantPermissionsResult GrantPermissions(const TString& /*path*/,
        const TGrantPermissionsOptions& /*options*/) const
    {
        AbortUnimplemented(__FUNCTION__);
    }

    virtual TAsyncGetOperationResult GetOperation(const TString& /*operationId*/) const {
        AbortUnimplemented(__FUNCTION__);
    }

    // TODO: If you need other methods, add them.
};

#define OPTION(type, name, default_exp)             \
    private:                                        \
        type name default_exp;                      \
    public:                                         \
        TTypeTraits<type>::TFuncParam Y_CAT(Get, name)() const { \
            return name;                            \
        }                                           \
        TSelf& Y_CAT(Set, name)(TTypeTraits<type>::TFuncParam val) { \
            name = val;                             \
            return *this;                           \
        }                                           \

struct TClientOptions {
    using TSelf = TClientOptions;

    OPTION(TString, Endpoint, );
    OPTION(std::shared_ptr<NYdb::ICredentialsProviderFactory>, CredentialsProviderFactory, );
    OPTION(TDuration, RequestTimeout, = TDuration::Seconds(10));
    OPTION(bool, EnableSsl, = false);
};

#undef OPTION

// Factory interface for creation clients to different pq clusters.
struct IConnections : public TThrRefBase {
    using TPtr = TIntrusivePtr<IConnections>;

    virtual ~IConnections() = default;

    virtual void Stop(bool wait = false) = 0;

    virtual IClient::TPtr GetClient(const TClientOptions&) = 0;
};

} // namespace NPq::NConfigurationManager
