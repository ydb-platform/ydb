#pragma once
#include <ydb/library/yql/public/issue/yql_issue.h>

#include <ydb/public/sdk/cpp/client/ydb_types/credentials/credentials.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/typetraits.h>
#include <util/generic/yexception.h>
#include <util/system/defaults.h>

#include <variant>

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

struct IClient : public TThrRefBase {
    using TPtr = TIntrusivePtr<IClient>;

    virtual TAsyncDescribePathResult DescribePath(const TString& path) const = 0;

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
