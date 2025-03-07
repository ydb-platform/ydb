#pragma once

#include "aliases.h"

#include <ydb-cpp-sdk/client/topic/codecs.h>
#include <ydb-cpp-sdk/client/types/fluent_settings_helpers.h>
#include <ydb-cpp-sdk/client/types/request_settings.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <util/generic/size_literals.h>

namespace NYdb::inline Dev {
    class TProtoAccessor;
}

namespace NYdb::inline Dev::NPersQueue {
    
enum class EFormat {
    BASE = 1,
};

struct TCredentials {
    enum class EMode {
        NOT_SET = 1,
        OAUTH_TOKEN = 2,
        JWT_PARAMS = 3,
        IAM = 4,
    };

    TCredentials() = default;
    TCredentials(const Ydb::PersQueue::V1::Credentials& credentials);
    EMode GetMode() const;
    std::string GetOauthToken() const;
    std::string GetJwtParams() const;

    std::string GetIamServiceAccountKey() const;
    std::string GetIamEndpoint() const;

private:
    EMode Mode_;
    Ydb::PersQueue::V1::Credentials Credentials_;
};


// Result for describe resource request.
struct TDescribeTopicResult : public TStatus {
    friend class NYdb::TProtoAccessor;

    struct TTopicSettings {
        TTopicSettings(const Ydb::PersQueue::V1::TopicSettings&);

        #define GETTER(TYPE, NAME) TYPE NAME() const { return NAME##_; }

        struct TReadRule {
            TReadRule(const Ydb::PersQueue::V1::TopicSettings::ReadRule&);

            GETTER(std::string, ConsumerName);
            GETTER(bool, Important);
            GETTER(TInstant, StartingMessageTimestamp);
            GETTER(EFormat, SupportedFormat);
            const std::vector<ECodec>& SupportedCodecs() const {
                return SupportedCodecs_;
            }
            GETTER(ui32, Version);
            GETTER(std::string, ServiceType);

        private:
            std::string ConsumerName_;
            bool Important_;
            TInstant StartingMessageTimestamp_;
            EFormat SupportedFormat_;
            std::vector<ECodec> SupportedCodecs_;
            ui32 Version_;
            std::string ServiceType_;
        };

        struct TRemoteMirrorRule {
            TRemoteMirrorRule(const Ydb::PersQueue::V1::TopicSettings::RemoteMirrorRule&);
            GETTER(std::string, Endpoint);
            GETTER(std::string, TopicPath);
            GETTER(std::string, ConsumerName);
            GETTER(TInstant, StartingMessageTimestamp);
            GETTER(TCredentials, Credentials);
            GETTER(std::string, Database);

        private:
            std::string Endpoint_;
            std::string TopicPath_;
            std::string ConsumerName_;
            TInstant StartingMessageTimestamp_;
            TCredentials Credentials_;
            std::string Database_;
        };

        GETTER(ui32, PartitionsCount);
        GETTER(TDuration, RetentionPeriod);
        GETTER(EFormat, SupportedFormat);
        const std::vector<ECodec>& SupportedCodecs() const {
            return SupportedCodecs_;
        }
        GETTER(ui64, MaxPartitionStorageSize);
        GETTER(ui64, MaxPartitionWriteSpeed);
        GETTER(ui64, MaxPartitionWriteBurst);
        GETTER(bool, ClientWriteDisabled);

        // attributes
        GETTER(bool, AllowUnauthenticatedWrite);
        GETTER(bool, AllowUnauthenticatedRead);
        GETTER(std::optional<ui32>, PartitionsPerTablet);
        GETTER(std::optional<ui32>, AbcId);
        GETTER(std::optional<std::string>, AbcSlug);
        GETTER(std::optional<std::string>, FederationAccount);

        const std::vector<TReadRule>& ReadRules() const {
            return ReadRules_;
        }
        GETTER(std::optional<TRemoteMirrorRule>, RemoteMirrorRule);

        GETTER(std::optional<uint64_t>, MaxPartitionsCount);
        GETTER(std::optional<TDuration>, StabilizationWindow);
        GETTER(std::optional<uint64_t>, UpUtilizationPercent);
        GETTER(std::optional<uint64_t>, DownUtilizationPercent);
        GETTER(std::optional<Ydb::PersQueue::V1::AutoPartitioningStrategy>, AutoPartitioningStrategy);


#undef GETTER

    private:
        ui32 PartitionsCount_;
        TDuration RetentionPeriod_;
        EFormat SupportedFormat_;
        std::vector<ECodec> SupportedCodecs_;
        ui64 MaxPartitionStorageSize_;
        ui64 MaxPartitionWriteSpeed_;
        ui64 MaxPartitionWriteBurst_;
        bool ClientWriteDisabled_;
        std::vector<TReadRule> ReadRules_;
        std::optional<TRemoteMirrorRule> RemoteMirrorRule_;
        // attributes
        bool AllowUnauthenticatedRead_;
        bool AllowUnauthenticatedWrite_;
        std::optional<ui32> PartitionsPerTablet_;
        std::optional<ui32> AbcId_;
        std::optional<std::string> AbcSlug_;
        std::string FederationAccount_;

        std::optional<uint64_t> MaxPartitionsCount_;
        std::optional<TDuration> StabilizationWindow_;
        std::optional<uint64_t> UpUtilizationPercent_;
        std::optional<uint64_t> DownUtilizationPercent_;
        std::optional<Ydb::PersQueue::V1::AutoPartitioningStrategy> AutoPartitioningStrategy_;
    };

    TDescribeTopicResult(TStatus status, const Ydb::PersQueue::V1::DescribeTopicResult& result);

    const TTopicSettings& TopicSettings() const {
        return TopicSettings_;
    }

private:
    TTopicSettings TopicSettings_;
    [[nodiscard]] const Ydb::PersQueue::V1::DescribeTopicResult& GetProto() const {
        return Proto_;
    }
    const Ydb::PersQueue::V1::DescribeTopicResult Proto_;
};

using TAsyncDescribeTopicResult = NThreading::TFuture<TDescribeTopicResult>;



const std::vector<ECodec>& GetDefaultCodecs();

struct TReadRuleSettings {
    TReadRuleSettings() {}
    using TSelf = TReadRuleSettings;
    FLUENT_SETTING(std::string, ConsumerName);
    FLUENT_SETTING_DEFAULT(bool, Important, false);
    FLUENT_SETTING_DEFAULT(TInstant, StartingMessageTimestamp, TInstant::Zero());
    FLUENT_SETTING_DEFAULT(EFormat, SupportedFormat, EFormat::BASE)
    FLUENT_SETTING_DEFAULT(std::vector<ECodec>, SupportedCodecs, GetDefaultCodecs());

    FLUENT_SETTING_DEFAULT(ui32, Version, 0);
    FLUENT_SETTING(std::string, ServiceType);

    TReadRuleSettings& SetSettings(const TDescribeTopicResult::TTopicSettings::TReadRule& settings) {
        ConsumerName_ = settings.ConsumerName();
        Important_ = settings.Important();
        StartingMessageTimestamp_ = settings.StartingMessageTimestamp();
        SupportedFormat_ = settings.SupportedFormat();
        SupportedCodecs_.clear();
        for (const auto& codec : settings.SupportedCodecs()) {
            SupportedCodecs_.push_back(codec);
        }
        Version_ = settings.Version();
        ServiceType_ = settings.ServiceType();
        return *this;
    }

};

// Settings for topic.
template <class TDerived>
struct TTopicSettings : public TOperationRequestSettings<TDerived> {
    friend class TPersQueueClient;

    struct TRemoteMirrorRuleSettings {
        TRemoteMirrorRuleSettings() {}
        using TSelf = TRemoteMirrorRuleSettings;
        FLUENT_SETTING(std::string, Endpoint);
        FLUENT_SETTING(std::string, TopicPath);
        FLUENT_SETTING(std::string, ConsumerName);
        FLUENT_SETTING_DEFAULT(TInstant, StartingMessageTimestamp, TInstant::Zero());
        FLUENT_SETTING(TCredentials, Credentials);
        FLUENT_SETTING(std::string, Database);

        TRemoteMirrorRuleSettings& SetSettings(const TDescribeTopicResult::TTopicSettings::TRemoteMirrorRule& settings) {
            Endpoint_ = settings.Endpoint();
            TopicPath_ = settings.TopicPath();
            ConsumerName_ = settings.ConsumerName();
            StartingMessageTimestamp_ = settings.StartingMessageTimestamp();
            Credentials_ = settings.Credentials();
            Database_ = settings.Database();
            return *this;
        }

    };

    using TSelf = TDerived;

    FLUENT_SETTING_DEFAULT(ui32, PartitionsCount, 1);
    FLUENT_SETTING_DEFAULT(TDuration, RetentionPeriod, TDuration::Hours(18));
    FLUENT_SETTING_DEFAULT(EFormat, SupportedFormat, EFormat::BASE)
    FLUENT_SETTING_DEFAULT(std::vector<ECodec>, SupportedCodecs, GetDefaultCodecs());

    FLUENT_SETTING_DEFAULT(ui64, MaxPartitionStorageSize, 0);
    FLUENT_SETTING_DEFAULT(ui64, MaxPartitionWriteSpeed, 2_MB);
    FLUENT_SETTING_DEFAULT(ui64, MaxPartitionWriteBurst, 2_MB);

    FLUENT_SETTING_DEFAULT(bool, ClientWriteDisabled, false);
    FLUENT_SETTING_DEFAULT(bool, AllowUnauthenticatedWrite, false);
    FLUENT_SETTING_DEFAULT(bool, AllowUnauthenticatedRead, false);

    FLUENT_SETTING_OPTIONAL(ui32, PartitionsPerTablet);

    FLUENT_SETTING_OPTIONAL(ui32, AbcId);
    FLUENT_SETTING_OPTIONAL(std::string, AbcSlug);
    FLUENT_SETTING_OPTIONAL(std::string, FederationAccount);

    //TODO: FLUENT_SETTING_VECTOR
    FLUENT_SETTING_DEFAULT(std::vector<TReadRuleSettings>, ReadRules, {});
    FLUENT_SETTING_OPTIONAL(TRemoteMirrorRuleSettings, RemoteMirrorRule);

    TSelf& SetSettings(const TDescribeTopicResult::TTopicSettings& settings) {

        PartitionsCount_ = settings.PartitionsCount();
        RetentionPeriod_ = settings.RetentionPeriod();
        SupportedFormat_ = settings.SupportedFormat();
        SupportedCodecs_.clear();
        for (const auto& codec : settings.SupportedCodecs()) {
            SupportedCodecs_.push_back(codec);
        }
        MaxPartitionStorageSize_ = settings.MaxPartitionStorageSize();
        MaxPartitionWriteSpeed_ = settings.MaxPartitionWriteSpeed();
        MaxPartitionWriteBurst_ = settings.MaxPartitionWriteBurst();
        ClientWriteDisabled_ = settings.ClientWriteDisabled();
        AllowUnauthenticatedRead_ = settings.AllowUnauthenticatedRead();
        AllowUnauthenticatedWrite_ = settings.AllowUnauthenticatedWrite();
        PartitionsPerTablet_ = settings.PartitionsPerTablet();
        AbcId_ = settings.AbcId();
        AbcSlug_ = settings.AbcSlug();
        FederationAccount_ = settings.FederationAccount();
        ReadRules_.clear();
        for (const auto& readRule : settings.ReadRules()) {
            ReadRules_.push_back({});
            ReadRules_.back().SetSettings(readRule);
        }
        if (settings.RemoteMirrorRule()) {
            RemoteMirrorRule_ = TRemoteMirrorRuleSettings().SetSettings(settings.RemoteMirrorRule().value());
        }

        MaxPartitionsCount_ = settings.MaxPartitionsCount();
        StabilizationWindow_ = settings.StabilizationWindow();
        UpUtilizationPercent_ = settings.UpUtilizationPercent();
        DownUtilizationPercent_ = settings.DownUtilizationPercent();
        AutoPartitioningStrategy_ = settings.AutoPartitioningStrategy();

        return static_cast<TDerived&>(*this);
    }

private:
    std::optional<uint64_t> MaxPartitionsCount_;
    std::optional<TDuration> StabilizationWindow_;
    std::optional<uint64_t> UpUtilizationPercent_;
    std::optional<uint64_t> DownUtilizationPercent_;
    std::optional<Ydb::PersQueue::V1::AutoPartitioningStrategy> AutoPartitioningStrategy_;
};


// Settings for create resource request.
struct TCreateTopicSettings : public TTopicSettings<TCreateTopicSettings> {
};

// Settings for alter resource request.
struct TAlterTopicSettings : public TTopicSettings<TAlterTopicSettings> {
};

// Settings for drop resource request.
struct TDropTopicSettings : public TOperationRequestSettings<TDropTopicSettings> {};

// Settings for describe resource request.
struct TDescribeTopicSettings : public TOperationRequestSettings<TDescribeTopicSettings> {};

// Settings for add read rule request
struct TAddReadRuleSettings : public TTopicSettings<TAddReadRuleSettings> {
    FLUENT_SETTING(TReadRuleSettings, ReadRule);
};

// Settings for remove read rule request
struct TRemoveReadRuleSettings : public TOperationRequestSettings<TRemoveReadRuleSettings> {
    FLUENT_SETTING(std::string, ConsumerName);
};

}  // namespace NYdb::NPersQueue
