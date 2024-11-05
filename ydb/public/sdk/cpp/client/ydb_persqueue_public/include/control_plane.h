#pragma once

#include "aliases.h"

#include <ydb/public/sdk/cpp/client/ydb_topic/include/codecs.h>
#include <ydb/public/sdk/cpp/client/ydb_types/fluent_settings_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>

#include <util/generic/size_literals.h>


namespace NYdb {
    class TProtoAccessor;
}

namespace NYdb::NPersQueue {

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
    TString GetOauthToken() const;
    TString GetJwtParams() const;

    TString GetIamServiceAccountKey() const;
    TString GetIamEndpoint() const;

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

            GETTER(TString, ConsumerName);
            GETTER(bool, Important);
            GETTER(TInstant, StartingMessageTimestamp);
            GETTER(EFormat, SupportedFormat);
            const TVector<ECodec>& SupportedCodecs() const {
                return SupportedCodecs_;
            }
            GETTER(ui32, Version);
            GETTER(TString, ServiceType);

        private:
            TString ConsumerName_;
            bool Important_;
            TInstant StartingMessageTimestamp_;
            EFormat SupportedFormat_;
            TVector<ECodec> SupportedCodecs_;
            ui32 Version_;
            TString ServiceType_;
        };

        struct TRemoteMirrorRule {
            TRemoteMirrorRule(const Ydb::PersQueue::V1::TopicSettings::RemoteMirrorRule&);
            GETTER(TString, Endpoint);
            GETTER(TString, TopicPath);
            GETTER(TString, ConsumerName);
            GETTER(TInstant, StartingMessageTimestamp);
            GETTER(TCredentials, Credentials);
            GETTER(TString, Database);

        private:
            TString Endpoint_;
            TString TopicPath_;
            TString ConsumerName_;
            TInstant StartingMessageTimestamp_;
            TCredentials Credentials_;
            TString Database_;
        };

        GETTER(ui32, PartitionsCount);
        GETTER(TDuration, RetentionPeriod);
        GETTER(EFormat, SupportedFormat);
        const TVector<ECodec>& SupportedCodecs() const {
            return SupportedCodecs_;
        }
        GETTER(ui64, MaxPartitionStorageSize);
        GETTER(ui64, MaxPartitionWriteSpeed);
        GETTER(ui64, MaxPartitionWriteBurst);
        GETTER(bool, ClientWriteDisabled);

        // attributes
        GETTER(bool, AllowUnauthenticatedWrite);
        GETTER(bool, AllowUnauthenticatedRead);
        GETTER(TMaybe<ui32>, PartitionsPerTablet);
        GETTER(TMaybe<ui32>, AbcId);
        GETTER(TMaybe<TString>, AbcSlug);
        GETTER(TMaybe<TString>, FederationAccount);

        const TVector<TReadRule>& ReadRules() const {
            return ReadRules_;
        }
        GETTER(TMaybe<TRemoteMirrorRule>, RemoteMirrorRule);

        GETTER(TMaybe<ui64>, MaxPartitionsCount);
        GETTER(TMaybe<TDuration>, StabilizationWindow);
        GETTER(TMaybe<ui64>, UpUtilizationPercent);
        GETTER(TMaybe<ui64>, DownUtilizationPercent);
        GETTER(TMaybe<Ydb::PersQueue::V1::AutoPartitioningStrategy>, AutoPartitioningStrategy);


#undef GETTER

    private:
        ui32 PartitionsCount_;
        TDuration RetentionPeriod_;
        EFormat SupportedFormat_;
        TVector<ECodec> SupportedCodecs_;
        ui64 MaxPartitionStorageSize_;
        ui64 MaxPartitionWriteSpeed_;
        ui64 MaxPartitionWriteBurst_;
        bool ClientWriteDisabled_;
        TVector<TReadRule> ReadRules_;
        TMaybe<TRemoteMirrorRule> RemoteMirrorRule_;
        // attributes
        bool AllowUnauthenticatedRead_;
        bool AllowUnauthenticatedWrite_;
        TMaybe<ui32> PartitionsPerTablet_;
        TMaybe<ui32> AbcId_;
        TMaybe<TString> AbcSlug_;
        TString FederationAccount_;

        TMaybe<ui64> MaxPartitionsCount_;
        TMaybe<TDuration> StabilizationWindow_;
        TMaybe<ui64> UpUtilizationPercent_;
        TMaybe<ui64> DownUtilizationPercent_;
        TMaybe<Ydb::PersQueue::V1::AutoPartitioningStrategy> AutoPartitioningStrategy_;
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



const TVector<ECodec>& GetDefaultCodecs();

struct TReadRuleSettings {
    TReadRuleSettings() {}
    using TSelf = TReadRuleSettings;
    FLUENT_SETTING(TString, ConsumerName);
    FLUENT_SETTING_DEFAULT(bool, Important, false);
    FLUENT_SETTING_DEFAULT(TInstant, StartingMessageTimestamp, TInstant::Zero());
    FLUENT_SETTING_DEFAULT(EFormat, SupportedFormat, EFormat::BASE)
    FLUENT_SETTING_DEFAULT(TVector<ECodec>, SupportedCodecs, GetDefaultCodecs());

    FLUENT_SETTING_DEFAULT(ui32, Version, 0);
    FLUENT_SETTING(TString, ServiceType);

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
        FLUENT_SETTING(TString, Endpoint);
        FLUENT_SETTING(TString, TopicPath);
        FLUENT_SETTING(TString, ConsumerName);
        FLUENT_SETTING_DEFAULT(TInstant, StartingMessageTimestamp, TInstant::Zero());
        FLUENT_SETTING(TCredentials, Credentials);
        FLUENT_SETTING(TString, Database);

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
    FLUENT_SETTING_DEFAULT(TVector<ECodec>, SupportedCodecs, GetDefaultCodecs());

    FLUENT_SETTING_DEFAULT(ui64, MaxPartitionStorageSize, 0);
    FLUENT_SETTING_DEFAULT(ui64, MaxPartitionWriteSpeed, 2_MB);
    FLUENT_SETTING_DEFAULT(ui64, MaxPartitionWriteBurst, 2_MB);

    FLUENT_SETTING_DEFAULT(bool, ClientWriteDisabled, false);
    FLUENT_SETTING_DEFAULT(bool, AllowUnauthenticatedWrite, false);
    FLUENT_SETTING_DEFAULT(bool, AllowUnauthenticatedRead, false);

    FLUENT_SETTING_OPTIONAL(ui32, PartitionsPerTablet);

    FLUENT_SETTING_OPTIONAL(ui32, AbcId);
    FLUENT_SETTING_OPTIONAL(TString, AbcSlug);
    FLUENT_SETTING_OPTIONAL(TString, FederationAccount);

    //TODO: FLUENT_SETTING_VECTOR
    FLUENT_SETTING_DEFAULT(TVector<TReadRuleSettings>, ReadRules, {});
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
            RemoteMirrorRule_ = TRemoteMirrorRuleSettings().SetSettings(settings.RemoteMirrorRule().GetRef());
        }

        MaxPartitionsCount_ = settings.MaxPartitionsCount();
        StabilizationWindow_ = settings.StabilizationWindow();
        UpUtilizationPercent_ = settings.UpUtilizationPercent();
        DownUtilizationPercent_ = settings.DownUtilizationPercent();
        AutoPartitioningStrategy_ = settings.AutoPartitioningStrategy();

        return static_cast<TDerived&>(*this);
    }

    private:
        TMaybe<ui64> MaxPartitionsCount_;
        TMaybe<TDuration> StabilizationWindow_;
        TMaybe<ui64> UpUtilizationPercent_;
        TMaybe<ui64> DownUtilizationPercent_;
        TMaybe<Ydb::PersQueue::V1::AutoPartitioningStrategy> AutoPartitioningStrategy_;

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
    FLUENT_SETTING(TString, ConsumerName);
};

}  // namespace NYdb::NPersQueue
