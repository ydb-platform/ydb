#pragma once
#include <ydb/public/api/grpc/draft/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/retry/retry_policy.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/ptr.h>
#include <util/generic/size_literals.h>
#include <util/string/builder.h>
#include <util/thread/pool.h>

#include <exception>
#include <variant>

namespace NYdb {
    class TProtoAccessor;

    namespace NScheme {
        struct TPermissions;
    }
}

namespace NYdb::NTopic {

enum class ECodec : ui32 {
    RAW = 1,
    GZIP = 2,
    LZOP = 3,
    ZSTD = 4,
    CUSTOM = 10000,
};


class TConsumer {
public:
    TConsumer(const Ydb::Topic::Consumer&);

    const TString& GetConsumerName() const;
    bool GetImportant() const;
    const TInstant& GetReadFrom() const;
    const TVector<ECodec>& GetSupportedCodecs() const;
    const TMap<TString, TString>& GetAttributes() const;
private:
    TString ConsumerName_;
    bool Important_;
    TInstant ReadFrom_;
    TMap<TString, TString> Attributes_;
    TVector<ECodec> SupportedCodecs_;
};

class TPartitionInfo {
public:
    TPartitionInfo(const Ydb::Topic::DescribeTopicResult::PartitionInfo& partitionInfo);
    ui64 GetPartitionId() const;
    bool GetActive() const;
    const TVector<ui64> GetChildPartitionIds() const;
    const TVector<ui64> GetParentPartitionIds() const;

private:
    ui64 PartitionId_;
    bool Active_;
    TVector<ui64> ChildPartitionIds_;
    TVector<ui64> ParentPartitionIds_;
};


class TPartitioningSettings {
public:
    TPartitioningSettings() : MinActivePartitions_(0), PartitionCountLimit_(0){}
    TPartitioningSettings(const Ydb::Topic::PartitioningSettings& settings);
    TPartitioningSettings(ui64 minActivePartitions, ui64 partitionCountLimit) : MinActivePartitions_(minActivePartitions), PartitionCountLimit_(partitionCountLimit) {}

    ui64 GetMinActivePartitions() const;
    ui64 GetPartitionCountLimit() const;
private:
    ui64 MinActivePartitions_;
    ui64 PartitionCountLimit_;
};

class TTopicDescription {
    friend class NYdb::TProtoAccessor;

public:
    TTopicDescription(Ydb::Topic::DescribeTopicResult&& desc);

    const TString& GetOwner() const;

    const TVector<NScheme::TPermissions>& GetPermissions() const;

    const TVector<NScheme::TPermissions>& GetEffectivePermissions() const;

    const TPartitioningSettings& GetPartitioningSettings() const;

    ui32 GetTotalPartitionsCount() const;

    const TVector<TPartitionInfo>& GetPartitions() const;

    const TVector<ECodec>& GetSupportedCodecs() const;

    const TDuration& GetRetentionPeriod() const;

    TMaybe<ui64> GetRetentionStorageMb() const;

    ui64 GetPartitionWriteSpeedBytesPerSecond() const;

    ui64 GetPartitionWriteBurstBytes() const;

    const TMap<TString, TString>& GetAttributes() const;

    const TVector<TConsumer>& GetConsumers() const;

    void SerializeTo(Ydb::Topic::CreateTopicRequest& request) const;
private:

    const Ydb::Topic::DescribeTopicResult& GetProto() const;

    const Ydb::Topic::DescribeTopicResult Proto_;
    TVector<TPartitionInfo> Partitions_;
    TVector<ECodec> SupportedCodecs_;
    TPartitioningSettings PartitioningSettings_;
    TDuration RetentionPeriod_;
    TMaybe<ui64> RetentionStorageMb_;
    ui64 PartitionWriteSpeedBytesPerSecond_;
    ui64 PartitionWriteBurstBytes_;
    TMap<TString, TString> Attributes_;
    TVector<TConsumer> Consumers_;

    TString Owner_;
    TVector<NScheme::TPermissions> Permissions_;
    TVector<NScheme::TPermissions> EffectivePermissions_;
};


// Result for describe resource request.
struct TDescribeTopicResult : public TStatus {
    friend class NYdb::TProtoAccessor;


    TDescribeTopicResult(TStatus&& status, Ydb::Topic::DescribeTopicResult&& result);

    const TTopicDescription& GetTopicDescription() const;

private:
    TTopicDescription TopicDescription_;
};

using TAsyncDescribeTopicResult = NThreading::TFuture<TDescribeTopicResult>;

template <class TSettings>
class TAlterAttributesBuilderImpl {
public:
    TAlterAttributesBuilderImpl(TSettings& parent)
    : Parent_(parent)
    { }

    TAlterAttributesBuilderImpl& Alter(const TString& key, const TString& value) {
        Parent_.AlterAttributes_[key] = value;
        return *this;
    }

    TAlterAttributesBuilderImpl& Add(const TString& key, const TString& value) {
        return Alter(key, value);
    }

    TAlterAttributesBuilderImpl& Drop(const TString& key) {
        return Alter(key, "");
    }

    TSettings& EndAlterAttributes() { return Parent_; }

private:
    TSettings& Parent_;
};


struct TAlterConsumerSettings;
struct TAlterTopicSettings;
struct TCreateTopicSettings;

typedef TAlterAttributesBuilderImpl<TAlterConsumerSettings> TAlterConsumerAttributesBuilder;

typedef TAlterAttributesBuilderImpl<TAlterTopicSettings> TAlterTopicAttributesBuilder;

template<class TSettings>
struct TConsumerSettings {
    using TSelf = TConsumerSettings;

    using TAttributes = TMap<TString, TString>;

    TConsumerSettings(TSettings& parent): Parent_(parent) {}
    TConsumerSettings(TSettings& parent, const TString& name) : ConsumerName_(name), Parent_(parent) {}

    FLUENT_SETTING(TString, ConsumerName);
    FLUENT_SETTING_DEFAULT(bool, Important, false);
    FLUENT_SETTING_DEFAULT(TInstant, ReadFrom, TInstant::Zero());

    FLUENT_SETTING_VECTOR(ECodec, SupportedCodecs);

    FLUENT_SETTING(TAttributes, Attributes);

    TConsumerSettings& AddAttribute(const TString& key, const TString& value) {
        Attributes_[key] = value;
        return *this;
    }

    TConsumerSettings& SetAttributes(TMap<TString, TString>&& attributes) {
        Attributes_ = std::move(attributes);
        return *this;
    }

    TConsumerSettings& SetAttributes(const TMap<TString, TString>& attributes) {
        Attributes_ = attributes;
        return *this;
    }

    TConsumerSettings& SetSuportedCodecs(TVector<ECodec>&& codecs) {
        SupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TConsumerSettings& SetSuportedCodecs(const TVector<ECodec>& codecs) {
        SupportedCodecs_ = codecs;
        return *this;
    }

    TSettings& EndAddConsumer() { return Parent_; };

private:
    TSettings& Parent_;
};


struct TAlterConsumerSettings {
    using TSelf = TAlterConsumerSettings;

    using TAlterAttributes = TMap<TString, TString>;

    TAlterConsumerSettings(TAlterTopicSettings& parent): Parent_(parent) {}
    TAlterConsumerSettings(TAlterTopicSettings& parent, const TString& name) : ConsumerName_(name), Parent_(parent) {}

    FLUENT_SETTING(TString, ConsumerName);
    FLUENT_SETTING_OPTIONAL(bool, SetImportant);
    FLUENT_SETTING_OPTIONAL(TInstant, SetReadFrom);

    FLUENT_SETTING_OPTIONAL_VECTOR(ECodec, SetSupportedCodecs);

    FLUENT_SETTING(TAlterAttributes, AlterAttributes);

    TAlterConsumerAttributesBuilder BeginAlterAttributes() {
        return TAlterConsumerAttributesBuilder(*this);
    }

    TAlterConsumerSettings& SetSuportedCodecs(TVector<ECodec>&& codecs) {
        SetSupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TAlterConsumerSettings& SetSuportedCodecs(const TVector<ECodec>& codecs) {
        SetSupportedCodecs_ = codecs;
        return *this;
    }

    TAlterTopicSettings& EndAlterConsumer() { return Parent_; };

private:
    TAlterTopicSettings& Parent_;
};


struct TCreateTopicSettings : public TOperationRequestSettings<TCreateTopicSettings> {

    using TSelf = TCreateTopicSettings;
    using TAttributes = TMap<TString, TString>;

    FLUENT_SETTING(TPartitioningSettings, PartitioningSettings);

    FLUENT_SETTING_DEFAULT(TDuration, RetentionPeriod, TDuration::Hours(24));

    FLUENT_SETTING_VECTOR(ECodec, SupportedCodecs);

    FLUENT_SETTING_DEFAULT(ui64, RetentionStorageMb, 0);

    FLUENT_SETTING_DEFAULT(ui64, PartitionWriteSpeedBytesPerSecond, 0);
    FLUENT_SETTING_DEFAULT(ui64, PartitionWriteBurstBytes, 0);

    FLUENT_SETTING_VECTOR(TConsumerSettings<TCreateTopicSettings>, Consumers);

    FLUENT_SETTING(TAttributes, Attributes);


    TCreateTopicSettings& SetSuportedCodecs(TVector<ECodec>&& codecs) {
        SupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TCreateTopicSettings& SetSuportedCodecs(const TVector<ECodec>& codecs) {
        SupportedCodecs_ = codecs;
        return *this;
    }

    TConsumerSettings<TCreateTopicSettings>& BeginAddConsumer() {
        Consumers_.push_back({*this});
        return Consumers_.back();
    }

    TConsumerSettings<TCreateTopicSettings>& BeginAddConsumer(const TString& name) {
        Consumers_.push_back({*this, name});
        return Consumers_.back();
    }

    TCreateTopicSettings& AddAttribute(const TString& key, const TString& value) {
        Attributes_[key] = value;
        return *this;
    }

    TCreateTopicSettings& SetAttributes(TMap<TString, TString>&& attributes) {
        Attributes_ = std::move(attributes);
        return *this;
    }

    TCreateTopicSettings& SetAttributes(const TMap<TString, TString>& attributes) {
        Attributes_ = attributes;
        return *this;
    }

    TCreateTopicSettings& PartitioningSettings(ui64 minActivePartitions, ui64 partitionCountLimit) {
        PartitioningSettings_ = TPartitioningSettings(minActivePartitions, partitionCountLimit);
        return *this;
    }
};


struct TAlterTopicSettings : public TOperationRequestSettings<TAlterTopicSettings> {

    using TSelf = TAlterTopicSettings;
    using TAlterAttributes = TMap<TString, TString>;

    FLUENT_SETTING_OPTIONAL(TPartitioningSettings, AlterPartitioningSettings);

    FLUENT_SETTING_OPTIONAL(TDuration, SetRetentionPeriod);

    FLUENT_SETTING_OPTIONAL_VECTOR(ECodec, SetSupportedCodecs);

    FLUENT_SETTING_OPTIONAL(ui64, SetRetentionStorageMb);

    FLUENT_SETTING_OPTIONAL(ui64, SetPartitionWriteSpeedBytesPerSecond);
    FLUENT_SETTING_OPTIONAL(ui64, SetPartitionWriteBurstBytes);

    FLUENT_SETTING_VECTOR(TConsumerSettings<TAlterTopicSettings>, AddConsumers);
    FLUENT_SETTING_VECTOR(TString, DropConsumers);
    FLUENT_SETTING_VECTOR(TAlterConsumerSettings, AlterConsumers);

    FLUENT_SETTING(TAlterAttributes, AlterAttributes);

    TAlterTopicAttributesBuilder BeginAlterAttributes() {
        return TAlterTopicAttributesBuilder(*this);
    }

    TAlterTopicSettings& SetSuportedCodecs(TVector<ECodec>&& codecs) {
        SetSupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TAlterTopicSettings& SetSuportedCodecs(const TVector<ECodec>& codecs) {
        SetSupportedCodecs_ = codecs;
        return *this;
    }

    TConsumerSettings<TAlterTopicSettings>& BeginAddConsumer() {
        AddConsumers_.push_back({*this});
        return AddConsumers_.back();
    }

    TConsumerSettings<TAlterTopicSettings>& BeginAddConsumer(const TString& name) {
        AddConsumers_.push_back({*this, name});
        return AddConsumers_.back();
    }

    TAlterConsumerSettings& BeginAlterConsumer() {
        AlterConsumers_.push_back({*this});
        return AlterConsumers_.back();
    }

    TAlterConsumerSettings& BeginAlterConsumer(const TString& name) {
        AlterConsumers_.push_back({*this, name});
        return AlterConsumers_.back();
    }

    TAlterTopicSettings& AlterPartitioningSettings(ui64 minActivePartitions, ui64 partitionCountLimit) {
        AlterPartitioningSettings_ = TPartitioningSettings(minActivePartitions, partitionCountLimit);
        return *this;
    }
};


// Settings for drop resource request.
struct TDropTopicSettings : public TOperationRequestSettings<TDropTopicSettings> {};

// Settings for describe resource request.
struct TDescribeTopicSettings : public TOperationRequestSettings<TDescribeTopicSettings> {};


struct TTopicClientSettings : public TCommonClientSettingsBase<TTopicClientSettings> {
    using TSelf = TTopicClientSettings;

};

// Topic client.
class TTopicClient {
public:
    class TImpl;

    TTopicClient(const TDriver& driver, const TTopicClientSettings& settings = TTopicClientSettings());

    // Create a new topic.
    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings = {});

    // Update a topic.
    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& = {});

    // Delete a topic.
    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& = {});

    // Describe settings of topic.
    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NTopic
