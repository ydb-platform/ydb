#pragma once

#include "codecs.h"

#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <util/datetime/base.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

#include <limits>

namespace NYdb {
    class TProtoAccessor;

    namespace NScheme {
        struct TPermissions;
    }
}

namespace NYdb::NTopic {

enum class EMeteringMode : ui32 {
    Unspecified = 0,
    ReservedCapacity = 1,
    RequestUnits = 2,

    Unknown = std::numeric_limits<int>::max(),
};

enum class EAutoPartitioningStrategy: ui32 {
    Unspecified = 0,
    Disabled = 1,
    ScaleUp = 2,
    ScaleUpAndDown = 3,
    Paused = 4
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


class TTopicStats {
public:
    TTopicStats(const Ydb::Topic::DescribeTopicResult::TopicStats& topicStats);

    ui64 GetStoreSizeBytes() const;
    TDuration GetMaxWriteTimeLag() const;
    TInstant GetMinLastWriteTime() const;
    ui64 GetBytesWrittenPerMinute() const;
    ui64 GetBytesWrittenPerHour() const;
    ui64 GetBytesWrittenPerDay() const;

private:
    ui64 StoreSizeBytes_;
    TInstant MinLastWriteTime_;
    TDuration MaxWriteTimeLag_;
    ui64 BytesWrittenPerMinute_;
    ui64 BytesWrittenPerHour_;
    ui64 BytesWrittenPerDay_;
};


class TPartitionStats {
public:
    TPartitionStats(const Ydb::Topic::PartitionStats& partitionStats);

    ui64 GetStartOffset() const;
    ui64 GetEndOffset() const;
    ui64 GetStoreSizeBytes() const;
    TDuration GetMaxWriteTimeLag() const;
    TInstant GetLastWriteTime() const;
    ui64 GetBytesWrittenPerMinute() const;
    ui64 GetBytesWrittenPerHour() const;
    ui64 GetBytesWrittenPerDay() const;

private:
    ui64 StartOffset_;
    ui64 EndOffset_;
    ui64 StoreSizeBytes_;
    TInstant LastWriteTime_;
    TDuration MaxWriteTimeLag_;
    ui64 BytesWrittenPerMinute_;
    ui64 BytesWrittenPerHour_;
    ui64 BytesWrittenPerDay_;
};

class TPartitionConsumerStats {
public:
    TPartitionConsumerStats(const Ydb::Topic::DescribeConsumerResult::PartitionConsumerStats& partitionStats);
    ui64 GetCommittedOffset() const;
    ui64 GetLastReadOffset() const;
    TString GetReaderName() const;
    TString GetReadSessionId() const;

private:
    ui64 CommittedOffset_;
    i64 LastReadOffset_;
    TString ReaderName_;
    TString ReadSessionId_;
};

// Topic partition location
class TPartitionLocation {
public:
    TPartitionLocation(const Ydb::Topic::PartitionLocation& partitionLocation);
    i32 GetNodeId() const;
    i64 GetGeneration() const;

private:
    // Node identificator.
    i32 NodeId_ = 1;

    // Partition generation.
    i64 Generation_ = 2;
};

class TPartitionInfo {
public:
    TPartitionInfo(const Ydb::Topic::DescribeTopicResult::PartitionInfo& partitionInfo);
    TPartitionInfo(const Ydb::Topic::DescribeConsumerResult::PartitionInfo& partitionInfo);

    ui64 GetPartitionId() const;
    bool GetActive() const;
    const TVector<ui64> GetChildPartitionIds() const;
    const TVector<ui64> GetParentPartitionIds() const;

    const TMaybe<TPartitionStats>& GetPartitionStats() const;
    const TMaybe<TPartitionConsumerStats>& GetPartitionConsumerStats() const;
    const TMaybe<TPartitionLocation>& GetPartitionLocation() const;

private:
    ui64 PartitionId_;
    bool Active_;
    TVector<ui64> ChildPartitionIds_;
    TVector<ui64> ParentPartitionIds_;
    TMaybe<TPartitionStats> PartitionStats_;
    TMaybe<TPartitionConsumerStats> PartitionConsumerStats_;
    TMaybe<TPartitionLocation> PartitionLocation_;
};

struct TAlterPartitioningSettings;
struct TAlterTopicSettings;

struct TAutoPartitioningSettings {
friend struct TAutoPartitioningSettingsBuilder;
public:
    TAutoPartitioningSettings()
        : Strategy_(EAutoPartitioningStrategy::Disabled)
        , StabilizationWindow_(TDuration::Seconds(0))
        , DownUtilizationPercent_(0)
        , UpUtilizationPercent_(0) {
    }
    TAutoPartitioningSettings(const Ydb::Topic::AutoPartitioningSettings& settings);
    TAutoPartitioningSettings(EAutoPartitioningStrategy strategy, TDuration stabilizationWindow, ui64 downUtilizationPercent, ui64 upUtilizationPercent)
        : Strategy_(strategy)
        , StabilizationWindow_(stabilizationWindow)
        , DownUtilizationPercent_(downUtilizationPercent)
        , UpUtilizationPercent_(upUtilizationPercent) {}

    EAutoPartitioningStrategy GetStrategy() const;
    TDuration GetStabilizationWindow() const;
    ui32 GetDownUtilizationPercent() const;
    ui32 GetUpUtilizationPercent() const;
private:
    EAutoPartitioningStrategy Strategy_;
    TDuration StabilizationWindow_;
    ui32 DownUtilizationPercent_;
    ui32 UpUtilizationPercent_;
};

struct TAlterAutoPartitioningSettings {
    using TSelf = TAlterAutoPartitioningSettings;
    public:
        TAlterAutoPartitioningSettings(TAlterPartitioningSettings& parent): Parent_(parent) {}

    FLUENT_SETTING_OPTIONAL(EAutoPartitioningStrategy, Strategy);
    FLUENT_SETTING_OPTIONAL(TDuration, StabilizationWindow);
    FLUENT_SETTING_OPTIONAL(ui64, DownUtilizationPercent);
    FLUENT_SETTING_OPTIONAL(ui64, UpUtilizationPercent);

    TAlterPartitioningSettings& EndAlterAutoPartitioningSettings() { return Parent_; };

    private:
        TAlterPartitioningSettings& Parent_;
};

class TPartitioningSettings {
    using TSelf = TPartitioningSettings;
    friend struct TPartitioningSettingsBuilder;
public:
    TPartitioningSettings() : MinActivePartitions_(0), MaxActivePartitions_(0), PartitionCountLimit_(0), AutoPartitioningSettings_(){}
    TPartitioningSettings(const Ydb::Topic::PartitioningSettings& settings);
    TPartitioningSettings(ui64 minActivePartitions, ui64 maxActivePartitions, TAutoPartitioningSettings autoscalingSettings = {})
        : MinActivePartitions_(minActivePartitions)
        , MaxActivePartitions_(maxActivePartitions)
        , PartitionCountLimit_(0)
        , AutoPartitioningSettings_(autoscalingSettings)
 {
    }

    ui64 GetMinActivePartitions() const;
    ui64 GetMaxActivePartitions() const;
    ui64 GetPartitionCountLimit() const;
    TAutoPartitioningSettings GetAutoPartitioningSettings() const;
private:
    ui64 MinActivePartitions_;
    ui64 MaxActivePartitions_;
    ui64 PartitionCountLimit_;
    TAutoPartitioningSettings AutoPartitioningSettings_;
};

struct TAlterTopicSettings;

struct TAlterPartitioningSettings {
    using TSelf = TAlterPartitioningSettings;
public:
    TAlterPartitioningSettings(TAlterTopicSettings& parent): Parent_(parent) {}

    FLUENT_SETTING_OPTIONAL(ui64, MinActivePartitions);
    FLUENT_SETTING_OPTIONAL(ui64, MaxActivePartitions);

    TAlterTopicSettings& EndAlterTopicPartitioningSettings() { return Parent_; };

    TAlterAutoPartitioningSettings& BeginAlterAutoPartitioningSettings() {
        AutoPartitioningSettings_.ConstructInPlace(*this);
        return *AutoPartitioningSettings_;
    }

    TMaybe<TAlterAutoPartitioningSettings> AutoPartitioningSettings_;

private:
    TAlterTopicSettings& Parent_;
};

class TTopicDescription {
    friend class NYdb::TProtoAccessor;

public:
    TTopicDescription(Ydb::Topic::DescribeTopicResult&& desc);

    const TString& GetOwner() const;

    const NScheme::TVirtualTimestamp& GetCreationTimestamp() const;

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

    EMeteringMode GetMeteringMode() const;

    const TTopicStats& GetTopicStats() const;

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
    EMeteringMode MeteringMode_;
    TMap<TString, TString> Attributes_;
    TVector<TConsumer> Consumers_;
    TTopicStats TopicStats_;

    TString Owner_;
    NScheme::TVirtualTimestamp CreationTimestamp_;
    TVector<NScheme::TPermissions> Permissions_;
    TVector<NScheme::TPermissions> EffectivePermissions_;
};


class TConsumerDescription {
    friend class NYdb::TProtoAccessor;

public:
    TConsumerDescription(Ydb::Topic::DescribeConsumerResult&& desc);

    const TVector<TPartitionInfo>& GetPartitions() const;

    const TConsumer& GetConsumer() const;

private:

    const Ydb::Topic::DescribeConsumerResult& GetProto() const;


    const Ydb::Topic::DescribeConsumerResult Proto_;
    TVector<TPartitionInfo> Partitions_;
    TConsumer Consumer_;
};

class TPartitionDescription {
    friend class NYdb::TProtoAccessor;

public:
    TPartitionDescription(Ydb::Topic::DescribePartitionResult&& desc);

    const TPartitionInfo& GetPartition() const;
private:
    const Ydb::Topic::DescribePartitionResult& GetProto() const;

    const Ydb::Topic::DescribePartitionResult Proto_;
    TPartitionInfo Partition_;
};

// Result for describe topic request.
struct TDescribeTopicResult : public TStatus {
    friend class NYdb::TProtoAccessor;

    TDescribeTopicResult(TStatus&& status, Ydb::Topic::DescribeTopicResult&& result);

    const TTopicDescription& GetTopicDescription() const;

private:
    TTopicDescription TopicDescription_;
};

// Result for describe consumer request.
struct TDescribeConsumerResult : public TStatus {
    friend class NYdb::TProtoAccessor;

    TDescribeConsumerResult(TStatus&& status, Ydb::Topic::DescribeConsumerResult&& result);

    const TConsumerDescription& GetConsumerDescription() const;

private:
    TConsumerDescription ConsumerDescription_;
};

// Result for describe partition request.
struct TDescribePartitionResult: public TStatus {
    friend class NYdb::TProtoAccessor;

    TDescribePartitionResult(TStatus&& status, Ydb::Topic::DescribePartitionResult&& result);

    const TPartitionDescription& GetPartitionDescription() const;

private:
    TPartitionDescription PartitionDescription_;
};

using TAsyncDescribeTopicResult = NThreading::TFuture<TDescribeTopicResult>;
using TAsyncDescribeConsumerResult = NThreading::TFuture<TDescribeConsumerResult>;
using TAsyncDescribePartitionResult = NThreading::TFuture<TDescribePartitionResult>;

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

using TAlterConsumerAttributesBuilder = TAlterAttributesBuilderImpl<TAlterConsumerSettings>;
using TAlterTopicAttributesBuilder = TAlterAttributesBuilderImpl<TAlterTopicSettings>;

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

    TConsumerSettings& SetSupportedCodecs(TVector<ECodec>&& codecs) {
        SupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TConsumerSettings& SetSupportedCodecs(const TVector<ECodec>& codecs) {
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

    TAlterConsumerSettings& SetSupportedCodecs(TVector<ECodec>&& codecs) {
        SetSupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TAlterConsumerSettings& SetSupportedCodecs(const TVector<ECodec>& codecs) {
        SetSupportedCodecs_ = codecs;
        return *this;
    }

    TAlterTopicSettings& EndAlterConsumer() { return Parent_; };

private:
    TAlterTopicSettings& Parent_;
};

struct TPartitioningSettingsBuilder;
struct TCreateTopicSettings : public TOperationRequestSettings<TCreateTopicSettings> {

    using TSelf = TCreateTopicSettings;
    using TAttributes = TMap<TString, TString>;

    FLUENT_SETTING(TPartitioningSettings, PartitioningSettings);

    FLUENT_SETTING_DEFAULT(TDuration, RetentionPeriod, TDuration::Hours(24));

    FLUENT_SETTING_VECTOR(ECodec, SupportedCodecs);

    FLUENT_SETTING_DEFAULT(ui64, RetentionStorageMb, 0);
    FLUENT_SETTING_DEFAULT(EMeteringMode, MeteringMode, EMeteringMode::Unspecified);

    FLUENT_SETTING_DEFAULT(ui64, PartitionWriteSpeedBytesPerSecond, 0);
    FLUENT_SETTING_DEFAULT(ui64, PartitionWriteBurstBytes, 0);

    FLUENT_SETTING_VECTOR(TConsumerSettings<TCreateTopicSettings>, Consumers);

    FLUENT_SETTING(TAttributes, Attributes);

    TCreateTopicSettings& SetSupportedCodecs(TVector<ECodec>&& codecs) {
        SupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TCreateTopicSettings& SetSupportedCodecs(const TVector<ECodec>& codecs) {
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

    TCreateTopicSettings& PartitioningSettings(ui64 minActivePartitions, ui64 maxActivePartitions, TAutoPartitioningSettings autoPartitioningSettings = {}) {
        PartitioningSettings_ = TPartitioningSettings(minActivePartitions, maxActivePartitions, autoPartitioningSettings);
        return *this;
    }

    TPartitioningSettingsBuilder BeginConfigurePartitioningSettings();
};

struct TAutoPartitioningSettingsBuilder {
    using TSelf = TAutoPartitioningSettingsBuilder;
public:
    TAutoPartitioningSettingsBuilder(TPartitioningSettingsBuilder& parent, TAutoPartitioningSettings& settings): Parent_(parent), Settings_(settings) {}

    TSelf Strategy(EAutoPartitioningStrategy value) {
        Settings_.Strategy_ = value;
        return *this;
    }

    TSelf StabilizationWindow(TDuration value) {
        Settings_.StabilizationWindow_ = value;
        return *this;
    }

    TSelf DownUtilizationPercent(ui32 value) {
        Settings_.DownUtilizationPercent_ = value;
        return *this;
    }

    TSelf UpUtilizationPercent(ui32 value) {
        Settings_.UpUtilizationPercent_ = value;
        return *this;
    }

    TPartitioningSettingsBuilder& EndConfigureAutoPartitioningSettings() {
        return Parent_;
    }

private:
    TPartitioningSettingsBuilder& Parent_;
    TAutoPartitioningSettings& Settings_;
};

struct TPartitioningSettingsBuilder {
    using TSelf = TPartitioningSettingsBuilder;
public:
    TPartitioningSettingsBuilder(TCreateTopicSettings& parent): Parent_(parent) {}

    TSelf MinActivePartitions(ui64 value) {
        Parent_.PartitioningSettings_.MinActivePartitions_ = value;
        return *this;
    }

    TSelf MaxActivePartitions(ui64 value) {
        Parent_.PartitioningSettings_.MaxActivePartitions_ = value;
        return *this;
    }

    TAutoPartitioningSettingsBuilder BeginConfigureAutoPartitioningSettings() {
        return {*this, Parent_.PartitioningSettings_.AutoPartitioningSettings_};
    }

    TCreateTopicSettings& EndConfigurePartitioningSettings() {
        return Parent_;
    }

private:
    TCreateTopicSettings& Parent_;
};

struct TAlterTopicSettings : public TOperationRequestSettings<TAlterTopicSettings> {

    using TSelf = TAlterTopicSettings;
    using TAlterAttributes = TMap<TString, TString>;

    FLUENT_SETTING_OPTIONAL(TDuration, SetRetentionPeriod);

    FLUENT_SETTING_OPTIONAL_VECTOR(ECodec, SetSupportedCodecs);

    FLUENT_SETTING_OPTIONAL(ui64, SetRetentionStorageMb);

    FLUENT_SETTING_OPTIONAL(ui64, SetPartitionWriteSpeedBytesPerSecond);
    FLUENT_SETTING_OPTIONAL(ui64, SetPartitionWriteBurstBytes);

    FLUENT_SETTING_OPTIONAL(EMeteringMode, SetMeteringMode);

    FLUENT_SETTING_VECTOR(TConsumerSettings<TAlterTopicSettings>, AddConsumers);
    FLUENT_SETTING_VECTOR(TString, DropConsumers);
    FLUENT_SETTING_VECTOR(TAlterConsumerSettings, AlterConsumers);

    FLUENT_SETTING(TAlterAttributes, AlterAttributes);

    TAlterTopicAttributesBuilder BeginAlterAttributes() {
        return TAlterTopicAttributesBuilder(*this);
    }

    TAlterTopicSettings& SetSupportedCodecs(TVector<ECodec>&& codecs) {
        SetSupportedCodecs_ = std::move(codecs);
        return *this;
    }

    TAlterTopicSettings& SetSupportedCodecs(const TVector<ECodec>& codecs) {
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

    TAlterPartitioningSettings& BeginAlterPartitioningSettings() {
        AlterPartitioningSettings_.ConstructInPlace(*this);
        return *AlterPartitioningSettings_;
    }

    TAlterTopicSettings& AlterPartitioningSettings(ui64 minActivePartitions, ui64 maxActivePartitions) {
        AlterPartitioningSettings_.ConstructInPlace(*this);
        AlterPartitioningSettings_->MinActivePartitions(minActivePartitions);
        AlterPartitioningSettings_->MaxActivePartitions(maxActivePartitions);
        return *this;
    }

    TMaybe<TAlterPartitioningSettings> AlterPartitioningSettings_;
};

inline TPartitioningSettingsBuilder TCreateTopicSettings::BeginConfigurePartitioningSettings() {
    return {*this};
}


// Settings for drop resource request.
struct TDropTopicSettings : public TOperationRequestSettings<TDropTopicSettings> {
    using TOperationRequestSettings<TDropTopicSettings>::TOperationRequestSettings;
};

// Settings for describe topic request.
struct TDescribeTopicSettings : public TOperationRequestSettings<TDescribeTopicSettings> {
    using TSelf = TDescribeTopicSettings;

    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);

    FLUENT_SETTING_DEFAULT(bool, IncludeLocation, false);
};

// Settings for describe consumer request.
struct TDescribeConsumerSettings : public TOperationRequestSettings<TDescribeConsumerSettings> {
    using TSelf = TDescribeConsumerSettings;

    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);

    FLUENT_SETTING_DEFAULT(bool, IncludeLocation, false);
};

// Settings for describe partition request.
struct TDescribePartitionSettings: public TOperationRequestSettings<TDescribePartitionSettings> {
    using TSelf = TDescribePartitionSettings;

    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);

    FLUENT_SETTING_DEFAULT(bool, IncludeLocation, false);
};

// Settings for commit offset request.
struct TCommitOffsetSettings : public TOperationRequestSettings<TCommitOffsetSettings> {};

}  // namespace NYdb::NTopic
