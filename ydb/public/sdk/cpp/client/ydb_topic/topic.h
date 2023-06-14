#pragma once
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>

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

enum class EMeteringMode : ui32 {
    Unspecified = 0,
    ReservedCapacity = 1,
    RequestUnits = 2,

    Unknown = std::numeric_limits<int>::max(),
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

private:
    ui64 PartitionId_;
    bool Active_;
    TVector<ui64> ChildPartitionIds_;
    TVector<ui64> ParentPartitionIds_;
    TMaybe<TPartitionStats> PartitionStats_;
    TMaybe<TPartitionConsumerStats> PartitionConsumerStats_;
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


// Result for describe resource request.
struct TDescribeTopicResult : public TStatus {
    friend class NYdb::TProtoAccessor;


    TDescribeTopicResult(TStatus&& status, Ydb::Topic::DescribeTopicResult&& result);

    const TTopicDescription& GetTopicDescription() const;

private:
    TTopicDescription TopicDescription_;
};

// Result for describe resource request.
struct TDescribeConsumerResult : public TStatus {
    friend class NYdb::TProtoAccessor;


    TDescribeConsumerResult(TStatus&& status, Ydb::Topic::DescribeConsumerResult&& result);

    const TConsumerDescription& GetConsumerDescription() const;

private:
    TConsumerDescription ConsumerDescription_;
};


using TAsyncDescribeTopicResult = NThreading::TFuture<TDescribeTopicResult>;
using TAsyncDescribeConsumerResult = NThreading::TFuture<TDescribeConsumerResult>;

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

    TAlterTopicSettings& AlterPartitioningSettings(ui64 minActivePartitions, ui64 partitionCountLimit) {
        AlterPartitioningSettings_ = TPartitioningSettings(minActivePartitions, partitionCountLimit);
        return *this;
    }
};


// Settings for drop resource request.
struct TDropTopicSettings : public TOperationRequestSettings<TDropTopicSettings> {
    using TOperationRequestSettings<TDropTopicSettings>::TOperationRequestSettings;
};

// Settings for describe resource request.
struct TDescribeTopicSettings : public TOperationRequestSettings<TDescribeTopicSettings> {
    using TSelf = TDescribeTopicSettings;

    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);
};

// Settings for describe resource request.
struct TDescribeConsumerSettings : public TOperationRequestSettings<TDescribeConsumerSettings> {
    using TSelf = TDescribeConsumerSettings;

    FLUENT_SETTING_DEFAULT(bool, IncludeStats, false);
};

// Settings for commit offset request.
struct TCommitOffsetSettings : public TOperationRequestSettings<TCommitOffsetSettings> {};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//! Session metainformation.
struct TWriteSessionMeta: public TThrRefBase {
    using TPtr = TIntrusivePtr<TWriteSessionMeta>;

    //! User defined fields.
    THashMap<TString, TString> Fields;
};

//! Event that is sent to client during session destruction.
struct TSessionClosedEvent: public TStatus {
    using TStatus::TStatus;

    TString DebugString() const;
};

struct TWriteStat : public TThrRefBase {
    TDuration WriteTime;
    TDuration MinTimeInPartitionQueue;
    TDuration MaxTimeInPartitionQueue;
    TDuration PartitionQuotedTime;
    TDuration TopicQuotedTime;
    using TPtr = TIntrusivePtr<TWriteStat>;
};

class TContinuationToken : public TMoveOnly {
    friend class TWriteSessionImpl;
private:
    TContinuationToken() = default;
};

struct TWriterCounters : public TThrRefBase {
    using TSelf = TWriterCounters;
    using TPtr = TIntrusivePtr<TSelf>;

    explicit TWriterCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CurrentSessionLifetimeMs;

    ::NMonitoring::TDynamicCounters::TCounterPtr BytesWritten;
    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesWritten;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesWrittenCompressed;

    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflightUncompressed;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflightCompressed;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflightTotal;
    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesInflight;

    //! Histograms reporting % usage of memory limit in time.
    //! Provides a histogram looking like: 10% : 100ms, 20%: 300ms, ... 50%: 200ms, ... 100%: 50ms
    //! Which means that < 10% memory usage was observed for 100ms during the period and 50% usage was observed for 200ms
    //! Used to monitor if the writer successfully deals with data flow provided. Larger values in higher buckets
    //! mean that writer is close to overflow (or being overflown) for major periods of time
    //! 3 histograms stand for:
    //! Total memory usage:
    ::NMonitoring::THistogramPtr TotalBytesInflightUsageByTime;
    //! Memory usage by messages waiting for comression:
    ::NMonitoring::THistogramPtr UncompressedBytesInflightUsageByTime;
    //! Memory usage by compressed messages pending for write:
    ::NMonitoring::THistogramPtr CompressedBytesInflightUsageByTime;
};

struct TReaderCounters: public TThrRefBase {
    using TSelf = TReaderCounters;
    using TPtr = TIntrusivePtr<TSelf>;

    TReaderCounters() = default;
    explicit TReaderCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    ::NMonitoring::TDynamicCounters::TCounterPtr Errors;
    ::NMonitoring::TDynamicCounters::TCounterPtr CurrentSessionLifetimeMs;

    ::NMonitoring::TDynamicCounters::TCounterPtr BytesRead;
    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesRead;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesReadCompressed;

    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflightUncompressed;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflightCompressed;
    ::NMonitoring::TDynamicCounters::TCounterPtr BytesInflightTotal;
    ::NMonitoring::TDynamicCounters::TCounterPtr MessagesInflight;

    //! Histograms reporting % usage of memory limit in time.
    //! Provides a histogram looking like: 10% : 100ms, 20%: 300ms, ... 50%: 200ms, ... 100%: 50ms
    //! Which means < 10% memory usage was observed for 100ms during the period and 50% usage was observed for 200ms.
    //! Used to monitor if the read session successfully deals with data flow provided. Larger values in higher buckets
    //! mean that read session is close to overflow (or being overflown) for major periods of time.
    //!
    //! Total memory usage.
    ::NMonitoring::THistogramPtr TotalBytesInflightUsageByTime;
    //! Memory usage by messages waiting that are ready to be received by user.
    ::NMonitoring::THistogramPtr UncompressedBytesInflightUsageByTime;
    //! Memory usage by compressed messages pending for decompression.
    ::NMonitoring::THistogramPtr CompressedBytesInflightUsageByTime;
};

//! Partition session.
struct TPartitionSession: public TThrRefBase {
    using TPtr = TIntrusivePtr<TPartitionSession>;

public:
    //! Request partition session status.
    //! Result will come to TPartitionSessionStatusEvent.
    virtual void RequestStatus() = 0;

    //!
    //! Properties.
    //!

    //! Unique identifier of partition session.
    //! It is unique within one read session.
    ui64 GetPartitionSessionId() const {
        return PartitionSessionId;
    }

    //! Topic path.
    const TString& GetTopicPath() const {
        return TopicPath;
    }

    //! Partition id.
    ui64 GetPartitionId() const {
        return PartitionId;
    }

protected:
    ui64 PartitionSessionId;
    TString TopicPath;
    ui64 PartitionId;
};

//! Events for read session.
struct TReadSessionEvent {
    //! Event with new data.
    //! Contains batch of messages from single partition session.
    struct TDataReceivedEvent {
        struct TMessageInformation {
            TMessageInformation(ui64 offset,
                                TString producerId,
                                ui64 seqNo,
                                TInstant createTime,
                                TInstant writeTime,
                                TWriteSessionMeta::TPtr meta,
                                TWriteSessionMeta::TPtr messageMeta,
                                ui64 uncompressedSize,
                                TString messageGroupId);
            ui64 Offset;
            TString ProducerId;
            ui64 SeqNo;
            TInstant CreateTime;
            TInstant WriteTime;
            TWriteSessionMeta::TPtr Meta;
            TWriteSessionMeta::TPtr MessageMeta;
            ui64 UncompressedSize;
            TString MessageGroupId;
        };

        class IMessage {
        public:
            IMessage(const TString& data, TPartitionSession::TPtr partitionSession);

            virtual ~IMessage() = default;

            virtual const TString& GetData() const;

            //! Partition session. Same as in batch.
            const TPartitionSession::TPtr& GetPartitionSession() const;

            virtual void Commit() = 0;

            TString DebugString(bool printData = false) const;
            virtual void DebugString(TStringBuilder& ret, bool printData = false) const = 0;

        protected:
            TString Data;

            TPartitionSession::TPtr PartitionSession;
        };

        //! Single message.
        struct TMessage: public IMessage {
            TMessage(const TString& data, std::exception_ptr decompressionException,
                     const TMessageInformation& information, TPartitionSession::TPtr partitionSession);

            //! User data.
            //! Throws decompressor exception if decompression failed.
            const TString& GetData() const override;

            bool HasException() const;

            //! Message offset.
            ui64 GetOffset() const;

            //! Producer id.
            const TString& GetProducerId() const;

            //! Message group id.
            const TString& GetMessageGroupId() const;

            //! Sequence number.
            ui64 GetSeqNo() const;

            //! Message creation timestamp.
            TInstant GetCreateTime() const;

            //! Message write timestamp.
            TInstant GetWriteTime() const;

            //! Metainfo.
            const TWriteSessionMeta::TPtr& GetMeta() const;

            //! Message level meta info.
            const TWriteSessionMeta::TPtr& GetMessageMeta() const;

            //! Commits single message.
            void Commit() override;

            using IMessage::DebugString;
            void DebugString(TStringBuilder& ret, bool printData = false) const override;

        private:
            std::exception_ptr DecompressionException;
            TMessageInformation Information;
        };

        struct TCompressedMessage: public IMessage {
            TCompressedMessage(ECodec codec, const TString& data, const TMessageInformation& information,
                               TPartitionSession::TPtr partitionSession);

            virtual ~TCompressedMessage() {
            }

            //! Message codec
            ECodec GetCodec() const;

            //! Message offset.
            ui64 GetOffset() const;

            //! Producer id
            const TString& GetProducerId() const;

            //! Message group id.
            const TString& GetMessageGroupId() const;

            //! Sequence number.
            ui64 GetSeqNo() const;

            //! Message creation timestamp.
            TInstant GetCreateTime() const;

            //! Message write timestamp.
            TInstant GetWriteTime() const;

            //! Metainfo.
            const TWriteSessionMeta::TPtr& GetMeta() const;

            //! Message level meta info.
            const TWriteSessionMeta::TPtr& GetMessageMeta() const;

            //! Uncompressed size.
            ui64 GetUncompressedSize() const;

            //! Commits all offsets in compressed message.
            void Commit() override;

            using IMessage::DebugString;
            void DebugString(TStringBuilder& ret, bool printData = false) const override;

        private:
            ECodec Codec;
            TMessageInformation Information;
        };

    public:
        TDataReceivedEvent(TVector<TMessage> messages, TVector<TCompressedMessage> compressedMessages,
                           TPartitionSession::TPtr partitionSession);

        //! Partition session.
        const TPartitionSession::TPtr& GetPartitionSession() const {
            return PartitionSession;
        }

        bool HasCompressedMessages() const {
            return !CompressedMessages.empty();
        }

        size_t GetMessagesCount() const {
            return Messages.size() + CompressedMessages.size();
        }

        //! Get messages.
        TVector<TMessage>& GetMessages() {
            CheckMessagesFilled(false);
            return Messages;
        }

        const TVector<TMessage>& GetMessages() const {
            CheckMessagesFilled(false);
            return Messages;
        }

        //! Get compressed messages.
        TVector<TCompressedMessage>& GetCompressedMessages() {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        const TVector<TCompressedMessage>& GetCompressedMessages() const {
            CheckMessagesFilled(true);
            return CompressedMessages;
        }

        //! Commits all messages in batch.
        void Commit();

        TString DebugString(bool printData = false) const;

    private:
        void CheckMessagesFilled(bool compressed) const {
            Y_VERIFY(!Messages.empty() || !CompressedMessages.empty());
            if (compressed && CompressedMessages.empty()) {
                ythrow yexception() << "cannot get compressed messages, parameter decompress=true for read session";
            }
            if (!compressed && Messages.empty()) {
                ythrow yexception() << "cannot get decompressed messages, parameter decompress=false for read session";
            }
        }

    private:
        TVector<TMessage> Messages;
        TVector<TCompressedMessage> CompressedMessages;
        TPartitionSession::TPtr PartitionSession;
        std::vector<std::pair<ui64, ui64>> OffsetRanges;
    };

    //! Acknowledgement for commit request.
    struct TCommitOffsetAcknowledgementEvent {
        TCommitOffsetAcknowledgementEvent(TPartitionSession::TPtr partitionSession, ui64 committedOffset);

        //! Partition session.
        const TPartitionSession::TPtr& GetPartitionSession() const {
            return PartitionSession;
        }

        //! Committed offset.
        //! This means that from now the first available
        //! message offset in current partition
        //! for current consumer is this offset.
        //! All messages before are committed and futher never be available.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        TString DebugString() const;

    private:
        TPartitionSession::TPtr PartitionSession;
        ui64 CommittedOffset;
    };

    //! Server command for creating and starting partition session.
    struct TStartPartitionSessionEvent {
        explicit TStartPartitionSessionEvent(TPartitionSession::TPtr, ui64 committedOffset, ui64 endOffset);

        const TPartitionSession::TPtr& GetPartitionSession() const {
            return PartitionSession;
        }

        //! Current committed offset in partition session.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Offset of first not existing message in partition session.
        ui64 GetEndOffset() const {
            return EndOffset;
        }

        //! Confirm partition session creation.
        //! This signals that user is ready to receive data from this partition session.
        //! If maybe is empty then no rewinding
        void Confirm(TMaybe<ui64> readOffset = Nothing(), TMaybe<ui64> commitOffset = Nothing());

        TString DebugString() const;

    private:
        TPartitionSession::TPtr PartitionSession;
        ui64 CommittedOffset;
        ui64 EndOffset;
    };

    //! Server command for stopping and destroying partition session.
    //! Server can destroy partition session gracefully
    //! for rebalancing among all topic clients.
    struct TStopPartitionSessionEvent {
        TStopPartitionSessionEvent(TPartitionSession::TPtr partitionSession, bool committedOffset);

        const TPartitionSession::TPtr& GetPartitionSession() const {
            return PartitionSession;
        }

        //! Last offset of the partition session that was committed.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Confirm partition session destruction.
        //! Confirm has no effect if TPartitionSessionClosedEvent for same partition session with is received.
        void Confirm();

        TString DebugString() const;

    private:
        TPartitionSession::TPtr PartitionSession;
        ui64 CommittedOffset;
    };

    //! Status for partition session requested via TPartitionSession::RequestStatus()
    struct TPartitionSessionStatusEvent {
        TPartitionSessionStatusEvent(TPartitionSession::TPtr partitionSession, ui64 committedOffset, ui64 readOffset,
                                     ui64 endOffset, TInstant writeTimeHighWatermark);

        const TPartitionSession::TPtr& GetPartitionSession() const {
            return PartitionSession;
        }

        //! Committed offset.
        ui64 GetCommittedOffset() const {
            return CommittedOffset;
        }

        //! Offset of next message (that is not yet read by session).
        ui64 GetReadOffset() const {
            return ReadOffset;
        }

        //! Offset of first not existing message in partition.
        ui64 GetEndOffset() const {
            return EndOffset;
        }

        //! Write time high watermark.
        //! Write timestamp of next message written to this partition will be no less than this.
        TInstant GetWriteTimeHighWatermark() const {
            return WriteTimeHighWatermark;
        }

        TString DebugString() const;

    private:
        TPartitionSession::TPtr PartitionSession;
        ui64 CommittedOffset = 0;
        ui64 ReadOffset = 0;
        ui64 EndOffset = 0;
        TInstant WriteTimeHighWatermark;
    };

    //! Event that signals user about
    //! partition session death.
    //! This could be after graceful stop of partition session
    //! or when connection with partition was lost.
    struct TPartitionSessionClosedEvent {
        enum class EReason {
            StopConfirmedByUser,
            Lost,
            ConnectionLost,
        };

    public:
        TPartitionSessionClosedEvent(TPartitionSession::TPtr partitionSession, EReason reason);

        const TPartitionSession::TPtr& GetPartitionSession() const {
            return PartitionSession;
        }

        EReason GetReason() const {
            return Reason;
        }

        TString DebugString() const;

    private:
        TPartitionSession::TPtr PartitionSession;
        EReason Reason;
    };

    using TEvent = std::variant<TDataReceivedEvent,
                                TCommitOffsetAcknowledgementEvent,
                                TStartPartitionSessionEvent,
                                TStopPartitionSessionEvent,
                                TPartitionSessionStatusEvent,
                                TPartitionSessionClosedEvent,
                                TSessionClosedEvent>;
};

//! Set of offsets to commit.
//! Class that could store offsets in order to commit them later.
//! This class is not thread safe.
class TDeferredCommit {
public:
    //! Add message to set.
    void Add(const TReadSessionEvent::TDataReceivedEvent::TMessage& message);

    //! Add all messages from dataReceivedEvent to set.
    void Add(const TReadSessionEvent::TDataReceivedEvent& dataReceivedEvent);

    //! Add offsets range to set.
    void Add(const TPartitionSession::TPtr& partitionSession, ui64 startOffset, ui64 endOffset);

    //! Add offset to set.
    void Add(const TPartitionSession::TPtr& partitionSession, ui64 offset);

    //! Commit all added offsets.
    void Commit();

    TDeferredCommit();
    TDeferredCommit(const TDeferredCommit&) = delete;
    TDeferredCommit(TDeferredCommit&&);
    TDeferredCommit& operator=(const TDeferredCommit&) = delete;
    TDeferredCommit& operator=(TDeferredCommit&&);

    ~TDeferredCommit();

private:
    class TImpl;
    THolder<TImpl> Impl;
};

//! Event debug string.
TString DebugString(const TReadSessionEvent::TEvent& event);

//! Retry policy.
//! Calculates delay before next retry.
//! Has several default implementations:
//! - exponential backoff policy;
//! - retries with fixed interval;
//! - no retries.

struct IRetryPolicy: ::IRetryPolicy<EStatus> {
    //!
    //! Default implementations.
    //!

    static TPtr GetDefaultPolicy(); // Exponential backoff with infinite retry attempts.
    static TPtr GetNoRetryPolicy(); // Denies all kind of retries.

    //! Randomized exponential backoff policy.
    static TPtr GetExponentialBackoffPolicy(
        TDuration minDelay = TDuration::MilliSeconds(10),
        // Delay for statuses that require waiting before retry (such as OVERLOADED).
        TDuration minLongRetryDelay = TDuration::MilliSeconds(200), TDuration maxDelay = TDuration::Seconds(30),
        size_t maxRetries = std::numeric_limits<size_t>::max(), TDuration maxTime = TDuration::Max(),
        double scaleFactor = 2.0, std::function<ERetryErrorClass(EStatus)> customRetryClassFunction = {});

    //! Randomized fixed interval policy.
    static TPtr GetFixedIntervalPolicy(TDuration delay = TDuration::MilliSeconds(100),
                                       // Delay for statuses that require waiting before retry (such as OVERLOADED).
                                       TDuration longRetryDelay = TDuration::MilliSeconds(300),
                                       size_t maxRetries = std::numeric_limits<size_t>::max(),
                                       TDuration maxTime = TDuration::Max(),
                                       std::function<ERetryErrorClass(EStatus)> customRetryClassFunction = {});
};

class IExecutor: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<IExecutor>;
    using TFunction = std::function<void()>;

    // Is executor asynchronous.
    virtual bool IsAsync() const = 0;

    // Post function to execute.
    virtual void Post(TFunction&& f) = 0;

    // Start method.
    // This method is idempotent.
    // It can be called many times. Only the first one has effect.
    void Start() {
        with_lock(StartLock) {
            if (!Started) {
                DoStart();
                Started = true;
            }
        }
    }

private:
    virtual void DoStart() = 0;

private:
    bool Started = false;
    TAdaptiveLock StartLock;
};
IExecutor::TPtr CreateThreadPoolExecutorAdapter(
    std::shared_ptr<IThreadPool> threadPool); // Thread pool is expected to have been started.
IExecutor::TPtr CreateThreadPoolExecutor(size_t threads);

IExecutor::TPtr CreateSyncExecutor();

//! Events for write session.
struct TWriteSessionEvent {

    //! Event with acknowledge for written messages.
    struct TWriteAck {
        //! Write result.
        enum EEventState {
            EES_WRITTEN, //! Successfully written.
            EES_ALREADY_WRITTEN, //! Skipped on SeqNo deduplication.
            EES_DISCARDED //! In case of destruction of writer or retry policy discarded future retries in this writer.
        };
        //! Details of successfully written message.
        struct TWrittenMessageDetails {
            ui64 Offset;
            ui64 PartitionId;
        };
        //! Same SeqNo as provided on write.
        ui64 SeqNo;
        EEventState State;
        //! Filled only for EES_WRITTEN. Empty for ALREADY and DISCARDED.
        TMaybe<TWrittenMessageDetails> Details;
        //! Write stats from server. See TWriteStat. nullptr for DISCARDED event.
        TWriteStat::TPtr Stat;

    };

    struct TAcksEvent {
        //! Acks could be batched from several WriteBatch/Write requests.
        //! Acks for messages from one WriteBatch request could be emitted as several TAcksEvents -
        //! they are provided to client as soon as possible.
        TVector<TWriteAck> Acks;

        TString DebugString() const;

    };

    //! Indicates that a writer is ready to accept new message(s).
    //! Continuation token should be kept and then used in write methods.
    struct TReadyToAcceptEvent {
        TContinuationToken ContinuationToken;

        TString DebugString() const;

    };

    using TEvent = std::variant<TAcksEvent, TReadyToAcceptEvent, TSessionClosedEvent>;
};

//! Event debug string.
TString DebugString(const TWriteSessionEvent::TEvent& event);

using TSessionClosedHandler = std::function<void(const TSessionClosedEvent&)>;

//! Settings for write session.
struct TWriteSessionSettings : public TRequestSettings<TWriteSessionSettings> {
    using TSelf = TWriteSessionSettings;

    TWriteSessionSettings() = default;
    TWriteSessionSettings(const TWriteSessionSettings&) = default;
    TWriteSessionSettings(TWriteSessionSettings&&) = default;
    TWriteSessionSettings(const TString& path, const TString& producerId, const TString& messageGroupId) {
        Path(path);
        ProducerId(producerId);
        MessageGroupId(messageGroupId);
    }

    TWriteSessionSettings& operator=(const TWriteSessionSettings&) = default;
    TWriteSessionSettings& operator=(TWriteSessionSettings&&) = default;

    //! Path of topic to write.
    FLUENT_SETTING(TString, Path);

    //! ProducerId (aka SourceId) to use.
    FLUENT_SETTING(TString, ProducerId);

    //! MessageGroupId to use.
    FLUENT_SETTING(TString, MessageGroupId);

    //! Explicitly enables or disables deduplication for this write session.
    //! If ProducerId option is defined deduplication will always be enabled.
    //! If ProducerId option is empty, but deduplication is enable, a random ProducerId is generated.
    FLUENT_SETTING_OPTIONAL(bool, DeduplicationEnabled);

    //! Write to an exact partition. Generally server assigns partition automatically by message_group_id.
    //! Using this option is not recommended unless you know for sure why you need it.
    FLUENT_SETTING_OPTIONAL(ui32, PartitionId);

    //! codec and level to use for data compression prior to write.
    FLUENT_SETTING_DEFAULT(ECodec, Codec, ECodec::GZIP);
    FLUENT_SETTING_DEFAULT(i32, CompressionLevel, 4);

    //! Writer will not accept new messages if memory usage exceeds this limit.
    //! Memory usage consists of raw data pending compression and compressed messages being sent.
    FLUENT_SETTING_DEFAULT(ui64, MaxMemoryUsage, 20_MB);

    //! Maximum messages accepted by writer but not written (with confirmation from server).
    //! Writer will not accept new messages after reaching the limit.
    FLUENT_SETTING_DEFAULT(ui32, MaxInflightCount, 100000);

    //! Retry policy enables automatic retries for non-fatal errors.
    //! IRetryPolicy::GetDefaultPolicy() if null (not set).
    FLUENT_SETTING(IRetryPolicy::TPtr, RetryPolicy);

    //! User metadata that may be attached to write session.
    TWriteSessionSettings& AppendSessionMeta(const TString& key, const TString& value) {
        Meta_.Fields[key] = value;
        return *this;
    };

    TWriteSessionMeta Meta_;

    //! Writer will accumulate messages until reaching up to BatchFlushSize bytes
    //! but for no longer than BatchFlushInterval.
    //! Upon reaching FlushInterval or FlushSize limit, all messages will be written with one batch.
    //! Greatly increases performance for small messages.
    //! Setting either value to zero means immediate write with no batching. (Unrecommended, especially for clients
    //! sending small messages at high rate).
    FLUENT_SETTING_OPTIONAL(TDuration, BatchFlushInterval);
    FLUENT_SETTING_OPTIONAL(ui64, BatchFlushSizeBytes);

    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Seconds(30));

    FLUENT_SETTING_OPTIONAL(TWriterCounters::TPtr, Counters);

    //! Executor for compression tasks.
    //! If not set, default executor will be used.
    FLUENT_SETTING(IExecutor::TPtr, CompressionExecutor);

    struct TEventHandlers {
        using TSelf = TEventHandlers;
        using TWriteAckHandler = std::function<void(TWriteSessionEvent::TAcksEvent&)>;
        using TReadyToAcceptHandler = std::function<void(TWriteSessionEvent::TReadyToAcceptEvent&)>;

        //! Function to handle Acks events.
        //! If this handler is set, write ack events will be handled by handler,
        //! otherwise sent to TWriteSession::GetEvent().
        FLUENT_SETTING(TWriteAckHandler, AcksHandler);

        //! Function to handle ReadyToAccept event.
        //! If this handler is set, write these events will be handled by handler,
        //! otherwise sent to TWriteSession::GetEvent().
        FLUENT_SETTING(TReadyToAcceptHandler, ReadyToAcceptHander);

        //! Function to handle close session events.
        //! If this handler is set, close session events will be handled by handler
        //! and then sent to TWriteSession::GetEvent().
        FLUENT_SETTING(TSessionClosedHandler, SessionClosedHandler);

        //! Function to handle all event types.
        //! If event with current type has no handler for this type of event,
        //! this handler (if specified) will be used.
        //! If this handler is not specified, event can be received with TWriteSession::GetEvent() method.
        std::function<void(TWriteSessionEvent::TEvent&)> CommonHandler_;
        TSelf& CommonHandler(std::function<void(TWriteSessionEvent::TEvent&)>&& handler) {
            CommonHandler_ = std::move(handler);
            return static_cast<TSelf&>(*this);
        }

        //! Executor for handlers.
        //! If not set, default single threaded executor will be used.
        FLUENT_SETTING(IExecutor::TPtr, HandlersExecutor);
    };

    //! Event handlers.
    FLUENT_SETTING(TEventHandlers, EventHandlers);

    //! Enables validation of SeqNo. If enabled, then writer will check writing with seqNo and without it and throws exception.
    FLUENT_SETTING_DEFAULT(bool, ValidateSeqNo, true);
};

//! Read settings for single topic.
struct TTopicReadSettings {
    using TSelf = TTopicReadSettings;

    TTopicReadSettings() = default;
    TTopicReadSettings(const TTopicReadSettings&) = default;
    TTopicReadSettings(TTopicReadSettings&&) = default;
    TTopicReadSettings(const TString& path) {
        Path(path);
    }

    TTopicReadSettings& operator=(const TTopicReadSettings&) = default;
    TTopicReadSettings& operator=(TTopicReadSettings&&) = default;

    //! Path of topic to read.
    FLUENT_SETTING(TString, Path);

    //! Start reading from this timestamp.
    FLUENT_SETTING_OPTIONAL(TInstant, ReadFromTimestamp);

    //! Partition ids to read.
    //! 0-based.
    FLUENT_SETTING_VECTOR(ui64, PartitionIds);

    //! Max message time lag. All messages older that now - MaxLag will be ignored.
    FLUENT_SETTING_OPTIONAL(TDuration, MaxLag);
};

//! Settings for read session.
struct TReadSessionSettings: public TRequestSettings<TReadSessionSettings> {
    using TSelf = TReadSessionSettings;

    struct TEventHandlers {
        using TSelf = TEventHandlers;

        //! Set simple handler with data processing and also
        //! set other handlers with default behaviour.
        //! They automatically commit data after processing
        //! and confirm partition session events.
        //!
        //! Sets the following handlers:
        //! DataReceivedHandler: sets DataReceivedHandler to handler that calls dataHandler and (if
        //! commitDataAfterProcessing is set) then calls Commit(). CommitAcknowledgementHandler to handler that does
        //! nothing. CreatePartitionSessionHandler to handler that confirms event. StopPartitionSessionHandler to
        //! handler that confirms event. PartitionSessionStatusHandler to handler that does nothing.
        //! PartitionSessionClosedHandler to handler that does nothing.
        //!
        //! dataHandler: handler of data event.
        //! commitDataAfterProcessing: automatically commit data after calling of dataHandler.
        //! gracefulReleaseAfterCommit: wait for commit acknowledgements for all inflight data before confirming
        //! partition session destroy.
        TSelf& SimpleDataHandlers(std::function<void(TReadSessionEvent::TDataReceivedEvent&)> dataHandler,
                                  bool commitDataAfterProcessing = false, bool gracefulStopAfterCommit = true);

        //! Data size limit for the DataReceivedHandler handler.
        //! The data size may exceed this limit.
        FLUENT_SETTING_DEFAULT(size_t, MaxMessagesBytes, Max<size_t>());

        //! Function to handle data events.
        //! If this handler is set, data events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TDataReceivedEvent&)>, DataReceivedHandler);

        //! Function to handle commit ack events.
        //! If this handler is set, commit ack events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TCommitOffsetAcknowledgementEvent&)>,
                       CommitOffsetAcknowledgementHandler);

        //! Function to handle start partition session events.
        //! If this handler is set, create partition session events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TStartPartitionSessionEvent&)>,
                       StartPartitionSessionHandler);

        //! Function to handle stop partition session events.
        //! If this handler is set, destroy partition session events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TStopPartitionSessionEvent&)>,
                       StopPartitionSessionHandler);

        //! Function to handle partition session status events.
        //! If this handler is set, partition session status events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TPartitionSessionStatusEvent&)>,
                       PartitionSessionStatusHandler);

        //! Function to handle partition session closed events.
        //! If this handler is set, partition session closed events will be handled by handler,
        //! otherwise sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TPartitionSessionClosedEvent&)>,
                       PartitionSessionClosedHandler);

        //! Function to handle session closed events.
        //! If this handler is set, close session events will be handled by handler
        //! and then sent to TReadSession::GetEvent().
        //! Default value is empty function (not set).
        FLUENT_SETTING(TSessionClosedHandler, SessionClosedHandler);

        //! Function to handle all event types.
        //! If event with current type has no handler for this type of event,
        //! this handler (if specified) will be used.
        //! If this handler is not specified, event can be received with TReadSession::GetEvent() method.
        FLUENT_SETTING(std::function<void(TReadSessionEvent::TEvent&)>, CommonHandler);

        //! Executor for handlers.
        //! If not set, default single threaded executor will be used.
        FLUENT_SETTING(IExecutor::TPtr, HandlersExecutor);
    };

    //! Consumer.
    FLUENT_SETTING(TString, ConsumerName);

    //! Topics.
    FLUENT_SETTING_VECTOR(TTopicReadSettings, Topics);

    //! Maximum memory usage for read session.
    FLUENT_SETTING_DEFAULT(size_t, MaxMemoryUsageBytes, 100_MB);

    //! Max message time lag. All messages older that now - MaxLag will be ignored.
    FLUENT_SETTING_OPTIONAL(TDuration, MaxLag);

    //! Start reading from this timestamp.
    FLUENT_SETTING_OPTIONAL(TInstant, ReadFromTimestamp);

    //! Policy for reconnections.
    //! IRetryPolicy::GetDefaultPolicy() if null (not set).
    FLUENT_SETTING(IRetryPolicy::TPtr, RetryPolicy);

    //! Event handlers.
    //! See description in TEventHandlers class.
    FLUENT_SETTING(TEventHandlers, EventHandlers);

    //! Decompress messages
    FLUENT_SETTING_DEFAULT(bool, Decompress, true);

    //! Executor for decompression tasks.
    //! If not set, default executor will be used.
    FLUENT_SETTING(IExecutor::TPtr, DecompressionExecutor);

    //! Counters.
    //! If counters are not provided explicitly,
    //! they will be created inside session (without link with parent counters).
    FLUENT_SETTING(TReaderCounters::TPtr, Counters);

    FLUENT_SETTING_DEFAULT(TDuration, ConnectTimeout, TDuration::Seconds(30));

    //! Log.
    FLUENT_SETTING_OPTIONAL(TLog, Log);
};

//! Simple write session. Does not need event handlers. Does not provide Events, ContinuationTokens, write Acks.
class ISimpleBlockingWriteSession : public TThrRefBase {
public:
    //! Write single message. Blocks for up to blockTimeout if inflight is full or memoryUsage is exceeded;
    //! return - true if write succeeded, false if message was not enqueued for write within blockTimeout.
    //! no Ack is provided.
    virtual bool Write(TStringBuf data, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing(),
                       const TDuration& blockTimeout = TDuration::Max()) = 0;

    //! Blocks till SeqNo is discovered from server. Returns 0 in case of failure on init.
    virtual ui64 GetInitSeqNo() = 0;

    //! Complete all active writes, wait for ack from server and close.
    //! closeTimeout - max time to wait. Empty Maybe means infinity.
    //! return - true if all writes were completed and acked. false if timeout was reached and some writes were aborted.

    virtual bool Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Returns true if write session is alive and acitve. False if session was closed.
    virtual bool IsAlive() const = 0;

    virtual TWriterCounters::TPtr GetCounters() = 0;

    //! Close immediately and destroy, don't wait for anything.
    virtual ~ISimpleBlockingWriteSession() = default;
};

//! Generic write session with all capabilities.
class IWriteSession {
public:
    //! Future that is set when next event is available.
    virtual NThreading::TFuture<void> WaitEvent() = 0;

    //! Wait and return next event. Use WaitEvent() for non-blocking wait.
    virtual TMaybe<TWriteSessionEvent::TEvent> GetEvent(bool block = false) = 0;

    //! Get several events in one call.
    //! If blocking = false, instantly returns up to maxEventsCount available events.
    //! If blocking = true, blocks till maxEventsCount events are available.
    //! If maxEventsCount is unset, write session decides the count to return itself.
    virtual TVector<TWriteSessionEvent::TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing()) = 0;

    //! Future that is set when initial SeqNo is available.
    virtual NThreading::TFuture<ui64> GetInitSeqNo() = 0;

    //! Write single message.
    //! continuationToken - a token earlier provided to client with ReadyToAccept event.
    virtual void Write(TContinuationToken&& continuationToken, TStringBuf data, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) = 0;

    //! Write single message that is already coded by codec. Codec from settings does not apply to this message.
    //! continuationToken - a token earlier provided to client with ReadyToAccept event.
    //! originalSize - size of unpacked message
    virtual void WriteEncoded(TContinuationToken&& continuationToken, TStringBuf data, ECodec codec, ui32 originalSize, TMaybe<ui64> seqNo = Nothing(), TMaybe<TInstant> createTimestamp = Nothing()) = 0;


    //! Wait for all writes to complete (no more that closeTimeout()), than close. Empty maybe - means infinite timeout.
    //! return - true if all writes were completed and acked. false if timeout was reached and some writes were aborted.
    virtual bool Close(TDuration closeTimeout = TDuration::Max()) = 0;

    //! Writer counters with different stats (see TWriterConuters).
    virtual TWriterCounters::TPtr GetCounters() = 0;

    //! Close() with timeout = 0 and destroy everything instantly.
    virtual ~IWriteSession() = default;
};

class IReadSession {
public:
    //! Main reader loop.
    //! Wait for next reader event.
    virtual NThreading::TFuture<void> WaitEvent() = 0;

    //! Main reader loop.
    //! Get read session events.
    //! Blocks until event occurs if "block" is set.
    //!
    //! maxEventsCount: maximum events count in batch.
    //! maxByteSize: total size limit of data messages in batch.
    //! block: block until event occurs.
    //!
    //! If maxEventsCount is not specified,
    //! read session chooses event batch size automatically.
    virtual TVector<TReadSessionEvent::TEvent> GetEvents(bool block = false, TMaybe<size_t> maxEventsCount = Nothing(),
                                                         size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    //! Get single event.
    virtual TMaybe<TReadSessionEvent::TEvent> GetEvent(bool block = false,
                                                       size_t maxByteSize = std::numeric_limits<size_t>::max()) = 0;

    //! Close read session.
    //! Waits for all commit acknowledgments to arrive.
    //! Force close after timeout.
    //! This method is blocking.
    //! When session is closed,
    //! TSessionClosedEvent arrives.
    virtual bool Close(TDuration timeout = TDuration::Max()) = 0;

    //! Reader counters with different stats (see TReaderConuters).
    virtual TReaderCounters::TPtr GetCounters() const = 0;

    //! Get unique identifier of read session.
    virtual TString GetSessionId() const = 0;

    virtual ~IReadSession() = default;
};

struct TTopicClientSettings : public TCommonClientSettingsBase<TTopicClientSettings> {
    using TSelf = TTopicClientSettings;

    //! Default executor for compression tasks.
    FLUENT_SETTING_DEFAULT(IExecutor::TPtr, DefaultCompressionExecutor, CreateThreadPoolExecutor(2));

    //! Default executor for callbacks.
    FLUENT_SETTING_DEFAULT(IExecutor::TPtr, DefaultHandlersExecutor, CreateThreadPoolExecutor(1));
};

// Topic client.
class TTopicClient {
public:
    class TImpl;

    TTopicClient(const TDriver& driver, const TTopicClientSettings& settings = TTopicClientSettings());

    // Create a new topic.
    TAsyncStatus CreateTopic(const TString& path, const TCreateTopicSettings& settings = {});

    // Update a topic.
    TAsyncStatus AlterTopic(const TString& path, const TAlterTopicSettings& settings = {});

    // Delete a topic.
    TAsyncStatus DropTopic(const TString& path, const TDropTopicSettings& settings = {});

    // Describe settings of topic.
    TAsyncDescribeTopicResult DescribeTopic(const TString& path, const TDescribeTopicSettings& settings = {});

    // Describe settings of topic's consumer.
    TAsyncDescribeConsumerResult DescribeConsumer(const TString& path, const TString& consumer,
        const TDescribeConsumerSettings& settings = {});

    //! Create read session.
    std::shared_ptr<IReadSession> CreateReadSession(const TReadSessionSettings& settings);

    //! Create write session.
    std::shared_ptr<ISimpleBlockingWriteSession> CreateSimpleBlockingWriteSession(const TWriteSessionSettings& settings);
    std::shared_ptr<IWriteSession> CreateWriteSession(const TWriteSessionSettings& settings);

    // Commit offset
    TAsyncStatus CommitOffset(const TString& path, ui64 partitionId, const TString& consumerName, ui64 offset,
        const TCommitOffsetSettings& settings = {});

private:
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NTopic
