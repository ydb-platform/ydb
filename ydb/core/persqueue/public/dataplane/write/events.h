#pragma once

#include <ydb/core/persqueue/public/dataplane/dataplane.h>
#include <ydb/core/persqueue/writer/writer.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <optional>

namespace NKikimr::NPQ::NDataplane::NWrite {

template <typename T>
std::optional<T> ToOptional(const TMaybe<T>& value) {
    if (value) {
        return *value;
    }
    return std::nullopt;
}

template <typename T>
TMaybe<T> ToMaybe(const std::optional<T>& value) {
    if (value) {
        return *value;
    }
    return Nothing();
}

struct TWriteSessionMessage {
    i64 SeqNo = 0;
    i64 CmdSeqNo = 0;
    i64 ExpectedAckSeqNo = 0;
    ui64 CreateTimeMs = 0;
    ui64 UncompressedSize = 0;
    ui32 CodecId = 0;
    bool SkipCodecValidation = false;
    std::optional<ui32> ChunkCodec;
    TString Data;
    TVector<std::pair<TString, TString>> Metadata;
    std::optional<ui32> LogicalMessageCount;
    std::optional<i64> MaxSeqNo;
};

struct TWriteSessionAck {
    i64 SeqNo = 0;
    ui64 Offset = 0;
    bool AlreadyWritten = false;
    bool WrittenInTx = false;
    ui64 TotalTimeInPartitionQueueMs = 0;
    ui64 PartitionQuotedTimeMs = 0;
    ui64 TopicQuotedTimeMs = 0;
    ui64 WriteTimeMs = 0;
};

struct TEvInit : public NActors::TEventLocal<TEvInit, EEv::EvWriteInit> {
    TString TopicPath;
    TString PeerName;
    TString SourceId;
    bool UseDeduplication = true;
    ui32 PreferedPartition = Max<ui32>();
    std::optional<ui32> ExpectedGeneration;
    std::optional<bool> TrackProducerId;
    THashMap<TString, TString> SessionMeta;
};

struct TEvWrite : public NActors::TEventLocal<TEvWrite, EEv::EvWrite> {
    TVector<TWriteSessionMessage> Messages;
    std::optional<std::pair<TString, TString>> Tx;
    std::optional<TDeferredPublishWriterOpts> DeferredPublish;
    ui64 UserRequestByteSize = 0;
};

struct TEvUpdateToken : public NActors::TEventLocal<TEvUpdateToken, EEv::EvWriteUpdateToken> {
    TString Token;
};

struct TEvTokenRefreshed : public NActors::TEventLocal<TEvTokenRefreshed, EEv::EvWriteTokenRefreshed> {
    TIntrusiveConstPtr<NACLib::TUserToken> InternalToken;
};

struct TEvClientDone : public NActors::TEventLocal<TEvClientDone, EEv::EvWriteClientDone> {
};

struct TEvDieCommand : public NActors::TEventLocal<TEvDieCommand, EEv::EvWriteDieCommand> {
    TEvDieCommand(TString reason, Ydb::PersQueue::ErrorCode::ErrorCode errorCode,
                  std::optional<Ydb::StatusIds::StatusCode> statusOverride = std::nullopt);

    TString Reason;
    Ydb::PersQueue::ErrorCode::ErrorCode ErrorCode;
    std::optional<Ydb::StatusIds::StatusCode> StatusOverride;
};

struct TEvInitAck : public NActors::TEventLocal<TEvInitAck, EEv::EvWriteInitAck> {
    TString SessionId;
    ui32 PartitionId = 0;
    std::optional<ui64> LastSeqNo;
    TVector<TString> SupportedCodecNames;
    TString FederationPath;
    TString Cluster;
    bool BatchingSupported = false;
};

struct TEvWriteAck : public NActors::TEventLocal<TEvWriteAck, EEv::EvWriteAck> {
    ui32 PartitionId = 0;
    TVector<TWriteSessionAck> Acks;
};

struct TEvUpdateTokenAck : public NActors::TEventLocal<TEvUpdateTokenAck, EEv::EvWriteUpdateTokenAck> {
};

struct TEvRefreshToken : public NActors::TEventLocal<TEvRefreshToken, EEv::EvWriteRefreshToken> {
    TString Token;
    NWilson::TTraceId TraceId;
};

struct TEvUnauthenticated : public NActors::TEventLocal<TEvUnauthenticated, EEv::EvWriteUnauthenticated> {
    explicit TEvUnauthenticated(TString reason);

    TString Reason;
};

struct TEvClosed : public NActors::TEventLocal<TEvClosed, EEv::EvWriteClosed> {
    TString ErrorReason;
    Ydb::PersQueue::ErrorCode::ErrorCode ErrorCode = Ydb::PersQueue::ErrorCode::OK;
    std::optional<Ydb::StatusIds::StatusCode> StatusOverride;
};

struct TEvReadNext : public NActors::TEventLocal<TEvReadNext, EEv::EvWriteReadNext> {
};

struct TEvConsumedRequestUnits : public NActors::TEventLocal<TEvConsumedRequestUnits, EEv::EvWriteConsumedRequestUnits> {
    explicit TEvConsumedRequestUnits(ui64 amount);

    ui64 Amount = 0;
};

} // namespace NKikimr::NPQ::NDataplane::NWrite
