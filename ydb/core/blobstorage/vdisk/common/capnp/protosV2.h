#pragma once

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <kj/io.h>
#include <string>
#include <vector>
#include <optional>
#include <library/cpp/actors/core/event_pb.h>

#include "protos.capnp.h"

namespace NKikimrCapnProtoUtil {
    struct TRopeStream : public kj::InputStream {
        NActors::TRopeStream *underlying;

        virtual size_t tryRead(void* dst, size_t minBytes, size_t maxBytes) override {
            maxBytes = maxBytes + 1;

            size_t bytesRead = 0;
            while (bytesRead < minBytes) {
                const void* buf;
                int size;
                if (!underlying->Next(&buf, &size)) {
                    break;
                }
                memcpy((char*)dst + bytesRead, buf, size);
                bytesRead += size;
            }
            return bytesRead;
        }
    };
};

namespace NKikimrCapnProto {
    using namespace NKikimrCapnProto_;

    struct TMessageId {
        struct Reader : private NKikimrCapnProto_::TMessageId::Reader {
        public:
            Reader(NKikimrCapnProto_::TMessageId::Reader r) : NKikimrCapnProto_::TMessageId::Reader(r) {}
            Reader() = default;
            uint64_t GetSequenceId() const { return getSequenceId(); }
            uint64_t GetMsgId() const { return getMsgId(); }
            bool HasSequenceId() const { return getSequenceId() != 0; }
            bool HasMsgId() const { return getMsgId() != 0; }
            const NKikimrCapnProto_::TMessageId::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TMessageId::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TMessageId::Builder b) : NKikimrCapnProto_::TMessageId::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetSequenceId(const uint64_t& value) { return setSequenceId(value); }
            void SetMsgId(const uint64_t& value) { return setMsgId(value); }
            const NKikimrCapnProto_::TMessageId::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TTimestamps {
        struct Reader : private NKikimrCapnProto_::TTimestamps::Reader {
        public:
            Reader(NKikimrCapnProto_::TTimestamps::Reader r) : NKikimrCapnProto_::TTimestamps::Reader(r) {}
            Reader() = default;
            uint64_t GetSentByDSProxyUs() const { return getSentByDSProxyUs(); }
            uint64_t GetReceivedByVDiskUs() const { return getReceivedByVDiskUs(); }
            uint64_t GetSentByVDiskUs() const { return getSentByVDiskUs(); }
            uint64_t GetReceivedByDSProxyUs() const { return getReceivedByDSProxyUs(); }
            bool HasSentByDSProxyUs() const { return getSentByDSProxyUs() != 0; }
            bool HasReceivedByVDiskUs() const { return getReceivedByVDiskUs() != 0; }
            bool HasSentByVDiskUs() const { return getSentByVDiskUs() != 0; }
            bool HasReceivedByDSProxyUs() const { return getReceivedByDSProxyUs() != 0; }
            const NKikimrCapnProto_::TTimestamps::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TTimestamps::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TTimestamps::Builder b) : NKikimrCapnProto_::TTimestamps::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetSentByDSProxyUs(const uint64_t& value) { return setSentByDSProxyUs(value); }
            void SetReceivedByVDiskUs(const uint64_t& value) { return setReceivedByVDiskUs(value); }
            void SetSentByVDiskUs(const uint64_t& value) { return setSentByVDiskUs(value); }
            void SetReceivedByDSProxyUs(const uint64_t& value) { return setReceivedByDSProxyUs(value); }
            const NKikimrCapnProto_::TTimestamps::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EGetHandleClass {
        AsyncRead,
        FastRead,
        Discover,
        LowRead,
    };

    struct TActorId {
        struct Reader : private NKikimrCapnProto_::TActorId::Reader {
        public:
            Reader(NKikimrCapnProto_::TActorId::Reader r) : NKikimrCapnProto_::TActorId::Reader(r) {}
            Reader() = default;
            uint64_t GetRawX1() const { return getRawX1(); }
            uint64_t GetRawX2() const { return getRawX2(); }
            bool HasRawX1() const { return getRawX1() != 0; }
            bool HasRawX2() const { return getRawX2() != 0; }
            const NKikimrCapnProto_::TActorId::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TActorId::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TActorId::Builder b) : NKikimrCapnProto_::TActorId::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetRawX1(const uint64_t& value) { return setRawX1(value); }
            void SetRawX2(const uint64_t& value) { return setRawX2(value); }
            const NKikimrCapnProto_::TActorId::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TExecTimeStats {
        struct Reader : private NKikimrCapnProto_::TExecTimeStats::Reader {
        public:
            Reader(NKikimrCapnProto_::TExecTimeStats::Reader r) : NKikimrCapnProto_::TExecTimeStats::Reader(r) {}
            Reader() = default;
            uint64_t GetSubmitTimestamp() const { return getSubmitTimestamp(); }
            uint64_t GetInSenderQueue() const { return getInSenderQueue(); }
            uint64_t GetReceivedTimestamp() const { return getReceivedTimestamp(); }
            uint64_t GetTotal() const { return getTotal(); }
            uint64_t GetInQueue() const { return getInQueue(); }
            uint64_t GetExecution() const { return getExecution(); }
            uint64_t GetHugeWriteTime() const { return getHugeWriteTime(); }
            bool HasSubmitTimestamp() const { return getSubmitTimestamp() != 0; }
            bool HasInSenderQueue() const { return getInSenderQueue() != 0; }
            bool HasReceivedTimestamp() const { return getReceivedTimestamp() != 0; }
            bool HasTotal() const { return getTotal() != 0; }
            bool HasInQueue() const { return getInQueue() != 0; }
            bool HasExecution() const { return getExecution() != 0; }
            bool HasHugeWriteTime() const { return getHugeWriteTime() != 0; }
            const NKikimrCapnProto_::TExecTimeStats::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TExecTimeStats::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TExecTimeStats::Builder b) : NKikimrCapnProto_::TExecTimeStats::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetSubmitTimestamp(const uint64_t& value) { return setSubmitTimestamp(value); }
            void SetInSenderQueue(const uint64_t& value) { return setInSenderQueue(value); }
            void SetReceivedTimestamp(const uint64_t& value) { return setReceivedTimestamp(value); }
            void SetTotal(const uint64_t& value) { return setTotal(value); }
            void SetInQueue(const uint64_t& value) { return setInQueue(value); }
            void SetExecution(const uint64_t& value) { return setExecution(value); }
            void SetHugeWriteTime(const uint64_t& value) { return setHugeWriteTime(value); }
            const NKikimrCapnProto_::TExecTimeStats::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EStatus {
        Unknown,
        Success,
        WindowUpdate,
        Processed,
        IncorrectMsgId,
        HighWatermarkOverflow,
    };

    struct TWindowFeedback {
        struct Reader : private NKikimrCapnProto_::TWindowFeedback::Reader {
        public:
            Reader(NKikimrCapnProto_::TWindowFeedback::Reader r) : NKikimrCapnProto_::TWindowFeedback::Reader(r) {}
            Reader() = default;
            uint64_t GetActualWindowSize() const { return getActualWindowSize(); }
            uint64_t GetMaxWindowSize() const { return getMaxWindowSize(); }
            TMessageId::Reader GetExpectedMsgId() const { return getExpectedMsgId(); }
            TMessageId::Reader GetFailedMsgId() const { return getFailedMsgId(); }
            EStatus GetStatus() const { return static_cast<EStatus>(static_cast<size_t>(getStatus()) - 1); }
            bool HasExpectedMsgId() const { return hasExpectedMsgId(); }
            bool HasFailedMsgId() const { return hasFailedMsgId(); }
            bool HasStatus() const { return getStatus() != NKikimrCapnProto_::EStatus::NOT_SET; }
            bool HasActualWindowSize() const { return getActualWindowSize() != 0; }
            bool HasMaxWindowSize() const { return getMaxWindowSize() != 0; }
            const NKikimrCapnProto_::TWindowFeedback::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TWindowFeedback::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TWindowFeedback::Builder::getExpectedMsgId;
            using NKikimrCapnProto_::TWindowFeedback::Builder::getFailedMsgId;
        public:
            Builder(NKikimrCapnProto_::TWindowFeedback::Builder b) : NKikimrCapnProto_::TWindowFeedback::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetActualWindowSize(const uint64_t& value) { return setActualWindowSize(value); }
            void SetMaxWindowSize(const uint64_t& value) { return setMaxWindowSize(value); }
            void SetExpectedMsgId(const TMessageId::Reader& value) { return setExpectedMsgId(value.GetCapnpBase()); }
            void SetFailedMsgId(const TMessageId::Reader& value) { return setFailedMsgId(value.GetCapnpBase()); }
            void SetStatus(const EStatus& value) { return setStatus(static_cast<NKikimrCapnProto_::EStatus>(static_cast<size_t>(value) + 1)); }
            TMessageId::Builder MutableExpectedMsgId() { return getExpectedMsgId(); }
            TMessageId::Builder MutableFailedMsgId() { return getFailedMsgId(); }
            const NKikimrCapnProto_::TWindowFeedback::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EVDiskQueueId {
        Unknown,
        PutTabletLog,
        PutAsyncBlob,
        PutUserData,
        GetAsyncRead,
        GetFastRead,
        GetDiscover,
        GetLowRead,
        Begin,
        End,
    };

    enum class EVDiskInternalQueueId {
        IntUnknown,
        IntBegin,
        IntGetAsync,
        IntGetFast,
        IntPutLog,
        IntPutHugeForeground,
        IntPutHugeBackground,
        IntGetDiscover,
        IntLowRead,
        IntEnd,
    };

    struct TVDiskCostSettings {
        struct Reader : private NKikimrCapnProto_::TVDiskCostSettings::Reader {
        public:
            Reader(NKikimrCapnProto_::TVDiskCostSettings::Reader r) : NKikimrCapnProto_::TVDiskCostSettings::Reader(r) {}
            Reader() = default;
            uint64_t GetSeekTimeUs() const { return getSeekTimeUs(); }
            uint64_t GetReadSpeedBps() const { return getReadSpeedBps(); }
            uint64_t GetWriteSpeedBps() const { return getWriteSpeedBps(); }
            uint64_t GetReadBlockSize() const { return getReadBlockSize(); }
            uint64_t GetWriteBlockSize() const { return getWriteBlockSize(); }
            uint32_t GetMinREALHugeBlobInBytes() const { return getMinREALHugeBlobInBytes(); }
            bool HasSeekTimeUs() const { return getSeekTimeUs() != 0; }
            bool HasReadSpeedBps() const { return getReadSpeedBps() != 0; }
            bool HasWriteSpeedBps() const { return getWriteSpeedBps() != 0; }
            bool HasReadBlockSize() const { return getReadBlockSize() != 0; }
            bool HasWriteBlockSize() const { return getWriteBlockSize() != 0; }
            bool HasMinREALHugeBlobInBytes() const { return getMinREALHugeBlobInBytes() != 0; }
            const NKikimrCapnProto_::TVDiskCostSettings::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TVDiskCostSettings::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TVDiskCostSettings::Builder b) : NKikimrCapnProto_::TVDiskCostSettings::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetSeekTimeUs(const uint64_t& value) { return setSeekTimeUs(value); }
            void SetReadSpeedBps(const uint64_t& value) { return setReadSpeedBps(value); }
            void SetWriteSpeedBps(const uint64_t& value) { return setWriteSpeedBps(value); }
            void SetReadBlockSize(const uint64_t& value) { return setReadBlockSize(value); }
            void SetWriteBlockSize(const uint64_t& value) { return setWriteBlockSize(value); }
            void SetMinREALHugeBlobInBytes(const uint32_t& value) { return setMinREALHugeBlobInBytes(value); }
            const NKikimrCapnProto_::TVDiskCostSettings::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TMsgQoS {
        struct Reader : private NKikimrCapnProto_::TMsgQoS::Reader {
        public:
            Reader(NKikimrCapnProto_::TMsgQoS::Reader r) : NKikimrCapnProto_::TMsgQoS::Reader(r) {}
            Reader() = default;
            uint32_t GetDeadlineSeconds() const { return getDeadlineSeconds(); }
            uint64_t GetCost() const { return getCost(); }
            bool GetSendMeCostSettings() const { return getSendMeCostSettings(); }
            uint32_t GetProxyNodeId() const { return getProxyNodeId(); }
            uint32_t GetReplVDiskId() const { return getReplVDiskId(); }
            uint64_t GetVDiskLoadId() const { return getVDiskLoadId(); }
            uint32_t GetVPatchVDiskId() const { return getVPatchVDiskId(); }
            TMessageId::Reader GetMsgId() const { return getMsgId(); }
            TVDiskCostSettings::Reader GetCostSettings() const { return getCostSettings(); }
            TWindowFeedback::Reader GetWindow() const { return getWindow(); }
            TExecTimeStats::Reader GetExecTimeStats() const { return getExecTimeStats(); }
            TActorId::Reader GetSenderActorId() const { return getSenderActorId(); }
            EVDiskQueueId GetExtQueueId() const { return static_cast<EVDiskQueueId>(static_cast<size_t>(getExtQueueId()) - 1); }
            EVDiskInternalQueueId GetIntQueueId() const { return static_cast<EVDiskInternalQueueId>(static_cast<size_t>(getIntQueueId()) - 1); }
            bool HasMsgId() const { return hasMsgId(); }
            bool HasCostSettings() const { return hasCostSettings(); }
            bool HasWindow() const { return hasWindow(); }
            bool HasExecTimeStats() const { return hasExecTimeStats(); }
            bool HasSenderActorId() const { return hasSenderActorId(); }
            bool HasExtQueueId() const { return getExtQueueId() != NKikimrCapnProto_::EVDiskQueueId::NOT_SET; }
            bool HasIntQueueId() const { return getIntQueueId() != NKikimrCapnProto_::EVDiskInternalQueueId::NOT_SET; }
            bool HasDeadlineSeconds() const { return getDeadlineSeconds() != 0; }
            bool HasCost() const { return getCost() != 0; }
            bool HasSendMeCostSettings() const { return getSendMeCostSettings() != 0; }
            bool HasProxyNodeId() const { return getProxyNodeId() != 0; }
            bool HasReplVDiskId() const { return getReplVDiskId() != 0; }
            bool HasVDiskLoadId() const { return getVDiskLoadId() != 0; }
            bool HasVPatchVDiskId() const { return getVPatchVDiskId() != 0; }
            const NKikimrCapnProto_::TMsgQoS::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TMsgQoS::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TMsgQoS::Builder::getMsgId;
            using NKikimrCapnProto_::TMsgQoS::Builder::getCostSettings;
            using NKikimrCapnProto_::TMsgQoS::Builder::getWindow;
            using NKikimrCapnProto_::TMsgQoS::Builder::getExecTimeStats;
            using NKikimrCapnProto_::TMsgQoS::Builder::getSenderActorId;
        public:
            Builder(NKikimrCapnProto_::TMsgQoS::Builder b) : NKikimrCapnProto_::TMsgQoS::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetDeadlineSeconds(const uint32_t& value) { return setDeadlineSeconds(value); }
            void SetCost(const uint64_t& value) { return setCost(value); }
            void SetSendMeCostSettings(const bool& value) { return setSendMeCostSettings(value); }
            void SetProxyNodeId(const uint32_t& value) { return setProxyNodeId(value); }
            void SetReplVDiskId(const uint32_t& value) { return setReplVDiskId(value); }
            void SetVDiskLoadId(const uint64_t& value) { return setVDiskLoadId(value); }
            void SetVPatchVDiskId(const uint32_t& value) { return setVPatchVDiskId(value); }
            void SetMsgId(const TMessageId::Reader& value) { return setMsgId(value.GetCapnpBase()); }
            void SetCostSettings(const TVDiskCostSettings::Reader& value) { return setCostSettings(value.GetCapnpBase()); }
            void SetWindow(const TWindowFeedback::Reader& value) { return setWindow(value.GetCapnpBase()); }
            void SetExecTimeStats(const TExecTimeStats::Reader& value) { return setExecTimeStats(value.GetCapnpBase()); }
            void SetSenderActorId(const TActorId::Reader& value) { return setSenderActorId(value.GetCapnpBase()); }
            void SetExtQueueId(const EVDiskQueueId& value) { return setExtQueueId(static_cast<NKikimrCapnProto_::EVDiskQueueId>(static_cast<size_t>(value) + 1)); }
            void SetIntQueueId(const EVDiskInternalQueueId& value) { return setIntQueueId(static_cast<NKikimrCapnProto_::EVDiskInternalQueueId>(static_cast<size_t>(value) + 1)); }
            TMessageId::Builder MutableMsgId() { return getMsgId(); }
            TVDiskCostSettings::Builder MutableCostSettings() { return getCostSettings(); }
            TWindowFeedback::Builder MutableWindow() { return getWindow(); }
            TExecTimeStats::Builder MutableExecTimeStats() { return getExecTimeStats(); }
            TActorId::Builder MutableSenderActorId() { return getSenderActorId(); }
            const NKikimrCapnProto_::TMsgQoS::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TVDiskID {
        struct Reader : private NKikimrCapnProto_::TVDiskID::Reader {
        public:
            Reader(NKikimrCapnProto_::TVDiskID::Reader r) : NKikimrCapnProto_::TVDiskID::Reader(r) {}
            Reader() = default;
            uint32_t GetGroupID() const { return getGroupID(); }
            uint32_t GetGroupGeneration() const { return getGroupGeneration(); }
            uint32_t GetRing() const { return getRing(); }
            uint32_t GetDomain() const { return getDomain(); }
            uint32_t GetVDisk() const { return getVDisk(); }
            bool HasGroupID() const { return getGroupID() != 0; }
            bool HasGroupGeneration() const { return getGroupGeneration() != 0; }
            bool HasRing() const { return getRing() != 0; }
            bool HasDomain() const { return getDomain() != 0; }
            bool HasVDisk() const { return getVDisk() != 0; }
            const NKikimrCapnProto_::TVDiskID::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TVDiskID::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TVDiskID::Builder b) : NKikimrCapnProto_::TVDiskID::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetGroupID(const uint32_t& value) { return setGroupID(value); }
            void SetGroupGeneration(const uint32_t& value) { return setGroupGeneration(value); }
            void SetRing(const uint32_t& value) { return setRing(value); }
            void SetDomain(const uint32_t& value) { return setDomain(value); }
            void SetVDisk(const uint32_t& value) { return setVDisk(value); }
            const NKikimrCapnProto_::TVDiskID::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TLogoBlobID {
        struct Reader : private NKikimrCapnProto_::TLogoBlobID::Reader {
        public:
            Reader(NKikimrCapnProto_::TLogoBlobID::Reader r) : NKikimrCapnProto_::TLogoBlobID::Reader(r) {}
            Reader() = default;
            int64_t GetRawX1() const { return getRawX1(); }
            int64_t GetRawX2() const { return getRawX2(); }
            int64_t GetRawX3() const { return getRawX3(); }
            bool HasRawX1() const { return getRawX1() != 0; }
            bool HasRawX2() const { return getRawX2() != 0; }
            bool HasRawX3() const { return getRawX3() != 0; }
            const NKikimrCapnProto_::TLogoBlobID::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TLogoBlobID::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TLogoBlobID::Builder b) : NKikimrCapnProto_::TLogoBlobID::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetRawX1(const int64_t& value) { return setRawX1(value); }
            void SetRawX2(const int64_t& value) { return setRawX2(value); }
            void SetRawX3(const int64_t& value) { return setRawX3(value); }
            const NKikimrCapnProto_::TLogoBlobID::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TRangeQuery {
        struct Reader : private NKikimrCapnProto_::TRangeQuery::Reader {
        public:
            Reader(NKikimrCapnProto_::TRangeQuery::Reader r) : NKikimrCapnProto_::TRangeQuery::Reader(r) {}
            Reader() = default;
            uint64_t GetCookie() const { return getCookie(); }
            uint32_t GetMaxResults() const { return getMaxResults(); }
            TLogoBlobID::Reader GetFrom() const { return getFrom(); }
            TLogoBlobID::Reader GetTo() const { return getTo(); }
            bool HasFrom() const { return hasFrom(); }
            bool HasTo() const { return hasTo(); }
            bool HasCookie() const { return getCookie() != 0; }
            bool HasMaxResults() const { return getMaxResults() != 0; }
            const NKikimrCapnProto_::TRangeQuery::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TRangeQuery::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TRangeQuery::Builder::getFrom;
            using NKikimrCapnProto_::TRangeQuery::Builder::getTo;
        public:
            Builder(NKikimrCapnProto_::TRangeQuery::Builder b) : NKikimrCapnProto_::TRangeQuery::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetMaxResults(const uint32_t& value) { return setMaxResults(value); }
            void SetFrom(const TLogoBlobID::Reader& value) { return setFrom(value.GetCapnpBase()); }
            void SetTo(const TLogoBlobID::Reader& value) { return setTo(value.GetCapnpBase()); }
            TLogoBlobID::Builder MutableFrom() { return getFrom(); }
            TLogoBlobID::Builder MutableTo() { return getTo(); }
            const NKikimrCapnProto_::TRangeQuery::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TExtremeQuery {
        struct Reader : private NKikimrCapnProto_::TExtremeQuery::Reader {
        public:
            Reader(NKikimrCapnProto_::TExtremeQuery::Reader r) : NKikimrCapnProto_::TExtremeQuery::Reader(r) {}
            Reader() = default;
            uint64_t GetShift() const { return getShift(); }
            uint64_t GetSize() const { return getSize(); }
            uint64_t GetCookie() const { return getCookie(); }
            TLogoBlobID::Reader GetId() const { return getId(); }
            bool HasId() const { return hasId(); }
            bool HasShift() const { return getShift() != 0; }
            bool HasSize() const { return getSize() != 0; }
            bool HasCookie() const { return getCookie() != 0; }
            const NKikimrCapnProto_::TExtremeQuery::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TExtremeQuery::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TExtremeQuery::Builder::getId;
        public:
            Builder(NKikimrCapnProto_::TExtremeQuery::Builder b) : NKikimrCapnProto_::TExtremeQuery::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetShift(const uint64_t& value) { return setShift(value); }
            void SetSize(const uint64_t& value) { return setSize(value); }
            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetId(const TLogoBlobID::Reader& value) { return setId(value.GetCapnpBase()); }
            TLogoBlobID::Builder MutableId() { return getId(); }
            const NKikimrCapnProto_::TExtremeQuery::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TTabletData {
        struct Reader : private NKikimrCapnProto_::TTabletData::Reader {
        public:
            Reader(NKikimrCapnProto_::TTabletData::Reader r) : NKikimrCapnProto_::TTabletData::Reader(r) {}
            Reader() = default;
            uint64_t GetId() const { return getId(); }
            uint32_t GetGeneration() const { return getGeneration(); }
            bool HasId() const { return getId() != 0; }
            bool HasGeneration() const { return getGeneration() != 0; }
            const NKikimrCapnProto_::TTabletData::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TTabletData::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TTabletData::Builder b) : NKikimrCapnProto_::TTabletData::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetId(const uint64_t& value) { return setId(value); }
            void SetGeneration(const uint32_t& value) { return setGeneration(value); }
            const NKikimrCapnProto_::TTabletData::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TEvVGet {
        struct Reader : private NKikimrCapnProto_::TEvVGet::Reader {
        private:
            std::optional<capnp::PackedMessageReader> messageReader;
        public:
            Reader(NKikimrCapnProto_::TEvVGet::Reader r) : NKikimrCapnProto_::TEvVGet::Reader(r) {}
            Reader() = default;
            bool GetNotifyIfNotReady() const { return getNotifyIfNotReady(); }
            bool GetShowInternals() const { return getShowInternals(); }
            uint64_t GetCookie() const { return getCookie(); }
            bool GetIndexOnly() const { return getIndexOnly(); }
            bool GetSuppressBarrierCheck() const { return getSuppressBarrierCheck(); }
            uint64_t GetTabletId() const { return getTabletId(); }
            bool GetAcquireBlockedGeneration() const { return getAcquireBlockedGeneration(); }
            uint32_t GetForceBlockedGeneration() const { return getForceBlockedGeneration(); }
            std::string GetSnapshotId() const { return {reinterpret_cast<const char*>(getSnapshotId().begin()), getSnapshotId().size()}; }
            TRangeQuery::Reader GetRangeQuery() const { return getRangeQuery(); }
            TVDiskID::Reader GetVDiskID() const { return getVDiskID(); }
            TMsgQoS::Reader GetMsgQoS() const { return getMsgQoS(); }
            TTimestamps::Reader GetTimestamps() const { return getTimestamps(); }
            TTabletData::Reader GetReaderTabletData() const { return getReaderTabletData(); }
            TTabletData::Reader GetForceBlockTabletData() const { return getForceBlockTabletData(); }
            EGetHandleClass GetHandleClass() const { return static_cast<EGetHandleClass>(static_cast<size_t>(getHandleClass()) - 1); }
            bool HasRangeQuery() const { return hasRangeQuery(); }
            bool HasVDiskID() const { return hasVDiskID(); }
            bool HasMsgQoS() const { return hasMsgQoS(); }
            bool HasTimestamps() const { return hasTimestamps(); }
            bool HasReaderTabletData() const { return hasReaderTabletData(); }
            bool HasForceBlockTabletData() const { return hasForceBlockTabletData(); }
            bool HasHandleClass() const { return getHandleClass() != NKikimrCapnProto_::EGetHandleClass::NOT_SET; }
            bool HasNotifyIfNotReady() const { return getNotifyIfNotReady() != 0; }
            bool HasShowInternals() const { return getShowInternals() != 0; }
            bool HasCookie() const { return getCookie() != 0; }
            bool HasIndexOnly() const { return getIndexOnly() != 0; }
            bool HasSuppressBarrierCheck() const { return getSuppressBarrierCheck() != 0; }
            bool HasTabletId() const { return getTabletId() != 0; }
            bool HasAcquireBlockedGeneration() const { return getAcquireBlockedGeneration() != 0; }
            bool HasForceBlockedGeneration() const { return getForceBlockedGeneration() != 0; }
            bool HasSnapshotId() const { return getSnapshotId() != 0; }
            const NKikimrCapnProto_::TEvVGet::Reader& GetCapnpBase() const { return *this; }

            bool ParseFromZeroCopyStream(NActors::TRopeStream *input) {
                NKikimrCapnProtoUtil::TRopeStream stream;
                stream.underlying = input;

                kj::BufferedInputStreamWrapper buffered(stream);
                static_cast<NKikimrCapnProto_::TEvVGet::Reader&>(*this) = messageReader.emplace(buffered).getRoot<NKikimrCapnProto_::TEvVGet>();
                return true;
            }
    };

    struct Builder : private capnp::MallocMessageBuilder, private NKikimrCapnProto_::TEvVGet::Builder, public Reader {
    private:
        using NKikimrCapnProto_::TEvVGet::Builder::getRangeQuery;
            using NKikimrCapnProto_::TEvVGet::Builder::getVDiskID;
            using NKikimrCapnProto_::TEvVGet::Builder::getMsgQoS;
            using NKikimrCapnProto_::TEvVGet::Builder::getTimestamps;
            using NKikimrCapnProto_::TEvVGet::Builder::getReaderTabletData;
            using NKikimrCapnProto_::TEvVGet::Builder::getForceBlockTabletData;
            using NKikimrCapnProto_::TEvVGet::Builder::totalSize;
    public:
            Builder() : NKikimrCapnProto_::TEvVGet::Builder(initRoot<NKikimrCapnProto_::TEvVGet>()), Reader(asReader()) {}
            Builder(NKikimrCapnProto_::TEvVGet::Builder b) : NKikimrCapnProto_::TEvVGet::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }

            int ByteSize() const {
                return totalSize().wordCount * 8;
            }

            TString ShortDebugString() const {
                return "ShortDebugString";
            }

            TString GetTypeName() const {
                return "TEvVGet";
            }

            inline void CopyFrom(const Builder& other) {
                // TODO(stetsyuk): think of a better solution
                static_cast<NKikimrCapnProto_::TEvVGet::Builder&>(*this) = static_cast<const NKikimrCapnProto_::TEvVGet::Builder&>(other);
            }

            inline bool ParseFromString(const TString& ) { return true; }

            inline bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                kj::VectorOutputStream stream;
                capnp::writePackedMessage(stream, const_cast<Builder&>(*this));
                output->WriteAliasedRaw(stream.getArray().begin(), stream.getArray().size());
                return true;
            }

            void SetNotifyIfNotReady(const bool& value) { return setNotifyIfNotReady(value); }
            void SetShowInternals(const bool& value) { return setShowInternals(value); }
            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetIndexOnly(const bool& value) { return setIndexOnly(value); }
            void SetSuppressBarrierCheck(const bool& value) { return setSuppressBarrierCheck(value); }
            void SetTabletId(const uint64_t& value) { return setTabletId(value); }
            void SetAcquireBlockedGeneration(const bool& value) { return setAcquireBlockedGeneration(value); }
            void SetForceBlockedGeneration(const uint32_t& value) { return setForceBlockedGeneration(value); }
            void SetSnapshotId(const std::string& value) { return setSnapshotId({reinterpret_cast<const unsigned char*>(value.data()), value.size()}); }
            void SetRangeQuery(const TRangeQuery::Reader& value) { return setRangeQuery(value.GetCapnpBase()); }
            void SetVDiskID(const TVDiskID::Reader& value) { return setVDiskID(value.GetCapnpBase()); }
            void SetMsgQoS(const TMsgQoS::Reader& value) { return setMsgQoS(value.GetCapnpBase()); }
            void SetTimestamps(const TTimestamps::Reader& value) { return setTimestamps(value.GetCapnpBase()); }
            void SetReaderTabletData(const TTabletData::Reader& value) { return setReaderTabletData(value.GetCapnpBase()); }
            void SetForceBlockTabletData(const TTabletData::Reader& value) { return setForceBlockTabletData(value.GetCapnpBase()); }
            void SetHandleClass(const EGetHandleClass& value) { return setHandleClass(static_cast<NKikimrCapnProto_::EGetHandleClass>(static_cast<size_t>(value) + 1)); }
            TRangeQuery::Builder MutableRangeQuery() { return getRangeQuery(); }
            TVDiskID::Builder MutableVDiskID() { return getVDiskID(); }
            TMsgQoS::Builder MutableMsgQoS() { return getMsgQoS(); }
            TTimestamps::Builder MutableTimestamps() { return getTimestamps(); }
            TTabletData::Builder MutableReaderTabletData() { return getReaderTabletData(); }
            TTabletData::Builder MutableForceBlockTabletData() { return getForceBlockTabletData(); }
            const NKikimrCapnProto_::TEvVGet::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EReplyStatus {
        Ok,
        Error,
        Already,
        Timeout,
        Race,
        NoData,
        Blocked,
        NotReady,
        Overrun,
        TryLater,
        TryLaterTime,
        TryLaterSize,
        Deadline,
        Corrupted,
        Scheduled,
        OutOfSpace,
        VdiskErrorState,
        InvalidOwner,
        InvalidRound,
        Restart,
        NotYet,
        NoGroup,
        Unknown,
    };

    struct TQueryResult {
        struct Reader : private NKikimrCapnProto_::TQueryResult::Reader {
        public:
            Reader(NKikimrCapnProto_::TQueryResult::Reader r) : NKikimrCapnProto_::TQueryResult::Reader(r) {}
            Reader() = default;
            uint64_t GetShift() const { return getShift(); }
            uint64_t GetSize() const { return getSize(); }
            std::string GetSuffer() const { return {reinterpret_cast<const char*>(getSuffer().begin()), getSuffer().size()}; }
            uint64_t GetCookie() const { return getCookie(); }
            uint64_t GetFullDataSize() const { return getFullDataSize(); }
            uint64_t GetIngress() const { return getIngress(); }
            bool GetKeep() const { return getKeep(); }
            bool GetDoNotKeep() const { return getDoNotKeep(); }
            TLogoBlobID::Reader GetBlobID() const { return getBlobID(); }
            EReplyStatus GetStatus() const { return static_cast<EReplyStatus>(static_cast<size_t>(getStatus()) - 1); }
            bool HasBlobID() const { return hasBlobID(); }
            bool HasStatus() const { return getStatus() != NKikimrCapnProto_::EReplyStatus::NOT_SET; }
            bool HasShift() const { return getShift() != 0; }
            bool HasSize() const { return getSize() != 0; }
            bool HasSuffer() const { return getSuffer() != 0; }
            bool HasCookie() const { return getCookie() != 0; }
            bool HasFullDataSize() const { return getFullDataSize() != 0; }
            bool HasIngress() const { return getIngress() != 0; }
            bool HasKeep() const { return getKeep() != 0; }
            bool HasDoNotKeep() const { return getDoNotKeep() != 0; }
            const NKikimrCapnProto_::TQueryResult::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TQueryResult::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TQueryResult::Builder::getBlobID;
        public:
            Builder(NKikimrCapnProto_::TQueryResult::Builder b) : NKikimrCapnProto_::TQueryResult::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetShift(const uint64_t& value) { return setShift(value); }
            void SetSize(const uint64_t& value) { return setSize(value); }
            void SetSuffer(const std::string& value) { return setSuffer({reinterpret_cast<const unsigned char*>(value.data()), value.size()}); }
            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetFullDataSize(const uint64_t& value) { return setFullDataSize(value); }
            void SetIngress(const uint64_t& value) { return setIngress(value); }
            void SetKeep(const bool& value) { return setKeep(value); }
            void SetDoNotKeep(const bool& value) { return setDoNotKeep(value); }
            void SetBlobID(const TLogoBlobID::Reader& value) { return setBlobID(value.GetCapnpBase()); }
            void SetStatus(const EReplyStatus& value) { return setStatus(static_cast<NKikimrCapnProto_::EReplyStatus>(static_cast<size_t>(value) + 1)); }
            TLogoBlobID::Builder MutableBlobID() { return getBlobID(); }
            const NKikimrCapnProto_::TQueryResult::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TVDiskLocation {
        struct Reader : private NKikimrCapnProto_::TVDiskLocation::Reader {
        public:
            Reader(NKikimrCapnProto_::TVDiskLocation::Reader r) : NKikimrCapnProto_::TVDiskLocation::Reader(r) {}
            Reader() = default;
            uint32_t GetNodeId() const { return getNodeId(); }
            uint32_t GetPDiskId() const { return getPDiskId(); }
            uint32_t GetVDiskSlotId() const { return getVDiskSlotId(); }
            uint64_t GetPDiskGuid() const { return getPDiskGuid(); }
            bool HasNodeId() const { return getNodeId() != 0; }
            bool HasPDiskId() const { return getPDiskId() != 0; }
            bool HasVDiskSlotId() const { return getVDiskSlotId() != 0; }
            bool HasPDiskGuid() const { return getPDiskGuid() != 0; }
            const NKikimrCapnProto_::TVDiskLocation::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TVDiskLocation::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TVDiskLocation::Builder b) : NKikimrCapnProto_::TVDiskLocation::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetNodeId(const uint32_t& value) { return setNodeId(value); }
            void SetPDiskId(const uint32_t& value) { return setPDiskId(value); }
            void SetVDiskSlotId(const uint32_t& value) { return setVDiskSlotId(value); }
            void SetPDiskGuid(const uint64_t& value) { return setPDiskGuid(value); }
            const NKikimrCapnProto_::TVDiskLocation::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TFailDomain {
        struct Reader : private NKikimrCapnProto_::TFailDomain::Reader {
        public:
            Reader(NKikimrCapnProto_::TFailDomain::Reader r) : NKikimrCapnProto_::TFailDomain::Reader(r) {}
            Reader() = default;
            const NKikimrCapnProto_::TFailDomain::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TFailDomain::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TFailDomain::Builder b) : NKikimrCapnProto_::TFailDomain::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            const NKikimrCapnProto_::TFailDomain::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EEntityStatus {
        Initial,
        Create,
        Destroy,
        Restart,
    };

    struct TFailRealm {
        struct Reader : private NKikimrCapnProto_::TFailRealm::Reader {
        public:
            Reader(NKikimrCapnProto_::TFailRealm::Reader r) : NKikimrCapnProto_::TFailRealm::Reader(r) {}
            Reader() = default;
            const NKikimrCapnProto_::TFailRealm::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TFailRealm::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TFailRealm::Builder b) : NKikimrCapnProto_::TFailRealm::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            const NKikimrCapnProto_::TFailRealm::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TScopeId {
        struct Reader : private NKikimrCapnProto_::TScopeId::Reader {
        public:
            Reader(NKikimrCapnProto_::TScopeId::Reader r) : NKikimrCapnProto_::TScopeId::Reader(r) {}
            Reader() = default;
            int64_t GetX1() const { return getX1(); }
            int64_t GetX2() const { return getX2(); }
            bool HasX1() const { return getX1() != 0; }
            bool HasX2() const { return getX2() != 0; }
            const NKikimrCapnProto_::TScopeId::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TScopeId::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TScopeId::Builder b) : NKikimrCapnProto_::TScopeId::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetX1(const int64_t& value) { return setX1(value); }
            void SetX2(const int64_t& value) { return setX2(value); }
            const NKikimrCapnProto_::TScopeId::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EPDiskType {
        Rot,
        Ssd,
        Nvme,
        UnknownType,
    };

    enum class E {
        None,
        Pending,
        InProgress,
        Done,
    };

    struct TGroupInfo {
        struct Reader : private NKikimrCapnProto_::TGroupInfo::Reader {
        public:
            Reader(NKikimrCapnProto_::TGroupInfo::Reader r) : NKikimrCapnProto_::TGroupInfo::Reader(r) {}
            Reader() = default;
            uint32_t GetGroupID() const { return getGroupID(); }
            uint32_t GetGroupGeneration() const { return getGroupGeneration(); }
            uint32_t GetErasureSpecies() const { return getErasureSpecies(); }
            uint32_t GetEncryptionMode() const { return getEncryptionMode(); }
            uint32_t GetLifeCyclePhase() const { return getLifeCyclePhase(); }
            std::string GetMainKeyId() const { return {reinterpret_cast<const char*>(getMainKeyId().begin()), getMainKeyId().size()}; }
            std::string GetEncryptedGroupKey() const { return {reinterpret_cast<const char*>(getEncryptedGroupKey().begin()), getEncryptedGroupKey().size()}; }
            uint64_t GetGroupKeyNonce() const { return getGroupKeyNonce(); }
            uint64_t GetMainKeyVersion() const { return getMainKeyVersion(); }
            std::string GetStoragePoolName() const { return getStoragePoolName(); }
            uint64_t GetBlobDepotId() const { return getBlobDepotId(); }
            TScopeId::Reader GetAcceptedScope() const { return getAcceptedScope(); }
            EEntityStatus GetEntityStatus() const { return static_cast<EEntityStatus>(static_cast<size_t>(getEntityStatus()) - 1); }
            EPDiskType GetDeviceType() const { return static_cast<EPDiskType>(static_cast<size_t>(getDeviceType()) - 1); }
            E GetDecommitStatus() const { return static_cast<E>(static_cast<size_t>(getDecommitStatus()) - 1); }
            bool HasAcceptedScope() const { return hasAcceptedScope(); }
            bool HasEntityStatus() const { return getEntityStatus() != NKikimrCapnProto_::EEntityStatus::NOT_SET; }
            bool HasDeviceType() const { return getDeviceType() != NKikimrCapnProto_::EPDiskType::NOT_SET; }
            bool HasDecommitStatus() const { return getDecommitStatus() != NKikimrCapnProto_::E::NOT_SET; }
            bool HasGroupID() const { return getGroupID() != 0; }
            bool HasGroupGeneration() const { return getGroupGeneration() != 0; }
            bool HasErasureSpecies() const { return getErasureSpecies() != 0; }
            bool HasEncryptionMode() const { return getEncryptionMode() != 0; }
            bool HasLifeCyclePhase() const { return getLifeCyclePhase() != 0; }
            bool HasMainKeyId() const { return getMainKeyId() != 0; }
            bool HasEncryptedGroupKey() const { return getEncryptedGroupKey() != 0; }
            bool HasGroupKeyNonce() const { return getGroupKeyNonce() != 0; }
            bool HasMainKeyVersion() const { return getMainKeyVersion() != 0; }
            bool HasStoragePoolName() const { return getStoragePoolName() != 0; }
            bool HasBlobDepotId() const { return getBlobDepotId() != 0; }
            const NKikimrCapnProto_::TGroupInfo::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private NKikimrCapnProto_::TGroupInfo::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TGroupInfo::Builder::getAcceptedScope;
        public:
            Builder(NKikimrCapnProto_::TGroupInfo::Builder b) : NKikimrCapnProto_::TGroupInfo::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }
            void SetGroupID(const uint32_t& value) { return setGroupID(value); }
            void SetGroupGeneration(const uint32_t& value) { return setGroupGeneration(value); }
            void SetErasureSpecies(const uint32_t& value) { return setErasureSpecies(value); }
            void SetEncryptionMode(const uint32_t& value) { return setEncryptionMode(value); }
            void SetLifeCyclePhase(const uint32_t& value) { return setLifeCyclePhase(value); }
            void SetMainKeyId(const std::string& value) { return setMainKeyId({reinterpret_cast<const unsigned char*>(value.data()), value.size()}); }
            void SetEncryptedGroupKey(const std::string& value) { return setEncryptedGroupKey({reinterpret_cast<const unsigned char*>(value.data()), value.size()}); }
            void SetGroupKeyNonce(const uint64_t& value) { return setGroupKeyNonce(value); }
            void SetMainKeyVersion(const uint64_t& value) { return setMainKeyVersion(value); }
            void SetStoragePoolName(const std::string& value) { return setStoragePoolName({reinterpret_cast<const char*>(value.data()), value.size()}); }
            void SetBlobDepotId(const uint64_t& value) { return setBlobDepotId(value); }
            void SetAcceptedScope(const TScopeId::Reader& value) { return setAcceptedScope(value.GetCapnpBase()); }
            void SetEntityStatus(const EEntityStatus& value) { return setEntityStatus(static_cast<NKikimrCapnProto_::EEntityStatus>(static_cast<size_t>(value) + 1)); }
            void SetDeviceType(const EPDiskType& value) { return setDeviceType(static_cast<NKikimrCapnProto_::EPDiskType>(static_cast<size_t>(value) + 1)); }
            void SetDecommitStatus(const E& value) { return setDecommitStatus(static_cast<NKikimrCapnProto_::E>(static_cast<size_t>(value) + 1)); }
            TScopeId::Builder MutableAcceptedScope() { return getAcceptedScope(); }
            const NKikimrCapnProto_::TGroupInfo::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TEvVGetResult {
        struct Reader : private NKikimrCapnProto_::TEvVGetResult::Reader {
        private:
            std::optional<capnp::PackedMessageReader> messageReader;
        public:
            Reader(NKikimrCapnProto_::TEvVGetResult::Reader r) : NKikimrCapnProto_::TEvVGetResult::Reader(r) {}
            Reader() = default;
            uint64_t GetCookie() const { return getCookie(); }
            uint32_t GetBlockedGeneration() const { return getBlockedGeneration(); }
            bool GetIsRangeOverflow() const { return getIsRangeOverflow(); }
            int64_t GetIncarnationGuid() const { return getIncarnationGuid(); }
            TVDiskID::Reader GetVDiskID() const { return getVDiskID(); }
            TMsgQoS::Reader GetMsgQoS() const { return getMsgQoS(); }
            TTimestamps::Reader GetTimestamps() const { return getTimestamps(); }
            TGroupInfo::Reader GetRecentGroup() const { return getRecentGroup(); }
            EReplyStatus GetStatus() const { return static_cast<EReplyStatus>(static_cast<size_t>(getStatus()) - 1); }
            bool HasVDiskID() const { return hasVDiskID(); }
            bool HasMsgQoS() const { return hasMsgQoS(); }
            bool HasTimestamps() const { return hasTimestamps(); }
            bool HasRecentGroup() const { return hasRecentGroup(); }
            bool HasStatus() const { return getStatus() != NKikimrCapnProto_::EReplyStatus::NOT_SET; }
            bool HasCookie() const { return getCookie() != 0; }
            bool HasBlockedGeneration() const { return getBlockedGeneration() != 0; }
            bool HasIsRangeOverflow() const { return getIsRangeOverflow() != 0; }
            bool HasIncarnationGuid() const { return getIncarnationGuid() != 0; }
            const NKikimrCapnProto_::TEvVGetResult::Reader& GetCapnpBase() const { return *this; }

            bool ParseFromZeroCopyStream(NActors::TRopeStream *input) {
                NKikimrCapnProtoUtil::TRopeStream stream;
                stream.underlying = input;

                kj::BufferedInputStreamWrapper buffered(stream);
                static_cast<NKikimrCapnProto_::TEvVGetResult::Reader&>(*this) = messageReader.emplace(buffered).getRoot<NKikimrCapnProto_::TEvVGetResult>();
                return true;
            }
        };

        struct Builder : private capnp::MallocMessageBuilder, private NKikimrCapnProto_::TEvVGetResult::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TEvVGetResult::Builder::getVDiskID;
            using NKikimrCapnProto_::TEvVGetResult::Builder::getMsgQoS;
            using NKikimrCapnProto_::TEvVGetResult::Builder::getTimestamps;
            using NKikimrCapnProto_::TEvVGetResult::Builder::getRecentGroup;
            using NKikimrCapnProto_::TEvVGetResult::Builder::totalSize;
        public:
            Builder() : NKikimrCapnProto_::TEvVGetResult::Builder(initRoot<NKikimrCapnProto_::TEvVGetResult>()), Reader(asReader()) {}
            Builder(NKikimrCapnProto_::TEvVGetResult::Builder b) : NKikimrCapnProto_::TEvVGetResult::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }

            int ByteSize() const {
                return totalSize().wordCount * 8;
            }

            TString ShortDebugString() const {
                return "ShortDebugString";
            }

            TString GetTypeName() const {
                return "TEvVGet";
            }

            void CopyFrom(const Builder& other) {
                // TODO(stetsyuk): think of a better solution
                static_cast<NKikimrCapnProto_::TEvVGetResult::Builder&>(*this) = static_cast<const NKikimrCapnProto_::TEvVGetResult::Builder&>(other);
            }

            bool ParseFromString(const TString& ) { return true; }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                kj::VectorOutputStream stream;
                capnp::writePackedMessage(stream, const_cast<Builder&>(*this));
                output->WriteAliasedRaw(stream.getArray().begin(), stream.getArray().size());
                return true;
            }

            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetBlockedGeneration(const uint32_t& value) { return setBlockedGeneration(value); }
            void SetIsRangeOverflow(const bool& value) { return setIsRangeOverflow(value); }
            void SetIncarnationGuid(const int64_t& value) { return setIncarnationGuid(value); }
            void SetVDiskID(const TVDiskID::Reader& value) { return setVDiskID(value.GetCapnpBase()); }
            void SetMsgQoS(const TMsgQoS::Reader& value) { return setMsgQoS(value.GetCapnpBase()); }
            void SetTimestamps(const TTimestamps::Reader& value) { return setTimestamps(value.GetCapnpBase()); }
            void SetRecentGroup(const TGroupInfo::Reader& value) { return setRecentGroup(value.GetCapnpBase()); }
            void SetStatus(const EReplyStatus& value) { return setStatus(static_cast<NKikimrCapnProto_::EReplyStatus>(static_cast<size_t>(value) + 1)); }
            TVDiskID::Builder MutableVDiskID() { return getVDiskID(); }
            TMsgQoS::Builder MutableMsgQoS() { return getMsgQoS(); }
            TTimestamps::Builder MutableTimestamps() { return getTimestamps(); }
            TGroupInfo::Builder MutableRecentGroup() { return getRecentGroup(); }
            const NKikimrCapnProto_::TEvVGetResult::Builder& GetCapnpBase() const { return *this; }
        };
    };
};
