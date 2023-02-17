#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <string>
#include <vector>
#include <optional>

#include "tevvget.capnp.h"

namespace Capnroto {
    struct TMessageId {
        struct Reader : private ::TMessageId::Reader {
            Reader(::TMessageId::Reader r) : ::TMessageId::Reader(r) {}
            Reader() = default;
            uint64_t GetSequenceId() const { return getSequenceId(); }
            uint64_t GetMsgId() const { return getMsgId(); }
            const ::TMessageId::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TMessageId::Builder, public Reader {
        private:

        public:
            Builder(::TMessageId::Builder b) : ::TMessageId::Builder(b), Reader(b.asReader()) {}
            void SetSequenceId(const uint64_t& value) { return setSequenceId(value); }
            void SetMsgId(const uint64_t& value) { return setMsgId(value); }
            const ::TMessageId::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TTimestamps {
        struct Reader : private ::TTimestamps::Reader {
            Reader(::TTimestamps::Reader r) : ::TTimestamps::Reader(r) {}
            Reader() = default;
            uint64_t GetSentByDSProxyUs() const { return getSentByDSProxyUs(); }
            uint64_t GetReceivedByVDiskUs() const { return getReceivedByVDiskUs(); }
            uint64_t GetSentByVDiskUs() const { return getSentByVDiskUs(); }
            uint64_t GetReceivedByDSProxyUs() const { return getReceivedByDSProxyUs(); }
            const ::TTimestamps::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TTimestamps::Builder, public Reader {
        private:

        public:
            Builder(::TTimestamps::Builder b) : ::TTimestamps::Builder(b), Reader(b.asReader()) {}
            void SetSentByDSProxyUs(const uint64_t& value) { return setSentByDSProxyUs(value); }
            void SetReceivedByVDiskUs(const uint64_t& value) { return setReceivedByVDiskUs(value); }
            void SetSentByVDiskUs(const uint64_t& value) { return setSentByVDiskUs(value); }
            void SetReceivedByDSProxyUs(const uint64_t& value) { return setReceivedByDSProxyUs(value); }
            const ::TTimestamps::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EGetHandleClass {
        asyncRead,
        fastRead,
        discover,
        lowRead,
    };

    struct TActorId {
        struct Reader : private ::TActorId::Reader {
            Reader(::TActorId::Reader r) : ::TActorId::Reader(r) {}
            Reader() = default;
            uint64_t GetRawX1() const { return getRawX1(); }
            uint64_t GetRawX2() const { return getRawX2(); }
            const ::TActorId::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TActorId::Builder, public Reader {
        private:

        public:
            Builder(::TActorId::Builder b) : ::TActorId::Builder(b), Reader(b.asReader()) {}
            void SetRawX1(const uint64_t& value) { return setRawX1(value); }
            void SetRawX2(const uint64_t& value) { return setRawX2(value); }
            const ::TActorId::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TExecTimeStats {
        struct Reader : private ::TExecTimeStats::Reader {
            Reader(::TExecTimeStats::Reader r) : ::TExecTimeStats::Reader(r) {}
            Reader() = default;
            uint64_t GetSubmitTimestamp() const { return getSubmitTimestamp(); }
            uint64_t GetInSenderQueue() const { return getInSenderQueue(); }
            uint64_t GetReceivedTimestamp() const { return getReceivedTimestamp(); }
            uint64_t GetTotal() const { return getTotal(); }
            uint64_t GetInQueue() const { return getInQueue(); }
            uint64_t GetExecution() const { return getExecution(); }
            uint64_t GetHugeWriteTime() const { return getHugeWriteTime(); }
            const ::TExecTimeStats::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TExecTimeStats::Builder, public Reader {
        private:

        public:
            Builder(::TExecTimeStats::Builder b) : ::TExecTimeStats::Builder(b), Reader(b.asReader()) {}
            void SetSubmitTimestamp(const uint64_t& value) { return setSubmitTimestamp(value); }
            void SetInSenderQueue(const uint64_t& value) { return setInSenderQueue(value); }
            void SetReceivedTimestamp(const uint64_t& value) { return setReceivedTimestamp(value); }
            void SetTotal(const uint64_t& value) { return setTotal(value); }
            void SetInQueue(const uint64_t& value) { return setInQueue(value); }
            void SetExecution(const uint64_t& value) { return setExecution(value); }
            void SetHugeWriteTime(const uint64_t& value) { return setHugeWriteTime(value); }
            const ::TExecTimeStats::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EStatus {
        unknown,
        success,
        windowUpdate,
        processed,
        incorrectMsgId,
        highWatermarkOverflow,
    };

    struct TWindowFeedback {
        struct Reader : private ::TWindowFeedback::Reader {
            Reader(::TWindowFeedback::Reader r) : ::TWindowFeedback::Reader(r) {}
            Reader() = default;
            uint64_t GetActualWindowSize() const { return getActualWindowSize(); }
            uint64_t GetMaxWindowSize() const { return getMaxWindowSize(); }
            TMessageId::Reader GetExpectedMsgId() const { return getExpectedMsgId(); }
            TMessageId::Reader GetFailedMsgId() const { return getFailedMsgId(); }
            bool HasExpectedMsgId() const { return hasExpectedMsgId(); }
            bool HasFailedMsgId() const { return hasFailedMsgId(); }
            const ::TWindowFeedback::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TWindowFeedback::Builder, public Reader {
        private:
            using ::TWindowFeedback::Builder::getExpectedMsgId;
            using ::TWindowFeedback::Builder::getFailedMsgId;
        public:
            Builder(::TWindowFeedback::Builder b) : ::TWindowFeedback::Builder(b), Reader(b.asReader()) {}
            void SetActualWindowSize(const uint64_t& value) { return setActualWindowSize(value); }
            void SetMaxWindowSize(const uint64_t& value) { return setMaxWindowSize(value); }
            void SetExpectedMsgId(const TMessageId::Reader& value) { return setExpectedMsgId(value.GetCapnpBase()); }
            void SetFailedMsgId(const TMessageId::Reader& value) { return setFailedMsgId(value.GetCapnpBase()); }
            TMessageId::Builder MutableExpectedMsgId() { return getExpectedMsgId(); }
            TMessageId::Builder MutableFailedMsgId() { return getFailedMsgId(); }
            const ::TWindowFeedback::Builder& GetCapnpBase() const { return *this; }
        };
    };

    enum class EVDiskQueueId {
        unknown,
        putTabletLog,
        putAsyncBlob,
        putUserData,
        getAsyncRead,
        getFastRead,
        getDiscover,
        getLowRead,
        begin,
        end,
    };

    enum class EVDiskInternalQueueId {
        intUnknown,
        intBegin,
        intGetAsync,
        intGetFast,
        intPutLog,
        intPutHugeForeground,
        intPutHugeBackground,
        intGetDiscover,
        intLowRead,
        intEnd,
    };

    struct TVDiskCostSettings {
        struct Reader : private ::TVDiskCostSettings::Reader {
            Reader(::TVDiskCostSettings::Reader r) : ::TVDiskCostSettings::Reader(r) {}
            Reader() = default;
            uint64_t GetSeekTimeUs() const { return getSeekTimeUs(); }
            uint64_t GetReadSpeedBps() const { return getReadSpeedBps(); }
            uint64_t GetWriteSpeedBps() const { return getWriteSpeedBps(); }
            uint64_t GetReadBlockSize() const { return getReadBlockSize(); }
            uint64_t GetWriteBlockSize() const { return getWriteBlockSize(); }
            uint32_t GetMinREALHugeBlobInBytes() const { return getMinREALHugeBlobInBytes(); }
            const ::TVDiskCostSettings::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TVDiskCostSettings::Builder, public Reader {
        private:

        public:
            Builder(::TVDiskCostSettings::Builder b) : ::TVDiskCostSettings::Builder(b), Reader(b.asReader()) {}
            void SetSeekTimeUs(const uint64_t& value) { return setSeekTimeUs(value); }
            void SetReadSpeedBps(const uint64_t& value) { return setReadSpeedBps(value); }
            void SetWriteSpeedBps(const uint64_t& value) { return setWriteSpeedBps(value); }
            void SetReadBlockSize(const uint64_t& value) { return setReadBlockSize(value); }
            void SetWriteBlockSize(const uint64_t& value) { return setWriteBlockSize(value); }
            void SetMinREALHugeBlobInBytes(const uint32_t& value) { return setMinREALHugeBlobInBytes(value); }
            const ::TVDiskCostSettings::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TMsgQoS {
        struct Reader : private ::TMsgQoS::Reader {
            Reader(::TMsgQoS::Reader r) : ::TMsgQoS::Reader(r) {}
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
            bool HasMsgId() const { return hasMsgId(); }
            bool HasCostSettings() const { return hasCostSettings(); }
            bool HasWindow() const { return hasWindow(); }
            bool HasExecTimeStats() const { return hasExecTimeStats(); }
            bool HasSenderActorId() const { return hasSenderActorId(); }
            const ::TMsgQoS::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TMsgQoS::Builder, public Reader {
        private:
            using ::TMsgQoS::Builder::getMsgId;
            using ::TMsgQoS::Builder::getCostSettings;
            using ::TMsgQoS::Builder::getWindow;
            using ::TMsgQoS::Builder::getExecTimeStats;
            using ::TMsgQoS::Builder::getSenderActorId;
        public:
            Builder(::TMsgQoS::Builder b) : ::TMsgQoS::Builder(b), Reader(b.asReader()) {}
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
            TMessageId::Builder MutableMsgId() { return getMsgId(); }
            TVDiskCostSettings::Builder MutableCostSettings() { return getCostSettings(); }
            TWindowFeedback::Builder MutableWindow() { return getWindow(); }
            TExecTimeStats::Builder MutableExecTimeStats() { return getExecTimeStats(); }
            TActorId::Builder MutableSenderActorId() { return getSenderActorId(); }
            const ::TMsgQoS::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TVDiskID {
        struct Reader : private ::TVDiskID::Reader {
            Reader(::TVDiskID::Reader r) : ::TVDiskID::Reader(r) {}
            Reader() = default;
            uint32_t GetGroupID() const { return getGroupID(); }
            uint32_t GetGroupGeneration() const { return getGroupGeneration(); }
            uint32_t GetRing() const { return getRing(); }
            uint32_t GetDomain() const { return getDomain(); }
            uint32_t GetVDisk() const { return getVDisk(); }
            const ::TVDiskID::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TVDiskID::Builder, public Reader {
        private:

        public:
            Builder(::TVDiskID::Builder b) : ::TVDiskID::Builder(b), Reader(b.asReader()) {}
            void SetGroupID(const uint32_t& value) { return setGroupID(value); }
            void SetGroupGeneration(const uint32_t& value) { return setGroupGeneration(value); }
            void SetRing(const uint32_t& value) { return setRing(value); }
            void SetDomain(const uint32_t& value) { return setDomain(value); }
            void SetVDisk(const uint32_t& value) { return setVDisk(value); }
            const ::TVDiskID::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TLogoBlobID {
        struct Reader : private ::TLogoBlobID::Reader {
            Reader(::TLogoBlobID::Reader r) : ::TLogoBlobID::Reader(r) {}
            Reader() = default;
            int64_t GetRawX1() const { return getRawX1(); }
            int64_t GetRawX2() const { return getRawX2(); }
            int64_t GetRawX3() const { return getRawX3(); }
            const ::TLogoBlobID::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TLogoBlobID::Builder, public Reader {
        private:

        public:
            Builder(::TLogoBlobID::Builder b) : ::TLogoBlobID::Builder(b), Reader(b.asReader()) {}
            void SetRawX1(const int64_t& value) { return setRawX1(value); }
            void SetRawX2(const int64_t& value) { return setRawX2(value); }
            void SetRawX3(const int64_t& value) { return setRawX3(value); }
            const ::TLogoBlobID::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TRangeQuery {
        struct Reader : private ::TRangeQuery::Reader {
            Reader(::TRangeQuery::Reader r) : ::TRangeQuery::Reader(r) {}
            Reader() = default;
            uint64_t GetCookie() const { return getCookie(); }
            uint32_t GetMaxResults() const { return getMaxResults(); }
            TLogoBlobID::Reader GetFrom() const { return getFrom(); }
            TLogoBlobID::Reader GetTo() const { return getTo(); }
            bool HasFrom() const { return hasFrom(); }
            bool HasTo() const { return hasTo(); }
            const ::TRangeQuery::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TRangeQuery::Builder, public Reader {
        private:
            using ::TRangeQuery::Builder::getFrom;
            using ::TRangeQuery::Builder::getTo;
        public:
            Builder(::TRangeQuery::Builder b) : ::TRangeQuery::Builder(b), Reader(b.asReader()) {}
            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetMaxResults(const uint32_t& value) { return setMaxResults(value); }
            void SetFrom(const TLogoBlobID::Reader& value) { return setFrom(value.GetCapnpBase()); }
            void SetTo(const TLogoBlobID::Reader& value) { return setTo(value.GetCapnpBase()); }
            TLogoBlobID::Builder MutableFrom() { return getFrom(); }
            TLogoBlobID::Builder MutableTo() { return getTo(); }
            const ::TRangeQuery::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TExtremeQuery {
        struct Reader : private ::TExtremeQuery::Reader {
            Reader(::TExtremeQuery::Reader r) : ::TExtremeQuery::Reader(r) {}
            Reader() = default;
            uint64_t GetShift() const { return getShift(); }
            uint64_t GetSize() const { return getSize(); }
            uint64_t GetCookie() const { return getCookie(); }
            TLogoBlobID::Reader GetId() const { return getId(); }
            bool HasId() const { return hasId(); }
            const ::TExtremeQuery::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TExtremeQuery::Builder, public Reader {
        private:
            using ::TExtremeQuery::Builder::getId;
        public:
            Builder(::TExtremeQuery::Builder b) : ::TExtremeQuery::Builder(b), Reader(b.asReader()) {}
            void SetShift(const uint64_t& value) { return setShift(value); }
            void SetSize(const uint64_t& value) { return setSize(value); }
            void SetCookie(const uint64_t& value) { return setCookie(value); }
            void SetId(const TLogoBlobID::Reader& value) { return setId(value.GetCapnpBase()); }
            TLogoBlobID::Builder MutableId() { return getId(); }
            const ::TExtremeQuery::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TTabletData {
        struct Reader : private ::TTabletData::Reader {
            Reader(::TTabletData::Reader r) : ::TTabletData::Reader(r) {}
            Reader() = default;
            uint64_t GetId() const { return getId(); }
            uint32_t GetGeneration() const { return getGeneration(); }
            const ::TTabletData::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private ::TTabletData::Builder, public Reader {
        private:

        public:
            Builder(::TTabletData::Builder b) : ::TTabletData::Builder(b), Reader(b.asReader()) {}
            void SetId(const uint64_t& value) { return setId(value); }
            void SetGeneration(const uint32_t& value) { return setGeneration(value); }
            const ::TTabletData::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TEvVGet {
        struct Reader : private ::TEvVGet::Reader {
        private:
            std::optional<capnp::PackedMessageReader> messageReader;
        protected:
            std::vector<TExtremeQuery::Reader> elements;
        public:
            Reader(::TEvVGet::Reader r) : ::TEvVGet::Reader(r) {}
            Reader() = default;

            bool HasExtremeQueries() const { return !elements.empty(); }
            TExtremeQuery::Reader GetExtremeQueries(int idx) const { return elements[idx]; }
            const std::vector<TExtremeQuery::Reader>& GetExtremeQueries() const {
                return elements;
            }

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
            bool HasRangeQuery() const { return hasRangeQuery(); }
            bool HasVDiskID() const { return hasVDiskID(); }
            bool HasMsgQoS() const { return hasMsgQoS(); }
            bool HasTimestamps() const { return hasTimestamps(); }
            bool HasReaderTabletData() const { return hasReaderTabletData(); }
            bool HasForceBlockTabletData() const { return hasForceBlockTabletData(); }
            const ::TEvVGet::Reader& GetCapnpBase() const { return *this; }
        };

        struct Builder : private capnp::MallocMessageBuilder, private ::TEvVGet::Builder, public Reader {
        private:
            using ::TEvVGet::Builder::getRangeQuery;
            using ::TEvVGet::Builder::getVDiskID;
            using ::TEvVGet::Builder::getMsgQoS;
            using ::TEvVGet::Builder::getTimestamps;
            using ::TEvVGet::Builder::getReaderTabletData;
            using ::TEvVGet::Builder::getForceBlockTabletData;
        public:
            Builder() : ::TEvVGet::Builder(initRoot<::TEvVGet>()) , Reader(asReader()) {}

            TExtremeQuery::Builder AddExtremeQueries() {
                auto orphan = getOrphanage().newOrphan<::TExtremeQuery>();
                elements.emplace_back(orphan.getReader());
                return orphan.get();
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
            TRangeQuery::Builder MutableRangeQuery() { return getRangeQuery(); }
            TVDiskID::Builder MutableVDiskID() { return getVDiskID(); }
            TMsgQoS::Builder MutableMsgQoS() { return getMsgQoS(); }
            TTimestamps::Builder MutableTimestamps() { return getTimestamps(); }
            TTabletData::Builder MutableReaderTabletData() { return getReaderTabletData(); }
            TTabletData::Builder MutableForceBlockTabletData() { return getForceBlockTabletData(); }
            const ::TEvVGet::Builder& GetCapnpBase() const { return *this; }
        };
    };

};

int main() {
    return 0;
}


