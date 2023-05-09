#pragma once

#include <capnp/message.h>
#include <capnp/serialize-packed.h>
#include <kj/io.h>
#include <string>
#include <vector>
#include <optional>
#include "protos.capnp.h"
#include "util.h"
#include <library/cpp/actors/core/event_pb.h>
#include <ydb/core/protos/blobstorage.pb.h>


namespace NKikimrCapnProto {
    using namespace NKikimrCapnProto_;

    const uint64_t UNSET_UINT64 = 18446744073699546569ull;
    const uint32_t UNSET_UINT32 = 4294866787u;

    struct TMessageId {
        struct Reader : private NKikimrCapnProto_::TMessageId::Reader {
            Reader(NKikimrCapnProto_::TMessageId::Reader r) : NKikimrCapnProto_::TMessageId::Reader(r) {}
            Reader() = default;
            uint64_t GetSequenceId() const { return getSequenceId(); }
            uint64_t GetMsgId() const { return getMsgId(); }
            bool HasSequenceId() const { return getSequenceId() != UNSET_UINT64; }
            bool HasMsgId() const { return getMsgId() != UNSET_UINT64; }
            const NKikimrCapnProto_::TMessageId::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#SequenceId " << GetSequenceId() << " "
                   << "#MsgId " << GetMsgId() << " "
                   << " }";
                return ss.Str();
            }
        };

        struct Builder : private NKikimrCapnProto_::TMessageId::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TMessageId::Builder b) : NKikimrCapnProto_::TMessageId::Builder(b), Reader(b.asReader()) {}

            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }

            void SetSequenceId(const uint64_t& value) {
                Y_VERIFY(value != UNSET_UINT64);
                return setSequenceId(value);
            }
            void SetMsgId(const uint64_t& value) {
                Y_VERIFY(value != UNSET_UINT64);
                return setMsgId(value);
            }
            const NKikimrCapnProto_::TMessageId::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TTimestamps {
        struct Reader : private NKikimrCapnProto_::TTimestamps::Reader {
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

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#SentByDSProxyUs " << GetSentByDSProxyUs() << " "
                   << "#ReceivedByVDiskUs " << GetReceivedByVDiskUs() << " "
                   << "#SentByVDiskUs " << GetSentByVDiskUs() << " "
                   << "#ReceivedByDSProxyUs " << GetReceivedByDSProxyUs() << " "
                   << " }";
                return ss.Str();
            }
        };

        struct Builder : private NKikimrCapnProto_::TTimestamps::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TTimestamps::Builder b) : NKikimrCapnProto_::TTimestamps::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }

            void CopyFrom(const Builder& other) {
                SetSentByDSProxyUs(other.GetSentByDSProxyUs());
                SetReceivedByVDiskUs(other.GetReceivedByVDiskUs());
                SetSentByVDiskUs(other.GetSentByVDiskUs());
                SetReceivedByDSProxyUs(other.GetReceivedByDSProxyUs());
            }

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
            Reader(NKikimrCapnProto_::TWindowFeedback::Reader r) : NKikimrCapnProto_::TWindowFeedback::Reader(r) {}
            Reader() = default;
            uint64_t GetActualWindowSize() const { return getActualWindowSize(); }
            uint64_t GetMaxWindowSize() const { return getMaxWindowSize(); }
            TMessageId::Reader GetExpectedMsgId() const { return getExpectedMsgId(); }
            TMessageId::Reader GetFailedMsgId() const { return getFailedMsgId(); }
            EStatus GetStatus() const {
                if (getStatus() == NKikimrCapnProto_::EStatus::NOT_SET) {
                    return EStatus::Unknown;
                }
                return static_cast<EStatus>(static_cast<size_t>(getStatus()) - 1);
            }
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
            void SetStatus(size_t value) { return setStatus(static_cast<NKikimrCapnProto_::EStatus>(value + 1)); }
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
            Reader(NKikimrCapnProto_::TMsgQoS::Reader r) : NKikimrCapnProto_::TMsgQoS::Reader(r) {}
            Reader() = default;
            uint32_t GetDeadlineSeconds() const { return getDeadlineSeconds(); }
            uint64_t GetCost() const { return getCost(); }
            bool GetSendMeCostSettings() const { return getSendMeCostSettings(); }
            uint32_t GetProxyNodeId() const { return getClientID().getProxyNodeId(); }
            uint32_t GetReplVDiskId() const { return getClientID().getReplVDiskId(); }
            uint64_t GetVDiskLoadId() const { return getClientID().getVDiskLoadId(); }
            uint32_t GetVPatchVDiskId() const { return getClientID().getVPatchVDiskId(); }
            TMessageId::Reader GetMsgId() const { return getMsgId(); }
            TVDiskCostSettings::Reader GetCostSettings() const { return getCostSettings(); }
            TWindowFeedback::Reader GetWindow() const { return getWindow(); }
            TExecTimeStats::Reader GetExecTimeStats() const { return getExecTimeStats(); }
            TActorId::Reader GetSenderActorId() const { return getSenderActorId(); }
            EVDiskQueueId GetExtQueueId() const {
                if (getExtQueueId() == NKikimrCapnProto_::EVDiskQueueId::NOT_SET) {
                    return EVDiskQueueId::Unknown;
                }
                return static_cast<EVDiskQueueId>(static_cast<size_t>(getExtQueueId()) - 1);
            }
            EVDiskInternalQueueId GetIntQueueId() const {
                if (getIntQueueId() == NKikimrCapnProto_::EVDiskInternalQueueId::NOT_SET) {
                    return EVDiskInternalQueueId::IntUnknown;
                }
                return static_cast<EVDiskInternalQueueId>(static_cast<size_t>(getIntQueueId()) - 1);
            }
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
            bool HasProxyNodeId() const { return getClientID().isProxyNodeId(); }
            bool HasReplVDiskId() const { return getClientID().isReplVDiskId(); }
            bool HasVDiskLoadId() const { return getClientID().isVDiskLoadId(); }
            bool HasVPatchVDiskId() const { return getClientID().isVPatchVDiskId(); }
            const NKikimrCapnProto_::TMsgQoS::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#DeadlineSeconds " << GetDeadlineSeconds() << " "
                   << "#Cost " << GetCost() << " "
                   << "#SendMeCostSettings " << GetSendMeCostSettings() << " "
                   << "#ProxyNodeId " << GetProxyNodeId() << " "
                   << "#ReplVDiskId " << GetReplVDiskId() << " "
                   << "#VDiskLoadId " << GetVDiskLoadId() << " "
                   << "#VPatchVDiskId " << GetVPatchVDiskId() << " "
                   << "#EVDiskQueueId " << static_cast<int>(GetExtQueueId()) << " "
                   << "#EVDiskInternalQueueId " << static_cast<int>(GetIntQueueId()) << " "
                   << "#MsgId " << GetMsgId().ShortDebugString() << " "
                   << " }";
                return ss.Str();
            }
        };

        struct Builder : private NKikimrCapnProto_::TMsgQoS::Builder, public Reader {
        private:
            using NKikimrCapnProto_::TMsgQoS::Builder::getMsgId;
            using NKikimrCapnProto_::TMsgQoS::Builder::getClientID;
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
            void SetProxyNodeId(const uint32_t& value) { return getClientID().setProxyNodeId(value); }
            void SetReplVDiskId(const uint32_t& value) { return getClientID().setReplVDiskId(value); }
            void SetVDiskLoadId(const uint64_t& value) { return getClientID().setVDiskLoadId(value); }
            void SetVPatchVDiskId(const uint32_t& value) { return getClientID().setVPatchVDiskId(value); }
            void SetMsgId(const TMessageId::Reader& value) { return setMsgId(value.GetCapnpBase()); }
            void SetCostSettings(const TVDiskCostSettings::Reader& value) { return setCostSettings(value.GetCapnpBase()); }
            void SetWindow(const TWindowFeedback::Reader& value) { return setWindow(value.GetCapnpBase()); }
            void SetExecTimeStats(const TExecTimeStats::Reader& value) { return setExecTimeStats(value.GetCapnpBase()); }
            void SetSenderActorId(const TActorId::Reader& value) { return setSenderActorId(value.GetCapnpBase()); }
            void SetExtQueueId(EVDiskQueueId value) { return setExtQueueId(static_cast<NKikimrCapnProto_::EVDiskQueueId>(static_cast<size_t>(value) + 1)); }
            void SetIntQueueId(EVDiskInternalQueueId value) { return setIntQueueId(static_cast<NKikimrCapnProto_::EVDiskInternalQueueId>(static_cast<size_t>(value) + 1)); }
            void SetIntQueueId(NKikimrBlobStorage::EVDiskInternalQueueId value) {
                switch (value) {
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntUnknown:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntUnknown);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntBegin:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntBegin);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntGetFast:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntGetFast);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntPutLog:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntPutLog);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntPutHugeForeground:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeForeground);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntPutHugeBackground:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntPutHugeBackground);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntGetDiscover:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntGetDiscover);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntLowRead:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntLowRead);
                        break;
                    case NKikimrBlobStorage::EVDiskInternalQueueId::IntEnd:
                        SetIntQueueId(NKikimrCapnProto::EVDiskInternalQueueId::IntEnd);
                        break;
                    default:
                        throw std::runtime_error("invalid protobuf EVDiskInternalQueueId value: " + std::to_string(static_cast<int>(value)));
                }
            }
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
            Reader(NKikimrCapnProto_::TVDiskID::Reader r) : NKikimrCapnProto_::TVDiskID::Reader(r) {}
            Reader() = default;
            uint32_t GetGroupID() const { return getGroupID(); }
            uint32_t GetGroupGeneration() const { return getGroupGeneration(); }
            uint32_t GetRing() const { return getRing(); }
            uint32_t GetDomain() const { return getDomain(); }
            uint32_t GetVDisk() const { return getVDisk(); }
            bool HasGroupID() const { return getGroupID() != UNSET_UINT32; }
            bool HasGroupGeneration() const { return getGroupGeneration() != UNSET_UINT32; }
            bool HasRing() const { return getRing() != UNSET_UINT32; }
            bool HasDomain() const { return getDomain() != UNSET_UINT32; }
            bool HasVDisk() const { return getVDisk() != UNSET_UINT32; }
            const NKikimrCapnProto_::TVDiskID::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#groupID " << GetGroupID() << " "
                   << "#groupGeneration " << GetGroupGeneration() << " "
                   << "#ring " << GetRing() << " "
                   << "#domain " << GetDomain() << " "
                   << "#vdisk " << GetVDisk() << " "
                   << " }";
                return ss.Str();
            }
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
            Reader(NKikimrCapnProto_::TLogoBlobID::Reader r) : NKikimrCapnProto_::TLogoBlobID::Reader(r) {}
            Reader() = default;
            uint64_t GetRawX1() const { return getRawX1(); }
            uint64_t GetRawX2() const { return getRawX2(); }
            uint64_t GetRawX3() const { return getRawX3(); }
            bool HasRawX1() const { return getRawX1() != 0; }
            bool HasRawX2() const { return getRawX2() != 0; }
            bool HasRawX3() const { return getRawX3() != 0; }
            const NKikimrCapnProto_::TLogoBlobID::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#rawX1 " << GetRawX1() << " "
                   << "#rawX2 " << GetRawX2() << " "
                   << "#rawX3 " << GetRawX3() << " "
                   << " }";
                return ss.Str();
            }
        };

        struct Builder : private NKikimrCapnProto_::TLogoBlobID::Builder, public Reader {
        private:

        public:
            Builder(NKikimrCapnProto_::TLogoBlobID::Builder b) : NKikimrCapnProto_::TLogoBlobID::Builder(b), Reader(b.asReader()) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }

            void SetRawX1(const uint64_t& value) { return setRawX1(value); }
            void SetRawX2(const uint64_t& value) { return setRawX2(value); }
            void SetRawX3(const uint64_t& value) { return setRawX3(value); }
            const NKikimrCapnProto_::TLogoBlobID::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TRangeQuery {
        struct Reader : private NKikimrCapnProto_::TRangeQuery::Reader {
            Reader(NKikimrCapnProto_::TRangeQuery::Reader r) : NKikimrCapnProto_::TRangeQuery::Reader(r) {}
            Reader() = default;
            uint64_t GetCookie() const { return getCookie(); }
            uint32_t GetMaxResults() const { return getMaxResults(); }
            TLogoBlobID::Reader GetFrom() const { return getFrom(); }
            TLogoBlobID::Reader GetTo() const { return getTo(); }
            bool HasFrom() const { return hasFrom(); }
            bool HasTo() const { return hasTo(); }
            bool HasCookie() const { return getCookie() != UNSET_UINT64; }
            bool HasMaxResults() const { return getMaxResults() != 0; }
            const NKikimrCapnProto_::TRangeQuery::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#cookie " << GetCookie() << " "
                   << "#maxResults " << GetMaxResults() << " "
                   << "#from " << GetFrom().ShortDebugString() << " "
                   << "#to " << GetTo().ShortDebugString() << " "
                   << " }";
                return ss.Str();
            }
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
            Reader(NKikimrCapnProto_::TExtremeQuery::Reader r) : NKikimrCapnProto_::TExtremeQuery::Reader(r) {}

            Reader() = default;
            uint64_t GetShift() const { return getShift(); }
            uint64_t GetSize() const { return getSize(); }
            uint64_t GetCookie() const { return getCookie(); }
            TLogoBlobID::Reader GetId() const { return getId(); }
            bool HasId() const { return hasId(); }
            bool HasShift() const { return getShift() != 0; }
            bool HasSize() const { return getSize() != 0; }
            bool HasCookie() const { return getCookie() != UNSET_UINT64; }
            const NKikimrCapnProto_::TExtremeQuery::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#shift " << GetShift() << " "
                   << "#size " << GetSize() << " "
                   << "#cookie " << GetCookie() << " "
                   << "#id " << GetId().ShortDebugString() << " "
                   << " }";
                return ss.Str();
            }
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

            void CopyFrom(const Reader& other) {
                // copy primitive fields
                SetShift(other.GetShift());
                SetSize(other.GetSize());
                SetCookie(other.GetCookie());

                // copy all other fields
                if (other.HasId()) {
                    SetId(other.GetId());
                }
            }

            TLogoBlobID::Builder MutableId() { return getId(); }
            const NKikimrCapnProto_::TExtremeQuery::Builder& GetCapnpBase() const { return *this; }
        };
    };

    struct TTabletData {
        struct Reader : private NKikimrCapnProto_::TTabletData::Reader {
            Reader(NKikimrCapnProto_::TTabletData::Reader r) : NKikimrCapnProto_::TTabletData::Reader(r) {}
            Reader() = default;
            uint64_t GetId() const { return getId(); }
            uint32_t GetGeneration() const { return getGeneration(); }
            bool HasId() const { return getId() != 0; }
            bool HasGeneration() const { return getGeneration() != 0; }
            const NKikimrCapnProto_::TTabletData::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#id " << GetId() << " "
                   << "#generation " << GetGeneration() << " "
                   << " }";
                return ss.Str();
            }
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
        protected:
            mutable std::vector<capnp::Orphan<NKikimrCapnProto_::TExtremeQuery>> elements;
        public:
            Reader(NKikimrCapnProto_::TEvVGet::Reader r) : NKikimrCapnProto_::TEvVGet::Reader(r) {}
            Reader() = default;

            bool HasExtremeQueries() const { return !elements.empty(); }
            TExtremeQuery::Reader GetExtremeQueries(uint32_t idx) const { return elements[idx].getReader(); }
            std::vector<TExtremeQuery::Reader> GetExtremeQueries() const {
                return {};
            }
            size_t ExtremeQueriesSize() const { return elements.size(); }

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
            EGetHandleClass GetHandleClass() const {
                if (getHandleClass() == NKikimrCapnProto_::EGetHandleClass::NOT_SET) {
                    return EGetHandleClass::AsyncRead;
                }

                return static_cast<EGetHandleClass>(static_cast<size_t>(getHandleClass()) - 1);
            }
            bool HasRangeQuery() const { return hasRangeQuery(); }
            bool HasVDiskID() const { return hasVDiskID(); }
            bool HasMsgQoS() const { return hasMsgQoS(); }
            bool HasTimestamps() const { return hasTimestamps(); }
            bool HasReaderTabletData() const { return hasReaderTabletData(); }
            bool HasForceBlockTabletData() const { return hasForceBlockTabletData(); }
            bool HasHandleClass() const { return getHandleClass() != NKikimrCapnProto_::EGetHandleClass::NOT_SET; }
            bool HasNotifyIfNotReady() const { return getNotifyIfNotReady() != 0; }
            bool HasShowInternals() const { return getShowInternals() != 0; }
            bool HasCookie() const { return getCookie() != UNSET_UINT64; }
            bool HasIndexOnly() const { return getIndexOnly() != 0; }
            bool HasSuppressBarrierCheck() const { return getSuppressBarrierCheck() != 0; }
            bool HasTabletId() const { return getTabletId() != 0; }
            bool HasAcquireBlockedGeneration() const { return getAcquireBlockedGeneration() != 0; }
            bool HasForceBlockedGeneration() const { return getForceBlockedGeneration() != 0; }
            bool HasSnapshotId() const { return getSnapshotId().size() != 0; }
            const NKikimrCapnProto_::TEvVGet::Reader& GetCapnpBase() const { return *this; }

            std::string ShortDebugString() const {
                TStringStream ss;
                ss << "{ "
                   << "#notifyIfNotReady " << GetNotifyIfNotReady() << " "
                   << "#showInternals " << GetShowInternals() << " "
                   << "#cookie " << GetCookie() << " "
                   << "#indexOnly " << GetIndexOnly() << " "
                   << "#suppressBarrierCheck " << GetSuppressBarrierCheck() << " "
                   << "#tabletId " << GetTabletId() << " "
                   << "#acquireBlockedGeneration " << GetAcquireBlockedGeneration() << " "
                   << "#forceBlockedGeneration" << GetForceBlockedGeneration() << " "
                   << "#snapshotId " << GetSnapshotId() << " "
                   << "#rangeQuery " << GetRangeQuery().ShortDebugString() << " "
                   << "#vdiskid " << GetVDiskID().ShortDebugString() << " "
                   << "#msgQos " << GetMsgQoS().ShortDebugString() << " "
                   << "#timestamps " << GetTimestamps().ShortDebugString() << " "
                   << "#readerTabletData " << GetReaderTabletData().ShortDebugString() << " "
                   << "#forceBlockTabletData " << GetForceBlockTabletData().ShortDebugString() << " "
                   << "#extremeQueriesCnt " << elements.size() << " "
                   << "#handleClass " << static_cast<int>(GetHandleClass()) << " "
                   << " }";
                return ss.Str();
            }

            std::string GetTypeName() const {
                return "TEvVGet";
            }
        };

        struct Builder : public Reader {
        private:
            std::unique_ptr<capnp::MallocMessageBuilder> message;
            NKikimrCapnProto_::TEvVGet::Builder builder;

        public:
            Builder(const Builder &) = delete;

            Builder(Builder&& other) : Reader(std::move(other)), message(std::move(other.message)), builder(std::move(other.builder)) {}

            ~Builder() {
                elements.clear();
            }

            Builder()
            : message(std::make_unique<capnp::MallocMessageBuilder>())
            , builder(message->initRoot<NKikimrCapnProto_::TEvVGet>())
            {
                static_cast<Reader&>(*this) = builder.asReader();
            }
            Builder(NKikimrCapnProto_::TEvVGet::Builder b) : Reader(b.asReader()), builder(b) {}
            Builder* operator->() { return this; }
            Builder& operator*() { return *this; }

            bool ParseFromString(TString s) {
                Y_VERIFY(s.Empty());
                return true;
            }

            TExtremeQuery::Builder AddExtremeQueries() {
                auto orphan = message->getOrphanage().newOrphan<NKikimrCapnProto_::TExtremeQuery>();
                elements.push_back(std::move(orphan));
                return elements.back().get();
            }

            void CopyFrom(const Reader& other) {
                // set the repeated field
                for (auto from : other.GetExtremeQueries()) {
                    auto to = AddExtremeQueries();

                    to.SetId(from.GetId());
                    to.SetCookie(from.GetCookie());
                    to.SetSize(from.GetSize());
                    to.SetShift(from.GetShift());
                }

                // set the primitive fields
                SetNotifyIfNotReady(other.GetNotifyIfNotReady());
                SetShowInternals(other.GetShowInternals());
                SetCookie(other.GetCookie());
                SetIndexOnly(other.GetIndexOnly());
                SetSuppressBarrierCheck(other.GetSuppressBarrierCheck());
                SetTabletId(other.GetTabletId());
                SetForceBlockedGeneration(other.GetForceBlockedGeneration());
                SetAcquireBlockedGeneration(other.GetAcquireBlockedGeneration());

                // set enum
                if (other.HasHandleClass()) {
                    SetHandleClass(other.GetHandleClass());
                }

                // set all other fields
                if (other.HasRangeQuery()) {
                    SetRangeQuery(other.GetRangeQuery());
                }

                if (other.HasVDiskID()) {
                    SetVDiskID(other.GetVDiskID());
                }

                if (other.HasSnapshotId()) {
                    SetSnapshotId(other.GetSnapshotId());
                }

                if (other.HasMsgQoS()) {
                    SetMsgQoS(other.GetMsgQoS());
                }

                if (other.HasTimestamps()) {
                    SetTimestamps(other.GetTimestamps());
                }

                if (other.HasReaderTabletData()) {
                    SetReaderTabletData(other.GetReaderTabletData());
                }

                if (other.HasForceBlockTabletData()) {
                    SetForceBlockTabletData(other.GetForceBlockTabletData());
                }
            }

            int ByteSize() const {
                return builder.totalSize().wordCount * 8;
            }

            long ByteSizeLong() const {
                return builder.totalSize().wordCount * 8;
            }

            bool ParseFromZeroCopyStream(NActors::TRopeStream *input) {
                NKikimrCapnProtoUtil::TRopeStream stream(input);
                kj::BufferedInputStreamWrapper buffered(stream);

                Y_VERIFY(message != nullptr);
                message->setRoot(capnp::PackedMessageReader{buffered}.getRoot<NKikimrCapnProto_::TEvVGet>());
                builder = message->getRoot<NKikimrCapnProto_::TEvVGet>();
                static_cast<Reader&>(*this) = builder.asReader();

                if (builder.hasExtremeQueries()) {
                    elements.reserve(builder.getExtremeQueries().size());
                    for (TExtremeQuery::Reader extremeQuery: builder.asReader().getExtremeQueries()) {
                        AddExtremeQueries().CopyFrom(extremeQuery);
                    }
                }

                return true;
            }

            bool SerializeToZeroCopyStream(NActors::TChunkSerializer *output) const {
                NKikimrCapnProto_::TEvVGet::Builder b(builder);
                auto interviews = b.initExtremeQueries(elements.size());
                for (size_t i = 0; i != elements.size(); ++i) {
                    interviews.adoptWithCaveats(i, std::move(elements[i]));
                }
                elements.clear();

                kj::VectorOutputStream stream;
                capnp::writePackedMessage(stream, *message);
                const TString s((const char *) stream.getArray().begin(), stream.getArray().size());
                output->WriteString(&s);

                std::cout << interviews.size() << ": " << stream.getArray().size() << "\n\n";

                return true;
            }

            void ClearForceBlockTabletData() {
                builder.disownForceBlockTabletData();
            }

            void ClearMsgQoS() {
                builder.disownMsgQoS();
            }

            void SetNotifyIfNotReady(const bool& value) { return builder.setNotifyIfNotReady(value); }
            void SetShowInternals(const bool& value) { return builder.setShowInternals(value); }
            void SetCookie(const uint64_t& value) { return builder.setCookie(value); }
            void SetIndexOnly(const bool& value) { return builder.setIndexOnly(value); }
            void SetSuppressBarrierCheck(const bool& value) { return builder.setSuppressBarrierCheck(value); }
            void SetTabletId(const uint64_t& value) { return builder.setTabletId(value); }
            void SetAcquireBlockedGeneration(const bool& value) { return builder.setAcquireBlockedGeneration(value); }
            void SetForceBlockedGeneration(const uint32_t& value) { return builder.setForceBlockedGeneration(value); }
            void SetSnapshotId(const std::string& value) { return builder.setSnapshotId({reinterpret_cast<const unsigned char*>(value.data()), value.size()}); }
            void SetRangeQuery(const TRangeQuery::Reader& value) { return builder.setRangeQuery(value.GetCapnpBase()); }
            void SetVDiskID(const TVDiskID::Reader& value) { return builder.setVDiskID(value.GetCapnpBase()); }
            void SetMsgQoS(const TMsgQoS::Reader& value) { return builder.setMsgQoS(value.GetCapnpBase()); }
            void SetTimestamps(const TTimestamps::Reader& value) { return builder.setTimestamps(value.GetCapnpBase()); }
            void SetReaderTabletData(const TTabletData::Reader& value) { return builder.setReaderTabletData(value.GetCapnpBase()); }
            void SetForceBlockTabletData(const TTabletData::Reader& value) { return builder.setForceBlockTabletData(value.GetCapnpBase()); }
            void SetHandleClass(const EGetHandleClass& value) { return builder.setHandleClass(static_cast<NKikimrCapnProto_::EGetHandleClass>(static_cast<size_t>(value) + 1)); }
            TRangeQuery::Builder MutableRangeQuery() { return builder.getRangeQuery(); }
            TVDiskID::Builder MutableVDiskID() { return builder.getVDiskID(); }
            TMsgQoS::Builder MutableMsgQoS() { return builder.getMsgQoS(); }
            TTimestamps::Builder MutableTimestamps() { return builder.getTimestamps(); }
            TTabletData::Builder MutableReaderTabletData() { return builder.getReaderTabletData(); }
            TTabletData::Builder MutableForceBlockTabletData() { return builder.getForceBlockTabletData(); }
            const NKikimrCapnProto_::TEvVGet::Builder& GetCapnpBase() const { return builder; }
        };
    };
};
