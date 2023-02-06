#pragma once
#include <util/stream/str.h>

namespace NKikimr {

    namespace RawSerializer {
        struct TMessageId {
            /// uint64_t SequenceId
            uint64_t SequenceId;
            bool hasSequenceId;

            void SetSequenceId(const uint64_t& value) {
                SequenceId = value;
                hasSequenceId = true;
            }

            bool HasSequenceId() const {
                return hasSequenceId;
            }

            const uint64_t& GetSequenceId() const {
                return SequenceId;
            }

            void ClearSequenceId() {
                SequenceId = {};
                hasSequenceId = false;
            }

            uint64_t* MutableSequenceId() {
                return &SequenceId;
            }


            /// uint64_t MsgId
            uint64_t MsgId;
            bool hasMsgId;

            void SetMsgId(const uint64_t& value) {
                MsgId = value;
                hasMsgId = true;
            }

            bool HasMsgId() const {
                return hasMsgId;
            }

            const uint64_t& GetMsgId() const {
                return MsgId;
            }

            void ClearMsgId() {
                MsgId = {};
                hasMsgId = false;
            }

            uint64_t* MutableMsgId() {
                return &MsgId;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TMessageId";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint64_t);
            }

            void CopyFrom(const TMessageId& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint64_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TTimestamps {
            /// uint64_t SentByDSProxyUs
            uint64_t SentByDSProxyUs = 0;
            bool hasSentByDSProxyUs;

            void SetSentByDSProxyUs(const uint64_t& value) {
                SentByDSProxyUs = value;
                hasSentByDSProxyUs = true;
            }

            bool HasSentByDSProxyUs() const {
                return hasSentByDSProxyUs;
            }

            const uint64_t& GetSentByDSProxyUs() const {
                return SentByDSProxyUs;
            }

            void ClearSentByDSProxyUs() {
                SentByDSProxyUs = {};
                hasSentByDSProxyUs = false;
            }

            uint64_t* MutableSentByDSProxyUs() {
                return &SentByDSProxyUs;
            }


            /// uint64_t ReceivedByVDiskUs
            uint64_t ReceivedByVDiskUs = 0;
            bool hasReceivedByVDiskUs;

            void SetReceivedByVDiskUs(const uint64_t& value) {
                ReceivedByVDiskUs = value;
                hasReceivedByVDiskUs = true;
            }

            bool HasReceivedByVDiskUs() const {
                return hasReceivedByVDiskUs;
            }

            const uint64_t& GetReceivedByVDiskUs() const {
                return ReceivedByVDiskUs;
            }

            void ClearReceivedByVDiskUs() {
                ReceivedByVDiskUs = {};
                hasReceivedByVDiskUs = false;
            }

            uint64_t* MutableReceivedByVDiskUs() {
                return &ReceivedByVDiskUs;
            }


            /// uint64_t SentByVDiskUs
            uint64_t SentByVDiskUs = 0;
            bool hasSentByVDiskUs;

            void SetSentByVDiskUs(const uint64_t& value) {
                SentByVDiskUs = value;
                hasSentByVDiskUs = true;
            }

            bool HasSentByVDiskUs() const {
                return hasSentByVDiskUs;
            }

            const uint64_t& GetSentByVDiskUs() const {
                return SentByVDiskUs;
            }

            void ClearSentByVDiskUs() {
                SentByVDiskUs = {};
                hasSentByVDiskUs = false;
            }

            uint64_t* MutableSentByVDiskUs() {
                return &SentByVDiskUs;
            }


            /// uint64_t ReceivedByDSProxyUs
            uint64_t ReceivedByDSProxyUs = 0;
            bool hasReceivedByDSProxyUs;

            void SetReceivedByDSProxyUs(const uint64_t& value) {
                ReceivedByDSProxyUs = value;
                hasReceivedByDSProxyUs = true;
            }

            bool HasReceivedByDSProxyUs() const {
                return hasReceivedByDSProxyUs;
            }

            const uint64_t& GetReceivedByDSProxyUs() const {
                return ReceivedByDSProxyUs;
            }

            void ClearReceivedByDSProxyUs() {
                ReceivedByDSProxyUs = {};
                hasReceivedByDSProxyUs = false;
            }

            uint64_t* MutableReceivedByDSProxyUs() {
                return &ReceivedByDSProxyUs;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TTimestamps";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
            }

            void CopyFrom(const TTimestamps& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        enum class EGetHandleClass {
            AsyncRead = 1,
            FastRead = 2,
            Discover = 3,
            LowRead = 4,
        };

        struct TActorId {
            /// uint64_t RawX1
            uint64_t RawX1;
            bool hasRawX1;

            void SetRawX1(const uint64_t& value) {
                RawX1 = value;
                hasRawX1 = true;
            }

            bool HasRawX1() const {
                return hasRawX1;
            }

            const uint64_t& GetRawX1() const {
                return RawX1;
            }

            void ClearRawX1() {
                RawX1 = {};
                hasRawX1 = false;
            }

            uint64_t* MutableRawX1() {
                return &RawX1;
            }


            /// uint64_t RawX2
            uint64_t RawX2;
            bool hasRawX2;

            void SetRawX2(const uint64_t& value) {
                RawX2 = value;
                hasRawX2 = true;
            }

            bool HasRawX2() const {
                return hasRawX2;
            }

            const uint64_t& GetRawX2() const {
                return RawX2;
            }

            void ClearRawX2() {
                RawX2 = {};
                hasRawX2 = false;
            }

            uint64_t* MutableRawX2() {
                return &RawX2;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TActorId";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint64_t);
            }

            void CopyFrom(const TActorId& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint64_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TExecTimeStats {
            /// uint64_t SubmitTimestamp
            uint64_t SubmitTimestamp;
            bool hasSubmitTimestamp;

            void SetSubmitTimestamp(const uint64_t& value) {
                SubmitTimestamp = value;
                hasSubmitTimestamp = true;
            }

            bool HasSubmitTimestamp() const {
                return hasSubmitTimestamp;
            }

            const uint64_t& GetSubmitTimestamp() const {
                return SubmitTimestamp;
            }

            void ClearSubmitTimestamp() {
                SubmitTimestamp = {};
                hasSubmitTimestamp = false;
            }

            uint64_t* MutableSubmitTimestamp() {
                return &SubmitTimestamp;
            }


            /// uint64_t InSenderQueue
            uint64_t InSenderQueue;
            bool hasInSenderQueue;

            void SetInSenderQueue(const uint64_t& value) {
                InSenderQueue = value;
                hasInSenderQueue = true;
            }

            bool HasInSenderQueue() const {
                return hasInSenderQueue;
            }

            const uint64_t& GetInSenderQueue() const {
                return InSenderQueue;
            }

            void ClearInSenderQueue() {
                InSenderQueue = {};
                hasInSenderQueue = false;
            }

            uint64_t* MutableInSenderQueue() {
                return &InSenderQueue;
            }


            /// uint64_t ReceivedTimestamp
            uint64_t ReceivedTimestamp;
            bool hasReceivedTimestamp;

            void SetReceivedTimestamp(const uint64_t& value) {
                ReceivedTimestamp = value;
                hasReceivedTimestamp = true;
            }

            bool HasReceivedTimestamp() const {
                return hasReceivedTimestamp;
            }

            const uint64_t& GetReceivedTimestamp() const {
                return ReceivedTimestamp;
            }

            void ClearReceivedTimestamp() {
                ReceivedTimestamp = {};
                hasReceivedTimestamp = false;
            }

            uint64_t* MutableReceivedTimestamp() {
                return &ReceivedTimestamp;
            }


            /// uint64_t Total
            uint64_t Total;
            bool hasTotal;

            void SetTotal(const uint64_t& value) {
                Total = value;
                hasTotal = true;
            }

            bool HasTotal() const {
                return hasTotal;
            }

            const uint64_t& GetTotal() const {
                return Total;
            }

            void ClearTotal() {
                Total = {};
                hasTotal = false;
            }

            uint64_t* MutableTotal() {
                return &Total;
            }


            /// uint64_t InQueue
            uint64_t InQueue;
            bool hasInQueue;

            void SetInQueue(const uint64_t& value) {
                InQueue = value;
                hasInQueue = true;
            }

            bool HasInQueue() const {
                return hasInQueue;
            }

            const uint64_t& GetInQueue() const {
                return InQueue;
            }

            void ClearInQueue() {
                InQueue = {};
                hasInQueue = false;
            }

            uint64_t* MutableInQueue() {
                return &InQueue;
            }


            /// uint64_t Execution
            uint64_t Execution;
            bool hasExecution;

            void SetExecution(const uint64_t& value) {
                Execution = value;
                hasExecution = true;
            }

            bool HasExecution() const {
                return hasExecution;
            }

            const uint64_t& GetExecution() const {
                return Execution;
            }

            void ClearExecution() {
                Execution = {};
                hasExecution = false;
            }

            uint64_t* MutableExecution() {
                return &Execution;
            }


            /// uint64_t HugeWriteTime
            uint64_t HugeWriteTime;
            bool hasHugeWriteTime;

            void SetHugeWriteTime(const uint64_t& value) {
                HugeWriteTime = value;
                hasHugeWriteTime = true;
            }

            bool HasHugeWriteTime() const {
                return hasHugeWriteTime;
            }

            const uint64_t& GetHugeWriteTime() const {
                return HugeWriteTime;
            }

            void ClearHugeWriteTime() {
                HugeWriteTime = {};
                hasHugeWriteTime = false;
            }

            uint64_t* MutableHugeWriteTime() {
                return &HugeWriteTime;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TExecTimeStats";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
            }

            void CopyFrom(const TExecTimeStats& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        enum class EStatus {
            Unknown = 0,
            Success = 1,
            WindowUpdate = 2,
            Processed = 3,
            IncorrectMsgId = 4,
            HighWatermarkOverflow = 5,
        };

        struct TWindowFeedback {
            /// EStatus Status
            EStatus Status;
            bool hasStatus;

            void SetStatus(const EStatus& value) {
                Status = value;
                hasStatus = true;
            }

            bool HasStatus() const {
                return hasStatus;
            }

            const EStatus& GetStatus() const {
                return Status;
            }

            void ClearStatus() {
                Status = {};
                hasStatus = false;
            }

            EStatus* MutableStatus() {
                return &Status;
            }


            /// uint64_t ActualWindowSize
            uint64_t ActualWindowSize;
            bool hasActualWindowSize;

            void SetActualWindowSize(const uint64_t& value) {
                ActualWindowSize = value;
                hasActualWindowSize = true;
            }

            bool HasActualWindowSize() const {
                return hasActualWindowSize;
            }

            const uint64_t& GetActualWindowSize() const {
                return ActualWindowSize;
            }

            void ClearActualWindowSize() {
                ActualWindowSize = {};
                hasActualWindowSize = false;
            }

            uint64_t* MutableActualWindowSize() {
                return &ActualWindowSize;
            }


            /// uint64_t MaxWindowSize
            uint64_t MaxWindowSize;
            bool hasMaxWindowSize;

            void SetMaxWindowSize(const uint64_t& value) {
                MaxWindowSize = value;
                hasMaxWindowSize = true;
            }

            bool HasMaxWindowSize() const {
                return hasMaxWindowSize;
            }

            const uint64_t& GetMaxWindowSize() const {
                return MaxWindowSize;
            }

            void ClearMaxWindowSize() {
                MaxWindowSize = {};
                hasMaxWindowSize = false;
            }

            uint64_t* MutableMaxWindowSize() {
                return &MaxWindowSize;
            }


            /// TMessageId ExpectedMsgId
            TMessageId ExpectedMsgId;
            bool hasExpectedMsgId;

            void SetExpectedMsgId(const TMessageId& value) {
                ExpectedMsgId = value;
                hasExpectedMsgId = true;
            }

            bool HasExpectedMsgId() const {
                return hasExpectedMsgId;
            }

            const TMessageId& GetExpectedMsgId() const {
                return ExpectedMsgId;
            }

            void ClearExpectedMsgId() {
                ExpectedMsgId = {};
                hasExpectedMsgId = false;
            }

            TMessageId* MutableExpectedMsgId() {
                return &ExpectedMsgId;
            }


            /// TMessageId FailedMsgId
            TMessageId FailedMsgId;
            bool hasFailedMsgId;

            void SetFailedMsgId(const TMessageId& value) {
                FailedMsgId = value;
                hasFailedMsgId = true;
            }

            bool HasFailedMsgId() const {
                return hasFailedMsgId;
            }

            const TMessageId& GetFailedMsgId() const {
                return FailedMsgId;
            }

            void ClearFailedMsgId() {
                FailedMsgId = {};
                hasFailedMsgId = false;
            }

            TMessageId* MutableFailedMsgId() {
                return &FailedMsgId;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TWindowFeedback";
            }

            int ByteSize() const {
                return sizeof(EStatus) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(TMessageId) + sizeof(TMessageId);
            }

            void CopyFrom(const TWindowFeedback& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(EStatus) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(TMessageId) + sizeof(TMessageId));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        enum class EVDiskQueueId {
            Unknown = 0,
            PutTabletLog = 1,
            PutAsyncBlob = 2,
            PutUserData = 3,
            GetAsyncRead = 4,
            GetFastRead = 5,
            GetDiscover = 6,
            GetLowRead = 7,
            Begin = 8,
            End = 9,
        };

        enum class EVDiskInternalQueueId {
            IntUnknown = 0,
            IntBegin = 1,
            IntGetAsync = 1,
            IntGetFast = 2,
            IntPutLog = 3,
            IntPutHugeForeground = 4,
            IntPutHugeBackground = 5,
            IntGetDiscover = 6,
            IntLowRead = 7,
            IntEnd = 8,
        };

        struct TVDiskCostSettings {
            /// uint64_t SeekTimeUs
            uint64_t SeekTimeUs;
            bool hasSeekTimeUs;

            void SetSeekTimeUs(const uint64_t& value) {
                SeekTimeUs = value;
                hasSeekTimeUs = true;
            }

            bool HasSeekTimeUs() const {
                return hasSeekTimeUs;
            }

            const uint64_t& GetSeekTimeUs() const {
                return SeekTimeUs;
            }

            void ClearSeekTimeUs() {
                SeekTimeUs = {};
                hasSeekTimeUs = false;
            }

            uint64_t* MutableSeekTimeUs() {
                return &SeekTimeUs;
            }


            /// uint64_t ReadSpeedBps
            uint64_t ReadSpeedBps;
            bool hasReadSpeedBps;

            void SetReadSpeedBps(const uint64_t& value) {
                ReadSpeedBps = value;
                hasReadSpeedBps = true;
            }

            bool HasReadSpeedBps() const {
                return hasReadSpeedBps;
            }

            const uint64_t& GetReadSpeedBps() const {
                return ReadSpeedBps;
            }

            void ClearReadSpeedBps() {
                ReadSpeedBps = {};
                hasReadSpeedBps = false;
            }

            uint64_t* MutableReadSpeedBps() {
                return &ReadSpeedBps;
            }


            /// uint64_t WriteSpeedBps
            uint64_t WriteSpeedBps;
            bool hasWriteSpeedBps;

            void SetWriteSpeedBps(const uint64_t& value) {
                WriteSpeedBps = value;
                hasWriteSpeedBps = true;
            }

            bool HasWriteSpeedBps() const {
                return hasWriteSpeedBps;
            }

            const uint64_t& GetWriteSpeedBps() const {
                return WriteSpeedBps;
            }

            void ClearWriteSpeedBps() {
                WriteSpeedBps = {};
                hasWriteSpeedBps = false;
            }

            uint64_t* MutableWriteSpeedBps() {
                return &WriteSpeedBps;
            }


            /// uint64_t ReadBlockSize
            uint64_t ReadBlockSize;
            bool hasReadBlockSize;

            void SetReadBlockSize(const uint64_t& value) {
                ReadBlockSize = value;
                hasReadBlockSize = true;
            }

            bool HasReadBlockSize() const {
                return hasReadBlockSize;
            }

            const uint64_t& GetReadBlockSize() const {
                return ReadBlockSize;
            }

            void ClearReadBlockSize() {
                ReadBlockSize = {};
                hasReadBlockSize = false;
            }

            uint64_t* MutableReadBlockSize() {
                return &ReadBlockSize;
            }


            /// uint64_t WriteBlockSize
            uint64_t WriteBlockSize;
            bool hasWriteBlockSize;

            void SetWriteBlockSize(const uint64_t& value) {
                WriteBlockSize = value;
                hasWriteBlockSize = true;
            }

            bool HasWriteBlockSize() const {
                return hasWriteBlockSize;
            }

            const uint64_t& GetWriteBlockSize() const {
                return WriteBlockSize;
            }

            void ClearWriteBlockSize() {
                WriteBlockSize = {};
                hasWriteBlockSize = false;
            }

            uint64_t* MutableWriteBlockSize() {
                return &WriteBlockSize;
            }


            /// uint32_t MinREALHugeBlobInBytes
            uint32_t MinREALHugeBlobInBytes;
            bool hasMinREALHugeBlobInBytes;

            void SetMinREALHugeBlobInBytes(const uint32_t& value) {
                MinREALHugeBlobInBytes = value;
                hasMinREALHugeBlobInBytes = true;
            }

            bool HasMinREALHugeBlobInBytes() const {
                return hasMinREALHugeBlobInBytes;
            }

            const uint32_t& GetMinREALHugeBlobInBytes() const {
                return MinREALHugeBlobInBytes;
            }

            void ClearMinREALHugeBlobInBytes() {
                MinREALHugeBlobInBytes = {};
                hasMinREALHugeBlobInBytes = false;
            }

            uint32_t* MutableMinREALHugeBlobInBytes() {
                return &MinREALHugeBlobInBytes;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TVDiskCostSettings";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t);
            }

            void CopyFrom(const TVDiskCostSettings& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint32_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TMsgQoS {
            /// uint32_t DeadlineSeconds
            uint32_t DeadlineSeconds;
            bool hasDeadlineSeconds;

            void SetDeadlineSeconds(const uint32_t& value) {
                DeadlineSeconds = value;
                hasDeadlineSeconds = true;
            }

            bool HasDeadlineSeconds() const {
                return hasDeadlineSeconds;
            }

            const uint32_t& GetDeadlineSeconds() const {
                return DeadlineSeconds;
            }

            void ClearDeadlineSeconds() {
                DeadlineSeconds = {};
                hasDeadlineSeconds = false;
            }

            uint32_t* MutableDeadlineSeconds() {
                return &DeadlineSeconds;
            }


            /// TMessageId MsgId
            TMessageId MsgId;
            bool hasMsgId;

            void SetMsgId(const TMessageId& value) {
                MsgId = value;
                hasMsgId = true;
            }

            bool HasMsgId() const {
                return hasMsgId;
            }

            const TMessageId& GetMsgId() const {
                return MsgId;
            }

            void ClearMsgId() {
                MsgId = {};
                hasMsgId = false;
            }

            TMessageId* MutableMsgId() {
                return &MsgId;
            }


            /// uint64_t Cost
            uint64_t Cost;
            bool hasCost;

            void SetCost(const uint64_t& value) {
                Cost = value;
                hasCost = true;
            }

            bool HasCost() const {
                return hasCost;
            }

            const uint64_t& GetCost() const {
                return Cost;
            }

            void ClearCost() {
                Cost = {};
                hasCost = false;
            }

            uint64_t* MutableCost() {
                return &Cost;
            }


            /// EVDiskQueueId ExtQueueId
            EVDiskQueueId ExtQueueId;
            bool hasExtQueueId;

            void SetExtQueueId(const EVDiskQueueId& value) {
                ExtQueueId = value;
                hasExtQueueId = true;
            }

            bool HasExtQueueId() const {
                return hasExtQueueId;
            }

            const EVDiskQueueId& GetExtQueueId() const {
                return ExtQueueId;
            }

            void ClearExtQueueId() {
                ExtQueueId = {};
                hasExtQueueId = false;
            }

            EVDiskQueueId* MutableExtQueueId() {
                return &ExtQueueId;
            }


            /// EVDiskInternalQueueId IntQueueId
            EVDiskInternalQueueId IntQueueId;
            bool hasIntQueueId;

            void SetIntQueueId(const EVDiskInternalQueueId& value) {
                IntQueueId = value;
                hasIntQueueId = true;
            }

            bool HasIntQueueId() const {
                return hasIntQueueId;
            }

            const EVDiskInternalQueueId& GetIntQueueId() const {
                return IntQueueId;
            }

            void ClearIntQueueId() {
                IntQueueId = {};
                hasIntQueueId = false;
            }

            EVDiskInternalQueueId* MutableIntQueueId() {
                return &IntQueueId;
            }


            /// TVDiskCostSettings CostSettings
            TVDiskCostSettings CostSettings;
            bool hasCostSettings;

            void SetCostSettings(const TVDiskCostSettings& value) {
                CostSettings = value;
                hasCostSettings = true;
            }

            bool HasCostSettings() const {
                return hasCostSettings;
            }

            const TVDiskCostSettings& GetCostSettings() const {
                return CostSettings;
            }

            void ClearCostSettings() {
                CostSettings = {};
                hasCostSettings = false;
            }

            TVDiskCostSettings* MutableCostSettings() {
                return &CostSettings;
            }


            /// bool SendMeCostSettings
            bool SendMeCostSettings;
            bool hasSendMeCostSettings;

            void SetSendMeCostSettings(const bool& value) {
                SendMeCostSettings = value;
                hasSendMeCostSettings = true;
            }

            bool HasSendMeCostSettings() const {
                return hasSendMeCostSettings;
            }

            const bool& GetSendMeCostSettings() const {
                return SendMeCostSettings;
            }

            void ClearSendMeCostSettings() {
                SendMeCostSettings = {};
                hasSendMeCostSettings = false;
            }

            bool* MutableSendMeCostSettings() {
                return &SendMeCostSettings;
            }


            /// TWindowFeedback Window
            TWindowFeedback Window;
            bool hasWindow;

            void SetWindow(const TWindowFeedback& value) {
                Window = value;
                hasWindow = true;
            }

            bool HasWindow() const {
                return hasWindow;
            }

            const TWindowFeedback& GetWindow() const {
                return Window;
            }

            void ClearWindow() {
                Window = {};
                hasWindow = false;
            }

            TWindowFeedback* MutableWindow() {
                return &Window;
            }


            /// uint32_t ProxyNodeId
            uint32_t ProxyNodeId = 10;
            bool hasProxyNodeId;

            void SetProxyNodeId(const uint32_t& value) {
                ProxyNodeId = value;
                hasProxyNodeId = true;
            }

            bool HasProxyNodeId() const {
                return hasProxyNodeId;
            }

            const uint32_t& GetProxyNodeId() const {
                return ProxyNodeId;
            }

            void ClearProxyNodeId() {
                ProxyNodeId = {};
                hasProxyNodeId = false;
            }

            uint32_t* MutableProxyNodeId() {
                return &ProxyNodeId;
            }


            /// uint32_t ReplVDiskId
            uint32_t ReplVDiskId = 11;
            bool hasReplVDiskId;

            void SetReplVDiskId(const uint32_t& value) {
                ReplVDiskId = value;
                hasReplVDiskId = true;
            }

            bool HasReplVDiskId() const {
                return hasReplVDiskId;
            }

            const uint32_t& GetReplVDiskId() const {
                return ReplVDiskId;
            }

            void ClearReplVDiskId() {
                ReplVDiskId = {};
                hasReplVDiskId = false;
            }

            uint32_t* MutableReplVDiskId() {
                return &ReplVDiskId;
            }


            /// uint64_t VDiskLoadId
            uint64_t VDiskLoadId = 13;
            bool hasVDiskLoadId;

            void SetVDiskLoadId(const uint64_t& value) {
                VDiskLoadId = value;
                hasVDiskLoadId = true;
            }

            bool HasVDiskLoadId() const {
                return hasVDiskLoadId;
            }

            const uint64_t& GetVDiskLoadId() const {
                return VDiskLoadId;
            }

            void ClearVDiskLoadId() {
                VDiskLoadId = {};
                hasVDiskLoadId = false;
            }

            uint64_t* MutableVDiskLoadId() {
                return &VDiskLoadId;
            }


            /// uint32_t VPatchVDiskId
            uint32_t VPatchVDiskId = 14;
            bool hasVPatchVDiskId;

            void SetVPatchVDiskId(const uint32_t& value) {
                VPatchVDiskId = value;
                hasVPatchVDiskId = true;
            }

            bool HasVPatchVDiskId() const {
                return hasVPatchVDiskId;
            }

            const uint32_t& GetVPatchVDiskId() const {
                return VPatchVDiskId;
            }

            void ClearVPatchVDiskId() {
                VPatchVDiskId = {};
                hasVPatchVDiskId = false;
            }

            uint32_t* MutableVPatchVDiskId() {
                return &VPatchVDiskId;
            }


            /// TExecTimeStats ExecTimeStats
            TExecTimeStats ExecTimeStats;
            bool hasExecTimeStats;

            void SetExecTimeStats(const TExecTimeStats& value) {
                ExecTimeStats = value;
                hasExecTimeStats = true;
            }

            bool HasExecTimeStats() const {
                return hasExecTimeStats;
            }

            const TExecTimeStats& GetExecTimeStats() const {
                return ExecTimeStats;
            }

            void ClearExecTimeStats() {
                ExecTimeStats = {};
                hasExecTimeStats = false;
            }

            TExecTimeStats* MutableExecTimeStats() {
                return &ExecTimeStats;
            }


            /// TActorId SenderActorId
            TActorId SenderActorId;
            bool hasSenderActorId;

            void SetSenderActorId(const TActorId& value) {
                SenderActorId = value;
                hasSenderActorId = true;
            }

            bool HasSenderActorId() const {
                return hasSenderActorId;
            }

            const TActorId& GetSenderActorId() const {
                return SenderActorId;
            }

            void ClearSenderActorId() {
                SenderActorId = {};
                hasSenderActorId = false;
            }

            TActorId* MutableSenderActorId() {
                return &SenderActorId;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TMsgQoS";
            }

            int ByteSize() const {
                return sizeof(uint32_t) + sizeof(TMessageId) + sizeof(uint64_t) + sizeof(EVDiskQueueId) + sizeof(EVDiskInternalQueueId) + sizeof(TVDiskCostSettings) + sizeof(bool) + sizeof(TWindowFeedback) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(TExecTimeStats) + sizeof(TActorId);
            }

            void CopyFrom(const TMsgQoS& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint32_t) + sizeof(TMessageId) + sizeof(uint64_t) + sizeof(EVDiskQueueId) + sizeof(EVDiskInternalQueueId) + sizeof(TVDiskCostSettings) + sizeof(bool) + sizeof(TWindowFeedback) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint64_t) + sizeof(uint32_t) + sizeof(TExecTimeStats) + sizeof(TActorId));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TVDiskID {
            /// uint32_t GroupID
            uint32_t GroupID;
            bool hasGroupID;

            void SetGroupID(const uint32_t& value) {
                GroupID = value;
                hasGroupID = true;
            }

            bool HasGroupID() const {
                return hasGroupID;
            }

            const uint32_t& GetGroupID() const {
                return GroupID;
            }

            void ClearGroupID() {
                GroupID = {};
                hasGroupID = false;
            }

            uint32_t* MutableGroupID() {
                return &GroupID;
            }


            /// uint32_t GroupGeneration
            uint32_t GroupGeneration;
            bool hasGroupGeneration;

            void SetGroupGeneration(const uint32_t& value) {
                GroupGeneration = value;
                hasGroupGeneration = true;
            }

            bool HasGroupGeneration() const {
                return hasGroupGeneration;
            }

            const uint32_t& GetGroupGeneration() const {
                return GroupGeneration;
            }

            void ClearGroupGeneration() {
                GroupGeneration = {};
                hasGroupGeneration = false;
            }

            uint32_t* MutableGroupGeneration() {
                return &GroupGeneration;
            }


            /// uint32_t Ring
            uint32_t Ring;
            bool hasRing;

            void SetRing(const uint32_t& value) {
                Ring = value;
                hasRing = true;
            }

            bool HasRing() const {
                return hasRing;
            }

            const uint32_t& GetRing() const {
                return Ring;
            }

            void ClearRing() {
                Ring = {};
                hasRing = false;
            }

            uint32_t* MutableRing() {
                return &Ring;
            }


            /// uint32_t Domain
            uint32_t Domain;
            bool hasDomain;

            void SetDomain(const uint32_t& value) {
                Domain = value;
                hasDomain = true;
            }

            bool HasDomain() const {
                return hasDomain;
            }

            const uint32_t& GetDomain() const {
                return Domain;
            }

            void ClearDomain() {
                Domain = {};
                hasDomain = false;
            }

            uint32_t* MutableDomain() {
                return &Domain;
            }


            /// uint32_t VDisk
            uint32_t VDisk;
            bool hasVDisk;

            void SetVDisk(const uint32_t& value) {
                VDisk = value;
                hasVDisk = true;
            }

            bool HasVDisk() const {
                return hasVDisk;
            }

            const uint32_t& GetVDisk() const {
                return VDisk;
            }

            void ClearVDisk() {
                VDisk = {};
                hasVDisk = false;
            }

            uint32_t* MutableVDisk() {
                return &VDisk;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TVDiskID";
            }

            int ByteSize() const {
                return sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t);
            }

            void CopyFrom(const TVDiskID& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t) + sizeof(uint32_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TLogoBlobID {
            /// uint64_t RawX1
            uint64_t RawX1;
            bool hasRawX1;

            void SetRawX1(const uint64_t& value) {
                RawX1 = value;
                hasRawX1 = true;
            }

            bool HasRawX1() const {
                return hasRawX1;
            }

            const uint64_t& GetRawX1() const {
                return RawX1;
            }

            void ClearRawX1() {
                RawX1 = {};
                hasRawX1 = false;
            }

            uint64_t* MutableRawX1() {
                return &RawX1;
            }


            /// uint64_t RawX2
            uint64_t RawX2;
            bool hasRawX2;

            void SetRawX2(const uint64_t& value) {
                RawX2 = value;
                hasRawX2 = true;
            }

            bool HasRawX2() const {
                return hasRawX2;
            }

            const uint64_t& GetRawX2() const {
                return RawX2;
            }

            void ClearRawX2() {
                RawX2 = {};
                hasRawX2 = false;
            }

            uint64_t* MutableRawX2() {
                return &RawX2;
            }


            /// uint64_t RawX3
            uint64_t RawX3;
            bool hasRawX3;

            void SetRawX3(const uint64_t& value) {
                RawX3 = value;
                hasRawX3 = true;
            }

            bool HasRawX3() const {
                return hasRawX3;
            }

            const uint64_t& GetRawX3() const {
                return RawX3;
            }

            void ClearRawX3() {
                RawX3 = {};
                hasRawX3 = false;
            }

            uint64_t* MutableRawX3() {
                return &RawX3;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TLogoBlobID";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
            }

            void CopyFrom(const TLogoBlobID& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TRangeQuery {
            /// TLogoBlobID From
            TLogoBlobID From;
            bool hasFrom;

            void SetFrom(const TLogoBlobID& value) {
                From = value;
                hasFrom = true;
            }

            bool HasFrom() const {
                return hasFrom;
            }

            const TLogoBlobID& GetFrom() const {
                return From;
            }

            void ClearFrom() {
                From = {};
                hasFrom = false;
            }

            TLogoBlobID* MutableFrom() {
                return &From;
            }


            /// TLogoBlobID To
            TLogoBlobID To;
            bool hasTo;

            void SetTo(const TLogoBlobID& value) {
                To = value;
                hasTo = true;
            }

            bool HasTo() const {
                return hasTo;
            }

            const TLogoBlobID& GetTo() const {
                return To;
            }

            void ClearTo() {
                To = {};
                hasTo = false;
            }

            TLogoBlobID* MutableTo() {
                return &To;
            }


            /// uint64_t Cookie
            uint64_t Cookie;
            bool hasCookie;

            void SetCookie(const uint64_t& value) {
                Cookie = value;
                hasCookie = true;
            }

            bool HasCookie() const {
                return hasCookie;
            }

            const uint64_t& GetCookie() const {
                return Cookie;
            }

            void ClearCookie() {
                Cookie = {};
                hasCookie = false;
            }

            uint64_t* MutableCookie() {
                return &Cookie;
            }


            /// uint32_t MaxResults
            uint32_t MaxResults;
            bool hasMaxResults;

            void SetMaxResults(const uint32_t& value) {
                MaxResults = value;
                hasMaxResults = true;
            }

            bool HasMaxResults() const {
                return hasMaxResults;
            }

            const uint32_t& GetMaxResults() const {
                return MaxResults;
            }

            void ClearMaxResults() {
                MaxResults = {};
                hasMaxResults = false;
            }

            uint32_t* MutableMaxResults() {
                return &MaxResults;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TRangeQuery";
            }

            int ByteSize() const {
                return sizeof(TLogoBlobID) + sizeof(TLogoBlobID) + sizeof(uint64_t) + sizeof(uint32_t);
            }

            void CopyFrom(const TRangeQuery& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(TLogoBlobID) + sizeof(TLogoBlobID) + sizeof(uint64_t) + sizeof(uint32_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TExtremeQuery {
            /// TLogoBlobID Id
            TLogoBlobID Id;
            bool hasId;

            void SetId(const TLogoBlobID& value) {
                Id = value;
                hasId = true;
            }

            bool HasId() const {
                return hasId;
            }

            const TLogoBlobID& GetId() const {
                return Id;
            }

            void ClearId() {
                Id = {};
                hasId = false;
            }

            TLogoBlobID* MutableId() {
                return &Id;
            }


            /// uint64_t Shift
            uint64_t Shift;
            bool hasShift;

            void SetShift(const uint64_t& value) {
                Shift = value;
                hasShift = true;
            }

            bool HasShift() const {
                return hasShift;
            }

            const uint64_t& GetShift() const {
                return Shift;
            }

            void ClearShift() {
                Shift = {};
                hasShift = false;
            }

            uint64_t* MutableShift() {
                return &Shift;
            }


            /// uint64_t Size
            uint64_t Size;
            bool hasSize;

            void SetSize(const uint64_t& value) {
                Size = value;
                hasSize = true;
            }

            bool HasSize() const {
                return hasSize;
            }

            const uint64_t& GetSize() const {
                return Size;
            }

            void ClearSize() {
                Size = {};
                hasSize = false;
            }

            uint64_t* MutableSize() {
                return &Size;
            }


            /// uint64_t Cookie
            uint64_t Cookie;
            bool hasCookie;

            void SetCookie(const uint64_t& value) {
                Cookie = value;
                hasCookie = true;
            }

            bool HasCookie() const {
                return hasCookie;
            }

            const uint64_t& GetCookie() const {
                return Cookie;
            }

            void ClearCookie() {
                Cookie = {};
                hasCookie = false;
            }

            uint64_t* MutableCookie() {
                return &Cookie;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TExtremeQuery";
            }

            int ByteSize() const {
                return sizeof(TLogoBlobID) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t);
            }

            void CopyFrom(const TExtremeQuery& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(TLogoBlobID) + sizeof(uint64_t) + sizeof(uint64_t) + sizeof(uint64_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TTabletData {
            /// uint64_t Id
            uint64_t Id;
            bool hasId;

            void SetId(const uint64_t& value) {
                Id = value;
                hasId = true;
            }

            bool HasId() const {
                return hasId;
            }

            const uint64_t& GetId() const {
                return Id;
            }

            void ClearId() {
                Id = {};
                hasId = false;
            }

            uint64_t* MutableId() {
                return &Id;
            }


            /// uint32_t Generation
            uint32_t Generation;
            bool hasGeneration;

            void SetGeneration(const uint32_t& value) {
                Generation = value;
                hasGeneration = true;
            }

            bool HasGeneration() const {
                return hasGeneration;
            }

            const uint32_t& GetGeneration() const {
                return Generation;
            }

            void ClearGeneration() {
                Generation = {};
                hasGeneration = false;
            }

            uint32_t* MutableGeneration() {
                return &Generation;
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TTabletData";
            }

            int ByteSize() const {
                return sizeof(uint64_t) + sizeof(uint32_t);
            }

            void CopyFrom(const TTabletData& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(uint64_t) + sizeof(uint32_t));
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };

        struct TEvVGet {
            /// TRangeQuery RangeQuery
            TRangeQuery RangeQuery;
            bool hasRangeQuery;

            void SetRangeQuery(const TRangeQuery& value) {
                RangeQuery = value;
                hasRangeQuery = true;
            }

            bool HasRangeQuery() const {
                return hasRangeQuery;
            }

            const TRangeQuery& GetRangeQuery() const {
                return RangeQuery;
            }

            void ClearRangeQuery() {
                RangeQuery = {};
                hasRangeQuery = false;
            }

            TRangeQuery* MutableRangeQuery() {
                return &RangeQuery;
            }


            /// TVDiskID VDiskID
            TVDiskID VDiskID;
            bool hasVDiskID;

            void SetVDiskID(const TVDiskID& value) {
                VDiskID = value;
                hasVDiskID = true;
            }

            bool HasVDiskID() const {
                return hasVDiskID;
            }

            const TVDiskID& GetVDiskID() const {
                return VDiskID;
            }

            void ClearVDiskID() {
                VDiskID = {};
                hasVDiskID = false;
            }

            TVDiskID* MutableVDiskID() {
                return &VDiskID;
            }


            /// bool NotifyIfNotReady
            bool NotifyIfNotReady;
            bool hasNotifyIfNotReady;

            void SetNotifyIfNotReady(const bool& value) {
                NotifyIfNotReady = value;
                hasNotifyIfNotReady = true;
            }

            bool HasNotifyIfNotReady() const {
                return hasNotifyIfNotReady;
            }

            const bool& GetNotifyIfNotReady() const {
                return NotifyIfNotReady;
            }

            void ClearNotifyIfNotReady() {
                NotifyIfNotReady = {};
                hasNotifyIfNotReady = false;
            }

            bool* MutableNotifyIfNotReady() {
                return &NotifyIfNotReady;
            }


            /// bool ShowInternals
            bool ShowInternals;
            bool hasShowInternals;

            void SetShowInternals(const bool& value) {
                ShowInternals = value;
                hasShowInternals = true;
            }

            bool HasShowInternals() const {
                return hasShowInternals;
            }

            const bool& GetShowInternals() const {
                return ShowInternals;
            }

            void ClearShowInternals() {
                ShowInternals = {};
                hasShowInternals = false;
            }

            bool* MutableShowInternals() {
                return &ShowInternals;
            }


            /// uint64_t Cookie
            uint64_t Cookie;
            bool hasCookie;

            void SetCookie(const uint64_t& value) {
                Cookie = value;
                hasCookie = true;
            }

            bool HasCookie() const {
                return hasCookie;
            }

            const uint64_t& GetCookie() const {
                return Cookie;
            }

            void ClearCookie() {
                Cookie = {};
                hasCookie = false;
            }

            uint64_t* MutableCookie() {
                return &Cookie;
            }


            /// TMsgQoS MsgQoS
            TMsgQoS MsgQoS;
            bool hasMsgQoS;

            void SetMsgQoS(const TMsgQoS& value) {
                MsgQoS = value;
                hasMsgQoS = true;
            }

            bool HasMsgQoS() const {
                return hasMsgQoS;
            }

            const TMsgQoS& GetMsgQoS() const {
                return MsgQoS;
            }

            void ClearMsgQoS() {
                MsgQoS = {};
                hasMsgQoS = false;
            }

            TMsgQoS* MutableMsgQoS() {
                return &MsgQoS;
            }


            /// bool IndexOnly
            bool IndexOnly = false;
            bool hasIndexOnly;

            void SetIndexOnly(const bool& value) {
                IndexOnly = value;
                hasIndexOnly = true;
            }

            bool HasIndexOnly() const {
                return hasIndexOnly;
            }

            const bool& GetIndexOnly() const {
                return IndexOnly;
            }

            void ClearIndexOnly() {
                IndexOnly = {};
                hasIndexOnly = false;
            }

            bool* MutableIndexOnly() {
                return &IndexOnly;
            }


            /// EGetHandleClass HandleClass
            EGetHandleClass HandleClass;
            bool hasHandleClass;

            void SetHandleClass(const EGetHandleClass& value) {
                HandleClass = value;
                hasHandleClass = true;
            }

            bool HasHandleClass() const {
                return hasHandleClass;
            }

            const EGetHandleClass& GetHandleClass() const {
                return HandleClass;
            }

            void ClearHandleClass() {
                HandleClass = {};
                hasHandleClass = false;
            }

            EGetHandleClass* MutableHandleClass() {
                return &HandleClass;
            }


            /// bool SuppressBarrierCheck
            bool SuppressBarrierCheck = false;
            bool hasSuppressBarrierCheck;

            void SetSuppressBarrierCheck(const bool& value) {
                SuppressBarrierCheck = value;
                hasSuppressBarrierCheck = true;
            }

            bool HasSuppressBarrierCheck() const {
                return hasSuppressBarrierCheck;
            }

            const bool& GetSuppressBarrierCheck() const {
                return SuppressBarrierCheck;
            }

            void ClearSuppressBarrierCheck() {
                SuppressBarrierCheck = {};
                hasSuppressBarrierCheck = false;
            }

            bool* MutableSuppressBarrierCheck() {
                return &SuppressBarrierCheck;
            }


            /// uint64_t TabletId
            uint64_t TabletId = 0;
            bool hasTabletId;

            void SetTabletId(const uint64_t& value) {
                TabletId = value;
                hasTabletId = true;
            }

            bool HasTabletId() const {
                return hasTabletId;
            }

            const uint64_t& GetTabletId() const {
                return TabletId;
            }

            void ClearTabletId() {
                TabletId = {};
                hasTabletId = false;
            }

            uint64_t* MutableTabletId() {
                return &TabletId;
            }


            /// bool AcquireBlockedGeneration
            bool AcquireBlockedGeneration = false;
            bool hasAcquireBlockedGeneration;

            void SetAcquireBlockedGeneration(const bool& value) {
                AcquireBlockedGeneration = value;
                hasAcquireBlockedGeneration = true;
            }

            bool HasAcquireBlockedGeneration() const {
                return hasAcquireBlockedGeneration;
            }

            const bool& GetAcquireBlockedGeneration() const {
                return AcquireBlockedGeneration;
            }

            void ClearAcquireBlockedGeneration() {
                AcquireBlockedGeneration = {};
                hasAcquireBlockedGeneration = false;
            }

            bool* MutableAcquireBlockedGeneration() {
                return &AcquireBlockedGeneration;
            }


            /// TTimestamps Timestamps
            TTimestamps Timestamps;
            bool hasTimestamps;

            void SetTimestamps(const TTimestamps& value) {
                Timestamps = value;
                hasTimestamps = true;
            }

            bool HasTimestamps() const {
                return hasTimestamps;
            }

            const TTimestamps& GetTimestamps() const {
                return Timestamps;
            }

            void ClearTimestamps() {
                Timestamps = {};
                hasTimestamps = false;
            }

            TTimestamps* MutableTimestamps() {
                return &Timestamps;
            }


            /// uint32_t ForceBlockedGeneration
            uint32_t ForceBlockedGeneration = 0;
            bool hasForceBlockedGeneration;

            void SetForceBlockedGeneration(const uint32_t& value) {
                ForceBlockedGeneration = value;
                hasForceBlockedGeneration = true;
            }

            bool HasForceBlockedGeneration() const {
                return hasForceBlockedGeneration;
            }

            const uint32_t& GetForceBlockedGeneration() const {
                return ForceBlockedGeneration;
            }

            void ClearForceBlockedGeneration() {
                ForceBlockedGeneration = {};
                hasForceBlockedGeneration = false;
            }

            uint32_t* MutableForceBlockedGeneration() {
                return &ForceBlockedGeneration;
            }


            /// TTabletData ReaderTabletData
            TTabletData ReaderTabletData;
            bool hasReaderTabletData;

            void SetReaderTabletData(const TTabletData& value) {
                ReaderTabletData = value;
                hasReaderTabletData = true;
            }

            bool HasReaderTabletData() const {
                return hasReaderTabletData;
            }

            const TTabletData& GetReaderTabletData() const {
                return ReaderTabletData;
            }

            void ClearReaderTabletData() {
                ReaderTabletData = {};
                hasReaderTabletData = false;
            }

            TTabletData* MutableReaderTabletData() {
                return &ReaderTabletData;
            }


            /// TTabletData ForceBlockTabletData
            TTabletData ForceBlockTabletData;
            bool hasForceBlockTabletData;

            void SetForceBlockTabletData(const TTabletData& value) {
                ForceBlockTabletData = value;
                hasForceBlockTabletData = true;
            }

            bool HasForceBlockTabletData() const {
                return hasForceBlockTabletData;
            }

            const TTabletData& GetForceBlockTabletData() const {
                return ForceBlockTabletData;
            }

            void ClearForceBlockTabletData() {
                ForceBlockTabletData = {};
                hasForceBlockTabletData = false;
            }

            TTabletData* MutableForceBlockTabletData() {
                return &ForceBlockTabletData;
            }


            /// TString SnapshotId
            TString SnapshotId;
            bool hasSnapshotId;

            void SetSnapshotId(const TString& value) {
                SnapshotId = value;
                hasSnapshotId = true;
            }

            bool HasSnapshotId() const {
                return hasSnapshotId;
            }

            const TString& GetSnapshotId() const {
                return SnapshotId;
            }

            void ClearSnapshotId() {
                SnapshotId = {};
                hasSnapshotId = false;
            }

            TString* MutableSnapshotId() {
                return &SnapshotId;
            }


            /// TExtremeQuery ExtremeQueries <repeated>
            std::vector<TExtremeQuery> ExtremeQueries;

            const std::vector<TExtremeQuery>& GetExtremeQueries() const {
                return ExtremeQueries;
            }

            const TExtremeQuery& GetExtremeQueries(int i) const {
                return ExtremeQueries[i];
            }

            TExtremeQuery* AddExtremeQueries() {
                ExtremeQueries.push_back({});
                return &ExtremeQueries.back();
            }

            size_t ExtremeQueriesSize() const {
                return ExtremeQueries.size();
            }


            /// struct-wide methods
            TString GetTypeName() const {
                return "TEvVGet";
            }

            int ByteSize() const {
                return sizeof(TRangeQuery) + sizeof(TVDiskID) + sizeof(bool) + sizeof(bool) + sizeof(uint64_t) + sizeof(TMsgQoS) + sizeof(bool) + sizeof(EGetHandleClass) + sizeof(bool) + sizeof(uint64_t) + sizeof(bool) + sizeof(TTimestamps) + sizeof(uint32_t) + sizeof(TTabletData) + sizeof(TTabletData) + sizeof(TString) + sizeof(TExtremeQuery) * ExtremeQueries.size();
            }

            void CopyFrom(const TEvVGet& other) {
                *this = other;
            }

            std::string ShortDebugString() const {
                return "short debug string";
            }

            bool SerializeToZeroCopyStream(NProtoBuf::io::ZeroCopyOutputStream *output) const {
                output->WriteAliasedRaw(this, sizeof(TRangeQuery) + sizeof(TVDiskID) + sizeof(bool) + sizeof(bool) + sizeof(uint64_t) + sizeof(TMsgQoS) + sizeof(bool) + sizeof(EGetHandleClass) + sizeof(bool) + sizeof(uint64_t) + sizeof(bool) + sizeof(TTimestamps) + sizeof(uint32_t) + sizeof(TTabletData) + sizeof(TTabletData) + sizeof(TString));
                output->WriteAliasedRaw(ExtremeQueries.data(), sizeof(TExtremeQuery) * ExtremeQueries.size());
                return true;

            }

            bool ParseFromString(TString) {
                *this = {};
                return true;
            }

        };
    }
}