#pragma once

#include <library/cpp/lwtrace/all.h>
#include <util/generic/vector.h>

#define LWACTORID(x) (x).RawX1(), (x).RawX2(), (x).NodeId(), (x).PoolID()
#define LWTYPE_ACTORID ui64, ui64, ui32, ui32
#define LWNAME_ACTORID(n) n "Raw1", n "Raw2", n "NodeId", n "PoolId"

#define ACTORLIB_PROVIDER(PROBE, EVENT, GROUPS, TYPES, NAMES)                                                                         \
    PROBE(SlowEvent, GROUPS("ActorLibSlow"),                                                                                          \
          TYPES(ui32, double, TString, TString, TString),                                                                             \
          NAMES("poolId", "eventMs", "eventType", "actorId", "actorType"))                                                            \
    PROBE(EventSlowDelivery, GROUPS("ActorLibSlow"),                                                                                  \
          TYPES(ui32, double, double, ui64, TString, TString, TString),                                                               \
          NAMES("poolId", "deliveryMs", "sinceActivationMs", "eventProcessedBefore", "eventType", "actorId", "actorType"))            \
    PROBE(SlowActivation, GROUPS("ActorLibSlow"),                                                                                     \
          TYPES(ui32, double),                                                                                                        \
          NAMES("poolId", "activationMs"))                                                                                            \
    PROBE(SlowRegisterNew, GROUPS("ActorLibSlow"),                                                                                    \
          TYPES(ui32, double),                                                                                                        \
          NAMES("poolId", "registerNewMs"))                                                                                           \
    PROBE(SlowRegisterAdd, GROUPS("ActorLibSlow"),                                                                                    \
          TYPES(ui32, double),                                                                                                        \
          NAMES("poolId", "registerAddMs"))                                                                                           \
    PROBE(MailboxPushedOutByTailSending, GROUPS("ActorLibMailbox", "ActorLibMailboxPushedOut"),                                       \
          TYPES(ui32, TString, ui32, TDuration, ui64, TString, TString),                                                              \
          NAMES("poolId", "pool", "eventsProcessed", "procTimeMs", "workerId", "actorId", "actorType"))                               \
    PROBE(MailboxPushedOutBySoftPreemption, GROUPS("ActorLibMailbox", "ActorLibMailboxPushedOut"),                                    \
          TYPES(ui32, TString, ui32, TDuration, ui64, TString, TString),                                                              \
          NAMES("poolId", "pool", "eventsProcessed", "procTimeMs", "workerId", "actorId", "actorType"))                               \
    PROBE(MailboxPushedOutByTime, GROUPS("ActorLibMailbox", "ActorLibMailboxPushedOut"),                                              \
          TYPES(ui32, TString, ui32, TDuration, ui64, TString, TString),                                                              \
          NAMES("poolId", "pool", "eventsProcessed", "procTimeMs", "workerId", "actorId", "actorType"))                               \
    PROBE(MailboxPushedOutByEventCount, GROUPS("ActorLibMailbox", "ActorLibMailboxPushedOut"),                                        \
          TYPES(ui32, TString, ui32, TDuration, ui64, TString, TString),                                                              \
          NAMES("poolId", "pool", "eventsProcessed", "procTimeMs", "workerId", "actorId", "actorType"))                               \
    PROBE(MailboxEmpty, GROUPS("ActorLibMailbox"),                                                                                    \
          TYPES(ui32, TString, ui32, TDuration, ui64, TString, TString),                                                              \
          NAMES("poolId", "pool", "eventsProcessed", "procTimeMs", "workerId", "actorId", "actorType"))                               \
    PROBE(ActivationBegin, GROUPS(),                                                                                                  \
          TYPES(ui32, ui32),                                                                                            \
          NAMES("poolId", "workerId"))                                                                             \
    PROBE(ActivationEnd, GROUPS(),                                                                                                    \
          TYPES(ui32, ui32),                                                                                                    \
          NAMES("poolId", "workerId"))                                                                                         \
    PROBE(ExecutorThreadStats, GROUPS("ActorLibStats"),                                                                               \
          TYPES(ui32, TString, ui64, ui64, ui64, double, double),                                                                     \
          NAMES("poolId", "pool", "workerId", "execCount", "readyActivationCount", "execMs", "nonExecMs"))                            \
    PROBE(SlowICReadLoopAdjustSize, GROUPS("ActorLibSlowIC"),                                                                         \
          TYPES(double),                                                                                                              \
          NAMES("icReadLoopAdjustSizeMs"))                                                                                            \
    PROBE(SlowICReadFromSocket, GROUPS("ActorLibSlowIC"),                                                                             \
          TYPES(double),                                                                                                              \
          NAMES("icReadFromSocketMs"))                                                                                                \
    PROBE(SlowICReadLoopSend, GROUPS("ActorLibSlowIC"),                                                                               \
          TYPES(double),                                                                                                              \
          NAMES("icReadLoopSendMs"))                                                                                                  \
    PROBE(SlowICAllocPacketBuffer, GROUPS("ActorLibSlowIC"),                                                                          \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "icAllocPacketBufferMs"))                                                                                   \
    PROBE(SlowICFillSendingBuffer, GROUPS("ActorLibSlowIC"),                                                                          \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "icFillSendingBufferMs"))                                                                                   \
    PROBE(SlowICPushSentPackets, GROUPS("ActorLibSlowIC"),                                                                            \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "icPushSentPacketsMs"))                                                                                     \
    PROBE(SlowICPushSendQueue, GROUPS("ActorLibSlowIC"),                                                                              \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "icPushSendQueueMs"))                                                                                       \
    PROBE(SlowICWriteData, GROUPS("ActorLibSlowIC"),                                                                                  \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "icWriteDataMs"))                                                                                           \
    PROBE(SlowICDropConfirmed, GROUPS("ActorLibSlowIC"),                                                                              \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "icDropConfirmedMs"))                                                                                       \
    PROBE(ActorsystemScheduler, GROUPS("Durations"),                                                                                  \
          TYPES(ui64, ui64, ui32, ui32, ui64, ui64),                                                                                  \
          NAMES("timeUs", "timerfd_expirations", "eventsGottenFromQueues", "eventsSent",                                              \
                "eventsInSendQueue", "eventSchedulingErrorUs"))                                                                       \
    PROBE(ForwardEvent, GROUPS("Orbit", "InterconnectSessionTCP"),                                                                    \
          TYPES(ui32, ui32, ui32, LWTYPE_ACTORID, LWTYPE_ACTORID, ui64, ui32),                                                        \
          NAMES("peerId", "type", "flags", LWNAME_ACTORID("r"), LWNAME_ACTORID("s"),                                                  \
                "cookie", "eventSerializedSize"))                                                                                     \
    PROBE(EnqueueEvent, GROUPS("InterconnectSessionTCP"),                                                                             \
          TYPES(ui32, ui64, TDuration, ui16, ui64, ui64),                                                                             \
          NAMES("peerId", "numEventsInReadyChannels", "enqueueBlockedTotalMs", "channelId", "queueSizeInEvents", "queueSizeInBytes")) \
    PROBE(SerializeToPacketBegin, GROUPS("InterconnectSessionTCP"),                                                                   \
          TYPES(ui32, ui16, ui64),                                                                                                    \
          NAMES("peerId", "channelId", "outputQueueSize"))                                                                            \
    PROBE(SerializeToPacketEnd, GROUPS("InterconnectSessionTCP"),                                                                     \
          TYPES(ui32, ui16, ui64, ui64),                                                                                              \
          NAMES("peerId", "channelId", "outputQueueSize", "offsetInPacket"))                                                          \
    PROBE(FillSendingBuffer, GROUPS("InterconnectSessionTCP"),                                                                        \
          TYPES(ui32, ui32, ui64, TDuration),                                                                                         \
          NAMES("peerId", "taskBytesGenerated", "numEventsInReadyChannelsBehind", "fillBlockedTotalMs"))                              \
    PROBE(PacketGenerated, GROUPS("InterconnectSessionTCP"),                                                                          \
          TYPES(ui32, ui64, ui64, ui64, ui64),                                                                                        \
          NAMES("peerId", "bytesUnwritten", "inflightBytes", "packetsGenerated", "packetSize"))                                       \
    PROBE(PacketWrittenToSocket, GROUPS("InterconnectSessionTCP"),                                                                    \
          TYPES(ui32, ui64, bool, ui64, ui64, TDuration, int),                                                                        \
          NAMES("peerId", "packetsWrittenToSocket", "triedWriting", "packetDataSize", "bytesUnwritten", "writeBlockedTotalMs", "fd")) \
    PROBE(GenerateTraffic, GROUPS("InterconnectSessionTCP"),                                                                          \
          TYPES(ui32, double, ui64, ui32, ui64),                                                                                      \
          NAMES("peerId", "generateTrafficMs", "dataBytesSent", "generatedPackets", "generatedBytes"))                                \
    PROBE(WriteToSocket, GROUPS("InterconnectSessionTCP"),                                                                            \
          TYPES(ui32, ui64, ui64, ui64, ui64, TDuration, int),                                                                        \
          NAMES("peerId", "bytesWritten", "packetsWritten", "packetsWrittenToSocket", "bytesUnwritten", "writeBlockedTotalMs", "fd")) \
    PROBE(UpdateFromInputSession, GROUPS("InterconnectSessionTCP"),                                                                   \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "pingMs"))                                                                                                  \
    PROBE(UnblockByDropConfirmed, GROUPS("InterconnectSessionTCP"),                                                                   \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "updateDeliveryMs"))                                                                                        \
    PROBE(DropConfirmed, GROUPS("InterconnectSessionTCP"),                                                                            \
          TYPES(ui32, ui64, ui64),                                                                                                    \
          NAMES("peerId", "droppedBytes", "inflightBytes"))                                                                           \
    PROBE(StartRam, GROUPS("InterconnectSessionTCP"),                                                                                 \
          TYPES(ui32),                                                                                                                \
          NAMES("peerId"))                                                                                                            \
    PROBE(FinishRam, GROUPS("InterconnectSessionTCP"),                                                                                \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "ramMs"))                                                                                                   \
    PROBE(SkipGenerateTraffic, GROUPS("InterconnectSessionTCP"),                                                                      \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "elapsedSinceRamMs"))                                                                                       \
    PROBE(StartBatching, GROUPS("InterconnectSessionTCP"),                                                                            \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "batchPeriodMs"))                                                                                           \
    PROBE(FinishBatching, GROUPS("InterconnectSessionTCP"),                                                                           \
          TYPES(ui32, double),                                                                                                        \
          NAMES("peerId", "finishBatchDeliveryMs"))                                                                                   \
    PROBE(BlockedWrite, GROUPS("InterconnectSessionTCP"),                                                                             \
          TYPES(ui32, double, ui64),                                                                                                  \
          NAMES("peerId", "sendQueueSize", "writtenBytes"))                                                                           \
    PROBE(ReadyWrite, GROUPS("InterconnectSessionTCP"),                                                                               \
          TYPES(ui32, double, double),                                                                                                \
          NAMES("peerId", "readyWriteDeliveryMs", "blockMs"))                                                                         \
    PROBE(EpollStartWaitIn, GROUPS("EpollThread"),                                                                                    \
          TYPES(),                                                                                                                    \
          NAMES())                                                                                                                    \
    PROBE(EpollFinishWaitIn, GROUPS("EpollThread"),                                                                                   \
          TYPES(i32),                                                                                                                 \
          NAMES("eventsCount"))                                                                                                       \
    PROBE(EpollWaitOut, GROUPS("EpollThread"),                                                                                        \
          TYPES(i32),                                                                                                                 \
          NAMES("eventsCount"))                                                                                                       \
    PROBE(EpollSendReadyRead, GROUPS("EpollThread"),                                                                                  \
          TYPES(bool, bool, int),                                                                                                     \
          NAMES("hangup", "event", "fd"))                                                                                             \
    PROBE(EpollSendReadyWrite, GROUPS("EpollThread"),                                                                                 \
          TYPES(bool, bool, int),                                                                                                     \
          NAMES("hangup", "event", "fd"))                                                                                             \
    PROBE(ThreadCount, GROUPS("BasicThreadPool"),                                                                                     \
          TYPES(ui32, TString, ui32, ui32, ui32, ui32),                                                                               \
          NAMES("poolId", "pool", "threacCount", "minThreadCount", "maxThreadCount", "defaultThreadCount"))                           \
    PROBE(HarmonizeCheckPool, GROUPS("Harmonizer"),                                                                                   \
          TYPES(ui32, TString, double, double, double, double, ui32, ui32, bool, bool, bool),                                         \
          NAMES("poolId", "pool", "elapsed", "cpu", "lastSecondElapsed", "lastSecondCpu", "threadCount", "maxThreadCount",    \
                  "isStarved", "isNeedy", "isHoggish"))                                                                               \
    PROBE(HarmonizeCheckPoolByThread, GROUPS("Harmonizer"),                                                                           \
          TYPES(ui32, TString, i16, double, double, double, double),                                                                  \
          NAMES("poolId", "pool", "threadIdx", "elapsed", "cpu", "lastSecondElapsed", "lastSecondCpu"))                       \
    PROBE(WakingUpConsumption, GROUPS("Harmonizer"),                                                                                  \
          TYPES(double, double, double, double, double),                                                                              \
          NAMES("avgWakingUpUs", "realAvgWakingUpUs", "avgAwakeningUs", "realAvgAwakeningUs", "total"))                               \
    PROBE(ChangeSpinThreshold, GROUPS("Harmonizer"),                                                                                  \
          TYPES(ui32, TString, ui64, double, ui64),                                                                                   \
          NAMES("poolId", "pool", "spinThreshold", "spinThresholdUs", "bucketIdx"))                                                   \
    PROBE(WaitingHistogram, GROUPS("Harmonizer"),                                                                                     \
          TYPES(ui32, TString, double, double, ui64),                                                                                 \
          NAMES("poolId", "pool", "fromUs", "toUs", "count"))                                                                         \
    PROBE(HarmonizeOperation, GROUPS("Harmonizer"),                                                                                   \
          TYPES(ui32, TString, TString, ui32, ui32, ui32),                                                                            \
          NAMES("poolId", "pool", "operation", "newCount", "minCount", "maxCount"))                                                   \
    PROBE(TryToHarmonize, GROUPS("Harmonizer"),                                                                                       \
          TYPES(ui32, TString),                                                                                                       \
          NAMES("poolId", "pool"))                                                                                                    \
    PROBE(SavedValues, GROUPS("Harmonizer"),                                                                                          \
          TYPES(ui32, TString, TString, double, double, double, double, double, double, double, double),                              \
          NAMES("poolId", "pool", "valueName", "[0]", "[1]", "[2]", "[3]", "[4]", "[5]", "[6]", "[7]"))                               \
    PROBE(RegisterValue, GROUPS("Harmonizer"),                                                                                        \
          TYPES(ui64, ui64, ui64, ui64, double, double, double),                                                                      \
          NAMES("ts", "lastTs", "dTs", "8sTs", "us", "lastUs", "dUs"))                                                                \
    PROBE(TryToHarmonizeFailed, GROUPS("Harmonizer"),                                                                                 \
          TYPES(ui64, ui64, bool, bool),                                                                                              \
          NAMES("ts", "nextHarmonizeTs", "isDisabled", "withLock"))                                                                   \
    PROBE(TryToHarmonizeSuccess, GROUPS("Harmonizer"),                                                                                \
          TYPES(ui64, ui64, ui64),                                                                                                    \
          NAMES("ts", "nextHarmonizeTs", "previousNextHarmonizeTs"))                                                                  \
    PROBE(SpinCycles, GROUPS("Harmonizer"),                                                                                           \
          TYPES(ui32, TString, ui64, bool),                                                                                           \
          NAMES("poolId", "pool", "spinPauseCount", "IsInterrupted"))                                                                 \
    PROBE(WaitingHistogramPerThread, GROUPS("Harmonizer"),                                                                            \
          TYPES(ui32, TString, ui32, double, double, ui64),                                                                           \
          NAMES("poolId", "pool", "threadIdx", "fromUs", "toUs", "count"))                                                            \
    PROBE(ChangeSpinThresholdPerThread, GROUPS("Harmonizer"),                                                                         \
          TYPES(ui32, TString, ui32, ui64, double, ui64),                                                                             \
          NAMES("poolId", "pool", "threadIdx", "spinThreshold", "spinThresholdUs", "bucketIdx"))                                      \
    /**/

LWTRACE_DECLARE_PROVIDER(ACTORLIB_PROVIDER)

namespace NActors {
    struct TActorSystemSetup;
    TVector<NLWTrace::TDashboard> LWTraceDashboards(TActorSystemSetup* setup);
}
