#pragma once

#include <util/stream/output.h>

class IOutputStream;

namespace NKikimr {
namespace NPDisk {

// Each request has a ReqId that allows its identification in LWTrace and log. Some special requests are created from
// special places and have a special ReqId from the list. Most requests don't have special ids and use a unique number.
// PDisks start counting ReqIds from different values to make it even harder to mix requests from different disks on
// one node.
//
// TODO(cthulhu): We should use few bits for source id and PDiskId%256 to better identify requests.

struct TReqId {
    enum EReqSource {
        Invalid = 0,
        FormatFillSysLog = 1,
        KillOwnerSysLog = 2,
        AfterInitCommonLoggerSysLog = 3,
        PreallocateChunksForNonceJump = 4,
        NonceChangeForNonceJump = 5,
        WriteApplyFormatRecordWrite = 6,
        WriteApplyFormatRecordFlush = 7,
        CreateTSectorWriterSeek = 8,
        InitialTSectorWriterReqId = 9,
        LogNonceJumpWriteHeader2 = 10,
        LogNonceJumpFlush = 11,
        LogNonceJumpTerminateLog = 12,
        CreateTSectorWriterWithBuffer = 13,
        EstimatorSeekCompl = 14,
        EstimatorSeekTimeNs = 15,
        EstimatorSpeed1 = 16,
        EstimatorSpeed2 = 17,
        EstimatorTrimSpeed1 = 18,
        EstimatorTrimSpeed2 = 19,
        EstimatorTrimSpeed3 = 20,
        EstimatorDurationRead = 21,
        EstimatorDurationWrite = 22,
        InnerConfigureScheduler = 23,
        RestoreFormatOnRead = 24,
        ReadAndParseMainLog = 25,
        InitialLogChunkAllocation = 26,
        InitCommonLoggerSwitchToNewChunk = 27,
        WriteSector = 28,
        InitialFormatRead = 29,
        ReadFormatInfo = 30,
        ReadSysLogData = 31,
        ReadSysLog = 32,
        Test0 = 33,
        Test1 = 34,
        Test2 = 35,
        Test3 = 36,
        Test4 = 37,
        YardInit = 38,
        CheckSpace = 39,
        LogWrite = 40,
        LogRead = 41,
        LogReadContinue = 42,
        LogReadResultProcess = 43,
        LogSectorRestore = 44,
        LogCommitDone = 45,
        ChunkRead = 46,
        ChunkWrite = 47,
        ChunkTrim = 48,
        Harakiri = 49,
        Slay = 50,
        ChunkLock = 51,
        ChunkUnlock = 52,
        ChunkReserve = 53,
        ConfigureScheduler = 54,
        Undelivered = 55,
        WhiteboardReport = 56,
        HttpInfo = 57,
        YardControl = 58,
        AskForCutLog = 59,
        CommitLogChunks = 60,
        FormatTrim = 61,
        TrimAllUntrimmedChunks = 62,
        TryTrimChunk = 63,
        ReleaseChunks = 64,
        StopDevice = 65,
        ChunkForget = 66,
        ReadMetadata = 67,
        InitialReadMetadataResult = 68,
        WriteMetadata = 69,
        WriteMetadataResult = 70,
        PushUnformattedMetadataSector = 71,
    };

    // 56 bit idx, 8 bit source

    ui64 Id;

    explicit TReqId(ui64 idx)
        : Id(ui64(EReqSource::Invalid) | (idx << 8))
    {}

    explicit TReqId(EReqSource source, ui64 idx)
        : Id(ui64(source) | (idx << 8))
    {}

    TReqId()
        : Id(Invalid)
    {}

    EReqSource GetSource() const {
        return (EReqSource)(Id & 0xff);
    }

    ui64 GetIdx() const {
        return (Id >> 8);
    }
};

enum class ERequestType {
    RequestLogRead,
    RequestLogReadContinue,
    RequestLogReadResultProcess,
    RequestLogSectorRestore,
    RequestLogWrite,
    RequestChunkRead,
    RequestChunkReadPiece,
    RequestChunkWrite,
    RequestChunkWritePiece,
    RequestChunkTrim,
    RequestYardInit,
    RequestCheckSpace,
    RequestHarakiri,
    RequestYardSlay,
    RequestChunkReserve,
    RequestChunkLock,
    RequestChunkUnlock,
    RequestYardControl,
    RequestAskForCutLog,
    RequestConfigureScheduler,
    RequestWhiteboartReport,
    RequestHttpInfo,
    RequestUndelivered,
    RequestNop,
    RequestCommitLogChunks,
    RequestLogCommitDone,
    RequestTryTrimChunk,
    RequestReleaseChunks,
    RequestStopDevice,
    RequestChunkForget,
    RequestReadMetadata,
    RequestInitialReadMetadataResult,
    RequestWriteMetadata,
    RequestWriteMetadataResult,
    RequestPushUnformattedMetadataSector,
};

inline IOutputStream& operator <<(IOutputStream& out, const TReqId& reqId) {
    return out << reqId.Id;
}


} // NPDisk
} // NKikimr
