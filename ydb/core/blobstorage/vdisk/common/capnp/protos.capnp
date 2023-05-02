@0xdbb9ad1f14bf0b36;

using Cxx = import "/capnp/c++.capnp";
$Cxx.namespace("NKikimrCapnProto_");

struct TMessageId {
    sequenceId @0 :UInt64 = 18446744073699546569;
    msgId @1 :UInt64 = 18446744073699546569;
}

struct TTimestamps {
    sentByDSProxyUs @0 :UInt64 = 0;
    receivedByVDiskUs @1 :UInt64 = 0;
    sentByVDiskUs @2 :UInt64 = 0;
    receivedByDSProxyUs @3 :UInt64 = 0;
}

enum EGetHandleClass {
    notSet @0;
    asyncRead @1;
    fastRead @2;
    discover @3;
    lowRead @4;
}

struct TActorId {
    rawX1 @0 :UInt64;
    rawX2 @1 :UInt64;
}

struct TExecTimeStats {
    submitTimestamp @0 :UInt64;
    inSenderQueue @1 :UInt64;
    receivedTimestamp @2 :UInt64;
    total @3 :UInt64;
    inQueue @4 :UInt64;
    execution @5 :UInt64;
    hugeWriteTime @6 :UInt64;
}

enum EStatus {
    notSet @0;
    unknown @1;
    success @2;
    windowUpdate @3;
    processed @4;
    incorrectMsgId @5;
    highWatermarkOverflow @6;
}

struct TWindowFeedback {
    status @0 :EStatus;
    actualWindowSize @1 :UInt64;
    maxWindowSize @2 :UInt64;
    expectedMsgId @3 :TMessageId;
    failedMsgId @4 :TMessageId;
}

enum EVDiskQueueId {
    notSet @0;
    unknown @1;
    putTabletLog @2;
    putAsyncBlob @3;
    putUserData @4;
    getAsyncRead @5;
    getFastRead @6;
    getDiscover @7;
    getLowRead @8;
    begin @9;
    end @10;
}

enum EVDiskInternalQueueId {
    notSet @0;
    intUnknown @1;
    intBegin @2;
    intGetAsync @3;
    intGetFast @4;
    intPutLog @5;
    intPutHugeForeground @6;
    intPutHugeBackground @7;
    intGetDiscover @8;
    intLowRead @9;
    intEnd @10;
}

struct TVDiskCostSettings {
    seekTimeUs @0 :UInt64;
    readSpeedBps @1 :UInt64;
    writeSpeedBps @2 :UInt64;
    readBlockSize @3 :UInt64;
    writeBlockSize @4 :UInt64;
    minREALHugeBlobInBytes @5 :UInt32;
}

struct TMsgQoS {
    deadlineSeconds @0 :UInt32;
    msgId @1 :TMessageId;
    cost @2 :UInt64;
    extQueueId @3 :EVDiskQueueId;
    intQueueId @4 :EVDiskInternalQueueId;
    costSettings @5 :TVDiskCostSettings;
    sendMeCostSettings @6 :Bool;
    window @7 :TWindowFeedback;

    clientID: union {
        proxyNodeId @8 :UInt32;
        replVDiskId @9 :UInt32;
        vDiskLoadId @10 :UInt64;
        vPatchVDiskId @11 :UInt32;
    }

    execTimeStats @12 :TExecTimeStats;
    senderActorId @13 :TActorId;
}

struct TVDiskID {
    groupID @0 :UInt32 = 4294866787;
    groupGeneration @1 :UInt32 = 4294866787;
    ring @2 :UInt32 = 4294866787;
    domain @3 :UInt32 = 4294866787;
    vDisk @4 :UInt32 = 4294866787;
}

struct TLogoBlobID {
    rawX1 @0 :UInt64;
    rawX2 @1 :UInt64;
    rawX3 @2 :UInt64;
}

struct TRangeQuery {
    from @0 :TLogoBlobID;
    to @1 :TLogoBlobID;
    cookie @2 :UInt64;
    maxResults @3 :UInt32;
}

struct TExtremeQuery {
    id @0 :TLogoBlobID;
    shift @1 :UInt64;
    size @2 :UInt64;
    cookie @3 :UInt64;
}

struct TTabletData {
    id @0 :UInt64;
    generation @1 :UInt32;
}

struct TEvVGet {
    rangeQuery @0 :TRangeQuery;
    extremeQueries @1 :List(TExtremeQuery);
    vDiskID @2 :TVDiskID;
    notifyIfNotReady @3 :Bool;
    showInternals @4 :Bool;
    cookie @5 :UInt64;
    msgQoS @6 :TMsgQoS;
    indexOnly @7 :Bool = false;
    handleClass @8 :EGetHandleClass;
    suppressBarrierCheck @9 :Bool = false;
    tabletId @10 :UInt64 = 0;
    acquireBlockedGeneration @11 :Bool = false;
    timestamps @12 :TTimestamps;
    forceBlockedGeneration @13 :UInt32 = 0;
    readerTabletData @14 :TTabletData;
    forceBlockTabletData @15 :TTabletData;
    snapshotId @16 :Data;
}
