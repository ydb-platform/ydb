#pragma once
#include "defs.h"
#include <ydb/library/pdisk_io/buffers.h>
#include "blobstorage_pdisk_crypto.h"

namespace NKikimr {
namespace NPDisk {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SysLog is located in a 'virtual' chunk 0. Its size is enough to fit ~16 SysLog records and may exceed 'physical'
// chunk size.
//
// +-------+-------+-------+-------+---
// |chunk 0|chunk 1|chunk 2|chunk 3|
// +-------+-------+-------+-------+---
// |virtual chunk 0        |chunk 3|
// +-----------------------+-------+---
//
// Virtual chunk 0 contains Format Record and cyclic SysLog.
// Format record consists of 3 32k replicas.
// SysLog consists of N SectorSets. Each SectorSet (or just Set) consists of 3 replicas of a sector (r0, r1 and r2).
// N is chosen so that ~16 full SysLog Records fit in the SysLog. SysLog Record may span multiple SectorSets.
//
// The image below is the artists depiction of the virtual chunk 0 (drawn for sector size of 8k).
//
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--
// |00|01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|    SectorIdx (0 based in virtual chunk 0)
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--
// | Format    | Format    | Format    | Set 0  | Set 1  | Set 2  | Set 3  |    SetIdx (0 based in SysLog)
// | Replica 1 | Replica 2 | Repclia 3 |--+--+--|--+--+--|--+--+--|--+--+--|--
// |           |           |           |r0|r1|r2|r0|r1|r2|r0|r1|r2|r0|r1|r2|
// +-----------+-----------+-----------+--+--+--+--+--+--+--+--+--+--+--+--+--
// ^                                   ^                                             ^
// | Format Record, 3 replicas         | SysLog, N SectorSets, 3 sectors in a set    |
//
// SysLog Record may span across 1 or more SectorSets. First SectorSet of the Record is marked with FirstFlag, last
// SectorSet of the Record is marked with LastFlag. Due to the cyclic nature of SysLog it's parsed not from Set 0 but
// from the first set containing FirstFlag, it's SectorSetIdx is called LoopOffset.
//
// The image below is the artists depiction of the SysLog SectorSets with 16 SysLog Records spanning 2 SectorSets each.
// For artistic purposes only sector set 01 is damaged and cannot be read.
//
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+
// |00|01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31| SectorSetIdx
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ F - FirstFlag,
// |L |X |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F |L |F | L - LastFlag
// +--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+ X - Damaged
//          ^
//          | LoopOffset = 3
//          |---------> Log record consistency is checked starting at LoopOffset and finishing after a full cycle
//            ^
//  --------->| This is intentional, since we don't have any special rules for the first and the last SectorSets
//              in the loop and decide on the Record Consistency when we see SectorSet with another Record's FirstFlag.
//
// During normal operation we expect random shutdowns and incomplete SysLog Records may appear. We also expect SysLog
// Record size increase in the future so we may get some variation of the following: A Record Z starts, maybe spans some
// SectorSets and then terminates abruptly. Underlying SysLog Recrod's (A) First Set may be lost and it's remains may
// span few more SectorSets, then it may have it's Last Set and then intact SysLog Recrods B, C, etc. continue.
// When write reordering occurs we get some interleaving SectorSets with data from different attempts at writing a
// SysLog Record, say O, P
//
// The picture of what we get may look like this:
// --+--+--+--+--+--+--+--+--
//   |ZF|Z |Z |A |A |AL|BF|     F - FirstFlag, L - LastFlag
// --+--+--+--+--+--+--+--+--
// Or like this:
// --+--+--+--+--+--+--+--+--
//   |ZF|Z |Z |P |A |OL|BF|
// --+--+--+--+--+--+--+--+--
// And some incompletely written SectorSets may be damaged and unreadable:
// --+--+--+--+--+--+--+--+--+--
//   |ZF|XX|XX|AF|XX|P |XX|BF|    XX - Damaged and unreadable
// --+--+--+--+--+--+--+--+--+--
//   |        |  |        |
//   |        |<-+--A---->|
//   |           |
//   |<--O,P,Z-->|
//
// We call incomplete and damaged SectorSets Junk.
// It is safe enough to suppose that no more than (2 * (Sets per Record) - 1) SectorSets may be Junk.
// The Junk region should start right after the last complete Record. So we find the record and set the
// JunkBeginOffset (SectorSetIdx) to the following SectorSet. Then we cycle through and find the JunkSetCount.
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TPDisk;

class TSysLogReader : public TThrRefBase {
    TPDisk *const PDisk;
    TActorSystem *const ActorSystem;
    const TActorId ReplyTo;
    const TReqId ReqId;

    THolder<TEvReadLogResult> Result;

    TPDiskStreamCypher Cypher;
    TVector<ui64> BadOffsets;

    ui32 SizeToRead;
    TAlignedData Data;

    struct TSectorSetInfo {
        ui8 *LastGoodSectorData = nullptr;
        ui8 *Payload = nullptr;
        ui64 Nonce = 0;
        ui64 FullPayloadSize = 0;
        ui64 PayloadPartSize = 0;
        ui64 PayloadLsn = 0;
        ui32 FirstSectorIdx = 0;
        ui32 GoodSectorFlags = 0;
        TLogSignature PayloadSignature = 0;
        bool IsIdeal = false;
        bool HasStart = false;
        bool HasMiddle = false;
        bool HasEnd = false;

        bool IsConsistent = true;
        bool IsNonceReversal = false;
    };

    TVector<TSectorSetInfo> SectorSetInfo;

    ui64 BestNonce = 0; // 0 => no best record found
    ui64 MaxNonce = 0;
    ui32 BeginSectorIdx = 0;
    ui32 EndSectorIdx = 0;
    ui32 LoopOffset = 0;
    ui32 BestRecordFirstOffset = 0;
    ui32 BestRecordLastOffset = 0;
    ui32 JunkBeginOffset = 0;
    ui32 JunkSetCount = 0;
    ui32 CriticalJunkCount = 0;

    bool IsReplied = false;

public:
    TSysLogReader(TPDisk *pDisk, TActorSystem *const actorSystem, const TActorId &replyTo, TReqId reqId);
    virtual ~TSysLogReader();

    void Start();
    void Exec();

protected:
    void RestoreSectorSets();
    void FindLoopOffset();
    void MarkConsistentRecords();
    void MarkInconsistent(ui32 beginSetIdx, ui32 endSetIdx);
    void MarkInconsistentIncluding(ui32 beginSetIdx, ui32 endSetIdx);
    void FindTheBestRecord();
    void MeasureJunkRegion();
    void FindMaxNonce();
    void PrepareResult();
    void Reply();

    bool VerboseCheck(bool condition, const char *desctiption);
    void DumpDebugInfo(TStringStream &str, bool isSingleLine);
};

} // NPDisk
} // NKikimr

