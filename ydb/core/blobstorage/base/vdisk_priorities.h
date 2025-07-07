#pragma once

#include "defs.h"

namespace NKikimr {

    //
    // Owner scheduler heirarchy:
    //
    // Log   Fresh   Comp
    //    \    |    /        Sync   Fast   Other
    //     B y t e s   Huge  Log    Read   Read   Load   Low
    //            \     |     |      |      |      |    /
    //             P e r - O w n e r - S c h e d u l e r
    //
    namespace NPriRead {
        constexpr ui8 SyncLog = 0;          // SyncLog: Synclog reads
        constexpr ui8 HullComp = 1;         // Comp: Background (compaction) read
        constexpr ui8 HullOnlineRt = 2;     // FastRead: Real-time online reads
        constexpr ui8 HullOnlineOther = 3;  // OtherRead: Other online reads
        constexpr ui8 HullLoad = 4;         // Load: Load index
        constexpr ui8 HullLow = 5;          // Low: Background User Data reads that 'do not affect other operations'
    } // TPriRead

    namespace NPriWrite {
        constexpr ui8 SyncLog = 6;          // SyncLog: Synclog writes
        constexpr ui8 HullFresh = 7;        // Fresh: Fresh compaction
        constexpr ui8 HullHugeAsyncBlob = 8;// Huge: Huge Async Blob writes
        constexpr ui8 HullHugeUserData = 9; // Huge: Huge User Data writes
        constexpr ui8 HullComp = 10;         // Comp: Ordinary compaction
    } // NPriWrite

    // PDisk internal use only (all goes through fair "Log" queue if necessary)
    namespace NPriInternal {
        constexpr ui8 LogRead = 11;
        constexpr ui8 LogWrite = 12;
        constexpr ui8 Other = 13;
        constexpr ui8 Trim = 14;
    } // NPriInternal

    constexpr ui64 BytesSchedulerWeightDefault = 1;
    constexpr ui64 LogWeightDefault = 1;
    constexpr ui64 FreshWeightDefault = 2;
    constexpr ui64 CompWeightDefault = 7;
    constexpr ui64 SyncLogWeightDefault = 15;
    constexpr ui64 HugeWeightDefault = 2;
    constexpr ui64 FastReadWeightDefault =  1;
    constexpr ui64 OtherReadWeightDefault = 1;
    constexpr ui64 LoadWeightDefault = 2;
    constexpr ui64 LowWeightDefault = 1;

    TString PriToString(ui8 pri);
} // NKikimr
