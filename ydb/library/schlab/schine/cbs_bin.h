#pragma once
#include "defs.h"
#include "cbs.h"
#include <util/generic/string.h>

namespace NKikimr {
namespace NSchLab {

struct TCbsBin {
    enum ELog {
        LogBegin = 0,
            LogTs,
            LogKeyframe,
            LogEpochtime,
            LogCbsIdxBegin,
                LogCbsName,
                LogCbsMaxBudget,
                LogCbsDeadline,
                LogCbsState,
                LogCbsCurBudget,
                LogCbsReqBegin,
                    LogCbsReqId,
                    LogCbsReqCost,
                    LogCbsReqState,
                    LogCbsReqSeqno,
                LogCbsReqEnd,
            LogCbsEnd,
            LogHgrubBegin,
                LogHgrubUact,
            LogHgrubEnd,
        LogEnd,
    };

    struct TKind {
        ui8 FixedSize;
        bool IsVarSize;
        i8 LevelChange;
    };

    static const TKind KindData[256];

    TString Log;
    ui64 LogPos = 0;

    bool AppendDiff(const TCbs &prev, const TCbs &cur);
    bool AppendKeyframe(const TCbs &cur);

    struct TLogReader {
        ui64 Pos = 0;
        const TCbsBin &Log;

        TLogReader(const TCbsBin &log)
            : Log(log)
        {}

        bool IsEof() const;
        ELog Kind() const;
        ui64 FixedSize() const;
        ui64 GetFixedUi64() const;
        const char* GetVarSizeData() const;  // nullptr if there is no varsize data
    };
};

} // NSchLab
} // NKikimr
