#pragma once
#include "defs.h"
#include "bin_log.h"
#include "name_table.h"
#include <util/stream/str.h>

namespace NKikimr {
namespace NSchLab {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// SchLog record format description
//
// SchLog is a series of TBinLogItems that logically represent Frames of 2 types: Keyframe and Diffframe.
//
// The following description consists of 4 columns:
// TBinLogItem type, Actual value type, Value name, Value description
//
// Frame format (1+ item):
// Ui64 EFrameType FrameType, either Keyframe or AddJobFrame
// a single frame of the corresponding type follows
//
// Frame Keyframe format (3+ items):
// Ui64 - TimeStamp
// Ui64 - EpochTime
// Double - Uact
// a variable number of CbsKeyframe records follow terminated with a single-item record with max<ui64> that is
//   intended to be parsed as CbsIdx of a nonexistent Cbs.
//
// Record CbsKeyframe fromat (7+ items):
// Ui64 - Idx, index of the Cbs, or terminator max<ui64>
// Ui64 - NameIdx, CbsName index in the NameStringTable
// Ui64 - MaxBudget
// Ui64 - Period
// Ui64 - CurBudget
// Ui64 - Deadline
// Ui64 ECbsState State
// a variable number of JobKeyframe records follow terminated with a single-item record with max<ui64> that is
//   intended to be parsed as
//
// Record JobKeyframe format (5 items):
// Ui64 - Id, id of the job, or terminator max<ui64>
// Ui64 - Cost
// Ui64 EJobState State
// Ui64 EJobKind Kind
// Ui64 - SeqNo
//
// Frame AddCbsFrame format (8+ items)
// Ui64 TimeStamp
// Record CbsKeyframe fromat (7+ items):
//
// Frame AddJobFrame format (3 + JobKeyframe):
// Ui64 - TimeStamp
// Double - Uact
// Ui64 - CbsIdx
// a single JobKeyframe record
//
// Frame SelectJobFrame format (7+):
// Ui64 - TimeStamp
// a variable number of CbsUpdate records follow terminated with a single-item record with max<ui64>
// Ui64 - CbsIdx, value or max<ui64> to skip
// Ui64 ECbsState CbsState, value or max<ui64> to skip
// Ui64 - JobId, value or max<ui64> to skip
// Ui64 EJobState JobState, value or max<ui64> to skip
// Double - Uact, value of -1.0 to skip
//
// Record CbsUpdate format (4 items):
// Ui64 - CbsIdx
// Ui64 ECbsState CbsState
// Ui64 - CbsDeadline, value or max<ui64> to skip
// Ui64 - CbsCurBudget, value or max<ui64> to skip
//
// Frame CompleteJobFrame format (7 items):
// Ui64 - TimeStamp
// Ui64 - AccountCbsIdx
// Ui64 - AccountCurBudget
// double - Uact
// Ui64 - CbsIdx
// Ui64 - JobId
// Ui64 EJobState JobState
//
// Frame RemoveJobFrame fromat (1+ items):
// Ui64 - TimeStamp
// a single CbsUpdate record
//
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////


struct TSchLog {
    TBinLog Bin;

    ui64 FirstKeyframeTimeStamp = 0;
    ui64 LastKeyframeTimeStamp = 0;
    ui64 KeyframeCount = 0;

    bool IsFull = false;

    void Clear();
    static inline bool OutputComa(IOutputStream &out, bool isFirst);
    double DoubleTimeStamp(ui64 timeStamp) const;
    bool OutputCbsKeyframeRecord(IOutputStream &out, const TNameTable &nameTable, bool *inOutIsFirstCbs);
    void OutputKeyframe(IOutputStream &out, const TNameTable &nameTable);
    void OutputAddCbsFrame(IOutputStream &out, const TNameTable &nameTable);
    void OutputAddJobFrame(IOutputStream &out);
    void OutputSelectJobFrame(IOutputStream &out);
    void OutputCompleteJobFrame(IOutputStream &out);
    void OutputRemoveJobFrame(IOutputStream &out);
    void Output(IOutputStream &out, const TNameTable &nameTable);
};

} // NSchLab
} // NKikimr
