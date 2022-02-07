#include "schlog.h"

#include "schlog_frame.h"
#include "cbs_state.h"
#include "job_state.h"
#include "job_kind.h"

namespace NKikimr {
namespace NSchLab {

void TSchLog::Clear() {
    Bin.Clear();
    LastKeyframeTimeStamp = 0;
    KeyframeCount = 0;
    IsFull = false;
}

inline bool TSchLog::OutputComa(IOutputStream &out, bool isFirst) {
    if (!isFirst) {
        out << ",";
    }
    return false;
}

double TSchLog::DoubleTimeStamp(ui64 timeStamp) const {
    double dts = static_cast<double>(timeStamp - FirstKeyframeTimeStamp);
    return dts;
}

bool TSchLog::OutputCbsKeyframeRecord(IOutputStream &out, const TNameTable &nameTable, bool *inOutIsFirstCbs) {
    ui64 idx = Bin.ReadUi64(); // 1
    if (idx == Max<ui64>()) {
        return false;
    }
    // Record CbsKeyframe fromat (7+ items):
    if (Bin.ReadableItemCount() >= 6) {
        ui64 nameIdx = Bin.ReadUi64(); // 2...
        ui64 maxBudget = Bin.ReadUi64();
        ui64 period = Bin.ReadUi64();
        ui64 curBudget = Bin.ReadUi64();
        ui64 deadline = Bin.ReadUi64();
        ui64 state = Bin.ReadUi64();


        *inOutIsFirstCbs = OutputComa(out, *inOutIsFirstCbs);
        out << "{";
        out << "\"idx\":" << idx;
        out << ",\"name\":\"" << nameTable.GetName(nameIdx) << "\"";
        out << ",\"max_budget\":" << maxBudget;
        out << ",\"period\":" << period;
        out << ",\"cur_budget\":" << curBudget;
        out << ",\"deadline\":" << DoubleTimeStamp(deadline);
        out << ",\"state\":\"" << CbsStateName((ECbsState)state) << "\"";

        out << ",\"req\":[";
        bool isFirstReq = true;
        while (true) {
            if (Bin.ReadableItemCount() >= 1) {
                ui64 id = Bin.ReadUi64(); // 1
                if (id == Max<ui64>()) {
                    break;
                }
                // Record JobKeyframe format (5 items):
                if (Bin.ReadableItemCount() >= 4) {
                    ui64 cost = Bin.ReadUi64();
                    ui64 state = Bin.ReadUi64();
                    ui64 kind = Bin.ReadUi64();
                    ui64 seqNo = Bin.ReadUi64();

                    isFirstReq = OutputComa(out, isFirstReq);
                    out << "{";
                    out << "\"id\":" << id;
                    out << ",\"cost\":" << cost;
                    out << ",\"state\":\"" << JobStateName((EJobState)state) << "\"";;
                    out << ",\"kind\":\"" << JobKindName((EJobKind)kind) << "\"";;
                    out << ",\"seqno\":" << seqNo;
                    out << "}";
                }
            }
        }
        out << "]"; // req
        out << "}";
    }
    return true;
}

void TSchLog::OutputKeyframe(IOutputStream &out, const TNameTable &nameTable) {
    // Frame Keyframe format (3+ items):
    if (Bin.ReadableItemCount() >= 3) {
        ui64 timeStamp = Bin.ReadUi64();
        ui64 epochTime = Bin.ReadUi64();
        double uact = Bin.ReadDouble();
        out << "{";
        out << "\"ts\":" << DoubleTimeStamp(timeStamp);
        out << ",\"keyframe\":true";
        out << ",\"epochtime\":" << epochTime;
        out << ",\"hgrub\":{\"uact\":" << uact << "}";
        out << ",\"cbs\":[";
        bool isFirstCbs = true;
        while (true) {
            if (Bin.ReadableItemCount() >= 1) {
                bool isOk = OutputCbsKeyframeRecord(out, nameTable, &isFirstCbs);
                if (!isOk) {
                    break;
                }
            } else {
                break;
            }
        }
        out << "]"; // cbs
        out << "}";
    }
}

void TSchLog::OutputAddCbsFrame(IOutputStream &out, const TNameTable &nameTable) {
    // Frame AddCbsFrame format (8+ items)
    if (Bin.ReadableItemCount() >= 8) {
        ui64 timeStamp = Bin.ReadUi64(); // 1
        out << "{";
        out << "\"ts\":" << DoubleTimeStamp(timeStamp);
        out << ",\"cbs\":[";
        bool isFirstCbs = true;
        if (Bin.ReadableItemCount() >= 1) {
            OutputCbsKeyframeRecord(out, nameTable, &isFirstCbs);
        }
        out << "]"; // cbs
        out << "}";
    }
}

void TSchLog::OutputAddJobFrame(IOutputStream &out) {
    // Frame AddJobFrame format (3 + JobKeyframe):
    // Record JobKeyframe format (5 items):
    if (Bin.ReadableItemCount() >= 8) {
        ui64 timeStamp = Bin.ReadUi64(); // 1
        double uact = Bin.ReadDouble();
        ui64 cbsIdx = Bin.ReadUi64();
        ui64 id = Bin.ReadUi64(); // 1
        ui64 cost = Bin.ReadUi64();
        ui64 state = Bin.ReadUi64();
        ui64 kind = Bin.ReadUi64();
        ui64 seqNo = Bin.ReadUi64();
        out << "{";
        out << "\"ts\":" << DoubleTimeStamp(timeStamp);
        out << ",\"hgrub\":{\"uact\":" << uact << "}";
        out << ",\"cbs\":[";
        out << "{";
        out << "\"idx\":" << cbsIdx;
        out << ",\"req\":[";
        out << "{";
        out << "\"id\":" << id;
        out << ",\"cost\":" << cost;
        out << ",\"state\":\"" << JobStateName((EJobState)state) << "\"";;
        out << ",\"kind\":\"" << JobKindName((EJobKind)kind) << "\"";;
        out << ",\"seqno\":" << seqNo;
        out << "}";
        out << "]"; // req
        out << "}";
        out << "]"; // cbs
        out << "}";
    }
}

void TSchLog::OutputSelectJobFrame(IOutputStream &out) {
    // Frame SelectJobFrame format (7+):
    if (Bin.ReadableItemCount() >= 7) {
        ui64 timeStamp = Bin.ReadUi64(); // 1
        out << "{";
        out << "\"ts\":" << DoubleTimeStamp(timeStamp);
        out << ",\"cbs\":[";
        bool isFirstCbs = true;
        while (true) {
            // a variable number of CbsUpdate records follow terminated with a single-item record with max<ui64>
            if (Bin.ReadableItemCount() >= 1) {
                // Record CbsUpdate format (4 items)
                ui64 cbsIdx = Bin.ReadUi64(); // 1
                if (cbsIdx == Max<ui64>()) {
                    break;
                }
                ui64 cbsState = Bin.ReadUi64(); // 2
                ui64 cbsDeadline = Bin.ReadUi64();
                ui64 cbsCurBudget = Bin.ReadUi64();

                isFirstCbs = OutputComa(out, isFirstCbs);
                out << "{";
                out << "\"idx\":" << cbsIdx;
                if (cbsCurBudget != Max<ui64>()) {
                    out << ",\"cur_budget\":" << cbsCurBudget;
                }
                if (cbsDeadline != Max<ui64>()) {
                    out << ",\"deadline\":" << DoubleTimeStamp(cbsDeadline);
                }
                if (cbsState != Max<ui64>()) {
                    out << ",\"state\":\"" << CbsStateName((ECbsState)cbsState) << "\"";
                }
                out << "}";
            }
        }
        double uact = -1.0;
        if (Bin.ReadableItemCount() >= 5) {
            ui64 cbsIdx = Bin.ReadUi64();
            ui64 cbsState = Bin.ReadUi64();
            ui64 jobId = Bin.ReadUi64();
            ui64 jobState = Bin.ReadUi64();
            uact = Bin.ReadDouble(); // value of -1.0 to skip
            if (cbsIdx != Max<ui64>()) {
                isFirstCbs = OutputComa(out, isFirstCbs);
                out << "{";
                out << "\"idx\":" << cbsIdx;
                if (cbsState != Max<ui64>()) {
                    out << ",\"state\":\"" << CbsStateName((ECbsState)cbsState) << "\"";
                }
                if (jobId != Max<ui64>() && jobState != Max<ui64>()) {
                    out << ",\"req\":[";
                    out << "{";
                    out << "\"id\":" << jobId;
                    out << ",\"state\":\"" << JobStateName((EJobState)jobState) << "\"";;
                    out << "}";
                    out << "]"; // req
                }
                out << "}";
            }
        }
        out << "]"; // cbs
        if (uact != -1.0) {
            out << ",\"hgrub\":{\"uact\":" << uact << "}";
        }
        out << "}";
    }
}

void TSchLog::OutputCompleteJobFrame(IOutputStream &out) {
    // Frame CompleteJobFrame format (7 items):
    if (Bin.ReadableItemCount() >= 7) {
        ui64 timeStamp = Bin.ReadUi64();
        ui64 accountCbsIdx = Bin.ReadUi64();
        ui64 accountCurBudget = Bin.ReadUi64();
        double uact = Bin.ReadDouble();
        ui64 cbsIdx = Bin.ReadUi64();
        ui64 jobId = Bin.ReadUi64();
        ui64 jobState = Bin.ReadUi64();
        out << "{";
        out << "\"ts\":" << DoubleTimeStamp(timeStamp);
        out << ",\"hgrub\":{\"uact\":" << uact << "}";
        out << ",\"cbs\":[";
        bool isFirstCbs = true;
        if (accountCbsIdx != cbsIdx) {
            isFirstCbs = OutputComa(out, isFirstCbs);
            out << "{";
            out << "\"idx\":" << accountCbsIdx;
            out << ",\"cur_budget\":" << accountCurBudget;
            out << "}";
        }
        isFirstCbs = OutputComa(out, isFirstCbs);
        out << "{";
        out << "\"idx\":" << cbsIdx;
        if (accountCbsIdx == cbsIdx) {
            out << ",\"cur_budget\":" << accountCurBudget;
        }
        out << ",\"req\":[";
        out << "{";
        out << "\"id\":" << jobId;
        out << ",\"state\":\"" << JobStateName((EJobState)jobState) << "\"";;
        out << "}";
        out << "]"; // req
        out << "}";
        out << "]"; // cbs
        out << "}";
    }
}

void TSchLog::OutputRemoveJobFrame(IOutputStream &out) {
    // Frame RemoveJobFrame fromat (1+ items):
    // Record CbsUpdate format (4 items):
    if (Bin.ReadableItemCount() >= 5) {
        ui64 timeStamp = Bin.ReadUi64();
        ui64 cbsIdx = Bin.ReadUi64();
        ui64 cbsState = Bin.ReadUi64();
        ui64 cbsDeadline = Bin.ReadUi64(); // value or max<ui64> to skip
        ui64 cbsCurBudget = Bin.ReadUi64(); // value or max<ui64> to skip
        out << "{";
        out << "\"ts\":" << DoubleTimeStamp(timeStamp);
        out << ",\"cbs\":[";
        out << "{";
        out << "\"idx\":" << cbsIdx;
        if (cbsCurBudget != Max<ui64>()) {
            out << ",\"cur_budget\":" << cbsCurBudget;
        }
        if (cbsDeadline != Max<ui64>()) {
            out << ",\"deadline\":" << DoubleTimeStamp(cbsDeadline);
        }
        if (cbsState != Max<ui64>()) {
            out << ",\"state\":\"" << CbsStateName((ECbsState)cbsState) << "\"";
        }
        out << "}";
        out << "]"; // cbs
        out << "}";
    }
}

void TSchLog::Output(IOutputStream &out, const TNameTable &nameTable) {
    out << "[";
    bool isFirstFrame = true;
    if (KeyframeCount) {
        Bin.ResetReadPosition();
        while (true) {
            // Frame format (1+ item):
            if (Bin.ReadableItemCount() >= 1) {
                EFrameType frameType = (EFrameType)Bin.ReadUi64();
                isFirstFrame = OutputComa(out, isFirstFrame);
                switch (frameType) {
                    case FrameTypeKey:
                        OutputKeyframe(out, nameTable);
                        break;
                    case FrameTypeAddCbs:
                        OutputAddCbsFrame(out, nameTable);
                        break;
                    case FrameTypeAddJob:
                        OutputAddJobFrame(out);
                        break;
                    case FrameTypeSelectJob:
                        OutputSelectJobFrame(out);
                        break;
                    case FrameTypeCompleteJob:
                        OutputCompleteJobFrame(out);
                        break;
                    case FrameTypeRemoveJob:
                        OutputRemoveJobFrame(out);
                        break;
                    default:
                        out << "!!!Unexpected frameType# " << (ui64)frameType << "!!!";
                        break;
                }
            } else {
                break;
            }
        }
    }
    out << "]";
}

} // NSchLab
} // NKikimr
