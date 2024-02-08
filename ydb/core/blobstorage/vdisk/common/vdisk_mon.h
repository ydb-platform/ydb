#pragma once
#include "defs.h"

#include <ydb/core/protos/node_whiteboard.pb.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // DATABASE MONITORING COUNTERS
    ////////////////////////////////////////////////////////////////////////////
    struct TDbMon {

        enum ESubRequestID {
            SkeletonStateId = 1,
            HullInfoId = 2,
            SyncerInfoId = 3,
            SyncLogId = 4,
            ReplId = 5,
            LogCutterId = 6,
            HandoffMonId = 7,
            HugeKeeperId = 8,
            DskSpaceTrackerId = 9,
            LocalRecovInfoId = 10,
            AnubisRunnerId = 11,
            DelayedCompactionDeleterId = 12,
            ScrubId = 13,
            DbMainPageLogoBlobs = 14,
            DbMainPageBlocks = 15,
            DbMainPageBarriers = 16,
            Defrag = 17,
        };

        static const char *SubRequestIDToStr(int val) {
            switch (val) {
                case SkeletonStateId:          return "SkeletonStateId";
                case HullInfoId:               return "HullInfoId";
                case SyncerInfoId:             return "SyncerInfoId";
                case SyncLogId:                return "SyncLogId";
                case ReplId:                   return "ReplId";
                case LogCutterId:              return "LogCutterId";
                case HandoffMonId:             return "HandoffMonId";
                case HugeKeeperId:             return "HugeKeeperId";
                case DskSpaceTrackerId:        return "DskSpaceTrackerId";
                case LocalRecovInfoId:         return "LocalRecovInfoId";
                case DelayedCompactionDeleterId: return "DelayedCompactionDeleterId";
                case ScrubId:                  return "ScrubId";
                case DbMainPageLogoBlobs:      return "DbMainPageLogoBlobs";
                case DbMainPageBlocks:         return "DbMainPageBlocks";
                case DbMainPageBarriers:       return "DbMainPageBarriers";
                case Defrag:                   return "Defrag";
            }
            return "Unknown";
        }

        struct TDbLocalRecovery {
            enum EState {
                Initial,
                YardInit,
                LoadDb,
                LoadBulkFormedSegments,
                ApplyLog,
                Error,
                Done
            };

            static const char *StateToStr(int val) {
                switch (val) {
                    case Initial: return "Initial";
                    case YardInit: return "YardInit";
                    case LoadDb: return "LoadDb";
                    case LoadBulkFormedSegments: return "LoadBulkFormedSegments";
                    case ApplyLog: return "ApplyLog";
                    case Error: return "Error";
                    case Done: return "Done";
                    default: return "Unknown";
                }
            }

            static NKikimrWhiteboard::EFlag StateToLight(int val) {
                switch (val) {
                    case Initial: return NKikimrWhiteboard::EFlag::Yellow;
                    case YardInit: return NKikimrWhiteboard::EFlag::Yellow;
                    case LoadDb: return NKikimrWhiteboard::EFlag::Yellow;
                    case LoadBulkFormedSegments: return NKikimrWhiteboard::EFlag::Yellow;
                    case ApplyLog: return NKikimrWhiteboard::EFlag::Yellow;
                    case Error: return NKikimrWhiteboard::EFlag::Red;
                    case Done: return NKikimrWhiteboard::EFlag::Green;
                    default: return NKikimrWhiteboard::EFlag::Red;
                }
            }
        };
    };

} // NKikimr

