#pragma once

#include "defs.h"
#include "vdisk_dbtype.h"
#include "vdisk_events.h"
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

namespace NKikimr {

    ////////////////////////////////////////////////////////////////////////////
    // THullCommitFinished
    ////////////////////////////////////////////////////////////////////////////
    struct THullCommitFinished : public TEventLocal<THullCommitFinished, TEvBlobStorage::EvHullCommitFinished> {
        enum EType {
            CommitFresh,
            CommitLevel,
            CommitAdvanceLsn,
            CommitReplSst
        };

        EType Type;

        THullCommitFinished(EType type)
            : Type(type)
        {}

        static const char* TypeToString(EType type) {
            switch (type) {
                case CommitFresh:      return "CommitFresh";
                case CommitLevel:      return "CommitLevel";
                case CommitAdvanceLsn: return "CommitAdvanceLsn";
                case CommitReplSst:    return "CommitReplSst";
                default:               return "<invalid>";
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvDelLogoBlobDataSyncLog/TEvDelLogoBlobDataSyncLogResult
    // Messages used by handoff module to notify SyncLog about local data deletion
    // TODO: subject to change when improving Handoff module
    ////////////////////////////////////////////////////////////////////////////
    struct TEvDelLogoBlobDataSyncLog
        : public TEventLocal<TEvDelLogoBlobDataSyncLog, TEvBlobStorage::EvDelLogoBlobDataSyncLog>
    {
        const TLogoBlobID Id;
        const TIngress Ingress;
        const ui64 OrderId;

        TEvDelLogoBlobDataSyncLog(const TLogoBlobID &id, const TIngress &ingress, ui64 orderId)
            : Id(id)
            , Ingress(ingress)
            , OrderId(orderId)
        {}
    };

    struct TEvDelLogoBlobDataSyncLogResult
        : public TEventLocal<TEvDelLogoBlobDataSyncLogResult, TEvBlobStorage::EvDelLogoBlobDataSyncLogResult>
        , public TEvVResultBase
    {
        const TLogoBlobID Id;
        const ui64 OrderId;

        TEvDelLogoBlobDataSyncLogResult(const TLogoBlobID &id, ui64 orderId, const TInstant &now,
                ::NMonitoring::TDynamicCounters::TCounterPtr counterPtr, NVDiskMon::TLtcHistoPtr histoPtr)
            : TEvVResultBase(now, TInterconnectChannels::IC_BLOBSTORAGE_SMALL_MSG, counterPtr,
                histoPtr)
            , Id(id)
            , OrderId(orderId)
        {}
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvCompactVDisk
    // Local message to compact selected databases of the current VDisk. Send this
    // message to Skeleton and wait for TEvCompactVDiskResult which signals that
    // compaction has been finished.
    ////////////////////////////////////////////////////////////////////////////
    struct TEvCompactVDisk : public TEventLocal<TEvCompactVDisk, TEvBlobStorage::EvCompactVDisk> {
        enum class EMode {
            FULL,
            FRESH_ONLY,
        };

        // mask of databases to compact in terms of EHullDbType type (can compact several databases at once)
        const ui32 Mask;
        const EMode Mode;

        TEvCompactVDisk(ui32 mask, EMode mode = EMode::FULL)
            : Mask(mask)
            , Mode(mode)
        {}

        // create a message for compaction one database of type 'type'
        static TEvCompactVDisk *Create(EHullDbType type, EMode mode = EMode::FULL) {
            return new TEvCompactVDisk(::NKikimr::Mask(type), mode);
        }

        static const char *ModeToString(EMode mode) {
            switch (mode) {
                case EMode::FULL: return "FULL";
                case EMode::FRESH_ONLY: return "FRESH_ONLY";
            }
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvCompactVDiskResult
    ////////////////////////////////////////////////////////////////////////////
    struct TEvCompactVDiskResult : public TEventLocal<TEvCompactVDiskResult, TEvBlobStorage::EvCompactVDiskResult> {
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullCompact
    // A private message for Hull Database to compact this database. When Hull Database
    // receives this message all in-flight messages to recovery log must be committe. Don't
    // use this message directly, use TEvCompactVDisk instead if you want to compact
    // some database in local VDisk
    ////////////////////////////////////////////////////////////////////////////
    struct TEvHullCompact : public TEventLocal<TEvHullCompact, TEvBlobStorage::EvHullCompact> {
        const EHullDbType Type;
        const ui64 RequestId;
        const TEvCompactVDisk::EMode Mode;

        TEvHullCompact(EHullDbType type, ui64 requestId, TEvCompactVDisk::EMode mode)
            : Type(type)
            , RequestId(requestId)
            , Mode(mode)
        {}

        TString ToString() const {
            return TStringBuilder() << "{Type# " << EHullDbTypeToString(Type) << " RequestId# " << RequestId
                << " Mode# " << TEvCompactVDisk::ModeToString(Mode) << "}";
        }
    };

    ////////////////////////////////////////////////////////////////////////////
    // TEvHullCompactResult
    ////////////////////////////////////////////////////////////////////////////
    struct TEvHullCompactResult : public TEventLocal<TEvHullCompactResult, TEvBlobStorage::EvHullCompactResult> {
        EHullDbType Type;
        ui64 RequestId = 0;

        TEvHullCompactResult(EHullDbType type, ui64 requestId)
            : Type(type)
            , RequestId(requestId)
        {}
    };

} // NKikimr

