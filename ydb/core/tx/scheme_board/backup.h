#pragma once

#include "helpers.h"
#include "mon_events.h"

namespace NKikimr::NSchemeBoard {

#define SBB_LOG_T(stream) SB_LOG_T(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_D(stream) SB_LOG_D(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_I(stream) SB_LOG_I(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_N(stream) SB_LOG_N(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_W(stream) SB_LOG_W(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_E(stream) SB_LOG_E(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)

struct TBackupProgress {
    enum class EStatus {
        Idle,
        Starting,
        Running,
        Completed,
        Error,
    };

    ui32 TotalPaths = 0;
    ui32 CompletedPaths = 0;
    EStatus Status = EStatus::Idle;
    TString ErrorMessage;

    TBackupProgress() = default;

    explicit TBackupProgress(const TSchemeBoardMonEvents::TEvBackupProgress& ev)
        : TotalPaths(ev.TotalPaths)
        , CompletedPaths(ev.CompletedPaths)
        , Status(EStatus::Running)
    {
    }

    explicit TBackupProgress(const TSchemeBoardMonEvents::TEvBackupResult& ev)
        : TotalPaths(0)
        , CompletedPaths(0)
        , Status(ev.Error ? EStatus::Error : EStatus::Completed)
        , ErrorMessage(ev.Error.GetOrElse(""))
    {
    }

    bool IsRunning() const {
        return Status == EStatus::Starting || Status == EStatus::Running;
    }

    double GetProgress() const {
        return TotalPaths > 0 ? (100. * CompletedPaths / TotalPaths) : 0.;
    }

    TString StatusToString() const;

    TString ToJson() const;
};

struct TRestoreProgress {
    enum class EStatus {
        Idle,
        Starting,
        Running,
        Completed,
        Error,
    };

    ui32 TotalPaths = 0;
    ui32 ProcessedPaths = 0;
    EStatus Status = EStatus::Idle;
    TString ErrorMessage;

    TRestoreProgress() = default;

    explicit TRestoreProgress(const TSchemeBoardMonEvents::TEvRestoreProgress& ev)
        : TotalPaths(ev.TotalPaths)
        , ProcessedPaths(ev.ProcessedPaths)
        , Status(EStatus::Running)
    {
    }

    explicit TRestoreProgress(const TSchemeBoardMonEvents::TEvRestoreResult& ev)
        : TotalPaths(0)
        , ProcessedPaths(0)
        , Status(ev.Error ? EStatus::Error : EStatus::Completed)
        , ErrorMessage(ev.Error.GetOrElse(""))
    {
    }

    bool IsRunning() const {
        return Status == EStatus::Starting || Status == EStatus::Running;
    }

    double GetProgress() const {
        return TotalPaths > 0 ? (100. * ProcessedPaths / TotalPaths) : 0.;
    }

    TString StatusToString() const;

    TString ToJson() const;
};

IActor* CreateSchemeBoardBackuper(const TString& filePath, ui32 inFlightLimit, const TActorId& parent);

IActor* CreateSchemeBoardRestorer(const TString& filePath, ui64 schemeShardId, ui64 generation, const TActorId& parent);

}
