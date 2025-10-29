#pragma once

#include "helpers.h"

namespace NKikimr {

struct TEvCommonProgress;
struct TEvCommonResult;

}

namespace NKikimr::NSchemeBoard {

#define SBB_LOG_T(stream) SB_LOG_T(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_D(stream) SB_LOG_D(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_I(stream) SB_LOG_I(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_N(stream) SB_LOG_N(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_W(stream) SB_LOG_W(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)
#define SBB_LOG_E(stream) SB_LOG_E(SCHEME_BOARD_BACKUP, "[" << LogPrefix() << "]" << this->SelfId() << " " << stream)

struct TCommonProgress {
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
    TString Error;
    TString Warning;

    TCommonProgress() = default;

    explicit TCommonProgress(const TEvCommonProgress& ev);

    explicit TCommonProgress(const TEvCommonResult& ev);

    bool IsRunning() const {
        return Status == EStatus::Starting || Status == EStatus::Running;
    }

    double GetProgress() const {
        return TotalPaths > 0 ? (100. * ProcessedPaths / TotalPaths) : 0.;
    }

    TString StatusToString() const;

    TString ToJson() const;
};

struct TBackupProgress: TCommonProgress {
    using TCommonProgress::TCommonProgress;
};

struct TRestoreProgress: TCommonProgress {
    using TCommonProgress::TCommonProgress;
};

IActor* CreateSchemeBoardBackuper(const TString& filePath, ui32 inFlightLimit, bool requireMajority, const TActorId& parent);

IActor* CreateSchemeBoardRestorer(const TString& filePath, ui64 schemeShardId, ui64 generation, const TActorId& parent);

}
