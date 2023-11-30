#pragma once

#include "defs.h"

#include <ydb/library/actors/core/log_settings.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NKikimr {

    template <NActors::NLog::EPriority MainPrio, NActors::NLog::EPriority MutePrio>
    struct TLogPriorityMuteChecker {
        using EPriority = NActors::NLog::EPriority;
        static constexpr EPriority MainPriority = MainPrio;
        static constexpr EPriority MutePriority = MutePrio;

        TInstant MuteDeadline;

        void MuteUntil(const TInstant &muteDeadline) {
            MuteDeadline = muteDeadline;
        }

        void Unmute() {
            MuteDeadline = TInstant::Zero();
        }

        bool IsMuted(const TInstant &now) const {
            return now < MuteDeadline;
        }

        EPriority CheckPriority(const TInstant &now) const {
            return IsMuted(now) ? MutePriority : MainPriority;
        }

        EPriority Register(const TInstant &now, const TInstant &muteDeadline) {
            if (IsMuted(now)) {
                return MutePriority;
            }
            MuteUntil(muteDeadline);
            return MainPriority;
        }

        EPriority Register(const TInstant &now, const TDuration &muteDuration) {
            if (IsMuted(now)) {
                return MutePriority;
            }
            MuteUntil(now + muteDuration);
            return MainPriority;
        }
    };

    template <NActors::NLog::EPriority MainPrio, NActors::NLog::EPriority MutePrio>
    struct TAtomicLogPriorityMuteChecker {
        using EPriority = NActors::NLog::EPriority;
        static constexpr EPriority MainPriority = MainPrio;
        static constexpr EPriority MutePriority = MutePrio;

        TAtomic MuteDeadlineUs = 0;

        void MuteUntil(const TInstant &muteDeadline) {
            AtomicSet(MuteDeadlineUs, muteDeadline.MicroSeconds());
        }

        void Unmute() {
            AtomicSet(MuteDeadlineUs, 0);
        }

        bool IsMuted(const TInstant &now) const {
            return now < TInstant::MicroSeconds(AtomicGet(MuteDeadlineUs));
        }

        EPriority CheckPriority(const TInstant &now) const {
            return IsMuted(now) ? MutePriority : MainPriority;
        }

        EPriority Register(const TInstant &now, const TInstant &muteDeadline) {
            if (IsMuted(now)) {
                return MutePriority;
            }
            MuteUntil(muteDeadline);
            return MainPriority;
        }

        EPriority Register(const TInstant &now, const TDuration &muteDuration) {
            if (IsMuted(now)) {
                return MutePriority;
            }
            MuteUntil(now + muteDuration);
            return MainPriority;
        }
    };

}
