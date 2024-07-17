#pragma once

namespace NYql::NDq {

/*

    Each async buffer (source/channel/sink) collect pair of TDqAsyncStats:
    1. push (in data), pause/resume/wait describe starvation when pop tries to get data from empty buffer
    2. pop (out data), pause/resume/wait describe backpressure when push is blocked by back pressure (full buffer)
    Minimum wait duration is needed to filter out short edge cases

    ---> ---> ---> time ---> ---> ---> --->
    0         1        2         3                     Push:
    012345678902345678901234567890123456789            F=00 P=13 R=21 L=35 W=8 (wait on 06-07 is too short and ignored)
    F           P=======R             L
    -----#---------------------######------ (full)
      ####                ###########
      ####    ##          ###########
    ######  ####        ############# ####
    --------------------------------------- (empty)
         F                     P====R    L
    012345678902345678901234567890123456789            Pop:
    0         1        2         3                     F=05 P=28 R=33 L=38 W=5 (wait on 05-05 is too short and ignored)

*/

enum TCollectStatsLevel {
    None,   // collect nothing
    Basic,  // aggregated per graph, collect bytes/rows/chunks/split, do not collect timings
    Full,   // agrregated per stage, collect all, data and timings
    Profile // like Full but don't aggregate, send full info to the client
};

inline bool StatsLevelCollectNone(TCollectStatsLevel level) {
    return level == TCollectStatsLevel::None;
}

inline bool StatsLevelCollectBasic(TCollectStatsLevel level) {
    return level != TCollectStatsLevel::None;
}

inline bool StatsLevelCollectFull(TCollectStatsLevel level) {
    return level == TCollectStatsLevel::Full || level == TCollectStatsLevel::Profile;
}

inline bool StatsLevelCollectProfile(TCollectStatsLevel level) {
    return level == TCollectStatsLevel::Profile;
}

struct TDqAsyncStats {
    TCollectStatsLevel Level = TCollectStatsLevel::None;
    TDuration MinWaitDuration = TDuration::MicroSeconds(100);
    std::optional<TInstant> CurrentPauseTs;
    bool MergeWaitPeriod = false;

    // basic stats
    ui64 Bytes = 0;
    ui64 DecompressedBytes = 0;
    ui64 Rows = 0;
    ui64 Chunks = 0;
    ui64 Splits = 0;

    // full stats 
    TInstant FirstMessageTs;
    TInstant PauseMessageTs;
    TInstant ResumeMessageTs;
    TInstant LastMessageTs;
    TDuration WaitTime;
    ui64 WaitPeriods = 0;

    void MergeData(const TDqAsyncStats& other) {
        Bytes += other.Bytes;
        DecompressedBytes += other.DecompressedBytes;
        Rows += other.Rows;
        Chunks += other.Chunks;
        Splits += other.Splits;
    }

    void MergeTime(const TDqAsyncStats& other) {
        if (other.FirstMessageTs && (!FirstMessageTs || FirstMessageTs > other.FirstMessageTs)) {
            FirstMessageTs = other.FirstMessageTs;
        }
        if (other.PauseMessageTs && (!PauseMessageTs || PauseMessageTs > other.PauseMessageTs)) {
            PauseMessageTs = other.PauseMessageTs;
        }
        if (other.ResumeMessageTs && (!ResumeMessageTs || ResumeMessageTs < other.ResumeMessageTs)) {
            ResumeMessageTs = other.ResumeMessageTs;
        }
        if (other.LastMessageTs && (!LastMessageTs || LastMessageTs < other.LastMessageTs)) {
            LastMessageTs = other.LastMessageTs;
        }
        WaitTime += other.WaitTime;
        WaitPeriods += other.WaitPeriods;
    }

    void Merge(const TDqAsyncStats& other) {
        MergeData(other);
        MergeTime(other);
    }

    inline void TryPause() {
        if (CollectFull()) {
            auto now = TInstant::Now();

            if (!CurrentPauseTs) {
                if (ResumeMessageTs) {
                    auto delta = now - ResumeMessageTs;
                    if (delta >= MinWaitDuration) {
                        CurrentPauseTs = now;
                    } else {
                        CurrentPauseTs = ResumeMessageTs;
                        MergeWaitPeriod = true;
                    }
                } else {
                    CurrentPauseTs = now;
                }
            }

            // we want to report wait time asap (not on next message only)
            // so we decrease wait period to minimal possible value of MinWaitDuration
            // (used to filter out short periods) and report the rest

            auto delta = now - *CurrentPauseTs;
            if (delta >= MinWaitDuration * 2) {
                WaitTime += delta;
                WaitTime -= MinWaitDuration;
                *CurrentPauseTs = now - MinWaitDuration;
            }
        }
    }

    inline void Resume() {
        if (CollectFull()) {
            auto now = TInstant::Now();
            if (!FirstMessageTs) {
                FirstMessageTs = now;
            }
            LastMessageTs = now;
            if (CurrentPauseTs) {
                auto delta = now - *CurrentPauseTs;
                if (delta >= MinWaitDuration) {
                    if (!PauseMessageTs) {
                        PauseMessageTs = *CurrentPauseTs;
                    }
                    ResumeMessageTs = now;
                    WaitTime += delta;
                }
                CurrentPauseTs.reset();
                if (MergeWaitPeriod) {
                    MergeWaitPeriod = false;
                } else {
                    WaitPeriods++;
                }
            }
        }
    }

    inline bool CollectNone() const {
        return StatsLevelCollectNone(Level);
    }

    inline bool CollectBasic() const {
        return StatsLevelCollectBasic(Level);
    }

    inline bool CollectFull() const {
        return StatsLevelCollectFull(Level);
    }

    inline bool CollectProfile() const {
        return StatsLevelCollectProfile(Level);
    }
};

} // namespace NYql::NDq