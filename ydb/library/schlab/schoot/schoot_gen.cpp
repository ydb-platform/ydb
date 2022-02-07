#include "schoot_gen.h"
#include <cmath>

#include <util/stream/output.h>

namespace NKikimr {
namespace NSchLab {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSchOotGen

TSchOotGen::TSchOotGen(const TSchOotGenCfg &cfg)
    : Cfg(cfg)
{
    Reset();
}

void TSchOotGen::Reset() {
    ReqSent = 0;
    LastReqToSend = 0;
    LastTime = 0.0;
}

bool TSchOotGen::Step(double time, ui64 *reqSize, double *timeToNextReq) {
    bool isSent = false;
    bool isPending = false;
    double timeToNext = 0.0;
    if (time < LastTime) {
        // Time may not go back! Don't even want to think about it!
        return false;
    }
    LastTime = time;
    if (Cfg.Period <= 0.0) {
        return false;
    }

    double duration = time - Cfg.StartTime;
    if (duration < 0.0) {
        timeToNext = -duration;
    } else {
        double periodIdx = floor(duration / Cfg.Period);
        ui64 iPeriodIdx = ui64(periodIdx);
        double periodPos = Max(0.0, duration - periodIdx * Cfg.Period);
        if (Cfg.ReqInterval < 0.0) {
            return false;
        }
        if (iPeriodIdx < Cfg.PeriodCount || Cfg.PeriodCount == 0) {
            double periodReqIdx = (Cfg.ReqInterval == 0.0 ? double(Cfg.ReqCount) : floor(periodPos / Cfg.ReqInterval));
            ui64 iPeriodReqIdx = ui64(periodReqIdx);
            ui64 periodReqCount = Min(Cfg.ReqCount, iPeriodReqIdx + 1);
            LastReqToSend = Max(LastReqToSend, iPeriodIdx * Cfg.ReqCount + periodReqCount);
            if (ReqSent < LastReqToSend) {
                ReqSent++;
                isSent = true;
                if (ReqSent < LastReqToSend) {
                    isPending = true;
                }
            }

            if (!isPending) {
                ui64 nextPeriodReqCount = periodReqCount + 1;
                if (nextPeriodReqCount <= Cfg.ReqCount) {
                    double nextPeriodReqTime = double(nextPeriodReqCount - 1) * Cfg.ReqInterval;
                    timeToNext = Max(0.0, nextPeriodReqTime - periodPos);
                } else {
                    double timeToNextPeriod = Max(0.0, Cfg.Period - periodPos);
                    timeToNext = timeToNextPeriod;
                }
                if (timeToNext == 0.0) {
                    // There might be a case when no request is sent but time to next request it 0.0.
                    // This will break the cycle if someone waits for exactly the time we specify.
                    if (LastReqToSend == ReqSent) {
                        LastReqToSend++;
                    }
                }
            }
        } else {
            timeToNext = 1000000000.0;
        }
    }

    if (reqSize) {
        *reqSize = (isSent ? Cfg.ReqSizeBytes : 0);
    }
    if (timeToNextReq) {
        *timeToNextReq = timeToNext;
    }
    return true;
}

TString TSchOotGen::GetUser() {
    return Cfg.User;
}

TString TSchOotGen::GetDesc() {
    return Cfg.Desc;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TSchOotGenSet

TSchOotGenSet::TSchOotGenSet(const TSchOotGenCfgSet &cfgSet) {
    if (!cfgSet.IsValid) {
        return;
    }
    ui64 size = cfgSet.Cfg.size();
    Gen.reserve(size);
    for (ui64 i = 0; i < size; ++i) {
        Gen.emplace_back(cfgSet.Cfg[i]);
    }
}

void TSchOotGenSet::Reset() {
    for (ui64 i = 0; i < Gen.size(); ++i) {
        Gen[i].Reset();
    }
}

bool TSchOotGenSet::Step(double time, ui64 *outReqSize, double *outTimeToNextReq, TString *user, TString *desc) {
    ui64 reqSize = 0;
    double timeToNextReq = Max<double>();
    double minTimeToNextReq = Max<double>();
    for (ui64 i = 0; i < Gen.size(); ++i) {
        bool isOk = Gen[i].Step(time, &reqSize, &timeToNextReq);
        if (!isOk) {
            return false;
        }
        if (reqSize) {
            if (outReqSize) {
                *outReqSize = reqSize;
            }
            if (outTimeToNextReq) {
                *outTimeToNextReq = 0.0;
            }
            if (user) {
                *user = Gen[i].GetUser();
            }
            if (desc) {
                *desc = Gen[i].GetDesc();
            }
            return true;
        }
        if (timeToNextReq < minTimeToNextReq) {
            minTimeToNextReq = timeToNextReq;
        }
    }
    if (outReqSize) {
        *outReqSize = 0;
    }
    if (outTimeToNextReq) {
        if (minTimeToNextReq == Max<double>()) {
            *outTimeToNextReq = 0.0;
        } else {
            *outTimeToNextReq = minTimeToNextReq;
        }
    }
    return true;
}

}; // NSchLab
}; // NKikimr

