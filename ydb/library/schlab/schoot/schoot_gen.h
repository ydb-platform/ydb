#pragma once
#include "defs.h"
#include "schoot_gen_cfg.h"
#include <util/generic/vector.h>

namespace NKikimr {
namespace NSchLab {

class TSchOotGen {
    TSchOotGenCfg Cfg;
    ui64 ReqSent;
    ui64 LastReqToSend;
    double LastTime;

public:
    TSchOotGen(const TSchOotGenCfg &cfg);
    void Reset();

    // timeToNext is just a hint, can be 0.0
    // returns false in case of error
    bool Step(double time, ui64 *reqSize, double *timeToNextReq);
    TString GetUser();
    TString GetDesc();
};

class TSchOotGenSet {
    TVector<TSchOotGen> Gen;

public:
    TSchOotGenSet(const TSchOotGenCfgSet &cfgSet);
    void Reset();

    // timeToNext is just a hint, can be 0.0
    // returns false in case of error
    bool Step(double time, ui64 *reqSize, double *timeToNextReq, TString *user, TString *desc);
};

}; // NSchLab
}; // NKikimir
