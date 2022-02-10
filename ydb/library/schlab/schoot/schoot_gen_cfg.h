#pragma once
#include "defs.h"
#include <util/generic/deque.h>
#include <util/generic/string.h>

namespace NKikimr {
namespace NSchLab {

struct TSchOotGenCfg {
    TString Label;
    double StartTime;
    double Period;
    ui64 PeriodCount;
    ui64 ReqSizeBytes;
    ui64 ReqCount;
    double ReqInterval;
    TString User;
    TString Desc;
};

struct TSchOotGenCfgSet {
    TDeque<TSchOotGenCfg> Cfg;

    bool IsValid;
    TString ErrorDetails;

    TSchOotGenCfgSet(const TString &json);
};


}; // NSchLab
}; // NKikimr
