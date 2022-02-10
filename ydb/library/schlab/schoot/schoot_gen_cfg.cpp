#include "schoot_gen_cfg.h"
#include <ydb/library/schlab/protos/schlab.pb.h>
#include <library/cpp/protobuf/json/json2proto.h>
#include <util/stream/str.h>
#include <google/protobuf/text_format.h>

namespace NKikimr {
namespace NSchLab {

TSchOotGenCfgSet::TSchOotGenCfgSet(const TString &json) {
    IsValid = true;
    NSchlab::TGenCfgSet message;
    try {
        message = NProtobufJson::Json2Proto<NSchlab::TGenCfgSet>(json);
    } catch (yexception e) {
        IsValid = false;
        TStringStream str;
        str << "Error reading json schoot gen config, Marker# SGC01. ";
        str << e.what();
        ErrorDetails = str.Str();
        return;
    }

    ui64 size = message.cfgSize();
    for (ui64 idx = 0; idx < size; ++idx) {
        const auto &s = message.Getcfg(idx);
        Cfg.emplace_back();
        auto &d = Cfg.back();
        d.Label = (s.Getlabel() == nullptr ? "" : s.Getlabel());
        d.StartTime = s.GetstartTime();
        d.Period = s.Getperiod();
        d.PeriodCount = s.GetperiodCount();
        d.ReqSizeBytes = s.GetreqSizeBytes();
        d.ReqCount = s.GetreqCount();
        d.ReqInterval = s.GetreqInterval();
        d.User = (s.Getuser() == nullptr ? "" : s.Getuser());
        d.Desc = (s.Getdesc() == nullptr ? "" : s.Getdesc());

        if (d.Period <= 0.0) {
            IsValid = false;
            TStringStream str;
            str << "Error in json schoot gen config, Marker# SGC02.";
            str << " period# " << d.Period << " is <= 0.0 at label# \"" << d.Label << "\".";
            ErrorDetails = str.Str();
            return;
        }
        if (d.ReqInterval < 0.0) {
            IsValid = false;
            TStringStream str;
            str << "Error in json schoot gen config, Marker# SGC03.";
            str << " reqInterval# " << d.ReqInterval << " is < 0.0 at label# \"" << d.Label << "\".";
            ErrorDetails = str.Str();
            return;
        }
    }
}

}; // NSchLab
}; // NKikimr

