#pragma once
#include "defs.h"
#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NKikimr {
namespace NSchLab {

class TNameTable {
    TVector<TString> Names;

public:
    TNameTable() {
        Names.reserve(1000);
        Names.push_back("");
    }

    ui64 AddName(TString name) {
        ui64 nameId = Names.size();
        Names.push_back(name);
        return nameId;
    }

    TString GetName(ui64 nameId) const {
        if (nameId < Names.size()) {
            return Names[nameId];
        }
        TStringStream str;
        str << "Unknown_NameId_" << nameId;
        return str.Str();
    }
};


} // NSchLab
} // NKikimr
