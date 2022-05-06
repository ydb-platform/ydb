#pragma once

#include "defs.h"
#include "tabletid.h"
#include <ydb/core/protos/tablet.pb.h>

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
/// The TTabletTypes class
////////////////////////////////////////////
struct TTabletTypes : NKikimrTabletBase::TTabletTypes {
public:
    static const char* TypeToStr(EType t) {
        return EType_Name(t).c_str();
    }

    static EType StrToType(const TString& t) {
        EType type;
        if (EType_Parse(t, &type)) {
            return type;
        } else {
            return TypeInvalid;
        }
    }
};

} // end of NKikimr

Y_DECLARE_OUT_SPEC(inline, NKikimrTabletBase::TTabletTypes::EType, os, type) {
    os << NKikimrTabletBase::TTabletTypes::EType_Name(type);
}
