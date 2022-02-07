#pragma once

#include "defs.h"
#include <util/string/builder.h>

////////////////////////////////////////////
namespace NKikimr {

////////////////////////////////////////////
class TTagDetails {
public:
    //
    TTagDetails(const TStringBuf& name, ui32 idx, ui32 valueType, ui32 payloadType)
        : Name(name)
        , Idx(idx)
        , ValueType(valueType)
        , PayloadType(payloadType)
    {
    }

    ~TTagDetails() {}

    TString Name;
    ui32 Idx;
    ui32 ValueType;
    ui32 PayloadType;

    bool IsLeaf() const {
        return !!PayloadType;
    }

    bool IsSingular() const {
        return !ValueType;
    }

    inline bool operator==(const TTagDetails& other) const {
        return Idx == other.Idx && ValueType == other.ValueType && PayloadType == other.PayloadType
            && (Name.empty() || other.Name.empty() || Name == other.Name);
    }

    inline bool operator!=(const TTagDetails& other) const {
        return !operator==(other);
    }
};

////////////////////////////////////////////
class TTagDetailsExtended {
public:
    //
    static const ui32 DEFAULT_LOCALITY_GROUP = 0;

    //
    explicit TTagDetailsExtended(ui32 localityGroupId = DEFAULT_LOCALITY_GROUP)
        : LocalityGroupId(localityGroupId)
    {}
    ~TTagDetailsExtended() {}

    //
    ui32 LocalityGroupId;
};

} // end of the NKikimr namespace

