#pragma once

#include <util/generic/ptr.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimrSchemeOp {
    class TColumnDescription;
    class TTableDescription;
}

namespace NKikimr::NReplication::NTestHelpers {

struct TTestTableDescription {
    struct TColumn {
        TString Name;
        TString Type;

        void SerializeTo(NKikimrSchemeOp::TColumnDescription& proto) const;
    };

    TString Name;
    TVector<TString> KeyColumns;
    TVector<TColumn> Columns;

    void SerializeTo(NKikimrSchemeOp::TTableDescription& proto) const;
};

THolder<NKikimrSchemeOp::TTableDescription> MakeTableDescription(const TTestTableDescription& desc);

}
