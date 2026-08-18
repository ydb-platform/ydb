#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/string.h>

namespace NKikimrPQ {
    class TSchemaChangeInfo;
}

namespace NKikimr::NPQ {

struct TSchemaChangeInfo {
    TRowVersion Version;
    TString Data;

    static TSchemaChangeInfo Parse(const NKikimrPQ::TSchemaChangeInfo& proto);
    void Serialize(NKikimrPQ::TSchemaChangeInfo& proto) const;
};

}
