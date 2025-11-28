#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/string.h>

namespace NKikimrPQ {
    class THeartbeat;
}

namespace NKikimr::NPQ {

struct THeartbeat {
    TRowVersion Version;
    TString Data;

    static THeartbeat Parse(const NKikimrPQ::THeartbeat& proto);
    void Serialize(NKikimrPQ::THeartbeat& proto) const;
};

}
