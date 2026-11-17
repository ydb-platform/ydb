#pragma once

#include <ydb/core/base/row_version.h>

namespace NKikimr::NReplication {

// End remains the map key. Begin is needed when only the suffix of a checksum
// boundary is cached on a service or writer.
struct TTxIdRange {
    ui64 TxId = 0;
    TRowVersion Begin = TRowVersion::Min();

    TTxIdRange() = default;
    TTxIdRange(ui64 txId, TRowVersion begin = TRowVersion::Min()) : TxId(txId), Begin(begin) {}
};

}
