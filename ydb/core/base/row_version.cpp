#include "row_version.h"

#include <ydb/core/protos/base.pb.h>

#include <util/stream/output.h>

namespace NKikimr {

TRowVersion TRowVersion::Parse(const NKikimrProto::TRowVersion& proto) {
    return TRowVersion(proto.GetStep(), proto.GetTxId());
}

void TRowVersion::Serialize(NKikimrProto::TRowVersion& proto) const {
    proto.SetStep(Step);
    proto.SetTxId(TxId);
}

} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TRowVersion, stream, value) {
    if (value == NKikimr::TRowVersion::Min()) {
        stream << "v{min}";
    } else if (value == NKikimr::TRowVersion::Max()) {
        stream << "v{max}";
    } else {
        stream << 'v' << value.Step << '/' << value.TxId;
    }
}
