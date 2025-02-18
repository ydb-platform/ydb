#include "row_version.h"

#include <ydb/core/protos/base.pb.h>

#include <util/stream/output.h>

namespace NKikimr {

TRowVersion TRowVersion::FromProto(const NKikimrProto::TRowVersion& proto) {
    return TRowVersion(proto.GetStep(), proto.GetTxId());
}

void TRowVersion::ToProto(NKikimrProto::TRowVersion& proto) const {
    proto.SetStep(Step);
    proto.SetTxId(TxId);
}

void TRowVersion::ToProto(NKikimrProto::TRowVersion* proto) const {
    ToProto(*proto);
}

NKikimrProto::TRowVersion TRowVersion::ToProto() const {
    NKikimrProto::TRowVersion proto;
    ToProto(proto);
    return proto;
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
