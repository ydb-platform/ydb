#include "row_version.h"

#include <util/stream/output.h>

Y_DECLARE_OUT_SPEC(, NKikimr::TRowVersion, stream, value) {
    if (value == NKikimr::TRowVersion::Min()) {
        stream << "v{min}";
    } else if (value == NKikimr::TRowVersion::Max()) {
        stream << "v{max}";
    } else {
        stream << 'v' << value.Step << '/' << value.TxId;
    }
}
