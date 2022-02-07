#include "blobstorage_hulloptlsn.h"
#include <util/stream/output.h>

namespace NKikimr {
    const TOptLsn TOptLsn::NotSet = {};
} // NKikimr

Y_DECLARE_OUT_SPEC(, NKikimr::TOptLsn, stream, value) {
    value.Output(stream);
}

