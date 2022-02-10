#include "blobstorage_syncstate.h"

Y_DECLARE_OUT_SPEC(, NKikimr::TSyncState, stream, value) {
    value.Output(stream);
}
