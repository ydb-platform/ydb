#include "blob_offset.h"

namespace NKikimr::NPQ {

ui64 HeaderOffsetToKeySpace(ui64 blobKeyOffset, ui64 firstHeaderOffset, ui64 headerOffset) {
    return blobKeyOffset + (headerOffset - firstHeaderOffset);
}

ui64 KeyOffsetToHeaderSpace(ui64 blobKeyOffset, ui64 firstHeaderOffset, ui64 keySpaceOffset) {
    return keySpaceOffset - blobKeyOffset + firstHeaderOffset;
}

} // namespace NKikimr::NPQ
