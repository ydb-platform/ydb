#pragma once

#include <util/system/types.h>

namespace NKikimr::NPQ {

// Conversion between blob *Key* offset space and packed batch *Header* offset space.
//
// Why two spaces exist
// --------------------
// On supportive → parent tx commit, BodyKeys are remapped with TKey::FromKey
// (Key.Offset becomes the parent partition offset), but blob *values* are not
// rewritten. Batch headers inside the value therefore keep supportive offsets
// (often starting at 0), while the KV key and client-facing TReadInfo::Offset
// use parent/key coordinates.
//
// CompactRequestedBlob (small-blob compaction) rewrites values so Key and
// headers agree again; RenameCompactedBlob / tx rename do not.
//
// Invariant inside one body blob
// ------------------------------
// Relative distances are preserved regardless of rename:
//
//   headerOffset - firstHeaderOffset  ==  keySpaceOffset - blobKeyOffset
//
// where:
//   blobKeyOffset     = TKey::GetOffset() of the body blob
//   firstHeaderOffset = Header.Offset of the first packed batch in that blob
//
// Bijection (used by AddBlobsFromBody)
// ------------------------------------
//   HeaderOffsetToKeySpace  — headers / FindPos result → client / Key space
//                             (trueOffset; Offset after FindPos)
//   KeyOffsetToHeaderSpace  — client / Key space → FindPos search coordinate
//                             (trueSearchOffset)
//
// When spaces coincide (firstHeaderOffset == blobKeyOffset), both maps are
// identity. That is the common non-rename case.

// Map an offset from batch-header coordinates into blob-key / client coordinates.
ui64 HeaderOffsetToKeySpace(ui64 blobKeyOffset, ui64 firstHeaderOffset, ui64 headerOffset);

// Inverse: map a key-space offset into batch-header coordinates for FindPos.
ui64 KeyOffsetToHeaderSpace(ui64 blobKeyOffset, ui64 firstHeaderOffset, ui64 keySpaceOffset);

} // namespace NKikimr::NPQ
