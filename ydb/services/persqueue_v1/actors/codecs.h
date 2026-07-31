#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <util/generic/fwd.h>
#include <util/generic/vector.h>

namespace NKikimr::NGRpcProxy {
    // Validates that client can safely write to the topic data compressed with specific codec
    bool ValidateWriteWithCodec(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const ui32 codecID, TString& error);

    // Builds the list of supported codec numbers (as reported in a write-session InitResponse)
    // from the tablet config. Codecs are stored in two parallel lists: string names (Codecs) and
    // numeric ids (Ids). The name list is used as the base to preserve legacy behavior, and any
    // entry a name cannot express (custom codecs are stored as "CUSTOM" and map to 0) is filled in
    // from the numeric ids. Returned numbers are protocol-independent (CODEC_RAW == 1, ...).
    TVector<i32> BuildSupportedCodecs(const NKikimrPQ::TPQTabletConfig& pqTabletConfig);
}
