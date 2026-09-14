#pragma once

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <util/generic/fwd.h>

namespace NKikimr::NGRpcProxy {
    // Validates that client can safely write to the topic data compressed with specific codec
    bool ValidateWriteWithCodec(const NKikimrPQ::TPQTabletConfig& pqTabletConfig, const ui32 codecID, TString& error);
}
