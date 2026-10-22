#pragma once

#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/strbuf.h>

namespace NKikimrConfig {
class TDataShardConfig;
}

namespace NKikimrPQ {
class TPQTabletConfig;
}

namespace NKikimr::NDataShard {

enum class ECdcPayloadCompression {
    Raw,
    Gzip,
};

ECdcPayloadCompression SelectCdcPayloadCompression(
    const NKikimrConfig::TDataShardConfig& config,
    TStringBuf replicationAttributes,
    const NKikimrPQ::TPQTabletConfig& topicConfig);

// Nothing means that the original RAW payload must be retained unchanged.
TMaybe<TString> CompressCdcPayload(TStringBuf payload, ECdcPayloadCompression compression);

} // namespace NKikimr::NDataShard
