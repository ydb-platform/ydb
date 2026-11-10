#pragma once

#include <ydb/core/base/row_version.h>

#include <util/generic/string.h>

namespace NKikimr::NCdcAggregator {

// DataShard tablet that produces changes of the stream.
using TSourceId = ui64;

// Progress boundary of a source or of the whole stream.
//
// Version H states that no change below H is pending: the source will not
// produce one and every existing one is durably written to the topic.
// Payload is the opaque heartbeat record for the topic in the stream format;
// equal versions carry equal payloads.
struct TBarrier {
    TRowVersion Version = TRowVersion::Min();
    TString Payload;

    bool operator==(const TBarrier&) const = default;
};

} // namespace NKikimr::NCdcAggregator
