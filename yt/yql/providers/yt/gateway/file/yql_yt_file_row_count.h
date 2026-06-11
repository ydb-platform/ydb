#pragma once

#include <library/cpp/yson/consumer.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NYql::NFile {

// Returns a consumer that counts top-level list-fragment rows while
// forwarding all YSON events to `inner`. rowCount is incremented in place.
std::unique_ptr<NYson::TYsonConsumerBase> MakeRowCountingYsonConsumer(
    NYT::NYson::IYsonConsumer* inner,
    ui64& rowCount
);

// Reads the existing .attr file (if present), sets "row_count", writes it back.
void SaveRowCountToAttr(const TString& dataFilePath, ui64 rowCount);

} // namespace NYql::NFile
