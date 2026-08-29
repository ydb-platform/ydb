#pragma once

#include "metrics.h"

#include <library/cpp/json/json_reader.h>

#include <util/generic/string.h>

#include <memory>

namespace NPlan2Svg {

TString GetEstimation(const NJson::TJsonValue& node);
const NJson::TJsonValue* GetOutputStatNode(const NJson::TJsonValue& node);
const NJson::TJsonValue* GetInputStatNode(const NJson::TJsonValue& node);
TString ParseTableOrIndexName(const TString& table);
TString ParseColumns(const NJson::TJsonValue* node);

// The { Bytes, Rows, Chunks } group carried by every channel stat node - stage
// output, connection input, ingress and the two CTE variants all report it the
// same way.
struct TChannelStats {
    // Always set. When the corresponding node is absent these are empty
    // placeholder metrics, matching what each call site used to build inline.
    std::shared_ptr<TSingleMetric> Bytes;
    std::shared_ptr<TSingleMetric> Rows;
    // Only set when Chunks.Sum was present and non-zero.
    std::shared_ptr<TScalarMetric> ChunkSize;
    ui64 Chunks = 0;
    bool HasChunks = false;
    // Null when the field was absent. Call sites that derive further metrics from
    // the raw node (operator rows, per-input rows) use these rather than a flag.
    const NJson::TJsonValue* BytesNode = nullptr;
    const NJson::TJsonValue* RowsNode = nullptr;
};

// chunkSizeSummary may be null for channels that do not report chunks, such as
// ingress. Chunks are then left unread: constructing a TScalarMetric feeds the
// summary it is given, so reading them anyway would pull values into an
// aggregate that does not see them today.
TChannelStats LoadChannelStats(const NJson::TJsonValue& statNode,
    const std::shared_ptr<TSummaryMetric>& bytesSummary,
    const std::shared_ptr<TSummaryMetric>& rowsSummary,
    const std::shared_ptr<TSummaryMetric>& chunkSizeSummary);

} // namespace NPlan2Svg
