#include "parse.h"

#include "config.h"
#include "format.h"

#include <util/string/builder.h>
#include <util/string/cast.h>
#include <util/string/printf.h>

namespace NPlan2Svg {

TString GetEstimation(const NJson::TJsonValue& node) {
    TStringBuilder ebuilder;
    auto* eCostNode = node.GetValueByPath("E-SelfCost");
    if (!eCostNode) {
        eCostNode = node.GetValueByPath("E-Cost");
    }
    if (eCostNode) {
        auto costString = eCostNode->GetStringSafe();
        if (costString != "No estimate") {
            ebuilder << "Est:";
            double cost;
            if (TryFromString(costString, cost)) {
                if (cost >= 1e+18) {
                    ebuilder << Sprintf(" %.2e", cost);
                } else {
                    ebuilder << ' ' << FormatIntegerValue(static_cast<ui64>(cost));
                }
            }
            if (auto* eRowsNode = node.GetValueByPath("E-Rows")) {
                double rows;
                if (TryFromString(eRowsNode->GetStringSafe(), rows)) {
                    if (rows >= 1e+18) {
                        ebuilder << Sprintf(" Rows: %.2e", rows);
                    } else {
                        ebuilder << " Rows: " << FormatIntegerValue(static_cast<ui64>(rows));
                    }
                }
            }
            if (auto* eSizeNode = node.GetValueByPath("E-Size")) {
                double size;
                if (TryFromString(eSizeNode->GetStringSafe(), size)) {
                    if (size >= 1e+18) {
                        ebuilder << Sprintf(" Size: %.2e", size);
                    } else {
                        ebuilder << " Size: " << FormatBytes(static_cast<ui64>(size));
                    }
                }
            }
        }
    }
    return ebuilder;
}

const NJson::TJsonValue* GetOutputStatNode(const NJson::TJsonValue& node) {
    // if Push.Bytes found, use Push (channels 1.0 do not report Output.Push.Bytes)
    auto* pushNode = node.GetValueByPath("Push");
    if (pushNode && pushNode->GetValueByPath("Bytes")) {
        return pushNode;
    }
    // else if Pop.Bytes found, use Pop
    auto* popNode = node.GetValueByPath("Pop");
    if (popNode && popNode->GetValueByPath("Bytes")) {
        return popNode;
    }
    // else Push as default
    return pushNode;
}

const NJson::TJsonValue* GetInputStatNode(const NJson::TJsonValue& node) {
    return node.GetValueByPath("Pop");
}

TString ParseTableOrIndexName(const TString& table) {
    auto n = table.find_last_of('/');
    if (n == table.npos) {
        return table;
    }

    auto tableName = table.substr(n + 1);
    if (n == 0 || tableName != "indexImplTable") {
        return tableName;
    }

    auto ni = table.find_last_of('/', n - 1);
    if (ni == table.npos) {
        return table.substr(0, n);
    }

    if (ni == 0) {
        return table.substr(ni + 1, n - ni - 1);
    }

    auto nt = table.find_last_of('/', ni - 1);
    if (nt == table.npos) {
        return table.substr(0, n);
    } else {
        return table.substr(nt + 1, n - nt - 1);
    }
}

TString ParseColumns(const NJson::TJsonValue* node) {
    TStringBuilder builder;
    builder << '(';
    if (node) {
        bool firstColumn = true;
        for (const auto& subNode : node->GetArray()) {
            if (firstColumn) {
                firstColumn = false;
            } else {
                builder << ", ";
            }
            builder << subNode.GetStringSafe();
        }
    }
    builder << ')';
    return builder;
}

TChannelStats LoadChannelStats(const NJson::TJsonValue& statNode,
    const std::shared_ptr<TSummaryMetric>& bytesSummary,
    const std::shared_ptr<TSummaryMetric>& rowsSummary,
    const std::shared_ptr<TSummaryMetric>& chunkSizeSummary)
{
    TChannelStats stats;

    if (auto* bytesNode = statNode.GetValueByPath("Bytes")) {
        stats.BytesNode = bytesNode;
        stats.Bytes = std::make_shared<TSingleMetric>(bytesSummary,
            *bytesNode, 0, 0,
            statNode.GetValueByPath("FirstMessageMs"),
            statNode.GetValueByPath("LastMessageMs"),
            statNode.GetValueByPath("WaitTimeUs.History")
        );
    } else {
        stats.Bytes = std::make_shared<TSingleMetric>(bytesSummary);
    }

    if (auto* rowsNode = statNode.GetValueByPath("Rows")) {
        stats.RowsNode = rowsNode;
        stats.Rows = std::make_shared<TSingleMetric>(rowsSummary, *rowsNode);
    } else {
        stats.Rows = std::make_shared<TSingleMetric>(rowsSummary);
    }

    if (!chunkSizeSummary) {
        return stats;
    }

    if (auto* chunksNode = statNode.GetValueByPath("Chunks")) {
        if (auto* sumNode = chunksNode->GetValueByPath("Sum")) {
            stats.Chunks = sumNode->GetIntegerSafe();
            stats.HasChunks = true;
            if (stats.Chunks) {
                stats.ChunkSize = std::make_shared<TScalarMetric>(chunkSizeSummary,
                    stats.Bytes->Details.Sum / stats.Chunks);
            }
        }
    }

    return stats;
}

} // namespace NPlan2Svg
