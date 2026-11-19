#pragma once

#include <ydb/core/formats/arrow/program/abstract.h>
#include <ydb/core/protos/columnshard_scan_stats.pb.h>

#include <util/generic/hash.h>

#include <optional>

namespace NKikimr::NOlap::NStats::NScan {

class TProcessorStatsAdapter {
    const NArrow::NSSA::IResourceProcessor& Processor;

public:
    explicit TProcessorStatsAdapter(const NArrow::NSSA::IResourceProcessor& processor)
        : Processor(processor)
    {
    }

    NArrow::NSSA::EProcessorType GetObservedType() const;
    THashMap<ui32, TString> DescribeExpressions(const THashMap<ui32, TString>& inputExpressions) const;
    std::optional<NColumnShardStats::TOperation> DescribeOperation(
        ui64 nodeId, const THashMap<ui32, TString>& names, const THashMap<ui32, TString>& expressions) const;
};

}   // namespace NKikimr::NOlap::NStats::NScan
