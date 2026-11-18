#pragma once

#include <ydb/core/tx/columnshard/stats/scan/observation.h>

#include <arrow/array.h>

namespace NKikimr::NOlap::NStats::NScan {

class TReadDeliveryObserver {
    TOperationScope Scope;
    TCallHandle Parent;
    NColumnShardStats::TDataSource DataSource;

    TReadDeliveryObserver(TOperationScope scope, TCallHandle parent, NColumnShardStats::TDataSource dataSource);
    void Complete(std::optional<ui64> bytes);

public:
    TReadDeliveryObserver() = default;

    static TReadDeliveryObserver ForSystemView(TLocalStats* local, const TCallHandle& parent, TStringBuf tablePath);
    static TReadDeliveryObserver ForInlineIndex(TLocalStats* local, const TCallHandle& parent, ui32 indexId);

    explicit operator bool() const noexcept {
        return static_cast<bool>(Scope);
    }

    void OnData(const std::shared_ptr<arrow::Array>& array);
    void OnData(ui64 bytes);
};

}   // namespace NKikimr::NOlap::NStats::NScan
