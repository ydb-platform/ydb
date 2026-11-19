#pragma once

#include <ydb/core/formats/arrow/accessor/abstract/decode_observer.h>
#include <ydb/core/tx/columnshard/stats/scan/observation.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NStats::NScan {

class TDecodeObserver: public NArrow::NAccessor::IDecodeObserver {
private:
    TLocalStats& LocalStats;
    using TNodePtr = TOperationNode*;
    YDB_ACCESSOR(TNodePtr, Parent, nullptr);
    YDB_ACCESSOR_DEF(TCallHandle, ParentCall);

public:
    explicit TDecodeObserver(TLocalStats& localStats)
        : LocalStats(localStats)
    {
    }

    std::unique_ptr<NArrow::NAccessor::IDecodeScope> OnDecodeBegin(
        const NArrow::NAccessor::TColumnLoader& loader, ui64 encodedBytes, std::optional<ui64> rows) override;
};

}   // namespace NKikimr::NOlap::NStats::NScan
