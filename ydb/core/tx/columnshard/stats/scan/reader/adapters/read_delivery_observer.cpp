#include "read_delivery_observer.h"
#include "read_observer.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/blobs_action/common/const.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NStats::NScan {

TReadDeliveryObserver::TReadDeliveryObserver(TOperationScope scope, TCallHandle parent, NColumnShardStats::TDataSource dataSource)
    : Scope(std::move(scope))
    , Parent(std::move(parent))
    , DataSource(std::move(dataSource))
{
}

TReadDeliveryObserver TReadDeliveryObserver::ForSystemView(TLocalStats* local, const TCallHandle& parent, TStringBuf tablePath) {
    if (!local) {
        return {};
    }
    auto* node = parent.GetNode() ? local->CloneBranch(*parent.GetNode()) : &local->GetRoot();
    if (!node) {
        return {};
    }
    auto definition = MakeStorageRead();
    definition.SetKey("StorageRead.SystemView");
    definition.MutableStorageRead()->SetStorageId(TString(tablePath));
    NColumnShardStats::TDataSource dataSource;
    dataSource.SetStorageId(TString(tablePath));
    dataSource.SetKind(NColumnShardStats::SYSTEM_VIEW);
    dataSource.MutableReadBytes()->SetBoundary(NColumnShardStats::LOGICAL_DELIVERY);
    dataSource.MutableReadBytes()->SetRepresentation(NColumnShardStats::ARROW_LOGICAL);
    return TReadDeliveryObserver(local->Measure(local->AddChild(*node, definition), parent), parent, std::move(dataSource));
}

TReadDeliveryObserver TReadDeliveryObserver::ForInlineIndex(TLocalStats* local, const TCallHandle& parent, ui32 indexId) {
    if (!local) {
        return {};
    }
    auto* node = parent.GetNode() ? local->CloneBranch(*parent.GetNode()) : &local->GetRoot();
    if (!node) {
        return {};
    }
    auto definition = MakeStorageRead();
    definition.SetKey(TStringBuilder() << "InlineIndex:" << indexId);
    NColumnShardStats::TDataSource dataSource;
    dataSource.SetStorageId(NBlobOperations::TGlobal::LocalMetadataStorageId);
    dataSource.SetKind(NColumnShardStats::INLINE_INDEX);
    dataSource.MutableReadBytes()->SetBoundary(NColumnShardStats::LOGICAL_DELIVERY);
    dataSource.MutableReadBytes()->SetRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
    return TReadDeliveryObserver(local->Measure(local->AddChild(*node, definition), parent), parent, std::move(dataSource));
}

void TReadDeliveryObserver::Complete(std::optional<ui64> bytes) {
    if (!Scope) {
        return;
    }
    Scope.RecordReadSelf(DataSource, bytes);
    auto attribution = MakeReadAttributionCallback(Parent, []() noexcept {
        return true;
    });
    if (!attribution(DataSource, bytes)) {
        Scope.ObserveLoss();
    }
    Scope.Finish();
}

void TReadDeliveryObserver::OnData(const std::shared_ptr<arrow::Array>& array) {
    if (Scope) {
        Complete(array ? std::make_optional(NArrow::GetArrayDataSize(array)) : std::nullopt);
    }
}

void TReadDeliveryObserver::OnData(ui64 bytes) {
    Complete(bytes);
}

}   // namespace NKikimr::NOlap::NStats::NScan
