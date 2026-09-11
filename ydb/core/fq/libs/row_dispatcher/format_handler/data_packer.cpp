#include "data_packer.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NFq::NRowDispatcher {

TMemoryLimitedDataPacker::TMemoryLimitedDataPacker(NYql::NDq::IMemoryQuotaManager::TPtr manager, ui64 itemSizeOverhead, NMonitoring::TDynamicCounterPtr counters)
    : ItemSizeOverhead(itemSizeOverhead)
    , PackingMemory(std::move(manager), "PackingMemory", std::move(counters))
{}

size_t TMemoryLimitedDataPacker::PackedSizeEstimate() const {
    Y_VALIDATE(Packer, "Packer is not initialized");
    return Packer->PackedSizeEstimate();
}

bool TMemoryLimitedDataPacker::IsEmpty() const {
    Y_VALIDATE(Packer, "Packer is not initialized");
    return Packer->IsEmpty();
}

void TMemoryLimitedDataPacker::SetPackerType(const NKikimr::NMiniKQL::TType* type) {
    Packer = std::make_unique<NKikimr::NMiniKQL::TValuePackerTransport<true>>(type, NKikimr::NMiniKQL::EValuePackerVersion::V0, NYql::DefaultDatumValidationMode);
    Y_VALIDATE(!Packer->IsBlock(), "Block type is not supported");
    ItemsCount = 0;
}

void TMemoryLimitedDataPacker::AddWideItem(const NYql::NUdf::TUnboxedValuePod* values, ui32 count) {
    Y_VALIDATE(Packer, "Packer is not initialized");

    try {
        Packer->AddWideItem(values, count);
        ItemsCount++;
        PackingMemory.Reserve(EstimateMemoryUsage(Packer->PackedSizeEstimate()));
    } catch (...) {
        Packer->Clear();
        ItemsCount = 0;
        throw;
    }
}

std::pair<NYql::TChunkedBuffer, ui64> TMemoryLimitedDataPacker::Finish() {
    Y_VALIDATE(Packer, "Packer is not initialized");
    try {
        auto data = Packer->Finish();
        const auto finalSize = EstimateMemoryUsage(data.Size());
        ItemsCount = 0;
        PackingMemory.Reserve(finalSize);
        return {std::move(data), finalSize};
    } catch (...) {
        Packer->Clear();
        ItemsCount = 0;
        throw;
    }
}

ui64 TMemoryLimitedDataPacker::EstimateMemoryUsage(size_t packedSize) const {
    using namespace NKikimr::NMiniKQL;
    constexpr size_t pageSize = TBufferPage::DefaultPageAllocSize;
    constexpr size_t pageCapacity = pageSize - sizeof(TBufferPage) - sizeof(NYql::NDecimal::TInt128);
    return ((packedSize + pageCapacity - 1) / pageCapacity) * pageSize + ItemSizeOverhead * ItemsCount;
}

} // namespace NFq::NRowDispatcher
