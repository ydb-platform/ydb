#include "data_packer.h"

namespace NFq::NRowDispatcher {

namespace {

ui64 EstimateMemoryUsage(size_t packedSize) {
    using namespace NKikimr::NMiniKQL;
    constexpr size_t pageSize = TBufferPage::DefaultPageAllocSize;
    constexpr size_t pageCapacity = pageSize - sizeof(TBufferPage) - sizeof(NYql::NDecimal::TInt128);
    return ((packedSize + pageCapacity - 1) / pageCapacity) * pageSize;
}

} // anonymous namespace

TMemoryLimitedDataPacker::TMemoryLimitedDataPacker(const NKikimr::NMiniKQL::TType* type, NYql::NDq::IMemoryQuotaManager::TPtr manager)
    : Manager(std::move(manager))
    , Memory(std::make_shared<TMemoryQuota>(Manager))
    , Packer(type, NKikimr::NMiniKQL::EValuePackerVersion::V0, NYql::DefaultDatumValidationMode)
{
    Y_ENSURE(!Packer.IsBlock());
}

void TMemoryLimitedDataPacker::AddWideItem(const NYql::NUdf::TUnboxedValuePod* values, ui32 count) {
    try {
        Packer.AddWideItem(values, count);
        Memory->Resize(EstimateMemoryUsage(Packer.PackedSizeEstimate()));
    } catch (...) {
        Packer.Clear();
        Memory->Resize(0);
        throw;
    }
}

size_t TMemoryLimitedDataPacker::PackedSizeEstimate() const {
    return Packer.PackedSizeEstimate();
}

bool TMemoryLimitedDataPacker::IsEmpty() const {
    return Packer.IsEmpty();
}

NYql::TChunkedBuffer TMemoryLimitedDataPacker::Finish() {
    try {
        auto data = Packer.Finish();
        Memory->Resize(EstimateMemoryUsage(data.Size()));
        auto nextMemory = std::make_shared<TMemoryQuota>(Manager);
        auto result = HoldMemoryQuota(std::move(data), Memory);
        Memory = std::move(nextMemory);
        return result;
    } catch (...) {
        Packer.Clear();
        Memory->Resize(0);
        throw;
    }
}

} // namespace NFq::NRowDispatcher
