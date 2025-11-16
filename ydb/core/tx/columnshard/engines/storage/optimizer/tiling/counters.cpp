
#include <ydb/core/tx/columnshard/engines/storage/optimizer/tiling/counters.h>


namespace NKikimr {

ITilingCounter::ITilingCounter(ui32 i, bool isLevel): TBase("TilingLevelCounter") {
    auto name = (isLevel ? "level=" : "acc=") +  ToString(i);
    Count = TBase::GetValue("Count/" + name);
    BlobBytes = TBase::GetValue("BlobBytes/" + name);
    RawBytes = TBase::GetValue("RawBytes/" + name);
    RecordsCount = TBase::GetValue("RecordsCount/" + name);
}

void ITilingCounter::AddPortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
    RecordsCount->Add(p->GetRecordsCount());
    Count->Inc();
    BlobBytes->Add(p->GetTotalBlobBytes());
    RawBytes->Add(p->GetTotalRawBytes());
}

void ITilingCounter::RemovePortion(const std::shared_ptr<const NOlap::TPortionInfo>& p) {
    RecordsCount->Sub(p->GetRecordsCount());
    Count->Dec();
    BlobBytes->Sub(p->GetTotalBlobBytes());
    RawBytes->Sub(p->GetTotalRawBytes());
}

TTilingLevelCounter::TTilingLevelCounter(ui32 i): TBase(i, true) {
    IntersectionsCount = TBase::GetValue("IntersectionsCount/level=" + ToString(i));
}

void TTilingLevelCounter::SetIntersections(ui32 intersections) {
    IntersectionsCount->Set(intersections);
}

}