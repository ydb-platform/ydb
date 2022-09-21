#include "trace_collection.h"

namespace NKikimr {
namespace NTracing {

static const ui64 MaxTracesPerTablet = 20;
static const ui64 MaxIntrospectionDataSize = 536870912;    // 512 MB

bool TTabletTraces::AddTrace(ITrace* trace) {
    bool incremented = true;
    if (Traces.size() >= MaxTracesPerTablet) {
        const auto& oldestTrace = *Traces.begin();
        TotalSize -= oldestTrace->GetSize();
        Indexes.erase(oldestTrace->GetSelfID());
        Traces.pop_front();
        incremented = false;
    }
    Traces.emplace_back(trace);
    const auto newElementIterator = --Traces.end();
    Indexes[(*newElementIterator)->GetSelfID()] = newElementIterator;
    TotalSize += (*newElementIterator)->GetSize();
    return incremented;
}

void TTabletTraces::GetTraces(TVector<TTraceID>& result) const {
    result.reserve(Traces.size());
    for (auto& trace : Traces) {
        result.push_back(trace->GetSelfID());
    }
}

ITrace* TTabletTraces::GetTrace(TTraceID& traceID) {
    auto findResult = Indexes.find(traceID);
    if (findResult != Indexes.end()) {
        auto introspectionTracesIterator = findResult->second;

        // Move this element to the end of the list
        Traces.splice(Traces.end(), Traces, introspectionTracesIterator);
        introspectionTracesIterator = --Traces.end();
        Indexes[traceID] = introspectionTracesIterator;
        return introspectionTracesIterator->Get();
    }
    return nullptr;
}

ui64 TTabletTraces::GetSize() const {
    return TotalSize;
}

ui64 TTabletTraces::GetCount() const {
    return Traces.size();
}

TTraceCollection::TTraceCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    if (counters) {
        Reporting = true;
        ReportedSize = counters->GetCounter("totalsize");
        ReportedCurrentCount = counters->GetCounter("tracescurrent");
        ReportedTotalCount = counters->GetCounter("tracestotal");
        ReportedTabletCount = counters->GetCounter("tabletscurrent");
    }
}

void TTraceCollection::AddTrace(ui64 tabletID, ITrace* trace) {
    auto findResult = Indexes.find(tabletID);
    MainQueueType::iterator mainQueueIterator;
    if (findResult != Indexes.end()) {
        mainQueueIterator = findResult->second;
        SubTotalSize(mainQueueIterator->Traces.GetSize());
        // Move this element to the end of the list
        MainQueue.splice(MainQueue.end(), MainQueue, mainQueueIterator);
    } else {
        MainQueue.push_back({ tabletID, TTabletTraces() });
        ReportedTabletCount->Inc();
    }
    mainQueueIterator = --MainQueue.end();
    Indexes[tabletID] = mainQueueIterator;
    if (mainQueueIterator->Traces.AddTrace(trace)) {
        ReportedCurrentCount->Inc();
    }
    ReportedTotalCount->Inc();
    AddTotalSize(mainQueueIterator->Traces.GetSize());
    CheckSizeLimit();
}

void TTraceCollection::CheckSizeLimit() {
    while (TotalSize > MaxIntrospectionDataSize) {
        auto FrontElement = MainQueue.begin();
        RemoveElement(FrontElement);
    }
}

void TTraceCollection::RemoveElement(MainQueueType::iterator element) {
    SubTotalSize(element->Traces.GetSize());
    ReportedCurrentCount->Sub(element->Traces.GetCount());
    Indexes.erase(element->TabletID);
    MainQueue.erase(element);
    ReportedTabletCount->Dec();
}

void TTraceCollection::AddTotalSize(ui64 value) {
    TotalSize += value;
    if (Reporting) {
        ReportedSize->Add(value);
    }
}

void TTraceCollection::SubTotalSize(ui64 value) {
    TotalSize -= value;
    if (Reporting) {
        ReportedSize->Sub(value);
    }
}

void TTraceCollection::GetTabletIDs(TVector<ui64>& tabletIDs) const {
    tabletIDs.reserve(MainQueue.size());
    for (const auto& tabletEntry : MainQueue) {
        tabletIDs.push_back(tabletEntry.TabletID);
    }
}


bool TTraceCollection::HasTabletID(ui64 tabletID) const {
    return Indexes.contains(tabletID);
}

bool TTraceCollection::GetTraces(ui64 tabletID, TVector<TTraceID>& result) {
    auto findResult = Indexes.find(tabletID);
    if (findResult != Indexes.end()) {
        auto mainQueueIterator = findResult->second;
        mainQueueIterator->Traces.GetTraces(result);
        // Move this element to the end of the list
        MainQueue.splice(MainQueue.end(), MainQueue, mainQueueIterator);
        mainQueueIterator = --MainQueue.end();
        Indexes[tabletID] = mainQueueIterator;
        return true;
    }
    return false;
}

ITrace* TTraceCollection::GetTrace(ui64 tabletID, TTraceID& traceID) {
    auto findResult = Indexes.find(tabletID);
    if (findResult != Indexes.end()) {
        auto mainQueueIterator = findResult->second;
        // Move this element to the end of the list
        MainQueue.splice(MainQueue.end(), MainQueue, mainQueueIterator);
        mainQueueIterator = --MainQueue.end();
        Indexes[tabletID] = mainQueueIterator;
        return mainQueueIterator->Traces.GetTrace(traceID);
    }
    return nullptr;
}

ITraceCollection* CreateTraceCollection(TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TTraceCollection(counters);
}

}
}
