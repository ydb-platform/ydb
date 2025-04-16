#pragma once

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/public/udf/udf_types.h>
#include <unordered_map>

namespace NKikimr::NMiniKQL {

// -------------------------------------------------------------------
// This class is used for benchmarks only. Doesn't take part in any logic
class TResourceMeter {
private:
    struct TStatistics {
        std::pair<ui64, ui64>   avgMemoryConsumpted{0, 0};
        ui64                    memoryConsumptionPeak{0};
        ui64                    processedTime{0}; // in microseconds
        THashMap<TString, ui64> stages; // time spent for concrete stage in microseconds
    };

public:
    TResourceMeter() {
        MakeNewHistoryPage();
    }

    void MakeNewHistoryPage() {
        History_.emplace_back();
    }

    // Accumulate time
    void UpdateSpentTime(const char* id, ui64 msec) {
        auto& page = History_.back();
        page[id].processedTime += msec;
    }

    // Upate memory consumption stats
    void UpdateConsumptedMemory(const char* id, ui64 memoryConsumpted) {
        using std::max;
        auto& page = History_.back();
        page[id].avgMemoryConsumpted.first += memoryConsumpted;
        page[id].avgMemoryConsumpted.second++;
        page[id].memoryConsumptionPeak = max(page[id].memoryConsumptionPeak, memoryConsumpted);
    }

    void UpdateStageSpentTime(const char* id, const char* stage, ui64 msec) {
        auto& page = History_.back();
        page[id].stages[stage] += msec;
    }

    // Aggragate two pages in "to", "from" page will be removed
    void MergeHistoryPages(const char* from, const char* to) {
        using std::max;
        auto& page = History_.back();
        page[to].avgMemoryConsumpted.first += page[from].avgMemoryConsumpted.first;
        page[to].avgMemoryConsumpted.second += page[from].avgMemoryConsumpted.second;
        page[to].memoryConsumptionPeak = max(
            page[to].memoryConsumptionPeak, page[from].memoryConsumptionPeak);
        page[to].processedTime += page[from].processedTime;
        for (const auto& [stage, t]: page[from].stages) {
            page[to].stages[stage] += t;
        }
        page.erase(from);
    }

    void Clear() {
        History_.clear();
    }

    void ClearLastHistoryPage() {
        if (History_.empty()) {
            return;
        }
        History_.pop_back();
    }

    TString GetFullLog(ui64 datasetSize /* in bytes */) {
        TStringStream out;
        out << "=========================================\n";
        for (size_t i = 0; i < History_.size(); ++i) {
            const auto& page = History_[i];
            out << "---------- Run N. " << i << " begin  -----------\n";
            for (const auto& [id, stats]: page) {
                out << "Node: " << id << "\n"
                    << " > Dataset size:     " << datasetSize / 1024 << " [KB]\n"
                    << " > Total time spent: " << stats.processedTime << " [us]\n";
                for (const auto& [stage, t]: stats.stages) {
                    out << "   > \"" << stage << "\": " << t << " [us]\n";
                }
                out << " > Speed:            " << datasetSize / stats.processedTime << " [MB/s]\n"
                    << " > Avg. memory consumption: " << stats.avgMemoryConsumpted.first / (stats.avgMemoryConsumpted.second * 1024) << " [KB]\n"
                    << " > Max memory consumption:  " << stats.memoryConsumptionPeak / 1024 << " [KB]\n\n";
            }
            out << "----------- Run N. " << i << " end  ------------\n";
            if (i != History_.size() - 1) {
                out << "\n";
            }
        }
        out << "=========================================\n";
        return out.Str();
    }

private:
    TVector<THashMap<TString, TStatistics>>   History_;
};

// -------------------------------------------------------------------
extern TResourceMeter globalResourceMeter;

} // namespace NKikimr::NMiniKQL
