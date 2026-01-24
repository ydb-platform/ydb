#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/datetime/base.h>

#include <algorithm>
#include <cctype>
#include <string>

namespace NYdb::NConsoleClient {

// Minimal entry structure for testing sorting/filtering logic
struct TTestEntry {
    TString Name;
    bool IsDirectory = false;
    bool IsTopic = false;
    ui32 PartitionCount = 0;
    ui32 ConsumerCount = 0;
    ui64 TotalSizeBytes = 0;
    TDuration RetentionPeriod;
    ui64 WriteSpeedBytesPerSec = 0;
    ui32 ChildCount = 0;  // For directories
};

// Comparison function for sorting entries
// Returns: negative if a < b, 0 if equal, positive if a > b
inline int CompareEntries(const TTestEntry& a, const TTestEntry& b, int sortColumn) {
    switch (sortColumn) {
        case 0: // Name
            return a.Name.compare(b.Name);
        case 1: // Type (dir vs topic)
            return (a.IsTopic && !b.IsTopic) ? 1 : (!a.IsTopic && b.IsTopic) ? -1 : 0;
        case 2: // Parts
            {
                ui32 aVal = a.IsTopic ? a.PartitionCount : a.ChildCount;
                ui32 bVal = b.IsTopic ? b.PartitionCount : b.ChildCount;
                return (aVal < bVal) ? -1 : (aVal > bVal) ? 1 : 0;
            }
        case 3: // Size
            return (a.TotalSizeBytes < b.TotalSizeBytes) ? -1 : (a.TotalSizeBytes > b.TotalSizeBytes) ? 1 : 0;
        case 4: // Consumers
            return (a.ConsumerCount < b.ConsumerCount) ? -1 : (a.ConsumerCount > b.ConsumerCount) ? 1 : 0;
        case 5: // Retention
            return (a.RetentionPeriod < b.RetentionPeriod) ? -1 : (a.RetentionPeriod > b.RetentionPeriod) ? 1 : 0;
        case 6: // Write speed
            return (a.WriteSpeedBytesPerSec < b.WriteSpeedBytesPerSec) ? -1 : (a.WriteSpeedBytesPerSec > b.WriteSpeedBytesPerSec) ? 1 : 0;
        default:
            return a.Name.compare(b.Name);
    }
}

// Sort entries with special rules:
// 1. ".." always first
// 2. Directories before topics
// 3. Then by specified column
inline void SortTestEntries(TVector<TTestEntry>& entries, int sortColumn, bool sortAscending) {
    std::stable_sort(entries.begin(), entries.end(),
        [sortColumn, sortAscending](const TTestEntry& a, const TTestEntry& b) {
            // ".." always comes first
            if (a.Name == "..") return true;
            if (b.Name == "..") return false;
            
            // Directories before topics
            if (a.IsDirectory && !b.IsDirectory) return true;
            if (!a.IsDirectory && b.IsDirectory) return false;
            
            // Same type - sort by selected column
            int cmp = CompareEntries(a, b, sortColumn);
            return sortAscending ? (cmp < 0) : (cmp > 0);
        });
}

// Get indices of entries matching a search query (case-insensitive substring)
inline TVector<size_t> FilterEntriesByName(const TVector<TTestEntry>& entries, const std::string& query) {
    TVector<size_t> result;
    
    if (query.empty()) {
        for (size_t i = 0; i < entries.size(); ++i) {
            result.push_back(i);
        }
        return result;
    }
    
    // Case-insensitive
    std::string queryLower = query;
    for (auto& c : queryLower) {
        c = std::tolower(static_cast<unsigned char>(c));
    }
    
    for (size_t i = 0; i < entries.size(); ++i) {
        std::string nameLower = std::string(entries[i].Name.c_str());
        for (auto& c : nameLower) {
            c = std::tolower(static_cast<unsigned char>(c));
        }
        
        if (nameLower.find(queryLower) != std::string::npos) {
            result.push_back(i);
        }
    }
    
    return result;
}

// Helper to check if a string matches a pattern (prefix match, case-insensitive)
inline bool MatchesPrefix(const TString& text, const TString& prefix) {
    if (prefix.empty()) return true;
    if (text.size() < prefix.size()) return false;
    
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (std::tolower(text[i]) != std::tolower(prefix[i])) {
            return false;
        }
    }
    return true;
}

// Factory functions for creating test entries
inline TTestEntry CreateParentEntry() {
    TTestEntry entry;
    entry.Name = "..";
    entry.IsDirectory = true;
    return entry;
}

inline TTestEntry CreateDirEntry(const TString& name, ui32 childCount = 0) {
    TTestEntry entry;
    entry.Name = name;
    entry.IsDirectory = true;
    entry.ChildCount = childCount;
    return entry;
}

inline TTestEntry CreateTopicEntry(const TString& name, ui32 partitions = 1, ui32 consumers = 0) {
    TTestEntry entry;
    entry.Name = name;
    entry.IsTopic = true;
    entry.PartitionCount = partitions;
    entry.ConsumerCount = consumers;
    return entry;
}

inline TTestEntry CreateTopicWithSize(const TString& name, ui64 sizeBytes) {
    TTestEntry entry;
    entry.Name = name;
    entry.IsTopic = true;
    entry.TotalSizeBytes = sizeBytes;
    return entry;
}

inline TTestEntry CreateTopicWithRetention(const TString& name, TDuration retention) {
    TTestEntry entry;
    entry.Name = name;
    entry.IsTopic = true;
    entry.RetentionPeriod = retention;
    return entry;
}

inline TTestEntry CreateTopicWithWriteSpeed(const TString& name, ui64 bytesPerSec) {
    TTestEntry entry;
    entry.Name = name;
    entry.IsTopic = true;
    entry.WriteSpeedBytesPerSec = bytesPerSec;
    return entry;
}

} // namespace NYdb::NConsoleClient
