#pragma once
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/accessor.h>
#include <ydb/core/formats/arrow/accessor/sub_columns/settings.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

#include <ydb/library/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/library/formats/arrow/accessor/common/const.h>

namespace NKikimr::NOlap::NCompaction::NSubColumns {

class TRemapColumns {
private:
    using TDictStats = NArrow::NAccessor::NSubColumns::TDictStats;
    using TOthersData = NArrow::NAccessor::NSubColumns::TOthersData;
    using TSettings = NArrow::NAccessor::NSubColumns::TSettings;

    class TRemapInfo {
    private:
        YDB_READONLY(ui32, CommonKeyIndex, 0);
        YDB_READONLY(bool, IsColumnKey, false);

    public:
        TRemapInfo(const ui32 keyIndex, const bool isColumnKey)
            : CommonKeyIndex(keyIndex)
            , IsColumnKey(isColumnKey) {
        }
    };

    class TSourceAddress {
    private:
        const ui32 SourceIndex;
        const ui32 SourceKeyIndex;
        const bool IsColumnKey;

    public:
        TSourceAddress(const ui32 sourceIndex, const ui32 sourceKeyIndex, const bool isColumnKey)
            : SourceIndex(sourceIndex)
            , SourceKeyIndex(sourceKeyIndex)
            , IsColumnKey(isColumnKey) {
        }

        ui32 GetSourceIndex() const {
            return SourceIndex;
        }

        bool operator<(const TSourceAddress& item) const {
            return std::tie(SourceIndex, SourceKeyIndex, IsColumnKey) < std::tie(item.SourceIndex, item.SourceKeyIndex, item.IsColumnKey);
        }
    };

    const TDictStats* ResultColumnStats = nullptr;
    std::map<TSourceAddress, TRemapInfo> RemapInfo;
    std::map<TString, ui32> TemporaryKeyIndex;

    ui32 RegisterNewOtherIndex(const TString& keyName) {
        return TemporaryKeyIndex.emplace(keyName, TemporaryKeyIndex.size()).first->second;
    }

    ui32 RegisterNewOtherIndex(const std::string_view keyName) {
        return TemporaryKeyIndex.emplace(TString(keyName.data(), keyName.size()), TemporaryKeyIndex.size()).first->second;
    }

public:
    TRemapColumns() {
    }

    TOthersData::TFinishContext BuildRemapInfo(const std::vector<TDictStats::TRTStatsValue>& statsByKeyIndex, const TSettings& settings, const ui32 recordsCount) const;

    void RegisterColumnStats(const TDictStats& resultColumnStats) {
        ResultColumnStats = &resultColumnStats;
    }

    void StartSourceChunk(const ui32 sourceIdx, const TDictStats& sourceColumnStats, const TDictStats& sourceOtherStats) {
        for (auto it = RemapInfo.begin(); it != RemapInfo.end();) {
            if (it->first.GetSourceIndex() == sourceIdx) {
                it = RemapInfo.erase(it);
            } else {
                ++it;
            }
        }
        AFL_VERIFY(ResultColumnStats);
        for (ui32 i = 0; i < sourceColumnStats.GetColumnsCount(); ++i) {
            if (auto commonKeyIndex = ResultColumnStats->GetKeyIndexOptional(sourceColumnStats.GetColumnName(i))) {
                AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, true), TRemapInfo(*commonKeyIndex, true)).second);
            } else {
                commonKeyIndex = RegisterNewOtherIndex(sourceColumnStats.GetColumnName(i));
                AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, true), TRemapInfo(*commonKeyIndex, false)).second);
            }
        }
        for (ui32 i = 0; i < sourceOtherStats.GetColumnsCount(); ++i) {
            if (auto commonKeyIndex = ResultColumnStats->GetKeyIndexOptional(sourceOtherStats.GetColumnName(i))) {
                AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, false), TRemapInfo(*commonKeyIndex, true)).second);
            } else {
                commonKeyIndex = RegisterNewOtherIndex(sourceOtherStats.GetColumnName(i));
                AFL_VERIFY(RemapInfo.emplace(TSourceAddress(sourceIdx, i, false), TRemapInfo(*commonKeyIndex, false)).second);
            }
        }
    }

    TRemapInfo RemapIndex(const ui32 sourceIdx, const ui32 sourceKeyIndex, const bool isColumnKey) const {
        auto it = RemapInfo.find(TSourceAddress(sourceIdx, sourceKeyIndex, isColumnKey));
        AFL_VERIFY(it != RemapInfo.end());
        return it->second;
    }
};

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
