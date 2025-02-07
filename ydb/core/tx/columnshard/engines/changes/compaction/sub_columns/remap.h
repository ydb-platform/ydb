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
    std::vector<std::vector<std::vector<std::optional<TRemapInfo>>>> RemapInfo;
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
        if (RemapInfo.size() <= sourceIdx) {
            RemapInfo.resize(sourceIdx);
        }
        RemapInfo[sourceIdx].clear();
        auto& remapSourceInfo = RemapInfo[sourceIdx];
        remapSourceInfo.resize(2);
        auto& remapSourceInfoColumns = remapSourceInfo[1];
        auto& remapSourceInfoOthers = remapSourceInfo[0];
        AFL_VERIFY(ResultColumnStats);
        for (ui32 i = 0; i < sourceColumnStats.GetColumnsCount(); ++i) {
            if (remapSourceInfoColumns.size() <= i) {
                remapSourceInfoColumns.resize((i + 1) * 2);
            }
            AFL_VERIFY(!remapSourceInfoColumns[i]);
            if (auto commonKeyIndex = ResultColumnStats->GetKeyIndexOptional(sourceColumnStats.GetColumnName(i))) {
                remapSourceInfoColumns[i] = TRemapInfo(*commonKeyIndex, true);
            } else {
                commonKeyIndex = RegisterNewOtherIndex(sourceColumnStats.GetColumnName(i));
                remapSourceInfoColumns[i] = TRemapInfo(*commonKeyIndex, false);
            }
        }
        for (ui32 i = 0; i < sourceOtherStats.GetColumnsCount(); ++i) {
            if (remapSourceInfoOthers.size() <= i) {
                remapSourceInfoOthers.resize((i + 1) * 2);
            }
            AFL_VERIFY(!remapSourceInfoOthers[i]);
            if (auto commonKeyIndex = ResultColumnStats->GetKeyIndexOptional(sourceOtherStats.GetColumnName(i))) {
                remapSourceInfoOthers[i] = TRemapInfo(*commonKeyIndex, true);
            } else {
                commonKeyIndex = RegisterNewOtherIndex(sourceOtherStats.GetColumnName(i));
                remapSourceInfoOthers[i] = TRemapInfo(*commonKeyIndex, false);
            }
        }
    }

    TRemapInfo RemapIndex(const ui32 sourceIdx, const ui32 sourceKeyIndex, const bool isColumnKey) const {
        AFL_VERIFY(sourceIdx < RemapInfo.size());
        AFL_VERIFY(RemapInfo[sourceIdx].size() == 2);
        AFL_VERIFY(sourceKeyIndex < RemapInfo[sourceIdx][isColumnKey ? 1 : 0].size());
        auto result = RemapInfo[sourceIdx][isColumnKey ? 1 : 0][sourceKeyIndex];
        AFL_VERIFY(result);
        return *result;
    }
};

}   // namespace NKikimr::NOlap::NCompaction::NSubColumns
