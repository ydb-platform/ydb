#include "shards_splitter.h"
#include "columnshard_splitter.h"

namespace NKikimr::NEvWrite {

    TString IShardsSplitter::TFullSplitData::ShortLogString(const ui32 sizeLimit) const {
        TStringBuilder sb;
        for (auto&& i : ShardsInfo) {
            sb << i.first << ",";
            if (sb.size() >= sizeLimit) {
                break;
            }
        }
        return sb;
    }

    ui32 IShardsSplitter::TFullSplitData::GetShardRequestsCount() const {
        ui32 result = 0;
        for (auto&& i : ShardsInfo) {
            result += i.second.size();
        }
        return result;
    }

    const THashMap<ui64, std::vector<IShardsSplitter::IShardInfo::TPtr>>& IShardsSplitter::TFullSplitData::GetShardsInfo() const {
        return ShardsInfo;
    }

    ui32 IShardsSplitter::TFullSplitData::GetShardsCount() const {
        return AllShardsCount;
    }

    void IShardsSplitter::TFullSplitData::AddShardInfo(const ui64 tabletId, const IShardInfo::TPtr& info) {
        ShardsInfo[tabletId].emplace_back(info);
    }

    IShardsSplitter::TPtr IShardsSplitter::BuildSplitter(const NSchemeCache::TSchemeCacheNavigate::TEntry& schemeEntry) {
        switch (schemeEntry.Kind) {
            case NSchemeCache::TSchemeCacheNavigate::KindColumnTable:
                return std::make_shared<TColumnShardShardsSplitter>();
            default:
                return nullptr;
        }
        return nullptr;
    }
}
