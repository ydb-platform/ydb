#include "yql_yt_table_data_service_local.h"
#include <util/string/join.h>
#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TLocalTableDataService: public ILocalTableDataService {
public:

    NThreading::TFuture<void> Put(const TString& key, const TString& value) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Putting key " << key << " to local table data service";
        TGuard<TMutex> guard(Mutex_);
        auto [group, chunk] = GetTableDataServiceGroupAndChunk(key);
        Data_[group][chunk] = value;
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& key) const override {
        YQL_CLOG(TRACE, FastMapReduce) << "Getting key " << key << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        auto [group, chunk] = GetTableDataServiceGroupAndChunk(key);
        if (!Data_.contains(group) || !Data_.at(group).contains(chunk)) {
            TMaybe<TString> emptyRes = Nothing();
            return NThreading::MakeFuture(emptyRes);
        }
        TMaybe<TString> value = Data_.at(group).at(chunk);
        return NThreading::MakeFuture(value);
    }

    NThreading::TFuture<void> Delete(const TString& key) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Deleting key " << key << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        auto [group, chunk] = GetTableDataServiceGroupAndChunk(key);
        if (!Data_.contains(group)) {
            return NThreading::MakeFuture();
        }
        Data_[group].erase(chunk);
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& groupsToDelete) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Registering deletion for groups " << JoinRange(' ', groupsToDelete.begin(), groupsToDelete.end()) << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        for (auto& group: groupsToDelete) {
            Data_.erase(group);
        }
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TTableDataServiceStats> GetStatistics() const override {
        ui64 keysNum = 0, dataWeight = 0;
        for (auto& [group, chunks]: Data_) {
            for (auto& [chunkId, value]: chunks) {
                ++keysNum;
                dataWeight += value.size();
            }
        }
        return NThreading::MakeFuture(TTableDataServiceStats{.KeysNum = keysNum, .DataWeight = dataWeight});
    }

private:
    std::unordered_map<TString, std::unordered_map<ui64, TString>> Data_;  // Groups -> ChunkId -> Values
    TMutex Mutex_ = TMutex();
};

} // namespace

ILocalTableDataService::TPtr MakeLocalTableDataService() {
    return MakeIntrusive<TLocalTableDataService>();
}

} // namespace NYql::NFmr
