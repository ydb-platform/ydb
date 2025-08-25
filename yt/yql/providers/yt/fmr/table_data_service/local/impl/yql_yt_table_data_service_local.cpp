#include "yql_yt_table_data_service_local.h"
#include <util/string/join.h>
#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TLocalTableDataService: public ILocalTableDataService {
public:

    NThreading::TFuture<void> Put(const TString& group, const TString& chunkId, const TString& value) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Putting key with group " << group << " and chunkId " << chunkId << " to local table data service";
        TGuard<TMutex> guard(Mutex_);
        Data_[group][chunkId] = value;
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& group, const TString& chunkId) const override {
        YQL_CLOG(TRACE, FastMapReduce) << "Getting key with group " << group << " and chunkId " << chunkId << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        if (!Data_.contains(group) || !Data_.at(group).contains(chunkId)) {
            TMaybe<TString> emptyRes = Nothing();
            return NThreading::MakeFuture(emptyRes);
        }
        TMaybe<TString> value = Data_.at(group).at(chunkId);
        return NThreading::MakeFuture(value);
    }

    NThreading::TFuture<void> Delete(const TString& group, const TString& chunkId) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Deleting key with group " << group << " and chunkId " << chunkId << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        if (!Data_.contains(group)) {
            return NThreading::MakeFuture();
        }
        Data_[group].erase(chunkId);
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
        TGuard<TMutex> guard(Mutex_);
        ui64 keysNum = 0, dataWeight = 0;
        for (auto& [group, chunks]: Data_) {
            for (auto& [chunkId, value]: chunks) {
                ++keysNum;
                dataWeight += value.size();
            }
        }
        return NThreading::MakeFuture(TTableDataServiceStats{.KeysNum = keysNum, .DataWeight = dataWeight});
    }

    NThreading::TFuture<void> Clear() override {
        TGuard<TMutex> guard(Mutex_);
        Data_.clear();
        return NThreading::MakeFuture();
    }

private:
    std::unordered_map<TString, std::unordered_map<TString, TString>> Data_;  // Groups -> ChunkId -> Values
    TMutex Mutex_ = TMutex();
};

} // namespace

ILocalTableDataService::TPtr MakeLocalTableDataService() {
    return MakeIntrusive<TLocalTableDataService>();
}

} // namespace NYql::NFmr
