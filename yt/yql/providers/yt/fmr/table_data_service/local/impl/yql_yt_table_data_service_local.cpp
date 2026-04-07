#include "yql_yt_table_data_service_local.h"
#include <util/string/join.h>
#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TLocalTableDataService: public ILocalTableDataService {
public:
    TLocalTableDataService(const TTableDataServiceSettings& settings) : MaxDataWeight_(settings.MaxDataWeight)
    {
    }

    NThreading::TFuture<bool> Put(const TString& group, const TString& chunkId, const TString& value) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Putting key with group " << group << " and chunkId " << chunkId << " to local table data service";
        TGuard<TMutex> guard(Mutex_);
        if (CurDataWeight_ + value.size() > MaxDataWeight_) {
            YQL_CLOG(ERROR, FastMapReduce) << " Failed to put key with group " << group << " and chunkId " << chunkId << " to local table data service - max data weight" << MaxDataWeight_ << "exceeded";
            return NThreading::MakeFuture(false);
        }
        Data_[group][chunkId] = value;
        CurDataWeight_ += value.size();
        return NThreading::MakeFuture(true);
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& group, const TString& chunkId) const override {
        YQL_CLOG(TRACE, FastMapReduce) << "Getting key with group " << group << " and chunkId " << chunkId << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        if (!Data_.contains(group) || !Data_.at(group).contains(chunkId)) {
            TMaybe<TString> emptyRes = Nothing();
            YQL_CLOG(ERROR, FastMapReduce) << " Failed to get key with group " << group << " and chunkId " << chunkId;
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
        if (Data_[group].contains(chunkId)) {
            CurDataWeight_ -= Data_[group][chunkId].size();
        }
        Data_[group].erase(chunkId);

        return NThreading::MakeFuture();
    }

    NThreading::TFuture<void> RegisterDeletion(const std::vector<TString>& groupsToDelete) override {
        YQL_CLOG(TRACE, FastMapReduce) << "Registering deletion for groups " << JoinRange(' ', groupsToDelete.begin(), groupsToDelete.end()) << " from local table data service";
        TGuard<TMutex> guard(Mutex_);
        for (auto& group: groupsToDelete) {
            if (Data_.contains(group)) {
                for (auto& [chunkId, value]: Data_[group]) {
                    CurDataWeight_ -= value.size();
                }
            }
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
        CurDataWeight_ = 0;
        return NThreading::MakeFuture();
    }

private:
    std::unordered_map<TString, std::unordered_map<TString, TString>> Data_;  // Groups -> ChunkId -> Values
    TMutex Mutex_ = TMutex();
    ui64 MaxDataWeight_;
    ui64 CurDataWeight_ = 0;
};

} // namespace

ILocalTableDataService::TPtr MakeLocalTableDataService(const TTableDataServiceSettings& settings) {
    return MakeIntrusive<TLocalTableDataService>(settings);
}

} // namespace NYql::NFmr
