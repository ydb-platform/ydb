#include "yql_yt_table_data_service_local.h"
#include <util/system/mutex.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>
#include <yql/essentials/utils/log/log.h>

namespace NYql::NFmr {

namespace {

class TLocalTableDataService: public ITableDataService {
public:
    TLocalTableDataService(const TLocalTableDataServiceSettings& settings): NumParts_(settings.NumParts) {
        Data_.resize(NumParts_);
    }

    NThreading::TFuture<void> Put(const TString& key, const TString& value) {
        TGuard<TMutex> guard(Mutex_);
        YQL_CLOG(TRACE, FastMapReduce) << "Putting key " << key << " to local table data service";
        auto& map = Data_[std::hash<TString>()(key) % NumParts_];
        auto it = map.find(key);
        if (it != map.end()) {
            return NThreading::MakeFuture();
        }
        map.insert({key, value});
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& key) {
        TGuard<TMutex> guard(Mutex_);
        YQL_CLOG(TRACE, FastMapReduce) << "Getting key " << key << " from local table data service";
        TMaybe<TString> value = Nothing();
        auto& map = Data_[std::hash<TString>()(key) % NumParts_];
        auto it = map.find(key);
        if (it != map.end()) {
            value = it->second;
        }
        return NThreading::MakeFuture(value);
    }

    NThreading::TFuture<void> Delete(const TString& key) {
        TGuard<TMutex> guard(Mutex_);
        YQL_CLOG(TRACE, FastMapReduce) << "Deleting key " << key << " from local table data service";
        auto& map = Data_[std::hash<TString>()(key) % NumParts_];
        auto it = map.find(key);
        if (it == map.end()) {
            return NThreading::MakeFuture();
        }
        map.erase(key);
        return NThreading::MakeFuture();
    }

private:
    std::vector<std::unordered_map<TString, TString>> Data_;
    const ui32 NumParts_;
    TMutex Mutex_ = TMutex();
};

} // namespace

ITableDataService::TPtr MakeLocalTableDataService(const TLocalTableDataServiceSettings& settings) {
    return MakeIntrusive<TLocalTableDataService>(settings);
}


} // namespace NYql::NFmr
