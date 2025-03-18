#include "table_data_service.h"
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>

namespace NYql::NFmr {

namespace {

class TLocalTableDataService: public ITableDataService {
public:
    TLocalTableDataService(const TLocalTableDataServiceSettings& settings): NumParts_(settings.NumParts) {
        Data_.resize(NumParts_);
    }

    NThreading::TFuture<void> Put(const TString& key, const TString& value) {
        auto& map = Data_[std::hash<TString>()(key) % NumParts_];
        auto it = map.find(key);
        if (it != map.end()) {
            return NThreading::MakeFuture();
        }
        map.insert({key, value});
        return NThreading::MakeFuture();
    }

    NThreading::TFuture<TMaybe<TString>> Get(const TString& key) {
        TMaybe<TString> value = Nothing();
        auto& map = Data_[std::hash<TString>()(key) % NumParts_];
        auto it = map.find(key);
        if (it != map.end()) {
            value = it->second;
        }
        return NThreading::MakeFuture(value);
    }

    NThreading::TFuture<void> Delete(const TString& key) {
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
};

} // namespace

ITableDataService::TPtr MakeLocalTableDataService(const TLocalTableDataServiceSettings& settings) {
    return MakeIntrusive<TLocalTableDataService>(settings);
}


} // namespace NYql::NFmr
