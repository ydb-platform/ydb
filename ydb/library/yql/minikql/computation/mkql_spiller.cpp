#include "mkql_spiller.h"

#include <ydb/library/yql/dq/actors/spilling/compute_storage.h>

namespace NKikimr::NMiniKQL {

using namespace NYql::NDq;

class TSpiller: public ISpiller{
public:
    TSpiller(std::function<void()>&& wakeUpCallback) {
        TStringStream spillerName;
        spillerName << "Spiller" << "_" << static_cast<const void*>(this);
        ComputeStorage_ = MakeComputeStorage(spillerName.Str(), std::move(wakeUpCallback));
    }

    NThreading::TFuture<TKey> Put(TRope&& blob) override {

        return ComputeStorage_->Put(std::move(blob));
    }
    std::optional<NThreading::TFuture<TRope>> Get(TKey key) override {
        return ComputeStorage_->Get(key);
    }
    std::optional<NThreading::TFuture<TRope>> Extract(TKey key) override {
        return ComputeStorage_->Extract(key);
    }
    NThreading::TFuture<void> Delete(TKey key) override {
        return ComputeStorage_->Delete(key);
    }
private:
    IDqComputeStorage::TPtr ComputeStorage_;
};
ISpiller::TPtr MakeSpiller(std::function<void()>&& wakeUpCallback) {
    return std::make_shared<TSpiller>(std::move(wakeUpCallback));
}

} //namespace NKikimr::NMiniKQL
