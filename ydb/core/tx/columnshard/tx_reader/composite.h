#pragma once
#include "abstract.h"

namespace NKikimr {

class TTxCompositeReader: public ITxReader {
private:
    using TBase = ITxReader;
    std::vector<std::shared_ptr<ITxReader>> Children;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        return true;
    }

    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override {
        for (auto&& i : Children) {
            if (!i->Execute(txc, ctx)) {
                return false;
            }
        }
        return true;
    }

public:
    using TBase::TBase;

    void AddChildren(const std::shared_ptr<ITxReader>& reader) {
        AFL_VERIFY(!GetIsStarted());
        reader->AddNamePrefix(GetStageName() + "/");
        Children.emplace_back(reader);
    }
};

}   // namespace NKikimr
