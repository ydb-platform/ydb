#pragma once
#include "abstract.h"

namespace NKikimr {

class TTxCompositeReader: public ITxReader {
private:
    using TBase = ITxReader;
    std::vector<std::shared_ptr<ITxReader>> Children;
    size_t Pos = 0;

    virtual bool DoPrecharge(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        AFL_VERIFY(false);
        return true;
    }

    virtual bool DoExecute(NTabletFlatExecutor::TTransactionContext& /*txc*/, const TActorContext& /*ctx*/) override {
        AFL_VERIFY(false);
        return true;
    }

    virtual bool Execute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& ctx) override;

    virtual bool GetIsFinished() const override {
        return Children.size() == Pos;
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
