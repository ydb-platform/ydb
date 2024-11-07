#pragma once

namespace NKikimr::NOlap::NDataAccessorControl {
class IGranuleDataAccessor {
private:
    const ui64 PathId;

    virtual void DoAskData(const std::shared_ptr<TDataAccessorsRequest>& request) = 0;
    virtual void DoModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) = 0;

public:
    virtual ~IGranuleDataAccessor() = default;

    ui64 GetPathId() const {
        return PathId;
    }

    IGranuleDataAccessor(const ui64 pathId)
        : PathId(pathId) {
    }

    void AskData(const std::shared_ptr<TDataAccessorsRequest>& request) {
        AFL_VERIFY(request);
        AFL_VERIFY(request->HasSubscriber());
        return DoAskData(request);
    }
    void ModifyPortions(const std::vector<TPortionDataAccessor>& add, const std::vector<ui64>& remove) {
        return DoModifyPortions(add, remove);
    }
};

}   // namespace NKikimr::NOlap::NDataAccessorControl
