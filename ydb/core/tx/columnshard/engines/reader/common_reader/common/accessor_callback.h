#pragma once
#include "script_cursor.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NDataAccessorControl {
class IDataAccessorsManager;
}

namespace NKikimr::NOlap::NReader::NCommon {

class TAccessorsRequestJob: public IAsyncJob {
private:
    const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> Manager;
    const std::shared_ptr<TDataAccessorsRequest> Request;

public:
    TAccessorsRequestJob(
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& manager, std::shared_ptr<TDataAccessorsRequest>&& request)
        : Manager(manager)
        , Request(std::move(request))
    {
        AFL_VERIFY(Manager);
        AFL_VERIFY(Request);
    }

    virtual void Start() override;
};

class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    NReader::NCommon::TFetchingScriptCursor Step;
    std::shared_ptr<NCommon::IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
    NActors::TActorId ScanActorId;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override;

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override;

public:
    TPortionAccessorFetchingSubscriber(const NReader::NCommon::TFetchingScriptCursor& step, const std::shared_ptr<NCommon::IDataSource>& source);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
