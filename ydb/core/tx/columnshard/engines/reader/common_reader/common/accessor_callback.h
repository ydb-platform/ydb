#pragma once
#include "script_cursor.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NDataAccessorControl {
class IDataAccessorsManager;
}

namespace NKikimr::NOlap::NReader::NCommon {

class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    NReader::NCommon::TFetchingScriptCursor Step;
    std::unique_ptr<TDataSourceLease> SourceLease;
    const NColumnShard::TCounterGuard Guard;
    NActors::TActorId ScanActorId;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override;

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override;

public:
    class TStartJob: public IAsyncJob {
    private:
        const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager> Manager;
        const std::shared_ptr<TDataAccessorsRequest> Request;
        const NReader::NCommon::TFetchingScriptCursor Step;

    public:
        TStartJob(const std::shared_ptr<NDataAccessorControl::IDataAccessorsManager>& manager, std::shared_ptr<TDataAccessorsRequest>&& request,
            const NReader::NCommon::TFetchingScriptCursor& step)
            : Manager(manager)
            , Request(std::move(request))
            , Step(step)
        {
            AFL_VERIFY(Manager);
            AFL_VERIFY(Request);
        }

        virtual void Start(std::unique_ptr<TDataSourceLease> sourceLease) override;
    };

    TPortionAccessorFetchingSubscriber(const NReader::NCommon::TFetchingScriptCursor& step, std::unique_ptr<TDataSourceLease> sourceLease);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
