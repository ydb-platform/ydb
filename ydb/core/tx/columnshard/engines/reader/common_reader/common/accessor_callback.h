#pragma once
#include "script_cursor.h"

#include <ydb/core/tx/columnshard/data_accessor/request.h>

namespace NKikimr::NOlap::NReader::NCommon {

class TPortionAccessorFetchingSubscriber: public IDataAccessorRequestsSubscriber {
private:
    NReader::NCommon::TFetchingScriptCursor Step;
    std::shared_ptr<NCommon::IDataSource> Source;
    const NColumnShard::TCounterGuard Guard;
    ui64 ConveyorProcessId;
    NActors::TActorId ScanActorId;
    virtual const std::shared_ptr<const TAtomicCounter>& DoGetAbortionFlag() const override;

    virtual void DoOnRequestsFinished(TDataAccessorsResult&& result) override;

public:
    TPortionAccessorFetchingSubscriber(const NReader::NCommon::TFetchingScriptCursor& step, const std::shared_ptr<NCommon::IDataSource>& source);
};

}   // namespace NKikimr::NOlap::NReader::NCommon
