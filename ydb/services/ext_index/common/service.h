#pragma once
#include "events.h"

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <shared_mutex>

namespace NKikimr::NCSIndex {

class IDataUpsertController {
public:
    using TPtr = std::shared_ptr<IDataUpsertController>;
    virtual ~IDataUpsertController() = default;
    virtual void OnAllIndexesUpserted() = 0;
    virtual void OnAllIndexesUpsertionFailed(const TString& errorMessage) = 0;
};

class TEvAddData: public NActors::TEventLocal<TEvAddData, EEvents::EvAddData> {
private:
    YDB_READONLY_DEF(std::shared_ptr<arrow::RecordBatch>, Data);
    YDB_READONLY_DEF(TString, TablePath);
    YDB_READONLY_DEF(IDataUpsertController::TPtr, ExternalController);
public:
    TEvAddData(std::shared_ptr<arrow::RecordBatch> data, const TString& tablePath, IDataUpsertController::TPtr externalController)
        : Data(data)
        , TablePath(tablePath)
        , ExternalController(externalController)
    {

    }
};

class TEvAddDataResult: public NActors::TEventLocal<TEvAddDataResult, EEvents::EvAddDataResult> {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
public:
    TEvAddDataResult() = default;

    TEvAddDataResult(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

class TNaiveDataUpsertController: public IDataUpsertController {
private:
    const TActorIdentity OwnerId;
public:
    TNaiveDataUpsertController(const TActorIdentity& ownerId)
        : OwnerId(ownerId) {

    }
    virtual void OnAllIndexesUpserted() override {
        OwnerId.Send(OwnerId, new TEvAddDataResult());
    }
    virtual void OnAllIndexesUpsertionFailed(const TString& errorMessage) override {
        OwnerId.Send(OwnerId, new TEvAddDataResult(errorMessage));
    }
};

NActors::TActorId MakeServiceId(const ui32 node);

class TConfig;

class TServiceOperator {
private:
    friend class TExecutor;
    std::shared_mutex Lock;
    bool EnabledFlag = false;
    static void Register(const TConfig& config);
public:
    static bool IsEnabled();
};

}
