#include "ydb_workload.h"
#include <library/cpp/threading/future/async_semaphore.h>

namespace NYdb::NConsoleClient {

class TWorkloadCommandImport final: public TClientCommandTree {
public:
    TWorkloadCommandImport(NYdbWorkload::TWorkloadParams& workloadParams, NYdbWorkload::TWorkloadDataInitializer::TList initializers);
    virtual void Config(TConfig& config) override;

private:
    struct TUploadParams {
        TUploadParams();
        ui32 Threads;
        ui32 MaxInFlight = 128;
    };
    class TUploadCommand;

    TUploadParams UploadParams;
    NYdbWorkload::TWorkloadParams& WorkloadParams;
};

class TWorkloadCommandImport::TUploadCommand final: public TWorkloadCommandBase {
public:
    TUploadCommand(NYdbWorkload::TWorkloadParams& workloadParams, const TUploadParams& uploadParams, NYdbWorkload::TWorkloadDataInitializer::TPtr initializer);
    virtual void Config(TConfig& config) override;

private:
    NTable::TSession GetSession();
    int DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) override;
    TAsyncStatus SendDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) const;
    void ProcessDataGenerator(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen) noexcept;

    const TUploadParams& UploadParams;
    NYdbWorkload::TWorkloadDataInitializer::TPtr Initializer;
    THolder<TProgressBar> Bar;
    TAdaptiveLock Lock;
    NThreading::TAsyncSemaphore::TPtr InFlightSemaphore;
    TAtomic ErrorsCount;
};

}