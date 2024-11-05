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
        TFsPath FileOutputPath;
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
    class IWriter {
    public:
        IWriter(const TWorkloadCommandImport::TUploadCommand& owner)
            : Owner(owner)
        {}
        virtual ~IWriter() = default;
        virtual TAsyncStatus WriteDataPortion(NYdbWorkload::IBulkDataGenerator::TDataPortionPtr portion) = 0;
    protected:
        const TWorkloadCommandImport::TUploadCommand& Owner;
    };
    class TFileWriter;
    class TDbWriter;
    NTable::TSession GetSession();
    int DoRun(NYdbWorkload::IWorkloadQueryGenerator& workloadGen, TConfig& config) override;
    void ProcessDataGenerator(std::shared_ptr<NYdbWorkload::IBulkDataGenerator> dataGen) noexcept;

    const TUploadParams& UploadParams;
    NYdbWorkload::TWorkloadDataInitializer::TPtr Initializer;
    THolder<TProgressBar> Bar;
    TAdaptiveLock Lock;
    THolder<TFastSemaphore> InFlightSemaphore;
    TAtomic ErrorsCount;
    THolder<IWriter> Writer;
};

}