#include "ydb_workload.h"
#include <library/cpp/threading/future/async_semaphore.h>

namespace NYdb::NConsoleClient {

class TWorkloadCommandImport final: public TClientCommandTree {
public:
    virtual void Config(TConfig& config) override;
    static std::unique_ptr<TClientCommand> Create(NYdbWorkload::TWorkloadParams& workloadParams);

private:
    TWorkloadCommandImport(NYdbWorkload::TWorkloadParams& workloadParams, NYdbWorkload::TWorkloadDataInitializer::TList initializers);
    struct TUploadParams {
        TUploadParams(NYdbWorkload::TWorkloadParams& workloadParams);
        void Config(TConfig& config);
        ui32 Threads;
        ui32 MaxInFlight = 128;
        TFsPath FileOutputPath;
        NYdbWorkload::TWorkloadParams& WorkloadParams;
    };
    class TUploadCommand;

    TUploadParams UploadParams;
};

class TWorkloadCommandImport::TUploadCommand final: public TWorkloadCommandBase {
public:
    TUploadCommand(NYdbWorkload::TWorkloadParams& workloadParams, const TUploadParams* uploadParams, NYdbWorkload::TWorkloadDataInitializer::TPtr initializer);
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

    THolder<TUploadParams> OwnedUploadParams;
    const TUploadParams& UploadParams;
    NYdbWorkload::TWorkloadDataInitializer::TPtr Initializer;
    THolder<TProgressBar> Bar;
    TAdaptiveLock Lock;
    THolder<TFastSemaphore> InFlightSemaphore;
    TAtomic ErrorsCount;
    THolder<IWriter> Writer;
};

}