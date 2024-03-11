#pragma once

#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/task_runner/tasks_runner_pipe.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io_factory.h>

#include <ydb/library/yql/utils/log/proto/logger_config.pb.h>

#include <yt/cpp/mapreduce/interface/client.h>

namespace NYql::NDq::NWorker {
    struct IWorkerConfigurator
    {
        virtual ~IWorkerConfigurator() = default;

        virtual void ConfigureMetrics(const THolder<NYql::NProto::TLoggingConfig>& loggerConfig, const THolder<NActors::TActorSystem>& actorSystem, const NProto::TDqConfig::TYtBackend& backendConfig, const TResourceManagerOptions& rmOptions, ui32 nodeId) const = 0;
        virtual IDqAsyncIoFactory::TPtr CreateAsyncIoFactory() const = 0;
        virtual void OnWorkerFinish() = 0;
    };

    struct TDefaultWorkerConfigurator
        : public IWorkerConfigurator
    {
        void ConfigureMetrics(const THolder<NYql::NProto::TLoggingConfig>& /*loggerConfig*/, const THolder<NActors::TActorSystem>& /*actorSystem*/, const NProto::TDqConfig::TYtBackend& /*backendConfig*/, const TResourceManagerOptions& /*rmOptions*/, ui32 /*nodeId*/) const override;
        IDqAsyncIoFactory::TPtr CreateAsyncIoFactory() const override;
        void OnWorkerFinish() override;
    };

    class TWorkerJob: public NYT::IVanillaJob<void> {
    public:
        TWorkerJob();

        void SetConfigFile(const TString& configFile);
        void SetWorkerConfigurator(THolder<IWorkerConfigurator> workerConfigurator);

        void Do() override;

    private:
        TString ConfigFile;
        THolder<IWorkerConfigurator> WorkerConfigurator;
    };

} // namespace NYql::NDq::NWorker
