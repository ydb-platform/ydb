#include "sensor_service.h"
#include "cube.h"
#include "exporter.h"
#include "registry.h"
#include "private.h"

#include <yt/yt/core/concurrency/async_rw_lock.h>
#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>
#include <yt/yt/core/ytree/ypath_detail.h>

namespace NYT::NProfiling {

using namespace NConcurrency;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = SolomonLogger;

////////////////////////////////////////////////////////////////////////////////

class TSensorServiceImpl
    : public TYPathServiceBase
    , public TSupportsGet
{
public:
    TSensorServiceImpl(
        TString name,
        TSolomonRegistry* registry,
        TAsyncReaderWriterLock* const exporterLock)
        : Name_(std::move(name))
        , Registry_(std::move(registry))
        , ExporterLock_(exporterLock)
    { }

private:
    using TTagMap = THashMap<TString, TString>;

    const TString Name_;
    TSolomonRegistry* const Registry_;
    TAsyncReaderWriterLock* const ExporterLock_;

    struct TGetSensorOptions
    {
        std::optional<TString> Name;
        TTagMap TagMap;
        bool ReadAllProjections = false;
        bool ExportSummaryAsMax = true;
        bool SummaryAsMaxForAllTime = false;
    };

    TGetSensorOptions ParseGetSensorOptions(TReqGet* request) const
    {
        auto requestOptions = NYTree::FromProto(request->options());

        TGetSensorOptions options;
        // Set default value depending on whether we are in the sensor service root.
        bool defaultReadAllProjections;
        if (Name_) {
            THROW_ERROR_EXCEPTION_IF(requestOptions->Contains("name"),
                "Specifying \"name\" option is allowed only in requests to sensor service root");

            defaultReadAllProjections = true;
            options.Name = Name_;
        } else {
            defaultReadAllProjections = false;
            options.Name = requestOptions->Find<TString>("name");
        }

        options.ReadAllProjections = requestOptions->Get<bool>("read_all_projections", /*defaultValue*/ defaultReadAllProjections);
        options.TagMap = requestOptions->Get<TTagMap>("tags", /*defaultValue*/ TTagMap{});
        options.ExportSummaryAsMax = requestOptions->Get<bool>("export_summary_as_max", /*defaultValue*/ true);
        options.SummaryAsMaxForAllTime = requestOptions->Get<bool>("summary_as_max_for_all_time", /*defaultValue*/ false);

        return options;
    }

    void GetSelf(TReqGet* request, TRspGet* response, const TCtxGetPtr& context) override
    {
        auto options = ParseGetSensorOptions(request);
        YT_LOG_DEBUG("Received sensor value request (RequestId: %v, Name: %v, Tags: %v, ReadAllProjections: %v, ExportSummaryAsMax: %v)",
            context->GetRequestId(),
            options.Name,
            options.TagMap,
            options.ReadAllProjections,
            options.ExportSummaryAsMax);

        if (!options.Name) {
            response->set_value(BuildYsonStringFluently().Entity().ToString());
            context->Reply();
            return;
        }

        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(ExporterLock_))
            .ValueOrThrow();

        TTagList tags(options.TagMap.begin(), options.TagMap.end());
        TReadOptions readOptions{
            .SummaryPolicy = options.ExportSummaryAsMax ? ESummaryPolicy::Max : ESummaryPolicy::Default,
            .ReadAllProjections = options.ReadAllProjections,
            .SummaryAsMaxForAllTime = options.SummaryAsMaxForAllTime,
        };
        response->set_value(BuildYsonStringFluently()
            .Do([&] (TFluentAny fluent) {
                Registry_->ReadRecentSensorValues(*options.Name, tags, readOptions, fluent);
            }).ToString());
        context->Reply();
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(Get);
        return TYPathServiceBase::DoInvoke(context);
    }
};

using TSensorServiceImplPtr = TIntrusivePtr<TSensorServiceImpl>;

////////////////////////////////////////////////////////////////////////////////

class TSensorService
    : public TYPathServiceBase
    , public TSupportsList
{
public:
    TSensorService(
        TSolomonExporterConfigPtr config,
        TSolomonRegistryPtr registry,
        TSolomonExporterPtr exporter)
        : Config_(std::move(config))
        , Registry_(std::move(registry))
        , Exporter_(std::move(exporter))
        , RootSensorServiceImpl_(New<TSensorServiceImpl>(/*name*/ TString(), Registry_.Get(), &Exporter_->Lock_))
        , Root_(GetEphemeralNodeFactory(/*shouldHideAttributes*/ true)->CreateMap())
        , SensorTreeUpdateDuration_(Registry_->GetSelfProfiler().Timer("/sensor_service_tree_update_duration"))
    {
        UpdateSensorTreeExecutor_ = New<TPeriodicExecutor>(
            Exporter_->ControlQueue_->GetInvoker(),
            BIND(&TSensorService::UpdateSensorTree, MakeWeak(this)),
            Config_->UpdateSensorServiceTreePeriod);
        UpdateSensorTreeExecutor_->Start();
    }

private:
    const TSolomonExporterConfigPtr Config_;
    const TSolomonRegistryPtr Registry_;
    const TSolomonExporterPtr Exporter_;
    const TSensorServiceImplPtr RootSensorServiceImpl_;
    const IMapNodePtr Root_;

    THashMap<TString, TSensorServiceImplPtr> NameToSensorServiceImpl_;

    TEventTimer SensorTreeUpdateDuration_;
    TPeriodicExecutorPtr UpdateSensorTreeExecutor_;

    void UpdateSensorTree()
    {
        YT_LOG_DEBUG("Updating sensor service tree");

        TWallTimer timer;

        int addedSensorCount = 0;
        int malformedSensorCount = 0;
        auto sensors = Registry_->ListSensors();
        for (const auto& sensorInfo : sensors) {
            const auto& name = sensorInfo.Name;

            if (NameToSensorServiceImpl_.contains(name)) {
                continue;
            }

            auto sensorServiceImpl = New<TSensorServiceImpl>(name, Registry_.Get(), &Exporter_->Lock_);
            EmplaceOrCrash(NameToSensorServiceImpl_, name, sensorServiceImpl);

            auto node = CreateVirtualNode(std::move(sensorServiceImpl));
            auto path = "/" + name;
            try {
                ForceYPath(Root_, path);
                SetNodeByYPath(Root_, path, node);
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Failed to add new sensor to the sensor service tree (Name: %v)", name);

                // Ignore sensors with weird names.
                ++malformedSensorCount;
                continue;
            }

            ++addedSensorCount;
        }

        auto elapsed = timer.GetElapsedTime();
        SensorTreeUpdateDuration_.Record(elapsed);

        YT_LOG_DEBUG(
            "Finished updating sensor service tree"
            "(TotalSensorCount: %v, AddedSensorCount: %v, MalformedSensorCount: %v, Elapsed: %v)",
            sensors.size(),
            addedSensorCount,
            malformedSensorCount,
            elapsed);
    }

    IYPathService::TResolveResult ResolveSelf(
        const TYPath& path,
        const IYPathServiceContextPtr& context) override
    {
        if (context->GetMethod() == "List") {
            return TResolveResultHere{path};
        }
        return TResolveResultThere{RootSensorServiceImpl_, path};
    }

    IYPathService::TResolveResult ResolveRecursive(
        const TYPath& path,
        const IYPathServiceContextPtr& /*context*/) override
    {
        return TResolveResultThere{Root_, "/" + path};
    }

    bool DoInvoke(const IYPathServiceContextPtr& context) override
    {
        DISPATCH_YPATH_SERVICE_METHOD(List);
        return TYPathServiceBase::DoInvoke(context);
    }

    void ListSelf(TReqList* request, TRspList* response, const TCtxListPtr& context) override
    {
        auto guard = WaitFor(TAsyncLockReaderGuard::Acquire(&Exporter_->Lock_))
            .ValueOrThrow();

        auto attributeKeys = NYT::FromProto<THashSet<TString>>(request->attributes().keys());
        context->SetRequestInfo("AttributeKeys: %v", attributeKeys);

        response->set_value(BuildYsonStringFluently()
            .DoListFor(Registry_->ListSensors(), [&] (TFluentList fluent, const TSensorInfo& sensorInfo) {
                if (!sensorInfo.Error.IsOK()) {
                    THROW_ERROR_EXCEPTION("Broken sensor")
                        << TErrorAttribute("name", sensorInfo.Name)
                        << sensorInfo.Error;
                }

                fluent
                    .Item().Do([&] (TFluentAny fluent) {
                        if (!attributeKeys.empty()) {
                            fluent
                                .BeginAttributes()
                                .DoIf(attributeKeys.contains("cube_size"), [&] (TFluentMap fluent) {
                                    fluent.Item("cube_size").Value(sensorInfo.CubeSize);
                                })
                                .DoIf(attributeKeys.contains("object_count"), [&] (TFluentMap fluent) {
                                    fluent.Item("object_count").Value(sensorInfo.CubeSize);
                                })
                                .EndAttributes();
                        }

                        fluent.Value(sensorInfo.Name);
                    });
            }).ToString());

        context->Reply();
    }
};

////////////////////////////////////////////////////////////////////////////////

IYPathServicePtr CreateSensorService(
    TSolomonExporterConfigPtr config,
    TSolomonRegistryPtr registry,
    TSolomonExporterPtr exporter)
{
    return New<TSensorService>(
        std::move(config),
        std::move(registry),
        std::move(exporter));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
