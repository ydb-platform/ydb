#include "jaeger_tracing_configurator.h"
#include "library/cpp/testing/unittest/registar.h"
#include "ut_helpers.h"
#include "util/generic/ptr.h"
#include "ydb/core/jaeger_tracing/request_discriminator.h"

namespace NKikimr {

using namespace NConsole;
using namespace NUT;
using namespace NJaegerTracing;

namespace {

TTenantTestConfig::TTenantPoolConfig StaticTenantPoolConfig() {
    TTenantTestConfig::TTenantPoolConfig res = {
        // Static slots {tenant, {cpu, memory, network}}
        {{ {DOMAIN1_NAME, {1, 1, 1}} }},
        // NodeType
        "type1"
    };
    return res;
}

TTenantTestConfig DefaultConsoleTestConfig() {
    TTenantTestConfig res = {
        // Domains {name, schemeshard {{ subdomain_names }}}
        {{ {DOMAIN1_NAME, SCHEME_SHARD1_ID, TVector<TString>()} }},
        // HiveId
        HIVE_ID,
        // FakeTenantSlotBroker
        true,
        // FakeSchemeShard
        false,
        // CreateConsole
        true,
        // Nodes {tenant_pool_config, data_center}
        {{
            {StaticTenantPoolConfig()},
        }},
        // DataCenterCount
        1,
        // CreateConfigsDispatcher
        true
    };
    return res;
}

void InitJaegerTracingConfigurator(
    TTenantTestRuntime& runtime,
    TSamplingThrottlingConfigurator configurator,
    const NKikimrConfig::TTracingConfig& initCfg
) {
    runtime.Register(CreateJaegerTracingConfigurator(std::move(configurator), initCfg));

    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvConfigsDispatcher::EvSetConfigSubscriptionResponse, 1);
    runtime.DispatchEvents(std::move(options));
}

void WaitForUpdate(TTenantTestRuntime& runtime) {
    TDispatchOptions options;
    options.FinalEvents.emplace_back(TEvConsole::EvConfigNotificationResponse, 1);
    runtime.DispatchEvents(std::move(options));
}

void CofigureAndWaitUpdate(TTenantTestRuntime& runtime, const NKikimrConfig::TTracingConfig& cfg, ui32 order) {
    auto configItem = MakeConfigItem(NKikimrConsole::TConfigItem::TracingConfigItem,
                                     NKikimrConfig::TAppConfig(), {}, {}, "", "", order,
                                     NKikimrConsole::TConfigItem::OVERWRITE, "");
    configItem.MutableConfig()->MutableTracingConfig()->CopyFrom(cfg);

    auto* event = new TEvConsole::TEvConfigureRequest;
    event->Record.AddActions()->CopyFrom(MakeAddAction(configItem));

    runtime.SendToConsole(event);
    WaitForUpdate(runtime);
}

class TTracingControls {
public:
    enum ETraceState {
        OFF,
        SAMPLED,
        EXTERNAL,
    };

    TTracingControls(
        TVector<TIntrusivePtr<TSamplingThrottlingControl>> controls,
        TIntrusivePtr<IRandomProvider>& rng
    )
        : Controls(std::move(controls))
        , Rng(rng->GenRand64())
    {}

    std::pair<ETraceState, ui8> HandleTracing(bool isExternal, TRequestDiscriminator discriminator) {
        auto& control = Controls[Rng.GenRand64() % Controls.size()];
        
        NWilson::TTraceId traceId;
        if (isExternal) {
            traceId = NWilson::TTraceId::NewTraceId(TComponentTracingLevels::ProductionVerbose, Max<ui32>());
        }
        auto before = traceId.Clone();

        control->HandleTracing(traceId, discriminator);
        if (!traceId) {
            return {OFF, 0};
        }

        ETraceState state;
        if (traceId == before) {
            state = ETraceState::EXTERNAL;
        } else {
            state = ETraceState::SAMPLED;
        }

        return {state, traceId.GetVerbosity()};
    }

private:
    TVector<TIntrusivePtr<TSamplingThrottlingControl>> Controls;
    TReallyFastRng32 Rng;
};

std::pair<TTracingControls, TSamplingThrottlingConfigurator>
    CreateSamplingThrottlingConfigurator(size_t n, TIntrusivePtr<ITimeProvider> timeProvider) {
    auto randomProvider = CreateDefaultRandomProvider();
    TSamplingThrottlingConfigurator configurator(timeProvider, randomProvider);
    TVector<TIntrusivePtr<TSamplingThrottlingControl>> controls;
    for (size_t i = 0; i < n; ++i) {
        controls.emplace_back(configurator.GetControl());
    }

    return {TTracingControls(std::move(controls), randomProvider), std::move(configurator)};
}

struct TTimeProviderMock : public ITimeProvider {
    TTimeProviderMock(TInstant now) : Now_(now) {}

    TInstant Now() override {
        return Now_;
    }

    void Advance(TDuration delta) {
        Now_ += delta;
    }

    TInstant Now_;
};

} // namespace anonymous

Y_UNIT_TEST_SUITE(TJaegerTracingConfiguratorTests) {
    Y_UNIT_TEST(DefaultConfig) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        InitJaegerTracingConfigurator(runtime, std::move(configurator), {});

        for (size_t i = 0; i < 100; ++i) {
            auto [state, _] = controls.HandleTracing(false, {});
            UNIT_ASSERT_EQUAL(state, TTracingControls::OFF); // No requests are sampled
        }

        for (size_t i = 0; i < 100; ++i) {
            auto [state, level] = controls.HandleTracing(true, {});
            UNIT_ASSERT_EQUAL(state, TTracingControls::EXTERNAL);
            UNIT_ASSERT_EQUAL(level, TComponentTracingLevels::ProductionVerbose); // All external traces are accepted
        }
        WaitForUpdate(runtime); // Initial update
    }

    Y_UNIT_TEST(GlobalRules) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddExternalThrottling();
            rule->SetMaxTracesBurst(0);
            rule->SetMaxTracesPerMinute(60);
        }
        {
            auto rule = cfg.AddSampling();
            rule->SetFraction(1. / 3);
            rule->SetLevel(5);
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(30);
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        TVector<TRequestDiscriminator> discriminators = {
            // {
            //     .RequestType = ERequestType::TABLE_READROWS,
            //     .Database = "/Root/test3",
            // },
            {
                .RequestType = ERequestType::KEYVALUE_READ,
            },
            // {
            //     .Database = "/Root/test2",
            // },
            // {},
        };

        {
            size_t sampled = 0;
            size_t traced = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(true, discriminators[i % discriminators.size()]);

                switch (state) {
                case TTracingControls::OFF:
                    break;
                case TTracingControls::SAMPLED:
                    UNIT_ASSERT_EQUAL(level, 5);
                    ++sampled;
                    break;
                case TTracingControls::EXTERNAL:
                    ++traced;
                    break;
                }
                timeProvider->Advance(TDuration::MilliSeconds(250));
            }
            UNIT_ASSERT_EQUAL(traced, 250);
            UNIT_ASSERT(sampled >= 110 && sampled <= 135);
        }
        timeProvider->Advance(TDuration::Minutes(1));

        {
            for (size_t i = 0; i < 100; ++i) {
                auto [state, _] = controls.HandleTracing(true, discriminators[i % discriminators.size()]);
                UNIT_ASSERT_EQUAL(state, TTracingControls::EXTERNAL);
                timeProvider->Advance(TDuration::Seconds(1));
            }
        }
        timeProvider->Advance(TDuration::Minutes(1));

        {
            size_t sampled = 0;
            for (size_t i = 0; i < 750; ++i) {
                auto [state, level] = controls.HandleTracing(false, discriminators[i % discriminators.size()]);
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    ++sampled;
                    UNIT_ASSERT_EQUAL(level, 5);
                }
                timeProvider->Advance(TDuration::Seconds(1));
            }
            UNIT_ASSERT(sampled >= 210 && sampled <= 300);
        }
    }

    Y_UNIT_TEST(RequestTypeThrottler) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddExternalThrottling();
            rule->SetMaxTracesBurst(5);
            rule->SetMaxTracesPerMinute(120);
            rule->MutableScope()->AddRequestTypes()->assign("KeyValue.ExecuteTransaction");
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        for (size_t i = 0; i < 100; ++i) {
            auto [state, _] = controls.HandleTracing(false, {});
            UNIT_ASSERT_EQUAL(state, TTracingControls::OFF); // No requests are sampled
        }

        UNIT_ASSERT_EQUAL(controls.HandleTracing(true, {}).first, TTracingControls::OFF); // No request type
        UNIT_ASSERT_EQUAL(controls.HandleTracing(true, {.RequestType = ERequestType::KEYVALUE_READ}).first,
                          TTracingControls::OFF); // Wrong request type
        TRequestDiscriminator executeTransactionDiscriminators[] = {
            {
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
            },
            {
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
                .Database = "/Root/test",
            }
        };

        for (size_t i = 0; i < 6; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, executeTransactionDiscriminators[i % 2]).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, executeTransactionDiscriminators[0]).first,
            TTracingControls::OFF);
        timeProvider->Advance(TDuration::MilliSeconds(1500));
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, executeTransactionDiscriminators[(i + 1) & 1]).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, executeTransactionDiscriminators[1]).first,
            TTracingControls::OFF);

        WaitForUpdate(runtime); // Initial update
        cfg.MutableExternalThrottling(0)->SetMaxTracesPerMinute(10);
        cfg.MutableExternalThrottling(0)->SetMaxTracesBurst(2);
        CofigureAndWaitUpdate(runtime, cfg, 1);

        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, executeTransactionDiscriminators[i & 1]).first,
                TTracingControls::EXTERNAL);
        }
        auto [state, _] = controls.HandleTracing(true, executeTransactionDiscriminators[1]);
        UNIT_ASSERT_EQUAL(
            state,
            TTracingControls::OFF);

        timeProvider->Advance(TDuration::Seconds(12));
        for (size_t i = 0; i < 2; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, executeTransactionDiscriminators[i & 1]).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, executeTransactionDiscriminators[0]).first,
            TTracingControls::OFF);

        timeProvider->Advance(TDuration::Seconds(60));
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, executeTransactionDiscriminators[i & 1]).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, executeTransactionDiscriminators[1]).first,
            TTracingControls::OFF);
    }

    Y_UNIT_TEST(RequestTypeSampler) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddSampling();
            rule->SetMaxTracesBurst(5);
            rule->SetMaxTracesPerMinute(120);
            rule->SetFraction(0.5);
            rule->SetLevel(10);
            rule->MutableScope()->AddRequestTypes()->assign("KeyValue.ExecuteTransaction");
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        for (size_t i = 0; i < 1000; ++i) {
            auto [state, level] = controls.HandleTracing(false, {});
            UNIT_ASSERT_EQUAL(state, TTracingControls::OFF);
        }

        for (size_t i = 0; i < 10; ++i) {
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, {}).first, TTracingControls::OFF); // No request type
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, {.RequestType = ERequestType::KEYVALUE_READ}).first,
                              TTracingControls::OFF); // Wrong request type
        }
        TRequestDiscriminator executeTransactionDiscriminators[] = {
            {
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
            },
            {
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
                .Database = "/Root/test",
            }
        };

        {
            uint64_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, executeTransactionDiscriminators[i % 2]);
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    ++sampled;
                    UNIT_ASSERT_EQUAL(level, 10);
                    timeProvider->Advance(TDuration::MilliSeconds(500));
                }
            }
            UNIT_ASSERT(sampled >= 400 && sampled <= 600);
        }

        {
            uint64_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, executeTransactionDiscriminators[i % 2]);
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    ++sampled;
                    UNIT_ASSERT_EQUAL(level, 10);
                }
                timeProvider->Advance(TDuration::MilliSeconds(125));
            }
            UNIT_ASSERT(sampled >= 190 && sampled <= 260);
        }
        for (size_t i = 0; i < 50; ++i) {
            controls.HandleTracing(false, executeTransactionDiscriminators[i % 2]);
        }
        for (size_t i = 0; i < 50; ++i) {
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, executeTransactionDiscriminators[i % 2]).first, TTracingControls::OFF);
        }
        timeProvider->Advance(TDuration::Seconds(10));

        WaitForUpdate(runtime); // Initial update
        {
            auto& rule = *cfg.MutableSampling(0);
            rule.SetMaxTracesPerMinute(10);
            rule.SetMaxTracesBurst(2);
            rule.SetLevel(9);
            rule.SetFraction(0.25);
            rule.MutableScope()->MutableRequestTypes(0)->assign("KeyValue.ReadRange");
        }
        CofigureAndWaitUpdate(runtime, cfg, 1);

        TRequestDiscriminator readRangeDiscriminators[] = {
            {
                .RequestType = ERequestType::KEYVALUE_READRANGE,
            },
            {
                .RequestType = ERequestType::KEYVALUE_READRANGE,
                .Database = "/Root/test2",
            }
        };

        for (size_t i = 0; i < 20; ++i) {
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, executeTransactionDiscriminators[i & 1]).first, TTracingControls::OFF);
        }
        {
            uint64_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, readRangeDiscriminators[i % 2]);
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    ++sampled;
                    UNIT_ASSERT_EQUAL(level, 9);
                }
                timeProvider->Advance(TDuration::Seconds(6));
            }
            UNIT_ASSERT(sampled >= 190 && sampled <= 310);
        }
    }

    Y_UNIT_TEST(SamplingSameScope) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddSampling();
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(120);
            rule->SetFraction(0.5);
            rule->SetLevel(8);
        }
        {
            auto rule = cfg.AddSampling();
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(60);
            rule->SetFraction(1. / 3);
            rule->SetLevel(10);
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        {
            size_t level8 = 0;
            size_t level10 = 0;
            for (size_t i = 0; i < 1500; ++i) {
                auto [state, level] = controls.HandleTracing(false, {});
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT(level == 8 || level == 10);
                    if (level == 8) {
                        ++level8;
                    } else {
                        ++level10;
                    }
                }
                timeProvider->Advance(TDuration::Seconds(1));
            }
            UNIT_ASSERT(level8 >= 450 && level8 <= 570);
            UNIT_ASSERT(level10 >= 450 && level10 <= 570);
        }
        timeProvider->Advance(TDuration::Minutes(1));

        {
            size_t level8 = 0;
            size_t level10 = 0;
            for (size_t i = 0; i < 1500; ++i) {
                auto [state, level] = controls.HandleTracing(false, {});
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT(level == 8 || level == 10);
                    if (level == 8) {
                        ++level8;
                    } else {
                        ++level10;
                    }
                }
                timeProvider->Advance(TDuration::MilliSeconds(250));
            }
            // level8 <= 750
            // level10 <= 375
            Cerr << "Level8: " << level8 << Endl;
            Cerr << "Level10: " << level10 << Endl;
            UNIT_ASSERT(level8 >= 470 && level8 <= 760);
            UNIT_ASSERT(level10 >= 340 && level10 <= 385);
        }
    }
}

} // namespace NKikimr
