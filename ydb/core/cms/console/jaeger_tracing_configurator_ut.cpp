#include "ut_helpers.h"
#include "jaeger_tracing_configurator.h"

#include <ydb/core/jaeger_tracing/request_discriminator.h>

#include <util/generic/ptr.h>
#include <util/random/random.h>

#include <library/cpp/testing/unittest/registar.h>

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

void ConfigureAndWaitUpdate(TTenantTestRuntime& runtime, const NKikimrConfig::TTracingConfig& cfg, ui32 order) {
    auto configItem = MakeConfigItem(NKikimrConsole::TConfigItem::TracingConfigItem,
                                     NKikimrConfig::TAppConfig(), {}, {}, "", "", order,
                                     NKikimrConsole::TConfigItem::OVERWRITE, "");
    configItem.MutableConfig()->MutableTracingConfig()->CopyFrom(cfg);

    auto* event = new TEvConsole::TEvConfigureRequest;
    event->Record.AddActions()->CopyFrom(MakeAddAction(configItem));

    runtime.SendToConsole(event);
    WaitForUpdate(runtime);
}

auto& RandomChoice(auto& Container) {
    return Container[RandomNumber<size_t>() % Container.size()];
}

class TTracingControls {
public:
    enum ETraceState {
        OFF,
        SAMPLED,
        EXTERNAL,
    };

    TTracingControls(TVector<TIntrusivePtr<TSamplingThrottlingControl>> controls)
        : Controls(std::move(controls))
    {}

    std::pair<ETraceState, ui8> HandleTracing(bool isExternal, TRequestDiscriminator discriminator) {
        auto& control = RandomChoice(Controls);
        
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
};

std::pair<TTracingControls, TSamplingThrottlingConfigurator>
    CreateSamplingThrottlingConfigurator(size_t n, TIntrusivePtr<ITimeProvider> timeProvider) {
    auto randomProvider = CreateDefaultRandomProvider();
    TSamplingThrottlingConfigurator configurator(timeProvider, randomProvider);
    TVector<TIntrusivePtr<TSamplingThrottlingControl>> controls;
    for (size_t i = 0; i < n; ++i) {
        controls.emplace_back(configurator.GetControl());
    }

    return {TTracingControls(std::move(controls)), std::move(configurator)};
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
            auto [state, _] = controls.HandleTracing(true, {});
            UNIT_ASSERT_EQUAL(state, TTracingControls::OFF); // No request with trace-id are traced
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

        std::array discriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_READROWS,
                .Database = "/Root/test3",
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::KEYVALUE_READ,
            },
            TRequestDiscriminator{
                .Database = "/Root/test2",
            },
            TRequestDiscriminator{},
        };

        {
            size_t sampled = 0;
            size_t traced = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(true, RandomChoice(discriminators));

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
                auto [state, _] = controls.HandleTracing(true, RandomChoice(discriminators));
                UNIT_ASSERT_EQUAL(state, TTracingControls::EXTERNAL);
                timeProvider->Advance(TDuration::Seconds(1));
            }
        }
        timeProvider->Advance(TDuration::Minutes(1));

        {
            size_t sampled = 0;
            for (size_t i = 0; i < 750; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(discriminators));
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
        std::array executeTransactionDiscriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
                .Database = "/Root/test",
            }
        };

        for (size_t i = 0; i < 6; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
            TTracingControls::OFF);
        timeProvider->Advance(TDuration::MilliSeconds(1500));
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
            TTracingControls::OFF);

        WaitForUpdate(runtime); // Initial update
        cfg.MutableExternalThrottling(0)->SetMaxTracesPerMinute(10);
        cfg.MutableExternalThrottling(0)->SetMaxTracesBurst(2);
        ConfigureAndWaitUpdate(runtime, cfg, 1);

        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
                TTracingControls::EXTERNAL);
        }
        auto [state, _] = controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators));
        UNIT_ASSERT_EQUAL(
            state,
            TTracingControls::OFF);

        timeProvider->Advance(TDuration::Seconds(12));
        for (size_t i = 0; i < 2; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
            TTracingControls::OFF);

        timeProvider->Advance(TDuration::Seconds(60));
        for (size_t i = 0; i < 3; ++i) {
            UNIT_ASSERT_EQUAL(
                controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
                TTracingControls::EXTERNAL);
        }
        UNIT_ASSERT_EQUAL(
            controls.HandleTracing(true, RandomChoice(executeTransactionDiscriminators)).first,
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
        std::array executeTransactionDiscriminators{
            TRequestDiscriminator {
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
            },
            TRequestDiscriminator {
                .RequestType = ERequestType::KEYVALUE_EXECUTETRANSACTION,
                .Database = "/Root/test",
            }
        };

        {
            uint64_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(executeTransactionDiscriminators));
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
                auto [state, level] = controls.HandleTracing(false, RandomChoice(executeTransactionDiscriminators));
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
            controls.HandleTracing(false, RandomChoice(executeTransactionDiscriminators));
        }
        for (size_t i = 0; i < 50; ++i) {
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, RandomChoice(executeTransactionDiscriminators)).first, TTracingControls::OFF);
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
        ConfigureAndWaitUpdate(runtime, cfg, 1);

        std::array readRangeDiscriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::KEYVALUE_READRANGE,
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::KEYVALUE_READRANGE,
                .Database = "/Root/test2",
            }
        };

        for (size_t i = 0; i < 20; ++i) {
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, RandomChoice(executeTransactionDiscriminators)).first, TTracingControls::OFF);
        }
        {
            uint64_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(readRangeDiscriminators));
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
            UNIT_ASSERT(level8 >= 470 && level8 <= 760);
            UNIT_ASSERT(level10 >= 340 && level10 <= 385);
        }
    }

    Y_UNIT_TEST(ThrottlingByDb) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddExternalThrottling();
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(60);
            rule->MutableScope()->MutableDatabase()->assign("/Root/db1");
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        std::array discriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_READROWS,
                .Database = "/Root/db1",
            },
            TRequestDiscriminator{
                .Database = "/Root/db1",
            },
        };

        {
            size_t traced = 0;
            for (size_t i = 0; i < 100; ++i) {
                auto [state, _] = controls.HandleTracing(true, RandomChoice(discriminators));
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::SAMPLED);
                if (state == TTracingControls::EXTERNAL) {
                    ++traced;
                }
                timeProvider->Advance(TDuration::Seconds(1));
            }
            UNIT_ASSERT_EQUAL(traced, 100);

            for (size_t i = 0; i < 12; ++i) {
                auto [state, _] = controls.HandleTracing(true, RandomChoice(discriminators));
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::SAMPLED);
                if (state == TTracingControls::EXTERNAL) {
                    ++traced;
                }
            }
            UNIT_ASSERT_EQUAL(traced, 111);
        }

        cfg.MutableExternalThrottling(0)->MutableScope()->AddRequestTypes()->assign("Table.ReadRows");
        WaitForUpdate(runtime); // Initial update
        ConfigureAndWaitUpdate(runtime, cfg, 1);
        timeProvider->Advance(TDuration::Minutes(1));

        {
            size_t traced = 0;
            for (size_t i = 0; i < 12; ++i) {
                auto [state, _] = controls.HandleTracing(true, discriminators[0]);
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::SAMPLED);
                if (state == TTracingControls::EXTERNAL) {
                    ++traced;
                }
            }
            UNIT_ASSERT_EQUAL(traced, 11);
            timeProvider->Advance(TDuration::Minutes(1));

            std::array notMatchingDiscriminators{
                discriminators[1],
                TRequestDiscriminator{
                    .RequestType = ERequestType::TABLE_DROPTABLE,
                    .Database = "/Root/db1",
                },
                TRequestDiscriminator{
                    .RequestType = ERequestType::TABLE_READROWS,
                    .Database = "/Root/db2",
                },
                TRequestDiscriminator{
                    .RequestType = ERequestType::TABLE_READROWS,
                },
                TRequestDiscriminator{
                    .Database = "/Root/db1",
                },
                TRequestDiscriminator{},
            };

            for (auto& discriminator : notMatchingDiscriminators) {
                UNIT_ASSERT_EQUAL(controls.HandleTracing(true, discriminator).first, TTracingControls::OFF);
                timeProvider->Advance(TDuration::Seconds(1));
            }
        }
    }

    Y_UNIT_TEST(SamplingByDb) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddSampling();
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(60);
            rule->SetLevel(0);
            rule->SetFraction(0.5);
            rule->MutableScope()->MutableDatabase()->assign("/Root/db1");
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        std::array discriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_READROWS,
                .Database = "/Root/db1",
            },
            TRequestDiscriminator{
                .Database = "/Root/db1",
            },
        };

        {
            size_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(discriminators));
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT_EQUAL(level, 0);
                    ++sampled;
                }
                timeProvider->Advance(TDuration::Seconds(1));
            }
            UNIT_ASSERT(sampled >= 400 && sampled <= 600);

        }
        {
            size_t sampled = 0;
            for (size_t i = 0; i < 60; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(discriminators));
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT_EQUAL(level, 0);
                    ++sampled;
                }
            }
            UNIT_ASSERT_EQUAL(sampled, 11);
        }

        cfg.MutableSampling(0)->MutableScope()->AddRequestTypes()->assign("Table.ReadRows");
        WaitForUpdate(runtime); // Initial update
        ConfigureAndWaitUpdate(runtime, cfg, 1);
        timeProvider->Advance(TDuration::Minutes(1));

        {
            size_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, discriminators[0]);
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT_EQUAL(level, 0);
                    ++sampled;
                }
                timeProvider->Advance(TDuration::Seconds(1));
            }
            UNIT_ASSERT(sampled >= 400 && sampled <= 600);
            timeProvider->Advance(TDuration::Minutes(1));

            std::array notMatchingDiscriminators{
                discriminators[1],
                TRequestDiscriminator{
                    .RequestType = ERequestType::TABLE_DROPTABLE,
                    .Database = "/Root/db1",
                },
                TRequestDiscriminator{
                    .RequestType = ERequestType::TABLE_READROWS,
                    .Database = "/Root/db2",
                },
                TRequestDiscriminator{
                    .RequestType = ERequestType::TABLE_READROWS,
                },
                TRequestDiscriminator{
                    .Database = "/Root/db1",
                },
                TRequestDiscriminator{},
            };

            for (size_t i = 0; i < 10; ++i) {
                for (auto& discriminator : notMatchingDiscriminators) {
                    UNIT_ASSERT_EQUAL(controls.HandleTracing(false, discriminator).first, TTracingControls::OFF);
                    timeProvider->Advance(TDuration::Seconds(1));
                }
            }
        }
    }

    Y_UNIT_TEST(SharedThrottlingLimits) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddExternalThrottling();
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(60);
            auto scope = rule->MutableScope();
            scope->AddRequestTypes("Table.DropTable");
            scope->AddRequestTypes("Table.ReadRows");
            scope->AddRequestTypes("Table.AlterTable");
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        std::array matchingDiscriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_DROPTABLE,
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_ALTERTABLE,
                .Database = "/Root/db1",
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_READROWS,
                .Database = "/Root/db2",
            },
        };

        std::array notMatchingDiscriminators{
            TRequestDiscriminator{},
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_KEEPALIVE,
            },
        };

        for (size_t i = 0; i < 21; ++i) {
            UNIT_ASSERT_EQUAL(controls.HandleTracing(false, RandomChoice(matchingDiscriminators)).first, TTracingControls::OFF);
            UNIT_ASSERT_EQUAL(controls.HandleTracing(true, RandomChoice(matchingDiscriminators)).first, TTracingControls::EXTERNAL);
            UNIT_ASSERT_EQUAL(controls.HandleTracing(true, RandomChoice(notMatchingDiscriminators)).first, TTracingControls::OFF);
            timeProvider->Advance(TDuration::MilliSeconds(500));
        }
        UNIT_ASSERT_EQUAL(controls.HandleTracing(true, RandomChoice(matchingDiscriminators)).first, TTracingControls::OFF);
    }

    Y_UNIT_TEST(SharedSamplingLimits) {
        TTenantTestRuntime runtime(DefaultConsoleTestConfig());
        auto timeProvider = MakeIntrusive<TTimeProviderMock>(TInstant::Now());
        auto [controls, configurator] = CreateSamplingThrottlingConfigurator(10, timeProvider);
        NKikimrConfig::TTracingConfig cfg;
        {
            auto rule = cfg.AddSampling();
            rule->SetMaxTracesBurst(10);
            rule->SetMaxTracesPerMinute(60);
            rule->SetLevel(8);
            rule->SetFraction(0.5);
            auto scope = rule->MutableScope();
            scope->AddRequestTypes("Table.DropTable");
            scope->AddRequestTypes("Table.ReadRows");
            scope->AddRequestTypes("Table.AlterTable");
        }
        InitJaegerTracingConfigurator(runtime, std::move(configurator), cfg);

        std::array matchingDiscriminators{
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_DROPTABLE,
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_ALTERTABLE,
                .Database = "/Root/db1",
            },
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_READROWS,
                .Database = "/Root/db2",
            },
        };

        std::array notMatchingDiscriminators{
            TRequestDiscriminator{},
            TRequestDiscriminator{
                .RequestType = ERequestType::TABLE_KEEPALIVE,
            },
        };

        {
            size_t sampled = 0;
            for (size_t i = 0; i < 1000; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(matchingDiscriminators));
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT_EQUAL(level, 8);
                    ++sampled;
                }
                UNIT_ASSERT_EQUAL(controls.HandleTracing(false, RandomChoice(notMatchingDiscriminators)).first, TTracingControls::OFF);
                timeProvider->Advance(TDuration::Seconds(1));
            }
            UNIT_ASSERT(sampled >= 400 && sampled <= 600);
        }
        timeProvider->Advance(TDuration::Minutes(1));

        {
            size_t sampled = 0;
            for (size_t i = 0; i < 65; ++i) {
                auto [state, level] = controls.HandleTracing(false, RandomChoice(matchingDiscriminators));
                UNIT_ASSERT_UNEQUAL(state, TTracingControls::EXTERNAL);
                if (state == TTracingControls::SAMPLED) {
                    UNIT_ASSERT_EQUAL(level, 8);
                    ++sampled;
                }
            }
            UNIT_ASSERT_EQUAL(sampled, 11);
        }
    }

}
} // namespace NKikimr
