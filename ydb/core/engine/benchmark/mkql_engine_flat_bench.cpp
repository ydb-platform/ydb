#include <ydb/core/engine/kikimr_program_builder.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>

#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>

#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

#include <benchmark/benchmark.h>

#include <util/string/builder.h>
#include <util/string/type.h>
#include <util/system/env.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;

namespace {

    using TTypeInfo = NScheme::TTypeInfo;
    using TProgramFactory = std::function<TRuntimeNode(TKikimrProgramBuilder&, ui32)>;

    const ui64 OwnerId = 200;
    const ui64 ReadTableId = 301;
    const ui64 WriteTableIds[] = {302, 303, 304}; // Infly, MessageData, SentTimestampIdx in DeleteMessage
    const ui64 ReadShardBase = 100;
    const ui64 WriteShardBase = 1000;
    const ui64 WriteShardsPerTable = 1000;

    struct TShardLayout {
        ui32 ReadShardCount = 0;
        ui32 WriteShardCount = 0; // per write table
    };

    TVector<TTypeInfo> Uint64Key() {
        return {TTypeInfo(NUdf::TDataType<ui64>::Id)};
    }

    TRuntimeNode SelectReadRow(TKikimrProgramBuilder& pgmBuilder, ui64 key) {
        TVector<TTypeInfo> keyTypes = Uint64Key();
        TVector<TSelectColumn> columns;
        columns.emplace_back("Offset", 1, TTypeInfo(NUdf::TDataType<ui64>::Id), EColumnTypeConstraint::Nullable);
        TRuntimeNode::TList row{pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(key)};
        return pgmBuilder.SelectRow(TTableId(OwnerId, ReadTableId), keyTypes, columns, row);
    }

    // Point SelectRows plus one EraseRow with a computed key spanning every write shard.
    TRuntimeNode BuildDeleteShapedProgram(TKikimrProgramBuilder& pgmBuilder, ui32 keyCount) {
        TVector<TRuntimeNode> effects(Reserve(keyCount + 1));
        for (ui32 key = 0; key < keyCount; ++key) {
            effects.push_back(pgmBuilder.SetResult(TStringBuilder() << "row" << key, SelectReadRow(pgmBuilder, key)));
        }
        TVector<TTypeInfo> eraseKeyTypes = Uint64Key();
        TRuntimeNode::TList eraseRow{pgmBuilder.Add(pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1), pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2))};
        effects.push_back(pgmBuilder.EraseRow(TTableId(OwnerId, WriteTableIds[0]), eraseKeyTypes, eraseRow));
        return pgmBuilder.Build(pgmBuilder.AsList(effects));
    }

    // Mirrors ymq std DeleteMessage: point reads, then erases in three tables keyed by the read rows.
    TRuntimeNode BuildDeleteMessageProgram(TKikimrProgramBuilder& pgmBuilder, ui32 keyCount) {
        TVector<TRuntimeNode> records(Reserve(keyCount));
        for (ui32 key = 0; key < keyCount; ++key) {
            records.push_back(SelectReadRow(pgmBuilder, key));
        }
        const TRuntimeNode existed = pgmBuilder.AsList(records);
        TVector<TTypeInfo> eraseKeyTypes = Uint64Key();
        TVector<TRuntimeNode> effects{pgmBuilder.AsList(pgmBuilder.SetResult("deleted", existed))};
        for (ui64 tableId : WriteTableIds) {
            effects.push_back(pgmBuilder.Map(existed, [&](TRuntimeNode item) {
                TRuntimeNode::TList row{pgmBuilder.Member(item, "Offset")};
                return pgmBuilder.EraseRow(TTableId(OwnerId, tableId), eraseKeyTypes, row);
            }));
        }
        return pgmBuilder.Build(pgmBuilder.Extend(effects));
    }

    // Point reads and point erases: every shard gets its own set of keys, so no program can be shared.
    TRuntimeNode BuildPointDeleteProgram(TKikimrProgramBuilder& pgmBuilder, ui32 keyCount) {
        TVector<TTypeInfo> eraseKeyTypes = Uint64Key();
        TVector<TRuntimeNode> effects(Reserve(2 * keyCount));
        for (ui32 key = 0; key < keyCount; ++key) {
            effects.push_back(pgmBuilder.SetResult(TStringBuilder() << "row" << key, SelectReadRow(pgmBuilder, key)));
            TRuntimeNode::TList eraseRow{pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(key)};
            effects.push_back(pgmBuilder.EraseRow(TTableId(OwnerId, WriteTableIds[0]), eraseKeyTypes, eraseRow));
        }
        return pgmBuilder.Build(pgmBuilder.AsList(effects));
    }

    ui64 WriteShardId(const TKeyDesc& key, ui32 index) {
        return WriteShardBase + (key.TableId.PathId.LocalPathId - ReadTableId) * WriteShardsPerTable + index;
    }

    // Point keys go round-robin over the shards of their table; computed keys cover every shard of the table.
    void ResolveKeys(TArrayRef<THolder<TKeyDesc>> keys, TShardLayout layout) {
        ui32 readIndex = 0;
        ui32 writeIndex = 0;
        for (auto& key : keys) {
            key->Status = TKeyDesc::EStatus::Ok;
            TVector<TKeyDesc::TPartitionInfo> partitions;
            if (key->RowOperation == TKeyDesc::ERowOperation::Read) {
                partitions.emplace_back(ReadShardBase + readIndex++ % layout.ReadShardCount);
            } else if (key->Range.Point) {
                partitions.emplace_back(WriteShardId(*key, writeIndex++ % layout.WriteShardCount));
            } else {
                partitions.reserve(layout.WriteShardCount);
                for (ui32 index = 0; index < layout.WriteShardCount; ++index) {
                    partitions.emplace_back(WriteShardId(*key, index));
                }
            }
            key->Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
        }
    }

    TString CompileProgram(const TProgramFactory& factory, ui32 keyCount) {
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TKikimrProgramBuilder pgmBuilder(env, *functionRegistry);
        return SerializeRuntimeNode(factory(pgmBuilder, keyCount), env);
    }

    // Same activation context the engine uses to read ShareShardedMkqlProgram.
    struct TShareShardedMkqlProgramContext {
        TAppData Data{0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr};
        THolder<NActors::TActorSystem> System;
        NActors::TMailbox Mailbox;
        THolder<NActors::TExecutorThread> Thread;
        THolder<NActors::TActorContext> Context;
        NActors::TActivationContext* Previous = NActors::TlsActivationContext;

        explicit TShareShardedMkqlProgramContext(bool share) {
            Data.FeatureFlags.SetShareShardedMkqlProgram(share);
            THolder<NActors::TActorSystemSetup> setup(new NActors::TActorSystemSetup);
            System.Reset(new NActors::TActorSystem(setup, &Data));
            Thread.Reset(new NActors::TExecutorThread(0, System.Get(), nullptr, "mkql-flag-bench"));
            Context.Reset(new NActors::TActorContext(Mailbox, *Thread, GetCycleCountFast(), NActors::TActorId()));
            NActors::TlsActivationContext = Context.Get();
        }

        ~TShareShardedMkqlProgramContext() {
            NActors::TlsActivationContext = Previous;
        }
    };

    void PrepareAndSerialize(benchmark::State& state, const TProgramFactory& factory, ui32 keyCount, TShardLayout layout) {
        TShareShardedMkqlProgramContext context(IsTrue(GetEnv("MKQL_SHARE_SHARDED_PROGRAM")));
        const TString program = CompileProgram(factory, keyCount);
        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(1);
        ui32 affectedShardCount = 0;
        for (auto _ : state) {
            TEngineFlatSettings settings(IEngineFlat::EProtocol::V1, functionRegistry.Get(), *randomProvider, *timeProvider);
            auto engine = CreateEngineFlat(settings);
            Y_ENSURE(engine->SetProgram(program) == IEngineFlat::EResult::Ok, engine->GetErrors());
            ResolveKeys(engine->GetDbKeys(), layout);
            Y_ENSURE(engine->PrepareShardPrograms() == IEngineFlat::EResult::Ok, engine->GetErrors());
            ui64 serializedBytes = 0;
            affectedShardCount = engine->GetAffectedShardCount();
            for (ui32 index = 0; index < affectedShardCount; ++index) {
                IEngineFlat::TShardData shardData;
                Y_ENSURE(engine->GetAffectedShard(index, shardData) == IEngineFlat::EResult::Ok);
                serializedBytes += shardData.Program.size();
            }
            engine->AfterShardProgramsExtracted();
            benchmark::DoNotOptimize(serializedBytes);
        }
        state.counters["shards"] = affectedShardCount;
        state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(keyCount) * affectedShardCount);
    }

    void PrepareAndSerializeShardPrograms(benchmark::State& state) {
        PrepareAndSerialize(state, BuildDeleteShapedProgram, state.range(0), {.ReadShardCount = 1, .WriteShardCount = static_cast<ui32>(state.range(1))});
    }

    void DeleteMessageShardPrograms(benchmark::State& state) {
        PrepareAndSerialize(state, BuildDeleteMessageProgram, state.range(0), {.ReadShardCount = static_cast<ui32>(state.range(1)), .WriteShardCount = static_cast<ui32>(state.range(2))});
    }

    void PointDeleteShardPrograms(benchmark::State& state) {
        PrepareAndSerialize(state, BuildPointDeleteProgram, state.range(0), {.ReadShardCount = static_cast<ui32>(state.range(1)), .WriteShardCount = static_cast<ui32>(state.range(2))});
    }

} // namespace

BENCHMARK(PrepareAndSerializeShardPrograms)->ArgNames({"keys", "writeShards"})->Args({200, 32});
BENCHMARK(DeleteMessageShardPrograms)->ArgNames({"keys", "readShards", "writeShards"})->Args({10, 1, 1})->Args({200, 1, 32})->Args({200, 8, 32})->Args({200, 32, 32})->Args({1000, 32, 32});
BENCHMARK(PointDeleteShardPrograms)->ArgNames({"keys", "readShards", "writeShards"})->Args({200, 8, 8})->Args({200, 32, 32});
