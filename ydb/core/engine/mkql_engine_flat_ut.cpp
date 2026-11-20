#include "mkql_engine_flat.h"
#include "mkql_engine_flat_host.h"

#include <yql/essentials/minikql/mkql_function_registry.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/aclib/user_context.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/core/tablet_flat/test/libs/table/test_dummy.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/base/appdata.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/mailbox.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
    using TTypeInfo = NScheme::TTypeInfo;

    const ui64 Shard1 = 100;
    const ui64 Shard2 = 101;
    //const ui64 Shard3 = 102;
    //const ui64 Shard4 = 103;

    const ui64 OwnerId = 200;
    const ui64 Table1Id = 301;
    const ui64 Table2Id = 302;

    struct Schema1 : NIceDb::Schema {
        struct Table1 : Table<Table1Id> {
            struct ID : Column<1, NUdf::TDataType<ui32>::Id> {};
            struct Value : Column<2, NUdf::TDataType<NUdf::TUtf8>::Id> {};

            using TKey = TableKey<ID>;
            using TColumns = TableColumns<ID, Value>;
        };

        using TTables = SchemaTables<Table1>;
    };

    struct Schema2 : NIceDb::Schema {
        struct Table2 : Table<Table2Id> {
            struct ID : Column<3, NUdf::TDataType<ui64>::Id> {};
            struct Value : Column<4, NUdf::TDataType<ui32>::Id> {};

            using TKey = TableKey<ID>;
            using TColumns = TableColumns<ID, Value>;
        };

        using TTables = SchemaTables<Table2>;
    };

    struct TShardDbState {
        THashMap<ui64, TAutoPtr<NTable::TDatabase>> Dbs;
        THashMap<ui64, NTable::TDummyEnv> Envs;
        THashMap<ui64, ui32> Steps;

        template <typename TSchema>
        void AddShard(ui64 tabletId) {
            auto& db = Dbs[tabletId];

            db.Reset(new NTable::TDatabase);
            Steps[tabletId] = 0;

            { // Create tables
                BeginTransaction(tabletId);
                NIceDb::TNiceDb(*db).Materialize<TSchema>();
                CommitTransaction(tabletId);
            }
        }

        void BeginTransaction(ui64 tabletId) {
            auto step = ++Steps[tabletId];
            Dbs[tabletId]->Begin(step, Envs[tabletId]);
        }

        void CommitTransaction(ui64 tabletId) {
            Dbs[tabletId]->Commit(Steps[tabletId], true);
            Envs.erase(tabletId);
        }
    };

    void EmptyShardResolver(TKeyDesc&) {
        UNIT_FAIL("Should not be called");
    }

    void NullShardResolver(TKeyDesc& key) {
        key.Status = TKeyDesc::EStatus::Ok;
    }

    void SingleShardResolver(TKeyDesc& key) {
        key.Status = TKeyDesc::EStatus::Ok;

        TVector<TKeyDesc::TPartitionInfo> partitions;
        partitions.push_back(TKeyDesc::TPartitionInfo(Shard1));
        key.Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
    }

    void DoubleShardResolver(TKeyDesc& key) {
        key.Status = TKeyDesc::EStatus::Ok;

        TVector<TKeyDesc::TPartitionInfo> partitions;
        partitions.push_back(TKeyDesc::TPartitionInfo(Shard1));
        partitions.push_back(TKeyDesc::TPartitionInfo(Shard2));
        key.Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
    }

    void TwoShardResolver(TKeyDesc& key) {
        key.Status = TKeyDesc::EStatus::Ok;
        TVector<TKeyDesc::TPartitionInfo> partitions;
        partitions.push_back(TKeyDesc::TPartitionInfo(key.TableId.PathId.LocalPathId == Table1Id ? Shard1 : Shard2));
        key.Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
    }

    struct TDriver {
        using TOutgoingReadsets = TMap<ui64, TMap<ui64, TString>>;
        using TIncomingReadsets = TMap<ui64, TVector<ui64>>;

        TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
        TIntrusivePtr<IRandomProvider> RandomProvider;
        TIntrusivePtr<ITimeProvider> TimeProvider;
        TScopedAlloc Alloc;
        TTypeEnvironment Env;
        TKikimrProgramBuilder PgmBuilder;
        TShardDbState ShardDbState;
        std::function<void(TEngineFlatSettings&)> SettingsConfigurer;
        std::function<void(ui64, const TString&)> ShardProgramInspector;
        std::function<void(const TOutgoingReadsets&, const TIncomingReadsets&)> ReadSetsInspector;
        std::function<void(ui64, const TString&)> ShardReplyInspector;

        TDriver()
            : FunctionRegistry(CreateFunctionRegistry(CreateBuiltinRegistry()))
            , RandomProvider(CreateDeterministicRandomProvider(1))
            , TimeProvider(CreateDeterministicTimeProvider(1))
            , Alloc(__LOCATION__)
            , Env(Alloc)
            , PgmBuilder(Env, *FunctionRegistry)
            , SettingsConfigurer([](TEngineFlatSettings&) {})
            , ShardProgramInspector([](ui64, const TString&) {})
            , ReadSetsInspector([](const TOutgoingReadsets&, const TIncomingReadsets&) {})
            , ShardReplyInspector([](ui64, const TString&) {})
        {
        }

        ~TDriver()
        {
        }

        IEngineFlat::EStatus Run(
            TRuntimeNode pgm,
            NKikimrMiniKQL::TResult& res,
            std::function<void(TKeyDesc&)> keyResolver,
            IEngineFlat::TShardLimits shardLimits = IEngineFlat::TShardLimits()) {
            TString pgmStr = SerializeRuntimeNode(pgm, Env);

            TEngineFlatSettings settings(IEngineFlat::EProtocol::V1, FunctionRegistry.Get(), *RandomProvider, *TimeProvider);
            SettingsConfigurer(settings);

            const auto unguard = Unguard(Alloc);
            auto proxyEngine = CreateEngineFlat(settings);
            if (proxyEngine->SetProgram(pgmStr) != IEngineFlat::EResult::Ok) {
                Cerr << proxyEngine->GetErrors() << Endl;
                return IEngineFlat::EStatus::Error;
            }

            auto& dbKeys = proxyEngine->GetDbKeys();
            for (auto& key : dbKeys) {
                keyResolver(*key);
                if (key->Status != TKeyDesc::EStatus::Ok)
                    return IEngineFlat::EStatus::Error;
            }

            if (proxyEngine->PrepareShardPrograms(shardLimits) != IEngineFlat::EResult::Ok) {
                Cerr << proxyEngine->GetErrors() << Endl;
                return IEngineFlat::EStatus::Error;
            }

            const ui32 affectedShardCount = proxyEngine->GetAffectedShardCount();
            TMap<ui64, TString> shardPrograms;
            for (ui32 index = 0; index < affectedShardCount; ++index) {
                IEngineFlat::TShardData shardData;
                UNIT_ASSERT(proxyEngine->GetAffectedShard(index, shardData) == IEngineFlat::EResult::Ok);
                ShardProgramInspector(shardData.ShardId, shardData.Program);
                shardPrograms[shardData.ShardId] = shardData.Program;
            }

            proxyEngine->AfterShardProgramsExtracted();

            THashMap<ui64, TAutoPtr<TEngineHostCounters>> hostCounters;
            THashMap<ui64, TAutoPtr<IEngineFlatHost>> hosts;
            for (const auto& shardPgm : shardPrograms) {
                auto& counters = hostCounters[shardPgm.first];
                counters.Reset(new TEngineHostCounters());
                hosts[shardPgm.first].Reset(new TUnversionedEngineHost(
                    *ShardDbState.Dbs[shardPgm.first], *counters, TEngineHostSettings(shardPgm.first, false)));
            }

            for (const auto& shardPgm : shardPrograms) {
                ShardDbState.BeginTransaction(shardPgm.first);
                auto dataEngine = CreateEngineFlat(TEngineFlatSettings(IEngineFlat::EProtocol::V1, FunctionRegistry.Get(),
                    *RandomProvider, *TimeProvider, NACLib::TUserContextBuilder().Build(), hosts[shardPgm.first].Get()));
                UNIT_ASSERT(dataEngine->AddProgram(shardPgm.first, shardPgm.second) == IEngineFlat::EResult::Ok);
                IEngineFlat::TValidationInfo validationInfo;
                IEngineFlat::EResult result = dataEngine->Validate(validationInfo);
                if (result != IEngineFlat::EResult::Ok) {
                    Cerr << dataEngine->GetErrors() << Endl;
                    return IEngineFlat::EStatus::Error;
                }

                UNIT_ASSERT(dataEngine->PinPages() == IEngineFlat::EResult::Ok);
                ShardDbState.CommitTransaction(shardPgm.first);
            }

            TOutgoingReadsets outgoingReadsets;
            TIncomingReadsets incomingReadsets;
            for (const auto& shardPgm : shardPrograms) {
                ShardDbState.BeginTransaction(shardPgm.first);
                auto dataEngine = CreateEngineFlat(TEngineFlatSettings(IEngineFlat::EProtocol::V1, FunctionRegistry.Get(),
                    *RandomProvider, *TimeProvider, NACLib::TUserContextBuilder().Build(), hosts[shardPgm.first].Get()));
                UNIT_ASSERT(dataEngine->AddProgram(shardPgm.first, shardPgm.second) == IEngineFlat::EResult::Ok);
                auto result = dataEngine->PrepareOutgoingReadsets();
                if (result != IEngineFlat::EResult::Ok) {
                    Cerr << dataEngine->GetErrors() << Endl;
                    return IEngineFlat::EStatus::Error;
                }

                ui32 outRsCount = dataEngine->GetOutgoingReadsetsCount();
                for (ui32 index = 0; index < outRsCount; ++index) {
                    const auto& readSet = dataEngine->GetOutgoingReadset(index);
                    UNIT_ASSERT_EQUAL(readSet.OriginShardId, shardPgm.first);
                    outgoingReadsets[readSet.OriginShardId][readSet.TargetShardId] = readSet.Body;
                }

                dataEngine->AfterOutgoingReadsetsExtracted();

                result = dataEngine->PrepareIncomingReadsets();
                if (result != IEngineFlat::EResult::Ok) {
                    Cerr << dataEngine->GetErrors() << Endl;
                    return IEngineFlat::EStatus::Error;
                }

                ui32 inRsCount = dataEngine->GetExpectedIncomingReadsetsCount();
                for (ui32 index = 0; index < inRsCount; ++index) {
                    const ui64 origin = dataEngine->GetExpectedIncomingReadsetOriginShard(index);
                    incomingReadsets[shardPgm.first].push_back(origin);
                }

                ShardDbState.CommitTransaction(shardPgm.first);
            }

            ReadSetsInspector(outgoingReadsets, incomingReadsets);
            for (const auto& shardPgm : shardPrograms) {
                ShardDbState.BeginTransaction(shardPgm.first);
                auto dataEngine = CreateEngineFlat(TEngineFlatSettings(IEngineFlat::EProtocol::V1, FunctionRegistry.Get(),
                    *RandomProvider, *TimeProvider, NACLib::TUserContextBuilder().Build(), hosts[shardPgm.first].Get()));
                UNIT_ASSERT(dataEngine->AddProgram(shardPgm.first, shardPgm.second) == IEngineFlat::EResult::Ok);

                if (incomingReadsets.contains(shardPgm.first)) {
                    for (ui64 rsOrigin : incomingReadsets[shardPgm.first]) {
                        auto it = outgoingReadsets.find(rsOrigin);
                        UNIT_ASSERT(it != outgoingReadsets.end());
                        auto it2 = it->second.find(shardPgm.first);
                        UNIT_ASSERT(it2 != it->second.end());
                        dataEngine->AddIncomingReadset(it2->second);
                        it->second.erase(it2);
                    }
                }

                dataEngine->PinPages();
                auto result = dataEngine->Execute();
                if (result != IEngineFlat::EResult::Ok) {
                    Cerr << dataEngine->GetErrors() << Endl;
                }
                else {
                    const TString reply = dataEngine->GetShardReply(shardPgm.first);
                    ShardReplyInspector(shardPgm.first, reply);
                    proxyEngine->AddShardReply(shardPgm.first, reply);
                    proxyEngine->FinalizeOriginReplies(shardPgm.first);
                }

                ShardDbState.CommitTransaction(shardPgm.first);
            }

            for (auto& outRS : outgoingReadsets) {
                UNIT_ASSERT_EQUAL(outRS.second.size(), 0);
            }

            proxyEngine->BuildResult();
            auto status = proxyEngine->GetStatus();
            if (status == IEngineFlat::EStatus::Complete || status == IEngineFlat::EStatus::Aborted) {
                proxyEngine->FillResultValue(res);
            }

            if (status == IEngineFlat::EStatus::Error) {
                Cerr << proxyEngine->GetErrors() << Endl;
            }

            return status;
        }

        TMap<ui64, TString> CollectShardPrograms(TRuntimeNode pgm, std::function<void(TKeyDesc&)> keyResolver) {
            TString pgmStr = SerializeRuntimeNode(pgm, Env);
            TEngineFlatSettings settings(IEngineFlat::EProtocol::V1, FunctionRegistry.Get(), *RandomProvider, *TimeProvider);
            SettingsConfigurer(settings);
            const auto unguard = Unguard(Alloc);
            auto proxyEngine = CreateEngineFlat(settings);
            UNIT_ASSERT_C(proxyEngine->SetProgram(pgmStr) == IEngineFlat::EResult::Ok, proxyEngine->GetErrors());
            for (auto& key : proxyEngine->GetDbKeys()) {
                keyResolver(*key);
                UNIT_ASSERT_EQUAL(key->Status, TKeyDesc::EStatus::Ok);
            }
            UNIT_ASSERT_C(proxyEngine->PrepareShardPrograms() == IEngineFlat::EResult::Ok, proxyEngine->GetErrors());
            TMap<ui64, TString> shardPrograms;
            const ui32 affectedShardCount = proxyEngine->GetAffectedShardCount();
            for (ui32 index = 0; index < affectedShardCount; ++index) {
                IEngineFlat::TShardData shardData;
                UNIT_ASSERT_EQUAL(proxyEngine->GetAffectedShard(index, shardData), IEngineFlat::EResult::Ok);
                shardPrograms[shardData.ShardId] = shardData.Program;
            }
            proxyEngine->AfterShardProgramsExtracted();
            return shardPrograms;
        }
    };
}

TString TypeSummary(const TType* type) {
    if (type->IsCallable()) {
        return TString(static_cast<const TCallableType*>(type)->GetName());
    }
    if (type->IsData()) {
        const auto schemeType = static_cast<const TDataType*>(type)->GetSchemeType();
        if (schemeType == NUdf::TDataType<ui64>::Id) {
            return "Uint64";
        }
        if (schemeType == NUdf::TDataType<ui32>::Id) {
            return "Uint32";
        }
        if (schemeType == NUdf::TDataType<char*>::Id) {
            return "String";
        }
        if (schemeType == NUdf::TDataType<NUdf::TUtf8>::Id) {
            return "Utf8";
        }
        return ToString(schemeType);
    }
    if (type->IsStruct()) {
        TStringBuilder summary;
        summary << "Struct";
        const auto* structType = static_cast<const TStructType*>(type);
        for (ui32 index = 0; index < structType->GetMembersCount(); ++index) {
            summary << (index == 0 ? " " : ", ") << structType->GetMemberName(index);
        }
        return TString(summary);
    }
    if (type->IsOptional()) {
        return "Optional";
    }
    if (type->IsList()) {
        return "List";
    }
    if (type->IsTuple()) {
        return "Tuple";
    }
    if (type->IsVoid()) {
        return "Void";
    }
    return "Other";
}

TString DataText(const TDataLiteral& data) {
    const auto schemeType = data.GetType()->GetSchemeType();
    const auto value = data.AsValue();
    if (schemeType == NUdf::TDataType<ui64>::Id) {
        return ToString(value.Get<ui64>());
    }
    if (schemeType == NUdf::TDataType<ui32>::Id) {
        return ToString(value.Get<ui32>());
    }
    if (schemeType == NUdf::TDataType<bool>::Id) {
        return value.Get<bool>() ? "true" : "false";
    }
    return TString(value.AsStringRef()).Quote();
}

void PrintProgram(TStringBuilder& out, const TNode* node, ui32 indent) {
    for (ui32 pad = 0; pad < indent; ++pad) {
        out << "  ";
    }
    if (node->GetType()->GetKind() == TType::EKind::Type) {
        out << "Type " << TypeSummary(static_cast<const TType*>(node)) << "\n";
        return;
    }
    switch (node->GetType()->GetKind()) {
        case TType::EKind::Struct: {
            const auto& value = static_cast<const TStructLiteral&>(*node);
            out << "Struct\n";
            for (ui32 index = 0; index < value.GetValuesCount(); ++index) {
                for (ui32 pad = 0; pad < indent + 1; ++pad) {
                    out << "  ";
                }
                out << value.GetType()->GetMemberName(index) << "\n";
                PrintProgram(out, value.GetValue(index).GetNode(), indent + 2);
            }
            return;
        }
        case TType::EKind::Callable: {
            const auto& callable = static_cast<const TCallable&>(*node);
            out << callable.GetType()->GetName() << " #" << callable.GetUniqueId() << "\n";
            for (ui32 index = 0; index < callable.GetInputsCount(); ++index) {
                PrintProgram(out, callable.GetInput(index).GetNode(), indent + 1);
            }
            return;
        }
        case TType::EKind::Data:
            out << TypeSummary(node->GetType()) << " " << DataText(static_cast<const TDataLiteral&>(*node)) << "\n";
            return;
        case TType::EKind::List: {
            const auto& list = static_cast<const TListLiteral&>(*node);
            out << "List " << list.GetItemsCount() << "\n";
            for (ui32 index = 0; index < list.GetItemsCount(); ++index) {
                PrintProgram(out, list.GetItems()[index].GetNode(), indent + 1);
            }
            return;
        }
        case TType::EKind::Tuple: {
            const auto& tuple = static_cast<const TTupleLiteral&>(*node);
            out << "Tuple\n";
            for (ui32 index = 0; index < tuple.GetValuesCount(); ++index) {
                PrintProgram(out, tuple.GetValue(index).GetNode(), indent + 1);
            }
            return;
        }
        case TType::EKind::Optional: {
            const auto& optional = static_cast<const TOptionalLiteral&>(*node);
            out << (optional.HasItem() ? "Optional\n" : "EmptyOptional\n");
            if (optional.HasItem()) {
                PrintProgram(out, optional.GetItem().GetNode(), indent + 1);
            }
            return;
        }
        case TType::EKind::Void:
            out << "Void\n";
            return;
        case TType::EKind::Null:
            out << "Null\n";
            return;
        default:
            out << "Node\n";
            return;
    }
}

TString ProgramText(const TString& serialized, TTypeEnvironment& env) {
    TStringBuilder out;
    PrintProgram(out, DeserializeRuntimeNode(serialized, env).GetNode(), 0);
    return TString(out);
}

// AppData() reads the flag from the actor system bound to the current activation context.
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
        Thread.Reset(new NActors::TExecutorThread(0, System.Get(), nullptr, "mkql-flag-test"));
        Context.Reset(new NActors::TActorContext(Mailbox, *Thread, GetCycleCountFast(), NActors::TActorId()));
        NActors::TlsActivationContext = Context.Get();
    }

    ~TShareShardedMkqlProgramContext() {
        NActors::TlsActivationContext = Previous;
    }
};

struct TPreparedShardPrograms {
    TRuntimeNode Program;
    std::function<void(TKeyDesc&)> Resolve;
};

TMap<ui64, TString> CollectProgramsWithFlag(bool share, const std::function<TPreparedShardPrograms(TDriver&)>& build) {
    TShareShardedMkqlProgramContext context(share);
    TDriver driver;
    TPreparedShardPrograms prepared = build(driver);
    return driver.CollectShardPrograms(prepared.Program, prepared.Resolve);
}

void AssertSameSerializedPrograms(TStringBuf title, const TMap<ui64, TString>& plain, const TMap<ui64, TString>& shared) {
    UNIT_ASSERT_C(!plain.empty(), title);
    UNIT_ASSERT_EQUAL_C(shared.size(), plain.size(), title);
    for (const auto& [shardId, program] : plain) {
        const TString* sharedProgram = shared.FindPtr(shardId);
        UNIT_ASSERT_C(sharedProgram, title << " missing shard " << shardId);
        UNIT_ASSERT_C(!program.empty(), title << " empty program for shard " << shardId);
        if (program == *sharedProgram) {
            continue;
        }
        size_t mismatchAt = 0;
        const size_t common = program.size() < sharedProgram->size() ? program.size() : sharedProgram->size();
        while (mismatchAt < common && program[mismatchAt] == (*sharedProgram)[mismatchAt]) {
            ++mismatchAt;
        }
        UNIT_FAIL(title << " serialized program mismatch for shard " << shardId << " sizes " << program.size() << " and " << sharedProgram->size() << " at offset " << mismatchAt);
    }
}

using TShardsByKey = TMap<std::pair<ui64, TKeyDesc::ERowOperation>, TVector<ui64>>; // (local table id, operation) -> shards

std::function<void(TKeyDesc&)> MakeShardResolver(TShardsByKey shardsByKey) {
    return [shardsByKey = std::move(shardsByKey)](TKeyDesc& key) {
        key.Status = TKeyDesc::EStatus::Ok;
        const TVector<ui64>* shards = shardsByKey.FindPtr(std::make_pair(key.TableId.PathId.LocalPathId, key.RowOperation));
        UNIT_ASSERT_C(shards, "Unexpected key for table " << key.TableId.PathId.LocalPathId);
        TVector<TKeyDesc::TPartitionInfo> partitions(Reserve(shards->size()));
        for (ui64 shard : *shards) {
            partitions.emplace_back(shard);
        }
        key.Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
    };
}

struct TShardRunCapture {
    TMap<ui64, TString> Programs;
    TDriver::TOutgoingReadsets Outgoing;
    TDriver::TIncomingReadsets Incoming;
};

void CaptureShardRun(TDriver& driver, TShardRunCapture& capture) {
    driver.ShardProgramInspector = [&capture](ui64 shard, const TString& program) {
        capture.Programs[shard] = program;
    };
    driver.ReadSetsInspector = [&capture](const TDriver::TOutgoingReadsets& outgoing, const TDriver::TIncomingReadsets& incoming) {
        capture.Outgoing = outgoing;
        capture.Incoming = incoming;
    };
}

void AssertReadsetsOnlyFrom(const TShardRunCapture& capture, ui64 origin, TArrayRef<const ui64> targets) {
    UNIT_ASSERT_VALUES_EQUAL(capture.Outgoing.size(), 1);
    const TMap<ui64, TString>* outgoing = capture.Outgoing.FindPtr(origin);
    UNIT_ASSERT(outgoing);
    TVector<ui64> outgoingTargets(Reserve(outgoing->size()));
    for (const auto& [target, body] : *outgoing) {
        outgoingTargets.push_back(target);
    }
    TVector<ui64> expectedTargets(targets.begin(), targets.end());
    Sort(expectedTargets);
    UNIT_ASSERT_EQUAL(outgoingTargets, expectedTargets);
    TDriver::TIncomingReadsets expectedIncoming;
    for (ui64 target : targets) {
        expectedIncoming[target].push_back(origin);
    }
    UNIT_ASSERT_EQUAL(capture.Incoming, expectedIncoming);
}

TRuntimeNode SelectTable1Rows(TKikimrProgramBuilder& pgmBuilder, std::initializer_list<ui32> ids) {
    TVector<TTypeInfo> keyTypes{TTypeInfo(NUdf::TDataType<ui32>::Id)};
    TVector<TSelectColumn> columns;
    columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType), EColumnTypeConstraint::Nullable);
    TRuntimeNode::TList rows;
    for (ui32 id : ids) {
        TRuntimeNode::TList row{pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(id)};
        rows.push_back(pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row));
    }
    return pgmBuilder.AsList(rows);
}

TRuntimeNode::TList Table2KeyFromRead(TKikimrProgramBuilder& pgmBuilder, TRuntimeNode item, ui32 shift) {
    return {pgmBuilder.Add(pgmBuilder.Convert(pgmBuilder.Member(item, "ID"), pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id))), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(shift))};
}

Y_UNIT_TEST_SUITE(TMiniKQLEngineFlatTest) {

    template<NUdf::TDataTypeId TKey, NUdf::TDataTypeId TValue>
    NKikimrMiniKQL::TResult WriteSomething(
        TDriver& driver,
        TRuntimeNode key = TRuntimeNode(),
        TRuntimeNode value = TRuntimeNode())
    {
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(TKey);
        TRuntimeNode::TList row(1);
        row[0] = key ? key : pgmBuilder.NewNull();
        auto update = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId = (ui32)Schema1::Table1::Value::ColumnId;

        update.SetColumn(columnId,
            TTypeInfo(TValue),
            value ? value : pgmBuilder.NewNull());

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        return res;
    }

    template<NUdf::TDataTypeId TKey>
    NKikimrMiniKQL::TResult SelectNullKey(
        TDriver& driver)
    {
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(TKey);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.NewNull();

        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto value = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row);

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        return res;
    }

    Y_UNIT_TEST(TestEmptyProgram) {
        TDriver driver;
        auto& pgmBuilder = driver.PgmBuilder;
        auto pgm = pgmBuilder.Build(pgmBuilder.NewEmptyListOfVoid());

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &EmptyShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestPureProgram) {
        TDriver driver;
        auto& pgmBuilder = driver.PgmBuilder;
        auto value = pgmBuilder.TProgramBuilder::TProgramBuilder::NewDataLiteral<ui32>(42);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &EmptyShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Data
            Data {
              Scheme: 2
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Uint32: 42
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestAbort) {
        TDriver driver;
        auto& pgmBuilder = driver.PgmBuilder;
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.Abort()));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &EmptyShardResolver), IEngineFlat::EStatus::Aborted);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRowWithoutColumnsNotExists) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.TProgramBuilder::TProgramBuilder::NewDataLiteral<ui32>(42);
        TVector<TSelectColumn> columns;
        auto value = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRowWithoutColumnsExists) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        TVector<TSelectColumn> columns;
        auto value = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Optional {
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRowPayload) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto value = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "2"
                    Type {
                      Kind: Optional
                      Optional {
                        Item {
                          Kind: Data
                          Data {
                            Scheme: 4608
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Optional {
        Struct {
          Optional {
            Text: "qwe"
          }
        }
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRowPayloadNullKey) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        WriteSomething<NUdf::TDataType<ui32>::Id, NUdf::TDataType<NUdf::TUtf8>::Id>(
            driver,
            pgmBuilder.NewNull(),
            pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("qwe"));

        auto res = SelectNullKey<NUdf::TDataType<ui32>::Id>(driver);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "2"
                    Type {
                      Kind: Optional
                      Optional {
                        Item {
                          Kind: Data
                          Data {
                            Scheme: 4608
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Optional {
        Struct {
          Optional {
            Text: "qwe"
          }
        }
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }


    Y_UNIT_TEST(TestEraseRow) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.EraseRow(TTableId(OwnerId, Table1Id), keyTypes, row)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && !selectRes.IsValid());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestEraseRowNullKey) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        {
            WriteSomething<NUdf::TDataType<ui32>::Id, NUdf::TDataType<NUdf::TUtf8>::Id>(
                driver,
                pgmBuilder.NewNull(),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("qwe"));
        }

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.NewNull();

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.EraseRow(TTableId(OwnerId, Table1Id), keyTypes, row)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            auto res = SelectNullKey<NUdf::TDataType<ui32>::Id>(driver);
            auto resStr = res.DebugString();
            auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "2"
                    Type {
                      Kind: Optional
                      Optional {
                        Item {
                          Kind: Data
                          Data {
                            Scheme: 4608
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        }
    }

    Y_UNIT_TEST(TestUpdateRowNotExistWithoutColumns) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto update = pgmBuilder.GetUpdateRowBuilder();
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(!selectRes.HaveValue<Schema1::Table1::Value>());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestUpdateRowNotExistSetPayload) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto update = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId = (ui32)Schema1::Table1::Value::ColumnId;
        update.SetColumn(columnId,
            TTypeInfo(NUdf::TDataType<NUdf::TUtf8>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("qwe"));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(selectRes.HaveValue<Schema1::Table1::Value>());
            UNIT_ASSERT_VALUES_EQUAL(selectRes.GetValue<Schema1::Table1::Value>(), "qwe");
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestUpdateRowNotExistSetPayloadNullValue) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        auto res = WriteSomething<NUdf::TDataType<ui32>::Id, NUdf::TDataType<NUdf::TUtf8>::Id>(
            driver,
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42));

        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(!selectRes.HaveValue<Schema1::Table1::Value>());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestUpdateRowNotExistErasePayload) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto update = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId = (ui32)Schema1::Table1::Value::ColumnId;
        update.EraseColumn(columnId);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(!selectRes.HaveValue<Schema1::Table1::Value>());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestUpdateRowExistChangePayload) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto update = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId = (ui32)Schema1::Table1::Value::ColumnId;
        update.SetColumn(columnId,
            TTypeInfo(NUdf::TDataType<NUdf::TUtf8>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("qwe"));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(selectRes.HaveValue<Schema1::Table1::Value>());
            UNIT_ASSERT_VALUES_EQUAL(selectRes.GetValue<Schema1::Table1::Value>(), "qwe");
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestUpdateRowExistErasePayload) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto update = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId = (ui32)Schema1::Table1::Value::ColumnId;
        update.EraseColumn(columnId);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(!selectRes.HaveValue<Schema1::Table1::Value>());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestSelectRangeFullWithoutColumnsNotExists) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeFullWithoutColumnsNotExistsNullKey) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewNull();
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }


    Y_UNIT_TEST(TestSelectRangeFullExists) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;

        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeFullExistsTruncatedByItems) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.ItemsLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeFullExistsTruncatedByItemsFromNull) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewNull();
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.ItemsLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeFullExistsTruncatedByBytes) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("loremipsumloremipsum1"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("loremipsumloremipsum2"));
                db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("loremipsumloremipsum3"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.BytesLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(32);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "loremipsumloremipsum1"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "loremipsumloremipsum2"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeFromInclusive) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.FromColumns = rowFrom;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "zsd"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeFromExclusive) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(
            TReadRangeOptions::TFlags::ExcludeInitValue |
            TReadRangeOptions::TFlags::IncludeTermValue);
        options.FromColumns = rowFrom;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "zsd"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeToInclusive) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        rowTo[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeNullNull) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.NewNull();
        rowTo[0] = pgmBuilder.NewNull();

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeToExclusive) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        rowTo[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::ExcludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeBothIncFromIncTo) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(41))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("abc"));
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        rowTo[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(
            TReadRangeOptions::TFlags::IncludeInitValue |
            TReadRangeOptions::TFlags::IncludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeBothExcFromIncTo) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(41))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("abc"));
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        rowTo[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(
            TReadRangeOptions::TFlags::ExcludeInitValue |
            TReadRangeOptions::TFlags::IncludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeBothIncFromExcTo) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(41))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("abc"));
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        rowTo[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(
            TReadRangeOptions::TFlags::IncludeInitValue |
            TReadRangeOptions::TFlags::ExcludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeBothExcFromExcTo) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(41))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("abc"));
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zsd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        TRuntimeNode::TList rowTo(1);
        rowFrom[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        rowTo[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(43);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(
            TReadRangeOptions::TFlags::ExcludeInitValue |
            TReadRangeOptions::TFlags::ExcludeTermValue);
        options.FromColumns = rowFrom;
        options.ToColumns = rowTo;
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRowManyShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema2::Table2>()
                .Key(ui64(11))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(67));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row1(1);
        TVector<TSelectColumn> columns1;
        columns1.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        row1[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        auto value1 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes1, columns1, row1);

        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        TRuntimeNode::TList row2(1);
        row2[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(11);
        TVector<TSelectColumn> columns2;
        columns2.emplace_back("4", (ui32)Schema2::Table2::Value::ColumnId, TTypeInfo(Schema2::Table2::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto value2 = pgmBuilder.SelectRow(TTableId(OwnerId, Table2Id), keyTypes2, columns2, row2);
        TRuntimeNode::TList resList;
        resList.push_back(pgmBuilder.SetResult("myRes1", value1));
        resList.push_back(pgmBuilder.SetResult("myRes2", value2));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(resList));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes1"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "2"
                    Type {
                      Kind: Optional
                      Optional {
                        Item {
                          Kind: Data
                          Data {
                            Scheme: 4608
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    Member {
      Name: "myRes2"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "4"
                    Type {
                      Kind: Optional
                      Optional {
                        Item {
                          Kind: Data
                          Data {
                            Scheme: 2
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Optional {
        Struct {
          Optional {
            Text: "qwe"
          }
        }
      }
    }
  }
  Struct {
    Optional {
      Optional {
        Struct {
          Optional {
            Uint32: 67
          }
        }
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestEraseRowManyShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema2::Table2>()
                .Key(ui64(11))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(67));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row1(1);
        row1[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        auto value1 = pgmBuilder.EraseRow(TTableId(OwnerId, Table1Id), keyTypes1, row1);

        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        TRuntimeNode::TList row2(1);
        row2[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(11);

        auto value2 = pgmBuilder.EraseRow(TTableId(OwnerId, Table2Id), keyTypes2, row2);
        TRuntimeNode::TList resList;
        resList.push_back(value1);
        resList.push_back(value2);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(resList));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && !selectRes.IsValid());
            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            auto selectRes = db.Table<Schema2::Table2>()
                .Key(ui64(11))
                .Select<Schema2::Table2::Value>();

            UNIT_ASSERT(selectRes.IsReady() && !selectRes.IsValid());
            driver.ShardDbState.CommitTransaction(Shard2);
        }
    }

    Y_UNIT_TEST(TestUpdateRowManyShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row1(1);
        row1[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        auto update1 = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId1 = (ui32)Schema1::Table1::Value::ColumnId;
        update1.SetColumn(columnId1,
            TTypeInfo(NUdf::TDataType<NUdf::TUtf8>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("qwe"));

        auto value1 = pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id), keyTypes1, row1, update1);

        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        TRuntimeNode::TList row2(1);
        row2[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(11);

        auto update2 = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId2 = (ui32)Schema2::Table2::Value::ColumnId;
        update2.SetColumn(columnId2,
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(21));

        auto value2 = pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), keyTypes2, row2, update2);
        TRuntimeNode::TList resList;
        resList.push_back(value1);
        resList.push_back(value2);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(resList));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(selectRes.HaveValue<Schema1::Table1::Value>());
            UNIT_ASSERT_VALUES_EQUAL(selectRes.GetValue<Schema1::Table1::Value>(), "qwe");
            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            auto selectRes = db.Table<Schema2::Table2>()
                .Key(ui64(11))
                .Select<Schema2::Table2::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(selectRes.HaveValue<Schema2::Table2::Value>());
            UNIT_ASSERT_VALUES_EQUAL(selectRes.GetValue<Schema2::Table2::Value>(), 21);
            driver.ShardDbState.CommitTransaction(Shard2);
        }
    }

    void TestCASBoth2Impl(bool simulateFail1, bool simulateFail2) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema2::Table2>()
                .Key(ui64(11))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(67));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row1(1);
        row1[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        auto update1 = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId1 = (ui32)Schema1::Table1::Value::ColumnId;
        update1.SetColumn(columnId1,
            TTypeInfo(NUdf::TDataType<NUdf::TUtf8>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("asd"));

        TVector<TSelectColumn> columns1;
        columns1.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto oldRow1 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes1, columns1, row1);
        auto value1 = pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id), keyTypes1, row1, update1);

        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        TRuntimeNode::TList row2(1);
        row2[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(11);

        auto update2 = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId2 = (ui32)Schema2::Table2::Value::ColumnId;
        update2.SetColumn(columnId2,
            TTypeInfo(NUdf::TDataType<ui32>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(32));

        TVector<TSelectColumn> columns2;
        columns2.emplace_back("4", (ui32)Schema2::Table2::Value::ColumnId, TTypeInfo(Schema2::Table2::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto oldRow2 = pgmBuilder.SelectRow(TTableId(OwnerId, Table2Id), keyTypes2, columns2, row2);
        auto value2 = pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), keyTypes2, row2, update2);
        TRuntimeNode::TList resList;
        resList.push_back(value1);
        resList.push_back(value2);
        auto prevValue1 = pgmBuilder.FlatMap(oldRow1, [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "2");
        });

        auto prevValue2 = pgmBuilder.FlatMap(oldRow2, [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "4");
        });

        const TString str(simulateFail1 ? "XXX" : "qwe");
        auto predicate = pgmBuilder.And({
            pgmBuilder.Exists(prevValue1), pgmBuilder.AggrEquals(pgmBuilder.Unwrap(prevValue1, pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(str)),
            pgmBuilder.Exists(prevValue2), pgmBuilder.AggrEquals(pgmBuilder.Unwrap(prevValue2, pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(simulateFail2 ? 999 : 67))});

        auto pgm = pgmBuilder.Build(pgmBuilder.If(predicate, pgmBuilder.AsList(resList), pgmBuilder.AsList(pgmBuilder.Abort())));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver),
            (simulateFail1 || simulateFail2) ? IEngineFlat::EStatus::Aborted : IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(selectRes.HaveValue<Schema1::Table1::Value>());
            UNIT_ASSERT_VALUES_EQUAL(selectRes.GetValue<Schema1::Table1::Value>(),
                (simulateFail1 || simulateFail2) ? "qwe" : "asd");
            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            auto selectRes = db.Table<Schema2::Table2>()
                .Key(ui64(11))
                .Select<Schema2::Table2::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            UNIT_ASSERT(selectRes.HaveValue<Schema2::Table2::Value>());
            UNIT_ASSERT_VALUES_EQUAL(selectRes.GetValue<Schema2::Table2::Value>(),
                (simulateFail1 || simulateFail2) ? 67 : 32);
            driver.ShardDbState.CommitTransaction(Shard2);
        }
    }

    Y_UNIT_TEST(TestCASBoth2Success) {
        TestCASBoth2Impl(false, false);
    }

    Y_UNIT_TEST(TestCASBoth2Fail1) {
        TestCASBoth2Impl(true, false);
    }

    Y_UNIT_TEST(TestCASBoth2Fail2) {
        TestCASBoth2Impl(false, true);
    }

    Y_UNIT_TEST(TestCASBoth2Fail12) {
        TestCASBoth2Impl(true, true);
    }

    Y_UNIT_TEST(TestSelectRowNoShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(42);
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto value = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &NullShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Optional
            Optional {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "2"
                    Type {
                      Kind: Optional
                      Optional {
                        Item {
                          Kind: Data
                          Data {
                            Scheme: 4608
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeNoShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &NullShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestUpdateRowNoShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto update = pgmBuilder.GetUpdateRowBuilder();
        ui32 columnId = (ui32)Schema1::Table1::Value::ColumnId;
        update.SetColumn(columnId,
            TTypeInfo(NUdf::TDataType<NUdf::TUtf8>::Id),
            pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>("qwe"));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id),
            keyTypes, row, update)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &NullShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && !selectRes.IsValid());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestEraseRowNoShards) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList row(1);
        row[0] = pgmBuilder.Add(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(40),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(2));

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.EraseRow(TTableId(OwnerId, Table1Id), keyTypes, row)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &NullShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            auto selectRes = db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Select<Schema1::Table1::Value>();

            UNIT_ASSERT(selectRes.IsReady() && selectRes.IsValid());
            driver.ShardDbState.CommitTransaction(Shard1);
        }
    }

    Y_UNIT_TEST(TestSelectRangeWithPartitions) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeWithPartitionsTruncatedByItems) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.ItemsLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeWithPartitionsTruncatedByBytes) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("zxc"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        const ui64 RowOverheadBytes = 8;
        options.BytesLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(4 + RowOverheadBytes);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeReverseWithPartitions) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("fgh"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(45))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("rty"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.Reverse = pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "rty"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "fgh"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "asd"
            }
          }
        }
      }
      Struct {
        Bool: false
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeReverseWithPartitionsTruncatedByItems1) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("fgh"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(45))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("rty"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.ItemsLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1);
        options.Reverse = pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "rty"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeReverseWithPartitionsTruncatedByItems2) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("fgh"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(45))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("rty"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.ItemsLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2);
        options.Reverse = pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "rty"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestSelectRangeReverseWithPartitionsTruncatedByItems3) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);
            db.Table<Schema1::Table1>()
                .Key(ui32(42))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("asd"));
            db.Table<Schema1::Table1>()
                .Key(ui32(43))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("fgh"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema1::Table1>()
                .Key(ui32(44))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("qwe"));
            db.Table<Schema1::Table1>()
                .Key(ui32(45))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("rty"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TSelectColumn> columns;
        columns.emplace_back("2", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        options.ItemsLimit = pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(3);
        options.Reverse = pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true);
        auto value = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Struct
            Struct {
              Member {
                Name: "List"
                Type {
                  Kind: List
                  List {
                    Item {
                      Kind: Struct
                      Struct {
                        Member {
                          Name: "2"
                          Type {
                            Kind: Optional
                            Optional {
                              Item {
                                Kind: Data
                                Data {
                                  Scheme: 4608
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
              Member {
                Name: "Truncated"
                Type {
                  Kind: Data
                  Data {
                    Scheme: 6
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Struct {
        List {
          Struct {
            Optional {
              Text: "rty"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "qwe"
            }
          }
        }
        List {
          Struct {
            Optional {
              Text: "fgh"
            }
          }
        }
      }
      Struct {
        Bool: true
      }
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestBug998) {
        TDriver driver;
        auto& pgmBuilder = driver.PgmBuilder;
        auto value1 = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(4294967172);
        auto value2 = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(123);
        auto value = pgmBuilder.Add(value1, value2);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("myRes", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &EmptyShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "myRes"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: Data
            Data {
              Scheme: 2
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
      Uint32: 4294967295
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestAcquireLocks) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        auto& pgmBuilder = driver.PgmBuilder;

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.AcquireLocks(
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(0))));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();

        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "__tx_locks"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: List
            List {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "Counter"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "DataShard"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "Generation"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 2
                      }
                    }
                  }
                  Member {
                    Name: "LockId"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    Member {
      Name: "__tx_locks2"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: List
            List {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "Counter"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "DataShard"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "Generation"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 2
                      }
                    }
                  }
                  Member {
                    Name: "LockId"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "PathId"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "SchemeShard"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
    }
  }
  Struct {
    Optional {
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestDiagnostics) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        auto& pgmBuilder = driver.PgmBuilder;

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.Diagnostics()));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);
        auto resStr = res.DebugString();
        //Cerr << resStr << Endl;

        auto expectedStr = R"___(Type {
  Kind: Struct
  Struct {
    Member {
      Name: "__tx_info"
      Type {
        Kind: Optional
        Optional {
          Item {
            Kind: List
            List {
              Item {
                Kind: Struct
                Struct {
                  Member {
                    Name: "ActorIdRawX1"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "ActorIdRawX2"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "ExecLatency"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "GenStep"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "Generation"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 2
                      }
                    }
                  }
                  Member {
                    Name: "IsFollower"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 6
                      }
                    }
                  }
                  Member {
                    Name: "PrepareArriveTime"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "ProposeLatency"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "Status"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 2
                      }
                    }
                  }
                  Member {
                    Name: "TabletId"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "TxId"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                  Member {
                    Name: "TxStep"
                    Type {
                      Kind: Data
                      Data {
                        Scheme: 4
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
Value {
  Struct {
    Optional {
    }
  }
}
)___";
        UNIT_ASSERT_STRINGS_EQUAL(resStr, expectedStr);
    }

    Y_UNIT_TEST(TestMapsPushdown) {
        const TString okValue = "Ok";

        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>(okValue));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>(okValue));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>(okValue));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>(okValue));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");
        TVector<TStringBuf> filterNullColumns {"Value"};
        auto filterNull = pgmBuilder.FilterNullMembers(list, filterNullColumns);
        auto filtered = pgmBuilder.Filter(filterNull, [&pgmBuilder, &okValue](TRuntimeNode item) {
            return pgmBuilder.AggrEquals(
                pgmBuilder.Member(item, "Value"),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(okValue));
        });
        auto mapped = pgmBuilder.Map(filtered, [&pgmBuilder](TRuntimeNode item) {
            return pgmBuilder.AddMember(item, "MappedID", pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10)));
        });
        auto flatmapped = pgmBuilder.FlatMap(mapped, [&pgmBuilder](TRuntimeNode item) {
            TString prefix = "mapped_";
            auto prefixNode = pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::String>(prefix);

            return pgmBuilder.AsList(pgmBuilder.AddMember(item, "RemappedId", pgmBuilder.Concat(
                prefixNode, pgmBuilder.ToString(pgmBuilder.Coalesce(pgmBuilder.Member(item, "MappedID"),
                    pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(0))))));
        });
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", flatmapped)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(program.Contains("Map"));
            UNIT_ASSERT(program.Contains("FlatMap"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["1"];["11"];"mapped_11";"Ok"];
            [["5"];["15"];"mapped_15";"Ok"];
            [["4"];["14"];"mapped_14";"Ok"];
            [["6"];["16"];"mapped_16";"Ok"]
        ]]])___", res);
    }

    Y_UNIT_TEST(NoMapPushdownMultipleConsumers) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");
        auto mapped = pgmBuilder.Map(list, [&pgmBuilder](TRuntimeNode item) {
            return pgmBuilder.AddMember(item, "MappedID", pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10)));
        });

        TRuntimeNode::TList results;
        results.push_back(pgmBuilder.SetResult("Result1", mapped));
        results.push_back(pgmBuilder.SetResult("Result2", list));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(results));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);
            UNIT_ASSERT(!program.Contains("Map"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([
            [[
                [["1"];["11"];["Ok"]];
                [["3"];["13"];["Bad"]];
                [["5"];["15"];["Ok"]]
            ]];
            [[
                [["1"];["Ok"]];
                [["3"];["Bad"]];
                [["5"];["Ok"]]
            ]]
        ])___", res);
    }

    Y_UNIT_TEST(NoMapPushdownNonPureLambda) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");
        auto mapped = pgmBuilder.Map(list, [&pgmBuilder, &keyTypes, &columns](TRuntimeNode item) {
            TRuntimeNode::TList row(1);
            row[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(3);

            return pgmBuilder.AddMember(item, "Fetch",
                pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row));
        });

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", mapped)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);
            UNIT_ASSERT(!program.Contains("Map"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [[[["3"];["Bad"]]];["1"];["Ok"]];
            [[[["3"];["Bad"]]];["3"];["Bad"]];
            [[[["3"];["Bad"]]];["5"];["Ok"]]
        ]]])___", res);
    }

    Y_UNIT_TEST(NoOrderedMapPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        const TString okValue = "Ok";
        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");
        auto filtered = pgmBuilder.OrderedFilter(list, [&pgmBuilder, &okValue](TRuntimeNode item) {
            return pgmBuilder.AggrEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(okValue));
        });
        auto mapped = pgmBuilder.OrderedMap(filtered, [&pgmBuilder](TRuntimeNode item) {
            return pgmBuilder.AddMember(item, "MappedID", pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10)));
        });
        auto flatmapped = pgmBuilder.OrderedFlatMap(mapped, [&pgmBuilder](TRuntimeNode item) {
            TString prefix = "mapped_";
            auto prefixNode = pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::String>(prefix);

            return pgmBuilder.AsList(pgmBuilder.AddMember(item, "RemappedId", pgmBuilder.Concat(
                prefixNode, pgmBuilder.ToString(pgmBuilder.Coalesce(pgmBuilder.Member(item, "MappedID"),
                    pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(0))))));
        });

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", flatmapped)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);
            UNIT_ASSERT(!program.Contains("Filter"));
            UNIT_ASSERT(!program.Contains("OrderedFilter"));
            UNIT_ASSERT(!program.Contains("Map"));
            UNIT_ASSERT(!program.Contains("OrderedMap"));
            UNIT_ASSERT(!program.Contains("FlatMap"));
            UNIT_ASSERT(!program.Contains("OrderedFlatMap"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["1"];["11"];"mapped_11";["Ok"]];
            [["5"];["15"];"mapped_15";["Ok"]]
        ]]])___", res);
    }

    Y_UNIT_TEST(NoMapPushdownWriteToTable) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);
            db.Table<Schema2::Table2>()
                .Key(ui64(0))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(0));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");
        auto mapped = pgmBuilder.Map(list, [&pgmBuilder](TRuntimeNode item) {
            return pgmBuilder.AddMember(item, "MappedID", pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10)));
        });

        auto write = pgmBuilder.Map(mapped, [&pgmBuilder](TRuntimeNode item) {
            TVector<TTypeInfo> keyTypes(1);
            keyTypes[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);

            TRuntimeNode::TList row(1);
            row[0] = pgmBuilder.Convert(pgmBuilder.Member(item, "ID"), pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id)));

            auto update = pgmBuilder.GetUpdateRowBuilder();
            ui32 columnId = (ui32)Schema2::Table2::Value::ColumnId;
            update.SetColumn(columnId, TTypeInfo(NUdf::TDataType<ui32>::Id), pgmBuilder.Member(item, "ID"));
            return pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), keyTypes, row, update);
        });

        auto pgm = pgmBuilder.Build(write);

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            if (shard == Shard1) {
                UNIT_ASSERT(!program.Contains("Map"));
            }
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);

        {
            TVector<TSelectColumn> columns;
            columns.emplace_back("ID", (ui32)Schema2::Table2::ID::ColumnId, TTypeInfo(Schema2::Table2::ID::ColumnType),
                EColumnTypeConstraint::Nullable);
            columns.emplace_back("Value", (ui32)Schema2::Table2::Value::ColumnId, TTypeInfo(Schema2::Table2::Value::ColumnType),
                EColumnTypeConstraint::Nullable);

            auto options = pgmBuilder.GetDefaultTableRangeOptions();
            TVector<TTypeInfo> keyTypes(1);
            keyTypes[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
            TRuntimeNode::TList rowFrom(1);
            rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id);
            options.FromColumns = rowFrom;
            options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
            auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table2Id), keyTypes, columns, options);
            auto checkpgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", range)));

            NKikimrMiniKQL::TResult res;
            UNIT_ASSERT_EQUAL(driver.Run(checkpgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);

            NKqp::CompareYson(R"___([[[[
                [["0"];["0"]];
                [["1"];["1"]];
                [["3"];["3"]];
                [["5"];["5"]]];
                %false]]])___", res);
        }
    }

    Y_UNIT_TEST(NoMapPushdownArgClosure) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema2::Table2>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(20));
            db.Table<Schema2::Table2>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(40));


            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns1;
        columns1.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns1.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto options1 = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom1(1);
        rowFrom1[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options1.FromColumns = rowFrom1;
        options1.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range1 = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes1, columns1, options1);
        auto list1 = pgmBuilder.Member(range1, "List");

        TVector<TSelectColumn> columns2;
        columns2.emplace_back("ID", (ui32)Schema2::Table2::ID::ColumnId, TTypeInfo(Schema2::Table2::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns2.emplace_back("Value", (ui32)Schema2::Table2::Value::ColumnId, TTypeInfo(Schema2::Table2::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto options2 = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        TRuntimeNode::TList rowFrom2(1);
        rowFrom2[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id);
        options2.FromColumns = rowFrom2;
        options2.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range2 = pgmBuilder.SelectRange(TTableId(OwnerId, Table2Id), keyTypes2, columns2, options2);
        auto list2 = pgmBuilder.Member(range2, "List");

        auto flatmapped = pgmBuilder.FlatMap(list1, [&pgmBuilder, &list2](TRuntimeNode item1) {
            auto mapped = pgmBuilder.Map(list2, [&pgmBuilder, &item1](TRuntimeNode item2) {
                return pgmBuilder.NewTuple({item1, item2});
            });

            return mapped;
        });

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", flatmapped)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(!program.Contains("Map"));
            UNIT_ASSERT(!program.Contains("FlatMap"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [[["1"];["Ok"]];[["2"];["20"]]];
            [[["1"];["Ok"]];[["4"];["40"]]];
            [[["3"];["Bad"]];[["2"];["20"]]];
            [[["3"];["Bad"]];[["4"];["40"]]]
        ]]])___", res);
    }

    Y_UNIT_TEST(TestSomePushDown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema2::Table2>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(20));
            db.Table<Schema2::Table2>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(40));


            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns1;
        columns1.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns1.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto options1 = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom1(1);
        rowFrom1[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options1.FromColumns = rowFrom1;
        options1.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range1 = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes1, columns1, options1);
        auto list1 = pgmBuilder.Member(range1, "List");

        TVector<TSelectColumn> columns2;
        columns2.emplace_back("ID", (ui32)Schema2::Table2::ID::ColumnId, TTypeInfo(Schema2::Table2::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns2.emplace_back("Value", (ui32)Schema2::Table2::Value::ColumnId, TTypeInfo(Schema2::Table2::Value::ColumnType),
            EColumnTypeConstraint::Nullable);
        auto options2 = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        TRuntimeNode::TList rowFrom2(1);
        rowFrom2[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id);
        options2.FromColumns = rowFrom2;
        options2.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range2 = pgmBuilder.SelectRange(TTableId(OwnerId, Table2Id), keyTypes2, columns2, options2);
        auto list2 = pgmBuilder.Member(range2, "List");

        auto mapped = pgmBuilder.Map(list2, [&pgmBuilder](TRuntimeNode item2) {
            return pgmBuilder.Member(item2, "Value");
        });

        auto flatmapped = pgmBuilder.FlatMap(list1, [&pgmBuilder, &mapped](TRuntimeNode item1) {
            return pgmBuilder.AsList({
                pgmBuilder.NewTuple({pgmBuilder.ToString(pgmBuilder.Member(item1, "ID")), mapped}),
                pgmBuilder.NewTuple({pgmBuilder.ToString(pgmBuilder.Member(item1, "Value")), mapped})
            });
        });

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", flatmapped)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            if (shard == Shard1) {
                UNIT_ASSERT(!program.Contains("Map"));
                UNIT_ASSERT(!program.Contains("FlatMap"));
            } else {
                UNIT_ASSERT(program.Contains("Map"));
                UNIT_ASSERT(!program.Contains("FlatMap"));
            }
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["1"];[["20"];["40"]]];
            [["Ok"];[["20"];["40"]]];
            [["3"];[["20"];["40"]]];
            [["Bad"];[["20"];["40"]]]
        ]]])___", res);
    }

    Y_UNIT_TEST(TestCombineByKeyPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        auto preMap = pgmBuilder.FlatMap(list, [&](TRuntimeNode item) {
            return pgmBuilder.NewOptional(item);
        });

        auto dict = pgmBuilder.ToHashedDict(preMap, true, [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "Value");
        }, [&](TRuntimeNode item) {
            return item;
        });

        auto values = pgmBuilder.DictItems(dict);

        auto agg = pgmBuilder.FlatMap(values, [&](TRuntimeNode item) {
            auto key = pgmBuilder.Nth(item, 0);
            auto payloadList = pgmBuilder.Nth(item, 1);
            auto fold1 = pgmBuilder.Fold1(payloadList, [&](TRuntimeNode item2) {
                std::vector<std::pair<std::string_view, TRuntimeNode>> fields {
                    {"Key", key},
                    {"Sum", pgmBuilder.Member(item2, "ID")}
                };
                return pgmBuilder.NewOptional(pgmBuilder.NewStruct(fields));
            }, [&](TRuntimeNode item2, TRuntimeNode state) {
                std::vector<std::pair<std::string_view, TRuntimeNode>> fields {
                    {"Key", pgmBuilder.Member(state, "Key")},
                    {"Sum", pgmBuilder.AggrAdd(pgmBuilder.Member(state, "Sum"), pgmBuilder.Member(item2, "ID"))}
                };
                return pgmBuilder.NewOptional(pgmBuilder.NewStruct(fields));
            });

            auto res = pgmBuilder.FlatMap(fold1, [&](TRuntimeNode state) {
                return state;
            });

            return res;
        });

        auto merge = pgmBuilder.CombineByKeyMerge(agg);

        auto sorted = pgmBuilder.Sort(merge,
            pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true),
            [&pgmBuilder](TRuntimeNode item) {
                return pgmBuilder.Member(item, "Key");
            });

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", sorted)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("FlatMap"));
            UNIT_ASSERT(program.Contains("ToHashedDict"));
            UNIT_ASSERT(program.Contains("DictItems"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["Value1"];["6"]];
            [["Value2"];["3"]];
            [["Value2"];["8"]];
            [["Value3"];["4"]]
        ]]])___", res);
    }

    Y_UNIT_TEST(TestCombineByKeyNoPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        auto dict = pgmBuilder.ToHashedDict(list, true, [&](TRuntimeNode item) {
            return pgmBuilder.Member(item, "Value");
        }, [&](TRuntimeNode item) {
            return item;
        });

        auto values = pgmBuilder.DictItems(dict);

        auto agg = pgmBuilder.FlatMap(values, [&](TRuntimeNode item) {
            auto key = pgmBuilder.Nth(item, 0);
            auto payloadList = pgmBuilder.Nth(item, 1);
            auto fold1 = pgmBuilder.Fold1(payloadList, [&](TRuntimeNode item2) {
                std::vector<std::pair<std::string_view, TRuntimeNode>> fields {
                    {"Key", key},
                    {"Sum", pgmBuilder.Member(item2, "ID")}
                };
                return pgmBuilder.NewOptional(pgmBuilder.NewStruct(fields));
            }, [&](TRuntimeNode item2, TRuntimeNode state) {
                std::vector<std::pair<std::string_view, TRuntimeNode>> fields {
                    {"Key", pgmBuilder.Member(state, "Key")},
                    {"Sum", pgmBuilder.AggrAdd(pgmBuilder.Member(state, "Sum"), pgmBuilder.Member(item2, "ID"))}
                };
                return pgmBuilder.NewOptional(pgmBuilder.NewStruct(fields));
            });

            auto res = pgmBuilder.FlatMap(fold1, [&](TRuntimeNode state) {
                return state;
            });

            return res;
        });

        auto merge = pgmBuilder.CombineByKeyMerge(agg);

        auto sorted = pgmBuilder.Sort(merge,
            pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true),
            [&pgmBuilder](TRuntimeNode item) {
                return pgmBuilder.Member(item, "Key");
            });

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", sorted)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(!program.Contains("FlatMap"));
            UNIT_ASSERT(!program.Contains("ToHashedDict"));
            UNIT_ASSERT(!program.Contains("DictItems"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["Value1"];["6"]];
            [["Value2"];["11"]];
            [["Value3"];["4"]]
        ]]])___", res);
    }

    Y_UNIT_TEST(TestTakePushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        const TString filterValue = "Value1";
        auto filtered = pgmBuilder.Filter(list, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto take = pgmBuilder.Take(filtered, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", take)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(program.Contains("Take"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["3"];["Value2"]];
            [["2"];["Value2"]]
        ]]])___", res);
    }

    Y_UNIT_TEST(TestNoOrderedTakePushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        auto take = pgmBuilder.Take(list, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(4));

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", take)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(!program.Contains("Take"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[
            [["1"];["Value1"]];
            [["2"];["Value2"]];
            [["3"];["Value1"]];
            [["4"];["Value2"]]
        ]]])___", res);
    }

    Y_UNIT_TEST(TestLengthPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        const TString filterValue = "Value2";
        auto filtered = pgmBuilder.Filter(list, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto length = pgmBuilder.Length(filtered);

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", length)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(program.Contains("Length"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([["3"]])___", res);
    }

    Y_UNIT_TEST(TestNoAggregatedPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        auto mapped = pgmBuilder.Map(list, [&pgmBuilder](TRuntimeNode item) {
            return pgmBuilder.AddMember(item, "MappedID", pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10)));
        });

        auto take = pgmBuilder.Take(mapped, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));

        const TString filterValue = "Value2";
        auto filtered = pgmBuilder.Filter(take, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });
        auto length = pgmBuilder.Length(filtered);

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", length)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Map"));
            UNIT_ASSERT(program.Contains("Take"));
            UNIT_ASSERT(!program.Contains("Filter"));
            UNIT_ASSERT(!program.Contains("Length"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([["1"]])___", res);
    }

    Y_UNIT_TEST(TestTopSortPushdownPk) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value5"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value4"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value6"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        const TString filterValue = "Value1";
        auto filtered = pgmBuilder.Filter(list, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto partialTake = pgmBuilder.PartialTake(filtered, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));
        auto sort = pgmBuilder.Sort(partialTake, pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(true),
            [&pgmBuilder] (TRuntimeNode item) {
                return pgmBuilder.Member(item, "ID");
            });

        auto take = pgmBuilder.Take(sort, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", take)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(program.Contains("Take"));
            UNIT_ASSERT(!program.Contains("Sort"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[[["2"];["Value2"]];[["3"];["Value3"]]]]])___", res);
    }

    Y_UNIT_TEST(TestTopSortPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value5"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value4"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value6"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        const TString filterValue = "Value6";
        auto filtered = pgmBuilder.Filter(list, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto partialSort = pgmBuilder.PartialSort(filtered, pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(false),
            [&pgmBuilder] (TRuntimeNode item) {
                return pgmBuilder.Member(item, "Value");
            });
        auto partialTake = pgmBuilder.PartialTake(partialSort, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));
        auto sort = pgmBuilder.Sort(partialTake, pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(false),
            [&pgmBuilder] (TRuntimeNode item) {
                return pgmBuilder.Member(item, "Value");
            });

        auto take = pgmBuilder.Take(sort, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", take)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(program.Contains("Take"));
            UNIT_ASSERT(program.Contains("Sort"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[[["5"];["Value5"]];[["4"];["Value4"]]]]])___", res);
    }

    Y_UNIT_TEST(TestTopSortNonImmediatePushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value5"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value4"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value6"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        const TString filterValue = "Value6";
        auto filtered = pgmBuilder.Filter(list, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto partialSort = pgmBuilder.PartialSort(filtered, pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(false),
            [&pgmBuilder] (TRuntimeNode item) {
                return pgmBuilder.Member(item, "Value");
            });

        auto takeCount = pgmBuilder.Add(pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1),
            pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1));

        auto partialTake = pgmBuilder.PartialTake(partialSort, takeCount);
        auto sort = pgmBuilder.Sort(partialTake, pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(false),
            [&pgmBuilder] (TRuntimeNode item) {
                return pgmBuilder.Member(item, "Value");
            });

        auto take = pgmBuilder.Take(sort, takeCount);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", take)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(program.Contains("Take"));
            UNIT_ASSERT(program.Contains("Sort"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[[["5"];["Value5"]];[["4"];["Value4"]]]]])___", res);
    }

    Y_UNIT_TEST(TestNoPartialSortPushdown) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value5"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema1::Table1>()
                .Key(ui64(2))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value2"));
            db.Table<Schema1::Table1>()
                .Key(ui64(4))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value4"));
            db.Table<Schema1::Table1>()
                .Key(ui64(6))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value6"));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType),
            EColumnTypeConstraint::Nullable);

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, columns, options);
        auto list = pgmBuilder.Member(range, "List");

        const TString filterValue = "Value6";
        auto filtered = pgmBuilder.Filter(list, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto partialSort = pgmBuilder.PartialSort(filtered, pgmBuilder.TProgramBuilder::NewDataLiteral<bool>(false),
            [&pgmBuilder] (TRuntimeNode item) {
                return pgmBuilder.Member(item, "Value");
            });

        const TString filter2Value = "Value5";
        auto filtered2 = pgmBuilder.Filter(partialSort, [&pgmBuilder, &filter2Value](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"), pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filter2Value));
        });

        auto take = pgmBuilder.Take(filtered2, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", take)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);

            UNIT_ASSERT(program.Contains("Filter"));
            UNIT_ASSERT(!program.Contains("Take"));
            UNIT_ASSERT(!program.Contains("Sort"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([[[[["4"];["Value4"]];[["3"];["Value3"]]]]])___", res);
    }

    Y_UNIT_TEST(TestSelectRangeNoColumns) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(ui64(1))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));
            db.Table<Schema1::Table1>()
                .Key(ui64(3))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Bad"));
            db.Table<Schema1::Table1>()
                .Key(ui64(5))
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Ok"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        auto options = pgmBuilder.GetDefaultTableRangeOptions();
        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TRuntimeNode::TList rowFrom(1);
        rowFrom[0] = pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id);
        options.FromColumns = rowFrom;
        options.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        auto range = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes, {}, options);
        auto list = pgmBuilder.Member(range, "List");
        auto length = pgmBuilder.Length(list);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("Result", length)));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            Y_UNUSED(shard);
            UNIT_ASSERT(!program.Contains("Map"));
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &SingleShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([["3"]])___", res);
    }

    Y_UNIT_TEST(TestInternalResult) {
        TDriver driver;
        auto& pgmBuilder = driver.PgmBuilder;
        auto value = pgmBuilder.TProgramBuilder::TProgramBuilder::NewDataLiteral<ui32>(42);
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(pgmBuilder.SetResult("__cantuse", value)));

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &EmptyShardResolver), IEngineFlat::EStatus::Error);

        Cout << res.DebugString() << Endl;
    }

    Y_UNIT_TEST(TestIndependentSelects) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        {
            driver.ShardDbState.BeginTransaction(Shard1);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard1]);

            db.Table<Schema1::Table1>()
                .Key(1)
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value1"));
            db.Table<Schema1::Table1>()
                .Key(3)
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value3"));
            db.Table<Schema1::Table1>()
                .Key(5)
                .Update(NIceDb::TUpdate<Schema1::Table1::Value>("Value5"));

            driver.ShardDbState.CommitTransaction(Shard1);
        }

        {
            driver.ShardDbState.BeginTransaction(Shard2);
            NIceDb::TNiceDb db(*driver.ShardDbState.Dbs[Shard2]);

            db.Table<Schema2::Table2>()
                .Key(2)
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(20));
            db.Table<Schema2::Table2>()
                .Key(4)
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(40));
            db.Table<Schema2::Table2>()
                .Key(6)
                .Update(NIceDb::TUpdate<Schema2::Table2::Value>(50));

            driver.ShardDbState.CommitTransaction(Shard2);
        }

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TSelectColumn> columns1 {
            {"ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType), EColumnTypeConstraint::Nullable},
            {"Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType), EColumnTypeConstraint::Nullable}
        };

        TRuntimeNode::TList fromColumns1{pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui32>::Id)};
        auto options1 = pgmBuilder.GetDefaultTableRangeOptions();
        options1.FromColumns = fromColumns1;
        options1.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        TVector<TTypeInfo> keyTypes1(1);
        keyTypes1[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        auto range1 = pgmBuilder.SelectRange(TTableId(OwnerId, Table1Id), keyTypes1, columns1, options1);
        auto list1 = pgmBuilder.Member(range1, "List");

        const TString filterValue = "Value6";
        auto filtered1 = pgmBuilder.Filter(list1, [&pgmBuilder, &filterValue](TRuntimeNode item) {
            return pgmBuilder.AggrNotEquals(
                pgmBuilder.Unwrap(pgmBuilder.Member(item, "Value"),
                pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(""), "", 0, 0),
                pgmBuilder.TProgramBuilder::NewDataLiteral<NUdf::EDataSlot::Utf8>(filterValue));
        });

        auto take1 = pgmBuilder.Take(filtered1, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));

        TVector<TSelectColumn> columns2 {
            {"ID", (ui32)Schema2::Table2::ID::ColumnId, TTypeInfo(Schema2::Table2::ID::ColumnType), EColumnTypeConstraint::Nullable},
            {"Value", (ui32)Schema2::Table2::Value::ColumnId, TTypeInfo(Schema2::Table2::Value::ColumnType), EColumnTypeConstraint::Nullable}
        };

        TRuntimeNode::TList fromColumns2{pgmBuilder.NewEmptyOptionalDataLiteral(NUdf::TDataType<ui64>::Id)};
        auto options2 = pgmBuilder.GetDefaultTableRangeOptions();
        options2.FromColumns = fromColumns2;
        options2.Flags = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(TReadRangeOptions::TFlags::IncludeTermValue);
        TVector<TTypeInfo> keyTypes2(1);
        keyTypes2[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);
        auto range2 = pgmBuilder.SelectRange(TTableId(OwnerId, Table2Id), keyTypes2, columns2, options2);
        auto list2 = pgmBuilder.Member(range2, "List");

        auto mapped2 = pgmBuilder.Map(list2, [&pgmBuilder](TRuntimeNode item) {
            return pgmBuilder.AddMember(item, "MappedID", pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10)));
        });

        auto take2 = pgmBuilder.Take(mapped2, pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2));

        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(
            pgmBuilder.SetResult("Result", pgmBuilder.NewTuple({take1, take2}))
        ));

        driver.ShardProgramInspector = [] (ui64 shard, const TString& program) {
            if (shard == Shard1) {
                UNIT_ASSERT(program.Contains("Filter"));
            }
        };

        driver.ReadSetsInspector = [] (const TDriver::TOutgoingReadsets& outReadsets,
            const TDriver::TIncomingReadsets& inReadsets)
        {
            UNIT_ASSERT(outReadsets.empty());
            UNIT_ASSERT(inReadsets.empty());
        };

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver), IEngineFlat::EStatus::Complete);

        NKqp::CompareYson(R"___([
            [[[[["1"];["Value1"]];[["3"];["Value3"]]];
            [[["2"];["12"];["20"]];[["4"];["14"];["40"]]]]]
        ])___", res);
    }

    Y_UNIT_TEST(TestMultiRSPerDestination) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);

        TRuntimeNode::TList row1(1);
        row1[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(1);
        auto value1 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row1);

        TRuntimeNode::TList row2(1);
        row2[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(3);
        auto value2 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row2);

        TRuntimeNode::TList row3(1);
        row3[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(5);
        auto value3 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row3);

        auto list = pgmBuilder.AsList({value1, value2, value3});

        auto mapped = pgmBuilder.Map(list, [&pgmBuilder, &keyTypes](TRuntimeNode item) {
            auto update = pgmBuilder.GetUpdateRowBuilder();

            TRuntimeNode::TList row(1);
            row[0] = pgmBuilder.Add(
                pgmBuilder.Member(item, "ID"),
                pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10));

            return pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id), keyTypes, row, update);
        });

        auto pgm = pgmBuilder.Build(mapped);

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver,
            IEngineFlat::TShardLimits(2, 2)), IEngineFlat::EStatus::Complete);

        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &DoubleShardResolver,
            IEngineFlat::TShardLimits(2, 1)), IEngineFlat::EStatus::Error);
        Cerr << res.DebugString() << Endl;
    }

    Y_UNIT_TEST(TestCrossTableRs) {
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema2>(Shard2);

        auto& pgmBuilder = driver.PgmBuilder;

        TVector<TTypeInfo> keyTypes(1);
        keyTypes[0] = TTypeInfo(NUdf::TDataType<ui32>::Id);
        TVector<TSelectColumn> columns;
        columns.emplace_back("ID", (ui32)Schema1::Table1::ID::ColumnId, TTypeInfo(Schema1::Table1::ID::ColumnType),
            EColumnTypeConstraint::Nullable);

        TVector<TTypeInfo> key2Types(1);
        key2Types[0] = TTypeInfo(NUdf::TDataType<ui64>::Id);

        TRuntimeNode::TList row1(1);
        row1[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(1);
        auto value1 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row1);

        TRuntimeNode::TList row2(1);
        row2[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(3);
        auto value2 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row2);

        TRuntimeNode::TList row3(1);
        row3[0] = pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(5);
        auto value3 = pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), keyTypes, columns, row3);

        auto list = pgmBuilder.AsList({value1, value2, value3});

        auto mapped = pgmBuilder.Map(list, [&pgmBuilder, &key2Types](TRuntimeNode item) {
            auto update = pgmBuilder.GetUpdateRowBuilder();

            TRuntimeNode::TList row(1);
            row[0] = pgmBuilder.Add(
                pgmBuilder.Convert(pgmBuilder.Member(item, "ID"), pgmBuilder.NewOptionalType(pgmBuilder.NewDataType(NUdf::TDataType<ui64>::Id))),
                pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10));

            return pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), key2Types, row, update);
        });

        auto pgm = pgmBuilder.Build(mapped);

        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver,
            IEngineFlat::TShardLimits(2, 1)), IEngineFlat::EStatus::Complete);

        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, &TwoShardResolver,
            IEngineFlat::TShardLimits(2, 0)), IEngineFlat::EStatus::Error);
        Cerr << res.DebugString() << Endl;
    }

    // Point SelectRows on one shard plus one dynamic EraseRow spanning the other shards.
    // The text is the deserialized program, so sharing must keep the same nodes.
    Y_UNIT_TEST(DeleteShapedShardProgramsStaySerialized) {
        TDriver driver;
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> readKeyTypes{TTypeInfo(NUdf::TDataType<ui64>::Id)};
        TVector<TSelectColumn> columns;
        columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType), EColumnTypeConstraint::Nullable);
        TRuntimeNode::TList effects;
        for (ui64 key : {ui64(1), ui64(2), ui64(3)}) {
            TRuntimeNode::TList row{pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(key)};
            effects.push_back(pgmBuilder.SetResult(TStringBuilder() << "row" << key, pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), readKeyTypes, columns, row)));
        }
        TVector<TTypeInfo> eraseKeyTypes{TTypeInfo(NUdf::TDataType<ui64>::Id)};
        TRuntimeNode::TList eraseRow{pgmBuilder.Add(pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1), pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2))};
        effects.push_back(pgmBuilder.EraseRow(TTableId(OwnerId, Table2Id), eraseKeyTypes, eraseRow));
        auto pgm = pgmBuilder.Build(pgmBuilder.AsList(effects));

        const ui64 writeShardA = 201;
        const ui64 writeShardB = 202;
        const ui64 writeShardC = 203;
        auto programs = driver.CollectShardPrograms(pgm, [&](TKeyDesc& key) {
            key.Status = TKeyDesc::EStatus::Ok;
            TVector<TKeyDesc::TPartitionInfo> partitions;
            if (key.RowOperation == TKeyDesc::ERowOperation::Read) {
                partitions.push_back(TKeyDesc::TPartitionInfo(Shard1));
            } else {
                partitions.push_back(TKeyDesc::TPartitionInfo(writeShardA));
                partitions.push_back(TKeyDesc::TPartitionInfo(writeShardB));
                partitions.push_back(TKeyDesc::TPartitionInfo(writeShardC));
            }
            key.Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
        });

        UNIT_ASSERT_EQUAL(programs.size(), 4);
        UNIT_ASSERT(programs.contains(Shard1));
        UNIT_ASSERT_EQUAL(programs.at(writeShardA), programs.at(writeShardB));
        UNIT_ASSERT_EQUAL(programs.at(writeShardA), programs.at(writeShardC));
        UNIT_ASSERT_UNEQUAL(programs.at(Shard1), programs.at(writeShardA));

        const TString readText = ProgramText(programs.at(Shard1), driver.Env);
        const TString writeText = ProgramText(programs.at(writeShardA), driver.Env);
        const TStringBuf expectedRead = R"prog(Struct
  AllReads
    Struct
      3
        SelectRow #3
          257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
          Type Struct Value
          Struct
            Value
              Uint32 2
          Tuple
            Uint64 3
          Uint32 0
      5
        SelectRow #5
          257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
          Type Struct Value
          Struct
            Value
              Uint32 2
          Tuple
            Uint64 2
          Uint32 0
      7
        SelectRow #7
          257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
          Type Struct Value
          Struct
            Value
              Uint32 2
          Tuple
            Uint64 1
          Uint32 0
  MyKeys
    Struct
      MyReads
        Struct
          3
            SelectRow #3
              257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
              Type Struct Value
              Struct
                Value
                  Uint32 2
              Tuple
                Uint64 3
              Uint32 0
          5
            SelectRow #5
              257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
              Type Struct Value
              Struct
                Value
                  Uint32 2
              Tuple
                Uint64 2
              Uint32 0
          7
            SelectRow #7
              257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
              Type Struct Value
              Struct
                Value
                  Uint32 2
              Tuple
                Uint64 1
              Uint32 0
      MyWrites
        Struct
  Run
    Struct
      Reply
        Struct
          3
            SelectRow #3
              257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
              Type Struct Value
              Struct
                Value
                  Uint32 2
              Tuple
                Uint64 3
              Uint32 0
          5
            SelectRow #5
              257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
              Type Struct Value
              Struct
                Value
                  Uint32 2
              Tuple
                Uint64 2
              Uint32 0
          7
            SelectRow #7
              257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
              Type Struct Value
              Struct
                Value
                  Uint32 2
              Tuple
                Uint64 1
              Uint32 0
      Write
        List 0
  ShardsForRead
    Struct
  ShardsToWrite
    Struct
      3
        List 0
      5
        List 0
      7
        List 0
)prog";
        const TStringBuf expectedWrite = R"prog(Struct
  AllReads
    Struct
      3
        SelectRow #3
          257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
          Type Struct Value
          Struct
            Value
              Uint32 2
          Tuple
            Uint64 3
          Uint32 0
      5
        SelectRow #5
          257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
          Type Struct Value
          Struct
            Value
              Uint32 2
          Tuple
            Uint64 2
          Uint32 0
      7
        SelectRow #7
          257 "\xC8\0\0\0\0\0\0\0-\1\0\0\0\0\0\0"
          Type Struct Value
          Struct
            Value
              Uint32 2
          Tuple
            Uint64 1
          Uint32 0
  MyKeys
    Struct
      MyReads
        Struct
      MyWrites
        Struct
          2
            EraseRow #2
              257 "\xC8\0\0\0\0\0\0\0.\1\0\0\0\0\0\0"
              Tuple
                Invoke #1
                  String "Add"
                  Uint64 1
                  Uint64 2
  Run
    Struct
      Reply
        Struct
      Write
        List 1
          EraseRow #2
            257 "\xC8\0\0\0\0\0\0\0.\1\0\0\0\0\0\0"
            Tuple
              Invoke #1
                String "Add"
                Uint64 1
                Uint64 2
  ShardsForRead
    Struct
  ShardsToWrite
    Struct
      3
        List 0
      5
        List 0
      7
        List 0
)prog";
        UNIT_ASSERT_STRINGS_EQUAL(readText, expectedRead);
        UNIT_ASSERT_STRINGS_EQUAL(writeText, expectedWrite);
    }

    // A SelectRow on Shard1 feeds an UpdateRow whose dynamic key spans three write shards with the same program.
    Y_UNIT_TEST(ReadsFeedWritesAcrossSiblingShards) {
        const ui64 writeShards[] = {201, 202, 203};
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        for (ui64 shard : writeShards) {
            driver.ShardDbState.AddShard<Schema2>(shard);
        }
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> key2Types{TTypeInfo(NUdf::TDataType<ui64>::Id)};
        auto mapped = pgmBuilder.Map(SelectTable1Rows(pgmBuilder, {1, 3, 5}), [&](TRuntimeNode item) {
            auto update = pgmBuilder.GetUpdateRowBuilder();
            return pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), key2Types, Table2KeyFromRead(pgmBuilder, item, 10), update);
        });
        auto pgm = pgmBuilder.Build(mapped);
        const auto resolver = MakeShardResolver({
            {{Table1Id, TKeyDesc::ERowOperation::Read}, {Shard1}},
            {{Table2Id, TKeyDesc::ERowOperation::Update}, {writeShards[0], writeShards[1], writeShards[2]}},
        });

        TShardRunCapture capture;
        CaptureShardRun(driver, capture);
        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, resolver, IEngineFlat::TShardLimits(4, 3)), IEngineFlat::EStatus::Complete);

        UNIT_ASSERT_VALUES_EQUAL(capture.Programs.size(), 4);
        UNIT_ASSERT_EQUAL(capture.Programs.at(writeShards[0]), capture.Programs.at(writeShards[1]));
        UNIT_ASSERT_EQUAL(capture.Programs.at(writeShards[0]), capture.Programs.at(writeShards[2]));
        AssertReadsetsOnlyFrom(capture, Shard1, writeShards);

        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, resolver, IEngineFlat::TShardLimits(4, 2)), IEngineFlat::EStatus::Error);
    }

    // Shard signatures: Shard1 {read, update1}, Shard2 {update1}, A and D {erase}, B {erase, update2}, C {update2}.
    // Only A and D may share a program; every write depends on the read, so Shard1 sends a readset to each other shard.
    Y_UNIT_TEST(MixedShardProgramGroups) {
        const ui64 shardA = 201;
        const ui64 shardB = 202;
        const ui64 shardC = 203;
        const ui64 shardD = 204;
        TDriver driver;
        driver.ShardDbState.AddShard<Schema1>(Shard1);
        driver.ShardDbState.AddShard<Schema1>(Shard2);
        for (ui64 shard : {shardA, shardB, shardC, shardD}) {
            driver.ShardDbState.AddShard<Schema2>(shard);
        }
        auto& pgmBuilder = driver.PgmBuilder;
        TVector<TTypeInfo> key1Types{TTypeInfo(NUdf::TDataType<ui32>::Id)};
        TVector<TTypeInfo> key2Types{TTypeInfo(NUdf::TDataType<ui64>::Id)};
        auto rows = SelectTable1Rows(pgmBuilder, {1});
        auto update1 = pgmBuilder.Map(rows, [&](TRuntimeNode item) {
            auto update = pgmBuilder.GetUpdateRowBuilder();
            TRuntimeNode::TList row{pgmBuilder.Add(pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10))};
            return pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id), key1Types, row, update);
        });
        auto erase = pgmBuilder.Map(rows, [&](TRuntimeNode item) {
            return pgmBuilder.EraseRow(TTableId(OwnerId, Table2Id), key2Types, Table2KeyFromRead(pgmBuilder, item, 10));
        });
        auto update2 = pgmBuilder.Map(rows, [&](TRuntimeNode item) {
            auto update = pgmBuilder.GetUpdateRowBuilder();
            return pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), key2Types, Table2KeyFromRead(pgmBuilder, item, 20), update);
        });
        auto pgm = pgmBuilder.Build(pgmBuilder.Extend({update1, erase, update2}));
        const auto resolver = MakeShardResolver({
            {{Table1Id, TKeyDesc::ERowOperation::Read}, {Shard1}},
            {{Table1Id, TKeyDesc::ERowOperation::Update}, {Shard1, Shard2}},
            {{Table2Id, TKeyDesc::ERowOperation::Erase}, {shardA, shardB, shardD}},
            {{Table2Id, TKeyDesc::ERowOperation::Update}, {shardB, shardC}},
        });

        TShardRunCapture capture;
        CaptureShardRun(driver, capture);
        NKikimrMiniKQL::TResult res;
        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, resolver, IEngineFlat::TShardLimits(6, 5)), IEngineFlat::EStatus::Complete);

        UNIT_ASSERT_VALUES_EQUAL(capture.Programs.size(), 6);
        UNIT_ASSERT_EQUAL(capture.Programs.at(shardA), capture.Programs.at(shardD));
        TSet<TString> distinctPrograms;
        for (const auto& [shard, program] : capture.Programs) {
            distinctPrograms.insert(program);
        }
        UNIT_ASSERT_VALUES_EQUAL(distinctPrograms.size(), 5);
        const ui64 readsetTargets[] = {Shard2, shardA, shardB, shardC, shardD};
        AssertReadsetsOnlyFrom(capture, Shard1, readsetTargets);

        UNIT_ASSERT_EQUAL(driver.Run(pgm, res, resolver, IEngineFlat::TShardLimits(6, 4)), IEngineFlat::EStatus::Error);
    }

    // Every shard program, with the flag and without it, must be the same byte string.
    Y_UNIT_TEST(ShareShardedMkqlProgramSerializesTheSamePrograms) {
        const auto buildDelete = [](TDriver& driver) {
            auto& pgmBuilder = driver.PgmBuilder;
            TVector<TTypeInfo> readKeyTypes{TTypeInfo(NUdf::TDataType<ui64>::Id)};
            TVector<TSelectColumn> columns;
            columns.emplace_back("Value", (ui32)Schema1::Table1::Value::ColumnId, TTypeInfo(Schema1::Table1::Value::ColumnType), EColumnTypeConstraint::Nullable);
            TRuntimeNode::TList effects;
            for (ui64 key : {ui64(1), ui64(2), ui64(3)}) {
                TRuntimeNode::TList row{pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(key)};
                effects.push_back(pgmBuilder.SetResult(TStringBuilder() << "row" << key, pgmBuilder.SelectRow(TTableId(OwnerId, Table1Id), readKeyTypes, columns, row)));
            }
            TVector<TTypeInfo> eraseKeyTypes{TTypeInfo(NUdf::TDataType<ui64>::Id)};
            TRuntimeNode::TList eraseRow{pgmBuilder.Add(pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(1), pgmBuilder.TProgramBuilder::NewDataLiteral<ui64>(2))};
            effects.push_back(pgmBuilder.EraseRow(TTableId(OwnerId, Table2Id), eraseKeyTypes, eraseRow));
            const ui64 writeShardA = 201;
            const ui64 writeShardB = 202;
            const ui64 writeShardC = 203;
            return TPreparedShardPrograms{
                .Program = pgmBuilder.Build(pgmBuilder.AsList(effects)),
                .Resolve = [=](TKeyDesc& key) {
                    key.Status = TKeyDesc::EStatus::Ok;
                    TVector<TKeyDesc::TPartitionInfo> partitions;
                    if (key.RowOperation == TKeyDesc::ERowOperation::Read) {
                        partitions.push_back(TKeyDesc::TPartitionInfo(Shard1));
                    } else {
                        partitions.push_back(TKeyDesc::TPartitionInfo(writeShardA));
                        partitions.push_back(TKeyDesc::TPartitionInfo(writeShardB));
                        partitions.push_back(TKeyDesc::TPartitionInfo(writeShardC));
                    }
                    key.Partitioning = std::make_shared<TPartitioning>(std::move(partitions));
                },
            };
        };
        {
            const TMap<ui64, TString> plain = CollectProgramsWithFlag(false, buildDelete);
            const TMap<ui64, TString> shared = CollectProgramsWithFlag(true, buildDelete);
            UNIT_ASSERT_VALUES_EQUAL(plain.size(), 4);
            AssertSameSerializedPrograms("delete", plain, shared);
        }

        const ui64 shardA = 201;
        const ui64 shardB = 202;
        const ui64 shardC = 203;
        const ui64 shardD = 204;
        const auto buildMixed = [=](TDriver& driver) {
            auto& pgmBuilder = driver.PgmBuilder;
            TVector<TTypeInfo> key1Types{TTypeInfo(NUdf::TDataType<ui32>::Id)};
            TVector<TTypeInfo> key2Types{TTypeInfo(NUdf::TDataType<ui64>::Id)};
            auto rows = SelectTable1Rows(pgmBuilder, {1});
            auto update1 = pgmBuilder.Map(rows, [&](TRuntimeNode item) {
                auto update = pgmBuilder.GetUpdateRowBuilder();
                TRuntimeNode::TList row{pgmBuilder.Add(pgmBuilder.Member(item, "ID"), pgmBuilder.TProgramBuilder::NewDataLiteral<ui32>(10))};
                return pgmBuilder.UpdateRow(TTableId(OwnerId, Table1Id), key1Types, row, update);
            });
            auto erase = pgmBuilder.Map(rows, [&](TRuntimeNode item) {
                return pgmBuilder.EraseRow(TTableId(OwnerId, Table2Id), key2Types, Table2KeyFromRead(pgmBuilder, item, 10));
            });
            auto update2 = pgmBuilder.Map(rows, [&](TRuntimeNode item) {
                auto update = pgmBuilder.GetUpdateRowBuilder();
                return pgmBuilder.UpdateRow(TTableId(OwnerId, Table2Id), key2Types, Table2KeyFromRead(pgmBuilder, item, 20), update);
            });
            return TPreparedShardPrograms{
                .Program = pgmBuilder.Build(pgmBuilder.Extend({update1, erase, update2})),
                .Resolve = MakeShardResolver({
                    {{Table1Id, TKeyDesc::ERowOperation::Read}, {Shard1}},
                    {{Table1Id, TKeyDesc::ERowOperation::Update}, {Shard1, Shard2}},
                    {{Table2Id, TKeyDesc::ERowOperation::Erase}, {shardA, shardB, shardD}},
                    {{Table2Id, TKeyDesc::ERowOperation::Update}, {shardB, shardC}},
                }),
            };
        };
        {
            const TMap<ui64, TString> plain = CollectProgramsWithFlag(false, buildMixed);
            const TMap<ui64, TString> shared = CollectProgramsWithFlag(true, buildMixed);
            UNIT_ASSERT_VALUES_EQUAL(plain.size(), 6);
            AssertSameSerializedPrograms("mixed", plain, shared);
        }
    }
}

}
}
