#include "mkql_engine_flat.h"
#include "mkql_engine_flat_host.h"
#include "mkql_engine_flat_impl.h"
#include "mkql_proto.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_node_printer.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/minikql/mkql_opt_literal.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

#include <ydb/core/tablet/tablet_exception.h>

#include <util/string/printf.h>
#include <util/string/vector.h>
#include <util/generic/set.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

static const ui32 AllocNotifyCallbackBytes = 1 * 1024 * 1024; // 1 MB

class TDeadlineExceededException: public yexception {
    const char* what() const noexcept override {
        return "TDeadlineExceededException";
    }
};

TStructLiteral& GetPgmStruct(const TRuntimeNode& pgm) {
    MKQL_ENSURE(pgm.IsImmediate() && pgm.GetStaticType()->IsStruct(),
        "Shard program: Expected immediate struct");
    auto& pgmStruct = static_cast<TStructLiteral&>(*pgm.GetNode());
    MKQL_ENSURE(pgmStruct.GetValuesCount() == 5, "Shard program: Expected 5 memebers: "
        << "AllReads, MyKeys, Run, ShardsForRead, ShardsToWrite");
    return pgmStruct;
}

TStructLiteral& GetPgmMyKeysStruct(TStructLiteral& pgmStruct) {
    TRuntimeNode myKeysNode = pgmStruct.GetValue(1);
    MKQL_ENSURE(myKeysNode.IsImmediate() && myKeysNode.GetNode()->GetType()->IsStruct(),
        "MyKeys: Expected immediate struct");
    auto& myKeys = static_cast<TStructLiteral&>(*myKeysNode.GetNode());
    MKQL_ENSURE(myKeys.GetValuesCount() == 2, "MyKeys: Expected 2 members: MyReads, MyWrites");
    return myKeys;
}

TStructLiteral& GetPgmMyReadsStruct(TStructLiteral& pgmStruct) {
    auto& myKeys = GetPgmMyKeysStruct(pgmStruct);
    TRuntimeNode myReadsNode = myKeys.GetValue(0);
    MKQL_ENSURE(myReadsNode.IsImmediate() && myReadsNode.GetNode()->GetType()->IsStruct(),
        "MyReads: Expected immediate struct");
    return static_cast<TStructLiteral&>(*myReadsNode.GetNode());
}

const TStructLiteral& GetPgmShardsForReadStruct(const TStructLiteral& pgmStruct) {
    TRuntimeNode shardsForReadNode = pgmStruct.GetValue(3);
    MKQL_ENSURE(shardsForReadNode.IsImmediate() && shardsForReadNode.GetNode()->GetType()->IsStruct(),
        "ShardsForRead: Expected immediate struct");
    return static_cast<TStructLiteral&>(*shardsForReadNode.GetNode());
}

TStructLiteral& GetPgmShardsToWriteStruct(TStructLiteral& pgmStruct) {
    TRuntimeNode shardsToWriteNode = pgmStruct.GetValue(4);
    MKQL_ENSURE(shardsToWriteNode.IsImmediate() && shardsToWriteNode.GetNode()->GetType()->IsStruct(),
        "ShardsToWrite: Expected immediate struct");
    return static_cast<TStructLiteral&>(*shardsToWriteNode.GetNode());
}

TRuntimeNode GetPgmRun(TStructLiteral& pgmStruct) {
    TRuntimeNode runPgm = pgmStruct.GetValue(2);
    MKQL_ENSURE(runPgm.IsImmediate() && runPgm.GetNode()->GetType()->IsStruct(),
        "Run: Expected immediate struct");
    return runPgm;
}

TStructLiteral& GetPgmRunStruct(TStructLiteral& pgmStruct) {
    TRuntimeNode runPgm = GetPgmRun(pgmStruct);
    auto& runStruct = static_cast<TStructLiteral&>(*runPgm.GetNode());
    MKQL_ENSURE(runStruct.GetValuesCount() == 2, "Run: Expected 2 members: Reply, Write");
    return runStruct;
}

TStructLiteral& GetPgmReplyStruct(TStructLiteral& pgmStruct) {
    auto& runStruct = GetPgmRunStruct(pgmStruct);
    TRuntimeNode replyNode = runStruct.GetValue(0);
    MKQL_ENSURE(replyNode.IsImmediate() && replyNode.GetNode()->GetType()->IsStruct(),
        "Reply: Expected immediate struct");
    return static_cast<TStructLiteral&>(*replyNode.GetNode());
}

class TCallableResults {
public:
    void AddResult(ui32 id, const TStringBuf& result, const TTypeEnvironment& env) {
        const auto insertResult = ResultsMap.emplace(id, env.NewStringValue(result));
        MKQL_ENSURE(insertResult.second, "TCallableResults: duplicate result id: " << id);
    }

    typedef std::unordered_map<ui32, NUdf::TUnboxedValue> TResultsMap;

    const TResultsMap& GetMap() const { return ResultsMap; }

public:
    TString ToString(const THolderFactory& holderFactory, const TTypeEnvironment& env) const {
        const NUdf::TUnboxedValue value(GetResultsValue(holderFactory));
        return TString(TValuePacker(false, GetResultsType(env)).Pack(value));
    }

    static TCallableResults FromString(const TStringBuf& valueStr, const THolderFactory& holderFactory,
        const TTypeEnvironment& env)
    {
        TCallableResults callableResults;

        TValuePacker packer(false, GetResultsType(env));
        NUdf::TUnboxedValue value = packer.Unpack(valueStr, holderFactory);
        const auto it = value.GetListIterator();
        for (NUdf::TUnboxedValue resultStruct; it.Next(resultStruct);) {
            ui32 id = resultStruct.GetElement(0).Get<ui32>();
            NUdf::TUnboxedValue result = resultStruct.GetElement(1);
            callableResults.AddResult(id, result.AsStringRef(), env);
        }

        return callableResults;
    }

private:
    static TListType* GetResultsType(const TTypeEnvironment& env) {
        const std::array<std::pair<TString, TType*>, 2> members = {{
            {"Id", TDataType::Create(NUdf::TDataType<ui32>::Id, env)},
            {"Result", TDataType::Create(NUdf::TDataType<char*>::Id, env)}
        }};

        return TListType::Create(TStructType::Create(members.data(), members.size(), env), env);
    }

    NUdf::TUnboxedValue GetResultsValue(const THolderFactory& holderFactory) const {
        NUdf::TUnboxedValue* items = nullptr;
        auto results = holderFactory.CreateDirectArrayHolder(ResultsMap.size(), items);
        for (const auto& pair : ResultsMap) {
            NUdf::TUnboxedValue* resultItems = nullptr;
            *items++ = holderFactory.CreateDirectArrayHolder(2, resultItems);
            resultItems[0] = NUdf::TUnboxedValuePod(pair.first);
            resultItems[1] = pair.second;
        }

        return std::move(results);
    }

private:
    TResultsMap ResultsMap;
};

TRuntimeNode ReplaceAsVoid(TCallable& callable, const TTypeEnvironment& env) {
    Y_UNUSED(callable);
    return TRuntimeNode(env.GetVoidLazy(), true);
};

TRuntimeNode RenameCallable(TCallable& callable, const TStringBuf& newName, const TTypeEnvironment& env) {
    TCallableBuilder builder(env, newName, callable.GetType()->GetReturnType());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        builder.Add(callable.GetInput(i));
    }

    return TRuntimeNode(builder.Build(), false);
}

void ExtractResultType(TCallable& callable, const TTypeEnvironment& env, TMap<TString, TType*>& resTypes) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

    const auto& labelInput = callable.GetInput(0);
    MKQL_ENSURE(labelInput.IsImmediate() && labelInput.GetNode()->GetType()->IsData(), "Expected immediate data");

    const auto& labelData = static_cast<const TDataLiteral&>(*labelInput.GetNode());
    MKQL_ENSURE(labelData.GetType()->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected string");

    TStringBuf label = labelData.AsValue().AsStringRef();
    MKQL_ENSURE(!label.empty(), "Empty result label is not allowed");
    MKQL_ENSURE(!label.StartsWith(TxInternalResultPrefix),
        TStringBuilder() << "Label can't be used in SetResult as it's reserved for internal purposes: " << label);

    auto payload = callable.GetInput(1);
    MKQL_ENSURE(CanExportType(payload.GetStaticType(), env),
        TStringBuilder() << "Failed to export type:" << *payload.GetStaticType());

    auto& type = resTypes[TString(label)];
    if (!type) {
        type = payload.GetStaticType();
        return;
    }

    MKQL_ENSURE(type->IsSameType(*payload.GetStaticType()),
        TStringBuilder() << "Mismatch of result type for label: " << label);
}

void ExtractAcquireLocksType(const TTypeEnvironment& env, TMap<TString, TType*>& resTypes) {
    {
        auto lockStructType = GetTxLockType(env, false);
        auto lockListType = TListType::Create(lockStructType, env);

        auto& type = resTypes[TString(TxLocksResultLabel)];
        MKQL_ENSURE(!type, TStringBuilder() << "Duplicate result label: " << TxLocksResultLabel);
        type = lockListType;
    }

    {
        auto lockStructType = GetTxLockType(env, true);
        auto lockListType = TListType::Create(lockStructType, env);

        auto& type = resTypes[TString(TxLocksResultLabel2)];
        MKQL_ENSURE(!type, TStringBuilder() << "Duplicate result label: " << TxLocksResultLabel2);
        type = lockListType;
    }
}

void ExtractDiagnosticsType(const TTypeEnvironment& env, TMap<TString, TType*>& resTypes) {
    auto structType = GetDiagnosticsType(env);
    auto listType = TListType::Create(structType, env);

    auto& type = resTypes[TString(TxInfoResultLabel)];
    MKQL_ENSURE(!type, TStringBuilder() << "Duplicate result label: " << TxInfoResultLabel);
    type = listType;
}

ui64 ExtractAcquireLocksTxId(TCallable& callable) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

    auto lockTxIdInput = callable.GetInput(0);
    MKQL_ENSURE(lockTxIdInput.IsImmediate() && lockTxIdInput.GetNode()->GetType()->IsData(), "Expected immediate data");

    const auto& lockTxIdData = static_cast<const TDataLiteral&>(*lockTxIdInput.GetNode());
    MKQL_ENSURE(lockTxIdData.GetType()->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64");
    return lockTxIdData.AsValue().Get<ui64>();
}

class TEngineFlat : public IEngineFlat {
public:
    TEngineFlat(const TEngineFlatSettings& settings)
        : Settings(settings)
        , Alloc(__LOCATION__, Settings.AllocCounters, settings.FunctionRegistry->SupportsSizedAllocators())
        , Env(Alloc)
        , Strings(Env)
        , NeedDiagnostics(false)
        , Status(EStatus::Unknown)
        , AreAffectedShardsPrepared(false)
        , AreShardProgramsExtracted(false)
        , IsResultBuilt(false)
        , IsProgramValidated(false)
        , AreOutgoingReadSetsPrepared(false)
        , AreOutgoingReadSetsExtracted(false)
        , AreIncomingReadsetsPrepared(false)
        , IsExecuted(false)
        , ReadOnlyOriginPrograms(true)
        , IsCancelled(false)
    {
        Ui64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, Env);
        ResultType = Env.GetEmptyStructLazy()->GetType();
        Alloc.DisableStrictAllocationCheck();
        Alloc.Release();
    }

    ~TEngineFlat() {
        Alloc.Acquire();
    }

    TString GetErrors() const noexcept override {
        return Errors;
    }

    void SetStepTxId(const std::pair<ui64, ui64>& stepTxId) noexcept override {
        StepTxId = stepTxId;
    }

    void AddTabletInfo(IEngineFlat::TTabletInfo&& info) noexcept override {
        TabletInfos.emplace_back(info);
    }

    void AddTxLock(IEngineFlat::TTxLock&& txLock) noexcept override {
        TxLocks.emplace_back(txLock);
    }

    TMaybe<ui64> GetLockTxId() noexcept override {
        return LockTxId;
    }

    bool HasDiagnosticsRequest() noexcept override {
        return NeedDiagnostics;
    }

    EResult SetProgram(TStringBuf program, TStringBuf params) noexcept override {
        Y_ABORT_UNLESS(!Program.GetNode(), "Program is already set");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        TRuntimeNode programNode;
        try {
            programNode = DeserializeRuntimeNode(program, Env);
            if (params) {
                TKikimrProgramBuilder builder(Env, *Settings.FunctionRegistry);
                TRuntimeNode paramsNode = DeserializeRuntimeNode(params, Env);
                programNode = builder.Bind(programNode, paramsNode, TKikimrProgramBuilder::TBindFlags::OptimizeLiterals);
            }

            auto listType = programNode.GetStaticType();
            if (listType->GetKind() != TType::EKind::List) {
                AddError("SetProgram", __LINE__, "Bad root node in program - expected list of void");
                return EResult::ProgramError;
            }

            const auto& listDetailedType = static_cast<const TListType&>(*listType);
            if (listDetailedType.GetItemType()->GetKind() != TType::EKind::Void) {
                AddError("SetProgram", __LINE__, "Bad root node in program - expected list of void");
                return EResult::ProgramError;
            }

            TVector<THolder<TKeyDesc>> dbKeys;
            TExploringNodeVisitor explorer;

            {
                explorer.Walk(programNode.GetNode(), Env);
                TMap<TString, TType*> resTypes;
                for (auto node : explorer.GetNodes()) {
                    if (node->GetType()->GetKind() != TType::EKind::Callable)
                        continue;

                    auto& callable = static_cast<TCallable&>(*node);
                    const ui32 uniqueId = 1 + ProxyCallables.size();
                    TCallableContext ctx;
                    ctx.Node = &callable;
                    auto it = ProxyCallables.insert(std::make_pair(uniqueId, ctx)).first;
                    callable.SetUniqueId(uniqueId);

                    THolder<TKeyDesc> desc = ExtractTableKey(callable, Strings, Env);
                    if (desc) {
                        it->second.Key = desc.Get();
                        dbKeys.push_back(std::move(desc));
                    } else if (callable.GetType()->GetNameStr() == Strings.SetResult) {
                        ExtractResultType(callable, Env, resTypes);
                    } else if (callable.GetType()->GetNameStr() == Strings.AcquireLocks) {
                        ExtractAcquireLocksType(Env, resTypes);
                        ui64 lockTxId = ExtractAcquireLocksTxId(callable);

                        if (LockTxId) {
                            AddError("SetProgram", __LINE__, "Multiple AcquireLocks calls.");
                            return EResult::ProgramError;
                        }

                        LockTxId = lockTxId;
                    } else if (callable.GetType()->GetNameStr() == Strings.Diagnostics) {
                        NeedDiagnostics = true;
                        ExtractDiagnosticsType(Env, resTypes);
                    }
                }

                TStructTypeBuilder resStructBuilder(Env);
                for (auto& x : resTypes) {
                    resStructBuilder.Add(x.first, TOptionalType::Create(x.second, Env));
                }

                ResultType = resStructBuilder.Build();
            }

            if (Settings.EvaluateResultValue) {
                DbKeys.swap(dbKeys);
            }
        }
        catch (yexception& e) {
            Alloc.InvalidateMemInfo();
            HandleException("SetProgram", __LINE__, e);
            return EResult::ProgramError;
        }

        Program = programNode;
        return EResult::Ok;
    }

    TVector<THolder<TKeyDesc>>& GetDbKeys() noexcept override {
        Y_ABORT_UNLESS(Program.GetNode(), "Program is not set");
        return DbKeys;
    }

    EResult PrepareShardPrograms(const TShardLimits& limits, ui32* outRSCount) noexcept override {
        Y_ABORT_UNLESS(!AreAffectedShardsPrepared, "PrepareShardPrograms is already called");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        AffectedShards.clear();
        TSet<ui64> affectedShardSet;
        TSet<ui64> writeSet;
        TSet<ui64> onlineReadSet;
        for (ui32 keyIndex = 0; keyIndex < DbKeys.size(); ++keyIndex) {
            auto& key = DbKeys[keyIndex];
            Y_ABORT_UNLESS(key->Status == TKeyDesc::EStatus::Ok, "Some DB keys are not resolved correctly");
            AddShards(affectedShardSet, *key);
            if (affectedShardSet.size() > limits.ShardCount) {
                AddError("PrepareShardPrograms", __LINE__,
                         Sprintf("too many affected shards: %u (max allowed %u)", (ui32)affectedShardSet.size(), limits.ShardCount).data());
                return EResult::TooManyShards;
            }

            if (key->RowOperation == TKeyDesc::ERowOperation::Update || key->RowOperation == TKeyDesc::ERowOperation::Erase) {
                AddShards(writeSet, *key);
            }

            if (key->RowOperation == TKeyDesc::ERowOperation::Read) {
                if (key->ReadTarget.GetMode() == TReadTarget::EMode::Online) {
                    AddShards(onlineReadSet, *key);
                }
            }
        }

        bool hasWrites = false;
        const auto& strings = Strings;
        auto writesCheck = [&hasWrites, &strings](TInternName name) {
            if (strings.DbWrites.contains(name)) {
                hasWrites = true;
            }

            return TCallableVisitFunc();
        };

        bool wereChanges;
        ProgramExplorer.Walk(Program.GetNode(), Env);
        SinglePassVisitCallables(Program, ProgramExplorer, writesCheck, Env, true, wereChanges);
        Y_ABORT_UNLESS(!wereChanges);

        ReadOnlyProgram = !hasWrites;

        AffectedShards.reserve(affectedShardSet.size());
        ui64 coordinatorRequiresShardCount = 0;
        for (ui64 shard : affectedShardSet) {
            auto shardData = TShardData(shard, TString());
            if (writeSet.contains(shard))
                shardData.HasWrites = true;
            if (onlineReadSet.contains(shard))
                shardData.HasOnlineReads = true;

            if (shardData.HasWrites || shardData.HasOnlineReads)
                ++coordinatorRequiresShardCount;

            AffectedShards.push_back(shardData);
        }

        if (!Settings.ForceOnline && (coordinatorRequiresShardCount <= 1)) {
            for (auto& shardData : AffectedShards) {
                shardData.Immediate = true;
            }
        }

        THashSet<std::pair<ui64, ui64>> readsets;
        if (Settings.EvaluateResultValue) {
            SpecializedParts.resize(AffectedShards.size());
            PrepareProxyProgram();
            PrepareShardsForRead();
            THashMap<ui32, const TVector<TKeyDesc::TPartitionInfo>*> proxyShards;
            for (auto& x : ProxyCallables) {
                auto name = x.second.Node->GetType()->GetNameStr();
                if (name == Strings.EraseRow || name == Strings.UpdateRow) {
                    proxyShards[x.first] = &x.second.Key->GetPartitions();
                }
            }

            for (ui32 shardIndex = 0; shardIndex < AffectedShards.size(); ++shardIndex) {
                PrepareShardProgram(AffectedShards[shardIndex].ShardId, proxyShards, SpecializedParts[shardIndex],
                    readsets);
            }

            BuildAllReads();

            for (ui32 shardIndex = 0; shardIndex < AffectedShards.size(); ++shardIndex) {
                FinalizeShardProgram(SpecializedParts[shardIndex]);
            }

            ProgramExplorer.Clear();
        }

        if (readsets.size() > limits.RSCount) {
            THashMap<ui64, TTableId> tableMap;
            for (auto& key : DbKeys) {
                for (auto& partition : key->GetPartitions()) {
                    tableMap[partition.ShardId] = key->TableId;
                }
            }

            TTablePathHashSet srcTables;
            TTablePathHashSet dstTables;
            for (auto& readset : readsets) {
                srcTables.emplace(tableMap[readset.first]);
                dstTables.emplace(tableMap[readset.second]);
            }

            AddError("PrepareShardPrograms", __LINE__,
                Sprintf("too many shard readsets (%u > %u), src tables: %s, dst tables: %s",
                (ui32)readsets.size(),
                limits.RSCount,
                JoinStrings(srcTables.begin(), srcTables.end(), ",").c_str(),
                JoinStrings(dstTables.begin(), dstTables.end(), ",").c_str()).data());
            return EResult::TooManyRS;
        }

        if (outRSCount) {
            *outRSCount = readsets.size();
        }

        AreAffectedShardsPrepared = true;
        return EResult::Ok;
    }

    bool IsReadOnlyProgram() const noexcept override {
        Y_ABORT_UNLESS(AreAffectedShardsPrepared, "PrepareShardPrograms must be called first");
        Y_ABORT_UNLESS(ReadOnlyProgram, "Invalid call to IsReadOnlyProgram");
        return *ReadOnlyProgram;
    }

    ui32 GetAffectedShardCount() const noexcept override {
        Y_ABORT_UNLESS(AreAffectedShardsPrepared, "PrepareShardPrograms must be called first");
        Y_ABORT_UNLESS(!AreShardProgramsExtracted, "AfterShardProgramsExtracted is already called");
        return static_cast<ui32>(AffectedShards.size());
    }

    EResult GetAffectedShard(ui32 index, TShardData& data) const noexcept override {
        Y_ABORT_UNLESS(AreAffectedShardsPrepared, "PrepareShardPrograms must be called first");
        Y_ABORT_UNLESS(!AreShardProgramsExtracted, "AfterShardProgramsExtracted is already called");
        Y_ABORT_UNLESS(index < AffectedShards.size(), "Bad index");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        data = AffectedShards[index];
        data.Program = SerializeRuntimeNode(SpecializedParts[index].Program, Env);
        return EResult::Ok;
    }

    void AfterShardProgramsExtracted() noexcept override {
        Y_ABORT_UNLESS(AreAffectedShardsPrepared, "PrepareShardPrograms must be called first");
        Y_ABORT_UNLESS(!AreShardProgramsExtracted, "AfterShardProgramsExtracted is already called");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        TVector<THolder<TKeyDesc>>().swap(DbKeys);
        TVector<TProgramParts>().swap(SpecializedParts);
        AreShardProgramsExtracted = true;
    }

    void AddShardReply(ui64 origin, const TStringBuf& reply) noexcept override {
        Y_ABORT_UNLESS(!IsResultBuilt, "BuildResult is already called");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        if (reply.empty()) {
            Status = EStatus::Error;
            AddError("AddShardReply", __LINE__, "Shard reply is empty");
            return;
        }

        auto insertResult = ExecutionReplies.insert(std::make_pair(origin, reply));
        Y_ABORT_UNLESS(insertResult.second);
    }

    void FinalizeOriginReplies(ui64 origin) noexcept override {
        Y_ABORT_UNLESS(!IsResultBuilt, "BuildResult is already called");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        for (const auto& shardData : AffectedShards) {
            if (shardData.ShardId == origin) {
                FinalizedShards.insert(origin);
                return;
            }
        }

        AddError("FinalizeOriginReplies", __LINE__, Sprintf("Unknown shard: %" PRIu64, origin).data());
        Status = EStatus::Error;
    }

    // Temporary check for KIKIMR-7112
    bool CheckValidUint8InKey(TKeyDesc& desc) const {
        if (!desc.Range.Point) {
            for (auto typeInfo : desc.KeyColumnTypes) {
                if (typeInfo.GetTypeId() == NScheme::NTypeIds::Uint8) {
                    AddError("Validate", __LINE__, "Bad shard program: dynamic keys with Uint8 columns are currently prohibited");
                    return false;
                }
            }
        } else {
            if (desc.Range.From.size() > desc.KeyColumnTypes.size()) {
                AddError("Validate", __LINE__, "Bad shard program: key size is greater that specified in schema");
                return false;
            }
            for (size_t i = 0; i < desc.Range.From.size(); ++i) {
                if (desc.KeyColumnTypes[i].GetTypeId() != NScheme::NTypeIds::Uint8)
                    continue;
                const TCell& c = desc.Range.From[i];
                if (!c.IsNull() && c.AsValue<ui8>() > 127) {
                    AddError("Validate", __LINE__, "Bad shard program: keys with Uint8 column values >127 are currently prohibited");
                    return false;
                }
            }
        }
        return true;
    }

    void BuildResult() noexcept override {
        Y_ABORT_UNLESS(AreShardProgramsExtracted, "AfterShardProgramsExtracted must be called first");
        Y_ABORT_UNLESS(!IsResultBuilt, "BuildResult is already called");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        if (Status == EStatus::Error)
            return;

        if (FinalizedShards.size() != AffectedShards.size()) {
            AddError("BuildResult", __LINE__, "Some shards are not finalized");
            Status = EStatus::Error;
            return;
        }

        if (Settings.EvaluateResultValue) {
            try {
                TProxyExecData execData(Settings, Strings, StepTxId, TabletInfos, TxLocks);

                {
                    TMemoryUsageInfo memInfo("Memory");
                    THolderFactory holderFactory(Alloc.Ref(), memInfo, Settings.FunctionRegistry);

                    for (auto& pair : ExecutionReplies) {
                        const TString& reply = pair.second;

                        TCallableResults results = TCallableResults::FromString(reply, holderFactory, Env);
                        for (const auto& pair : results.GetMap()) {
                            ui32 id = pair.first;

                            const auto nodeIt = ProxyRepliesCallables.find(id);
                            if (nodeIt == ProxyRepliesCallables.end()) {
                                AddError("BuildResult", __LINE__, Sprintf(
                                    "Bad shard reply, node %" PRIu32 " not found", id).data());
                                Status = EStatus::Error;
                                return;
                            }

                            execData.Results[id].emplace_back(pair.second.AsStringRef());
                        }
                    }
                }

                NUdf::TUnboxedValue value;
                {
                    TComputationPatternOpts opts(Alloc.Ref(), Env,
                        GetFlatProxyExecutionFactory(execData),
                        Settings.FunctionRegistry,
                        NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,
                        Settings.LlvmRuntime ? "" : "OFF", EGraphPerProcess::Multi);
                    Pattern = MakeComputationPattern(ProxyProgramExplorer, ProxyProgram, {}, opts);
                    ResultGraph = Pattern->Clone(opts.ToComputationOptions(Settings.RandomProvider, Settings.TimeProvider, &Env));

                    const TBindTerminator bind(ResultGraph->GetTerminator());

                    value = ResultGraph->GetValue();
                }

                Status = EStatus::Complete;
                TEngineFlatApplyContext applyCtx;
                applyCtx.ResultValues = &ResultValues;
                ApplyChanges(value, applyCtx);
                if (applyCtx.IsAborted) {
                    Status = EStatus::Aborted;
                }
            }
            catch (const TMemoryLimitExceededException& e) {
                Alloc.InvalidateMemInfo();
                AddError("Memory limit exceeded during query result computation");
                Status = EStatus::Error;
                return;
            }
            catch (TDeadlineExceededException&) {
                IsCancelled = true;
                AddError("Deadline exceeded during query result computation");
                Status = EStatus::Aborted;
                return;
            }
            catch (yexception& e) {
                Alloc.InvalidateMemInfo();
                HandleException("BuildResult", __LINE__, e);
                Status = EStatus::Error;
                return;
            }
        }
        else {
            Status = EStatus::Complete;
        }

        IsResultBuilt = true;
    }

    EStatus GetStatus() const noexcept override {
        return Status;
    }

    EResult FillResultValue(NKikimrMiniKQL::TResult& result) const noexcept override {
        if (IsCancelled) {
            return EResult::Cancelled;
        }

        Y_ABORT_UNLESS(IsResultBuilt, "BuildResult is not called yet");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        if (Settings.EvaluateResultType) {
            ExportTypeToProto(ResultType, *result.MutableType());
        }

        if (Settings.EvaluateResultValue) {
            try {
                const TBindTerminator bind(ResultGraph->GetTerminator());

                for (ui32 index = 0; index < ResultType->GetMembersCount(); ++index) {
                    auto memberType = ResultType->GetMemberType(index);
                    auto itemType = static_cast<TOptionalType*>(memberType)->GetItemType();
                    auto value = ResultValues.FindPtr(ResultType->GetMemberName(index));
                    auto resStruct = result.MutableValue()->AddStruct();
                    if (value) {
                        ExportValueToProto(itemType, *value, *resStruct->MutableOptional());
                    }

                    auto resultSize = result.ByteSize();
                    if (resultSize > (i32)MaxProxyReplySize) {
                        result = {};
                        TString error = TStringBuilder() << "Query result size limit exceeded. ("
                            << resultSize << " > " << MaxProxyReplySize << ")";

                        AddError(error);
                        return EResult::ResultTooBig;
                    }
                }
            }
            catch (const TMemoryLimitExceededException& e) {
                Alloc.InvalidateMemInfo();
                AddError("Memory limit exceeded during query result computation");
                return EResult::ProgramError;
            }
            catch (TDeadlineExceededException&) {
                AddError("Deadline exceeded during query result computation");
                return EResult::Cancelled;
            }
            catch (yexception& e) {
                Alloc.InvalidateMemInfo();
                HandleException("FillResultValue", __LINE__, e);
                return EResult::ProgramError;
            }
        }

        return EResult::Ok;
    }

    EResult AddProgram(ui64 origin, const TStringBuf& program, bool readOnly) noexcept override {
        Y_ABORT_UNLESS(ProgramPerOrigin.find(origin) == ProgramPerOrigin.end(), "Program for that origin is already added");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        TRuntimeNode node;
        try {
            node = DeserializeRuntimeNode(program, Env);
        }
        catch (yexception& e) {
            Alloc.InvalidateMemInfo();
            HandleException("AddProgram", __LINE__, e);
            return EResult::ProgramError;
        }

        ReadOnlyOriginPrograms = ReadOnlyOriginPrograms && readOnly;

        ProgramPerOrigin[origin] = node;
        return EResult::Ok;
    }

    EResult ValidateKeys(TValidationInfo& validationInfo) override {
        EResult result = EResult::Ok;

        for (auto& validKey : validationInfo.Keys) {
            TKeyDesc * key = validKey.Key.get();

            bool valid = Settings.Host->IsValidKey(*key);

            if (valid) {
                auto curSchemaVersion = Settings.Host->GetTableSchemaVersion(key->TableId);
                if (key->TableId.SchemaVersion && curSchemaVersion
                    && curSchemaVersion != key->TableId.SchemaVersion) {

                    const auto err = TStringBuilder()
                        << "Schema version missmatch for table id: " << key->TableId
                        << " mkql compiled on: " << key->TableId.SchemaVersion
                        << " current version: " << curSchemaVersion;
                    AddError(err);
                    return EResult::SchemeChanged;
                }
            } else {
                switch (key->Status) {
                case TKeyDesc::EStatus::SnapshotNotExist:
                    return EResult::SnapshotNotExist;
                case TKeyDesc::EStatus::SnapshotNotReady:
                    key->Status = TKeyDesc::EStatus::Ok;
                    result = EResult::SnapshotNotReady;
                    break;
                default:
                    TStringStream str;
                    str << "Key validation status: " << (ui32)key->Status;
                        //<< ", key: " << key; TODO
                    AddError("Validate", __LINE__, str.Str().data());
                    return EResult::KeyError;
                }
            }
        }

        return result;
    }

    EResult Validate(TValidationInfo& validationInfo) override {
        Y_ABORT_UNLESS(!IsProgramValidated, "Validate is already called");
        Y_ABORT_UNLESS(ProgramPerOrigin.size() == 1, "One program must be added to engine");
        Y_ABORT_UNLESS(Settings.Host, "Host is not set");
        TGuard<TScopedAlloc> allocGuard(Alloc);

        if (ProgramPerOrigin.begin()->first != Settings.Host->GetShardId())
            return EResult::SchemeChanged;

        auto pgm = ProgramPerOrigin.begin()->second;
        if (!pgm.IsImmediate() || pgm.GetStaticType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        // AllReads, MyKeys, Run, ShardsForRead, ShardsToWrite
        const auto& pgmStruct = static_cast<const TStructLiteral&>(*pgm.GetNode());
        if (pgmStruct.GetValuesCount() != 5) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        if (!pgmStruct.GetValue(0).IsImmediate() || !pgmStruct.GetValue(1).IsImmediate() ||
            !pgmStruct.GetValue(2).IsImmediate() || !pgmStruct.GetValue(3).IsImmediate() ||
            !pgmStruct.GetValue(4).IsImmediate()) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        auto allReads = pgmStruct.GetValue(0);
        if (allReads.GetNode()->GetType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        auto myKeys = pgmStruct.GetValue(1);
        if (myKeys.GetNode()->GetType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        const auto& myKeysStruct = static_cast<const TStructLiteral&>(*myKeys.GetNode());
        if (myKeysStruct.GetValuesCount() != 2) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        auto myReads = myKeysStruct.GetValue(0);
        if (myReads.GetNode()->GetType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        auto myWrites = myKeysStruct.GetValue(1);
        if (myWrites.GetNode()->GetType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        auto runPgm = pgmStruct.GetValue(2);
        if (runPgm.GetNode()->GetType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        // Reply,Write
        const auto& runPgmStruct = static_cast<const TStructLiteral&>(*runPgm.GetNode());
        if (runPgmStruct.GetValuesCount() != 2) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        auto replyPgm = runPgmStruct.GetValue(0);
        if (replyPgm.GetNode()->GetType()->GetKind() != TType::EKind::Struct) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        // Extract reads that are included in the reply
        THashSet<TStringBuf> replyIds;
        const auto& replyStruct = static_cast<const TStructLiteral&>(*replyPgm.GetNode());
        for (ui32 j = 0, f = replyStruct.GetValuesCount(); j < f; ++j) {
            TStringBuf uniqId(replyStruct.GetType()->GetMemberName(j));
            // Save the id of read operation that is included in reply
            replyIds.insert(uniqId);
        }

        auto writePgm = runPgmStruct.GetValue(1);
        auto listType = writePgm.GetStaticType();
        if (listType->GetKind() != TType::EKind::List) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        const auto& listDetailedType = static_cast<const TListType&>(*listType);
        if (listDetailedType.GetItemType()->GetKind() != TType::EKind::Void) {
            AddError("Validate", __LINE__, "Bad shard program");
            return EResult::ProgramError;
        }

        // Extract reads that are included in out readsets
        THashMap<TStringBuf, THashSet<ui64>> readTargets;
        const ui64 myShardId = Settings.Host->GetShardId();
        auto shardsToWriteNode = pgmStruct.GetValue(4);
        MKQL_ENSURE(shardsToWriteNode.IsImmediate() && shardsToWriteNode.GetNode()->GetType()->IsStruct(),
            "Expected immediate struct");
        const auto& shardsToWriteStruct = static_cast<const TStructLiteral&>(*shardsToWriteNode.GetNode());
        for (ui32 i = 0, e = shardsToWriteStruct.GetValuesCount(); i < e; ++i) {
            TStringBuf uniqId(shardsToWriteStruct.GetType()->GetMemberName(i));
            auto shardsList = AS_VALUE(TListLiteral, shardsToWriteStruct.GetValue(i));
            auto itemType = shardsList->GetType()->GetItemType();
            MKQL_ENSURE(itemType->IsData() && static_cast<TDataType*>(itemType)->GetSchemeType()
                        == NUdf::TDataType<ui64>::Id, "Bad shard list");
            for (ui32 shardIndex = 0; shardIndex < shardsList->GetItemsCount(); ++shardIndex) {
                ui64 shard = AS_VALUE(TDataLiteral, shardsList->GetItems()[shardIndex])->AsValue().Get<ui64>();
                if (shard != myShardId) {
                    // Save the target shard id for the read operation
                    readTargets[uniqId].insert(shard);
                }
            }
        }

        validationInfo.Clear();
        EResult result;
        {
            auto myReadsStruct = static_cast<TStructLiteral*>(myReads.GetNode());
            validationInfo.ReadsCount = myReadsStruct->GetValuesCount();

            auto myWritesStruct = static_cast<TStructLiteral*>(myWrites.GetNode());
            validationInfo.WritesCount = myWritesStruct->GetValuesCount();

            validationInfo.Keys.reserve(validationInfo.ReadsCount + validationInfo.WritesCount);

            {
                for (ui32 i = 0; i < validationInfo.ReadsCount; ++i) {
                    TStringBuf uniqId(myReadsStruct->GetType()->GetMemberName(i));
                    TRuntimeNode item = myReadsStruct->GetValue(i);
                    if (!item.GetNode()->GetType()->IsCallable()) {
                        AddError("Validate", __LINE__, "Bad shard program");
                        return EResult::ProgramError;
                    }

                    THolder<TKeyDesc> desc = ExtractTableKey(*static_cast<TCallable*>(item.GetNode()), Strings, Env);
                    Y_ABORT_UNLESS(desc);
                    Y_ABORT_UNLESS(desc->RowOperation == TKeyDesc::ERowOperation::Read);
                    TValidatedKey validKey(std::move(desc), false);

                    auto targetIt = readTargets.find(uniqId);
                    if (replyIds.contains(uniqId) || targetIt != readTargets.end()) {
                        // Is this read result included in the reply?
                        if (replyIds.contains(uniqId)) {
                            validKey.IsResultPart = true;
                        }
                        // Is this read result included into outgoing read sets?
                        if (targetIt != readTargets.end()) {
                            // TODO: can't we move them?
                            for (ui64 shard : targetIt->second) {
                                validKey.TargetShards.insert(shard);
                                validationInfo.HasOutReadsets = true;
                            }
                        }
                    }

                    validationInfo.Keys.emplace_back(std::move(validKey));
                }
            }

            {
                for (ui32 i = 0; i < validationInfo.WritesCount; ++i) {
                    TRuntimeNode item = myWritesStruct->GetValue(i);
                    if (!item.GetNode()->GetType()->IsCallable()) {
                        AddError("Validate", __LINE__, "Bad shard program");
                        return EResult::ProgramError;
                    }

                    THolder<TKeyDesc> desc = ExtractTableKey(*static_cast<TCallable*>(item.GetNode()), Strings, Env);
                    Y_ABORT_UNLESS(desc);
                    Y_ABORT_UNLESS(desc->RowOperation == TKeyDesc::ERowOperation::Update ||
                             desc->RowOperation == TKeyDesc::ERowOperation::Erase);
                    if (!desc->Range.Point) {
                        ++validationInfo.DynKeysCount;
                    }
                    if (!CheckValidUint8InKey(*desc)) {
                        return EResult::ProgramError;
                    }

                    validationInfo.Keys.emplace_back(TValidatedKey(std::move(desc), true));
                }
            }

            if (validationInfo.HasWrites() && Settings.Host->IsReadonly())
                return EResult::IsReadonly;

            result = ValidateKeys(validationInfo);
            switch (result) {
                case EResult::Ok:
                case EResult::SnapshotNotReady:
                    break;
                default:
                    return result;
            }

            // Check if we expect incoming readsets
            auto& shardForRead = GetPgmShardsForReadStruct(pgmStruct);
            for (ui32 i = 0; i < shardForRead.GetValuesCount() && !validationInfo.HasInReadsets; ++i) {
                auto shardsList = AS_VALUE(TListLiteral, shardForRead.GetValue(i));
                auto itemType = shardsList->GetType()->GetItemType();
                MKQL_ENSURE(itemType->IsData() && static_cast<TDataType*>(itemType)->GetSchemeType()
                            == NUdf::TDataType<ui64>::Id, "Bad shard list");

                for (ui32 shardIndex = 0; shardIndex < shardsList->GetItemsCount(); ++shardIndex) {
                    ui64 shard = AS_VALUE(TDataLiteral, shardsList->GetItems()[shardIndex])->AsValue().Get<ui64>();
                    if (shard != myShardId) {
                        validationInfo.HasInReadsets = true;
                        break;
                    }
                }
            }
        }

        try {
            TShardExecData execData(Settings, Strings, StepTxId);
            TExploringNodeVisitor explorer;
            explorer.Walk(runPgm.GetNode(), Env);
            TComputationPatternOpts opts(Alloc.Ref(), Env,
                GetFlatShardExecutionFactory(execData, true),
                Settings.FunctionRegistry,
                NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,
                Settings.LlvmRuntime ? "" : "OFF", EGraphPerProcess::Multi);
            auto pattern = MakeComputationPattern(explorer, runPgm, {}, opts);
            auto graph = pattern->Clone(opts.ToComputationOptions(Settings.RandomProvider, Settings.TimeProvider, &Env));

            const TBindTerminator bind(graph->GetTerminator());

            graph->Prepare();
        }
        catch (yexception& e) {
            Alloc.InvalidateMemInfo();
            HandleException("Validate", __LINE__, e);
            return EResult::ProgramError;
        }

        if (Y_LIKELY(result == EResult::Ok)) {
            validationInfo.SetLoaded();
        }

        IsProgramValidated = true;
        return result;
    }

    EResult PinPages(ui64 pageFaultCount) override {
        Y_ABORT_UNLESS(ProgramPerOrigin.size() == 1, "One program must be added to engine");
        Y_ABORT_UNLESS(Settings.Host, "Host is not set");

        if (IsCancelled) {
            // Do nothing and quickly proceed
            return EResult::Ok;
        }

        TGuard<TScopedAlloc> allocGuard(Alloc);

        TVector<THolder<TKeyDesc>> prechargeKeys;
        // iterate over all ProgramPerOrigin (for merged datashards)
        for (const auto& pi : ProgramPerOrigin) {
            auto pgm = pi.second;
            MKQL_ENSURE(pgm.IsImmediate() && pgm.GetStaticType()->IsStruct(), "Expected immediate struct");
            const auto& pgmStruct = static_cast<const TStructLiteral&>(*pgm.GetNode());
            MKQL_ENSURE(pgmStruct.GetValuesCount() == 5, "Expected 5 members"); // AllReads, MyKeys, Run, ShardsForRead, ShardsToWrite

            auto myKeys = pgmStruct.GetValue(1);
            MKQL_ENSURE(myKeys.IsImmediate() && myKeys.GetNode()->GetType()->IsStruct(), "Expected immediate struct");
            const auto& myKeysStruct = static_cast<const TStructLiteral&>(*myKeys.GetNode());
            MKQL_ENSURE(myKeysStruct.GetValuesCount() == 2, "Expected 2 members");

            // 0 - reads, 1 - writes
            for (ui32 opId = 0; opId <= 1; ++opId) {
                auto myOps = myKeysStruct.GetValue(opId);
                MKQL_ENSURE(myOps.IsImmediate() && myOps.GetNode()->GetType()->IsStruct(), "Expected immediate struct");

                auto myOpsStruct = static_cast<TStructLiteral*>(myOps.GetNode());
                for (ui32 i = 0, e = myOpsStruct->GetValuesCount(); i < e; ++i) {
                    TRuntimeNode item = myOpsStruct->GetValue(i);
                    Y_ABORT_UNLESS(item.GetNode()->GetType()->IsCallable(), "Bad shard program");
                    THolder<TKeyDesc> desc = ExtractTableKey(*static_cast<TCallable*>(item.GetNode()), Strings, Env);
                    Y_ABORT_UNLESS(desc);
                    prechargeKeys.emplace_back(std::move(desc));
                }
            }
        }

        Settings.Host->PinPages(prechargeKeys, pageFaultCount);
        return EResult::Ok;
    }

    EResult PrepareOutgoingReadsets() override {
        Y_ABORT_UNLESS(!AreOutgoingReadSetsPrepared, "PrepareOutgoingReadsets is already called");
        Y_ABORT_UNLESS(Settings.Host, "Host is not set");
        Y_ABORT_UNLESS(ProgramPerOrigin.size() > 0, "At least one program must be added to engine");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        try {
            OutgoingReadsets.clear();
            const ui64 myShardId = Settings.Host->GetShardId();

            TVector<TString> readResults;
            THashMap<ui64, TCallableResults> resultsPerTarget;

            TMemoryUsageInfo memInfo("Memory");
            THolderFactory holderFactory(Alloc.Ref(), memInfo, Settings.FunctionRegistry);

            for (auto pgm : ProgramPerOrigin) {
                auto& pgmStruct = GetPgmStruct(pgm.second);
                auto& myReads = GetPgmMyReadsStruct(pgmStruct);
                auto& shardsToWrite = GetPgmShardsToWriteStruct(pgmStruct);

                ui32 readIdx = 0;
                ui32 writeIdx = 0;
                while (readIdx < myReads.GetValuesCount() && writeIdx < shardsToWrite.GetValuesCount()) {
                    auto readName = myReads.GetType()->GetMemberName(readIdx);
                    auto writeName = shardsToWrite.GetType()->GetMemberName(writeIdx);

                    if (readName == writeName) {
                        auto shardsList = AS_VALUE(TListLiteral, shardsToWrite.GetValue(writeIdx));
                        auto itemType = shardsList->GetType()->GetItemType();
                        MKQL_ENSURE(itemType->IsData() && static_cast<TDataType*>(itemType)->GetSchemeType()
                            == NUdf::TDataType<ui64>::Id, "Bad shard list type.");

                        if (shardsList->GetItemsCount() > 0) {
                            if(!IsCancelled) {
                                TRuntimeNode item = myReads.GetValue(readIdx);
                                MKQL_ENSURE(item.GetNode()->GetType()->IsCallable(), "Expected callable");
                                auto callable = static_cast<TCallable*>(item.GetNode());

                                NUdf::TUnboxedValue readValue;
                                auto name = callable->GetType()->GetNameStr();
                                if (name == Strings.SelectRow) {
                                    readValue = PerformLocalSelectRow(*callable, *Settings.Host, holderFactory, Env);
                                }
                                else if (name == Strings.SelectRange) {
                                    readValue = PerformLocalSelectRange(*callable, *Settings.Host, holderFactory, Env);
                                }
                                else {
                                    THROW TWithBackTrace<yexception>() << "Unknown callable: "
                                        << callable->GetType()->GetName();
                                }

                                ui32 readCallableId = FromString<ui32>(readName);
                                MKQL_ENSURE(readCallableId == callable->GetUniqueId(),
                                    "Invalid struct member name:" << myReads.GetType()->GetMemberName(readIdx));

                                auto returnType = GetActualReturnType(*callable, Env, Strings);
                                TValuePacker packer(false, returnType);
                                readResults.emplace_back(TString(packer.Pack(readValue)));
                                const TStringBuf& readValueStr = readResults.back();

                                for (ui32 shardIndex = 0; shardIndex < shardsList->GetItemsCount(); ++shardIndex) {
                                    ui64 shardId = AS_VALUE(TDataLiteral, shardsList->GetItems()[shardIndex])->AsValue().Get<ui64>();
                                    if (shardId != myShardId) {
                                        auto& results = resultsPerTarget[shardId];
                                        results.AddResult(callable->GetUniqueId(), readValueStr, Env);
                                    }
                                }
                            } else {
                                for (ui32 shardIndex = 0; shardIndex < shardsList->GetItemsCount(); ++shardIndex) {
                                    ui64 shardId = AS_VALUE(TDataLiteral, shardsList->GetItems()[shardIndex])->AsValue().Get<ui64>();
                                    if (shardId != myShardId) {
                                        // TODO: Pass special 'Tx was Cancelled' flag in the readset so that target shard can cancel this Tx as well
                                        resultsPerTarget[shardId];
                                    }
                                }
                            }
                        }

                        ++readIdx;
                        ++writeIdx;
                    } else if (readName < writeName) {
                        ++readIdx;
                    } else {
                        ++writeIdx;
                    }
                }

                for (auto& result : resultsPerTarget) {
                    TString resultValue = result.second.ToString(holderFactory, Env);
                    OutgoingReadsets.push_back(TReadSet(result.first, pgm.first, resultValue));
                }
            }

            AreOutgoingReadSetsPrepared = true;
            return EResult::Ok;
        }
        catch (TNotReadyTabletException&) {
            throw;
        }
        catch (TMemoryLimitExceededException&) {
            throw;
        }
        catch (yexception& e) {
            Alloc.InvalidateMemInfo();
            HandleException("PrepareOutgoingReadsets", __LINE__, e);
            Status = EStatus::Error;
            return EResult::ProgramError;
        }
    }

    ui32 GetOutgoingReadsetsCount() const noexcept override {
        Y_ABORT_UNLESS(AreOutgoingReadSetsPrepared, "PrepareOutgoingReadsets is not called yet");
        Y_ABORT_UNLESS(!AreOutgoingReadSetsExtracted, "AfterOutgoingReadsetsExtracted is already called");
        return static_cast<ui32>(OutgoingReadsets.size());
    }

    TReadSet GetOutgoingReadset(ui32 index) const override {
        Y_ABORT_UNLESS(AreOutgoingReadSetsPrepared, "PrepareOutgoingReadsets is not called yet");
        Y_ABORT_UNLESS(!AreOutgoingReadSetsExtracted, "AfterOutgoingReadsetsExtracted is already called");
        Y_ABORT_UNLESS(index < OutgoingReadsets.size(), "Bad index");
        return OutgoingReadsets[index];
    }

    void AfterOutgoingReadsetsExtracted() noexcept override {
        Y_ABORT_UNLESS(!AreOutgoingReadSetsExtracted, "AfterOutgoingReadsetsExtracted is already called");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        TVector<TReadSet>().swap(OutgoingReadsets);
        AreOutgoingReadSetsExtracted = true;
    }

    bool IsAfterOutgoingReadsetsExtracted() noexcept override {
        return AreOutgoingReadSetsExtracted;
    }

    EResult PrepareIncomingReadsets() override {
        Y_ABORT_UNLESS(!AreIncomingReadsetsPrepared, "PrepareIncomingReadsets is already called");
        Y_ABORT_UNLESS(Settings.Host, "Host is not set");
        Y_ABORT_UNLESS(ProgramPerOrigin.size() > 0, "At least one program must be added to engine");
        TGuard<TScopedAlloc> allocGuard(Alloc);
        IncomingReadsetsShards.clear();
        try {
            const ui64 myShardId = Settings.Host->GetShardId();
            THashSet<ui64> shards;

            for (const auto& pgm : ProgramPerOrigin) {
                auto& pgmStruct = GetPgmStruct(pgm.second);
                auto& shardForRead = GetPgmShardsForReadStruct(pgmStruct);

                for (ui32 i = 0; i < shardForRead.GetValuesCount(); ++i) {
                    auto shardsList = AS_VALUE(TListLiteral, shardForRead.GetValue(i));
                    auto itemType = shardsList->GetType()->GetItemType();
                    MKQL_ENSURE(itemType->IsData() && static_cast<TDataType*>(itemType)->GetSchemeType()
                        == NUdf::TDataType<ui64>::Id, "Bad shard list");

                    for (ui32 shardIndex = 0; shardIndex < shardsList->GetItemsCount(); ++shardIndex) {
                        ui64 shard = AS_VALUE(TDataLiteral, shardsList->GetItems()[shardIndex])->AsValue().Get<ui64>();
                        if (shard != myShardId) {
                            shards.insert(shard);
                        }
                    }
                }
            }

            IncomingReadsetsShards.assign(shards.begin(), shards.end());
            AreIncomingReadsetsPrepared = true;
            return EResult::Ok;
        }
        catch (yexception& e) {
            Alloc.InvalidateMemInfo();
            HandleException("PrepareOutgoingReadsets", __LINE__, e);
            Status = EStatus::Error;
            return EResult::ProgramError;
        }
    }

    ui32 GetExpectedIncomingReadsetsCount() const noexcept override {
        Y_ABORT_UNLESS(AreIncomingReadsetsPrepared, "PrepareIncomingReadsets is not called yet");
        return static_cast<ui32>(IncomingReadsetsShards.size());
    }

    ui64 GetExpectedIncomingReadsetOriginShard(ui32 index) const noexcept override {
        Y_ABORT_UNLESS(AreIncomingReadsetsPrepared, "PrepareIncomingReadsets is not called yet");
        Y_ABORT_UNLESS(index < IncomingReadsetsShards.size(), "Bad index");
        return IncomingReadsetsShards[index];
    }

    void AddIncomingReadset(const TStringBuf& readset) noexcept override {
        IncomingReadsets.push_back(TString(readset));
    }

    EResult Cancel() override {
        IsCancelled = true;
        Errors = "Tx was cancelled";
        return EResult::Ok;
    }

    EResult Execute() override {
        Y_ABORT_UNLESS(!IsExecuted, "Execute is already called");
        Y_ABORT_UNLESS(Settings.Host, "Host is not set");
        TGuard<TScopedAlloc> allocGuard(Alloc);

        if (IsCancelled) {
            IsExecuted = true;
            Status = EStatus::Error;
            return EResult::Cancelled;
        }

        if (Status == EStatus::Error) {
            IsExecuted = true;
            return EResult::ProgramError;
        }

        ExecutionReplies.clear();
        try {
            TMemoryUsageInfo memInfo("Memory");
            THolderFactory holderFactory(Alloc.Ref(), memInfo, Settings.FunctionRegistry);

            for (auto pgm : ProgramPerOrigin) {
                TShardExecData execData(Settings, Strings, StepTxId);
                for (auto& rs: IncomingReadsets) {
                    TCallableResults results = TCallableResults::FromString(rs, holderFactory, Env);
                    for (const auto& result : results.GetMap()) {
                        execData.Results[result.first].emplace_back(result.second.AsStringRef());
                    }
                }

                {
                    auto& pgmStruct = GetPgmStruct(pgm.second);
                    TRuntimeNode runPgm = GetPgmRun(pgmStruct);
                    auto& runStruct = GetPgmRunStruct(pgmStruct);
                    auto& replyStruct = GetPgmReplyStruct(pgmStruct);
                    auto& myReadsStruct = GetPgmMyReadsStruct(pgmStruct);

                    for (ui32 i = 0; i < myReadsStruct.GetValuesCount(); ++i) {
                        TRuntimeNode member = myReadsStruct.GetValue(i);
                        MKQL_ENSURE(member.GetNode()->GetType()->IsCallable(), "Expected callable");
                        auto callable = static_cast<TCallable*>(member.GetNode());
                        execData.LocalReadCallables.insert(callable->GetUniqueId());
                    }

                    auto expectedSizeIt = ProgramSizes.find(pgm.first);
                    if (expectedSizeIt != ProgramSizes.end()) {
                        MKQL_ENSURE(expectedSizeIt->second != 0,
                            "Undefined program size on consecutive execute, origin: " << pgm.first);
                    } else {
                        expectedSizeIt = ProgramSizes.emplace(pgm.first, 0).first;
                    }

                    TExploringNodeVisitor runExplorer;
                    runExplorer.Walk(&runStruct, Env);

                    auto nodesCount = runExplorer.GetNodes().size();
                    if (expectedSizeIt->second == 0) {
                        expectedSizeIt->second = nodesCount;
                    } else {
                        MKQL_ENSURE(expectedSizeIt->second == nodesCount, "Mismatch program size, expected: "
                            << expectedSizeIt->second << ", got: " << nodesCount << ", origin: " << pgm.first);
                    }

                    TComputationPatternOpts opts(Alloc.Ref(), Env,
                        GetFlatShardExecutionFactory(execData, false),
                        Settings.FunctionRegistry,
                        NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,
                        Settings.LlvmRuntime ? "" : "OFF", EGraphPerProcess::Multi);
                    auto pattern = MakeComputationPattern(runExplorer, runPgm, {}, opts);
                    auto compOpts = opts.ToComputationOptions(Settings.RandomProvider, Settings.TimeProvider, &Env);
                    THolder<IComputationGraph> runGraph = pattern->Clone(compOpts);

                    const TBindTerminator bind(runGraph->GetTerminator());

                    NUdf::TUnboxedValue runValue = runGraph->GetValue();
                    NUdf::TUnboxedValue replyValue = runValue.GetElement(0);
                    NUdf::TUnboxedValue writeValue = runValue.GetElement(1);

                    TCallableResults replyResults;
                    for (ui32 i = 0; i < replyStruct.GetValuesCount(); ++i) {
                        TRuntimeNode item = replyStruct.GetValue(i);
                        Y_ABORT_UNLESS(item.GetNode()->GetType()->IsCallable(), "Bad shard program");
                        auto callable = static_cast<TCallable*>(item.GetNode());
                        auto memberName = replyStruct.GetType()->GetMemberName(i);
                        ui32 resultId = FromString<ui32>(memberName);

                        NUdf::TUnboxedValue resultValue = replyValue.GetElement(i);
                        auto returnType = GetActualReturnType(*callable, Env, Strings);
                        TValuePacker packer(false, returnType);
                        replyResults.AddResult(resultId, packer.Pack(resultValue), Env);
                    }

                    auto replyStr = replyResults.ToString(holderFactory, Env);

                    // Note: we must apply side effects even if we reply with an error below
                    TEngineFlatApplyContext applyCtx;
                    applyCtx.Host = Settings.Host;
                    applyCtx.Env = &Env;
                    ApplyChanges(writeValue, applyCtx);

                    if (replyStr.size() > MaxDatashardReplySize) {
                        TString error = TStringBuilder() << "Datashard " << pgm.first
                            << ": reply size limit exceeded. ("
                            << replyStr.size() << " > " << MaxDatashardReplySize << ")";

                        LogError(TStringBuilder() << "Error executing transaction (read-only: "
                            << ReadOnlyOriginPrograms << "): " << error);

                        AddError(error);
                        Status = EStatus::Error;
                        return EResult::ResultTooBig;
                    }

                    ExecutionReplies[pgm.first] = replyStr;
                }
            }
        }
        catch (TNotReadyTabletException&) {
            throw;
        }
        catch (TMemoryLimitExceededException&) {
            throw;
        }
        catch (TDeadlineExceededException&) {
            Cancel();
            IsExecuted = true;
            Status = EStatus::Error;
            return EResult::Cancelled;
        }
        catch (yexception& e) {
            Alloc.InvalidateMemInfo();
            HandleException("Execute", __LINE__, e);
            Status = EStatus::Error;
            return EResult::ProgramError;
        }

        Y_ABORT_UNLESS(ExecutionReplies.size() == ProgramPerOrigin.size());
        ProgramPerOrigin.clear();
        ProgramSizes.clear();
        IsExecuted = true;
        return EResult::Ok;
    }

    TString GetShardReply(ui64 origin) const noexcept override {
        Y_ABORT_UNLESS(IsExecuted, "Execute is not called yet");
        auto it = ExecutionReplies.find(origin);
        Y_ABORT_UNLESS(it != ExecutionReplies.end(), "Bad origin: %" PRIu64, origin);
        return it->second;
    }

    size_t GetMemoryUsed() const noexcept override {
        return Alloc.GetUsed();
    }

    size_t GetMemoryAllocated() const noexcept override {
        return Alloc.GetAllocated();
    }

    size_t GetMemoryLimit() const noexcept override {
        return Alloc.GetLimit();
    }

    void SetMemoryLimit(size_t limit) noexcept override {
        Alloc.SetLimit(limit);
    }

    void ReleaseUnusedMemory() noexcept override {
        Alloc.ReleaseFreePages();
    }

    void SetDeadline(const TInstant& deadline) noexcept override {
        if (!ReadOnlyOriginPrograms) {
            return;
        }

        auto& timeProvider = Settings.TimeProvider;
        auto checkDeadlineCallback = [&timeProvider, deadline]() {
            if (timeProvider.Now() > deadline) {
                throw TDeadlineExceededException();
            }
        };

        Alloc.Ref().SetAllocNotifyCallback(checkDeadlineCallback, AllocNotifyCallbackBytes);
        if (Settings.Host) {
            Settings.Host->SetPeriodicCallback(checkDeadlineCallback);
        }
    }

private:
    struct TCallableContext {
        TCallable* Node;
        TKeyDesc* Key;
        THashSet<ui64> ShardsToWrite;
        TRuntimeNode ShardsForRead;

        TCallableContext()
            : Node(nullptr)
            , Key(nullptr)
        {}
    };

    struct TProgramParts {
        TRuntimeNode AllReads;
        TRuntimeNode MyKeys;
        TRuntimeNode Reply;
        TRuntimeNode Write;
        TRuntimeNode ShardsForRead;
        TRuntimeNode Program;
    };

    static void AddShards(TSet<ui64>& set, const TKeyDesc& key) {
        for (auto& partition : key.GetPartitions()) {
            Y_ABORT_UNLESS(partition.ShardId);
            set.insert(partition.ShardId);
        }
    }

    void ExtractShardsToWrite(ui64 shard, TExploringNodeVisitor& writeExplorer) {
        for (auto& node : writeExplorer.GetNodes()) {
            if (!node->GetType()->IsCallable()) {
                continue;
            }

            auto callable = static_cast<TCallable*>(node);
            auto name = callable->GetType()->GetNameStr();
            if (name == Strings.SelectRow || name == Strings.SelectRange) {
                auto ctxIt = ProxyCallables.find(callable->GetUniqueId());
                Y_ABORT_UNLESS(ctxIt != ProxyCallables.end());
                ctxIt->second.ShardsToWrite.insert(shard);
            }
        }
    }

    void PrepareShardProgram(ui64 shard, const THashMap<ui32, const TVector<TKeyDesc::TPartitionInfo>*>& proxyShards,
        TProgramParts& parts, THashSet<std::pair<ui64, ui64>>& readsets) {
        parts.Write = BuildWriteProgram(shard, proxyShards);

        TExploringNodeVisitor writeExplorer;
        writeExplorer.Walk(parts.Write.GetNode(), Env);

        BuildMyKeysAndReplies(shard, writeExplorer, parts, readsets);
        ExtractShardsToWrite(shard, writeExplorer);
    }

    void BuildAllReads() {
        TStructLiteralBuilder shardsToWriteBuilder(Env);
        for (auto& callable : ProxyCallables) {
            auto name = callable.second.Node->GetType()->GetNameStr();
            if (name == Strings.SelectRow || name == Strings.SelectRange) {
                TListLiteralBuilder listOfShards(Env, Ui64Type);
                for (auto shard : callable.second.ShardsToWrite) {
                    listOfShards.Add(TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(shard), NUdf::TDataType<ui64>::Id, Env), true));
                }

                shardsToWriteBuilder.Add(ToString(callable.second.Node->GetUniqueId()), TRuntimeNode(listOfShards.Build(), true));
            }
        }

        ShardsToWrite = TRuntimeNode(shardsToWriteBuilder.Build(), true);
    }

    void BuildMyKeysAndReplies(ui64 shard, TExploringNodeVisitor& writeExplorer, TProgramParts& parts,
        THashSet<std::pair<ui64, ui64>>& readsets)
    {
        TStructLiteralBuilder myKeysBuilder(Env);
        TStructLiteralBuilder myReadsBuilder(Env);
        TStructLiteralBuilder myWritesBuilder(Env);
        TStructLiteralBuilder replyBuilder(Env);
        TStructLiteralBuilder readsBuilder(Env);
        TStructLiteralBuilder shardsForReadBuilder(Env);

        for (auto& node : writeExplorer.GetNodes()) {
            if (!node->GetType()->IsCallable()) {
                continue;
            }

            auto callable = static_cast<TCallable*>(node);
            auto name = callable->GetType()->GetNameStr();
            if (name == Strings.UpdateRow || name == Strings.EraseRow) {
                myWritesBuilder.Add(ToString(callable->GetUniqueId()), TRuntimeNode(callable, false));
            }

            if (name == Strings.SelectRow || name == Strings.SelectRange) {
                auto ctxIt = ProxyCallables.find(callable->GetUniqueId());
                Y_ABORT_UNLESS(ctxIt != ProxyCallables.end());
                auto& ctx = ctxIt->second;
                auto uniqueName = ToString(callable->GetUniqueId());
                shardsForReadBuilder.Add(uniqueName, ctx.ShardsForRead);

                for (auto& partition : ctx.Key->GetPartitions()) {
                    auto shardForRead = partition.ShardId;
                    if (shardForRead != shard) {
                        readsets.insert(std::make_pair(shardForRead, shard));
                    }
                }
            }
        }

        auto checkShard = [shard] (const TCallableContext& ctx) {
            auto key = ctx.Key;
            Y_ABORT_UNLESS(key);

            for (auto& partition : key->GetPartitions()) {
                if (partition.ShardId == shard) {
                    return true;
                }
            }

            return false;
        };

        for (auto& callable : ProxyCallables) {
            auto name = callable.second.Node->GetType()->GetNameStr();
            auto uniqueName = ToString(callable.first);

            if (name == Strings.SelectRow || name == Strings.SelectRange) {
                readsBuilder.Add(uniqueName, TRuntimeNode(callable.second.Node, false));
                if (checkShard(callable.second)) {
                    myReadsBuilder.Add(uniqueName, TRuntimeNode(callable.second.Node, false));
                }
            }

            if (ProxyRepliesCallables.contains(callable.first)) {
                TCallableContext* readCtx = &callable.second;

                auto replyRead = ProxyRepliesReads.FindPtr(callable.first);
                if (replyRead) {
                    auto readCtxPtr = ProxyCallables.FindPtr((*replyRead)->GetUniqueId());
                    Y_ABORT_UNLESS(readCtxPtr);
                    readCtx = readCtxPtr;
                }

                if (checkShard(*readCtx)) {
                    replyBuilder.Add(uniqueName, TRuntimeNode(callable.second.Node, false));
                }
            }
        }

        myKeysBuilder.Add("MyReads", TRuntimeNode(myReadsBuilder.Build(), true));
        myKeysBuilder.Add("MyWrites", TRuntimeNode(myWritesBuilder.Build(), true));
        parts.AllReads = TRuntimeNode(readsBuilder.Build(), true);
        parts.MyKeys = TRuntimeNode(myKeysBuilder.Build(), true);
        parts.Reply = TRuntimeNode(replyBuilder.Build(), true);
        parts.ShardsForRead = TRuntimeNode(shardsForReadBuilder.Build(), true);
    }

    void FinalizeShardProgram(TProgramParts& parts) {
        TStructLiteralBuilder builder(Env);
        TStructLiteralBuilder runBuilder(Env);
        runBuilder.Add("Reply", parts.Reply);
        runBuilder.Add("Write", parts.Write);
        builder.Add("AllReads", parts.AllReads);
        builder.Add("MyKeys", parts.MyKeys);
        builder.Add("Run", TRuntimeNode(runBuilder.Build(), true));
        builder.Add("ShardsForRead", parts.ShardsForRead);
        builder.Add("ShardsToWrite", ShardsToWrite);
        TRuntimeNode specializedProgram = TRuntimeNode(builder.Build(), true);

        auto lpoProvider = GetLiteralPropagationOptimizationFuncProvider();
        auto funcProvider = [&](TInternName name) {
            auto lpoFunc = lpoProvider(name);
            if (lpoFunc)
                return lpoFunc;

            if (name == Strings.CombineByKeyMerge) {
                return TCallableVisitFunc([](TCallable& callable, const TTypeEnvironment& env) {
                    Y_UNUSED(env);
                    return callable.GetInput(0);
                });
            } else
            if (name == Strings.PartialSort) {
                return TCallableVisitFunc([](TCallable& callable, const TTypeEnvironment& env) {
                    return RenameCallable(callable, "Sort", env);
                });
            } else
            if (name == Strings.PartialTake) {
                return TCallableVisitFunc([](TCallable& callable, const TTypeEnvironment& env) {
                    return RenameCallable(callable, "Take", env);
                });
            }

            return TCallableVisitFunc();
        };

        TExploringNodeVisitor explorer;
        explorer.Walk(specializedProgram.GetNode(), Env);
        bool wereChanges = false;
        specializedProgram = SinglePassVisitCallables(specializedProgram, explorer, funcProvider, Env,
            false, wereChanges);

        parts.Program = specializedProgram;
    }

    void HandleException(const char* operation, ui32 line, yexception& e) const {
        if (Settings.BacktraceWriter) {
            Settings.BacktraceWriter(operation, line, e.BackTrace());
        }

        AddError(operation, line, e.what());
    }

    void LogError(const TString& message) {
        if (Settings.LogErrorWriter) {
            Settings.LogErrorWriter(message);
        }
    }

    void AddError(const TString& message) const {
        if (!Errors.empty())
            Errors += "\n";

        Errors += message;
    }

    void AddError(const char* operation, ui32 line, const char* text) const {
        AddError(Sprintf("%s (%" PRIu32 "): %s", operation, line, text));
    }

    void PrepareProxyProgram() {
        ui64 nodesCount = ProgramExplorer.GetNodes().size();

        auto lpoProvider = GetLiteralPropagationOptimizationFuncProvider();
        auto funcProvider = [&](TInternName name) {
            auto lpoFunc = lpoProvider(name);
            if (lpoFunc)
                return lpoFunc;

            return TCallableVisitFunc();
        };

        bool wereChanges = false;
        ProxyProgram = SinglePassVisitCallables(Program, const_cast<TExploringNodeVisitor&>(ProgramExplorer), funcProvider, Env, false, wereChanges);
        ProxyProgramExplorer.Walk(ProxyProgram.GetNode(), Env, {}, true, nodesCount);

        auto isPureLambda = [this] (TVector<TNode*> args, TRuntimeNode value) {
            THashSet<ui32> knownArgIds;
            for (auto& arg : args) {
                if (!arg->GetType()->IsCallable()) {
                    return false;
                }

                auto argCallable = static_cast<TCallable*>(arg);
                Y_ABORT_UNLESS(argCallable);

                knownArgIds.insert(argCallable->GetUniqueId());
            }

            TVector<TCallable*> foundArgs;
            THashSet<TNode*> visitedNodes;
            TExploringNodeVisitor lambdaExplorer;
            lambdaExplorer.Walk(value.GetNode(), Env, args);
            for (auto& node : lambdaExplorer.GetNodes()) {
                visitedNodes.insert(node);

                if (node->GetType()->IsCallable()) {
                    auto callable = static_cast<TCallable*>(node);
                    auto name = callable->GetType()->GetNameStr();

                    if (name == Strings.Builtins.Arg && !knownArgIds.contains(callable->GetUniqueId())) {
                        foundArgs.push_back(callable);
                    }

                    if (Strings.All.contains(name)) {
                        return false;
                    }
                }
            }

            for (TCallable* arg : foundArgs) {
                auto& consumers = ProxyProgramExplorer.GetConsumerNodes(*arg);
                for (auto& consumer : consumers) {
                    if (!visitedNodes.contains(consumer)) {
                        return false;
                    }
                }
            }

            return true;
        };

        THashSet<ui32> pureCallables;
        THashSet<ui32> aggregatedCallables;
        THashMap<ui32, ui32> callableConsumers;
        for (auto& node : ProxyProgramExplorer.GetNodes()) {
            if (!node->GetType()->IsCallable()) {
                continue;
            }

            auto callable = static_cast<TCallable*>(node);
            auto id = callable->GetUniqueId();
            auto name = callable->GetType()->GetNameStr();

            callableConsumers[id] = ProxyProgramExplorer.GetConsumerNodes(*callable).size();

            if (name == Strings.Builtins.DictItems ||
                name == Strings.Builtins.Member ||
                name == Strings.Builtins.Take ||
                name == Strings.Builtins.Length ||
                name == Strings.Builtins.FilterNullMembers ||
                name == Strings.Builtins.SkipNullMembers ||
                name == Strings.SelectRange ||
                name == Strings.CombineByKeyMerge ||
                name == Strings.PartialTake)
            {
                pureCallables.insert(id);
            } else
            if (name == Strings.Builtins.Filter ||
                name == Strings.Builtins.Map ||
                name == Strings.Builtins.FlatMap)
            {
                auto arg = callable->GetInput(1);
                auto lambda = callable->GetInput(2);

                // Check lambda for IO callables
                if (isPureLambda({arg.GetNode()}, lambda)) {
                    pureCallables.insert(id);
                }
            } else
            if (name == Strings.Builtins.ToHashedDict) {
                auto arg = callable->GetInput(1);
                auto key = callable->GetInput(2);
                auto payload = callable->GetInput(3);

                if (isPureLambda({arg.GetNode()}, key) && isPureLambda({arg.GetNode()}, payload)) {
                    pureCallables.insert(id);
                }
            } else
            if (name == Strings.PartialSort) {
                auto arg = callable->GetInput(1);
                auto lambda = callable->GetInput(2);
                if (isPureLambda({arg.GetNode()}, lambda)) {
                    pureCallables.insert(id);
                }
            }
        }

        auto firstPass = [&](TInternName name) {
            if (name == Strings.EraseRow || name == Strings.UpdateRow) {
                return TCallableVisitFunc(&ReplaceAsVoid);
            }

            auto lpoFunc = lpoProvider(name);
            if (lpoFunc)
                return lpoFunc;

            return TCallableVisitFunc();
        };

        ProxyProgram = SinglePassVisitCallables(ProxyProgram, const_cast<TExploringNodeVisitor&>(ProxyProgramExplorer),
            firstPass, Env, false, wereChanges);
        ProxyProgramExplorer.Walk(ProxyProgram.GetNode(), Env, {});

        auto getCallableForPushdown = [&pureCallables, &callableConsumers] (TRuntimeNode node,
            TInternName name) -> TCallable*
        {
            if (!node.GetNode()->GetType()->IsCallable()) {
                return nullptr;
            }

            auto callable = static_cast<TCallable*>(node.GetNode());
            if (name && callable->GetType()->GetNameStr() != name) {
                return nullptr;
            }

            auto consumersPtr = callableConsumers.FindPtr(callable->GetUniqueId());
            Y_ABORT_UNLESS(consumersPtr);

            // Make sure we're an exclusive consumer of the input
            if (*consumersPtr > 1) {
                return nullptr;
            }

            if (!pureCallables.contains(callable->GetUniqueId())) {
                return nullptr;
            }

            return callable;
        };

        for (auto& node : ProxyProgramExplorer.GetNodes()) {
            if (!node->GetType()->IsCallable()) {
                continue;
            }

            auto callable = static_cast<TCallable*>(node);
            auto name = callable->GetType()->GetNameStr();

            auto tryPushdownCallable =
                [this, callable, &pureCallables, &aggregatedCallables, getCallableForPushdown]
                (TCallable* input, bool aggregated) {
                    if (!pureCallables.contains(callable->GetUniqueId())) {
                        return;
                    }

                    // Walk through Member callable, required as SelectRange returns a struct
                    if (input->GetType()->GetNameStr() == Strings.Builtins.Member) {
                        input = getCallableForPushdown(input->GetInput(0), TInternName());
                        if (!input) {
                            return;
                        }
                    }

                    if (ProxyRepliesCallables.contains(input->GetUniqueId()) &&
                        !aggregatedCallables.contains(input->GetUniqueId()))
                    {
                        if (ProxyRepliesReads.contains(callable->GetUniqueId())) {
                            return;
                        }

                        auto inputRead = ProxyRepliesReads.FindPtr(input->GetUniqueId());
                        Y_ABORT_UNLESS(ProxyRepliesReads.insert(std::make_pair(callable->GetUniqueId(),
                            inputRead ? *inputRead : input)).second);

                        // Mark callable as a reply from datashards
                        ProxyRepliesCallables.erase(input->GetUniqueId());
                        ProxyRepliesCallables[callable->GetUniqueId()] = callable;

                        if (aggregated) {
                            aggregatedCallables.insert(callable->GetUniqueId());
                        }
                    }
                };

            if (name == Strings.SelectRow || name == Strings.SelectRange) {
                auto ctxIt = ProxyCallables.find(callable->GetUniqueId());
                Y_ABORT_UNLESS(ctxIt != ProxyCallables.end());
                ProxyRepliesCallables[callable->GetUniqueId()] = callable;
            } else if (name == Strings.Builtins.Filter ||
                       name == Strings.Builtins.FilterNullMembers ||
                       name == Strings.Builtins.SkipNullMembers ||
                       name == Strings.Builtins.Map ||
                       name == Strings.Builtins.FlatMap)
            {
                // Push computation of map callables down to datashards
                auto input = getCallableForPushdown(callable->GetInput(0), TInternName());
                if (!input) {
                    continue;
                }

                tryPushdownCallable(input, false);
            } else if (name == Strings.CombineByKeyMerge) {
                // Push computation of partial aggregations down to datashards
                auto flatmap = getCallableForPushdown(callable->GetInput(0), Strings.Builtins.FlatMap);
                if (!flatmap) {
                    continue;
                }
                auto items = getCallableForPushdown(flatmap->GetInput(0), Strings.Builtins.DictItems);
                if (!items) {
                    continue;
                }
                auto dict = getCallableForPushdown(items->GetInput(0), Strings.Builtins.ToHashedDict);
                if (!dict) {
                    continue;
                }
                auto preMap = getCallableForPushdown(dict->GetInput(0), Strings.Builtins.FlatMap);
                if (!preMap) {
                    continue;
                }

                tryPushdownCallable(preMap, true);
            } else if (name == Strings.Builtins.Take || name == Strings.PartialTake) {
                auto count = callable->GetInput(1);
                if (name == Strings.Builtins.Take && !count.IsImmediate()) {
                    continue;
                }

                if (!callable->GetInput(0).GetStaticType()->IsList()) {
                    continue;
                }

                auto input = getCallableForPushdown(callable->GetInput(0), TInternName());
                if (!input) {
                    continue;
                }

                if (name == Strings.PartialTake && input->GetType()->GetNameStr() == Strings.PartialSort) {
                    input = getCallableForPushdown(input->GetInput(0), TInternName());
                    if (!input) {
                        continue;
                    }
                }

                bool aggregated = false;
                if (name == Strings.Builtins.Take) {
                    aggregated = true;

                    auto inputName = input->GetType()->GetNameStr();
                    if (inputName != Strings.Builtins.Filter &&
                        inputName != Strings.Builtins.Map &&
                        inputName != Strings.Builtins.FlatMap)
                    {
                        // No pushdown of Take over ordered sequence, requires specific merge.
                        continue;
                    }
                }

                tryPushdownCallable(input, aggregated);
            } else if (name == Strings.Builtins.Length) {
                if (!callable->GetInput(0).GetStaticType()->IsList()) {
                    continue;
                }

                auto input = getCallableForPushdown(callable->GetInput(0), TInternName());
                if (!input) {
                    continue;
                }

                tryPushdownCallable(input, true);
            }
        }

        auto secondPass = [&](TInternName name) {
            if (name == Strings.CombineByKeyMerge) {
                return TCallableVisitFunc([this](TCallable& callable, const TTypeEnvironment& env) {
                    Y_UNUSED(env);
                    return ProxyRepliesCallables.contains(callable.GetUniqueId())
                        ? TRuntimeNode(&callable, false)
                        : callable.GetInput(0);
                });
            } else
            if (name == Strings.PartialSort) {
                return TCallableVisitFunc([this](TCallable& callable, const TTypeEnvironment& env) {
                    return ProxyRepliesCallables.contains(callable.GetUniqueId())
                        ? TRuntimeNode(&callable, false)
                        : RenameCallable(callable, "Sort", env);
                });
            } else
            if (name == Strings.PartialTake) {
                return TCallableVisitFunc([this](TCallable& callable, const TTypeEnvironment& env) {
                    return ProxyRepliesCallables.contains(callable.GetUniqueId())
                        ? TRuntimeNode(&callable, false)
                        : RenameCallable(callable, "Take", env);
                });
            }

            return TCallableVisitFunc();
        };

        ProxyProgram = SinglePassVisitCallables(ProxyProgram, const_cast<TExploringNodeVisitor&>(ProxyProgramExplorer),
            secondPass, Env, false, wereChanges);
        ProxyProgramExplorer.Walk(ProxyProgram.GetNode(), Env, {});
    }

    void PrepareShardsForRead() {
        for (auto& callable : ProxyCallables) {
            auto& ctx = callable.second;
            auto name = ctx.Node->GetType()->GetNameStr();
            if (name == Strings.SelectRow || name == Strings.SelectRange) {
                auto key = ctx.Key;
                Y_ABORT_UNLESS(key);

                TListLiteralBuilder listOfShards(Env, Ui64Type);
                for (auto& partition : key->GetPartitions()) {
                    listOfShards.Add(TRuntimeNode(BuildDataLiteral(NUdf::TUnboxedValuePod(partition.ShardId),
                        NUdf::TDataType<ui64>::Id, Env), true));
                }

                ctx.ShardsForRead = TRuntimeNode(listOfShards.Build(), true);
            }
        }
    }

    TRuntimeNode BuildWriteProgram(ui64 myShardId, const THashMap<ui32,
        const TVector<TKeyDesc::TPartitionInfo>*>& proxyShards) const
    {
        TCallableVisitFunc checkMyShardForWrite = [&](TCallable& callable, const TTypeEnvironment& env) {
            auto partitions = proxyShards.FindPtr(callable.GetUniqueId());
            Y_ABORT_UNLESS(partitions);
            for (auto partition : *(*partitions)) {
                Y_ABORT_UNLESS(partition.ShardId);
                if (myShardId == partition.ShardId) {
                    return TRuntimeNode(&callable, false);
                }
            }

            return TRuntimeNode(env.GetVoidLazy(), true);
        };

        auto lpoProvider = GetLiteralPropagationOptimizationFuncProvider();
        auto funcProvider = [&](TInternName name) {
            auto lpoFunc = lpoProvider(name);
            if (lpoFunc)
                return lpoFunc;

            if (name == Strings.Abort || name == Strings.SetResult)
                return TCallableVisitFunc(&ReplaceAsVoid);

            if (name == Strings.EraseRow || name == Strings.UpdateRow)
                return TCallableVisitFunc(checkMyShardForWrite);

            return TCallableVisitFunc();
        };

        bool wereChanges = false;
        return SinglePassVisitCallables(Program, const_cast<TExploringNodeVisitor&>(ProgramExplorer), funcProvider, Env, false, wereChanges);
    }

private:
    const TEngineFlatSettings Settings;
    mutable TScopedAlloc Alloc;
    TTypeEnvironment Env;
    TFlatEngineStrings Strings;
    TDataType* Ui64Type;
    TStructType* ResultType;
    std::pair<ui64, ui64> StepTxId;
    TVector<IEngineFlat::TTxLock> TxLocks;
    TMaybe<ui64> LockTxId;
    bool NeedDiagnostics;
    TVector<IEngineFlat::TTabletInfo> TabletInfos;

    mutable TString Errors;
    EStatus Status;
    TRuntimeNode Program;
    TExploringNodeVisitor ProgramExplorer;
    TRuntimeNode ProxyProgram;
    TExploringNodeVisitor ProxyProgramExplorer;
    TVector<TProgramParts> SpecializedParts;
    TMap<ui64, TRuntimeNode> ProgramPerOrigin;
    TMap<ui64, ui64> ProgramSizes;
    TVector<THolder<TKeyDesc>> DbKeys;
    TVector<TShardData> AffectedShards;
    TMaybe<bool> ReadOnlyProgram;
    THashMap<ui32, TCallableContext> ProxyCallables;
    THashMap<ui32, TCallable*> ProxyRepliesCallables;
    THashMap<ui32, TCallable*> ProxyRepliesReads;
    TRuntimeNode ShardsToWrite;
    bool AreAffectedShardsPrepared;
    bool AreShardProgramsExtracted;
    TSet<ui64> FinalizedShards;
    bool IsResultBuilt;
    bool IsProgramValidated;
    bool AreOutgoingReadSetsPrepared;
    bool AreOutgoingReadSetsExtracted;
    TVector<TReadSet> OutgoingReadsets;
    bool AreIncomingReadsetsPrepared;
    TVector<ui64> IncomingReadsetsShards;
    TVector<TString> IncomingReadsets;
    THashMap<ui64, TStructLiteral*> ReadPerOrigin;
    bool IsExecuted;
    TMap<ui64, TString> ExecutionReplies;
    IComputationPattern::TPtr Pattern;
    THolder<IComputationGraph> ResultGraph;
    THashMap<TString, NUdf::TUnboxedValue> ResultValues;
    bool ReadOnlyOriginPrograms;
    bool IsCancelled;
};

}

TAutoPtr<IEngineFlat> CreateEngineFlat(const TEngineFlatSettings& settings) {
    Y_ABORT_UNLESS(settings.Protocol == IEngineFlat::EProtocol::V1);
    return new TEngineFlat(settings);
}

} // namespace NMiniKQL
} // namespace NKikimr

template<>
void Out<NKikimr::NMiniKQL::IEngineFlat::EStatus>(IOutputStream& o, NKikimr::NMiniKQL::IEngineFlat::EStatus status) {
    using namespace NKikimr::NMiniKQL;
    switch (status) {
        case IEngineFlat::EStatus::Unknown:   o << "Unknown"; break;
        case IEngineFlat::EStatus::Error:     o << "Error"; break;
        case IEngineFlat::EStatus::Complete:  o << "Complete"; break;
        case IEngineFlat::EStatus::Aborted:   o << "Aborted"; break;
    }
}
