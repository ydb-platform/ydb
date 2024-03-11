#pragma once

#include "mkql_keys.h"

#include <ydb/library/yql/minikql/defs.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/mkql_proto/protos/minikql.pb.h>
#include <ydb/core/protos/minikql_engine.pb.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>

namespace NKikimr {
namespace NMiniKQL {

const TStringBuf TxInternalResultPrefix = "__";
const TStringBuf TxLocksResultLabel = "__tx_locks";
const TStringBuf TxLocksResultLabel2 = "__tx_locks2";
const TStringBuf TxInfoResultLabel = "__tx_info";

// Should be strictly less than NActors::TEventPB::MaxByteSize to avoid VERIFY in actorlib
const ui32 MaxDatashardReplySize = 48 * 1024 * 1024; // 48 MB
const ui32 MaxProxyReplySize = 48 * 1024 * 1024; // 48 MB

class IEngineFlat {
public:
    virtual ~IEngineFlat() {}

    enum class EProtocol {
        V1 = 0,
    };

    enum class EStatus {
        Unknown = 0,
        Error = 1,
        Complete = 2,
        Aborted = 3
    };

    using EResult = NKikimrMiniKQLEngine::EResult;

    struct TShardData {
        ui64 ShardId;
        TString Program;
        bool HasWrites;
        bool HasOnlineReads;
        bool Immediate;

        TShardData()
            : ShardId(0)
            , HasWrites(false)
            , HasOnlineReads(false)
            , Immediate(false)
        {}

        TShardData(ui64 shardId, const TString& program)
            : ShardId(shardId)
            , Program(program)
            , HasWrites(false)
            , HasOnlineReads(false)
            , Immediate(false)
        {}
    };

    struct TReadSet {
        ui64 TargetShardId;
        ui64 OriginShardId;
        TRuntimeNode Root;
        TString Body;

        TReadSet()
        {}

        TReadSet(ui64 targetShardId, ui64 originShardId, TRuntimeNode root)
            : TargetShardId(targetShardId)
            , OriginShardId(originShardId)
            , Root(root)
        {}

        TReadSet(ui64 targetShardId, ui64 originShardId, const TString& body)
            : TargetShardId(targetShardId)
            , OriginShardId(originShardId)
            , Body(body)
        {}
    };

    struct TShardLimits {
        ui32 ShardCount = 10000;
        ui32 RSCount = 1000000;

        TShardLimits()
        {}

        TShardLimits(ui32 shardCount, ui32 rsCount)
            : ShardCount(shardCount)
            , RSCount(rsCount)
        {}
    };

    struct TTxLock {
        TTxLock(ui64 lockId, ui64 dataShard, ui32 generation, ui64 counter, ui64 ssId, ui64 pathId)
            : LockId(lockId)
            , DataShard(dataShard)
            , Generation(generation)
            , Counter(counter)
            , SchemeShard(ssId)
            , PathId(pathId)
        {}

        ui64 LockId;
        ui64 DataShard;
        ui32 Generation;
        ui64 Counter;
        ui64 SchemeShard;
        ui64 PathId;
    };

    struct TTabletInfo {
        struct TTxInfo {
            std::pair<ui64, ui64> StepTxId = {0,0};
            ui32 Status = 0;
            TInstant PrepareArriveTime;
            TDuration ProposeLatency;
            TDuration ExecLatency;
        };

        TTabletInfo(ui64 tabletId, const std::pair<ui64, ui64>& actorId, ui32 gen, ui64 step, bool isFollower, TTxInfo&& txInfo)
            : TabletId(tabletId)
            , ActorId(actorId)
            , TabletGenStep(gen, step)
            , IsFollower(isFollower)
            , TxInfo(txInfo)
        {}

        ui64 TabletId;
        std::pair<ui64, ui64> ActorId;
        std::pair<ui32, ui64> TabletGenStep;
        bool IsFollower;
        TTxInfo TxInfo;
    };

    struct TValidatedKey {
        std::unique_ptr<TKeyDesc> Key;
        bool IsWrite;
        bool IsResultPart;
        THashSet<ui64> TargetShards;

        TValidatedKey(THolder<TKeyDesc>&& key, bool isWrite)
            : Key(key.Release())
            , IsWrite(isWrite)
            , IsResultPart(false)
        {}

        TValidatedKey(TValidatedKey&&) = default;
        TValidatedKey(const TValidatedKey&) = delete;

        bool NeedSizeCalculation() const { return !IsWrite && (IsResultPart || TargetShards); }
    };

    struct TValidationInfo {
        TVector<TValidatedKey> Keys;
        ui32 ReadsCount;
        ui32 WritesCount;
        ui32 DynKeysCount;
        bool HasOutReadsets;
        bool HasInReadsets;
        bool Loaded;

        TValidationInfo() {
            Clear();
        }

        TValidationInfo(TValidationInfo&&) = default;
        TValidationInfo(const TValidationInfo&) = delete;

        bool HasWrites() const { return WritesCount > 0; }
        bool HasReads() const { return ReadsCount > 0; }

        void Clear() {
            Keys.clear();
            ReadsCount = 0;
            WritesCount = 0;
            DynKeysCount = 0;
            HasOutReadsets = false;
            HasInReadsets = false;
            Loaded = false;
        }

        void SetLoaded() {
            Loaded = true;
        }
    };

    //-- error reporting
    virtual TString GetErrors() const noexcept = 0;

    //-- proxy/shard interface
    virtual void SetStepTxId(const std::pair<ui64, ui64>& stepTxId) noexcept = 0;
    virtual void AddTabletInfo(IEngineFlat::TTabletInfo&& info) noexcept = 0;
    virtual void AddTxLock(IEngineFlat::TTxLock&& txLock) noexcept = 0;
    virtual TMaybe<ui64> GetLockTxId() noexcept = 0;
    virtual bool HasDiagnosticsRequest() noexcept = 0;

    //-- proxy interface
    virtual EResult SetProgram(TStringBuf program, TStringBuf params = TStringBuf()) noexcept = 0;
    virtual TVector<THolder<TKeyDesc>>& GetDbKeys() noexcept = 0;
    virtual EResult PrepareShardPrograms(const TShardLimits& shardLimits = TShardLimits(), ui32* outRSCount = nullptr) noexcept = 0;
    virtual ui32 GetAffectedShardCount() const noexcept = 0;
    virtual EResult GetAffectedShard(ui32 index, TShardData& data) const noexcept = 0;
    virtual void AfterShardProgramsExtracted() noexcept = 0;
    virtual void AddShardReply(ui64 origin, const TStringBuf& reply) noexcept = 0;
    virtual void FinalizeOriginReplies(ui64 origin) noexcept = 0;
    virtual void BuildResult() noexcept = 0;
    virtual EStatus GetStatus() const noexcept = 0;
    virtual EResult FillResultValue(NKikimrMiniKQL::TResult& result) const noexcept = 0;
    virtual bool IsReadOnlyProgram() const noexcept = 0;

    //-- datashard interface
    virtual EResult AddProgram(ui64 origin, const TStringBuf& program, bool readOnly = false) noexcept = 0;
    virtual EResult ValidateKeys(TValidationInfo& validationInfo) = 0;
    virtual EResult Validate(TValidationInfo& validationInfo) = 0;

    virtual EResult PrepareOutgoingReadsets() = 0;
    virtual ui32 GetOutgoingReadsetsCount() const noexcept = 0;
    virtual TReadSet GetOutgoingReadset(ui32 index) const = 0;
    virtual void AfterOutgoingReadsetsExtracted() noexcept = 0;
    virtual bool IsAfterOutgoingReadsetsExtracted() noexcept = 0;

    virtual EResult PrepareIncomingReadsets() = 0;
    virtual ui32 GetExpectedIncomingReadsetsCount() const noexcept = 0;
    virtual ui64 GetExpectedIncomingReadsetOriginShard(ui32 index) const noexcept = 0;
    virtual void AddIncomingReadset(const TStringBuf& readset) noexcept = 0;

    virtual EResult Cancel() = 0;
    virtual EResult PinPages(ui64 pageFaultCount = 0) = 0;
    virtual EResult Execute() = 0;
    virtual TString GetShardReply(ui64 origin) const noexcept = 0;

    virtual size_t GetMemoryUsed() const noexcept = 0;
    virtual size_t GetMemoryAllocated() const noexcept = 0;

    virtual size_t GetMemoryLimit() const noexcept = 0;
    virtual void SetMemoryLimit(size_t limit) noexcept = 0;

    virtual void SetDeadline(const TInstant& deadline) noexcept = 0;

    virtual void ReleaseUnusedMemory() noexcept = 0;
};

} // namespace NMiniKQL

namespace NMiniKQL {
    class IEngineFlatHost;
    class IFunctionRegistry;

    struct TEngineFlatSettings {
        const IEngineFlat::EProtocol Protocol;
        const IFunctionRegistry* FunctionRegistry;
        IRandomProvider& RandomProvider;
        ITimeProvider& TimeProvider;
        IEngineFlatHost* Host;
        TAlignedPagePoolCounters AllocCounters;
        std::function<void(const char* operation, ui32 line, const TBackTrace*)> BacktraceWriter;
        std::function<void(const TString& message)> LogErrorWriter;
        bool ForceOnline;
        bool EvaluateResultType = true;
        bool EvaluateResultValue = true;
        bool LlvmRuntime = false;

        TEngineFlatSettings(
                IEngineFlat::EProtocol protocol,
                const IFunctionRegistry* functionRegistry,
                IRandomProvider& randomProvider,
                ITimeProvider& timeProvider,
                IEngineFlatHost* host = nullptr,
                const TAlignedPagePoolCounters& allocCounters = TAlignedPagePoolCounters()
                )
            : Protocol(protocol)
            , FunctionRegistry(functionRegistry)
            , RandomProvider(randomProvider)
            , TimeProvider(timeProvider)
            , Host(host)
            , AllocCounters(allocCounters)
            , ForceOnline(false)
        {
            Y_ABORT_UNLESS(FunctionRegistry);
        }
    };

    TAutoPtr<IEngineFlat> CreateEngineFlat(const TEngineFlatSettings& settings);

} // namespace NMiniKQL
} // namespace NKikimr
