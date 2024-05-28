#include "kqp_sequencer_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/engine/minikql/minikql_engine_host.h>
#include <ydb/core/kqp/common/kqp_resolve.h>
#include <ydb/core/kqp/gateway/kqp_gateway.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_stats.pb.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/kqp/common/kqp_event_ids.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/kqp/runtime/kqp_scan_data.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>
#include <ydb/core/tx/sequenceproxy/public/events.h>

#include <list>

namespace NKikimr {
namespace NKqp {

namespace {

NScheme::TTypeInfo BuildTypeInfo(const ::NKikimrKqp::TKqpColumnMetadataProto& proto) {
    NScheme::TTypeId typeId = static_cast<NScheme::TTypeId>(proto.GetTypeId());
    if (typeId != NKikimr::NScheme::NTypeIds::Pg) {
        return NScheme::TTypeInfo(typeId);
    } else {
        return NScheme::TTypeInfo(typeId, NPg::TypeDescFromPgTypeId(proto.GetTypeInfo().GetPgTypeId()));
    }
}

using namespace NKikimr::NSequenceProxy;

const NActors::TActorId SequenceProxyId = NSequenceProxy::MakeSequenceProxyServiceID();

class TKqpSequencerActor : public NActors::TActorBootstrapped<TKqpSequencerActor>, public NYql::NDq::IDqComputeActorAsyncInput {

    struct TColumnSequenceInfo {
        using TCProto = NKikimrKqp::TKqpColumnMetadataProto;

        TString DefaultFromSequence;
        std::set<i64> AllocatedSequenceValues;
        NScheme::TTypeInfo TypeInfo;
        NUdf::TUnboxedValue UvLiteral;
        Ydb::TypedValue Literal;
        bool InitialiedLiteral = false;
        TCProto::EDefaultKind DefaultKind = TCProto::DEFAULT_KIND_UNSPECIFIED; 

        explicit TColumnSequenceInfo(const ::NKikimrKqp::TKqpColumnMetadataProto& proto)
            : TypeInfo(BuildTypeInfo(proto))
            , DefaultKind(proto.GetDefaultKind())
        {
            if (DefaultKind == TCProto::DEFAULT_KIND_SEQUENCE) {
                DefaultFromSequence = proto.GetDefaultFromSequence();
            }

            if (DefaultKind == TCProto::DEFAULT_KIND_LITERAL) {
                Literal = proto.GetDefaultFromLiteral();
            }
        }

        bool IsDefaultFromLiteral() const {
            return DefaultKind == TCProto::DEFAULT_KIND_LITERAL;
        }

        bool IsDefaultFromSequence() const {
            return DefaultKind == TCProto::DEFAULT_KIND_SEQUENCE;
        }

        bool HasValues() const {
            return AllocatedSequenceValues.size() > 0;
        }

        NUdf::TUnboxedValue GetDefaultLiteral(
            const NMiniKQL::TTypeEnvironment& env, const NMiniKQL::THolderFactory& factory)
        {
            YQL_ENSURE(IsDefaultFromLiteral());
            if (InitialiedLiteral) {
                return UvLiteral;
            }

            InitialiedLiteral = true;
            NKikimr::NMiniKQL::TType* type = nullptr;
            std::tie(type, UvLiteral) = NMiniKQL::ImportValueFromProto(
                Literal.type(), Literal.value(), env, factory);
            return UvLiteral;
        }

        i64 AcquireNextVal() {
            YQL_ENSURE(HasValues());
            i64 res = *AllocatedSequenceValues.begin();
            AllocatedSequenceValues.erase(AllocatedSequenceValues.begin());
            return res;
        }
    };

public:
    TKqpSequencerActor(ui64 inputIndex, NYql::NDq::TCollectStatsLevel statsLevel, const NUdf::TUnboxedValue& input,
        const NActors::TActorId& computeActorId, const NMiniKQL::TTypeEnvironment& typeEnv,
        const NMiniKQL::THolderFactory& holderFactory, std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc,
        NKikimrKqp::TKqpSequencerSettings&& settings, TIntrusivePtr<TKqpCounters> counters)
        : LogPrefix(TStringBuilder() << "SequencerActor, inputIndex: " << inputIndex << ", CA Id " << computeActorId)
        , InputIndex(inputIndex)
        , Input(input)
        , ComputeActorId(computeActorId)
        , TypeEnv(typeEnv)
        , HolderFactory(holderFactory)
        , Alloc(alloc)
        , Settings(std::move(settings))
        , Counters(counters)
    {
        ColumnSequenceInfo.reserve(Settings.GetColumns().size());
        for(int colId = 0; colId < Settings.GetColumns().size(); ++colId) {
            const auto& col = Settings.GetColumns(colId);
            ColumnSequenceInfo.emplace_back(col);
        }
        IngressStats.Level = statsLevel;
    }

    virtual ~TKqpSequencerActor() {
        if (Input.HasValue() && Alloc) {
            TGuard<NMiniKQL::TScopedAlloc> allocGuard(*Alloc);
            Input.Clear();

            NKikimr::NMiniKQL::TUnboxedValueDeque emptyList;
            emptyList.swap(PendingRows);

            for(int columnIdx = 0; columnIdx < Settings.GetColumns().size(); ++columnIdx) {
                auto& columnInfo = ColumnSequenceInfo[columnIdx];
                columnInfo.UvLiteral.Clear();
            }
        }
    }

    void Bootstrap() {
        Counters->SequencerActorsCount->Inc();

        CA_LOG_D("Start stream lookup actor");
        Become(&TKqpSequencerActor::StateFunc);
    }

private:
    void SaveState(const NYql::NDqProto::TCheckpoint&, NYql::NDq::TSourceState&) final {}
    void LoadState(const NYql::NDq::TSourceState&) final {}
    void CommitState(const NYql::NDqProto::TCheckpoint&) final {}

    ui64 GetInputIndex() const final {
        return InputIndex;
    }

    const NYql::NDq::TDqAsyncStats& GetIngressStats() const final {
        return IngressStats;
    }

    void PassAway() final {
        Counters->SequencerActorsCount->Dec();

        {
            auto guard = BindAllocator();
            Input.Clear();
            NKikimr::NMiniKQL::TUnboxedValueDeque emptyList;
            emptyList.swap(PendingRows);

            for(int columnIdx = 0; columnIdx < Settings.GetColumns().size(); ++columnIdx) {
                auto& columnInfo = ColumnSequenceInfo[columnIdx];
                columnInfo.UvLiteral.Clear();
            }
        }

        TActorBootstrapped<TKqpSequencerActor>::PassAway();
    }

    i64 GetAsyncInputData(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, TMaybe<TInstant>&, bool& finished, i64 freeSpace) final {
        YQL_ENSURE(!batch.IsWide(), "Wide stream is not supported");

        i64 totalDataSize = ReplyResult(batch, freeSpace);
        auto status = FetchRows();

        finished = (status == NUdf::EFetchStatus::Finish)
            && (UnprocessedRows == 0);

        if (PendingRows.size() > 0 && WaitingReplies == 0) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        } 

        CA_LOG_D("Returned " << totalDataSize << " bytes, finished: " << finished);
        return totalDataSize;
    }

    STFUNC(StateFunc) {
        try {
            switch (ev->GetTypeRewrite()) {
                    hFunc(TEvSequenceProxy::TEvNextValResult, Handle);
                default:
                    RuntimeError(TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite(),
                        NYql::NDqProto::StatusIds::INTERNAL_ERROR);
            }
        } catch (const yexception& e) {
            RuntimeError(e.what(), NYql::NDqProto::StatusIds::INTERNAL_ERROR);
        }
    }

    i64 ReplyResult(NKikimr::NMiniKQL::TUnboxedValueBatch& batch, i64 freeSpace) {
        auto guard = BindAllocator();

        bool hasSequences = true;
        i64 totalSize = 0;

        for(int columnIdx = 0; columnIdx < Settings.GetColumns().size(); ++columnIdx) {
            auto& columnInfo = ColumnSequenceInfo[columnIdx];
            if (columnInfo.IsDefaultFromSequence()) {
                hasSequences &= columnInfo.HasValues();
            }
        }

        while(PendingRows.size() > 0 && freeSpace > 0 && hasSequences) {
            --UnprocessedRows;
            i64 rowSize = 0;
            NUdf::TUnboxedValue currentValue = std::move(PendingRows.front());
            PendingRows.pop_front();

            NUdf::TUnboxedValue* rowItems = nullptr;
            auto newValue = HolderFactory.CreateDirectArrayHolder(Settings.GetColumns().size(), rowItems);

            int inputColIdx = 0;
            for(int columnIdx = 0; columnIdx < Settings.GetColumns().size(); ++columnIdx) {
                auto& columnInfo = ColumnSequenceInfo[columnIdx];
                if (columnInfo.IsDefaultFromLiteral()) {
                    NUdf::TUnboxedValue defaultV = columnInfo.GetDefaultLiteral(TypeEnv, HolderFactory);
                    rowSize += NMiniKQL::GetUnboxedValueSize((defaultV), columnInfo.TypeInfo).AllocatedBytes;
                    *rowItems++ = defaultV;
                } else if (columnInfo.IsDefaultFromSequence()) {
                    i64 nextVal = columnInfo.AcquireNextVal();
                    *rowItems++ = NUdf::TUnboxedValuePod(nextVal);
                    rowSize += sizeof(NUdf::TUnboxedValuePod);
                    hasSequences &= columnInfo.HasValues();
                } else {
                    *rowItems++ = currentValue.GetElement(inputColIdx);
                    rowSize += NMiniKQL::GetUnboxedValueSize(currentValue.GetElement(inputColIdx), columnInfo.TypeInfo).AllocatedBytes;
                    ++inputColIdx;
                }
            }

            totalSize += rowSize;
            freeSpace -= rowSize;

            batch.emplace_back(std::move(newValue));
        }

        return totalSize;
    }

    void SendSequencerRequests(size_t pendingRequests) {
        while(pendingRequests > 0) {
            --pendingRequests;
            for(size_t colIdx = 0; colIdx < ColumnSequenceInfo.size(); ++colIdx) {
                const auto& col = ColumnSequenceInfo[colIdx];

                if (!col.IsDefaultFromSequence()) {
                    continue;
                }

                Send(SequenceProxyId, new TEvSequenceProxy::TEvNextVal(Settings.GetDatabase(), col.DefaultFromSequence), 0, colIdx);
                WaitingReplies++;
            }
        }
    }

    void Handle(TEvSequenceProxy::TEvNextValResult::TPtr& ev) {
        WaitingReplies--;

        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Counters->SequencerErrors->Inc();
            TStringBuilder result;
            result << "Failed to get next val for sequence: " << ColumnSequenceInfo[ev->Cookie].DefaultFromSequence
                << ", status: " << ev->Get()->Status; 
            RuntimeError(result, NYql::NDq::YdbStatusToDqStatus(ev->Get()->Status), ev->Get()->Issues);
            return;
        }

        Counters->SequencerOk->Inc();

        auto& allocs = ColumnSequenceInfo[ev->Cookie].AllocatedSequenceValues;
        allocs.emplace(ev->Get()->Value);
        if (WaitingReplies == 0) {
            Send(ComputeActorId, new TEvNewAsyncInputDataArrived(InputIndex));
        }
    }

    NUdf::EFetchStatus FetchRows() {
        auto guard = BindAllocator();

        NUdf::EFetchStatus status;
        NUdf::TUnboxedValue currentValue;

        size_t addedRows = 0;
        while ((status = Input.Fetch(currentValue)) == NUdf::EFetchStatus::Ok) {
            PendingRows.push_back(std::move(currentValue));
            UnprocessedRows++;
            addedRows++;
        }

        SendSequencerRequests(addedRows);

        return status;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TypeEnv.BindAllocator();
    }

    void RuntimeError(const TString& message, NYql::NDqProto::StatusIds::StatusCode statusCode, const NYql::TIssues& subIssues = {}) {
        NYql::TIssue issue(message);
        for (const auto& i : subIssues) {
            issue.AddSubIssue(MakeIntrusive<NYql::TIssue>(i));
        }

        NYql::TIssues issues;
        issues.AddIssue(std::move(issue));
        Send(ComputeActorId, new TEvAsyncInputError(InputIndex, std::move(issues), statusCode));
    }

private:
    const TString LogPrefix;
    const ui64 InputIndex;
    NYql::NDq::TDqAsyncStats IngressStats;
    NUdf::TUnboxedValue Input;
    NKikimr::NMiniKQL::TUnboxedValueBatch UnprocessedBatch;
    const NActors::TActorId ComputeActorId;
    const NMiniKQL::TTypeEnvironment& TypeEnv;
    const NMiniKQL::THolderFactory& HolderFactory;
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc;
    NKikimrKqp::TKqpSequencerSettings Settings;
    NKikimr::NMiniKQL::TUnboxedValueDeque PendingRows;
    std::vector<TColumnSequenceInfo> ColumnSequenceInfo;
    ui64 UnprocessedRows = 0;
    i64 WaitingReplies = 0;
    TIntrusivePtr<TKqpCounters> Counters;
};

} // namespace

std::pair<NYql::NDq::IDqComputeActorAsyncInput*, NActors::IActor*> CreateSequencerActor(ui64 inputIndex,
    NYql::NDq::TCollectStatsLevel statsLevel, const NUdf::TUnboxedValue& input, const NActors::TActorId& computeActorId,
    const NMiniKQL::TTypeEnvironment& typeEnv, const NMiniKQL::THolderFactory& holderFactory,
    std::shared_ptr<NMiniKQL::TScopedAlloc>& alloc, NKikimrKqp::TKqpSequencerSettings&& settings,
    TIntrusivePtr<TKqpCounters> counters) {
    auto actor = new TKqpSequencerActor(inputIndex, statsLevel, input, computeActorId, typeEnv, holderFactory, alloc,
        std::move(settings), counters);
    return {actor, actor};
}

} // namespace NKqp
} // namespace NKikimr
