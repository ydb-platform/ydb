#include "mkql_engine_flat_host.h"
#include "mkql_engine_flat_impl.h"
#include "mkql_keys.h"
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders_codegen.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_pack.h>
#include <ydb/library/yql/minikql/computation/mkql_custom_list.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>

#include <util/generic/cast.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

    TCell ExtractCell(TRuntimeNode value, const TTypeEnvironment& env) {
        TRuntimeNode data = value;
        if (value.GetStaticType()->IsOptional()) {
            auto opt = AS_VALUE(TOptionalLiteral, value);
            if (!opt->HasItem()) {
                return TCell();
            }

            data = opt->GetItem();
        }

        const auto literal = AS_VALUE(TDataLiteral, data);
        // TODO: support pg types
        auto typeInfo = NScheme::TTypeInfo(literal->GetType()->GetSchemeType());
        return MakeCell(typeInfo, literal->AsValue(), env, false);
    }

    void ExtractRow(TVector<TCell>& row, TTupleLiteral* tupleNode, const TTypeEnvironment& env) {
        row.resize(tupleNode->GetValuesCount());
        for (ui32 i = 0; i < tupleNode->GetValuesCount(); ++i) {
            auto value = tupleNode->GetValue(i);
            row[i] = ExtractCell(value, env);
        }
    }

    TCell ExtractCell(const NUdf::TUnboxedValue& value, TType* type, const TTypeEnvironment& env) {
        if (type->IsOptional() && !value) {
            return TCell();
        }
        NScheme::TTypeInfo typeInfo = NScheme::TypeInfoFromMiniKQLType(type);
        return MakeCell(typeInfo, value, env);
    }

    void ExtractRow(const NUdf::TUnboxedValue& row, TTupleType* rowType, TVector<TCell>& cells, const TTypeEnvironment& env) {
        cells.resize(rowType->GetElementsCount());
        for (ui32 i = 0; i < rowType->GetElementsCount(); ++i) {
            cells[i] = ExtractCell(row.GetElement(i), rowType->GetElementType(i), env);
        }
    }

    class TAbortHolder : public TComputationValue<TAbortHolder> {
    public:
        TAbortHolder(TMemoryUsageInfo* memInfo)
            : TComputationValue(memInfo)
        {
        }
    private:
        void Apply(NUdf::IApplyContext& applyContext) const override {
            auto& engineCtx = *CheckedCast<TEngineFlatApplyContext*>(&applyContext);
            engineCtx.IsAborted = true;
        }
    };

    class TEmptyRangeHolder : public TComputationValue<TEmptyRangeHolder> {
    public:
        TEmptyRangeHolder(const THolderFactory& holderFactory)
            : TComputationValue(&holderFactory.GetMemInfo())
            , EmptyContainer(holderFactory.GetEmptyContainerLazy())
        {
        }
    private:
        NUdf::TUnboxedValue GetElement(ui32 index) const override {
            switch (index) {
                case 0: return EmptyContainer;
                case 1: return NUdf::TUnboxedValuePod(false);
                case 2: return NUdf::TUnboxedValuePod::Zero();
                case 3: return NUdf::TUnboxedValuePod((ui64)0);
            }

            MKQL_ENSURE(false, "Wrong index: " << index);
        }

        const NUdf::TUnboxedValue EmptyContainer;
    };

    class TResultWrapper : public TMutableComputationNode<TResultWrapper> {
        typedef TMutableComputationNode<TResultWrapper> TBaseComputation;
    public:
        class TResult : public TComputationValue<TResult> {
        public:
            TResult(
                TMemoryUsageInfo* memInfo,
                const NUdf::TUnboxedValue& payload,
                const TStringBuf& label)
                : TComputationValue(memInfo)
                , Payloads(1, payload)
                , Labels(1, label)
            {
            }

            TResult(
                TMemoryUsageInfo* memInfo,
                const TVector<NUdf::TUnboxedValue>& payloads,
                const TVector<TStringBuf>& labels)
                : TComputationValue(memInfo)
                , Payloads(payloads)
                , Labels(labels)
            {
                Y_ABORT_UNLESS(payloads.size() == labels.size());
            }

        private:
            void Apply(NUdf::IApplyContext& applyContext) const override {
                auto& engineCtx = *CheckedCast<TEngineFlatApplyContext*>(&applyContext);
                for (ui32 i = 0; i < Payloads.size(); ++i) {
                    (*engineCtx.ResultValues)[Labels[i]] = Payloads[i];
                }
            }

            TVector<NUdf::TUnboxedValue> Payloads;
            TVector<TStringBuf> Labels;
        };

        TResultWrapper(
            TComputationMutables& mutables,
            const TStringBuf& label,
            IComputationNode* payload)
            :
            TBaseComputation(mutables),
            Label(label),
            Payload(payload)
        {
        }

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
            return ctx.HolderFactory.Create<TResult>(Payload->GetValue(ctx), Label);
        }

    private:
        void RegisterDependencies() const final {
            DependsOn(Payload);
        }

        TStringBuf Label;
        IComputationNode* const Payload;
    };

    class TAcquireLocksWrapper : public TMutableComputationNode<TAcquireLocksWrapper> {
        typedef TMutableComputationNode<TAcquireLocksWrapper> TBaseComputation;
    public:
        TAcquireLocksWrapper(TComputationMutables& mutables, NUdf::TUnboxedValue locks, NUdf::TUnboxedValue locks2)
            : TBaseComputation(mutables)
            , Labels({TxLocksResultLabel, TxLocksResultLabel2})
        {
            Locks.reserve(2);
            Locks.push_back(locks);
            Locks.push_back(locks2);
        }

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
            return ctx.HolderFactory.Create<TResultWrapper::TResult>(Locks, Labels);
        }

    private:
        void RegisterDependencies() const final {}

        TVector<TStringBuf> Labels;
        TVector<NUdf::TUnboxedValue> Locks;
    };

    class TDiagnosticsWrapper : public TMutableComputationNode<TDiagnosticsWrapper> {
        typedef TMutableComputationNode<TDiagnosticsWrapper> TBaseComputation;
    public:
        TDiagnosticsWrapper(TComputationMutables& mutables, NUdf::TUnboxedValue&& diags)
            : TBaseComputation(mutables)
            , Diagnostics(std::move(diags))
        {}

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
            return ctx.HolderFactory.Create<TResultWrapper::TResult>(Diagnostics, TxInfoResultLabel);
        }

    private:
        void RegisterDependencies() const final {}

        const NUdf::TUnboxedValue Diagnostics;
    };

    IComputationNode* WrapAsVoid(const TComputationNodeFactoryContext& ctx) {
        return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValue::Void());
    }

    class TDummyWrapper : public TMutableComputationNode<TDummyWrapper> {
        typedef TMutableComputationNode<TDummyWrapper> TBaseComputation;
    public:
        TDummyWrapper(TComputationMutables& mutables)
            : TBaseComputation(mutables)
        {}

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext&) const {
            Y_ABORT("Failed to build value for dummy node");
        }
    private:
        void RegisterDependencies() const final {}
    };

    class TEraseRowWrapper : public TMutableComputationNode<TEraseRowWrapper> {
        typedef TMutableComputationNode<TEraseRowWrapper> TBaseComputation;
    public:
        friend class TResult;

        class TResult : public TComputationValue<TResult> {
        public:
            TResult(TMemoryUsageInfo* memInfo, const TEraseRowWrapper* owner, const NUdf::TUnboxedValue& row)
                : TComputationValue(memInfo)
                , Owner(owner)
                , Row(row)
            {
            }

        private:
            void Apply(NUdf::IApplyContext& applyContext) const override {
                auto& engineCtx = *CheckedCast<TEngineFlatApplyContext*>(&applyContext);
                TVector<TCell> row;
                ExtractRow(Row, Owner->RowType, row, *engineCtx.Env);

                if (!engineCtx.Host->IsPathErased(Owner->TableId) && engineCtx.Host->IsMyKey(Owner->TableId, row)) {
                    engineCtx.Host->EraseRow(Owner->TableId, row);
                }
            }

            const TEraseRowWrapper *const Owner;
            const NUdf::TUnboxedValue Row;
        };

        TEraseRowWrapper(TComputationMutables& mutables, const TTableId& tableId, TTupleType* rowType, IComputationNode* row)
            : TBaseComputation(mutables)
            , TableId(tableId)
            , RowType(rowType)
            , Row(row)
        {
        }

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
            return ctx.HolderFactory.Create<TResult>(this, Row->GetValue(ctx));
        }

    private:
        void RegisterDependencies() const final {
            DependsOn(Row);
        }

        const TTableId TableId;
        TTupleType* const RowType;
        IComputationNode* const Row;
    };

    class TUpdateRowWrapper : public TMutableComputationNode<TUpdateRowWrapper> {
        typedef TMutableComputationNode<TUpdateRowWrapper> TBaseComputation;
    public:
        friend class TResult;

        class TResult : public TComputationValue<TResult> {
        public:
            TResult(TMemoryUsageInfo* memInfo, const TUpdateRowWrapper* owner,
                NUdf::TUnboxedValue&& row,
                NUdf::TUnboxedValue&& update)
                : TComputationValue(memInfo)
                , Owner(owner)
                , Row(std::move(row))
                , Update(std::move(update))
            {
            }

        private:
            void Apply(NUdf::IApplyContext& applyContext) const override {
                auto& engineCtx = *CheckedCast<TEngineFlatApplyContext*>(&applyContext);
                TVector<TCell> row;
                ExtractRow(Row, Owner->RowType, row, *engineCtx.Env);

                if (!engineCtx.Host->IsPathErased(Owner->TableId) && engineCtx.Host->IsMyKey(Owner->TableId, row)) {
                    auto updateStruct = Owner->UpdateStruct;
                    TVector<IEngineFlatHost::TUpdateCommand> commands(updateStruct->GetValuesCount());
                    for (ui32 i = 0; i < updateStruct->GetValuesCount(); ++i) {
                        auto& cmd = commands[i];
                        auto columnIdBuf = updateStruct->GetType()->GetMemberName(i);
                        cmd.Column = FromString<ui32>(columnIdBuf);
                        auto type = updateStruct->GetType()->GetMemberType(i);
                        if (type->IsVoid()) {
                            // erase
                            cmd.Operation = TKeyDesc::EColumnOperation::Set;
                        }
                        else if (!type->IsTuple()) {
                            // write
                            cmd.Operation = TKeyDesc::EColumnOperation::Set;
                            cmd.Value = ExtractCell(Update.GetElement(i), type, *engineCtx.Env);
                        } else {
                            // inplace update
                            cmd.Operation = TKeyDesc::EColumnOperation::InplaceUpdate;
                            auto item = Update.GetElement(i);
                            auto mode = item.GetElement(0);
                            auto modeBuf = mode.AsStringRef();
                            cmd.InplaceUpdateMode = (EInplaceUpdateMode)*(const ui8*)modeBuf.Data();
                            cmd.Value = ExtractCell(item.GetElement(1),
                                AS_TYPE(TTupleType, type)->GetElementType(1), *engineCtx.Env);
                        }
                    }

                    engineCtx.Host->UpdateRow(Owner->TableId, row, commands);
                }
            }

            const TUpdateRowWrapper *const Owner;
            const NUdf::TUnboxedValue Row;
            const NUdf::TUnboxedValue Update;
        };

        TUpdateRowWrapper(TComputationMutables& mutables, const TTableId& tableId, TTupleType* rowType, IComputationNode* row,
            TStructLiteral* updateStruct, IComputationNode* update)
            : TBaseComputation(mutables)
            , TableId(tableId)
            , RowType(rowType)
            , Row(row)
            , UpdateStruct(updateStruct)
            , Update(update)
        {
        }

        NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
            return ctx.HolderFactory.Create<TResult>(this, Row->GetValue(ctx), Update->GetValue(ctx));
        }

    private:
        void RegisterDependencies() const final {
            DependsOn(Row);
            DependsOn(Update);
        }

        const TTableId TableId;
        TTupleType* const RowType;
        IComputationNode* const Row;
        TStructLiteral* const UpdateStruct;
        IComputationNode* const Update;
    };

    IComputationNode* WrapAsDummy(TComputationMutables& mutables) {
        return new TDummyWrapper(mutables);
    }

    IComputationNode* WrapResult(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
        MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 args");

        const auto& labelInput = callable.GetInput(0);
        MKQL_ENSURE(labelInput.IsImmediate() && labelInput.GetNode()->GetType()->IsData(), "Expected immediate data");

        const auto& labelData = static_cast<const TDataLiteral&>(*labelInput.GetNode());
        MKQL_ENSURE(labelData.GetType()->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected string");

        TStringBuf label = labelData.AsValue().AsStringRef();

        auto payloadNode = LocateNode(ctx.NodeLocator, callable, 1);
        return new TResultWrapper(ctx.Mutables, label, payloadNode);
    }

    IComputationNode* WrapAcquireLocks(TCallable& callable, const TComputationNodeFactoryContext& ctx,
        const TVector<IEngineFlat::TTxLock>& txLocks)
    {
        MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");

        const auto& lockTxIdInput = callable.GetInput(0);
        MKQL_ENSURE(lockTxIdInput.IsImmediate() && lockTxIdInput.GetNode()->GetType()->IsData(), "Expected immediate data");

        const auto& lockTxIdData = static_cast<const TDataLiteral&>(*lockTxIdInput.GetNode());
        MKQL_ENSURE(lockTxIdData.GetType()->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected Uint64");

        auto structType = GetTxLockType(ctx.Env, false);

        NUdf::TUnboxedValue *listItems = nullptr;
        auto locksList = ctx.HolderFactory.CreateDirectArrayHolder(txLocks.size(), listItems);
        for (auto& txLock : txLocks) {
            NUdf::TUnboxedValue *items = nullptr;
            *listItems++ = ctx.HolderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), items);
            items[structType->GetMemberIndex("Counter")] = NUdf::TUnboxedValuePod(txLock.Counter);
            items[structType->GetMemberIndex("DataShard")] = NUdf::TUnboxedValuePod(txLock.DataShard);
            items[structType->GetMemberIndex("Generation")] = NUdf::TUnboxedValuePod(txLock.Generation);
            items[structType->GetMemberIndex("LockId")] = NUdf::TUnboxedValuePod(txLock.LockId);
        }

        auto structType2 = GetTxLockType(ctx.Env, true);

        NUdf::TUnboxedValue *listItems2 = nullptr;
        auto locksList2 = ctx.HolderFactory.CreateDirectArrayHolder(txLocks.size(), listItems2);
        for (auto& txLock : txLocks) {
            NUdf::TUnboxedValue *items = nullptr;
            *listItems2++ = ctx.HolderFactory.CreateDirectArrayHolder(structType2->GetMembersCount(), items);
            items[structType2->GetMemberIndex("Counter")] = NUdf::TUnboxedValuePod(txLock.Counter);
            items[structType2->GetMemberIndex("DataShard")] = NUdf::TUnboxedValuePod(txLock.DataShard);
            items[structType2->GetMemberIndex("Generation")] = NUdf::TUnboxedValuePod(txLock.Generation);
            items[structType2->GetMemberIndex("LockId")] = NUdf::TUnboxedValuePod(txLock.LockId);
            items[structType2->GetMemberIndex("PathId")] = NUdf::TUnboxedValuePod(txLock.PathId);
            items[structType2->GetMemberIndex("SchemeShard")] = NUdf::TUnboxedValuePod(txLock.SchemeShard);
        }

        return new TAcquireLocksWrapper(ctx.Mutables, std::move(locksList), std::move(locksList2));
    }

    IComputationNode* WrapDiagnostics(TCallable& callable, const TComputationNodeFactoryContext& ctx,
        const TVector<IEngineFlat::TTabletInfo>& tabletInfos)
    {
        MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected zero args");

        auto structType = GetDiagnosticsType(ctx.Env);

        NUdf::TUnboxedValue *listItems = nullptr;
        auto diagList = ctx.HolderFactory.CreateDirectArrayHolder(tabletInfos.size(), listItems);
        for (auto& info : tabletInfos) {
            NUdf::TUnboxedValue *items = nullptr;
            *listItems++ = ctx.HolderFactory.CreateDirectArrayHolder(structType->GetMembersCount(), items);

            items[structType->GetMemberIndex("ActorIdRawX1")] = NUdf::TUnboxedValuePod(info.ActorId.first);
            items[structType->GetMemberIndex("ActorIdRawX2")] = NUdf::TUnboxedValuePod(info.ActorId.second);
            items[structType->GetMemberIndex("TabletId")] = NUdf::TUnboxedValuePod(info.TabletId);
            items[structType->GetMemberIndex("Generation")] = NUdf::TUnboxedValuePod(info.TabletGenStep.first);
            items[structType->GetMemberIndex("GenStep")] = NUdf::TUnboxedValuePod(info.TabletGenStep.second);
            items[structType->GetMemberIndex("IsFollower")] = NUdf::TUnboxedValuePod(info.IsFollower);

            items[structType->GetMemberIndex("TxStep")] = NUdf::TUnboxedValuePod(info.TxInfo.StepTxId.first);
            items[structType->GetMemberIndex("TxId")] = NUdf::TUnboxedValuePod(info.TxInfo.StepTxId.second);
            items[structType->GetMemberIndex("Status")] = NUdf::TUnboxedValuePod(info.TxInfo.Status);
            items[structType->GetMemberIndex("PrepareArriveTime")] =
                NUdf::TUnboxedValuePod(info.TxInfo.PrepareArriveTime.MilliSeconds());
            items[structType->GetMemberIndex("ProposeLatency")] =
                NUdf::TUnboxedValuePod(info.TxInfo.ProposeLatency.MilliSeconds());
            items[structType->GetMemberIndex("ExecLatency")] =
                NUdf::TUnboxedValuePod(info.TxInfo.ExecLatency.MilliSeconds());
        }

        return new TDiagnosticsWrapper(ctx.Mutables, std::move(diagList));
    }

    IComputationNode* WrapAbort(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
        MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected zero args");
        return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod(new TAbortHolder(&ctx.HolderFactory.GetMemInfo())));
    }

    IComputationNode* WrapStepTxId(TCallable& callable, const TComputationNodeFactoryContext& ctx,
        const std::pair<ui64, ui64>& stepTxId) {
        MKQL_ENSURE(callable.GetInputsCount() == 0, "Expected zero args");

        auto ui64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, ctx.Env);
        TVector<TType*> tupleTypes(2, ui64Type);
        auto tupleType = TTupleType::Create(tupleTypes.size(), tupleTypes.data(), ctx.Env);

        NUdf::TUnboxedValue* items = nullptr;
        auto tuple = ctx.HolderFactory.CreateDirectArrayHolder(tupleType->GetElementsCount(), items);
        items[0] = NUdf::TUnboxedValuePod(stepTxId.first);
        items[1] = NUdf::TUnboxedValuePod(stepTxId.second);

        return ctx.NodeFactory.CreateImmutableNode(std::move(tuple));
    }

    IComputationNode* WrapConcatenatedResults(TCallable& callable, TIncomingResults::const_iterator resultIt,
        const TComputationNodeFactoryContext& ctx)
    {
        auto returnType = callable.GetType()->GetReturnType();
        MKQL_ENSURE(returnType->IsList(), "Expected list type");

        TUnboxedValueVector lists;
        TValuePacker listPacker(false, returnType);
        for (const auto& result : resultIt->second) {
            lists.emplace_back(listPacker.Unpack(result, ctx.HolderFactory));
        }

        return ctx.NodeFactory.CreateImmutableNode(ctx.HolderFactory.Create<TExtendListValue>(std::move(lists)));
    }

    IComputationNode* WrapMergedSelectRow(TCallable& callable, TIncomingResults& results,
        const THashSet<ui32>& localReadCallables, IEngineFlatHost* host, const TComputationNodeFactoryContext& ctx)
    {
        TUnboxedValueVector values;
        if (localReadCallables.contains(callable.GetUniqueId())) {
            values.push_back(PerformLocalSelectRow(callable, *host, ctx.HolderFactory, ctx.Env));
        }

        auto returnType = callable.GetType()->GetReturnType();
        MKQL_ENSURE(returnType->IsOptional(), "Expected optional type");
        TValuePacker valuePacker(false, returnType);

        auto resultIt = results.find(callable.GetUniqueId());
        if (resultIt != results.end()) {
            for (auto& result : resultIt->second) {
                values.push_back(valuePacker.Unpack(result, ctx.HolderFactory));
            }
        }

        if (values.empty()) {
            return ctx.NodeFactory.CreateOptionalNode(nullptr);
        }

        auto choosenValue = std::move(values.front());
        for (size_t i = 1; i < values.size(); ++i) {
            if (auto& value = values[i]) {
                MKQL_ENSURE(!choosenValue, "Multiple non-empty results for SelectRow");
                choosenValue = std::move(value);
            }
        }

        return ctx.NodeFactory.CreateImmutableNode(std::move(choosenValue));
    }

    class TPartialList : public TCustomListValue {
    public:
        class TIterator : public TComputationValue<TIterator> {
        public:
            TIterator(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& listIterator, ui64 itemsLimit)
                : TComputationValue(memInfo)
                , ListIterator(std::move(listIterator))
                , ItemsLimit(itemsLimit)
                , ItemsCount(0) {}

        private:
            bool Next(NUdf::TUnboxedValue& value) override {
                if (ItemsCount >= ItemsLimit) {
                    return false;
                }

                MKQL_ENSURE(ListIterator.Next(value), "Unexpected end of list");
                ++ItemsCount;
                return true;
            }

            bool Skip() override {
                if (ItemsCount >= ItemsLimit) {
                    return false;
                }

                MKQL_ENSURE(ListIterator.Skip(), "Unexpected end of list");
                ++ItemsCount;
                return true;
            }

            const NUdf::TUnboxedValue ListIterator;
            ui64 ItemsLimit;
            ui64 ItemsCount;
        };

    public:
        TPartialList(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& list, ui64 itemsLimit)
            : TCustomListValue(memInfo)
            , List(std::move(list))
            , ItemsLimit(itemsLimit)
        {
            Length = itemsLimit;
            HasItems = itemsLimit > 0;
        }

    private:
        NUdf::TUnboxedValue GetListIterator() const override {
            return NUdf::TUnboxedValuePod(new TIterator(GetMemInfo(), List.GetListIterator(), ItemsLimit));
        }

        NUdf::TUnboxedValue List;
        ui64 ItemsLimit;
    };

    IComputationNode* WrapMergedSelectRange(TCallable& callable, TIncomingResults& results,
        const THashSet<ui32>& localReadCallables, IEngineFlatHost* host, const TComputationNodeFactoryContext& ctx,
        const TFlatEngineStrings& strings)
    {
        TUnboxedValueVector values;
        if (localReadCallables.contains(callable.GetUniqueId())) {
            values.emplace_back(PerformLocalSelectRange(callable, *host, ctx.HolderFactory, ctx.Env));
        }

        auto resultIt = results.find(callable.GetUniqueId());
        if (resultIt != results.end()) {
            auto returnType = GetActualReturnType(callable, ctx.Env, strings);
            MKQL_ENSURE(returnType->IsStruct(), "Expected struct type");
            TValuePacker valuePacker(false, returnType);

            for (auto& result : resultIt->second) {
                values.emplace_back(valuePacker.Unpack(result, ctx.HolderFactory));
            }
        }

        if (values.empty()) {
            return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod(new TEmptyRangeHolder(ctx.HolderFactory)));
        }

        if (values.size() == 1) {
            return ctx.NodeFactory.CreateImmutableNode(std::move(values.front()));
        }

        bool truncatedAny = false;
        ui32 keyColumnsCount = 0;
        TVector<NScheme::TTypeInfo> types;

        using TPartKey = std::tuple<const TCell*, NUdf::TUnboxedValue, ui64, ui64, NUdf::TUnboxedValue, bool>;

        std::vector<TPartKey> parts;
        std::vector<TSerializedCellVec> dataBuffers;
        parts.reserve(values.size());
        dataBuffers.reserve(values.size());

        for (auto& value : values) {
            MKQL_ENSURE(value.IsBoxed(), "Expected boxed value");

            auto list = value.GetElement(0);
            MKQL_ENSURE(list.IsBoxed(), "Expected boxed value");

            bool truncated = value.GetElement(1).Get<bool>();
            auto firstKeyValue = value.GetElement(2);
            ui64 sizeInBytes = value.GetElement(3).Get<ui64>();

            TString firstKey(firstKeyValue.AsStringRef());
            truncatedAny = truncatedAny || truncated;

            ui64 itemsCount = list.GetListLength();
            if (itemsCount == 0) {
                continue;
            }

            MKQL_ENSURE(firstKey.size() >= sizeof(ui32), "Corrupted key");
            ui32 partKeyColumnsCount = ReadUnaligned<ui32>(firstKey.data());
            ui32 typesSize = sizeof(ui32) + partKeyColumnsCount * sizeof(NUdf::TDataTypeId);
            MKQL_ENSURE(firstKey.size() >= typesSize, "Corrupted key");
            const char* partTypes = firstKey.data() + sizeof(ui32);

            if (!keyColumnsCount) {
                keyColumnsCount = partKeyColumnsCount;
                for (ui32 i = 0; i < partKeyColumnsCount; ++i) {
                    auto partType = ReadUnaligned<NUdf::TDataTypeId>(partTypes + i * sizeof(NUdf::TDataTypeId));
                    // TODO: support pg types
                    types.push_back(NScheme::TTypeInfo(partType));
                }
            } else {
                MKQL_ENSURE(keyColumnsCount == partKeyColumnsCount, "Mismatch of key columns count");
                for (ui32 i = 0; i < keyColumnsCount; ++i) {
                    auto partType = ReadUnaligned<NUdf::TDataTypeId>(partTypes + i * sizeof(NUdf::TDataTypeId));
                    // TODO: support pg types
                    MKQL_ENSURE(partType == types[i].GetTypeId(), "Mismatch of key columns type");
                }
            }

            dataBuffers.emplace_back(firstKey.substr(typesSize, firstKey.size() - typesSize));
            parts.emplace_back(nullptr, std::move(list), sizeInBytes, itemsCount, std::move(firstKeyValue), truncated);
        }

        for (ui32 i = 0; i < dataBuffers.size(); ++i) {
            std::get<0>(parts[i]) = dataBuffers[i].GetCells().data();
        }

        bool reverse = false;
        if (callable.GetInputsCount() >= 11) {
            reverse = AS_VALUE(TDataLiteral, callable.GetInput(10))->AsValue().Get<bool>();
        }

        Sort(parts, [&](const TPartKey& lhs, const TPartKey& rhs) {
            if (reverse) {
                return CompareTypedCellVectors(std::get<0>(rhs), std::get<0>(lhs), types.data(), keyColumnsCount) < 0;
            } else {
                return CompareTypedCellVectors(std::get<0>(lhs), std::get<0>(rhs), types.data(), keyColumnsCount) < 0;
            }
        });

        ui64 itemsLimit = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui64>();
        ui64 bytesLimit = AS_VALUE(TDataLiteral, callable.GetInput(7))->AsValue().Get<ui64>();

        TUnboxedValueVector resultLists;
        bool resultTruncated = false;
        ui64 totalSize = 0;
        ui64 totalItems = 0;
        for (auto& part : parts) {
            ui64 size = std::get<2>(part);
            bool truncated = std::get<5>(part);

            auto list = std::get<1>(part);
            ui64 items = std::get<3>(part);
            if (itemsLimit && itemsLimit < (items + totalItems)) {
                resultTruncated = true;
                ui64 itemsToFetch = itemsLimit - totalItems;
                if (itemsToFetch > 0) {
                    NUdf::TUnboxedValuePod listPart(new TPartialList(&ctx.HolderFactory.GetMemInfo(), std::move(list), itemsToFetch));
                    resultLists.emplace_back(std::move(listPart));
                }

                break;
            }

            resultLists.emplace_back(std::move(list));
            totalSize += size;
            totalItems += items;

            if (bytesLimit && bytesLimit < totalSize) {
                resultTruncated = true;
                break;
            }

            if (truncated) {
                if (!bytesLimit) {
                    if (itemsLimit) {
                        MKQL_ENSURE(totalItems == itemsLimit, "SelectRange merge: not enough items in truncated part");
                    } else {
                        MKQL_ENSURE(false, "SelectRange merge: unexpected truncated part without specified limits");
                    }
                }

                resultTruncated = true;
                break;
            }
        }

        if (truncatedAny) {
            MKQL_ENSURE(resultTruncated, "SelectRange merge: result not truncated while truncated parts are present");
        }

        if (resultLists.empty()) {
            return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod(new TEmptyRangeHolder(ctx.HolderFactory)));
        }

        auto resultList = ctx.HolderFactory.Create<TExtendListValue>(std::move(resultLists));

        NUdf::TUnboxedValue* resultItems = nullptr;
        auto result = ctx.HolderFactory.CreateDirectArrayHolder(4, resultItems);
        resultItems[0] = std::move(resultList);
        resultItems[1] = NUdf::TUnboxedValuePod(resultTruncated);
        resultItems[2] = std::move(std::get<4>(parts.front()));
        resultItems[3] = NUdf::TUnboxedValuePod(totalSize);

        return ctx.NodeFactory.CreateImmutableNode(std::move(result));
    }

    IComputationNode* WrapEraseRow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
        MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arg");

        auto tableNode = callable.GetInput(0);
        const auto tableId = ExtractTableId(tableNode);
        auto tupleType = AS_TYPE(TTupleType, callable.GetInput(1));

        return new TEraseRowWrapper(ctx.Mutables, tableId, tupleType,
            LocateNode(ctx.NodeLocator, callable, 1));
    }

    IComputationNode* WrapUpdateRow(TCallable& callable, const TComputationNodeFactoryContext& ctx, IEngineFlatHost* host) {
        MKQL_ENSURE(callable.GetInputsCount() == 3, "Expected 3 arg");

        auto tableNode = callable.GetInput(0);
        const auto tableId = ExtractTableId(tableNode);

        auto tableInfo = host->GetTableInfo(tableId);
        MKQL_ENSURE(tableInfo, "Table not found: " << tableId.PathId.ToString());

        auto tupleType = AS_TYPE(TTupleType, callable.GetInput(1));
        for (ui32 i = 0; i < tupleType->GetElementsCount(); ++i) {
            auto columnId = tableInfo->KeyColumns[i];
            const auto& column = tableInfo->Columns.at(columnId);
            if (column.NotNull) {
                MKQL_ENSURE(!tupleType->GetElementType(i)->IsOptional(),
                    "Not null column " << column.Name << " can't be optional");
            }
        }

        auto structUpdate = AS_VALUE(TStructLiteral, callable.GetInput(2));
        for (ui32 i = 0; i < structUpdate->GetValuesCount(); ++i) {
            auto columnId = FromString<ui32>(structUpdate->GetType()->GetMemberName(i));
            const auto& column = tableInfo->Columns.at(columnId);
            auto type = structUpdate->GetType()->GetMemberType(i);

            if (type->IsTuple()) {
                auto itemType = AS_TYPE(TTupleType, type);
                if (column.NotNull) {
                    MKQL_ENSURE(!itemType->GetElementType(1)->IsOptional(),
                        "Not null column " << column.Name << " can't be optional");
                }
            } else if (!type->IsVoid()) {
                if (column.NotNull) {
                    MKQL_ENSURE(!type->IsOptional(),
                        "Not null column " << column.Name << " can't be optional");
                }
            }
        }

        return new TUpdateRowWrapper(ctx.Mutables, tableId, tupleType,
            LocateNode(ctx.NodeLocator, callable, 1), structUpdate, LocateNode(ctx.NodeLocator, callable, 2));
    }

    IComputationNode* WrapMergedTakeResults(TCallable& callable, TIncomingResults::const_iterator resultIt,
        const TComputationNodeFactoryContext& ctx)
    {
        auto returnType = callable.GetType()->GetReturnType();
        MKQL_ENSURE(returnType->IsList(), "Expected list type");

        TUnboxedValueVector lists;
        TValuePacker listPacker(false, returnType);
        for (const auto& result : resultIt->second) {
            lists.emplace_back(listPacker.Unpack(result, ctx.HolderFactory));
        }

        auto countNode = callable.GetInput(1);
        MKQL_ENSURE(countNode.IsImmediate() && countNode.GetStaticType()->IsData(), "Expected immediate data");
        const auto& countData = static_cast<const TDataLiteral&>(*countNode.GetNode());
        MKQL_ENSURE(countData.GetType()->GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");
        auto takeCount = countData.AsValue().Get<ui64>();

        return ctx.NodeFactory.CreateImmutableNode(ctx.Builder->TakeList(ctx.HolderFactory.Create<TExtendListValue>(std::move(lists)), takeCount));
    }

    IComputationNode* WrapMergedLength(TCallable& callable, TIncomingResults::const_iterator resultIt,
        const TComputationNodeFactoryContext& ctx)
    {
        auto returnType = callable.GetType()->GetReturnType();
        MKQL_ENSURE(returnType->IsData(), "Expected list type");
        const auto& returnDataType = static_cast<const TDataType&>(*returnType);
        MKQL_ENSURE(returnDataType.GetSchemeType() == NUdf::TDataType<ui64>::Id, "Expected ui64");

        ui64 totalLength = 0;
        TValuePacker listPacker(false, returnType);
        for (auto& result : resultIt->second) {
            const auto value = listPacker.Unpack(result, ctx.HolderFactory);
            totalLength += value.Get<ui64>();
        }

        return ctx.NodeFactory.CreateImmutableNode(NUdf::TUnboxedValuePod(totalLength));
    }
}

TComputationNodeFactory GetFlatShardExecutionFactory(TShardExecData& execData, bool validateOnly) {
    auto builtins = GetBuiltinFactory();
    return [builtins, &execData, validateOnly]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        const TEngineFlatSettings& settings = execData.Settings;
        const TFlatEngineStrings& strings = execData.Strings;

        auto res = builtins(callable, ctx);
        if (res)
            return res;

        auto nameStr = callable.GetType()->GetNameStr();
        if (nameStr == strings.Abort)
            return WrapAsVoid(ctx);

        if (nameStr == strings.StepTxId)
            return WrapStepTxId(callable, ctx, execData.StepTxId);

        if (nameStr == strings.SetResult)
            return WrapAsVoid(ctx);

        if (nameStr == strings.SelectRow) {
            if (validateOnly) {
                return WrapAsDummy(ctx.Mutables);
            }
            else {
                return WrapMergedSelectRow(
                    callable, execData.Results, execData.LocalReadCallables, settings.Host, ctx);
            }
        }

        if (nameStr == strings.SelectRange) {
            if (validateOnly) {
                return WrapAsDummy(ctx.Mutables);
            }
            else {
                return WrapMergedSelectRange(
                    callable, execData.Results, execData.LocalReadCallables, settings.Host, ctx, strings);
            }
        }

        if (nameStr == strings.EraseRow) {
            return WrapEraseRow(callable, ctx);
        }

        if (nameStr == strings.UpdateRow) {
            return WrapUpdateRow(callable, ctx, settings.Host);
        }

        if (nameStr == strings.AcquireLocks) {
            return WrapAsVoid(ctx);
        }

        if (nameStr == strings.Diagnostics) {
            return WrapAsVoid(ctx);
        }

        return nullptr;
    };
}

TComputationNodeFactory GetFlatProxyExecutionFactory(TProxyExecData& execData)
{
    auto builtins = GetBuiltinFactory();
    return [builtins, &execData]
        (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode*
    {
        const TFlatEngineStrings& strings = execData.Strings;
        const TEngineFlatSettings& settings = execData.Settings;
        TIncomingResults& results = execData.Results;

        auto nameStr = callable.GetType()->GetNameStr();

        if (nameStr == strings.Abort)
            return WrapAbort(callable, ctx);

        if (nameStr == strings.StepTxId)
            return WrapStepTxId(callable, ctx, execData.StepTxId);

        if (nameStr == strings.UpdateRow || nameStr == strings.EraseRow)
            return WrapAsVoid(ctx);

        if (nameStr == strings.SetResult)
            return WrapResult(callable, ctx);

        if (nameStr == strings.SelectRow)
            return WrapMergedSelectRow(callable, results, {}, settings.Host, ctx);

        if (nameStr == strings.SelectRange)
            return WrapMergedSelectRange(callable, results, {}, settings.Host, ctx, strings);

        if (nameStr == strings.AcquireLocks)
            return WrapAcquireLocks(callable, ctx, execData.TxLocks);

        if (nameStr == strings.Diagnostics)
            return WrapDiagnostics(callable, ctx, execData.TabletInfos);

        auto resultIt = results.find(callable.GetUniqueId());
        if (resultIt != results.end()) {
            // Callable results were computed on datashards, we need to merge them on the proxy in
            // a callable-specific way.
            if (nameStr == strings.Builtins.Filter ||
                nameStr == strings.Builtins.FilterNullMembers ||
                nameStr == strings.Builtins.SkipNullMembers ||
                nameStr == strings.Builtins.Map ||
                nameStr == strings.Builtins.FlatMap ||
                nameStr == strings.CombineByKeyMerge ||
                nameStr == strings.PartialTake)
            {
                return WrapConcatenatedResults(callable, resultIt, ctx);
            } else if (nameStr == strings.Builtins.Take) {
                return WrapMergedTakeResults(callable, resultIt, ctx);
            } else if (nameStr == strings.Builtins.Length) {
                return WrapMergedLength(callable, resultIt, ctx);
            } else {
                Y_ABORT("Don't know how to merge results for callable: %s", TString(nameStr.Str()).data());
            }
        }

        return builtins(callable, ctx);
    };
}

NUdf::TUnboxedValue PerformLocalSelectRow(TCallable& callable, IEngineFlatHost& engineHost,
    const THolderFactory& holderFactory, const TTypeEnvironment& env)
{
    MKQL_ENSURE(callable.GetInputsCount() == 5, "Expected 5 args");
    auto tableNode = callable.GetInput(0);
    const auto tableId = ExtractTableId(tableNode);
    auto tupleNode = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    TVector<TCell> row;
    ExtractRow(row, tupleNode, env);

    auto returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(callable.GetInput(1).GetNode()->GetType()->IsType(), "Expected type");

    return engineHost.SelectRow(tableId, row,
        AS_VALUE(TStructLiteral, callable.GetInput(2)), AS_TYPE(TOptionalType, returnType),
        ExtractFlatReadTarget(callable.GetInput(4)), holderFactory);
}

NUdf::TUnboxedValue PerformLocalSelectRange(TCallable& callable, IEngineFlatHost& engineHost,
    const THolderFactory& holderFactory, const TTypeEnvironment& env)
{
    MKQL_ENSURE(callable.GetInputsCount() >= 9 && callable.GetInputsCount() <= 13, "Expected 9 to 13 args");
    auto tableNode = callable.GetInput(0);
    const auto tableId = ExtractTableId(tableNode);
    ui32 flags = AS_VALUE(TDataLiteral, callable.GetInput(5))->AsValue().Get<ui32>();
    ui64 itemsLimit = AS_VALUE(TDataLiteral, callable.GetInput(6))->AsValue().Get<ui64>();
    ui64 bytesLimit = AS_VALUE(TDataLiteral, callable.GetInput(7))->AsValue().Get<ui64>();

    TVector<TCell> fromValues;
    TVector<TCell> toValues;
    ExtractRow(fromValues, AS_VALUE(TTupleLiteral, callable.GetInput(3)), env);
    ExtractRow(toValues, AS_VALUE(TTupleLiteral, callable.GetInput(4)), env);

    bool inclusiveFrom = !(flags & TReadRangeOptions::TFlags::ExcludeInitValue);
    bool inclusiveTo = !(flags & TReadRangeOptions::TFlags::ExcludeTermValue);
    TTableRange range(fromValues, inclusiveFrom, toValues, inclusiveTo);
    auto returnType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(callable.GetInput(1).GetNode()->GetType()->IsType(), "Expected type");

    TListLiteral* skipNullKeys = nullptr;
    if (callable.GetInputsCount() > 9) {
        skipNullKeys = AS_VALUE(TListLiteral, callable.GetInput(9));
    }

    bool reverse = false;
    if (callable.GetInputsCount() > 10) {
        reverse = AS_VALUE(TDataLiteral, callable.GetInput(10))->AsValue().Get<bool>();
    }

    std::pair<TListLiteral*, TListLiteral*> forbidNullArgs{nullptr, nullptr};
    if (callable.GetInputsCount() > 12) {
        forbidNullArgs.first = AS_VALUE(TListLiteral, callable.GetInput(11));
        forbidNullArgs.second = AS_VALUE(TListLiteral, callable.GetInput(12));
    }

    return engineHost.SelectRange(tableId, range,
        AS_VALUE(TStructLiteral, callable.GetInput(2)), skipNullKeys, AS_TYPE(TStructType, returnType),
        ExtractFlatReadTarget(callable.GetInput(8)), itemsLimit, bytesLimit, reverse, forbidNullArgs, holderFactory);
}

TStructType* GetTxLockType(const TTypeEnvironment& env, bool v2) {
    auto ui32Type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
    auto ui64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, env);

    if (v2) {
        TVector<std::pair<TString, TType*>> lockStructMembers = {
            std::make_pair("Counter", ui64Type),
            std::make_pair("DataShard", ui64Type),
            std::make_pair("Generation", ui32Type),
            std::make_pair("LockId", ui64Type),
            std::make_pair("PathId", ui64Type),
            std::make_pair("SchemeShard", ui64Type),
        };

        auto lockStructType = TStructType::Create(lockStructMembers.data(), lockStructMembers.size(), env);
        return lockStructType;
    }

    TVector<std::pair<TString, TType*>> lockStructMembers = {
        std::make_pair("Counter", ui64Type),
        std::make_pair("DataShard", ui64Type),
        std::make_pair("Generation", ui32Type),
        std::make_pair("LockId", ui64Type),
    };

    auto lockStructType = TStructType::Create(lockStructMembers.data(), lockStructMembers.size(), env);
    return lockStructType;
}

TStructType* GetDiagnosticsType(const TTypeEnvironment& env) {
    auto ui32Type = TDataType::Create(NUdf::TDataType<ui32>::Id, env);
    auto ui64Type = TDataType::Create(NUdf::TDataType<ui64>::Id, env);
    auto boolType = TDataType::Create(NUdf::TDataType<bool>::Id, env);

    TVector<std::pair<TString, TType*>> diagStructMembers = {
        std::make_pair("ActorIdRawX1", ui64Type),
        std::make_pair("ActorIdRawX2", ui64Type),
        std::make_pair("ExecLatency", ui64Type),
        std::make_pair("GenStep", ui64Type),
        std::make_pair("Generation", ui32Type),
        std::make_pair("IsFollower", boolType),
        std::make_pair("PrepareArriveTime", ui64Type),
        std::make_pair("ProposeLatency", ui64Type),
        std::make_pair("Status", ui32Type),
        std::make_pair("TabletId", ui64Type),
        std::make_pair("TxId", ui64Type),
        std::make_pair("TxStep", ui64Type),
    };

    return TStructType::Create(diagStructMembers.data(), diagStructMembers.size(), env);
}

TType* GetActualReturnType(const TCallable& callable, const TTypeEnvironment& env,
    const TFlatEngineStrings& strings)
{
    auto name = callable.GetType()->GetNameStr();
    auto returnType = callable.GetType()->GetReturnType();

    if (name == strings.SelectRange) {
        // SelectRange implementataion use extended return struct (with _FirstKey and _Size)
        // required for merging purposes.
        // TODO: Fix SelectRange type, pass this information ouside of the callable result or
        // recompute during merging phase.
        MKQL_ENSURE(returnType->GetKind() == TType::EKind::Struct,
            "Expected struct as SelectRange return type.");
        auto& structType = static_cast<TStructType&>(*returnType);
        if (structType.GetMembersCount() == 2) {
            TStructTypeBuilder extendedStructBuilder(env);
            extendedStructBuilder.Add(structType.GetMemberName(0), structType.GetMemberType(0));
            extendedStructBuilder.Add(structType.GetMemberName(1), structType.GetMemberType(1));
            extendedStructBuilder.Add("_FirstKey", TDataType::Create(NUdf::TDataType<char*>::Id, env));
            extendedStructBuilder.Add("_Size", TDataType::Create(NUdf::TDataType<ui64>::Id, env));

            return extendedStructBuilder.Build();
        }
    }

    return returnType;
}

}
}
