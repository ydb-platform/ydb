#include "mkql_timezone.h"
#include <yql/essentials/utils/runtime_dispatch.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/mkql_type_ops.h>

#include <util/string/cast.h>

#include <array>

namespace NKikimr::NMiniKQL {

namespace {

class TTimezoneIdWrapper: public TMutableComputationNode<TTimezoneIdWrapper> {
    using TBaseComputation = TMutableComputationNode<TTimezoneIdWrapper>;

public:
    TTimezoneIdWrapper(TComputationMutables& mutables, IComputationNode* value)
        : TBaseComputation(mutables)
        , Value_(value)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Value_->GetValue(ctx);
        if (!value) {
            return {};
        }

        auto id = FindTimezoneId(value.AsStringRef());
        if (!id) {
            return {};
        }

        return NUdf::TUnboxedValuePod(ui16(*id));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Value_);
    }

    IComputationNode* const Value_;
};

class TTimezoneNameWrapper: public TMutableComputationNode<TTimezoneNameWrapper> {
    using TBaseComputation = TMutableComputationNode<TTimezoneNameWrapper>;

public:
    TTimezoneNameWrapper(TComputationMutables& mutables, IComputationNode* value)
        : TBaseComputation(mutables)
        , Value_(value)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Value_->GetValue(ctx);
        if (!value) {
            return {};
        }

        auto name = FindTimezoneIANAName(value.Get<ui16>());
        if (!name) {
            return {};
        }

        return MakeString(*name);
    }

private:
    void RegisterDependencies() const final {
        DependsOn(Value_);
    }

    IComputationNode* const Value_;
};

template <bool IsOptional1, bool IsOptional2>
class TAddTimezoneWrapper: public TMutableCodegeneratorNode<TAddTimezoneWrapper<IsOptional1, IsOptional2>> {
    using TBaseComputation = TMutableCodegeneratorNode<TAddTimezoneWrapper<IsOptional1, IsOptional2>>;

public:
    TAddTimezoneWrapper(TComputationMutables& mutables, IComputationNode* value, IComputationNode* id)
        : TBaseComputation(mutables, EValueRepresentation::Embedded)
        , Datetime_(value)
        , Id_(id)
        , TimezonesCount_(InitTimezones())
        , BlackList_(GetTzBlackList())
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        auto value = Datetime_->GetValue(ctx);
        if (IsOptional1 && !value) {
            return {};
        }

        const auto zone = Id_->GetValue(ctx);
        if (IsOptional2 && !zone) {
            return {};
        }

        const auto id = zone.Get<ui16>();
        if (!IsValidTimezoneId(id)) {
            return {};
        }

        value.SetTimezoneId(id);
        return value.Release();
    }

#ifndef MKQL_DISABLE_CODEGEN
    Value* DoGenerateGetValue(const TCodegenContext& ctx, BasicBlock*& block) const override {
        auto& context = ctx.Codegen.GetContext();

        const auto setz = BasicBlock::Create(context, "setz", ctx.Func);
        const auto done = BasicBlock::Create(context, "done", ctx.Func);

        const auto value = GetNodeValue(Datetime_, ctx, block);
        const auto result = PHINode::Create(value->getType(), 2U + (IsOptional1 ? 1U : 0U), "result", done);

        if (IsOptional1) {
            result->addIncoming(value, block);
            const auto good = BasicBlock::Create(context, "good", ctx.Func);
            BranchInst::Create(done, good, IsEmpty(value, block, context), block);

            block = good;
        }

        const auto tz = GetNodeValue(Id_, ctx, block);
        const auto id = GetterFor<ui16>(tz, context, block);

        const auto big = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_UGE, id, ConstantInt::get(id->getType(), TimezonesCount_), "big", block);
        auto test = IsOptional2 ? BinaryOperator::CreateOr(IsEmpty(tz, block, context), big, "test", block) : static_cast<Value*>(big);

        for (const auto black : BlackList_) {
            const auto& str = ToString(black);
            const auto bad = CmpInst::Create(Instruction::ICmp, ICmpInst::ICMP_EQ, id, ConstantInt::get(id->getType(), black), ("bad_" + str).c_str(), block);
            test = BinaryOperator::CreateOr(test, bad, ("test_" + str).c_str(), block);
        }

        result->addIncoming(ConstantInt::get(value->getType(), 0), block);
        BranchInst::Create(done, setz, test, block);

        {
            block = setz;

            const std::array<uint64_t, 2> init = {~0ULL, ~0xFFFFULL};
            const auto mask = ConstantInt::get(value->getType(), APInt(128, init));
            const auto clean = BinaryOperator::CreateAnd(value, mask, "clean", block);
            const auto tzid = BinaryOperator::CreateShl(tz, ConstantInt::get(tz->getType(), 64), "tzid", block);
            const auto full = BinaryOperator::CreateOr(clean, tzid, "full", block);

            result->addIncoming(full, block);
            BranchInst::Create(done, block);
        }

        block = done;
        return result;
    }
#endif
private:
    void RegisterDependencies() const final {
        this->DependsOn(Datetime_);
        this->DependsOn(Id_);
    }

    IComputationNode* const Datetime_;
    IComputationNode* const Id_;
    const ui16 TimezonesCount_;
    const std::vector<ui16> BlackList_;
};

} // namespace

IComputationNode* WrapTimezoneId(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<char*>::Id, "Expected string");
    const auto value = LocateNode(ctx.NodeLocator, callable, 0);
    return new TTimezoneIdWrapper(ctx.Mutables, value);
}

IComputationNode* WrapTimezoneName(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    bool isOptional;
    const auto dataType = UnpackOptionalData(callable.GetInput(0), isOptional);
    MKQL_ENSURE(dataType->GetSchemeType() == NUdf::TDataType<ui16>::Id, "Expected Uint16");
    const auto value = LocateNode(ctx.NodeLocator, callable, 0);
    return new TTimezoneNameWrapper(ctx.Mutables, value);
}

IComputationNode* WrapAddTimezone(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 2, "Expected 2 arg");
    bool isOptional1;
    const auto dataType1 = UnpackOptionalData(callable.GetInput(0), isOptional1);
    MKQL_ENSURE(NUdf::GetDataTypeInfo(*dataType1->GetDataSlot()).Features & NUdf::DateType, "Expected date type");

    bool isOptional2;
    const auto dataType2 = UnpackOptionalData(callable.GetInput(1), isOptional2);
    MKQL_ENSURE(dataType2->GetSchemeType() == NUdf::TDataType<ui16>::Id, "Expected ui16");

    const auto value = LocateNode(ctx.NodeLocator, callable, 0);
    const auto id = LocateNode(ctx.NodeLocator, callable, 1);
    return YQL_RUNTIME_DISPATCH_NEW(IComputationNode*, TAddTimezoneWrapper, 2, isOptional1, isOptional2, ctx.Mutables, value, id);
}

} // namespace NKikimr::NMiniKQL
