#include "pg_ops.h"

#include <yql/essentials/parser/pg_catalog/catalog.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/parser/pg_wrapper/utils.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TPgSignNumericState: public TPgSign::TCallState {
public:
    TPgSignNumericState(const TPgResolvedCallWithCast& call, const TPgCast& resultCaster)
        : Call_(call)
        , ResultCaster_(resultCaster)
        , CallState_(MakeHolder<TPgResolvedCallWithCastState>(call.GetCall(), call.GetCasters()))
        , ResultCastState_(MakeHolder<TPgCastState>(resultCaster.GetFInfo1(), resultCaster.GetFInfo2()))
    {
    }

    TMaybe<i32> GetSign(const NUdf::TUnboxedValue& value) override {
        std::array<NUdf::TUnboxedValue, 1> args = {value};
        auto result = Call_.CallFunctionWithCast(*CallState_, args);
        if (!result) {
            return Nothing();
        }

        auto floatResult = ResultCaster_.Calculate(result, -1, *ResultCastState_);
        MKQL_ENSURE(floatResult, "Result cast returned NULL unexpectedly");
        auto floatValue = DatumGetFloat8(ScalarDatumFromPod(floatResult));
        if (floatValue > 0) {
            return 1;
        } else if (floatValue < 0) {
            return -1;
        } else {
            return 0;
        }
    }

private:
    const TPgResolvedCallWithCast& Call_;
    const TPgCast& ResultCaster_;
    THolder<TPgResolvedCallWithCastState> CallState_;
    THolder<TPgCastState> ResultCastState_;
};

// State for interval sign
class TPgSignIntervalState: public TPgSign::TCallState {
public:
    TPgSignIntervalState(const TPgResolvedCall<false>& eqCall, const TPgResolvedCall<false>& gtCall,
                         const TPgConst& zeroInterval)
        : ZeroInterval_(zeroInterval)
        , EqCall_(eqCall)
        , GtCall_(gtCall)
        , EqState_(MakeHolder<TPgResolvedCallState>(eqCall.GetArgCount(), &eqCall.GetFInfo()))
        , GtState_(MakeHolder<TPgResolvedCallState>(gtCall.GetArgCount(), &gtCall.GetFInfo()))
    {
    }

    TMaybe<i32> GetSign(const NUdf::TUnboxedValue& value) override {
        auto zeroValue = ZeroInterval_.ExtractConst();
        std::array<NUdf::TUnboxedValue, 2> args = {value, zeroValue};
        // Check if == 0
        auto eqResult = EqCall_.CallFunction(*EqState_, args);
        if (!eqResult) {
            return Nothing();
        }
        if (DatumGetBool(ScalarDatumFromPod(eqResult))) {
            return 0;
        }

        // Check if > 0
        auto gtResult = GtCall_.CallFunction(*GtState_, args);

        MKQL_ENSURE(gtResult, "Comparison operator returned NULL unexpectedly");
        if (DatumGetBool(ScalarDatumFromPod(gtResult))) {
            return 1;
        }

        return -1;
    }

private:
    const TPgConst& ZeroInterval_;
    const TPgResolvedCall<false>& EqCall_;
    const TPgResolvedCall<false>& GtCall_;
    THolder<TPgResolvedCallState> EqState_;
    THolder<TPgResolvedCallState> GtState_;
};

// Implementation for numeric types (uses sign() function)
class TPgSignNumeric: public TPgSign {
public:
    explicit TPgSignNumeric(ui32 inputTypeOid);

    std::unique_ptr<TCallState> MakeCallState() const override;

private:
    TPgResolvedCallWithCast Call_;
    TPgCast ResultCaster_;
};

// Implementation for interval type (uses == and > operators)
class TPgSignInterval: public TPgSign {
public:
    explicit TPgSignInterval(ui32 inputTypeOid);

    std::unique_ptr<TCallState> MakeCallState() const override;

private:
    TPgResolvedCall<false> EqCall_;
    TPgResolvedCall<false> GtCall_;
    TPgConst ZeroInterval_;
};

// TPgSign factory
std::unique_ptr<TPgSign> TPgSign::Create(ui32 inputTypeOid) {
    if (inputTypeOid == INTERVALOID) {
        return std::make_unique<TPgSignInterval>(inputTypeOid);
    }
    return std::make_unique<TPgSignNumeric>(inputTypeOid);
}

// TPgSignNumeric implementation
TPgSignNumeric::TPgSignNumeric(ui32 inputTypeOid)
    : Call_(TPgResolvedCallWithCast::ForProc("sign", {inputTypeOid}))
    , ResultCaster_(Call_.GetReturnTypeId(), FLOAT8OID, false)
{
}

std::unique_ptr<TPgSign::TCallState> TPgSignNumeric::MakeCallState() const {
    return std::make_unique<TPgSignNumericState>(Call_, ResultCaster_);
}

namespace {

TPgResolvedCall<false> MakeOperatorCall(std::string_view operName, ui32 leftType, ui32 rightType) {
    const auto& oper = NPg::LookupOper(TString(operName), {leftType, rightType});
    MKQL_ENSURE(oper.LeftType == leftType && oper.RightType == rightType,
                "Type mismatch for operator " << operName);
    TVector<ui32> argTypes = {oper.LeftType, oper.RightType};
    return TPgResolvedCall<false>(operName, oper.ProcId, argTypes, oper.ResultType);
}

} // namespace

// TPgSignInterval implementation
TPgSignInterval::TPgSignInterval(ui32 inputTypeOid)
    : EqCall_(MakeOperatorCall("=", inputTypeOid, INTERVALOID))
    , GtCall_(MakeOperatorCall(">", inputTypeOid, INTERVALOID))
    , ZeroInterval_(INTERVALOID, "0 seconds")
{
    MKQL_ENSURE(EqCall_.GetReturnTypeId() == BOOLOID,
                "Equality operator must return bool, got " << EqCall_.GetReturnTypeId());
    MKQL_ENSURE(GtCall_.GetReturnTypeId() == BOOLOID,
                "Greater-than operator must return bool, got " << GtCall_.GetReturnTypeId());
    MKQL_ENSURE(EqCall_.GetArgTypeId(0) == INTERVALOID && EqCall_.GetArgTypeId(1) == INTERVALOID,
                "Equality operator must expect (interval, interval), got (" << EqCall_.GetArgTypeId(0) << ", " << EqCall_.GetArgTypeId(1) << ")");
    MKQL_ENSURE(GtCall_.GetArgTypeId(0) == INTERVALOID && GtCall_.GetArgTypeId(1) == INTERVALOID,
                "Greater-than operator must expect (interval, interval), got (" << GtCall_.GetArgTypeId(0) << ", " << GtCall_.GetArgTypeId(1) << ")");
}

std::unique_ptr<TPgSign::TCallState> TPgSignInterval::MakeCallState() const {
    return std::make_unique<TPgSignIntervalState>(EqCall_, GtCall_, ZeroInterval_);
}

TPgCompareOp::TCallState::TCallState(const TPgResolvedCallWithCast& call)
    : State(call.CreateState())
    , Call(call)
{
}

TPgCompareOp::TPgCompareOp(ui32 lhsTypeOid, ui32 rhsTypeOid, std::string_view operName)
    : Call(TPgResolvedCallWithCast::ForOperator(operName, {lhsTypeOid, rhsTypeOid}))
{
    MKQL_ENSURE(Call.GetReturnTypeId() == BOOLOID,
                "Comparison operator must return bool, got " << Call.GetReturnTypeId());
}

TPgCompareOp::TCallState TPgCompareOp::MakeCallState() const {
    return TCallState(Call);
}

TMaybe<bool> TPgCompareOp::TCallState::Compare(const NUdf::TUnboxedValue& lhs, const NUdf::TUnboxedValue& rhs) {
    std::array<NUdf::TUnboxedValue, 2> args = {lhs, rhs};
    auto result = Call.CallFunctionWithCast(State, args);
    if (!result) {
        return Nothing();
    }
    return DatumGetBool(ScalarDatumFromPod(result));
}

} // namespace NYql
