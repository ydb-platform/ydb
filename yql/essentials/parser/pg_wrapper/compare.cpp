#include "pg_include.h"

#include <yql/essentials/parser/pg_wrapper/interface/sign.h>

#include <yql/essentials/parser/pg_wrapper/pg_ops.h>

#include <yql/essentials/minikql/mkql_alloc.h>

namespace NKikimr::NMiniKQL {

std::expected<bool, TString> PgCompareWithCasts(
    TStringBuf lhs, ui32 lhsTypeId,
    TStringBuf rhs, ui32 rhsTypeId,
    EPgCompareType cmpType)
{
    try {
        NKikimr::NMiniKQL::TOnlyThrowingBindTerminator bind;
        NKikimr::NMiniKQL::TScopedAlloc alloc(__LOCATION__);
        NKikimr::NMiniKQL::TPAllocScope scope;

        NYql::TPgConst lhsConst(lhsTypeId, lhs);
        auto lhsValue = lhsConst.ExtractConst();

        NYql::TPgConst rhsConst(rhsTypeId, rhs);
        auto rhsValue = rhsConst.ExtractConst();

        std::string_view operName = (cmpType == EPgCompareType::Less) ? "<" : ">";
        NYql::TPgCompareOp cmpFunc(lhsTypeId, rhsTypeId, operName);
        auto result = cmpFunc.MakeCallState().Compare(lhsValue, rhsValue);
        if (!result) {
            return std::unexpected(TString("Comparison result is NULL"));
        }
        return *result;
    } catch (const NYql::NPg::TOperatorNotFoundException& e) {
        return std::unexpected(TString(e.what()));
    }
}

} // namespace NKikimr::NMiniKQL
