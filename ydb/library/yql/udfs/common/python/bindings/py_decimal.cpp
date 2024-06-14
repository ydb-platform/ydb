#include "py_decimal.h"
#include "py_errors.h"
#include "py_utils.h"
#include "py_cast.h"

#include <util/stream/str.h>

#include <ydb/library/yql/public/udf/udf_value.h>

using namespace NKikimr;

namespace NPython {

TPyObjectPtr ToPyDecimal(const TPyCastContext::TPtr& ctx, const NKikimr::NUdf::TUnboxedValuePod& value, ui8 precision, ui8 scale)
{
    const auto str = NYql::NDecimal::ToString(value.GetInt128(), precision, scale);
    PY_ENSURE(str, "Bad decimal value.");

    const TPyObjectPtr pyStr(PyRepr(str));

    const TPyObjectPtr args(PyTuple_Pack(1, pyStr.Get()));
    PY_ENSURE(args, "Can't pack args.");

    const TPyObjectPtr dec(PyObject_CallObject(ctx->GetDecimal().Get(), args.Get()));
    PY_ENSURE(dec, "Can't create Decimal.");
    return dec;
}

NKikimr::NUdf::TUnboxedValue FromPyDecimal(const TPyCastContext::TPtr& ctx, PyObject* value, ui8 precision, ui8 scale)
{
    const TPyObjectPtr print(PyObject_Str(value));
    PY_ENSURE(print, "Can't print decimal.");

    TString str;
    PY_ENSURE(TryPyCast<TString>(print.Get(), str), "Can't get decimal string.");

    if (str.EndsWith("Infinity")) {
        str.resize(str.size() - 5U);
    }

    const auto dec = NYql::NDecimal::FromStringEx(str.c_str(), precision, scale);
    PY_ENSURE(!NYql::NDecimal::IsError(dec), "Can't make Decimal from string.");

    return NKikimr::NUdf::TUnboxedValuePod(dec);
}

const TPyObjectPtr& TPyCastContext::GetDecimal() {
    if (!Decimal) {
        const TPyObjectPtr module(PyImport_ImportModule("decimal"));
        PY_ENSURE(module, "Can't import decimal.");

        Decimal.ResetSteal(PyObject_GetAttrString(module.Get(), "Decimal"));
        PY_ENSURE(Decimal, "Can't get Decimal.");
    }

    return Decimal;
}

} // namespace NPython
