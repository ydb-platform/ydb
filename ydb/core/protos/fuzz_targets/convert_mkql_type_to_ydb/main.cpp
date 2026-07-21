#include <ydb/core/protos/fuzz_targets/common.h>
#include <contrib/libs/protobuf-mutator/src/libfuzzer/libfuzzer_macro.h>

#include <yql/essentials/public/udf/udf_data_type.h>

#include <limits>

using namespace NKikimr;
using namespace NFuzzHelpers;

namespace {

bool IsValidSimpleDataType(const NKikimrMiniKQL::TType& type) {
    if (!IsSimpleDataType(type)) {
        return false;
    }

    const auto scheme = type.GetData().GetScheme();
    if (scheme > std::numeric_limits<NYql::NUdf::TDataTypeId>::max()) {
        return false;
    }

    if (!NYql::NUdf::FindDataSlot(static_cast<NYql::NUdf::TDataTypeId>(scheme))) {
        return false;
    }

    if (scheme == NYql::NProto::TypeIds::Decimal) {
        TString error;
        const auto& params = type.GetData().GetDecimalParams();
        return NScheme::TDecimalType::Validate(
            params.GetPrecision(),
            params.GetScale(),
            error
        );
    }

    return true;
}

} // namespace

DEFINE_PROTO_FUZZER(const NKikimrFuzz::TFuzzInput& input) {
    if (!input.has_mini_kql_type()) {
        return;
    }
    const auto& t = input.mini_kql_type();
    if (!IsValidSimpleDataType(t)) {
        return;
    }

    Ydb::Type out;
    try {
        ConvertMiniKQLTypeToYdbType(t, out);
    } catch (...) {
    }
    (void)out;
}
