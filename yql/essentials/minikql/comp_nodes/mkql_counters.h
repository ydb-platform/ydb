#pragma once

#include <yql/essentials/public/udf/udf_counter.h>

namespace NKikimr {
namespace NMiniKQL {

using TStringRef = NYql::NUdf::TStringRef;

constexpr TStringRef Operator_Filter("Operator.Filter.");
constexpr TStringRef Operator_Join("Operator.Join.");
constexpr TStringRef Operator_Aggregation("Operator.Aggregation.");

constexpr TStringRef Counter_Bytes("Bytes");
constexpr TStringRef Counter_Rows("Rows");
constexpr TStringRef Counter_Positive("Positive");
constexpr TStringRef Counter_Negative("Negative");

}
}
