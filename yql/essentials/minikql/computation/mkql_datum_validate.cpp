#include "mkql_datum_validate.h"

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/public/udf/arrow/args_dechunker.h>

#include <util/string/builder.h>

#include <arrow/array/validate.h>
#include <arrow/util/config.h>

namespace NKikimr::NMiniKQL {
namespace {
// In order to take a subarray for any nesting depth of a tuple,
// you only need to change the offset of the top-most ArrayData representative.
// All other children should remain as is, without changing their offsets.
// However, in YQL, we recursively traverse all nested ArrayData and change the offset there as well.
// Accordingly, this creates a difference between the classic Arrow ArrayData representation and what we have.

// E.g.
// We have Tuple<Int> array with following structure.
// {
//     len = 10
//     offset = 0
//     children = {
//          len = 10
//          offset = 5
//     }
// }
// To create a slice with offset == 3 apache arrow need only to change outer offset.
// {
//     len = 10
//     offset = 0 + 3
//     children = {
//          len = 10
//          offset = 5
//     }
// }
// But in YQL we change both: outer and inner offset.
// {
//     len = 10
//     offset = 0 + 3
//     children = {
//          len = 10
//          offset = 5 + 3

// }
// }
// So here is the helper that can help fix this.
// It simply sets the offset to 0 for types that have this problem.
// Also check bitmask before fixing.
//
// FIXME(YQL-20162): Change the validation algorithm.
std::shared_ptr<arrow::ArrayData> ConvertYqlOffsetsToArrowStandard(
    const arrow::ArrayData& arrayData) {
    auto result = arrayData.Copy();
    if (result->type->id() == arrow::Type::STRUCT ||
        result->type->id() == arrow::Type::DENSE_UNION ||
        result->type->id() == arrow::Type::SPARSE_UNION) {
        if (result->buffers[0]) {
            auto actualSize = arrow::BitUtil::BytesForBits(result->length + result->offset);
            MKQL_ENSURE(result->buffers[0]->size() >= actualSize, "Bitmask is invalid.");
        }
        result->offset = 0;
        result->null_count = arrow::kUnknownNullCount;
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> children;
    for (const auto& child : result->child_data) {
        children.push_back(ConvertYqlOffsetsToArrowStandard(*child));
    }
    result->child_data = children;
    return result;
}

arrow::Status ValidateArrayCheap(arrow::Datum datum) {
    auto array = ConvertYqlOffsetsToArrowStandard(*datum.array());
    arrow::Status status = arrow::internal::ValidateArray(*array);
    return status;
}

arrow::Status ValidateArrayExpensive(arrow::Datum datum) {
    ARROW_RETURN_NOT_OK(ValidateArrayCheap(datum));
    auto array = ConvertYqlOffsetsToArrowStandard(*datum.array());
    return arrow::internal::ValidateArrayFull(*array);
}

arrow::Status ValidateDatum(arrow::Datum datum, NYql::NUdf::EValidateDatumMode validateMode) {
    if (validateMode == NYql::NUdf::EValidateDatumMode::None) {
        return arrow::Status::OK();
    }
    if (datum.is_arraylike()) {
        NYql::NUdf::TArgsDechunker dechunker({datum});
        std::vector<arrow::Datum> chunk;
        while (dechunker.Next(chunk)) {
            Y_ENSURE(chunk[0].is_array());
            switch (validateMode) {
                case NYql::NUdf::EValidateDatumMode::None:
                    break;
                case NYql::NUdf::EValidateDatumMode::Cheap:
                    if (auto status = ValidateArrayCheap(chunk[0]); !status.ok()) {
                        return status;
                    }
                    break;
                case NYql::NUdf::EValidateDatumMode::Expensive:
                    if (auto status = ValidateArrayExpensive(chunk[0]); !status.ok()) {
                        return status;
                    }
                    break;
            }
        }
    } else if (datum.is_scalar()) {
        // Scalar validation is supported in ARROW-13132.
        // Add scalar support after library update (this is very similar to above array validation).
        static_assert(ARROW_VERSION_MAJOR == 5, "If you see this message please notify owners about update and remove this assert.");
    } else {
        // Must be either arraylike or scalar.
        Y_UNREACHABLE();
    }
    return arrow::Status::OK();
}

} // namespace

void ValidateDatum(arrow::Datum datum, TMaybe<arrow::ValueDescr> expectedDescription, NYql::NUdf::EValidateDatumMode validateMode) {
    if (expectedDescription) {
        ARROW_DEBUG_CHECK_DATUM_TYPES(*expectedDescription, datum.descr());
    }
    auto status = ValidateDatum(datum, validateMode);
    Y_DEBUG_ABORT_UNLESS(status.ok(), "%s", (TStringBuilder() << "Type: " << datum.descr().ToString() << ". Original error is: " << status.message()).c_str());
}

} // namespace NKikimr::NMiniKQL
