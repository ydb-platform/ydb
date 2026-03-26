#include "mkql_datum_validate.h"

#include <yql/essentials/minikql/defs.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/public/udf/arrow/args_dechunker.h>
#include <yql/essentials/public/udf/arrow/dispatch_traits.h>

#include <util/string/builder.h>

#include <arrow/array/validate.h>
#include <arrow/util/bit_util.h>
#include <arrow/util/config.h>

namespace NKikimr::NMiniKQL {
namespace {
// This helper is introduced to fix differences between Apache Arrow and YQL data invariants.
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
std::shared_ptr<arrow20::ArrayData> ConvertYqlOffsetsToArrowStandard(
    const arrow20::ArrayData& arrayData) {
    auto result = arrayData.Copy();
    if (result->type->id() == arrow20::Type::STRUCT ||
        result->type->id() == arrow20::Type::DENSE_UNION ||
        result->type->id() == arrow20::Type::SPARSE_UNION) {
        if (result->buffers[0]) {
            auto actualSize = arrow20::bit_util::BytesForBits(result->length + result->offset);
            MKQL_ENSURE(result->buffers[0]->size() >= actualSize, "Bitmask is invalid.");
        }
        result->offset = 0;
        result->null_count = arrow20::kUnknownNullCount;
    }

    std::vector<std::shared_ptr<arrow20::ArrayData>> children;
    for (const auto& child : result->child_data) {
        children.push_back(ConvertYqlOffsetsToArrowStandard(*child));
    }
    result->child_data = children;
    return result;
}

// Helper for datum validation.
// It checks invariants that cannot be checked via standart Apache Arrow validator.
class IDatumValidator {
public:
    virtual ~IDatumValidator() = default;
    virtual void Validate(arrow20::Datum datum) const = 0;
};

class TDatumValidatorBase: public IDatumValidator {
public:
    using TPtr = std::unique_ptr<TDatumValidatorBase>;

    explicit TDatumValidatorBase(const NYql::NUdf::TType* type)
        : Type_(type)
    {
    }

protected:
    const NYql::NUdf::TType* Type() const {
        return Type_;
    }

private:
    const NYql::NUdf::TType* Type_;
};

class TUnimplementedValidator: public TDatumValidatorBase {
public:
    using TDatumValidatorBase::TDatumValidatorBase;
    void Validate(arrow20::Datum datum) const override {
        Y_UNUSED(datum);
    }
};

template <bool IsNull>
class TSingularValidator: public TDatumValidatorBase {
public:
    using TDatumValidatorBase::TDatumValidatorBase;

    void Validate(arrow20::Datum datum) const override {
        ValidateNullCount(datum);
        ValidateEmptyNullBuffer(datum);
    }

private:
    void ValidateNullCount(arrow20::Datum datum) const {
        if (datum.is_scalar()) {
            MKQL_ENSURE(datum.scalar()->is_valid == !IsNull, "Singular type invariant violation.");
        } else {
            auto expectedNullCount = IsNull ? datum.array()->length : 0;
            MKQL_ENSURE(datum.array()->GetNullCount() == expectedNullCount,
                        TStringBuilder() << "Singular type invariant null count violation. Expected: " << expectedNullCount << ", Got: " << datum.array()->GetNullCount());
        }
    }

    void ValidateEmptyNullBuffer(arrow20::Datum datum) const {
        if (datum.is_scalar()) {
            return;
        }
        MKQL_ENSURE(datum.array()->buffers[0] == nullptr, "Must be empty buffer.");
    }
};

template <bool Nullable>
class TTupleValidator: public TDatumValidatorBase {
public:
    TTupleValidator(TVector<TDatumValidatorBase::TPtr>&& children,
                    const NYql::NUdf::TType* type)
        : TDatumValidatorBase(type)
        , Children_(std::move(children))
    {
    }

    void Validate(arrow20::Datum datum) const override {
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                for (size_t i = 0; i < Children_.size(); ++i) {
                    Children_[i]->Validate(arrow20::Datum(dynamic_cast<const arrow20::StructScalar&>(*datum.scalar()).value.at(i)));
                }
            }
        } else {
            for (size_t i = 0; i < Children_.size(); ++i) {
                Children_[i]->Validate(*datum.array()->child_data[i]);
            }
        }
    }

protected:
    TVector<TDatumValidatorBase::TPtr> Children_;
};

class TExternalOptionalValidator: public TDatumValidatorBase {
public:
    TExternalOptionalValidator(TDatumValidatorBase::TPtr base, const NYql::NUdf::TType* type)
        : TDatumValidatorBase(type)
        , Base_(std::move(base))
    {
    }

    void Validate(arrow20::Datum datum) const override {
        if (datum.is_scalar()) {
            if (datum.scalar()->is_valid) {
                Base_->Validate(arrow20::Datum(dynamic_cast<const arrow20::StructScalar&>(*datum.scalar()).value.at(0)));
            }
        } else {
            Base_->Validate(*datum.array()->child_data[0]);
        }
    }

protected:
    TDatumValidatorBase::TPtr Base_;
};

struct TValidatorTraits {
    using TResult = TDatumValidatorBase;

    template <bool Nullable>
    using TTuple = TTupleValidator<Nullable>;

    template <typename T, bool Nullable>
    using TFixedSize = TUnimplementedValidator;

    template <typename TStringType, bool Nullable, NKikimr::NUdf::EDataSlot TOriginal>
    using TStrings = TUnimplementedValidator;
    using TExtOptional = TExternalOptionalValidator;
    template <bool Nullable>
    using TResource = TUnimplementedValidator;

    template <typename TTzDate, bool Nullable>
    using TTzDateValidator = TUnimplementedValidator;
    template <bool IsNull>
    using TSingular = TSingularValidator<IsNull>;

    constexpr static bool PassType = true;

    static std::unique_ptr<TResult> MakePg(const NYql::NUdf::TPgTypeDescription& desc,
                                           const NYql::NUdf::IPgBuilder* pgBuilder,
                                           const NYql::NUdf::TType* type) {
        Y_UNUSED(desc, pgBuilder);
        return std::make_unique<TUnimplementedValidator>(type);
    }

    static std::unique_ptr<TResult> MakeResource(bool isOptional,
                                                 const NYql::NUdf::TType* type) {
        Y_UNUSED(isOptional);
        return std::make_unique<TUnimplementedValidator>(type);
    }

    template <typename TTzDate>
    static std::unique_ptr<TResult> MakeTzDate(bool isOptional,
                                               const NYql::NUdf::TType* type) {
        Y_UNUSED(isOptional);
        return std::make_unique<TUnimplementedValidator>(type);
    }

    template <bool IsNull>
    static std::unique_ptr<TResult> MakeSingular(const NYql::NUdf::TType* type) {
        return std::make_unique<TSingularValidator<IsNull>>(type);
    }
};

std::unique_ptr<TValidatorTraits::TResult> MakeBlockValidator(const NYql::NUdf::ITypeInfoHelper& typeInfoHelper,
                                                              const NYql::NUdf::TType* type) {
    MKQL_ENSURE(typeInfoHelper.GetTypeKind(type) == NYql::NUdf::ETypeKind::Block, "Expected block type.");
    return DispatchByArrowTraits<TValidatorTraits>(typeInfoHelper, NYql::NUdf::TBlockTypeInspector(typeInfoHelper, type).GetItemType(), /*pgBuilder=*/nullptr);
}

arrow20::Status ValidateArrayCheap(arrow20::Datum datum, const TType* type) {
    if (type) {
        MakeBlockValidator(TTypeInfoHelper(), type)->Validate(datum);
    }
    auto array = ConvertYqlOffsetsToArrowStandard(*datum.array());
    arrow20::Status status = arrow20::internal::ValidateArray(*array);
    return status;
}

arrow20::Status ValidateArrayExpensive(arrow20::Datum datum, const TType* type) {
    ARROW_RETURN_NOT_OK(ValidateArrayCheap(datum, type));
    auto array = ConvertYqlOffsetsToArrowStandard(*datum.array());
    return arrow20::internal::ValidateArrayFull(*array);
}

arrow20::Status ValidateDatum(arrow20::Datum datum, const TType* type, NYql::NUdf::EValidateDatumMode validateMode) {
    if (datum.is_arraylike()) {
        NYql::NUdf::TArgsDechunker dechunker({datum});
        std::vector<arrow20::Datum> chunk;
        while (dechunker.Next(chunk)) {
            Y_ENSURE(chunk[0].is_array());
            switch (validateMode) {
                case NYql::NUdf::EValidateDatumMode::None:
                    break;
                case NYql::NUdf::EValidateDatumMode::Cheap:
                    if (auto status = ValidateArrayCheap(chunk[0], type); !status.ok()) {
                        return status;
                    }
                    break;
                case NYql::NUdf::EValidateDatumMode::Expensive:
                    if (auto status = ValidateArrayExpensive(chunk[0], type); !status.ok()) {
                        return status;
                    }
                    break;
            }
        }
    } else if (datum.is_scalar()) {
        if (type) {
            MakeBlockValidator(TTypeInfoHelper(), type)->Validate(datum);
        }
        // Apache arrow scalar validation is supported in ARROW-13132.
        // Add scalar support after library update (this is very similar to above array validation).
        // NOLINTNEXTLINE(misc-redundant-expression)
        static_assert(ARROW_VERSION_MAJOR == 5, "If you see this message please notify owners about update and remove this assert.");
    } else {
        // Must be either arraylike or scalar.
        Y_UNREACHABLE();
    }
    return arrow20::Status::OK();
}

} // namespace

void ValidateDatum(arrow20::Datum datum, TMaybe<arrow20::TypeHolder> expectedType, const TType* type, NYql::NUdf::EValidateDatumMode validateMode) {
    if (validateMode == NYql::NUdf::EValidateDatumMode::None) {
        return;
    }
    if (expectedType) {
        ARROW_CHECK_DATUM_TYPES(*expectedType, arrow20::TypeHolder(datum.type()));
    }
    if (type) {
        if (const auto* blockType = dynamic_cast<const TBlockType*>(type)) {
            const bool wantScalar = blockType->GetShape() == TBlockType::EShape::Scalar;
            MKQL_ENSURE(wantScalar == datum.is_scalar(), "Block shape does not match datum kind");
        }
    }
    auto status = ValidateDatum(datum, type, validateMode);
    Y_ABORT_UNLESS(status.ok(), "%s",
                   (TStringBuilder() << "Type: " << (datum.type() ? datum.type()->ToString() : TString("<null>"))
                                      << ". Original error is: " << status.message())
                       .c_str());
}

} // namespace NKikimr::NMiniKQL
