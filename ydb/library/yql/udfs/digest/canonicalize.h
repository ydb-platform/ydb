#pragma once

#include <ydb/library/table_digest/json_document.h>

#include <yql/essentials/public/udf/udf_type_inspection.h>
#include <yql/essentials/public/udf/udf_value_builder.h>

#include <util/generic/vector.h>
#include <util/generic/yexception.h>

#include <algorithm>
#include <utility>

namespace NYql::NDigest {

// A type-directed plan is built once when the UDF is instantiated. Subtrees
// without JsonDocument retain their original types and values byte for byte.
class TCanonicalizePlan {
public:
    enum class EKind {
        Identity,
        JsonDocument,
        Optional,
        Array,
        List,
        Dict,
        Variant,
        Tagged,
    };

    static TCanonicalizePlan Build(const NUdf::TType* type,
                                   NUdf::IFunctionTypeInfoBuilder& builder) {
        TCanonicalizePlan result;
        result.ResultType_ = type;
        const auto helper = builder.TypeInfoHelper();
        switch (helper->GetTypeKind(type)) {
            case NUdf::ETypeKind::Data: {
                const NUdf::TDataTypeInspector data(*helper, type);
                if (data.GetTypeId() == NUdf::TDataType<NUdf::TJsonDocument>::Id) {
                    result.Kind_ = EKind::JsonDocument;
                    result.ResultType_ = builder.SimpleType<char*>();
                }
                break;
            }
            case NUdf::ETypeKind::Optional: {
                const NUdf::TOptionalTypeInspector optional(*helper, type);
                auto item = Build(optional.GetItemType(), builder);
                if (item.Changed()) {
                    result.Kind_ = EKind::Optional;
                    result.ResultType_ = builder.Optional()->Item(item.ResultType_).Build();
                    result.Children_.push_back(std::move(item));
                }
                break;
            }
            case NUdf::ETypeKind::Struct: {
                const NUdf::TStructTypeInspector structure(*helper, type);
                result.Children_.reserve(structure.GetMembersCount());
                for (ui32 i = 0; i < structure.GetMembersCount(); ++i) {
                    result.Children_.push_back(Build(structure.GetMemberType(i), builder));
                }
                if (result.HasChangedChildren()) {
                    result.Kind_ = EKind::Array;
                    result.Indices_.resize(result.Children_.size());
                    const auto output = builder.Struct(structure.GetMembersCount());
                    for (ui32 i = 0; i < structure.GetMembersCount(); ++i) {
                        output->AddField(structure.GetMemberName(i), result.Children_[i].ResultType_, &result.Indices_[i]);
                    }
                    result.ResultType_ = output->Build();
                } else {
                    result.Children_.clear();
                }
                break;
            }
            case NUdf::ETypeKind::Tuple: {
                const NUdf::TTupleTypeInspector tuple(*helper, type);
                result.Children_.reserve(tuple.GetElementsCount());
                for (ui32 i = 0; i < tuple.GetElementsCount(); ++i) {
                    result.Children_.push_back(Build(tuple.GetElementType(i), builder));
                }
                if (result.HasChangedChildren()) {
                    result.Kind_ = EKind::Array;
                    const auto output = builder.Tuple(tuple.GetElementsCount());
                    for (const auto& child : result.Children_) {
                        output->Add(child.ResultType_);
                    }
                    result.ResultType_ = output->Build();
                } else {
                    result.Children_.clear();
                }
                break;
            }
            case NUdf::ETypeKind::List: {
                const NUdf::TListTypeInspector list(*helper, type);
                auto item = Build(list.GetItemType(), builder);
                if (item.Changed()) {
                    result.Kind_ = EKind::List;
                    result.ResultType_ = builder.List()->Item(item.ResultType_).Build();
                    result.Children_.push_back(std::move(item));
                }
                break;
            }
            case NUdf::ETypeKind::Dict: {
                const NUdf::TDictTypeInspector dict(*helper, type);
                result.Children_.push_back(Build(dict.GetKeyType(), builder));
                result.Children_.push_back(Build(dict.GetValueType(), builder));
                if (result.HasChangedChildren()) {
                    result.Kind_ = EKind::Dict;
                    result.ResultType_ = builder.Dict()
                                             ->Key(result.Children_[0].ResultType_)
                                             .Value(result.Children_[1].ResultType_)
                                             .Build();
                } else {
                    result.Children_.clear();
                }
                break;
            }
            case NUdf::ETypeKind::Variant: {
                const NUdf::TVariantTypeInspector variant(*helper, type);
                auto alternatives = Build(variant.GetUnderlyingType(), builder);
                if (alternatives.Changed()) {
                    result.Kind_ = EKind::Variant;
                    result.ResultType_ = builder.Variant()->Over(alternatives.ResultType_).Build();
                    result.Children_ = std::move(alternatives.Children_);
                    result.Indices_ = std::move(alternatives.Indices_);
                }
                break;
            }
            case NUdf::ETypeKind::Tagged: {
                const NUdf::TTaggedTypeInspector tagged(*helper, type);
                auto base = Build(tagged.GetBaseType(), builder);
                if (base.Changed()) {
                    result.Kind_ = EKind::Tagged;
                    result.ResultType_ = builder.Tagged(base.ResultType_, tagged.GetTag());
                    result.Children_.push_back(std::move(base));
                }
                break;
            }
            default:
                break;
        }
        return result;
    }

    const NUdf::TType* GetResultType() const {
        return ResultType_;
    }

    NUdf::TUnboxedValue Apply(const NUdf::IValueBuilder* builder,
                              const NUdf::TUnboxedValuePod& value) const {
        switch (Kind_) {
            case EKind::Identity:
                return value;
            case EKind::JsonDocument:
                return builder->NewString(CanonicalJsonDocument(value.AsStringRef()));
            case EKind::Optional: {
                if (!value) {
                    return {};
                }
                auto item = Children_[0].Apply(builder, value.GetOptionalValue());
                return item.Release().MakeOptional();
            }
            case EKind::Array: {
                NUdf::TUnboxedValue* items = nullptr;
                auto result = builder->NewArray(static_cast<ui32>(Children_.size()), items);
                for (ui32 i = 0; i < Children_.size(); ++i) {
                    const ui32 outputIndex = Indices_.empty() ? i : Indices_[i];
                    items[outputIndex] = Children_[i].Apply(builder, value.GetElement(i));
                }
                return result;
            }
            case EKind::List: {
                auto output = builder->NewListBuilder();
                auto iterator = value.GetListIterator();
                NUdf::TUnboxedValue item;
                while (iterator.Next(item)) {
                    output->Add(Children_[0].Apply(builder, item));
                }
                return output->Build();
            }
            case EKind::Dict: {
                auto output = builder->NewDict(ResultType_, value.IsSortedDict()
                    ? NUdf::TDictFlags::Sorted : NUdf::TDictFlags::Hashed);
                auto iterator = value.GetDictIterator();
                NUdf::TUnboxedValue key;
                NUdf::TUnboxedValue payload;
                while (iterator.NextPair(key, payload)) {
                    output->Add(Children_[0].Apply(builder, key), Children_[1].Apply(builder, payload));
                }
                return output->Build();
            }
            case EKind::Variant: {
                const ui32 index = value.GetVariantIndex();
                Y_ENSURE(index < Children_.size(), "Invalid Digest variant index");
                auto item = Children_[index].Apply(builder, value.GetVariantItem());
                return builder->NewVariant(Indices_.empty() ? index : Indices_[index], std::move(item));
            }
            case EKind::Tagged:
                return Children_[0].Apply(builder, value);
        }
        Y_UNREACHABLE();
    }

private:
    bool Changed() const {
        return Kind_ != EKind::Identity;
    }

    bool HasChangedChildren() const {
        return std::any_of(Children_.begin(), Children_.end(), [](const auto& child) { return child.Changed(); });
    }

    EKind Kind_ = EKind::Identity;
    const NUdf::TType* ResultType_ = nullptr;
    TVector<TCanonicalizePlan> Children_;
    TVector<ui32> Indices_;
};

} // namespace NYql::NDigest
