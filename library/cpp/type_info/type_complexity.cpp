#include "type_complexity.h"

#include "type.h"


namespace NTi {

int ComputeTypeComplexity(const TTypePtr& type)
{
    return ComputeTypeComplexity(type.Get());
}

int ComputeTypeComplexity(const TType* type)
{
    switch (type->GetTypeName()) {
        case ETypeName::Bool:
        case ETypeName::Int8:
        case ETypeName::Int16:
        case ETypeName::Int32:
        case ETypeName::Int64:
        case ETypeName::Uint8:
        case ETypeName::Uint16:
        case ETypeName::Uint32:
        case ETypeName::Uint64:
        case ETypeName::Float:
        case ETypeName::Double:
        case ETypeName::String:
        case ETypeName::Utf8:
        case ETypeName::Date:
        case ETypeName::Datetime:
        case ETypeName::Timestamp:
        case ETypeName::TzDate:
        case ETypeName::TzDatetime:
        case ETypeName::TzTimestamp:
        case ETypeName::Interval:
        case ETypeName::Decimal:
        case ETypeName::Json:
        case ETypeName::Yson:
        case ETypeName::Uuid:
        case ETypeName::Date32:
        case ETypeName::Datetime64:
        case ETypeName::Timestamp64:
        case ETypeName::Interval64:
        case ETypeName::Void:
        case ETypeName::Null:
            return 1;

        case ETypeName::Optional:
            return 1 + ComputeTypeComplexity(type->AsOptionalRaw()->GetItemTypeRaw());

        case ETypeName::List:
            return 1 + ComputeTypeComplexity(type->AsListRaw()->GetItemTypeRaw());

        case ETypeName::Dict:
            return 1 + ComputeTypeComplexity(type->AsDictRaw()->GetKeyTypeRaw())
                   + ComputeTypeComplexity(type->AsDictRaw()->GetValueTypeRaw());
        case ETypeName::Struct: {
            int result = 1;
            for (const auto& member : type->AsStructRaw()->GetMembers()) {
                result += ComputeTypeComplexity(member.GetTypeRaw());
            }
            return result;
        }
        case ETypeName::Tuple: {
            int result = 1;
            for (const auto& element : type->AsTupleRaw()->GetElements()) {
                result += ComputeTypeComplexity(element.GetTypeRaw());
            }
            return result;
        }
        case ETypeName::Variant: {
            return ComputeTypeComplexity(type->AsVariantRaw()->GetUnderlyingTypeRaw());
        }
        case ETypeName::Tagged: {
            return 1 + ComputeTypeComplexity(type->AsTaggedRaw()->GetItemType());
        }
    }
    Y_ABORT("internal error: unreachable code");
}

} // namespace NTi

