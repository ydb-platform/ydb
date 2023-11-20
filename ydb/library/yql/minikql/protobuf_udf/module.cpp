#include "module.h"

namespace NYql {
namespace NUdf {

using namespace NProtoBuf;

void TProtobufBase::CleanupOnTerminate() const {
}

void TProtobufBase::GetAllFunctions(IFunctionsSink& sink) const {
    sink.Add(TStringRef::Of("Parse"));
    sink.Add(TStringRef::Of("ParseText"));
    sink.Add(TStringRef::Of("Serialize"));
    sink.Add(TStringRef::Of("SerializeText"));
}

void TProtobufBase::BuildFunctionTypeInfo(
        const TStringRef& name,
        TType* userType,
        const TStringRef& typeConfig,
        ui32 flags,
        IFunctionTypeInfoBuilder& builder) const
{
    Y_UNUSED(userType);
    Y_UNUSED(typeConfig);

    try {
        TProtoInfo typeInfo;
        ProtoTypeBuild(GetDescriptor(),
                       EEnumFormat::Number,
                       ERecursionTraits::Fail, true, builder, &typeInfo);

        auto stringType = builder.SimpleType<char*>();

        if ((TStringRef::Of("Serialize") == name) || (TStringRef::Of("SerializeText") == name)) {
            // function signature:
            //    String Serialize(Protobuf value)
            builder.Returns(stringType)
                   .Args()->Add(typeInfo.StructType)
                   .Flags(ICallablePayload::TArgumentFlags::AutoMap)
                   .Done();
        } else {
            // function signature:
            //    Protobuf Parse(String value)
            builder.Returns(typeInfo.StructType)
                   .Args()->Add(stringType)
                   .Flags(ICallablePayload::TArgumentFlags::AutoMap)
                   .Done();
        }


        if (TStringRef::Of("Serialize") == name) {
            if ((flags & TFlags::TypesOnly) == 0) {
                builder.Implementation(this->CreateSerialize(typeInfo, false));
            }
        }
        if (TStringRef::Of("SerializeText") == name) {
            if ((flags & TFlags::TypesOnly) == 0) {
                builder.Implementation(this->CreateSerialize(typeInfo, true));
            }
        }
        if (TStringRef::Of("Parse") == name) {
            if ((flags & TFlags::TypesOnly) == 0) {
                builder.Implementation(this->CreateValue(typeInfo, false));
            }
        }
        if (TStringRef::Of("ParseText") == name) {
            if ((flags & TFlags::TypesOnly) == 0) {
                builder.Implementation(this->CreateValue(typeInfo, true));
            }
        }
    } catch (...) {
        builder.SetError(CurrentExceptionMessage());
    }
}

} // namespace NUdf
} // namespace NYql
