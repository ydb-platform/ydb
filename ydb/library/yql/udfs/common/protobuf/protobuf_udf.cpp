#include <ydb/library/yql/minikql/protobuf_udf/type_builder.h>
#include <ydb/library/yql/minikql/protobuf_udf/value_builder.h>
#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <library/cpp/protobuf/yql/descriptor.h>

using namespace NKikimr::NUdf;
using namespace NProtoBuf;

namespace {
    class TDynamicProtoValue: public TProtobufValue {
    public:
        TDynamicProtoValue(const TProtoInfo& info, TDynamicInfoRef dyn)
            : TProtobufValue(info)
            , Dynamic_(dyn)
        {
            Y_ASSERT(Dynamic_ != nullptr);
        }

        TAutoPtr<Message> Parse(const TStringBuf& data) const override {
            return Dynamic_->Parse(data);
        }

    private:
        TDynamicInfoRef Dynamic_;
    };

    class TDynamicProtoSerialize: public TProtobufSerialize {
    public:
        TDynamicProtoSerialize(const TProtoInfo& info, TDynamicInfoRef dyn)
            : TProtobufSerialize(info)
            , Dynamic_(dyn)
        {
            Y_ASSERT(Dynamic_ != nullptr);
        }

        TMaybe<TString> Serialize(const Message& proto) const override {
            return Dynamic_->Serialize(proto);
        }

        TAutoPtr<Message> MakeProto() const override {
            return Dynamic_->MakeProto();
        }
    private:
        TDynamicInfoRef Dynamic_;
    };

    class TDynamicProtoValueSafe: public TDynamicProtoValue {
    public:
        TDynamicProtoValueSafe(const TProtoInfo& info, TDynamicInfoRef dyn)
            : TDynamicProtoValue(info, dyn) {}

        TAutoPtr<Message> Parse(const TStringBuf& data) const override {
            try {
                return TDynamicProtoValue::Parse(data);
            } catch (const std::exception& e) {
                return nullptr;
            }
        }
    };

    class TProtobufModule: public IUdfModule {
    public:
        TStringRef Name() const {
            return TStringRef("Protobuf");
        }

        void CleanupOnTerminate() const final {
        }

        void GetAllFunctions(IFunctionsSink& sink) const final {
            sink.Add(TStringRef::Of("Parse"))->SetTypeAwareness();
            sink.Add(TStringRef::Of("TryParse"))->SetTypeAwareness();
            sink.Add(TStringRef::Of("Serialize"))->SetTypeAwareness();
        }

        void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const final {
            Y_UNUSED(userType);

            try {
                auto dyn = TDynamicInfo::Create(TStringBuf(typeConfig.Data(), typeConfig.Size()));

                TProtoInfo typeInfo;
                ProtoTypeBuild(dyn->Descriptor(),
                               dyn->GetEnumFormat(),
                               dyn->GetRecursionTraits(),
                               dyn->GetOptionalLists(),
                               builder, &typeInfo,
                               EProtoStringYqlType::Bytes,
                               dyn->GetSyntaxAware(),
                               false,
                               dyn->GetYtMode());

                auto stringType = builder.SimpleType<char*>();
                auto structType = typeInfo.StructType;
                auto optionalStructType = builder.Optional()->Item(structType).Build();

                if (TStringRef::Of("Serialize") == name) {
                    // function signature:
                    //    String Serialize(Protobuf value)
                    builder.Returns(stringType)
                        .Args()
                        ->Add(structType)
                        .Flags(ICallablePayload::TArgumentFlags::AutoMap)
                        .Done();
                    if ((flags & TFlags::TypesOnly) == 0) {
                        builder.Implementation(new TDynamicProtoSerialize(typeInfo, dyn));
                    }
                } else {
                    // function signature:
                    //    Protobuf Parse(String value)
                    builder.Returns((TStringRef::Of("TryParse") == name) ? optionalStructType : structType)
                        .Args()
                        ->Add(stringType)
                        .Flags(ICallablePayload::TArgumentFlags::AutoMap)
                        .Done();

                    if (TStringRef::Of("Parse") == name) {
                        if ((flags & TFlags::TypesOnly) == 0) {
                            builder.Implementation(new TDynamicProtoValue(typeInfo, dyn));
                        }
                    } else if (TStringRef::Of("TryParse") == name) {
                        if ((flags & TFlags::TypesOnly) == 0) {
                            builder.Implementation(new TDynamicProtoValueSafe(typeInfo, dyn));
                        }
                    }
                }

            } catch (const std::exception& e) {
                builder.SetError(CurrentExceptionMessage());
            }
        }
    };

}

REGISTER_MODULES(TProtobufModule);
