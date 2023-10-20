#pragma once

#include "type_builder.h"
#include "value_builder.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_registrator.h>

#include <google/protobuf/message.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace NYql {
namespace NUdf {

class TProtobufBase : public IUdfModule {
public:
    void CleanupOnTerminate() const override;

    void GetAllFunctions(IFunctionsSink& sink) const override;

    void BuildFunctionTypeInfo(
            const TStringRef& name,
            TType* userType,
            const TStringRef& typeConfig,
            ui32 flags,
            IFunctionTypeInfoBuilder& builder) const override;

protected:
    virtual const NProtoBuf::Descriptor* GetDescriptor() const = 0;

    virtual TProtobufValue* CreateValue(const TProtoInfo& info, bool asText) const = 0;

    virtual TProtobufSerialize* CreateSerialize(const TProtoInfo& info, bool asText) const = 0;
};


template <typename T>
class TProtobufModule : public TProtobufBase {

    class TValue : public TProtobufValue {
    public:
        TValue(const TProtoInfo& info, bool asText)
            : TProtobufValue(info)
            , AsText_(asText)
        {
        }

        TAutoPtr<NProtoBuf::Message> Parse(const TStringBuf& data) const override {
            TAutoPtr<T> proto(new T);
            if (AsText_) {
                NProtoBuf::io::ArrayInputStream si(data.data(), data.size());
                if (!NProtoBuf::TextFormat::Parse(&si, proto.Get())) {
                    ythrow yexception() << "can't parse text protobuf";
                }
            } else {
                if (!proto->ParseFromArray(data.data(), data.size())) {
                    ythrow yexception() << "can't parse binary protobuf";
                }
            }
            return proto.Release();
        }

    private:
        const bool AsText_;
    };

    class TSerialize : public TProtobufSerialize {
    public:
        TSerialize(const TProtoInfo& info, bool asText)
            : TProtobufSerialize(info)
            , AsText_(asText)
        {
        }

        TMaybe<TString> Serialize(const NProtoBuf::Message& proto) const override {
            TString result;
            if (AsText_) {
                if (!NProtoBuf::TextFormat::PrintToString(proto, &result)) {
                    ythrow yexception() << "can't serialize prototext message";
                }
            } else {
                result.ReserveAndResize(proto.ByteSize());
                if (!proto.SerializeToArray(result.begin(), result.size())) {
                    ythrow yexception() << "can't serialize protobin message";
                }
            }
            return result;
        }

        TAutoPtr<NProtoBuf::Message> MakeProto() const override {
            return TAutoPtr<NProtoBuf::Message>(new T);
        }
    private:
        const bool AsText_;
    };

private:
    const NProtoBuf::Descriptor* GetDescriptor() const override {
        return T::descriptor();
    }

    TProtobufValue* CreateValue(const TProtoInfo& info, bool asText) const override {
        return new TValue(info, asText);
    }

    TProtobufSerialize* CreateSerialize(const TProtoInfo& info, bool asText) const override {
        return new TSerialize(info, asText);
    }
};

} // namespace NUdf
} // namespace NYql
