#pragma once

#include "type_builder.h"
#include "proto_builder.h"
#include <yql/essentials/public/udf/udf_value.h>

#include <google/protobuf/message.h>

namespace NYql::NUdf {

class TProtobufValue: public TBoxedValue {
public:
    explicit TProtobufValue(const TProtoInfo& info);
    ~TProtobufValue() override;

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override;

    virtual TAutoPtr<NProtoBuf::Message> Parse(const TStringBuf& data) const = 0;

protected:
    const TProtoInfo Info_;
};

class TProtobufSerialize: public TBoxedValue {
public:
    explicit TProtobufSerialize(const TProtoInfo& info);
    ~TProtobufSerialize() override;

    TUnboxedValue Run(
        const IValueBuilder* valueBuilder,
        const TUnboxedValuePod* args) const override;

    virtual TMaybe<TString> Serialize(const NProtoBuf::Message& proto) const = 0;

    virtual TAutoPtr<NProtoBuf::Message> MakeProto() const = 0;

protected:
    const TProtoInfo Info_;
};

TUnboxedValue FillValueFromProto(
    const NProtoBuf::Message& proto,
    const IValueBuilder* valueBuilder,
    const TProtoInfo& info);

} // namespace NYql::NUdf
