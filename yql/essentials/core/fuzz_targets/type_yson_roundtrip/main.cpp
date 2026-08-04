#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/ast/yql_type_string.h>
#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <library/cpp/yson/writer.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/string/cast.h>

#include <algorithm>

namespace {

using namespace NYql;

constexpr ui32 MaxDepth = 4;
constexpr size_t MaxItems = 3;

const NUdf::EDataSlot SimpleDataSlots[] = {
    NUdf::EDataSlot::Bool,
    NUdf::EDataSlot::Int8,
    NUdf::EDataSlot::Uint8,
    NUdf::EDataSlot::Int16,
    NUdf::EDataSlot::Uint16,
    NUdf::EDataSlot::Int32,
    NUdf::EDataSlot::Uint32,
    NUdf::EDataSlot::Int64,
    NUdf::EDataSlot::Uint64,
    NUdf::EDataSlot::Float,
    NUdf::EDataSlot::Double,
    NUdf::EDataSlot::String,
    NUdf::EDataSlot::Utf8,
    NUdf::EDataSlot::Yson,
    NUdf::EDataSlot::Json,
    NUdf::EDataSlot::Uuid,
    NUdf::EDataSlot::Date,
    NUdf::EDataSlot::Datetime,
    NUdf::EDataSlot::Timestamp,
    NUdf::EDataSlot::Interval,
    NUdf::EDataSlot::TzDate,
    NUdf::EDataSlot::TzDatetime,
    NUdf::EDataSlot::TzTimestamp,
    NUdf::EDataSlot::DyNumber,
    NUdf::EDataSlot::JsonDocument,
    NUdf::EDataSlot::Date32,
    NUdf::EDataSlot::Datetime64,
    NUdf::EDataSlot::Timestamp64,
    NUdf::EDataSlot::Interval64,
    NUdf::EDataSlot::TzDate32,
    NUdf::EDataSlot::TzDatetime64,
    NUdf::EDataSlot::TzTimestamp64,
};

const NUdf::EDataSlot HashableKeySlots[] = {
    NUdf::EDataSlot::Bool,
    NUdf::EDataSlot::Int8,
    NUdf::EDataSlot::Uint8,
    NUdf::EDataSlot::Int16,
    NUdf::EDataSlot::Uint16,
    NUdf::EDataSlot::Int32,
    NUdf::EDataSlot::Uint32,
    NUdf::EDataSlot::Int64,
    NUdf::EDataSlot::Uint64,
    NUdf::EDataSlot::String,
    NUdf::EDataSlot::Utf8,
    NUdf::EDataSlot::Uuid,
    NUdf::EDataSlot::Date,
    NUdf::EDataSlot::Datetime,
    NUdf::EDataSlot::Timestamp,
};

TStringBuf FuzzName(TExprContext& ctx, FuzzedDataProvider& fdp, TStringBuf prefix, size_t index) {
    TStringBuilder builder;
    builder << prefix << index;

    const size_t extra = fdp.ConsumeIntegralInRange<size_t>(0, 8);
    for (size_t i = 0; i < extra; ++i) {
        const ui8 ch = fdp.ConsumeIntegralInRange<ui8>(0, 37);
        if (ch < 10) {
            builder << char('0' + ch);
        } else if (ch < 36) {
            builder << char('a' + ch - 10);
        } else {
            builder << '_';
        }
    }

    return ctx.AppendString(builder);
}

const TTypeAnnotationNode* MakeDataType(TExprContext& ctx, FuzzedDataProvider& fdp) {
    if (fdp.ConsumeBool()) {
        const ui8 precision = fdp.ConsumeIntegralInRange<ui8>(1, 35);
        const ui8 scale = fdp.ConsumeIntegralInRange<ui8>(0, precision);
        return ctx.MakeType<TDataExprParamsType>(
            NUdf::EDataSlot::Decimal,
            ctx.AppendString(ToString(precision)),
            ctx.AppendString(ToString(scale)));
    }

    return ctx.MakeType<TDataExprType>(
        fdp.PickValueInArray<NUdf::EDataSlot>(SimpleDataSlots));
}

const TTypeAnnotationNode* MakeHashableKeyType(TExprContext& ctx, FuzzedDataProvider& fdp) {
    auto type = ctx.MakeType<TDataExprType>(
        fdp.PickValueInArray<NUdf::EDataSlot>(HashableKeySlots));
    if (fdp.ConsumeBool()) {
        return ctx.MakeType<TOptionalExprType>(type);
    }
    return type;
}

const TTypeAnnotationNode* MakeType(TExprContext& ctx, FuzzedDataProvider& fdp, ui32 depth);

const TTypeAnnotationNode* MakeLeafType(TExprContext& ctx, FuzzedDataProvider& fdp) {
    switch (fdp.ConsumeIntegralInRange<ui8>(0, 9)) {
        case 0:
            return ctx.MakeType<TVoidExprType>();
        case 1:
            return ctx.MakeType<TNullExprType>();
        case 2:
            return ctx.MakeType<TUnitExprType>();
        case 3:
            return ctx.MakeType<TEmptyListExprType>();
        case 4:
            return ctx.MakeType<TEmptyDictExprType>();
        case 5:
            return ctx.MakeType<TGenericExprType>();
        case 6:
            return ctx.MakeType<TResourceExprType>(FuzzName(ctx, fdp, "resource", 0));
        case 7:
            return ctx.MakeType<TUniversalExprType>();
        case 8:
            return ctx.MakeType<TUniversalStructExprType>();
        case 9:
            return MakeDataType(ctx, fdp);
    }

    return MakeDataType(ctx, fdp);
}

const TTypeAnnotationNode* MakeTupleType(TExprContext& ctx, FuzzedDataProvider& fdp, ui32 depth, size_t minItems = 0) {
    TTypeAnnotationNode::TListType items;
    const size_t itemCount = fdp.ConsumeIntegralInRange<size_t>(minItems, MaxItems);
    items.reserve(itemCount);
    for (size_t i = 0; i < itemCount; ++i) {
        items.push_back(MakeType(ctx, fdp, depth + 1));
    }

    return ctx.MakeType<TTupleExprType>(items);
}

const TTypeAnnotationNode* MakeStructType(TExprContext& ctx, FuzzedDataProvider& fdp, ui32 depth, size_t minItems = 0) {
    TVector<const TItemExprType*> items;
    const size_t itemCount = fdp.ConsumeIntegralInRange<size_t>(minItems, MaxItems);
    items.reserve(itemCount);
    for (size_t i = 0; i < itemCount; ++i) {
        items.push_back(ctx.MakeType<TItemExprType>(
            FuzzName(ctx, fdp, "member", i),
            MakeType(ctx, fdp, depth + 1)));
    }

    Sort(items, TStructExprType::TItemLess());
    items.erase(std::unique(items.begin(), items.end(), [](const TItemExprType* lhs, const TItemExprType* rhs) {
        return lhs->GetName() == rhs->GetName();
    }), items.end());

    return ctx.MakeType<TStructExprType>(items);
}

const TTypeAnnotationNode* MakeCallableType(TExprContext& ctx, FuzzedDataProvider& fdp, ui32 depth) {
    TVector<TCallableExprType::TArgumentInfo> args;
    const size_t argCount = fdp.ConsumeIntegralInRange<size_t>(0, MaxItems);
    const size_t optionalCount = argCount == 0 ? 0 : fdp.ConsumeIntegralInRange<size_t>(0, argCount);
    const bool namedArgs = fdp.ConsumeBool();
    args.reserve(argCount);
    for (size_t i = 0; i < argCount; ++i) {
        TCallableExprType::TArgumentInfo arg;
        arg.Type = MakeType(ctx, fdp, depth + 1);
        if (i >= argCount - optionalCount) {
            arg.Type = ctx.MakeType<TOptionalExprType>(arg.Type);
        }
        arg.Name = namedArgs ? FuzzName(ctx, fdp, "arg", i) : TStringBuf();
        arg.Flags = fdp.ConsumeIntegralInRange<ui64>(0, 3);
        args.push_back(arg);
    }

    return ctx.MakeType<TCallableExprType>(
        MakeType(ctx, fdp, depth + 1),
        args,
        optionalCount,
        fdp.ConsumeBool() ? FuzzName(ctx, fdp, "payload", 0) : TStringBuf());
}

const TTypeAnnotationNode* MakeType(TExprContext& ctx, FuzzedDataProvider& fdp, ui32 depth) {
    if (depth >= MaxDepth || fdp.remaining_bytes() == 0) {
        return MakeLeafType(ctx, fdp);
    }

    switch (fdp.ConsumeIntegralInRange<ui8>(0, 14)) {
        case 0:
            return MakeLeafType(ctx, fdp);
        case 1:
            return MakeDataType(ctx, fdp);
        case 2:
            return ctx.MakeType<TOptionalExprType>(MakeType(ctx, fdp, depth + 1));
        case 3:
            return ctx.MakeType<TListExprType>(MakeType(ctx, fdp, depth + 1));
        case 4:
            return ctx.MakeType<TStreamExprType>(MakeType(ctx, fdp, depth + 1));
        case 5:
            return ctx.MakeType<TBlockExprType>(MakeType(ctx, fdp, depth + 1));
        case 6:
            return ctx.MakeType<TScalarExprType>(MakeType(ctx, fdp, depth + 1));
        case 7:
            return ctx.MakeType<TLinearExprType>(MakeType(ctx, fdp, depth + 1));
        case 8:
            return ctx.MakeType<TDynamicLinearExprType>(MakeType(ctx, fdp, depth + 1));
        case 9:
            return ctx.MakeType<TDictExprType>(
                MakeHashableKeyType(ctx, fdp),
                MakeType(ctx, fdp, depth + 1));
        case 10:
            return MakeTupleType(ctx, fdp, depth);
        case 11:
            return MakeStructType(ctx, fdp, depth);
        case 12:
            return ctx.MakeType<TVariantExprType>(
                fdp.ConsumeBool() ? MakeTupleType(ctx, fdp, depth, 1) : MakeStructType(ctx, fdp, depth, 1));
        case 13:
            return ctx.MakeType<TTaggedExprType>(
                MakeType(ctx, fdp, depth + 1),
                FuzzName(ctx, fdp, "tag", 0));
        case 14:
            return MakeCallableType(ctx, fdp, depth);
    }

    return MakeLeafType(ctx, fdp);
}

void CheckRoundTrip(const TTypeAnnotationNode* type, NYT::NYson::EYsonFormat format, bool extendedForm) {
    TExprContext parseCtx;
    const TString yson = NCommon::WriteTypeToYson(type, format, extendedForm);
    const auto* parsed = NCommon::ParseTypeFromYson(TStringBuf(yson), parseCtx);
    if (!parsed) {
        ythrow yexception() << "failed to parse generated type yson; original type: " << FormatType(type);
    }

    const TString originalType = FormatType(type);
    const TString parsedType = FormatType(parsed);
    if (originalType != parsedType) {
        ythrow yexception()
            << "type yson roundtrip changed formatted type: " << originalType
            << " -> " << parsedType;
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 4096) {
        return 0;
    }

    FuzzedDataProvider fdp(data, size);
    TExprContext ctx;
    const auto* type = MakeType(ctx, fdp, 0);

    for (bool extendedForm : {false, true}) {
        CheckRoundTrip(type, NYT::NYson::EYsonFormat::Binary, extendedForm);
        CheckRoundTrip(type, NYT::NYson::EYsonFormat::Text, extendedForm);
        CheckRoundTrip(type, NYT::NYson::EYsonFormat::Pretty, extendedForm);
    }

    return 0;
}
