#include "util.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/dom/node.h>

namespace NYql::NS3Util {

namespace {

inline char d2x(unsigned x) {
    return (char)((x < 10) ? ('0' + x) : ('A' + x - 10));
}

char* UrlEscape(char* to, const char* from) {
    while (*from) {
        if (*from == '%' || *from == '#' || *from == '?' || (unsigned char)*from <= ' ' || (unsigned char)*from > '~') {
            *to++ = '%';
            *to++ = d2x((unsigned char)*from >> 4);
            *to++ = d2x((unsigned char)*from & 0xF);
        } else {
            *to++ = *from;
        }
        ++from;
    }

    *to = 0;

    return to;
}

struct TTypeError {
    const TTypeAnnotationNode* BadType = nullptr;
    TString Error = "unsupported type";
};

std::optional<TTypeError> ValidateJsonListIoType(const TTypeAnnotationNode* type, std::function<std::optional<TTypeError>(const TTypeAnnotationNode*)> defaultHandler) {
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Null:
        case ETypeAnnotationKind::Void:
        case ETypeAnnotationKind::EmptyList:
        case ETypeAnnotationKind::EmptyDict: {
            return std::nullopt;
        }
        case ETypeAnnotationKind::Data: {
            const auto dataSlot = type->Cast<TDataExprType>()->GetSlot();
            if (IsDataTypeDateOrTzDateOrInterval(dataSlot) || IsDataTypeDecimal(dataSlot) || dataSlot == NUdf::EDataSlot::Uuid || dataSlot == NUdf::EDataSlot::JsonDocument) {
                return TTypeError{type};
            }
            return std::nullopt;
        }
        case ETypeAnnotationKind::Optional: {
            return ValidateJsonListIoType(type->Cast<TOptionalExprType>()->GetItemType(), defaultHandler);
        }
        case ETypeAnnotationKind::List: {
            return ValidateJsonListIoType(type->Cast<TListExprType>()->GetItemType(), defaultHandler);
        }
        case ETypeAnnotationKind::Dict: {
            const auto* dictType = type->Cast<TDictExprType>();
            const auto* keyType = dictType->GetKeyType();
            if (keyType->GetKind() != ETypeAnnotationKind::Data) {
                return TTypeError{dictType, TStringBuilder() <<"unsupported dict key type, it should be Data type, but got: " << FormatType(keyType)};
            }
            if (const auto datSlot = keyType->Cast<TDataExprType>()->GetSlot(); datSlot != NUdf::EDataSlot::String && datSlot != NUdf::EDataSlot::Utf8) {
                return TTypeError{dictType, TStringBuilder() <<"unsupported dict key type, it should be String or Utf8, but got: " << FormatType(keyType)};
            }
            return ValidateJsonListIoType(dictType->GetPayloadType(), defaultHandler);
        }
        case ETypeAnnotationKind::Tuple: {
            for (const auto* item : type->Cast<TTupleExprType>()->GetItems()) {
                if (const auto error = ValidateJsonListIoType(item, defaultHandler)) {
                    return error;
                }
            }
            return std::nullopt;
        }
        case ETypeAnnotationKind::Struct: {
            for (const auto* item : type->Cast<TStructExprType>()->GetItems()) {
                if (const auto error = ValidateJsonListIoType(item->GetItemType(), defaultHandler)) {
                    return error;
                }
            }
            return std::nullopt;
        }
        case ETypeAnnotationKind::Resource: {
            if (type->Cast<TResourceExprType>()->GetTag() != NDom::NodeResourceName) {
                return TTypeError{type, TStringBuilder() << "unsupported resource type, allowed only: " << NDom::NodeResourceName};
            }
            return std::nullopt;
        }
        default: {
            break;
        }
    }
    return defaultHandler(type);
}

// Type compatible with Yson2.ConvertTo udf
std::optional<TTypeError> ValidateJsonListInputType(const TTypeAnnotationNode* type) {
    return ValidateJsonListIoType(type, [](const TTypeAnnotationNode* type) {
        return TTypeError{type};
    });
}

// Type compatible with Yson2.From udf
std::optional<TTypeError> DefaultJsonListOutputTypeHandler(const TTypeAnnotationNode* type) {
    if (type->GetKind() == ETypeAnnotationKind::Variant) {
        return ValidateJsonListIoType(type->Cast<TVariantExprType>()->GetUnderlyingType(), &DefaultJsonListOutputTypeHandler);
    }
    return TTypeError{type};
}

std::optional<TTypeError> ValidateJsonListOutputType(const TTypeAnnotationNode* type) {
    return ValidateJsonListIoType(type, &DefaultJsonListOutputTypeHandler);
}

std::optional<TTypeError> ValidateIoDataType(const TDataExprType* type, std::vector<EDataSlot> extraTypes = {}) {
    const auto dataSlot = type->GetSlot();
    if (IsDataTypeBigDate(dataSlot)) {
        return TTypeError{type, "big dates is not supported"};
    }
    if (IsDataTypeNumeric(dataSlot) || IsDataTypeDateOrTzDate(dataSlot)) {
        return std::nullopt;
    }
    if (IsIn({EDataSlot::Bool, EDataSlot::String, EDataSlot::Utf8, EDataSlot::Json, EDataSlot::Uuid}, dataSlot) || IsIn(extraTypes, dataSlot)) {
        return std::nullopt;
    }
    return TTypeError{type};
}

// Data type compatible with ClickHouseClient.ParseBlocks udf and S3 coro read actor
std::optional<TTypeError> ValidateCoroReadActorDataType(const TDataExprType* type) {
    return ValidateIoDataType(type, {EDataSlot::Interval, EDataSlot::Decimal});
}

// Data type compatible with ClickHouseClient.ParseFormat and ClickHouseClient.SerializeFormat udfs
std::optional<TTypeError> ValidateClickHouseUdfDataType(const TDataExprType* type) {
    return ValidateIoDataType(type, {});
}

// Type compatible with ClickHouseClient.ParseBlocks, ClickHouseClient.ParseFormat, ClickHouseClient.SerializeFormat udfs
std::optional<TTypeError> ValidateGenericIoType(const TTypeAnnotationNode* type, std::function<std::optional<TTypeError>(const TDataExprType*)> dataTypeChecker, bool underOptional = false) {
    switch (type->GetKind()) {
        case ETypeAnnotationKind::Optional: {
            if (underOptional) {
                return TTypeError{type, "double optional types are not supported"};
            }
            return ValidateGenericIoType(type->Cast<TOptionalExprType>()->GetItemType(), dataTypeChecker, true);
        }
        case ETypeAnnotationKind::List: {
            if (underOptional) {
                return TTypeError{type, "list under optional is not supported"};
            }
            return ValidateGenericIoType(type->Cast<TListExprType>()->GetItemType(), dataTypeChecker);
        }
        case ETypeAnnotationKind::Tuple: {
            if (underOptional) {
                return TTypeError{type, "tuple under optional is not supported"};
            }
            for (const auto* item : type->Cast<TTupleExprType>()->GetItems()) {
                if (const auto error = ValidateGenericIoType(item, dataTypeChecker)) {
                    return error;
                }
            }
            return std::nullopt;
        }
        case ETypeAnnotationKind::Data: {
            return dataTypeChecker(type->Cast<TDataExprType>());
        }
        case ETypeAnnotationKind::Pg: {
            return std::nullopt;
        }
        default: {
            break;
        }
    }
    return TTypeError{type};
}

bool ValidateIoSchema(TPositionHandle pos, const TStructExprType* schemaStructRowType, const TString& info, TExprContext& ctx, std::function<std::optional<TTypeError>(const TTypeAnnotationNode*)> typeChecker) {
    bool hasErrors = false;
    for (const auto* item : schemaStructRowType->GetItems()) {
        if (const auto error = typeChecker(item->GetItemType())) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Field '" << item->GetName() << "' has incompatible with " << info << " type: " << FormatType(error->BadType) << " (" << error->Error << ")"));
            hasErrors = true;
        }
    }
    return !hasErrors;
}

}

TIssues AddParentIssue(const TString& prefix, TIssues&& issues) {
    if (!issues) {
        return TIssues{};
    }
    TIssue result(prefix);
    for (auto& issue: issues) {
        result.AddSubIssue(MakeIntrusive<TIssue>(issue));
    }
    return TIssues{result};
}

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues) {
    return AddParentIssue(TString(prefix), std::move(issues));
}

TString UrlEscapeRet(const TStringBuf from) {
    TString to;
    to.ReserveAndResize(CgiEscapeBufLen(from.size()));
    to.resize(UrlEscape(to.begin(), from.begin()) - to.data());
    return to;
}

bool ValidateS3ReadSchema(TPositionHandle pos, std::string_view format, const TStructExprType* schemaStructRowType, bool enableCoroReadActor, TExprContext& ctx) {
    if (format == "raw"sv || format == "parquet"sv) {
        return true;
    }

    if (format == "json_list"sv) {
        return ValidateIoSchema(pos, schemaStructRowType, "S3 json_list input format", ctx, &ValidateJsonListInputType);
    }

    return ValidateIoSchema(pos, schemaStructRowType, TStringBuilder() << "S3 " << format << " input format", ctx, [enableCoroReadActor](const TTypeAnnotationNode* type) {
        return ValidateGenericIoType(type, enableCoroReadActor ? &ValidateCoroReadActorDataType : &ValidateClickHouseUdfDataType);
    });
}

bool ValidateS3WriteSchema(TPositionHandle pos, std::string_view format, const TStructExprType* schemaStructRowType, TExprContext& ctx) {
    if (format == "raw"sv) {
        if (const auto size = schemaStructRowType->GetSize(); size != 1) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Only one column in schema supported in raw format (you have " << size << " fields)"));
            return false;
        }

        const auto* rowType = schemaStructRowType->GetItems().front()->GetItemType();
        if (rowType->GetKind() != ETypeAnnotationKind::Data) {
            ctx.AddError(TIssue(ctx.GetPosition(pos), TStringBuilder() << "Only a column with a primitive type is allowed for the raw format (you have field with type " << *rowType << ")"));
            return false;
        }

        return true;
    }

    if (format == "json_list"sv) {
        return ValidateIoSchema(pos, schemaStructRowType, "S3 json_list output format", ctx, &ValidateJsonListOutputType);
    }

    return ValidateIoSchema(pos, schemaStructRowType, TStringBuilder() << "S3 " << format << " output format", ctx, [](const TTypeAnnotationNode* type) {
        return ValidateGenericIoType(type, &ValidateClickHouseUdfDataType);
    });
}

TUrlBuilder::TUrlBuilder(const TString& uri)
    : MainUri(uri)
{}

TUrlBuilder& TUrlBuilder::AddUrlParam(const TString& name, const TString& value) {
    Params.emplace_back(name, value);
    return *this;
}

TString TUrlBuilder::Build() const {
    if (Params.empty()) {
        return MainUri;
    }

    TStringBuilder result;
    result << MainUri << "?";

    TStringBuf separator = ""sv;
    for (const auto& p : Params) {
        result << separator << p.Name;
        if (auto value = p.Value) {
            Quote(value, "");
            result << "=" << value;
        }
        separator = "&"sv;
    }

    return std::move(result);
}

}
