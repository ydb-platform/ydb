#include "yql_s3_provider_impl.h"

#include <ydb/library/yql/providers/s3/actors/yql_arrow_column_converters.h>

#include <yql/essentials/providers/common/schema/expr/yql_expr_schema.h>

namespace NYql {

TExprNode::TPtr ExtractFormat(TExprNode::TListType& settings) {
    for (auto it = settings.cbegin(); settings.cend() != it; ++it) {
        if (const auto item = *it; item->Head().IsAtom("format")) {
            settings.erase(it);
            return item->TailPtr();
        }
    }

    return {};
}

bool UseBlocksSink(TStringBuf format, const TExprNode::TListType& keys, const TStructExprType* outputType, TS3Configuration::TPtr configuration, TString& error) {
    const auto useblockSink = configuration->UseBlocksSink.Get();
    if (useblockSink && !*useblockSink) {
        return false;
    }

    if (!keys.empty()) {
        if (useblockSink) {
            error = "Block sink is not supported for partitioned output";
        }
        return false;
    }

    if (format != "parquet") {
        if (useblockSink) {
            error = "Block sink supported only for parquet output format";
        }
        return false;
    }

    for (const auto* item : outputType->GetItems()) {
        const auto* unpackedType = item->GetItemType();
        if (unpackedType->GetKind() == ETypeAnnotationKind::Optional) {
            unpackedType = unpackedType->Cast<TOptionalExprType>()->GetItemType();
        }
        if (unpackedType->GetKind() != ETypeAnnotationKind::Data) {
            if (useblockSink) {
                error = TStringBuilder() << "Field '" << item->GetName() << "' has not supported for block sink type " << FormatType(item->GetItemType()) << ", allowed only data or optional of data types";
            }
            return false;
        }
        if (std::shared_ptr<arrow::DataType> arrowType; !NDq::S3ConvertArrowOutputType(unpackedType->Cast<TDataExprType>()->GetSlot(), arrowType)) {
            if (useblockSink) {
                error = TStringBuilder() << "Field '" << item->GetName() << "' has not supported for block sink data type " << FormatType(unpackedType);
            }
            return false;
        }
    }

    return true;
}

}
