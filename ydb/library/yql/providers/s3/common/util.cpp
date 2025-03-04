#include "util.h"

#include <library/cpp/string_utils/quote/quote.h>
#include <parquet/exception.h>
#include <ydb/library/formats/arrow/size_calcer.h>

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

}

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues) {
    if (!issues) {
        return TIssues{};
    }
    TIssue result(prefix);
    for (auto& issue: issues) {
        result.AddSubIssue(MakeIntrusive<TIssue>(issue));
    }
    return TIssues{result};
}

TString UrlEscapeRet(const TStringBuf from) {
    TString to;
    to.ReserveAndResize(CgiEscapeBufLen(from.size()));
    to.resize(UrlEscape(to.begin(), from.begin()) - to.data());
    return to;
}

bool ValidateS3ReadWriteSchema(const TStructExprType* schemaStructRowType, TExprContext& ctx) {
    for (const TItemExprType* item : schemaStructRowType->GetItems()) {
        const TTypeAnnotationNode* rowType = item->GetItemType();
        if (rowType->GetKind() == ETypeAnnotationKind::Optional) {
            rowType = rowType->Cast<TOptionalExprType>()->GetItemType();
        }

        if (rowType->GetKind() == ETypeAnnotationKind::Optional) {
            ctx.AddError(TIssue(TStringBuilder() << "Double optional types are not supported (you have '"
                << item->GetName() << " " << FormatType(item->GetItemType()) << "' field)"));
            return false;
        }
    }
    return true;
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

TArrowBlockSplitter::TArrowBlockSplitter(ui32 chunkSizeLimit, ui32 rowMetaSize)
    : ChunkSizeLimit(chunkSizeLimit)
    , RowMetaSize(rowMetaSize)
{}

void TArrowBlockSplitter::SplitRecordBatch(std::shared_ptr<arrow::RecordBatch> batch, ui64 firstRowId, std::vector<std::shared_ptr<arrow::RecordBatch>>& result) {
    if (!ChunkSizeLimit || CheckBatchSize(batch)) {
        result.emplace_back(std::move(batch));
        return;
    }

    const auto estimatedSize = NKikimr::NArrow::GetBatchDataSize(batch) / ChunkSizeLimit;
    result.reserve(estimatedSize);

    SplitStack.clear();
    SplitStack.emplace_back(std::move(batch));
    while (!SplitStack.empty()) {
        batch = std::move(SplitStack.back());
        SplitStack.pop_back();

        while (!CheckBatchSize(batch)) {
            if (batch->num_rows() <= 1) {
                throw parquet::ParquetException("Row ", firstRowId + 1, " size is ", NKikimr::NArrow::GetBatchDataSize(batch), ", that is larger than allowed limit ", ChunkSizeLimit);
            }
            const auto splitIndex = batch->num_rows() / 2;
            SplitStack.emplace_back(batch->Slice(splitIndex, batch->num_rows() - splitIndex));
            batch = batch->Slice(0, splitIndex);
        }

        firstRowId += batch->num_rows();
        result.emplace_back(std::move(batch));
    }
}

bool TArrowBlockSplitter::CheckBatchSize(const std::shared_ptr<arrow::RecordBatch>& batch) const {
    const ui64 batchSize = NKikimr::NArrow::GetBatchDataSize(batch) + batch->num_rows() * RowMetaSize;
    return batchSize <= ChunkSizeLimit;
}

}
