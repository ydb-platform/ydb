#pragma once

#include <arrow/record_batch.h>
#include <util/string/builder.h>
#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NYql::NS3Util {

TIssues AddParentIssue(const TStringBuilder& prefix, TIssues&& issues);

// Like UrlEscape with forceEscape = true
// from ydb/library/cpp/string_utils/quote/quote.h, but also escapes:
// '#', '?'
TString UrlEscapeRet(const TStringBuf from);

bool ValidateS3ReadWriteSchema(const TStructExprType* schemaStructRowType, TExprContext& ctx);

class TUrlBuilder {
    struct TParam {
        TString Name;
        TString Value;
    };

public:
    explicit TUrlBuilder(const TString& uri);

    TUrlBuilder& AddUrlParam(const TString& name, const TString& value = "");

    TString Build() const;

private:
    std::vector<TParam> Params;
    TString MainUri;
};

class TArrowBlockSplitter {
public:
    TArrowBlockSplitter(ui32 chunkSizeLimit, ui32 rowMetaSize);

    void SplitRecordBatch(std::shared_ptr<arrow::RecordBatch> batch, ui64 firstRowId, std::vector<std::shared_ptr<arrow::RecordBatch>>& result);

private:
    bool CheckBatchSize(const std::shared_ptr<arrow::RecordBatch>& batch) const;

private:
    const ui32 ChunkSizeLimit;
    const ui32 RowMetaSize;
    std::vector<std::shared_ptr<arrow::RecordBatch>> SplitStack;
};

}
