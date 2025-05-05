#pragma once

#include "formatters_common.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <util/stream/str.h>

namespace NKikimr::NSysView {

struct TViewQuerySplit {
    TString ContextRecreation;
    TString Select;

    TViewQuerySplit() = default;
    TViewQuerySplit(const TVector<TString>& statements);
};

bool SplitViewQuery(const TString& query, TViewQuerySplit& split, NYql::TIssues& issues);

class TCreateViewFormatter {
public:
    TFormatResult Format(const TString& viewRelativePath, const TString& viewAbsolutePath, const NKikimrSchemeOp::TViewDescription& viewDesc);

private:
    TStringStream Stream;
};

}
