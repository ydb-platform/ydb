#include "util.h"

#include <library/cpp/string_utils/quote/quote.h>


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

}
