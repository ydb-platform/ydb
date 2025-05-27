#include "lexer.h"
#include <yql/essentials/public/issue/yql_issue.h>

#include <util/string/builder.h>

namespace NSQLTranslation {

namespace {

class TDummyLexer : public ILexer {
public:
    TDummyLexer(const TString& name)
        : Name_(name)
    {}

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& onNextToken, NYql::TIssues& issues, size_t maxErrors) final {
        Y_UNUSED(query);
        Y_UNUSED(queryName);
        Y_UNUSED(onNextToken);
        Y_UNUSED(maxErrors);
        issues.AddIssue(NYql::TIssue({}, TStringBuilder() << "Lexer " << Name_ << " is not supported"));
        return false;
    }

private:
    const TString Name_;
};

class TDummyFactory: public ILexerFactory {
public:
    TDummyFactory(const TString& name)
        : Name_(name)
    {}

    ILexer::TPtr MakeLexer() const final {
        return MakeHolder<TDummyLexer>(Name_);
    }

private:
    const TString Name_;
};

}

TLexerFactoryPtr MakeDummyLexerFactory(const TString& name) {
    return MakeIntrusive<TDummyFactory>(name);
}

}
