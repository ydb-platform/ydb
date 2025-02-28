#include "translator.h"

namespace NSQLTranslation {

namespace {

class TDummyTranslator : public ITranslator {
public:
    TDummyTranslator(const TString& name)
        : Name_(name)
    {}

    NSQLTranslation::ILexer::TPtr MakeLexer(const NSQLTranslation::TTranslationSettings& settings) final {
        Y_UNUSED(settings);
        ThrowNotSupported();
    }

    NYql::TAstParseResult TextToAst(const TString& query, const NSQLTranslation::TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, NYql::TStmtParseInfo* stmtParseInfo) final {
        Y_UNUSED(query);
        Y_UNUSED(settings);
        Y_UNUSED(warningRules);
        Y_UNUSED(stmtParseInfo);
        ThrowNotSupported();
    }

    google::protobuf::Message* TextToMessage(const TString& query, const TString& queryName,
        NYql::TIssues& issues, size_t maxErrors, const TTranslationSettings& settings) final {
        Y_UNUSED(query);
        Y_UNUSED(queryName);
        Y_UNUSED(issues);
        Y_UNUSED(maxErrors);
        Y_UNUSED(settings);
        ThrowNotSupported();
    }

    NYql::TAstParseResult TextAndMessageToAst(const TString& query, const google::protobuf::Message& protoAst,
        const TSQLHints& hints, const TTranslationSettings& settings) final {
        Y_UNUSED(query);
        Y_UNUSED(protoAst);
        Y_UNUSED(hints);
        Y_UNUSED(settings);
        ThrowNotSupported();
    }

    TVector<NYql::TAstParseResult> TextToManyAst(const TString& query, const TTranslationSettings& settings,
        NYql::TWarningRules* warningRules, TVector<NYql::TStmtParseInfo>* stmtParseInfo) final {
        Y_UNUSED(query);
        Y_UNUSED(settings);
        Y_UNUSED(warningRules);
        Y_UNUSED(stmtParseInfo);
        ThrowNotSupported();
    }

private:
    [[noreturn]] void ThrowNotSupported() {
        throw yexception() << "Translator '" << Name_ << "' is not supported";
    }

private:
    const TString Name_;
};

}

TTranslatorPtr MakeDummyTranslator(const TString& name) {
    return MakeIntrusive<TDummyTranslator>(name);
}

}  // namespace NSQLTranslation
