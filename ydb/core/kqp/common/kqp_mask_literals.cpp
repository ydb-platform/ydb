#include "kqp_mask_literals.h"

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>

#include <library/cpp/protobuf/util/simple_reflection.h>

#include <util/generic/hash.h>
#include <util/generic/maybe.h>
#include <util/string/builder.h>

#include <functional>

namespace NKikimr::NKqp {

namespace {

using namespace NSQLv1Generated;

class TStringMaskVisitor;
using TVisitFunctor = std::function<void(TStringMaskVisitor&, const NProtoBuf::Message&)>;
using TDispatchMap = THashMap<const NProtoBuf::Descriptor*, TVisitFunctor>;

template <typename T>
TVisitFunctor MakeFunctor(void (TStringMaskVisitor::*memberPtr)(const T&)) {
    return [memberPtr](TStringMaskVisitor& visitor, const NProtoBuf::Message& rawMsg) {
        (visitor.*memberPtr)(static_cast<const T&>(rawMsg));
    };
}

// Replaces string literals in a query with '***removed***' while keeping numbers,
// identifiers, and query shape intact. Used by request logging so that sensitive
// data (passwords, secrets, user payloads) never ends up in logs verbatim.
//
// Masking is driven by the grammar, not by scanning token text: YQL accepts
// several string quotings ('x', "x", @@x@@, typed literals) and a token-level
// check would miss some of them. Each handler below matches a grammar rule
// where a string may carry sensitive data and sets NextToken_, which VisitToken
// then emits instead of the original token value.
class TStringMaskVisitor {
public:
    explicit TStringMaskVisitor(const TDispatchMap& dispatch)
        : Dispatch_(dispatch)
    {
    }

    TString Process(const NProtoBuf::Message& msg) {
        Visit(msg);
        return Sb_;
    }

    void VisitToken(const TToken& token) {
        auto str = token.GetValue();
        if (str == "<EOF>") {
            return;
        }

        if (!First_) {
            Sb_ << ' ';
        } else {
            First_ = false;
        }

        if (NextToken_) {
            Sb_ << *NextToken_;
            NextToken_ = Nothing();
        } else {
            Sb_ << str;
        }
    }

    // General path for user payloads: SELECT 'foo', WHERE col = "bar",
    // INSERT ... VALUES (@@multi@@). Covers all string-quoting forms uniformly.
    void VisitLiteralValue(const TRule_literal_value& msg) {
        if (msg.Alt_case() == TRule_literal_value::kAltLiteralValue3) {
            NextToken_ = "'***removed***'";
        }
        VisitAllFields(TRule_literal_value::GetDescriptor(), msg);
    }

    // pragma_value is a separate grammar rule, not a literal_value, so PRAGMA
    // foo = 'secret' would slip past VisitLiteralValue without its own handler.
    void VisitPragmaValue(const TRule_pragma_value& msg) {
        if (msg.Alt_case() == TRule_pragma_value::kAltPragmaValue3) {
            NextToken_ = "'***removed***'";
        }
        VisitAllFields(TRule_pragma_value::GetDescriptor(), msg);
    }

    // CREATE/ALTER USER ... PASSWORD '...'. The password is consumed by
    // password_value directly via STRING_VALUE, bypassing literal_value.
    // Both the string and NULL alternatives map to Token1 in the generated
    // proto, so we mask unconditionally.
    void VisitPasswordValue(const TRule_password_value& msg) {
        NextToken_ = "'***removed***'";
        VisitAllFields(TRule_password_value::GetDescriptor(), msg);
    }

    // CREATE/ALTER USER ... HASH '...'. The hash is secret material for
    // offline attack and is the alternative form of PASSWORD, so we treat
    // it the same. Token1 is the HASH keyword, Token2 is the string.
    void VisitHashOption(const TRule_hash_option& msg) {
        Visit(msg.GetToken1());
        NextToken_ = "'***removed***'";
        Visit(msg.GetToken2());
    }

    // CREATE OBJECT ... (TYPE SECRET) WITH (value = "..."); also used to pass
    // external datasource credentials (AWS keys, PG/MySQL/CH passwords, ...).
    // The string comes in as STRING_VALUE inside object_feature_value, not
    // through literal_value.
    void VisitObjectFeatureValue(const TRule_object_feature_value& msg) {
        if (msg.Alt_case() == TRule_object_feature_value::kAltObjectFeatureValue3) {
            NextToken_ = "'***removed***'";
        }
        VisitAllFields(TRule_object_feature_value::GetDescriptor(), msg);
    }

private:
    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        auto funcPtr = Dispatch_.FindPtr(descr);
        if (funcPtr) {
            (*funcPtr)(*this, msg);
        } else {
            VisitAllFields(descr, msg);
        }
    }

    void VisitAllFields(const NProtoBuf::Descriptor* descr, const NProtoBuf::Message& msg) {
        for (int i = 0; i < descr->field_count(); ++i) {
            const NProtoBuf::FieldDescriptor* fd = descr->field(i);
            NProtoBuf::TConstField field(msg, fd);
            if (field.IsMessage()) {
                for (size_t j = 0; j < field.Size(); ++j) {
                    Visit(*field.Get<NProtoBuf::Message>(j));
                }
            }
        }
    }

    const TDispatchMap& Dispatch_;
    TStringBuilder Sb_;
    bool First_ = true;
    TMaybe<TString> NextToken_;
};

const TDispatchMap& GetDispatch() {
    static const TDispatchMap dispatch = {
        {TToken::GetDescriptor(), MakeFunctor(&TStringMaskVisitor::VisitToken)},
        {TRule_literal_value::GetDescriptor(), MakeFunctor(&TStringMaskVisitor::VisitLiteralValue)},
        {TRule_pragma_value::GetDescriptor(), MakeFunctor(&TStringMaskVisitor::VisitPragmaValue)},
        {TRule_password_value::GetDescriptor(), MakeFunctor(&TStringMaskVisitor::VisitPasswordValue)},
        {TRule_hash_option::GetDescriptor(), MakeFunctor(&TStringMaskVisitor::VisitHashOption)},
        {TRule_object_feature_value::GetDescriptor(), MakeFunctor(&TStringMaskVisitor::VisitObjectFeatureValue)},
    };
    return dispatch;
}

NSQLTranslationV1::TLexers MakeLexers() {
    return {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory(),
    };
}

NSQLTranslationV1::TParsers MakeParsers() {
    return {
        .Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(),
        .Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(),
    };
}

} // anonymous namespace

TString MaskSensitiveLiterals(const TString& query) {
    static NSQLTranslationV1::TLexers lexers = MakeLexers();
    static NSQLTranslationV1::TParsers parsers = MakeParsers();

    google::protobuf::Arena arena;
    NYql::TIssues issues;
    auto* ast = NSQLTranslationV1::SqlAST(
        parsers, query, "", issues,
        NSQLTranslation::SQL_MAX_PARSER_ERRORS,
        /* ansiLexer = */ false,
        &arena);
    if (!ast) {
        return query;
    }

    TStringMaskVisitor visitor(GetDispatch());
    return visitor.Process(*ast);
}

} // namespace NKikimr::NKqp
