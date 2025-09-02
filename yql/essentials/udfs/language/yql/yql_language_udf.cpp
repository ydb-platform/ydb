#include <yql/essentials/public/udf/udf_helpers.h>

#include <yql/essentials/sql/v1/context.h>
#include <yql/essentials/sql/v1/sql_translation.h>
#include <yql/essentials/sql/v1/reflect/sql_reflect.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>
#include <yql/essentials/sql/v1/format/sql_format.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <library/cpp/protobuf/util/simple_reflection.h>

using namespace NYql;
using namespace NKikimr::NUdf;
using namespace NSQLTranslation;
using namespace NSQLTranslationV1;
using namespace NSQLv1Generated;

class TRuleFreqTranslation : public TSqlTranslation
{
public:
    TRuleFreqTranslation(TContext& ctx)
        : TSqlTranslation(ctx, ctx.Settings.Mode)
    {}
};

class TRuleFreqVisitor {
public:
    TRuleFreqVisitor(TContext& ctx)
        : Translation_(ctx)
    {
        KeywordNames_ = NSQLReflect::LoadLexerGrammar().KeywordNames;
    }

    void Visit(const NProtoBuf::Message& msg) {
        const NProtoBuf::Descriptor* descr = msg.GetDescriptor();
        if (descr == TToken::GetDescriptor()) {
            const auto& token = dynamic_cast<const TToken&>(msg);
            auto upper = to_upper(token.GetValue());
            if (KeywordNames_.contains(upper)) {
                Freqs_[std::make_pair("KEYWORD", upper)] += 1;
            }
            return;
        } else if (descr == TRule_use_stmt::GetDescriptor()) {
            VisitUseStmt(dynamic_cast<const TRule_use_stmt&>(msg));
        } else if (descr == TRule_unary_casual_subexpr::GetDescriptor()) {
            VisitUnaryCasualSubexpr(dynamic_cast<const TRule_unary_casual_subexpr&>(msg));
        } else if (descr == TRule_in_unary_casual_subexpr::GetDescriptor()) {
            VisitUnaryCasualSubexpr(dynamic_cast<const TRule_in_unary_casual_subexpr&>(msg));
        } else if (descr == TRule_type_name_simple::GetDescriptor()) {
            VisitSimpleType(dynamic_cast<const TRule_type_name_simple&>(msg));
        } else if (descr == TRule_pragma_stmt::GetDescriptor()) {
            VisitPragmaStmt(dynamic_cast<const TRule_pragma_stmt&>(msg));
        } else if (descr == TRule_into_simple_table_ref::GetDescriptor()) {
            VisitInsertTableRef(dynamic_cast<const TRule_into_simple_table_ref&>(msg));
        } else if (descr == TRule_table_ref::GetDescriptor()) {
            VisitReadTableRef(dynamic_cast<const TRule_table_ref&>(msg));
        }

        TStringBuf fullName = descr->full_name();
        fullName.SkipPrefix("NSQLv1Generated.");
        for (int i = 0; i < descr->field_count(); ++i) {
            const NProtoBuf::FieldDescriptor* fd = descr->field(i);
            NProtoBuf::TConstField field(msg, fd);
            if (!field.HasValue()) {
                continue;
            }

            TStringBuf fieldFullName = fd->full_name();
            fieldFullName.SkipPrefix("NSQLv1Generated.");
            if (fieldFullName.EndsWith(".Descr")) {
                continue;
            }


            Freqs_[std::make_pair(fullName, fieldFullName)] += 1;
        }

        VisitAllFields(msg, descr);
    }

    const THashMap<std::pair<TString, TString>, ui64>& GetFreqs() const {
        return Freqs_;
    }

private:
    void VisitUseStmt(const TRule_use_stmt& msg) {
        const auto& cluster = msg.GetRule_cluster_expr2();
        if (cluster.GetBlock2().Alt_case() == TRule_cluster_expr::TBlock2::kAlt1) {
            const auto& val = cluster.GetBlock2().GetAlt1().GetRule_pure_column_or_named1();
            if (val.Alt_case() == TRule_pure_column_or_named::kAltPureColumnOrNamed2) {
                const auto& id = val.GetAlt_pure_column_or_named2().GetRule_an_id1();
                Freqs_[std::make_pair("USE", Id(id, Translation_))] += 1;
            }
        }
    }

    void VisitPragmaStmt(const TRule_pragma_stmt& msg) {
        const TString prefix = OptIdPrefixAsStr(msg.GetRule_opt_id_prefix_or_type2(), Translation_);
        const TString pragma(Id(msg.GetRule_an_id3(), Translation_));
        Freqs_[std::make_pair("PRAGMA", prefix.empty() ? pragma : (prefix + "." + pragma))] += 1;
    }

    void VisitHint(const TRule_table_hint& msg, const TString& parent) {
        switch (msg.Alt_case()) {
        case TRule_table_hint::kAltTableHint1: {
            const auto& alt = msg.GetAlt_table_hint1();
            const TString id = Id(alt.GetRule_an_id_hint1(), Translation_);
            Freqs_[std::make_pair(parent, id)] += 1;
            break;
        }
        case TRule_table_hint::kAltTableHint2: {
            const auto& alt = msg.GetAlt_table_hint2();
            Freqs_[std::make_pair(parent, alt.GetToken1().GetValue())] += 1;
            break;
        }
        case TRule_table_hint::kAltTableHint3: {
            const auto& alt = msg.GetAlt_table_hint3();
            Freqs_[std::make_pair(parent, alt.GetToken1().GetValue())] += 1;
            break;
        }
        case TRule_table_hint::kAltTableHint4: {
            const auto& alt = msg.GetAlt_table_hint4();
            Freqs_[std::make_pair(parent, alt.GetToken1().GetValue())] += 1;
            break;
        }
        case TRule_table_hint::ALT_NOT_SET:
            return;
        }
    }

    void VisitHints(const TRule_table_hints& msg, const TString& parent) {
        auto& block = msg.GetBlock2();
        switch (block.Alt_case()) {
        case TRule_table_hints::TBlock2::kAlt1: {
            VisitHint(block.GetAlt1().GetRule_table_hint1(), parent);
            break;
        }
        case TRule_table_hints::TBlock2::kAlt2: {
            VisitHint(block.GetAlt2().GetRule_table_hint2(), parent);
            for (const auto& x : block.GetAlt2().GetBlock3()) {
                VisitHint(x.GetRule_table_hint2(), parent);
            }

            break;
        }
        case TRule_table_hints::TBlock2::ALT_NOT_SET:
            return;
        }
    }

    void VisitReadTableRef(const TRule_table_ref& msg) {
        if (msg.HasBlock4()) {
            const auto& hints = msg.GetBlock4().GetRule_table_hints1();
            VisitHints(hints, "READ_HINT");
        }
    }

    void VisitInsertTableRef(const TRule_into_simple_table_ref& msg) {
        const auto& tableRef = msg.GetRule_simple_table_ref1();
        if (tableRef.HasBlock2()) {
            const auto& hints = tableRef.GetBlock2().GetRule_table_hints1();
            VisitHints(hints, "INSERT_HINT");
        }
    }

    template<typename TUnaryCasualExprRule>
    void VisitUnaryCasualSubexpr(const TUnaryCasualExprRule& msg) {
        const auto& block = msg.GetBlock1();
        TString func;
        TString module;
        switch (block.Alt_case()) {
            case TUnaryCasualExprRule::TBlock1::kAlt1: {
                const auto& alt = block.GetAlt1();
                if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                    func = Id(alt.GetRule_id_expr1(), Translation_);
                } else {
                    func = Id(alt.GetRule_id_expr_in1(), Translation_);
                }
                break;
            }
            case TUnaryCasualExprRule::TBlock1::kAlt2: {
                auto& alt = block.GetAlt2();
                if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                    if (!ParseUdf(alt.GetRule_atom_expr1(), module, func)) {
                        return;
                    }
                } else {
                    if (!ParseUdf(alt.GetRule_in_atom_expr1(), module, func)) {
                        return;
                    }
                }

                Freqs_[std::make_pair("MODULE", module)] += 1;
                auto lowerModule = to_lower(module);
                if (lowerModule.Contains("javascript") || lowerModule.Contains("python")) {
                    return;
                }

                Freqs_[std::make_pair("MODULE_FUNC", module + "::" + func)] += 1;
                return;
            }
            case TUnaryCasualExprRule::TBlock1::ALT_NOT_SET:
                Y_ABORT("You should change implementation according to grammar changes");
        }

        const auto& suffix = msg.GetRule_unary_subexpr_suffix2();
        const bool suffixIsEmpty = suffix.GetBlock1().empty() && !suffix.HasBlock2();
        if (suffixIsEmpty) {
            if (auto simpleType = LookupSimpleType(func, true, false); simpleType) {
                Freqs_[std::make_pair("TYPE", func)] += 1;
            }
        }

        for (auto& _b : suffix.GetBlock1()) {
            const auto& b = _b.GetBlock1();
            switch (b.Alt_case()) {
                case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt1: {
                    // key_expr
                    return;
                }
                case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt2: {
                    // invoke_expr
                    Freqs_[std::make_pair("FUNC", func)] += 1;
                    return;
                }
                case TRule_unary_subexpr_suffix::TBlock1::TBlock1::kAlt3: {
                    // dot
                    return;
                }
                case TRule_unary_subexpr_suffix::TBlock1::TBlock1::ALT_NOT_SET:
                    Y_ABORT("You should change implementation according to grammar changes");
            }
        }
    }

    void VisitSimpleType(const TRule_type_name_simple& msg) {
        Freqs_[std::make_pair("TYPE", Id(msg.GetRule_an_id_pure1(), Translation_))] += 1;
    }

    bool ParseUdf(const TRule_atom_expr& msg, TString& module, TString& func) {
        if (msg.Alt_case() != TRule_atom_expr::kAltAtomExpr7) {
            return false;
        }

        const auto& alt = msg.GetAlt_atom_expr7();
        module = Id(alt.GetRule_an_id_or_type1(), Translation_);
        switch (alt.GetBlock3().Alt_case()) {
        case TRule_atom_expr::TAlt7::TBlock3::kAlt1:
            func = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), Translation_);
            break;
        case TRule_atom_expr::TAlt7::TBlock3::kAlt2: {
            return false;
        }
        case TRule_atom_expr::TAlt7::TBlock3::ALT_NOT_SET:
            Y_ABORT("Unsigned number: you should change implementation according to grammar changes");
        }

        return true;
    }

    bool ParseUdf(const TRule_in_atom_expr& msg, TString& module, TString& func) {
        if (msg.Alt_case() != TRule_in_atom_expr::kAltInAtomExpr6) {
            return false;
        }

        const auto& alt = msg.GetAlt_in_atom_expr6();
        module = Id(alt.GetRule_an_id_or_type1(), Translation_);
        switch (alt.GetBlock3().Alt_case()) {
        case TRule_in_atom_expr::TAlt6::TBlock3::kAlt1:
            func = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), Translation_);
            break;
        case TRule_in_atom_expr::TAlt6::TBlock3::kAlt2: {
            return false;
        }
        case TRule_in_atom_expr::TAlt6::TBlock3::ALT_NOT_SET:
            Y_ABORT("You should change implementation according to grammar changes");
        }

        return true;
    }

    void VisitAllFields(const NProtoBuf::Message& msg, const NProtoBuf::Descriptor* descr) {
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

    THashMap<std::pair<TString, TString>, ui64> Freqs_;
    TRuleFreqTranslation Translation_;
    THashSet<TString> KeywordNames_;
};

SIMPLE_UDF(TObfuscate, TOptional<char*>(TAutoMap<char*>)) {
    using namespace NSQLFormat;
    try {
        const auto sqlRef = args[0].AsStringRef();
        TString formattedQuery;
        NYql::TIssues issues;
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        if (!MakeSqlFormatter(lexers, parsers, settings)->Format(TString(sqlRef), formattedQuery, issues, EFormatMode::Obfuscate)) {
            return {};
        }

        return valueBuilder->NewString(formattedQuery);
    } catch (const yexception&) {
        return {};
    }
}

using TRuleFreqResult = TListType<TTuple<char*, char*, ui64>>;

SIMPLE_UDF(TRuleFreq, TOptional<TRuleFreqResult>(TAutoMap<char*>)) {
    try {
        const TString query(args[0].AsStringRef());
        NYql::TIssues issues;
        google::protobuf::Arena arena;
        NSQLTranslation::TTranslationSettings settings;
        settings.Arena = &arena;
        if (!ParseTranslationSettings(query, settings, issues)) {
            return {};
        }

        NSQLTranslationV1::TLexers lexers;
        lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
        auto lexer = NSQLTranslationV1::MakeLexer(lexers, settings.AnsiLexer, true);
        auto onNextToken = [&](NSQLTranslation::TParsedToken&& token) {
            Y_UNUSED(token);
        };

        if (!lexer->Tokenize(query, "", onNextToken, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS)) {
            return {};
        }

        NSQLTranslationV1::TParsers parsers;
        parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
        parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory();
        auto msg = NSQLTranslationV1::SqlAST(parsers, query, "", issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS,
            settings.AnsiLexer, true, &arena);
        if (!msg) {
            return {};
        }

        TContext ctx(lexers, parsers, settings, {}, issues, query);
        TRuleFreqVisitor visitor(ctx);
        visitor.Visit(*msg);

        auto listBuilder = valueBuilder->NewListBuilder();
        for (const auto& [key, f] : visitor.GetFreqs()) {
            TUnboxedValue* items;
            auto tuple = valueBuilder->NewArray(3, items);
            items[0] = valueBuilder->NewString(key.first);
            items[1] = valueBuilder->NewString(key.second);
            items[2] = TUnboxedValuePod(f);
            listBuilder->Add(std::move(tuple));
        }

        return listBuilder->Build();
    } catch (const yexception&) {
        return {};
    }
}

SIMPLE_MODULE(TYqlLangModule,
    TObfuscate,
    TRuleFreq
);

REGISTER_MODULES(TYqlLangModule);

