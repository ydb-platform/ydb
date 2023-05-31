#include "sql.h"

#include "context.h"
#include "node.h"
#include "sql_call_param.h"
#include "object_processing.h"
#include "ydb/library/yql/ast/yql_ast.h"
#include <ydb/library/yql/parser/lexer_common/hints.h>
#include <ydb/library/yql/parser/proto_ast/collect_issues/collect_issues.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1/SQLv1Parser.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_ansi/SQLv1Parser.h>

#include <ydb/library/yql/sql/v1/lexer/lexer.h>
#include <ydb/library/yql/sql/v1/proto_parser/proto_parser.h>

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/parser/proto_ast/gen/v1_proto/SQLv1Parser.pb.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/sql/settings/partitioning.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/core/yql_atom_enums.h>

#include <library/cpp/charset/ci_string.h>
#include <library/cpp/json/json_reader.h>

#include <ydb/library/yql/utils/utf8.h>
#include <ydb/library/yql/utils/yql_paths.h>

#include <google/protobuf/repeated_field.h>

#include <util/charset/wide.h>
#include <util/generic/array_ref.h>
#include <util/generic/scope.h>
#include <util/generic/set.h>
#include <util/generic/ylimits.h>
#include <util/string/ascii.h>
#include <util/string/cast.h>
#include <util/string/reverse.h>
#include <util/string/split.h>
#include <util/string/hex.h>
#include <util/string/join.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

using namespace NYql;

namespace NSQLTranslationV1 {

TNodePtr BuildSqlCall(TContext& ctx, TPosition pos, const TString& module, const TString& name, const TVector<TNodePtr>& args,
    TNodePtr positionalArgs, TNodePtr namedArgs, TNodePtr customUserType, const TDeferredAtom& typeConfig, TNodePtr runConfig);

using NALPDefault::SQLv1LexerTokens;

using namespace NSQLv1Generated;

static TPosition GetPos(const TToken& token) {
    return TPosition(token.GetColumn(), token.GetLine());
}

template <typename TToken>
TIdentifier GetIdentifier(TTranslation& ctx, const TToken& node) {
    auto token = node.GetToken1();
    return TIdentifier(TPosition(token.GetColumn(), token.GetLine()), ctx.Identifier(token));
}

inline TIdentifier GetKeywordId(TTranslation& ctx, const TRule_keyword& node) {
    // keyword:
    //    keyword_compat
    //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_keyword::kAltKeyword1:
            return GetIdentifier(ctx, node.GetAlt_keyword1().GetRule_keyword_compat1());
        case TRule_keyword::kAltKeyword2:
            return GetIdentifier(ctx, node.GetAlt_keyword2().GetRule_keyword_expr_uncompat1());
        case TRule_keyword::kAltKeyword3:
            return GetIdentifier(ctx, node.GetAlt_keyword3().GetRule_keyword_table_uncompat1());
        case TRule_keyword::kAltKeyword4:
            return GetIdentifier(ctx, node.GetAlt_keyword4().GetRule_keyword_select_uncompat1());
        case TRule_keyword::kAltKeyword5:
            return GetIdentifier(ctx, node.GetAlt_keyword5().GetRule_keyword_alter_uncompat1());
        case TRule_keyword::kAltKeyword6:
            return GetIdentifier(ctx, node.GetAlt_keyword6().GetRule_keyword_in_uncompat1());
        case TRule_keyword::kAltKeyword7:
            return GetIdentifier(ctx, node.GetAlt_keyword7().GetRule_keyword_window_uncompat1());
        case TRule_keyword::kAltKeyword8:
            return GetIdentifier(ctx, node.GetAlt_keyword8().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

inline TString GetKeyword(TTranslation& ctx, const TRule_keyword& node) {
    return GetKeywordId(ctx, node).Name;
}

template <typename TRule>
inline TString GetKeyword(TTranslation& ctx, const TRule& node) {
    return GetIdentifier(ctx, node).Name;
}

static TString Id(const TRule_identifier& node, TTranslation& ctx) {
    // identifier: ID_PLAIN | ID_QUOTED;
    return ctx.Identifier(node.GetToken1());
}

static TString Id(const TRule_id& node, TTranslation& ctx) {
    // id: identifier | keyword;
    switch (node.Alt_case()) {
        case TRule_id::kAltId1:
            return Id(node.GetAlt_id1().GetRule_identifier1(), ctx);
        case TRule_id::kAltId2:
            return GetKeyword(ctx, node.GetAlt_id2().GetRule_keyword1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_or_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_or_type::kAltIdOrType1:
            return Id(node.GetAlt_id_or_type1().GetRule_id1(), ctx);
        case TRule_id_or_type::kAltIdOrType2:
            return ctx.Identifier(node.GetAlt_id_or_type2().GetRule_type_id1().GetToken1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_as_compat& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_as_compat::kAltIdAsCompat1:
            return Id(node.GetAlt_id_as_compat1().GetRule_identifier1(), ctx);
        case TRule_id_as_compat::kAltIdAsCompat2:
            return ctx.Token(node.GetAlt_id_as_compat2().GetRule_keyword_as_compat1().GetToken1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_as_compat& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_an_id_as_compat::kAltAnIdAsCompat1:
            return Id(node.GetAlt_an_id_as_compat1().GetRule_id_as_compat1(), ctx);
        case TRule_an_id_as_compat::kAltAnIdAsCompat2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_as_compat2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_schema& node, TTranslation& ctx) {
    //id_schema:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    // //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_schema::kAltIdSchema1:
            return Id(node.GetAlt_id_schema1().GetRule_identifier1(), ctx);
        case TRule_id_schema::kAltIdSchema2:
            return GetKeyword(ctx, node.GetAlt_id_schema2().GetRule_keyword_compat1());
        case TRule_id_schema::kAltIdSchema3:
            return GetKeyword(ctx, node.GetAlt_id_schema3().GetRule_keyword_expr_uncompat1());
        case TRule_id_schema::kAltIdSchema4:
            return GetKeyword(ctx, node.GetAlt_id_schema4().GetRule_keyword_select_uncompat1());
        case TRule_id_schema::kAltIdSchema5:
            return GetKeyword(ctx, node.GetAlt_id_schema5().GetRule_keyword_in_uncompat1());
        case TRule_id_schema::kAltIdSchema6:
            return GetKeyword(ctx, node.GetAlt_id_schema6().GetRule_keyword_window_uncompat1());
        case TRule_id_schema::kAltIdSchema7:
            return GetKeyword(ctx, node.GetAlt_id_schema7().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_or_type& node, TTranslation& ctx) {
    // an_id_or_type: id_or_type | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_or_type::kAltAnIdOrType1:
            return Id(node.GetAlt_an_id_or_type1().GetRule_id_or_type1(), ctx);
        case TRule_an_id_or_type::kAltAnIdOrType2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_or_type2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static std::pair<bool, TString> Id(const TRule_id_or_at& node, TTranslation& ctx) {
    bool hasAt = node.HasBlock1();
    return std::make_pair(hasAt, Id(node.GetRule_an_id_or_type2(), ctx) );
}

static TString Id(const TRule_id_table& node, TTranslation& ctx) {
    //id_table:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    // //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_table::kAltIdTable1:
            return Id(node.GetAlt_id_table1().GetRule_identifier1(), ctx);
        case TRule_id_table::kAltIdTable2:
            return GetKeyword(ctx, node.GetAlt_id_table2().GetRule_keyword_compat1());
        case TRule_id_table::kAltIdTable3:
            return GetKeyword(ctx, node.GetAlt_id_table3().GetRule_keyword_expr_uncompat1());
        case TRule_id_table::kAltIdTable4:
            return GetKeyword(ctx, node.GetAlt_id_table4().GetRule_keyword_select_uncompat1());
        case TRule_id_table::kAltIdTable5:
            return GetKeyword(ctx, node.GetAlt_id_table5().GetRule_keyword_in_uncompat1());
        case TRule_id_table::kAltIdTable6:
            return GetKeyword(ctx, node.GetAlt_id_table6().GetRule_keyword_window_uncompat1());
        case TRule_id_table::kAltIdTable7:
            return GetKeyword(ctx, node.GetAlt_id_table7().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_table& node, TTranslation& ctx) {
    // an_id_table: id_table | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_table::kAltAnIdTable1:
            return Id(node.GetAlt_an_id_table1().GetRule_id_table1(), ctx);
        case TRule_an_id_table::kAltAnIdTable2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_table2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_table_or_type& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
        case TRule_id_table_or_type::kAltIdTableOrType1:
            return Id(node.GetAlt_id_table_or_type1().GetRule_an_id_table1(), ctx);
        case TRule_id_table_or_type::kAltIdTableOrType2:
            return ctx.Identifier(node.GetAlt_id_table_or_type2().GetRule_type_id1().GetToken1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_expr& node, TTranslation& ctx) {
    //id_expr:
    //    identifier
    //  | keyword_compat
    // //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    // //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_expr::kAltIdExpr1:
            return Id(node.GetAlt_id_expr1().GetRule_identifier1(), ctx);
        case TRule_id_expr::kAltIdExpr2:
            return GetKeyword(ctx, node.GetAlt_id_expr2().GetRule_keyword_compat1());
        case TRule_id_expr::kAltIdExpr3:
            return GetKeyword(ctx, node.GetAlt_id_expr3().GetRule_keyword_alter_uncompat1());
        case TRule_id_expr::kAltIdExpr4:
            return GetKeyword(ctx, node.GetAlt_id_expr4().GetRule_keyword_in_uncompat1());
        case TRule_id_expr::kAltIdExpr5:
            return GetKeyword(ctx, node.GetAlt_id_expr5().GetRule_keyword_window_uncompat1());
        case TRule_id_expr::kAltIdExpr6:
            return GetKeyword(ctx, node.GetAlt_id_expr6().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static bool IsQuotedId(const TRule_id_expr& node, TTranslation& ctx) {
    if (node.Alt_case() != TRule_id_expr::kAltIdExpr1) {
        return false;
    }
    const auto& id = ctx.Token(node.GetAlt_id_expr1().GetRule_identifier1().GetToken1());
    // identifier: ID_PLAIN | ID_QUOTED;
    return id.StartsWith('`');
}

static TString Id(const TRule_id_expr_in& node, TTranslation& ctx) {
    //id_expr_in:
    //    identifier
    //  | keyword_compat
    // //  | keyword_expr_uncompat
    // //  | keyword_table_uncompat
    // //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    // //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_expr_in::kAltIdExprIn1:
            return Id(node.GetAlt_id_expr_in1().GetRule_identifier1(), ctx);
        case TRule_id_expr_in::kAltIdExprIn2:
            return GetKeyword(ctx, node.GetAlt_id_expr_in2().GetRule_keyword_compat1());
        case TRule_id_expr_in::kAltIdExprIn3:
            return GetKeyword(ctx, node.GetAlt_id_expr_in3().GetRule_keyword_alter_uncompat1());
        case TRule_id_expr_in::kAltIdExprIn4:
            return GetKeyword(ctx, node.GetAlt_id_expr_in4().GetRule_keyword_window_uncompat1());
        case TRule_id_expr_in::kAltIdExprIn5:
            return GetKeyword(ctx, node.GetAlt_id_expr_in5().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_window& node, TTranslation& ctx) {
    //id_window:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    // //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_window::kAltIdWindow1:
            return Id(node.GetAlt_id_window1().GetRule_identifier1(), ctx);
        case TRule_id_window::kAltIdWindow2:
            return GetKeyword(ctx, node.GetAlt_id_window2().GetRule_keyword_compat1());
        case TRule_id_window::kAltIdWindow3:
            return GetKeyword(ctx, node.GetAlt_id_window3().GetRule_keyword_expr_uncompat1());
        case TRule_id_window::kAltIdWindow4:
            return GetKeyword(ctx, node.GetAlt_id_window4().GetRule_keyword_table_uncompat1());
        case TRule_id_window::kAltIdWindow5:
            return GetKeyword(ctx, node.GetAlt_id_window5().GetRule_keyword_select_uncompat1());
        case TRule_id_window::kAltIdWindow6:
            return GetKeyword(ctx, node.GetAlt_id_window6().GetRule_keyword_alter_uncompat1());
        case TRule_id_window::kAltIdWindow7:
            return GetKeyword(ctx, node.GetAlt_id_window7().GetRule_keyword_in_uncompat1());
        case TRule_id_window::kAltIdWindow8:
            return GetKeyword(ctx, node.GetAlt_id_window8().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_without& node, TTranslation& ctx) {
    //id_without:
    //    identifier
    //  | keyword_compat
    // //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    // //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_without::kAltIdWithout1:
            return Id(node.GetAlt_id_without1().GetRule_identifier1(), ctx);
        case TRule_id_without::kAltIdWithout2:
            return GetKeyword(ctx, node.GetAlt_id_without2().GetRule_keyword_compat1());
        case TRule_id_without::kAltIdWithout3:
            return GetKeyword(ctx, node.GetAlt_id_without3().GetRule_keyword_table_uncompat1());
        case TRule_id_without::kAltIdWithout4:
            return GetKeyword(ctx, node.GetAlt_id_without4().GetRule_keyword_alter_uncompat1());
        case TRule_id_without::kAltIdWithout5:
            return GetKeyword(ctx, node.GetAlt_id_without5().GetRule_keyword_in_uncompat1());
        case TRule_id_without::kAltIdWithout6:
            return GetKeyword(ctx, node.GetAlt_id_without6().GetRule_keyword_window_uncompat1());
        case TRule_id_without::kAltIdWithout7:
            return GetKeyword(ctx, node.GetAlt_id_without7().GetRule_keyword_hint_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_id_hint& node, TTranslation& ctx) {
    //id_hint:
    //    identifier
    //  | keyword_compat
    //  | keyword_expr_uncompat
    //  | keyword_table_uncompat
    //  | keyword_select_uncompat
    //  | keyword_alter_uncompat
    //  | keyword_in_uncompat
    //  | keyword_window_uncompat
    // //  | keyword_hint_uncompat
    //;
    switch (node.Alt_case()) {
        case TRule_id_hint::kAltIdHint1:
            return Id(node.GetAlt_id_hint1().GetRule_identifier1(), ctx);
        case TRule_id_hint::kAltIdHint2:
            return GetKeyword(ctx, node.GetAlt_id_hint2().GetRule_keyword_compat1());
        case TRule_id_hint::kAltIdHint3:
            return GetKeyword(ctx, node.GetAlt_id_hint3().GetRule_keyword_expr_uncompat1());
        case TRule_id_hint::kAltIdHint4:
            return GetKeyword(ctx, node.GetAlt_id_hint4().GetRule_keyword_table_uncompat1());
        case TRule_id_hint::kAltIdHint5:
            return GetKeyword(ctx, node.GetAlt_id_hint5().GetRule_keyword_select_uncompat1());
        case TRule_id_hint::kAltIdHint6:
            return GetKeyword(ctx, node.GetAlt_id_hint6().GetRule_keyword_alter_uncompat1());
        case TRule_id_hint::kAltIdHint7:
            return GetKeyword(ctx, node.GetAlt_id_hint7().GetRule_keyword_in_uncompat1());
        case TRule_id_hint::kAltIdHint8:
            return GetKeyword(ctx, node.GetAlt_id_hint8().GetRule_keyword_window_uncompat1());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id& node, TTranslation& ctx) {
    // an_id: id | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id::kAltAnId1:
            return Id(node.GetAlt_an_id1().GetRule_id1(), ctx);
        case TRule_an_id::kAltAnId2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_schema& node, TTranslation& ctx) {
    // an_id_schema: id_schema | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_schema::kAltAnIdSchema1:
            return Id(node.GetAlt_an_id_schema1().GetRule_id_schema1(), ctx);
        case TRule_an_id_schema::kAltAnIdSchema2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_schema2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_expr& node, TTranslation& ctx) {
    // an_id_expr: id_expr | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_expr::kAltAnIdExpr1:
            return Id(node.GetAlt_an_id_expr1().GetRule_id_expr1(), ctx);
        case TRule_an_id_expr::kAltAnIdExpr2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_expr2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_window& node, TTranslation& ctx) {
    // an_id_window: id_window | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_window::kAltAnIdWindow1:
            return Id(node.GetAlt_an_id_window1().GetRule_id_window1(), ctx);
        case TRule_an_id_window::kAltAnIdWindow2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_window2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_without& node, TTranslation& ctx) {
    // an_id_without: id_without | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_without::kAltAnIdWithout1:
            return Id(node.GetAlt_an_id_without1().GetRule_id_without1(), ctx);
        case TRule_an_id_without::kAltAnIdWithout2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_without2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_hint& node, TTranslation& ctx) {
    // an_id_hint: id_hint | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_hint::kAltAnIdHint1:
            return Id(node.GetAlt_an_id_hint1().GetRule_id_hint1(), ctx);
        case TRule_an_id_hint::kAltAnIdHint2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_hint2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

static TString Id(const TRule_an_id_pure& node, TTranslation& ctx) {
    // an_id_pure: identifier | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_an_id_pure::kAltAnIdPure1:
            return Id(node.GetAlt_an_id_pure1().GetRule_identifier1(), ctx);
        case TRule_an_id_pure::kAltAnIdPure2:
            return IdContentFromString(ctx.Context(), ctx.Token(node.GetAlt_an_id_pure2().GetToken1()));
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

template<typename TRule>
static TIdentifier IdEx(const TRule& node, TTranslation& ctx) {
    const TString name(Id(node, ctx));
    const TPosition pos(ctx.Context().Pos());
    return TIdentifier(pos, name);
}

static bool NamedNodeImpl(const TRule_bind_parameter& node, TString& name, TTranslation& ctx) {
    // bind_parameter: DOLLAR (an_id_or_type | TRUE | FALSE);
    TString id;
    switch (node.GetBlock2().Alt_case()) {
        case TRule_bind_parameter::TBlock2::kAlt1:
            id = Id(node.GetBlock2().GetAlt1().GetRule_an_id_or_type1(), ctx);
            break;
        case TRule_bind_parameter::TBlock2::kAlt2:
            id = ctx.Token(node.GetBlock2().GetAlt2().GetToken1());
            break;
        case TRule_bind_parameter::TBlock2::kAlt3:
            id = ctx.Token(node.GetBlock2().GetAlt3().GetToken1());
            break;
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    auto dollar = ctx.Token(node.GetToken1());
    if (id.empty()) {
        ctx.Error() << "Empty symbol name is not allowed";
        return false;
    }

    name = dollar + id;
    return true;
}

static TString OptIdPrefixAsStr(const TRule_opt_id_prefix& node, TTranslation& ctx, const TString& defaultStr = {}) {
    if (!node.HasBlock1()) {
        return defaultStr;
    }
    return Id(node.GetBlock1().GetRule_an_id1(), ctx);
}

static TString OptIdPrefixAsStr(const TRule_opt_id_prefix_or_type& node, TTranslation& ctx, const TString& defaultStr = {}) {
    if (!node.HasBlock1()) {
        return defaultStr;
    }
    return Id(node.GetBlock1().GetRule_an_id_or_type1(), ctx);
}

static void PureColumnListStr(const TRule_pure_column_list& node, TTranslation& ctx, TVector<TString>& outList) {
    outList.push_back(Id(node.GetRule_an_id2(), ctx));
    for (auto& block: node.GetBlock3()) {
        outList.push_back(Id(block.GetRule_an_id2(), ctx));
    }
}

static bool NamedNodeImpl(const TRule_opt_bind_parameter& node, TString& name, bool& isOptional, TTranslation& ctx) {
    // opt_bind_parameter: bind_parameter QUESTION?;
    isOptional = false;
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name, ctx)) {
        return false;
    }
    isOptional = node.HasBlock2();
    return true;
}

static TDeferredAtom PureColumnOrNamed(const TRule_pure_column_or_named& node, TTranslation& ctx) {
    switch (node.Alt_case()) {
    case TRule_pure_column_or_named::kAltPureColumnOrNamed1: {
        TString named;
        if (!NamedNodeImpl(node.GetAlt_pure_column_or_named1().GetRule_bind_parameter1(), named, ctx)) {
            return {};
        }
        auto namedNode = ctx.GetNamedNode(named);
        if (!namedNode) {
            return {};
        }

        return TDeferredAtom(namedNode, ctx.Context());
    }

    case TRule_pure_column_or_named::kAltPureColumnOrNamed2:
        return TDeferredAtom(ctx.Context().Pos(), Id(node.GetAlt_pure_column_or_named2().GetRule_an_id1(), ctx));
    default:
        Y_FAIL("You should change implementation according to grammar changes");
    }
}

static bool PureColumnOrNamedListStr(const TRule_pure_column_or_named_list& node, TTranslation& ctx, TVector<TDeferredAtom>& outList) {
    outList.push_back(PureColumnOrNamed(node.GetRule_pure_column_or_named2(), ctx));
    if (outList.back().Empty()) {
        return false;
    }

    for (auto& block : node.GetBlock3()) {
        outList.push_back(PureColumnOrNamed(block.GetRule_pure_column_or_named2(), ctx));
        if (outList.back().Empty()) {
            return false;
        }
    }

    return true;
}

static bool CreateTableIndex(const TRule_table_index& node, TTranslation& ctx, TVector<TIndexDescription>& indexes) {
    indexes.emplace_back(IdEx(node.GetRule_an_id2(), ctx));

    const auto& indexType = node.GetRule_table_index_type3();
    switch (indexType.Alt_case()) {
        case TRule_table_index_type::kAltTableIndexType1: {
            auto globalIndex = indexType.GetAlt_table_index_type1().GetRule_global_index1();
            if (globalIndex.HasBlock2()) {
                ctx.AltNotImplemented("unique", indexType);
                return false;
            }
            if (globalIndex.HasBlock3()) {
                const TString token = to_lower(ctx.Token(globalIndex.GetBlock3().GetToken1()));
                if (token == "sync") {
                    indexes.back().Type = TIndexDescription::EType::GlobalSync;
                } else if (token == "async") {
                    indexes.back().Type = TIndexDescription::EType::GlobalAsync;
                } else {
                    Y_FAIL("You should change implementation according to grammar changes");
                }
            }
        }
        break;
        case TRule_table_index_type::kAltTableIndexType2:
            ctx.AltNotImplemented("local", indexType);
            return false;
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    if (node.HasBlock4()) {
        ctx.AltNotImplemented("with", indexType);
        return false;
    }

    indexes.back().IndexColumns.emplace_back(IdEx(node.GetRule_an_id_schema7(), ctx));
    for (const auto& block : node.GetBlock8()) {
        indexes.back().IndexColumns.emplace_back(IdEx(block.GetRule_an_id_schema2(), ctx));
    }

    if (node.HasBlock10()) {
        const auto& block = node.GetBlock10();
        indexes.back().DataColumns.emplace_back(IdEx(block.GetRule_an_id_schema3(), ctx));
        for (const auto& inner : block.GetBlock4()) {
            indexes.back().DataColumns.emplace_back(IdEx(inner.GetRule_an_id_schema2(), ctx));
        }
    }

    return true;
}

static std::pair<TString, TString> TableKeyImpl(const std::pair<bool, TString>& nameWithAt, TString view, TTranslation& ctx) {
    if (nameWithAt.first) {
        view = "@";
        ctx.Context().IncrementMonCounter("sql_features", "AnonymousTable");
    }

    return std::make_pair(nameWithAt.second, view);
}

static std::pair<TString, TString> TableKeyImpl(const TRule_table_key& node, TTranslation& ctx, bool hasAt) {
    auto name(Id(node.GetRule_id_table_or_type1(), ctx));
    TString view;
    if (node.HasBlock2()) {
        view = Id(node.GetBlock2().GetRule_an_id2(), ctx);
        ctx.Context().IncrementMonCounter("sql_features", "View");
    }

    return TableKeyImpl(std::make_pair(hasAt, name), view, ctx);
}

/// \return optional prefix
static TString ColumnNameAsStr(TTranslation& ctx, const TRule_column_name& node, TString& id) {
    id = Id(node.GetRule_an_id2(), ctx);
    return OptIdPrefixAsStr(node.GetRule_opt_id_prefix1(), ctx);
}

static TString ColumnNameAsSingleStr(TTranslation& ctx, const TRule_column_name& node) {
    TString body;
    const TString prefix = ColumnNameAsStr(ctx, node, body);
    return prefix ? prefix + '.' + body : body;
}

static void FillTargetList(TTranslation& ctx, const TRule_set_target_list& node, TVector<TString>& targetList) {
    targetList.push_back(ColumnNameAsSingleStr(ctx, node.GetRule_set_target2().GetRule_column_name1()));
    for (auto& block: node.GetBlock3()) {
        targetList.push_back(ColumnNameAsSingleStr(ctx, block.GetRule_set_target2().GetRule_column_name1()));
    }
}

TTableHints GetContextHints(TContext& ctx) {
    TTableHints hints;
    if (ctx.PragmaInferSchema) {
        hints["infer_schema"] = {};
    }
    if (ctx.PragmaDirectRead) {
        hints["direct_read"] = {};
    }

    return hints;
}

static TTableHints GetTableFuncHints(TStringBuf funcName) {
    TCiString func(funcName);
    TTableHints res;
    if (func.StartsWith("range") || func.StartsWith("like") || func.StartsWith("regexp") || func.StartsWith("filter")) {
        res.emplace("ignore_non_existing", TVector<TNodePtr>{});
    } else if (func.StartsWith("each")) {
        res.emplace("ignore_non_existing", TVector<TNodePtr>{});
        res.emplace("warn_non_existing", TVector<TNodePtr>{});
    }

    return res;
}

static bool ValidateForCounters(const TString& input) {
    for (auto c : input) {
        if (!(IsAlnum(c) || c == '_')) {
            return false;
        }
    }
    return true;
}

static bool IsColumnsOnly(const TVector<TSortSpecificationPtr>& container) {
    for (const auto& elem: container) {
        if (!elem->OrderExpr->GetColumnName()) {
            return false;
        }
    }
    return true;
}

static bool PackageVersionFromString(const TString& s, ui32& version) {
    if (s == "release") {
        version = 0;
        return true;
    }
    if (s == "draft") {
        version = 1;
        return true;
    }
    return TryFromString(s, version);
}

class TSqlQuery;

struct TSymbolNameWithPos {
    TString Name;
    TPosition Pos;
};

class TSqlTranslation: public TTranslation {
protected:
    TSqlTranslation(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TTranslation(ctx)
        , Mode(mode)
    {
        /// \todo remove NSQLTranslation::ESqlMode params
        YQL_ENSURE(ctx.Settings.Mode == mode);
    }

protected:
    enum class EExpr {
        Regular,
        GroupBy,
        SqlLambdaParams,
    };
    TNodePtr NamedExpr(const TRule_named_expr& node, EExpr exprMode = EExpr::Regular);
    bool NamedExprList(const TRule_named_expr_list& node, TVector<TNodePtr>& exprs, EExpr exprMode = EExpr::Regular);
    bool BindList(const TRule_bind_parameter_list& node, TVector<TSymbolNameWithPos>& bindNames);
    bool ActionOrSubqueryArgs(const TRule_action_or_subquery_args& node, TVector<TSymbolNameWithPos>& bindNames, ui32& optionalArgsCount);
    bool ModulePath(const TRule_module_path& node, TVector<TString>& path);
    bool NamedBindList(const TRule_named_bind_parameter_list& node, TVector<TSymbolNameWithPos>& names,
        TVector<TSymbolNameWithPos>& aliases);
    bool NamedBindParam(const TRule_named_bind_parameter& node, TSymbolNameWithPos& name, TSymbolNameWithPos& alias);
    TNodePtr NamedNode(const TRule_named_nodes_stmt& rule, TVector<TSymbolNameWithPos>& names);

    bool ImportStatement(const TRule_import_stmt& stmt, TVector<TString>* namesPtr = nullptr);
    TNodePtr DoStatement(const TRule_do_stmt& stmt, bool makeLambda, const TVector<TString>& args = {});
    bool DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt);
    bool DefineActionOrSubqueryBody(TSqlQuery& query, TBlocks& blocks, const TRule_define_action_or_subquery_body& body);
    TNodePtr IfStatement(const TRule_if_stmt& stmt);
    TNodePtr ForStatement(const TRule_for_stmt& stmt);
    TMaybe<TTableArg> TableArgImpl(const TRule_table_arg& node);
    bool TableRefImpl(const TRule_table_ref& node, TTableRef& result, bool unorderedSubquery);
    TMaybe<TSourcePtr> AsTableImpl(const TRule_table_ref& node);
    bool ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, TString& service, TDeferredAtom& cluster);
    bool ClusterExprOrBinding(const TRule_cluster_expr& node, TString& service, TDeferredAtom& cluster, bool& isBinding);
    bool ApplyTableBinding(const TString& binding, TTableRef& tr, TTableHints& hints);

    TMaybe<TColumnSchema> ColumnSchemaImpl(const TRule_column_schema& node);
    bool CreateTableEntry(const TRule_create_table_entry& node, TCreateTableParameters& params);

    bool FillFamilySettingsEntry(const TRule_family_settings_entry& settingNode, TFamilyEntry& family);
    bool FillFamilySettings(const TRule_family_settings& settingsNode, TFamilyEntry& family);
    bool CreateTableSettings(const TRule_with_table_settings& settingsNode, TCreateTableParameters& params);
    bool StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings,
        ETableType tableType, bool alter, bool reset);
    bool StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings,
        bool alter, bool reset);
    bool StoreExternalTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings);
    bool StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value& value, TTableSettings& settings, ETableType tableType, bool alter = false);
    bool StoreDataSourceSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result);
    bool ResetTableSettingsEntry(const TIdentifier& id, TTableSettings& settings, ETableType tableType);

    TIdentifier GetTopicConsumerId(const TRule_topic_consumer_ref& node);
    bool CreateConsumerSettings(const TRule_topic_consumer_settings& settingsNode, TTopicConsumerSettings& settings);
    bool CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& params);
    bool CreateTopicConsumer(const TRule_topic_create_consumer_entry& node,
                             TVector<TTopicConsumerDescription>& consumers);
    bool CreateTopicEntry(const TRule_create_topic_entry& node, TCreateTopicParameters& params);

    bool AlterTopicConsumer(const TRule_alter_topic_alter_consumer& node,
                            THashMap<TString, TTopicConsumerDescription>& alterConsumers);

    bool AlterTopicConsumerEntry(const TRule_alter_topic_alter_consumer_entry& node,
                                 TTopicConsumerDescription& alterConsumer);


    bool AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params);


    TNodePtr TypeSimple(const TRule_type_name_simple& node, bool onlyDataAllowed);
    TNodePtr TypeDecimal(const TRule_type_name_decimal& node);
    TNodePtr AddOptionals(const TNodePtr& node, size_t optionalCount);
    TMaybe<std::pair<TVector<TNodePtr>, bool>> CallableArgList(const TRule_callable_arg_list& argList, bool namedArgsStarted);

    TNodePtr IntegerOrBind(const TRule_integer_or_bind& node);
    TNodePtr TypeNameTag(const TRule_type_name_tag& node);
    TNodePtr TypeNodeOrBind(const TRule_type_name_or_bind& node);
    TNodePtr TypeNode(const TRule_type_name& node);
    TNodePtr TypeNode(const TRule_type_name_composite& node);
    TNodePtr ValueConstructorLiteral(const TRule_value_constructor_literal& node);
    TNodePtr ValueConstructor(const TRule_value_constructor& node);
    TNodePtr ListLiteral(const TRule_list_literal& node);
    TNodePtr DictLiteral(const TRule_dict_literal& node);
    TNodePtr StructLiteral(const TRule_struct_literal& node);
    TMaybe<TTableHints> TableHintsImpl(const TRule_table_hints& node);
    bool TableHintImpl(const TRule_table_hint& rule, TTableHints& hints);
    bool SimpleTableRefImpl(const TRule_simple_table_ref& node, TTableRef& result);
    bool TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result);
    TWindowSpecificationPtr WindowSpecification(const TRule_window_specification_details& rule);
    bool OrderByClause(const TRule_order_by_clause& node, TVector<TSortSpecificationPtr>& orderBy);
    bool SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs);

    bool IsDistinctOptSet(const TRule_opt_set_quantifier& node) const;
    bool IsDistinctOptSet(const TRule_opt_set_quantifier& node, TPosition& distinctPos) const;

    bool AddObjectFeature(std::map<TString, TDeferredAtom>& result, const TRule_object_feature& feature);
    bool BindParameterClause(const TRule_bind_parameter& node, TDeferredAtom& result);
    bool ObjectFeatureValueClause(const TRule_object_feature_value & node, TDeferredAtom & result);
    bool ParseObjectFeatures(std::map<TString, TDeferredAtom> & result, const TRule_object_features & features);
    bool ParseExternalDataSourceSettings(std::map<TString, TDeferredAtom> & result, const TRule_with_table_settings & settings);
    bool RoleNameClause(const TRule_role_name& node, TDeferredAtom& result, bool allowSystemRoles);
    bool RoleParameters(const TRule_create_user_option& node, TRoleParameters& result);
    bool PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption);
    bool PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result);
    bool PermissionNameClause(const TRule_permission_id& node, TDeferredAtom& result);

    bool ValidateExternalTable(const TCreateTableParameters& params);
private:
    bool SimpleTableRefCoreImpl(const TRule_simple_table_ref_core& node, TTableRef& result);
    static bool IsValidFrameSettings(TContext& ctx, const TFrameSpecification& frameSpec, size_t sortSpecSize);
    static TString FrameSettingsToString(EFrameSettings settings, bool isUnbounded);

    bool FrameBound(const TRule_window_frame_bound& rule, TFrameBoundPtr& bound);
    bool FrameClause(const TRule_window_frame_clause& node, TFrameSpecificationPtr& frameSpec, size_t sortSpecSize);
    bool SortSpecification(const TRule_sort_specification& node, TVector<TSortSpecificationPtr>& sortSpecs);

    bool ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, bool allowBinding, TString& service, TDeferredAtom& cluster, bool& isBinding);
    bool StructLiteralItem(TVector<TNodePtr>& labels, const TRule_expr& label, TVector<TNodePtr>& values, const TRule_expr& value);
protected:
    NSQLTranslation::ESqlMode Mode;
};

class TSqlExpression: public TSqlTranslation {
public:
    enum class ESmartParenthesis {
        Default,
        GroupBy,
        InStatement,
        SqlLambdaParams,
    };

    TSqlExpression(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_expr& node) {
        // expr:
        //     or_subexpr (OR or_subexpr)*
        //   | type_name_composite
        switch (node.Alt_case()) {
            case TRule_expr::kAltExpr1: {
                auto getNode = [](const TRule_expr_TAlt1_TBlock2& b) -> const TRule_or_subexpr& { return b.GetRule_or_subexpr2(); };
                return BinOper("Or", node.GetAlt_expr1().GetRule_or_subexpr1(), getNode,
                    node.GetAlt_expr1().GetBlock2().begin(), node.GetAlt_expr1().GetBlock2().end(), {});
            }
            case TRule_expr::kAltExpr2: {
                return TypeNode(node.GetAlt_expr2().GetRule_type_name_composite1());
            }
            default:
                Y_FAIL("You should change implementation according to grammar changes");
        }
    }

    void SetSmartParenthesisMode(ESmartParenthesis mode) {
        SmartParenthesisMode = mode;
    }

    void MarkAsNamed() {
        MaybeUnnamedSmartParenOnTop = false;
    }

    TMaybe<TExprOrIdent> LiteralExpr(const TRule_literal_value& node);
private:
    struct TTrailingQuestions {
        size_t Count = 0;
        TPosition Pos;
    };

    TNodePtr BindParameterRule(const TRule_bind_parameter& rule, const TTrailingQuestions& tail);
    TNodePtr LambdaRule(const TRule_lambda& rule);
    TNodePtr CastRule(const TRule_cast_expr& rule);
    TNodePtr BitCastRule(const TRule_bitcast_expr& rule);
    TNodePtr ExistsRule(const TRule_exists_expr& rule);
    TNodePtr CaseRule(const TRule_case_expr& rule);

    TMaybe<TExprOrIdent> AtomExpr(const TRule_atom_expr& node, const TTrailingQuestions& tail);
    TMaybe<TExprOrIdent> InAtomExpr(const TRule_in_atom_expr& node, const TTrailingQuestions& tail);

    TNodePtr JsonInputArg(const TRule_json_common_args& node);
    TNodePtr JsonPathSpecification(const TRule_jsonpath_spec& node);
    TNodePtr JsonReturningTypeRule(const TRule_type_name_simple& node);
    TNodePtr JsonValueCaseHandler(const TRule_json_case_handler& node, EJsonValueHandlerMode& mode);
    void AddJsonValueCaseHandlers(const TRule_json_value& node, TVector<TNodePtr>& children);
    void AddJsonVariable(const TRule_json_variable& node, TVector<TNodePtr>& children);
    void AddJsonVariables(const TRule_json_variables& node, TVector<TNodePtr>& children);
    TNodePtr JsonVariables(const TRule_json_common_args& node);
    void AddJsonCommonArgs(const TRule_json_common_args& node, TVector<TNodePtr>& children);
    TNodePtr JsonValueExpr(const TRule_json_value& node);
    void AddJsonExistsHandler(const TRule_json_exists& node, TVector<TNodePtr>& children);
    TNodePtr JsonExistsExpr(const TRule_json_exists& node);
    EJsonQueryWrap JsonQueryWrapper(const TRule_json_query& node);
    EJsonQueryHandler JsonQueryHandler(const TRule_json_query_handler& node);
    TNodePtr JsonQueryExpr(const TRule_json_query& node);
    TNodePtr JsonApiExpr(const TRule_json_api_expr& node);

    template<typename TUnaryCasualExprRule>
    TNodePtr UnaryCasualExpr(const TUnaryCasualExprRule& node, const TTrailingQuestions& tail);

    template<typename TUnarySubExprRule>
    TNodePtr UnaryExpr(const TUnarySubExprRule& node, const TTrailingQuestions& tail);

    bool SqlLambdaParams(const TNodePtr& node, TVector<TSymbolNameWithPos>& args, ui32& optionalArgumentsCount);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq);
    bool SqlLambdaExprBody(TContext& ctx, const TRule_expr& node, TVector<TNodePtr>& exprSeq);

    TNodePtr KeyExpr(const TRule_key_expr& node) {
        TSqlExpression expr(Ctx, Mode);
        return expr.Build(node.GetRule_expr2());
    }

    TNodePtr SubExpr(const TRule_con_subexpr& node, const TTrailingQuestions& tail);
    TNodePtr SubExpr(const TRule_xor_subexpr& node, const TTrailingQuestions& tail);

    TNodePtr SubExpr(const TRule_mul_subexpr& node, const TTrailingQuestions& tail) {
        // mul_subexpr: con_subexpr (DOUBLE_PIPE con_subexpr)*;
        auto getNode = [](const TRule_mul_subexpr::TBlock2& b) -> const TRule_con_subexpr& { return b.GetRule_con_subexpr2(); };
        return BinOper("Concat", node.GetRule_con_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    TNodePtr SubExpr(const TRule_add_subexpr& node, const TTrailingQuestions& tail) {
        // add_subexpr: mul_subexpr ((ASTERISK | SLASH | PERCENT) mul_subexpr)*;
        auto getNode = [](const TRule_add_subexpr::TBlock2& b) -> const TRule_mul_subexpr& { return b.GetRule_mul_subexpr2(); };
        return BinOpList(node.GetRule_mul_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    TNodePtr SubExpr(const TRule_bit_subexpr& node, const TTrailingQuestions& tail) {
        // bit_subexpr: add_subexpr ((PLUS | MINUS) add_subexpr)*;
        auto getNode = [](const TRule_bit_subexpr::TBlock2& b) -> const TRule_add_subexpr& { return b.GetRule_add_subexpr2(); };
        return BinOpList(node.GetRule_add_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    TNodePtr SubExpr(const TRule_neq_subexpr& node, const TTrailingQuestions& tailExternal) {
        //neq_subexpr: bit_subexpr ((SHIFT_LEFT | shift_right | ROT_LEFT | rot_right | AMPERSAND | PIPE | CARET) bit_subexpr)*
        //  // trailing QUESTIONS are used in optional simple types (String?) and optional lambda args: ($x, $y?) -> ($x)
        //  ((double_question neq_subexpr) => double_question neq_subexpr | QUESTION+)?;
        YQL_ENSURE(tailExternal.Count == 0);
        MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && !node.HasBlock3();
        TTrailingQuestions tail;
        if (node.HasBlock3() && node.GetBlock3().Alt_case() == TRule_neq_subexpr::TBlock3::kAlt2) {
            auto& questions = node.GetBlock3().GetAlt2();
            tail.Count = questions.GetBlock1().size();
            tail.Pos = Ctx.TokenPosition(questions.GetBlock1().begin()->GetToken1());
            YQL_ENSURE(tail.Count > 0);
        }

        auto getNode = [](const TRule_neq_subexpr::TBlock2& b) -> const TRule_bit_subexpr& { return b.GetRule_bit_subexpr2(); };
        auto result = BinOpList(node.GetRule_bit_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
        if (!result) {
            return {};
        }
        if (node.HasBlock3()) {
            auto& block = node.GetBlock3();
            if (block.Alt_case() == TRule_neq_subexpr::TBlock3::kAlt1) {
                TSqlExpression altExpr(Ctx, Mode);
                auto altResult = SubExpr(block.GetAlt1().GetRule_neq_subexpr2(), {});
                if (!altResult) {
                    return {};
                }
                const TVector<TNodePtr> args({result, altResult});
                Token(block.GetAlt1().GetRule_double_question1().GetToken1());
                result = BuildBuiltinFunc(Ctx, Ctx.Pos(), "Coalesce", args);
            }
        }
        return result;
    }

    TNodePtr SubExpr(const TRule_eq_subexpr& node, const TTrailingQuestions& tail) {
        // eq_subexpr: neq_subexpr ((LESS | LESS_OR_EQ | GREATER | GREATER_OR_EQ) neq_subexpr)*;
        auto getNode = [](const TRule_eq_subexpr::TBlock2& b) -> const TRule_neq_subexpr& { return b.GetRule_neq_subexpr2(); };
        return BinOpList(node.GetRule_neq_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    TNodePtr SubExpr(const TRule_or_subexpr& node, const TTrailingQuestions& tail) {
        // or_subexpr: and_subexpr (AND and_subexpr)*;
        auto getNode = [](const TRule_or_subexpr::TBlock2& b) -> const TRule_and_subexpr& { return b.GetRule_and_subexpr2(); };
        return BinOper("And", node.GetRule_and_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    TNodePtr SubExpr(const TRule_and_subexpr& node, const TTrailingQuestions& tail) {
        // and_subexpr: xor_subexpr (XOR xor_subexpr)*;
        auto getNode = [](const TRule_and_subexpr::TBlock2& b) -> const TRule_xor_subexpr& { return b.GetRule_xor_subexpr2(); };
        return BinOper("Xor", node.GetRule_xor_subexpr1(), getNode, node.GetBlock2().begin(), node.GetBlock2().end(), tail);
    }

    template <typename TNode, typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    template <typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TRule_bit_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    template <typename TGetNode, typename TIter>
    TNodePtr BinOpList(const TRule_eq_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    TNodePtr BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const;

    struct TCaseBranch {
        TNodePtr Pred;
        TNodePtr Value;
    };
    TCaseBranch ReduceCaseBranches(TVector<TCaseBranch>::const_iterator begin, TVector<TCaseBranch>::const_iterator end) const;

    template <typename TNode, typename TGetNode, typename TIter>
    TNodePtr BinOper(const TString& operName, const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail);

    TNodePtr SqlInExpr(const TRule_in_expr& node, const TTrailingQuestions& tail);

    void UnexpectedQuestionToken(const TTrailingQuestions& tail) {
        YQL_ENSURE(tail.Count > 0);
        Ctx.Error(tail.Pos) << "Unexpected token '?' at the end of expression";
    }

    TNodePtr SmartParenthesis(const TRule_smart_parenthesis& node);

    ESmartParenthesis SmartParenthesisMode = ESmartParenthesis::Default;
    bool MaybeUnnamedSmartParenOnTop = true;

    THashMap<TString, TNodePtr> ExprShortcuts;
};

class TSqlCallExpr: public TSqlTranslation {
public:
    TSqlCallExpr(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSqlCallExpr(const TSqlCallExpr& call, const TVector<TNodePtr>& args)
        : TSqlTranslation(call.Ctx, call.Mode)
        , Pos(call.Pos)
        , Func(call.Func)
        , Module(call.Module)
        , Node(call.Node)
        , Args(args)
        , AggMode(call.AggMode)
        , DistinctAllowed(call.DistinctAllowed)
        , UsingCallExpr(call.UsingCallExpr)
        , IsExternalCall(call.IsExternalCall)
        , CallConfig(call.CallConfig)
    {
    }

    void AllowDistinct() {
        DistinctAllowed = true;
    }

    void InitName(const TString& name);
    void InitExpr(const TNodePtr& expr);

    bool Init(const TRule_using_call_expr& node);
    bool Init(const TRule_value_constructor& node);
    bool Init(const TRule_invoke_expr& node);
    bool ConfigureExternalCall(const TRule_external_call_settings& node);
    void IncCounters();

    TNodePtr BuildUdf(bool forReduce) {
        auto result = Node ? Node : BuildCallable(Pos, Module, Func, Args, forReduce);
        if (to_lower(Module) == "tensorflow" && Func == "RunBatch") {
            if (Args.size() > 2) {
                Args.erase(Args.begin() + 2);
            } else {
                Ctx.Error(Pos) << "Excepted >= 3 arguments, but got: " << Args.size();
                return nullptr;
            }
        }
        return result;
    }

    TNodePtr BuildCall() {
        TVector<TNodePtr> args;
        bool warnOnYqlNameSpace = true;

        TUdfNode* udf_node = Node ? dynamic_cast<TUdfNode*>(Node.Get()) : nullptr;
        if (udf_node) {
            if (!udf_node->DoInit(Ctx, nullptr)) {
                return nullptr;
            }
            TNodePtr positional_args = BuildTuple(Pos, PositionalArgs);
            TNodePtr positional = positional_args->Y("TypeOf", positional_args);
            TNodePtr named_args = BuildStructure(Pos, NamedArgs);
            TNodePtr named = named_args->Y("TypeOf", named_args);

            TNodePtr custom_user_type = new TCallNodeImpl(Pos, "TupleType", {positional, named, udf_node->GetExternalTypes()});

            return BuildSqlCall(Ctx, Pos, udf_node->GetModule(), udf_node->GetFunction(),
                                args, positional_args, named_args, custom_user_type,
                                udf_node->GetTypeConfig(), udf_node->GetRunConfig());
        }

        if (Node && !Node->FuncName()) {
            Module = "YQL";
            Func = NamedArgs.empty() ? "Apply" : "NamedApply";
            warnOnYqlNameSpace = false;
            args.push_back(Node);
        }

        if (Node && Node->FuncName()) {
            Module = Node->ModuleName() ? *Node->ModuleName() : "YQL";
            Func = *Node->FuncName();
        }
        bool mustUseNamed = !NamedArgs.empty();
        if (mustUseNamed) {
            if (Node && !Node->FuncName()) {
                mustUseNamed = false;
            }
            args.emplace_back(BuildTuple(Pos, PositionalArgs));
            args.emplace_back(BuildStructure(Pos, NamedArgs));
        } else if (IsExternalCall) {
            Func = "SqlExternalFunction";
            if (Args.size() < 2 || Args.size() > 3) {
                Ctx.Error(Pos) << "EXTERNAL FUNCTION requires from 2 to 3 arguments, but got: " << Args.size();
                return nullptr;
            }

            if (Args.size() == 3) {
                args.insert(args.end(), Args.begin(), Args.end() - 1);
                Args.erase(Args.begin(), Args.end() - 1);
            } else {
                args.insert(args.end(), Args.begin(), Args.end());
                Args.erase(Args.begin(), Args.end());
            }
            auto configNode = new TExternalFunctionConfig(Pos, CallConfig);
            auto configList = new TAstListNodeImpl(Pos, { new TAstAtomNodeImpl(Pos, "quote", 0), configNode });
            args.push_back(configList);
        } else {
            args.insert(args.end(), Args.begin(), Args.end());
        }

        auto result = BuildBuiltinFunc(Ctx, Pos, Func, args, Module, AggMode, &mustUseNamed, warnOnYqlNameSpace);
        if (mustUseNamed) {
            Error() << "Named args are used for call, but unsupported by function: " << Func;
            return nullptr;
        }

        if (WindowName) {
            result = BuildCalcOverWindow(Pos, WindowName, result);
        }

        return result;
    }

    TPosition GetPos() const {
        return Pos;
    }

    const TVector<TNodePtr>& GetArgs() const {
        return Args;
    }

    void SetOverWindow() {
        YQL_ENSURE(AggMode == EAggregateMode::Normal);
        AggMode = EAggregateMode::OverWindow;
    }

    void SetIgnoreNulls() {
        Func += "_IgnoreNulls";
    }

    bool IsExternal() {
        return IsExternalCall;
    }

private:
    bool ExtractCallParam(const TRule_external_call_param& node);
    bool FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node);
    bool FillArgs(const TRule_named_expr_list& node);

private:
    TPosition Pos;
    TString Func;
    TString Module;
    TNodePtr Node;
    TVector<TNodePtr> Args;
    TVector<TNodePtr> PositionalArgs;
    TVector<TNodePtr> NamedArgs;
    EAggregateMode AggMode = EAggregateMode::Normal;
    TString WindowName;
    bool DistinctAllowed = false;
    bool UsingCallExpr = false;
    bool IsExternalCall = false;
    TFunctionConfig CallConfig;
};

TNodePtr TSqlTranslation::NamedExpr(const TRule_named_expr& node, EExpr exprMode) {
    TSqlExpression expr(Ctx, Mode);
    if (exprMode == EExpr::GroupBy) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::GroupBy);
    } else if (exprMode == EExpr::SqlLambdaParams) {
        expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::SqlLambdaParams);
    }
    if (node.HasBlock2()) {
        expr.MarkAsNamed();
    }
    TNodePtr exprNode(expr.Build(node.GetRule_expr1()));
    if (!exprNode) {
        Ctx.IncrementMonCounter("sql_errors", "NamedExprInvalid");
        return nullptr;
    }
    if (node.HasBlock2()) {
        exprNode = SafeClone(exprNode);
        exprNode->SetLabel(Id(node.GetBlock2().GetRule_an_id_or_type2(), *this));
    }
    return exprNode;
}

bool TSqlTranslation::NamedExprList(const TRule_named_expr_list& node, TVector<TNodePtr>& exprs, EExpr exprMode) {
    exprs.emplace_back(NamedExpr(node.GetRule_named_expr1(), exprMode));
    if (!exprs.back()) {
        return false;
    }
    for (auto& b: node.GetBlock2()) {
        exprs.emplace_back(NamedExpr(b.GetRule_named_expr2(), exprMode));
        if (!exprs.back()) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::BindList(const TRule_bind_parameter_list& node, TVector<TSymbolNameWithPos>& bindNames) {
    bindNames.clear();

    TString name;
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name, *this)) {
        return false;
    }

    bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});
    for (auto& b: node.GetBlock2()) {
        if (!NamedNodeImpl(b.GetRule_bind_parameter2(), name, *this)) {
            return false;
        }

        bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});
    }
    return true;
}

bool TSqlTranslation::ActionOrSubqueryArgs(const TRule_action_or_subquery_args& node, TVector<TSymbolNameWithPos>& bindNames, ui32& optionalArgsCount) {
    bindNames.clear();
    optionalArgsCount = 0;

    TString name;
    bool isOptional = false;
    if (!NamedNodeImpl(node.GetRule_opt_bind_parameter1(), name, isOptional, *this)) {
        return false;
    }

    if (isOptional) {
        optionalArgsCount++;
    }
    bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});

    for (auto& b: node.GetBlock2()) {
        if (!NamedNodeImpl(b.GetRule_opt_bind_parameter2(), name, isOptional, *this)) {
            return false;
        }

        if (isOptional) {
            optionalArgsCount++;
        } else if (optionalArgsCount > 0) {
            Context().Error() << "Non-optional argument can not follow optional one";
            return false;
        }
        bindNames.emplace_back(TSymbolNameWithPos{name, Ctx.Pos()});
    }
    return true;
}

bool TSqlTranslation::ModulePath(const TRule_module_path& node, TVector<TString>& path) {
    if (node.HasBlock1()) {
        path.emplace_back(TString());
    }
    path.emplace_back(Id(node.GetRule_an_id2(), *this));
    for (auto& b: node.GetBlock3()) {
        path.emplace_back(Id(b.GetRule_an_id2(), *this));
    }
    return true;
}

bool TSqlTranslation::NamedBindList(const TRule_named_bind_parameter_list& node, TVector<TSymbolNameWithPos>& names,
    TVector<TSymbolNameWithPos>& aliases)
{
    names.clear();
    aliases.clear();
    TSymbolNameWithPos name;
    TSymbolNameWithPos alias;

    if (!NamedBindParam(node.GetRule_named_bind_parameter1(), name, alias)) {
        return false;
    }
    names.push_back(name);
    aliases.push_back(alias);

    for (auto& b: node.GetBlock2()) {
        if (!NamedBindParam(b.GetRule_named_bind_parameter2(), name, alias)) {
            return false;
        }
        names.push_back(name);
        aliases.push_back(alias);
    }
    return true;
}

bool TSqlTranslation::NamedBindParam(const TRule_named_bind_parameter& node, TSymbolNameWithPos& name, TSymbolNameWithPos& alias) {
    name = alias = {};
    if (!NamedNodeImpl(node.GetRule_bind_parameter1(), name.Name, *this)) {
        return false;
    }
    name.Pos = Ctx.Pos();
    if (node.HasBlock2()) {
        if (!NamedNodeImpl(node.GetBlock2().GetRule_bind_parameter2(), alias.Name, *this)) {
            return false;
        }
        alias.Pos = Ctx.Pos();
    }
    return true;
}

TMaybe<TTableArg> TSqlTranslation::TableArgImpl(const TRule_table_arg& node) {
    TTableArg ret;
    ret.HasAt = node.HasBlock1();
    TColumnRefScope scope(Ctx, EColumnRefState::AsStringLiteral);
    ret.Expr = NamedExpr(node.GetRule_named_expr2());
    if (!ret.Expr) {
        return Nothing();
    }

    if (node.HasBlock3()) {
        ret.View = Id(node.GetBlock3().GetRule_an_id2(), *this);
        Context().IncrementMonCounter("sql_features", "View");
    }

    return ret;
}

bool TSqlTranslation::ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, TString& service, TDeferredAtom& cluster) {
    bool allowBinding = false;
    bool isBinding;
    return ClusterExpr(node, allowWildcard, allowBinding, service, cluster, isBinding);
}

bool TSqlTranslation::ClusterExprOrBinding(const TRule_cluster_expr& node, TString& service, TDeferredAtom& cluster, bool& isBinding) {
    bool allowWildcard = false;
    bool allowBinding = true;
    return ClusterExpr(node, allowWildcard, allowBinding, service, cluster, isBinding);
}

bool TSqlTranslation::ClusterExpr(const TRule_cluster_expr& node, bool allowWildcard, bool allowBinding, TString& service,
    TDeferredAtom& cluster, bool& isBinding)
{
    service = "";
    cluster = TDeferredAtom();
    isBinding = false;
    if (node.HasBlock1()) {
        service = to_lower(Id(node.GetBlock1().GetRule_an_id1(), *this));
        allowBinding = false;
        if (service != YtProviderName &&
            service != KikimrProviderName &&
            service != RtmrProviderName && service != StatProviderName) {
            Ctx.Error() << "Unknown service: " << service;
            return false;
        }
    }

    switch (node.GetBlock2().Alt_case()) {
    case TRule_cluster_expr::TBlock2::kAlt1: {
        auto value = PureColumnOrNamed(node.GetBlock2().GetAlt1().GetRule_pure_column_or_named1(), *this);
        if (value.Empty()) {
            return false;
        }

        if (value.GetLiteral()) {
            TString clusterName = *value.GetLiteral();
            if (allowBinding && to_lower(clusterName) == "bindings") {
                isBinding = true;
                return true;
            }
            TString normalizedClusterName;
            auto foundProvider = Ctx.GetClusterProvider(clusterName, normalizedClusterName);
            if (!foundProvider) {
                Ctx.Error() << "Unknown cluster: " << clusterName;
                return false;
            }

            if (service && *foundProvider != service) {
                Ctx.Error() << "Mismatch of cluster " << clusterName << " service, expected: "
                    << *foundProvider << ", got: " << service;
                return false;
            }

            if (!service) {
                service = *foundProvider;
            }

            value = TDeferredAtom(Ctx.Pos(), normalizedClusterName);
        } else {
            if (!service) {
                Ctx.Error() << "Cluster service is not set";
                return false;
            }
        }

        cluster = value;
        return true;
    }
    case TRule_cluster_expr::TBlock2::kAlt2: {
        if (!allowWildcard) {
            Ctx.Error() << "Cluster wildcards allowed only in USE statement";
            return false;
        }

        return true;
    }
    default:
        Y_FAIL("You should change implementation according to grammar changes");
    }
}

bool ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node);

bool TSqlTranslation::ApplyTableBinding(const TString& binding, TTableRef& tr, TTableHints& hints) {
    NSQLTranslation::TBindingInfo bindingInfo;
    if (const auto& error = ExtractBindingInfo(Context().Settings, binding, bindingInfo)) {
        Ctx.Error() << error;
        return false;
    }

    if (bindingInfo.Schema) {
        TNodePtr schema = BuildQuotedAtom(Ctx.Pos(), bindingInfo.Schema);

        TNodePtr type = new TCallNodeImpl(Ctx.Pos(), "SqlTypeFromYson", { schema });
        TNodePtr columns = new TCallNodeImpl(Ctx.Pos(), "SqlColumnOrderFromYson", { schema });

        hints["user_schema"] = { type, columns };
    }

    for (auto& [key, values] : bindingInfo.Attributes) {
        TVector<TNodePtr> hintValue;
        for (auto& column : values) {
            hintValue.push_back(BuildQuotedAtom(Ctx.Pos(), column));
        }
        hints[key] = std::move(hintValue);
    }

    tr.Service = bindingInfo.ClusterType;
    tr.Cluster = TDeferredAtom(Ctx.Pos(), bindingInfo.Cluster);

    const TString view = "";
    tr.Keys = BuildTableKey(Ctx.Pos(), tr.Service, tr.Cluster, TDeferredAtom(Ctx.Pos(), bindingInfo.Path), view);

    return true;
}

bool TSqlTranslation::TableRefImpl(const TRule_table_ref& node, TTableRef& result, bool unorderedSubquery) {
    // table_ref:
    //   (cluster_expr DOT)? AT?
    //   (table_key | an_id_expr LPAREN (table_arg (COMMA table_arg)*)? RPAREN |
    //    bind_parameter (LPAREN expr_list? RPAREN)? (VIEW an_id)?)
    //   table_hints?;
    if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW && node.HasBlock1()) {
        Ctx.Error() << "Cluster should not be used in limited view";
        return false;
    }
    auto service = Context().Scoped->CurrService;
    auto cluster = Context().Scoped->CurrCluster;
    const bool hasAt = node.HasBlock2();
    bool isBinding = false;
    if (node.HasBlock1()) {
        const auto& clusterExpr = node.GetBlock1().GetRule_cluster_expr1();
        bool result = !hasAt ?
            ClusterExprOrBinding(clusterExpr, service, cluster, isBinding) : ClusterExpr(clusterExpr, false, service, cluster);
        if (!result) {
            return false;
        }
    }

    if (service == SolomonProviderName) {
        Ctx.Error() << "Selecting data from monitoring source is not supported";
        return false;
    }

    TTableRef tr(Context().MakeName("table"), service, cluster, nullptr);
    TPosition pos(Context().Pos());
    TTableHints hints = GetContextHints(Ctx);
    TTableHints tableHints;
    auto& block = node.GetBlock3();
    switch (block.Alt_case()) {
        case TRule_table_ref::TBlock3::kAlt1: {
            if (!isBinding && cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            auto pair = TableKeyImpl(block.GetAlt1().GetRule_table_key1(), *this, hasAt);
            if (isBinding) {
                TString binding = pair.first;
                TString view = pair.second;
                if (!view.empty()) {
                    YQL_ENSURE(view != "@");
                    Ctx.Error() << "VIEW is not supported for table bindings";
                    return false;
                }

                if (!ApplyTableBinding(binding, tr, tableHints)) {
                    return false;
                }
            } else {
                tr.Keys = BuildTableKey(pos, service, cluster, TDeferredAtom(pos, pair.first), pair.second);
            }
            break;
        }
        case TRule_table_ref::TBlock3::kAlt2: {
            if (cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return false;
            }

            auto& alt = block.GetAlt2();
            const TString func(Id(alt.GetRule_an_id_expr1(), *this));
            TVector<TTableArg> args;
            if (alt.HasBlock3()) {
                auto& argsBlock = alt.GetBlock3();
                auto arg = TableArgImpl(argsBlock.GetRule_table_arg1());
                if (!arg) {
                    return false;
                }

                args.push_back(std::move(*arg));
                for (auto& b : argsBlock.GetBlock2()) {
                    arg = TableArgImpl(b.GetRule_table_arg2());
                    if (!arg) {
                        return false;
                    }

                    args.push_back(std::move(*arg));
                }
            }
            tableHints = GetTableFuncHints(func);
            tr.Keys = BuildTableKeys(pos, service, cluster, func, args);
            break;
        }
        case TRule_table_ref::TBlock3::kAlt3: {
            auto& alt = block.GetAlt3();
            Ctx.IncrementMonCounter("sql_features", "NamedNodeUseSource");
            TString named;
            if (!NamedNodeImpl(alt.GetRule_bind_parameter1(), named, *this)) {
                return false;
            }
            if (hasAt) {
                if (alt.HasBlock2()) {
                    Ctx.Error() << "Subquery must not be used as anonymous table name";
                    return false;
                }

                if (alt.HasBlock3()) {
                    Ctx.Error() << "View is not supported for anonymous tables";
                    return false;
                }

                if (node.HasBlock4()) {
                    Ctx.Error() << "Hints are not supported for anonymous tables";
                    return false;
                }

                auto namedNode = GetNamedNode(named);
                if (!namedNode) {
                    return false;
                }

                auto source = TryMakeSourceFromExpression(Ctx, service, cluster, namedNode, "@");
                if (!source) {
                    Ctx.Error() << "Cannot infer cluster and table name";
                    return false;
                }

                result.Source = source;
                return true;
            }
            auto nodePtr = GetNamedNode(named);
            if (!nodePtr) {
                Ctx.IncrementMonCounter("sql_errors", "NamedNodeSourceError");
                return false;
            }
            if (alt.HasBlock2()) {
                if (alt.HasBlock3()) {
                    Ctx.Error() << "View is not supported for subqueries";
                    return false;
                }

                if (node.HasBlock4()) {
                    Ctx.Error() << "Hints are not supported for subqueries";
                    return false;
                }

                TVector<TNodePtr> values;
                values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "Apply", TNodeFlags::Default));
                values.push_back(nodePtr);
                values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "world", TNodeFlags::Default));

                TSqlExpression sqlExpr(Ctx, Mode);
                if (alt.GetBlock2().HasBlock2() && !ExprList(sqlExpr, values, alt.GetBlock2().GetBlock2().GetRule_expr_list1())) {
                    return false;
                }

                TNodePtr apply = new TAstListNodeImpl(Ctx.Pos(), std::move(values));
                if (unorderedSubquery && Ctx.UnorderedSubqueries) {
                    apply = new TCallNodeImpl(Ctx.Pos(), "UnorderedSubquery", { apply });
                }
                result.Source = BuildNodeSource(Ctx.Pos(), apply);
                return true;
            }

            TTableHints hints;
            TTableHints contextHints = GetContextHints(Ctx);
            auto ret = BuildInnerSource(Ctx.Pos(), nodePtr, service, cluster);
            if (alt.HasBlock3()) {
                auto view = Id(alt.GetBlock3().GetRule_an_id2(), *this);
                Ctx.IncrementMonCounter("sql_features", "View");
                if (!ret->SetViewName(Ctx, Ctx.Pos(), view)) {
                    return false;
                }
            }

            if (node.HasBlock4()) {
                auto tmp = TableHintsImpl(node.GetBlock4().GetRule_table_hints1());
                if (!tmp) {
                    return false;
                }

                hints = *tmp;
            }

            if (hints || contextHints) {
                if (!ret->SetTableHints(Ctx, Ctx.Pos(), hints, contextHints)) {
                    return false;
                }
            }

            result.Source = ret;
            return true;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    MergeHints(hints, tableHints);

    if (node.HasBlock4()) {
        auto tmp = TableHintsImpl(node.GetBlock4().GetRule_table_hints1());
        if (!tmp) {
            Ctx.Error() << "Failed to parse table hints";
            return false;
        }

        MergeHints(hints, *tmp);
    }

    if (!hints.empty()) {
        tr.Options = BuildInputOptions(pos, hints);
    }

    if (!tr.Keys) {
        return false;
    }

    result = tr;
    return true;
}

TMaybe<TSourcePtr> TSqlTranslation::AsTableImpl(const TRule_table_ref& node) {
    const auto& block = node.GetBlock3();

    if (block.Alt_case() == TRule_table_ref::TBlock3::kAlt2) {
        auto& alt = block.GetAlt2();
        TCiString func(Id(alt.GetRule_an_id_expr1(), *this));

        if (func == "as_table") {
            if (node.HasBlock1()) {
                Ctx.Error() << "Cluster shouldn't be specified for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (!alt.HasBlock3() || !alt.GetBlock3().GetBlock2().empty()) {
                Ctx.Error() << "Expected single argument for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (node.HasBlock4()) {
                Ctx.Error() << "No hints expected for AS_TABLE source";
                return TMaybe<TSourcePtr>(nullptr);
            }

            auto arg = TableArgImpl(alt.GetBlock3().GetRule_table_arg1());
            if (!arg) {
                return TMaybe<TSourcePtr>(nullptr);
            }

            if (arg->Expr->GetSource()) {
                Ctx.Error() << "AS_TABLE shouldn't be used for table sources";
                return TMaybe<TSourcePtr>(nullptr);
            }

            return BuildNodeSource(Ctx.Pos(), arg->Expr, true);
        }
    }

    return Nothing();
}

TMaybe<TColumnSchema> TSqlTranslation::ColumnSchemaImpl(const TRule_column_schema& node) {
    const bool nullable = !node.HasBlock4() || !node.GetBlock4().HasBlock1();
    const TString name(Id(node.GetRule_an_id_schema1(), *this));
    const TPosition pos(Context().Pos());
    const auto type = TypeNodeOrBind(node.GetRule_type_name_or_bind2());
    if (!type) {
        return {};
    }
    TVector<TIdentifier> families;
    if (node.HasBlock3()) {
        const auto& familyRelation = node.GetBlock3().GetRule_family_relation1();
        families.push_back(IdEx(familyRelation.GetRule_an_id2(), *this));
    }
    return TColumnSchema(pos, name, type, nullable, families);
}

bool TSqlTranslation::FillFamilySettingsEntry(const TRule_family_settings_entry& settingNode, TFamilyEntry& family) {
    TIdentifier id = IdEx(settingNode.GetRule_an_id1(), *this);
    const TRule_family_setting_value& value = settingNode.GetRule_family_setting_value3();
    if (to_lower(id.Name) == "data") {
        const TString stringValue(Ctx.Token(value.GetToken1()));
        family.Data = BuildLiteralSmartString(Ctx, stringValue);
    } else if (to_lower(id.Name) == "compression") {
        const TString stringValue(Ctx.Token(value.GetToken1()));
        family.Compression = BuildLiteralSmartString(Ctx, stringValue);
    } else {
        Ctx.Error() << "Unknown table setting: " << id.Name;
        return false;
    }
    return true;
}

bool TSqlTranslation::FillFamilySettings(const TRule_family_settings& settingsNode, TFamilyEntry& family) {
    // family_settings: LPAREN (family_settings_entry (COMMA family_settings_entry)*)? RPAREN;
    if (settingsNode.HasBlock2()) {
        auto& settings = settingsNode.GetBlock2();
        if (!FillFamilySettingsEntry(settings.GetRule_family_settings_entry1(), family)) {
            return false;
        }
        for (auto& block : settings.GetBlock2()) {
            if (!FillFamilySettingsEntry(block.GetRule_family_settings_entry2(), family)) {
                return false;
            }
        }
    }
    return true;
}

static bool ChangefeedSettingsEntry(const TRule_changefeed_settings_entry& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter) {
    const auto id = IdEx(node.GetRule_an_id1(), ctx);
    if (alter) {
        // currently we don't support alter settings
        ctx.Error() << to_upper(id.Name) << " alter is not supported";
        return false;
    }

    const auto& setting = node.GetRule_changefeed_setting_value3();
    auto exprNode = ctx.Build(setting.GetRule_expr1());

    if (!exprNode) {
        ctx.Context().Error(id.Pos) << "Invalid changefeed setting: " << id.Name;
        return false;
    }

    if (to_lower(id.Name) == "sink_type") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }

        const auto value = exprNode->GetLiteralValue();
        if (to_lower(value) == "local") {
            settings.SinkSettings = TChangefeedSettings::TLocalSinkSettings();
        } else {
            ctx.Context().Error() << "Unknown changefeed sink type: " << value;
            return false;
        }
    } else if (to_lower(id.Name) == "mode") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }
        settings.Mode = exprNode;
    } else if (to_lower(id.Name) == "format") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }
        settings.Format = exprNode;
    } else if (to_lower(id.Name) == "initial_scan") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.InitialScan = exprNode;
    } else if (to_lower(id.Name) == "virtual_timestamps") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "Bool") {
            ctx.Context().Error() << "Literal of Bool type is expected for " << id.Name;
            return false;
        }
        settings.VirtualTimestamps = exprNode;
    } else if (to_lower(id.Name) == "retention_period") {
        if (exprNode->GetOpName() != "Interval") {
            ctx.Context().Error() << "Literal of Interval type is expected for " << id.Name;
            return false;
        }
        settings.RetentionPeriod = exprNode;
    } else if (to_lower(id.Name) == "aws_region") {
        if (!exprNode->IsLiteral() || exprNode->GetLiteralType() != "String") {
            ctx.Context().Error() << "Literal of String type is expected for " << id.Name;
            return false;
        }
        settings.AwsRegion = exprNode;
    } else {
        ctx.Context().Error(id.Pos) << "Unknown changefeed setting: " << id.Name;
        return false;
    }

    return true;
}

static bool ChangefeedSettings(const TRule_changefeed_settings& node, TSqlExpression& ctx, TChangefeedSettings& settings, bool alter) {
    if (!ChangefeedSettingsEntry(node.GetRule_changefeed_settings_entry1(), ctx, settings, alter)) {
        return false;
    }

    for (auto& block : node.GetBlock2()) {
        if (!ChangefeedSettingsEntry(block.GetRule_changefeed_settings_entry2(), ctx, settings, alter)) {
            return false;
        }
    }

    return true;
}

static bool CreateChangefeed(const TRule_changefeed& node, TSqlExpression& ctx, TVector<TChangefeedDescription>& changefeeds) {
    changefeeds.emplace_back(IdEx(node.GetRule_an_id2(), ctx));

    if (!ChangefeedSettings(node.GetRule_changefeed_settings5(), ctx, changefeeds.back().Settings, false)) {
        return false;
    }

    return true;
}

bool TSqlTranslation::CreateTableEntry(const TRule_create_table_entry& node, TCreateTableParameters& params)
{
    switch (node.Alt_case()) {
        case TRule_create_table_entry::kAltCreateTableEntry1:
        {
            // column_schema
            auto columnSchema = ColumnSchemaImpl(node.GetAlt_create_table_entry1().GetRule_column_schema1());
            if (!columnSchema) {
                return false;
            }
            if (columnSchema->Families.size() > 1) {
                Ctx.Error() << "Several column families for a single column are not yet supported";
                return false;
            }
            params.Columns.push_back(*columnSchema);
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry2:
        {
            // table_constraint
            auto& constraint = node.GetAlt_create_table_entry2().GetRule_table_constraint1();
            switch (constraint.Alt_case()) {
                case TRule_table_constraint::kAltTableConstraint1: {
                    if (!params.PkColumns.empty()) {
                        Ctx.Error() << "PRIMARY KEY statement must be specified only once";
                        return false;
                    }
                    auto& pkConstraint = constraint.GetAlt_table_constraint1();
                    params.PkColumns.push_back(IdEx(pkConstraint.GetRule_an_id4(), *this));
                    for (auto& block : pkConstraint.GetBlock5()) {
                        params.PkColumns.push_back(IdEx(block.GetRule_an_id2(), *this));
                    }
                    break;
                }
                case TRule_table_constraint::kAltTableConstraint2: {
                    if (!params.PartitionByColumns.empty()) {
                        Ctx.Error() << "PARTITION BY statement must be specified only once";
                        return false;
                    }
                    auto& pbConstraint = constraint.GetAlt_table_constraint2();
                    params.PartitionByColumns.push_back(IdEx(pbConstraint.GetRule_an_id4(), *this));
                    for (auto& block : pbConstraint.GetBlock5()) {
                        params.PartitionByColumns.push_back(IdEx(block.GetRule_an_id2(), *this));
                    }
                    break;
                }
                case TRule_table_constraint::kAltTableConstraint3: {
                    if (!params.OrderByColumns.empty()) {
                        Ctx.Error() << "ORDER BY statement must be specified only once";
                        return false;
                    }
                    auto& obConstraint = constraint.GetAlt_table_constraint3();
                    auto extractDirection = [this] (const TRule_column_order_by_specification& spec, bool& desc) {
                        desc = false;
                        if (!spec.HasBlock2()) {
                            return true;
                        }

                        auto& token = spec.GetBlock2().GetToken1();
                        switch (token.GetId()) {
                            case SQLv1LexerTokens::TOKEN_ASC:
                                return true;
                            case SQLv1LexerTokens::TOKEN_DESC:
                                desc = true;
                                return true;
                            default:
                                Ctx.Error() << "Unsupported direction token: " << token.GetId();
                                return false;
                        }
                    };

                    bool desc = false;
                    auto& obSpec = obConstraint.GetRule_column_order_by_specification4();
                    if (!extractDirection(obSpec, desc)) {
                        return false;
                    }
                    params.OrderByColumns.push_back(std::make_pair(IdEx(obSpec.GetRule_an_id1(), *this), desc));

                    for (auto& block : obConstraint.GetBlock5()) {
                        auto& obSpec = block.GetRule_column_order_by_specification2();
                        if (!extractDirection(obSpec, desc)) {
                            return false;
                        }
                        params.OrderByColumns.push_back(std::make_pair(IdEx(obSpec.GetRule_an_id1(), *this), desc));
                    }
                    break;
                }
                default:
                    AltNotImplemented("table_constraint", constraint);
                    return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry3:
        {
            // table_index
            auto& table_index = node.GetAlt_create_table_entry3().GetRule_table_index1();
            if (!CreateTableIndex(table_index, *this, params.Indexes)) {
                return false;
            }
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry4:
        {
            // family_entry
            auto& family_entry = node.GetAlt_create_table_entry4().GetRule_family_entry1();
            TFamilyEntry family(IdEx(family_entry.GetRule_an_id2(), *this));
            if (!FillFamilySettings(family_entry.GetRule_family_settings3(), family)) {
                return false;
            }
            params.ColumnFamilies.push_back(family);
            break;
        }
        case TRule_create_table_entry::kAltCreateTableEntry5:
        {
            // changefeed
            auto& changefeed = node.GetAlt_create_table_entry5().GetRule_changefeed1();
            TSqlExpression expr(Ctx, Mode);
            if (!CreateChangefeed(changefeed, expr, params.Changefeeds)) {
                return false;
            }
            break;
        }
        default:
            AltNotImplemented("create_table_entry", node);
            return false;
    }
    return true;
}

TNodePtr LiteralNumber(TContext& ctx, const TRule_integer& node);

namespace {
    bool StoreId(const TRule_table_setting_value& from, TMaybe<TIdentifier>& to, TTranslation& ctx) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue1: {
            // id
            to = IdEx(from.GetAlt_table_setting_value1().GetRule_id1(), ctx);
            break;
        }
        default:
            return false;
        }
        return true;
    }

    bool StoreString(const TRule_table_setting_value& from, TNodePtr& to, TContext& ctx) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2: {
            // STRING_VALUE
            const TString stringValue(ctx.Token(from.GetAlt_table_setting_value2().GetToken1()));
            to = BuildLiteralSmartString(ctx, stringValue);
            break;
        }
        default:
            return false;
        }
        return true;
    }

    bool StoreString(const TRule_table_setting_value& from, TDeferredAtom& to, TContext& ctx, const TString& errorPrefix = {}) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue2: {
            // STRING_VALUE
            const TString stringValue(ctx.Token(from.GetAlt_table_setting_value2().GetToken1()));
            auto unescaped = StringContent(ctx, ctx.Pos(), stringValue);
            if (!unescaped) {
                ctx.Error() << errorPrefix << " value cannot be unescaped";
                return false;
            }
            to = TDeferredAtom(ctx.Pos(), unescaped->Content);
            break;
        }
        default:
            ctx.Error() << errorPrefix << " value should be a string literal";
            return false;
        }
        return true;
    }

    bool StoreInt(const TRule_table_setting_value& from, TNodePtr& to, TContext& ctx) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue3: {
            // integer
            to = LiteralNumber(ctx, from.GetAlt_table_setting_value3().GetRule_integer1());
            break;
        }
        default:
            return false;
        }
        return true;
    }

    bool StoreSplitBoundary(const TRule_literal_value_list& boundary, TVector<TVector<TNodePtr>>& to,
            TSqlExpression& expr, TContext& ctx) {
        TVector<TNodePtr> boundaryKeys;
        auto first_key = expr.LiteralExpr(boundary.GetRule_literal_value2());
        if (!first_key) {
            ctx.Error() << "Empty key in partition at keys";
            return false;
        }
        if (!first_key->Expr) {
            ctx.Error() << "Identifier is not expected in partition at keys";
            return false;
        }
        boundaryKeys.emplace_back(first_key->Expr);
        for (auto& key : boundary.GetBlock3()) {
            auto keyExprOrIdent = expr.LiteralExpr(key.GetRule_literal_value2());
            if (!keyExprOrIdent) {
                ctx.Error() << "Empty key in partition at keys";
                return false;
            }
            if (!keyExprOrIdent->Expr) {
                ctx.Error() << "Identifier is not expected in partition at keys";
                return false;
            }
            boundaryKeys.emplace_back(keyExprOrIdent->Expr);
        }
        to.push_back(boundaryKeys);
        return true;
    }

    bool StoreSplitBoundaries(const TRule_table_setting_value& from, TVector<TVector<TNodePtr>>& to,
            TSqlExpression& expr, TContext& ctx) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue4: {
            // split_boundaries
            const auto& boundariesNode = from.GetAlt_table_setting_value4().GetRule_split_boundaries1();
            switch (boundariesNode.Alt_case()) {
            case TRule_split_boundaries::kAltSplitBoundaries1: {
                // literal_value_list (COMMA literal_value_list)*
                auto& complexBoundaries = boundariesNode.GetAlt_split_boundaries1();

                auto& first_boundary = complexBoundaries.GetRule_literal_value_list2();
                if (!StoreSplitBoundary(first_boundary, to, expr, ctx)) {
                    return false;
                }

                for (auto& boundary : complexBoundaries.GetBlock3()) {
                    if (!StoreSplitBoundary(boundary.GetRule_literal_value_list2(), to, expr, ctx)) {
                        return false;
                    }
                }
                break;
            }
            case TRule_split_boundaries::kAltSplitBoundaries2: {
                // literal_value_list
                auto& simpleBoundaries = boundariesNode.GetAlt_split_boundaries2().GetRule_literal_value_list1();
                auto first_key = expr.LiteralExpr(simpleBoundaries.GetRule_literal_value2());
                if (!first_key) {
                    ctx.Error() << "Empty key in partition at keys";
                    return false;
                }
                if (!first_key->Expr) {
                    ctx.Error() << "Identifier is not expected in partition at keys";
                    return false;
                }
                to.push_back(TVector<TNodePtr>(1, first_key->Expr));
                for (auto& key : simpleBoundaries.GetBlock3()) {
                    auto keyExprOrIdent = expr.LiteralExpr(key.GetRule_literal_value2());
                    if (!keyExprOrIdent) {
                        ctx.Error() << "Empty key in partition at keys";
                        return false;
                    }
                    if (!first_key->Expr) {
                        ctx.Error() << "Identifier is not expected in partition at keys";
                        return false;
                    }
                    to.push_back(
                        TVector<TNodePtr>(1, keyExprOrIdent->Expr)
                    );
                }
                break;
            }
            default:
                return false;
            }
            break;
        }
        default:
            return false;
        }
        return true;
    }

    bool StoreTtlSettings(const TRule_table_setting_value& from, TResetableSetting<TTtlSettings, void>& to,
            TSqlExpression& expr, TContext& ctx, TTranslation& txc) {
        switch (from.Alt_case()) {
        case TRule_table_setting_value::kAltTableSettingValue5: {
            auto columnName = IdEx(from.GetAlt_table_setting_value5().GetRule_an_id3(), txc);
            auto exprNode = expr.Build(from.GetAlt_table_setting_value5().GetRule_expr1());
            if (!exprNode) {
                return false;
            }

            if (exprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for TTL";
                return false;
            }

            to.Set(TTtlSettings(columnName, exprNode));
            break;
        }
        default:
            return false;
        }
        return true;
    }

    bool WithoutAlpha(const std::string_view& literal) {
        return literal.cend() == std::find_if(literal.cbegin(), literal.cend(), [](char c) { return std::isalpha(c) || (c & '\x80'); });
    }

    template<typename TChar>
    struct TPatternComponent {
        TBasicString<TChar> Prefix;
        TBasicString<TChar> Suffix;
        bool IsSimple = true;

        void AppendPlain(TChar c) {
            if (IsSimple) {
                Prefix.push_back(c);
            }
            Suffix.push_back(c);
        }

        void AppendAnyChar() {
            IsSimple = false;
            Suffix.clear();
        }
    };

    template<typename TChar>
    TVector<TPatternComponent<TChar>> SplitPattern(const TBasicString<TChar>& pattern, TMaybe<char> escape, bool& inEscape) {
        inEscape = false;
        TVector<TPatternComponent<TChar>> result;
        TPatternComponent<TChar> current;
        bool prevIsPercentChar = false;
        for (const TChar c : pattern) {
            if (inEscape) {
                current.AppendPlain(c);
                inEscape = false;
                prevIsPercentChar = false;
            } else if (escape && c == static_cast<TChar>(*escape)) {
                inEscape = true;
            } else if (c == '%') {
                if (!prevIsPercentChar) {
                    result.push_back(std::move(current));
                }
                current = {};
                prevIsPercentChar = true;
            } else if (c == '_') {
                current.AppendAnyChar();
                prevIsPercentChar = false;
            } else {
                current.AppendPlain(c);
                prevIsPercentChar = false;
            }
        }
        result.push_back(std::move(current));
        return result;
    }
}

bool TSqlTranslation::StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value,
        TTableSettings& settings, ETableType tableType, bool alter, bool reset) {
    switch (tableType) {
    case ETableType::ExternalTable:
        return StoreExternalTableSettingsEntry(id, value, settings);
    case ETableType::Table:
    case ETableType::TableStore:
        return StoreTableSettingsEntry(id, value, settings, alter, reset);
    }
}

bool TSqlTranslation::StoreExternalTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, TTableSettings& settings) {
    if (to_lower(id.Name) == "data_source") {
        TDeferredAtom dataSource;
        if (!StoreString(*value, dataSource, Ctx, to_upper(id.Name))) {
            return false;
        }
        TString service = Context().Scoped->CurrService;
        TDeferredAtom cluster = Context().Scoped->CurrCluster;
        TNodePtr root = new TAstListNodeImpl(Ctx.Pos());
        root->Add("String", Ctx.GetPrefixedPath(service, cluster, dataSource));
        settings.DataSourcePath = root;
    } else if (to_lower(id.Name) == "location") {
        if (!StoreString(*value, settings.Location, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else {
        settings.ExternalSourceParameters.emplace_back(id, nullptr);
        auto& parameter = settings.ExternalSourceParameters.back();
        if (!StoreString(*value, parameter.second, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value,
        TTableSettings& settings, bool alter, bool reset) {
    YQL_ENSURE(value || reset);
    YQL_ENSURE(!reset || reset & alter);
    if (to_lower(id.Name) == "compaction_policy") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.CompactionPolicy, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_by_size") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.AutoPartitioningBySize, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_partition_size_mb") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.PartitionSizeMb, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_by_load") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.AutoPartitioningByLoad, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_min_partitions_count") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.MinPartitions, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "auto_partitioning_max_partitions_count") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.MaxPartitions, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "uniform_partitions") {
        if (alter) {
            Ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        if (!StoreInt(*value, settings.UniformPartitions, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an integer";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_at_keys") {
        if (alter) {
            Ctx.Error() << to_upper(id.Name) << " alter is not supported";
            return false;
        }
        TSqlExpression expr(Ctx, Mode);
        if (!StoreSplitBoundaries(*value, settings.PartitionAtKeys, expr, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a list of keys. "
                << "Example1: (10, 1000)  Example2: ((10), (1000, \"abc\"))";
            return false;
        }
    } else if (to_lower(id.Name) == "key_bloom_filter") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.KeyBloomFilter, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "read_replicas_settings") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.ReadReplicasSettings, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else if (to_lower(id.Name) == "ttl") {
        if (!reset) {
            TSqlExpression expr(Ctx, Mode);
            if (!StoreTtlSettings(*value, settings.TtlSettings, expr, Ctx, *this)) {
                Ctx.Error() << "Invalid TTL settings";
                return false;
            }
        } else {
            settings.TtlSettings.Reset();
        }
    } else if (to_lower(id.Name) == "tiering") {
        if (!reset) {
            TNodePtr tieringNode;
            if (!StoreString(*value, tieringNode, Ctx)) {
                Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.Tiering.Set(tieringNode);
        } else {
            settings.Tiering.Reset();
        }
    } else if (to_lower(id.Name) == "store") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreId(*value, settings.StoreType, *this)) {
            Ctx.Error() << to_upper(id.Name) << " value should be an identifier";
            return false;
        }
    } else if (to_lower(id.Name) == "partition_by_hash_function") {
        if (reset) {
            Ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!StoreString(*value, settings.PartitionByHashFunction, Ctx)) {
            Ctx.Error() << to_upper(id.Name) << " value should be a string literal";
            return false;
        }
    } else {
        Ctx.Error() << "Unknown table setting: " << id.Name;
        return false;
    }
    return true;
}

bool TSqlTranslation::StoreTableSettingsEntry(const TIdentifier& id, const TRule_table_setting_value& value,
        TTableSettings& settings, ETableType tableType, bool alter) {
    return StoreTableSettingsEntry(id, &value, settings, tableType, alter, false);
}

bool TSqlTranslation::ResetTableSettingsEntry(const TIdentifier& id, TTableSettings& settings, ETableType tableType) {
    return StoreTableSettingsEntry(id, nullptr, settings, tableType, true, true);
}

bool TSqlTranslation::CreateTableSettings(const TRule_with_table_settings& settingsNode, TCreateTableParameters& params) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreTableSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), firstEntry.GetRule_table_setting_value3(),
            params.TableSettings, params.TableType)) {
        return false;
    }
    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreTableSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), entry.GetRule_table_setting_value3(), params.TableSettings, params.TableType)) {
            return false;
        }
    }
    return true;
}

bool StoreConsumerSettingsEntry(
        const TIdentifier& id, const TRule_topic_consumer_setting_value* value, TSqlExpression& ctx,
        TTopicConsumerSettings& settings,
        bool reset
) {
    YQL_ENSURE(value || reset);
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = ctx.Build(value->GetRule_expr1());
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }
    if (to_lower(id.Name) == "important") {
        if (settings.Important) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in ALTER CONSUMER statements for single consumer";
            return false;
        }
        if (reset) {
            ctx.Error() << to_upper(id.Name) << " reset is not supported";
            return false;
        }
        if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "Bool") {
            ctx.Error() << to_upper(id.Name) << " value should be boolean";
            return false;
        }
        settings.Important = valueExprNode;

    } else if (to_lower(id.Name) == "read_from") {
        if (settings.ReadFromTs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in ALTER CONSUMER statements for single consumer";
            return false;
        }
        if (reset) {
            settings.ReadFromTs.Reset();
        } else {
            //ToDo: !! validate
            settings.ReadFromTs.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "supported_codecs") {
        if (settings.SupportedCodecs) {
            ctx.Error() << to_upper(id.Name) << " specified multiple times in ALTER CONSUMER statements for single consumer";
            return false;
        }
        if (reset) {
            settings.SupportedCodecs.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be a string literal";
                return false;
            }
            settings.SupportedCodecs.Set(valueExprNode);
        }
    } else {
        ctx.Error() << to_upper(id.Name) << ": unknown option for consumer";
        return false;
    }
    return true;
}

TIdentifier TSqlTranslation::GetTopicConsumerId(const TRule_topic_consumer_ref& node) {
    return IdEx(node.GetRule_an_id_pure1(), *this);
}

bool TSqlTranslation::CreateConsumerSettings(
        const TRule_topic_consumer_settings& node, TTopicConsumerSettings& settings
) {
    const auto& firstEntry = node.GetRule_topic_consumer_settings_entry1();
    TSqlExpression expr(Ctx, Mode);
    if (!StoreConsumerSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_consumer_setting_value3(),
            expr, settings, false
    )) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_consumer_settings_entry2();
        if (!StoreConsumerSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_consumer_setting_value3(),
                expr, settings, false
        )) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::CreateTopicConsumer(
        const TRule_topic_create_consumer_entry& node,
        TVector<TTopicConsumerDescription>& consumers
) {
    consumers.emplace_back(IdEx(node.GetRule_an_id2(), *this));

    if (node.HasBlock3()) {
        auto& settings = node.GetBlock3().GetRule_topic_consumer_with_settings1().GetRule_topic_consumer_settings3();
        if (!CreateConsumerSettings(settings, consumers.back().Settings)) {
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::AlterTopicConsumerEntry(
        const TRule_alter_topic_alter_consumer_entry& node, TTopicConsumerDescription& alterConsumer
) {
    switch (node.Alt_case()) {
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry1:
            return CreateConsumerSettings(
                    node.GetAlt_alter_topic_alter_consumer_entry1().GetRule_topic_alter_consumer_set1()
                            .GetRule_topic_consumer_settings3(),
                    alterConsumer.Settings
            );
        //case TRule_alter_topic_alter_consumer_entry::ALT_NOT_SET:
        case TRule_alter_topic_alter_consumer_entry::kAltAlterTopicAlterConsumerEntry2: {
            auto& resetNode = node.GetAlt_alter_topic_alter_consumer_entry2().GetRule_topic_alter_consumer_reset1();
            TSqlExpression expr(Ctx, Mode);
            if (!StoreConsumerSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    nullptr,
                    expr, alterConsumer.Settings, true
            )) {
                return false;
            }

            for (auto& resetItem: resetNode.GetBlock4()) {
                if (!StoreConsumerSettingsEntry(
                        IdEx(resetItem.GetRule_an_id2(), *this),
                        nullptr,
                        expr, alterConsumer.Settings, true
                )) {
                    return false;
                }
            }
            return true;
        }
        default:
            Ctx.Error() << "unknown alter consumer action";
            return false;
    }
    return true;
}

bool TSqlTranslation::AlterTopicConsumer(
        const TRule_alter_topic_alter_consumer& node,
        THashMap<TString, TTopicConsumerDescription>& alterConsumers
) {
    auto consumerId = GetTopicConsumerId(node.GetRule_topic_consumer_ref3());
    TString name = to_lower(consumerId.Name);
    auto iter = alterConsumers.insert(std::make_pair(
            name, TTopicConsumerDescription(std::move(consumerId))
    )).first;
    if (!AlterTopicConsumerEntry(node.GetRule_alter_topic_alter_consumer_entry4(), iter->second)) {
        return false;
    }
    return true;
}

bool TSqlTranslation::CreateTopicEntry(const TRule_create_topic_entry& node, TCreateTopicParameters& params) {
    // Will need a switch() here if (ever) create_topic_entry gets more than 1 type of statement
    auto& consumer = node.GetRule_topic_create_consumer_entry1();
    if (!CreateTopicConsumer(consumer, params.Consumers)) {
        return false;
    }
    return true;
}

static bool StoreTopicSettingsEntry(
        const TIdentifier& id, const TRule_topic_setting_value* value, TSqlExpression& ctx,
        TTopicSettings& settings, bool reset
) {
    YQL_ENSURE(value || reset);
    TNodePtr valueExprNode;
    if (value) {
        valueExprNode = ctx.Build(value->GetRule_expr1());
        if (!valueExprNode) {
            ctx.Error() << "invalid value for setting: " << id.Name;
            return false;
        }
    }

    if (to_lower(id.Name) == "min_active_partitions") {
        if (reset) {
            settings.MinPartitions.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.MinPartitions.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_count_limit") {
        if (reset) {
            settings.PartitionsLimit.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionsLimit.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "retention_period") {
        if (reset) {
            settings.RetentionPeriod.Reset();
        } else {
            if (valueExprNode->GetOpName() != "Interval") {
                ctx.Error() << "Literal of Interval type is expected for retention";
                return false;
            }
            settings.RetentionPeriod.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "retention_storage_mb") {
        if (reset) {
            settings.RetentionStorage.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.RetentionStorage.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_write_speed_bytes_per_second") {
        if (reset) {
            settings.PartitionWriteSpeed.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionWriteSpeed.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "partition_write_burst_bytes") {
        if (reset) {
            settings.PartitionWriteBurstSpeed.Reset();
        } else {
            if (!valueExprNode->IsIntegerLiteral()) {
                ctx.Error() << to_upper(id.Name) << " value should be an integer";
                return false;
            }
            settings.PartitionWriteBurstSpeed.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "metering_mode") {
        if (reset) {
            settings.MeteringMode.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.MeteringMode.Set(valueExprNode);
        }
    } else if (to_lower(id.Name) == "supported_codecs") {
        if (reset) {
            settings.SupportedCodecs.Reset();
        } else {
            if (!valueExprNode->IsLiteral() || valueExprNode->GetLiteralType() != "String") {
                ctx.Error() << to_upper(id.Name) << " value should be string";
                return false;
            }
            settings.SupportedCodecs.Set(valueExprNode);
        }
    } else {
        ctx.Error() << "unknown topic setting: " << id.Name;
        return false;
    }
    return true;
}

bool TSqlTranslation::AlterTopicAction(const TRule_alter_topic_action& node, TAlterTopicParameters& params) {
//    alter_topic_action:
//    alter_topic_add_consumer
//    | alter_topic_alter_consumer
//    | alter_topic_drop_consumer
//    | alter_topic_set_settings
//    | alter_topic_reset_settings

    switch (node.Alt_case()) {
        case TRule_alter_topic_action::kAltAlterTopicAction1: // alter_topic_add_consumer
            return CreateTopicConsumer(
                    node.GetAlt_alter_topic_action1().GetRule_alter_topic_add_consumer1()
                                                     .GetRule_topic_create_consumer_entry2(),
                    params.AddConsumers
           );

        case TRule_alter_topic_action::kAltAlterTopicAction2: // alter_topic_alter_consumer
            return AlterTopicConsumer(
                    node.GetAlt_alter_topic_action2().GetRule_alter_topic_alter_consumer1(),
                    params.AlterConsumers
            );

        case TRule_alter_topic_action::kAltAlterTopicAction3: // drop_consumer
            params.DropConsumers.emplace_back(GetTopicConsumerId(
                    node.GetAlt_alter_topic_action3().GetRule_alter_topic_drop_consumer1()
                                                     .GetRule_topic_consumer_ref3()
            ));
            return true;

        case TRule_alter_topic_action::kAltAlterTopicAction4: // set_settings
            return CreateTopicSettings(
                    node.GetAlt_alter_topic_action4().GetRule_alter_topic_set_settings1()
                                                     .GetRule_topic_settings3(),
                    params.TopicSettings
            );

        case TRule_alter_topic_action::kAltAlterTopicAction5: { // reset_settings
            auto& resetNode = node.GetAlt_alter_topic_action5().GetRule_alter_topic_reset_settings1();
            TSqlExpression expr(Ctx, Mode);
            if (!StoreTopicSettingsEntry(
                    IdEx(resetNode.GetRule_an_id3(), *this),
                    nullptr, expr,
                    params.TopicSettings, true
            )) {
                return false;
            }

            for (auto& resetItem: resetNode.GetBlock4()) {
                if (!StoreTopicSettingsEntry(
                        IdEx(resetItem.GetRule_an_id_pure2(), *this),
                        nullptr, expr,
                        params.TopicSettings, true
                )) {
                    return false;
                }
            }
            return true;
        }
        default:
            Ctx.Error() << "unknown alter topic action";
            return false;
    }
    return true;
}

bool TSqlTranslation::CreateTopicSettings(const TRule_topic_settings& node, TTopicSettings& settings) {
    const auto& firstEntry = node.GetRule_topic_settings_entry1();
    TSqlExpression expr(Ctx, Mode);

    if (!StoreTopicSettingsEntry(
            IdEx(firstEntry.GetRule_an_id1(), *this),
            &firstEntry.GetRule_topic_setting_value3(),
            expr, settings, false
    )) {
        return false;
    }
    for (auto& block : node.GetBlock2()) {
        const auto& entry = block.GetRule_topic_settings_entry2();
        if (!StoreTopicSettingsEntry(
                IdEx(entry.GetRule_an_id1(), *this),
                &entry.GetRule_topic_setting_value3(),
                expr, settings, false
        )) {
            return false;
        }
    }
    return true;
}

bool ParseNumbers(TContext& ctx, const TString& strOrig, ui64& value, TString& suffix);

TNodePtr TSqlTranslation::IntegerOrBind(const TRule_integer_or_bind& node) {
    switch (node.Alt_case()) {
        case TRule_integer_or_bind::kAltIntegerOrBind1: {
            const TString intString = Ctx.Token(node.GetAlt_integer_or_bind1().GetRule_integer1().GetToken1());
            ui64 value;
            TString suffix;
            if (!ParseNumbers(Ctx, intString, value, suffix)) {
                return {};
            }
            return BuildQuotedAtom(Ctx.Pos(), ToString(value), TNodeFlags::ArbitraryContent);
        }
        case TRule_integer_or_bind::kAltIntegerOrBind2: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_integer_or_bind2().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }
            auto atom = MakeAtomFromExpression(Ctx, namedNode);
            return atom.Build();
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlTranslation::TypeNameTag(const TRule_type_name_tag& node) {
    switch (node.Alt_case()) {
        case TRule_type_name_tag::kAltTypeNameTag1: {
            auto content = Id(node.GetAlt_type_name_tag1().GetRule_id1(), *this);
            auto atom = TDeferredAtom(Ctx.Pos(), content);
            return atom.Build();
        }
        case TRule_type_name_tag::kAltTypeNameTag2: {
            auto value = Token(node.GetAlt_type_name_tag2().GetToken1());
            auto parsed = StringContentOrIdContent(Ctx, Ctx.Pos(), value);
            if (!parsed) {
                return {};
            }
            auto atom = TDeferredAtom(Ctx.Pos(), parsed->Content);
            return atom.Build();
        }
        case TRule_type_name_tag::kAltTypeNameTag3: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_type_name_tag3().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }
            TDeferredAtom atom;
            MakeTableFromExpression(Ctx, namedNode, atom);
            return atom.Build();
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlTranslation::TypeSimple(const TRule_type_name_simple& node, bool onlyDataAllowed) {
    const TString origName = Id(node.GetRule_an_id_pure1(), *this);
    if (origName.empty()) {
        return {};
    }
    return BuildSimpleType(Ctx, Ctx.Pos(), origName, onlyDataAllowed);
}

TNodePtr TSqlTranslation::TypeDecimal(const TRule_type_name_decimal& node) {
    auto pos = Ctx.Pos();
    auto flags = TNodeFlags::Default;

    auto paramOne = IntegerOrBind(node.GetRule_integer_or_bind3());
    if (!paramOne) {
        return {};
    }
    auto paramTwo = IntegerOrBind(node.GetRule_integer_or_bind5());
    if (!paramTwo) {
        return {};
    }
    return new TCallNodeImpl(pos, "DataType", { BuildQuotedAtom(pos, "Decimal", flags), paramOne, paramTwo });
}

TNodePtr TSqlTranslation::AddOptionals(const TNodePtr& node, size_t optionalCount) {
    TNodePtr result = node;
    if (node) {
        TPosition pos = node->GetPos();
        for (size_t i = 0; i < optionalCount; ++i) {
            result = new TCallNodeImpl(pos, "OptionalType", { result });
        }
    }
    return result;
}


TMaybe<std::pair<TVector<TNodePtr>, bool>> TSqlTranslation::CallableArgList(const TRule_callable_arg_list& argList, bool namedArgsStarted) {
    auto pos = Ctx.Pos();
    auto flags = TNodeFlags::Default;
    auto& arg1 = argList.GetRule_callable_arg1();
    auto& varArg = arg1.GetRule_variant_arg1();
    TVector<TNodePtr> result;
    TVector<TNodePtr> items;
    auto typeNode = TypeNodeOrBind(varArg.GetRule_type_name_or_bind2());
    if (!typeNode) {
        return {};
    }
    items.push_back(typeNode);
    if (varArg.HasBlock1()) {
        namedArgsStarted = true;
        auto tag = TypeNameTag(varArg.GetBlock1().GetRule_type_name_tag1());
        if (!tag) {
            return {};
        }
        items.push_back(tag);
    }
    if (arg1.HasBlock2()) {
        if (!varArg.HasBlock1()) {
            items.push_back(BuildQuotedAtom(pos, "", flags));
        }
        items.push_back(BuildQuotedAtom(pos, "1", flags));
    }
    result.push_back(new TAstListNodeImpl(pos, items));

    for (auto& arg : argList.GetBlock2()) {
        auto& varArg = arg.GetRule_callable_arg2().GetRule_variant_arg1();
        TVector<TNodePtr> items;
        auto typeNode = TypeNodeOrBind(varArg.GetRule_type_name_or_bind2());
        if (!typeNode) {
            return {};
        }
        items.push_back(typeNode);
        if (varArg.HasBlock1()) {
            auto tag = TypeNameTag(varArg.GetBlock1().GetRule_type_name_tag1());
            if (!tag) {
                return {};
            }
            items.push_back(tag);
        } else {
            if (namedArgsStarted) {
                Ctx.Error() << "Expected named argument, previous argument was named";
                return {};
            }
            items.push_back(BuildQuotedAtom(pos, "", flags));
        }
        if (arg.GetRule_callable_arg2().HasBlock2()) {
            if (!varArg.HasBlock1()) {
                items.push_back(BuildQuotedAtom(pos, "", flags));
            }
            items.push_back(BuildQuotedAtom(pos, "1", flags));
        }
        result.push_back(new TAstListNodeImpl(pos, items));
    }
    return std::make_pair(result, namedArgsStarted);
}

TNodePtr TSqlTranslation::TypeNodeOrBind(const TRule_type_name_or_bind& node) {
    switch (node.Alt_case()) {
        case TRule_type_name_or_bind::kAltTypeNameOrBind1: {
            return TypeNode(node.GetAlt_type_name_or_bind1().GetRule_type_name1());
        }
        case TRule_type_name_or_bind::kAltTypeNameOrBind2: {
            TString bindName;
            if (!NamedNodeImpl(node.GetAlt_type_name_or_bind2().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            return GetNamedNode(bindName);
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlTranslation::TypeNode(const TRule_type_name& node) {
    //type_name:
    //    type_name_composite
    //  | (type_name_decimal | type_name_simple) QUESTION*;
    if (node.Alt_case() == TRule_type_name::kAltTypeName1) {
        return TypeNode(node.GetAlt_type_name1().GetRule_type_name_composite1());
    }

    TNodePtr result;
    TPosition pos = Ctx.Pos();

    auto& alt = node.GetAlt_type_name2();
    auto& block = alt.GetBlock1();
    switch (block.Alt_case()) {
        case TRule_type_name::TAlt2::TBlock1::kAlt1: {
            auto& decimalType = block.GetAlt1().GetRule_type_name_decimal1();
            result = TypeDecimal(decimalType);
            break;
        }
        case TRule_type_name::TAlt2::TBlock1::kAlt2: {
            auto& simpleType = block.GetAlt2().GetRule_type_name_simple1();
            result = TypeSimple(simpleType, false);
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    return AddOptionals(result, alt.GetBlock2().size());
}

TNodePtr TSqlTranslation::TypeNode(const TRule_type_name_composite& node) {
    //type_name_composite:
    //  ( type_name_optional
    //  | type_name_tuple
    //  | type_name_struct
    //  | type_name_variant
    //  | type_name_list
    //  | type_name_stream
    //  | type_name_flow
    //  | type_name_dict
    //  | type_name_set
    //  | type_name_enum
    //  | type_name_resource
    //  | type_name_tagged
    //  | type_name_callable
    //  ) QUESTION*;
    TNodePtr result;
    TPosition pos = Ctx.Pos();
    auto flags = TNodeFlags::Default;

    auto wrapOneParamType = [&] (const TRule_type_name_or_bind& param, const char* type) -> TNodePtr {
        auto node = TypeNodeOrBind(param);
        return node ? new TAstListNodeImpl(pos, { BuildAtom(pos, type, flags), node }) : nullptr;
    };
    auto makeVoid = [&] () -> TNodePtr {
        return new TAstListNodeImpl(pos, { BuildAtom(pos, "VoidType", flags) });
    };
    auto makeQuote = [&] (const TNodePtr& node) -> TNodePtr {
        return new TAstListNodeImpl(pos, { new TAstAtomNodeImpl(pos, "quote", 0), node });
    };

    auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TRule_type_name_composite_TBlock1::kAlt1: {
            auto& optionalType = block.GetAlt1().GetRule_type_name_optional1();
            result = wrapOneParamType(optionalType.GetRule_type_name_or_bind3(), "OptionalType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt2: {
            auto& tupleType = block.GetAlt2().GetRule_type_name_tuple1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "TupleType", flags));

            switch (tupleType.GetBlock2().Alt_case()) {
            case TRule_type_name_tuple::TBlock2::kAlt1: {
                if (tupleType.GetBlock2().GetAlt1().HasBlock2()) {
                    auto typeNode = TypeNodeOrBind(tupleType.GetBlock2().GetAlt1().GetBlock2().GetRule_type_name_or_bind1());
                    if (!typeNode) {
                        return {};
                    }
                    items.push_back(typeNode);
                    for (auto& arg : tupleType.GetBlock2().GetAlt1().GetBlock2().GetBlock2()) {
                        auto typeNode = TypeNodeOrBind(arg.GetRule_type_name_or_bind2());
                        if (!typeNode) {
                            return {};
                        }
                        items.push_back(typeNode);
                    }
                }
                [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
            }
            case TRule_type_name_tuple::TBlock2::kAlt2:
                break;
            default:
                Y_FAIL("You should change implementation according to grammar changes");
            }

            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt3: {
            auto& structType = block.GetAlt3().GetRule_type_name_struct1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "StructType", flags));

            switch (structType.GetBlock2().Alt_case()) {
                case TRule_type_name_struct::TBlock2::kAlt1: {
                    if (structType.GetBlock2().GetAlt1().HasBlock2()) {
                        auto& structArg = structType.GetBlock2().GetAlt1().GetBlock2().GetRule_struct_arg1();
                        auto typeNode = TypeNodeOrBind(structArg.GetRule_type_name_or_bind3());
                        if (!typeNode) {
                            return {};
                        }
                        auto tag = TypeNameTag(structArg.GetRule_type_name_tag1());
                        if (!tag) {
                            return {};
                        }

                        items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
                        for (auto& arg : structType.GetBlock2().GetAlt1().GetBlock2().GetBlock2()) {
                            auto typeNode = TypeNodeOrBind(arg.GetRule_struct_arg2().GetRule_type_name_or_bind3());
                            if (!typeNode) {
                                return {};
                            }
                            auto tag = TypeNameTag(arg.GetRule_struct_arg2().GetRule_type_name_tag1());
                            if (!tag) {
                                return {};
                            }
                            items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
                        }
                    }
                    [[fallthrough]]; // AUTOGENERATED_FALLTHROUGH_FIXME
                }
                case TRule_type_name_struct::TBlock2::kAlt2:
                    break;
                default:
                    Y_FAIL("You should change implementation according to grammar changes");
            }

            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt4: {
            auto& variantType = block.GetAlt4().GetRule_type_name_variant1();
            TVector<TNodePtr> items;
            bool overStruct = false;
            auto& variantArg = variantType.GetRule_variant_arg3();
            auto typeNode = TypeNodeOrBind(variantArg.GetRule_type_name_or_bind2());
            if (!typeNode) {
                return {};
            }
            if (variantArg.HasBlock1()) {
                items.push_back(BuildAtom(pos, "StructType", flags));
                overStruct = true;
                auto tag = TypeNameTag(variantArg.GetBlock1().GetRule_type_name_tag1());
                if (!tag) {
                    return {};
                }
                items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
            } else {
                items.push_back(BuildAtom(pos, "TupleType", flags));
                items.push_back(typeNode);
            }

            for (auto& arg : variantType.GetBlock4()) {
                auto typeNode = TypeNodeOrBind(arg.GetRule_variant_arg2().GetRule_type_name_or_bind2());
                if (!typeNode) {
                    return {};
                }
                if (overStruct) {
                    if (!arg.GetRule_variant_arg2().HasBlock1()) {
                        Ctx.Error() << "Variant over struct and tuple mixture";
                        return {};
                    }
                    auto tag = TypeNameTag(arg.GetRule_variant_arg2().GetBlock1().GetRule_type_name_tag1());
                    if (!tag) {
                        return {};
                    }
                    items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, typeNode })));
                } else {
                    if (arg.GetRule_variant_arg2().HasBlock1()) {
                        Ctx.Error() << "Variant over struct and tuple mixture";
                        return {};
                    }
                    items.push_back(typeNode);
                }
            }
            typeNode = new TAstListNodeImpl(pos, items);
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "VariantType", flags), typeNode });
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt5: {
            auto& listType = block.GetAlt5().GetRule_type_name_list1();
            result = wrapOneParamType(listType.GetRule_type_name_or_bind3(), "ListType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt6: {
            auto& streamType = block.GetAlt6().GetRule_type_name_stream1();
            result = wrapOneParamType(streamType.GetRule_type_name_or_bind3(), "StreamType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt7: {
            auto& flowType = block.GetAlt7().GetRule_type_name_flow1();
            result = wrapOneParamType(flowType.GetRule_type_name_or_bind3(), "FlowType");
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt8: {
            auto& dictType = block.GetAlt8().GetRule_type_name_dict1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "DictType", flags));
            auto typeNode = TypeNodeOrBind(dictType.GetRule_type_name_or_bind3());
            if (!typeNode) {
                return {};
            }
            items.push_back(typeNode);
            typeNode = TypeNodeOrBind(dictType.GetRule_type_name_or_bind5());
            if (!typeNode) {
                return {};
            }
            items.push_back(typeNode);
            result = new TAstListNodeImpl(pos, items);
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt9: {
            auto& setType = block.GetAlt9().GetRule_type_name_set1();
            auto typeNode = TypeNodeOrBind(setType.GetRule_type_name_or_bind3());
            if (!typeNode) {
                return {};
            }
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "DictType", flags), typeNode, makeVoid() });
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt10: {
            auto& enumType = block.GetAlt10().GetRule_type_name_enum1();
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "StructType", flags));
            auto tag = TypeNameTag(enumType.GetRule_type_name_tag3());
            if (!tag) {
                return {};
            }
            items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, makeVoid() })));
            for (auto& arg : enumType.GetBlock4()) {
                auto tag = TypeNameTag(arg.GetRule_type_name_tag2());
                if (!tag) {
                    return {};
                }
                items.push_back(makeQuote(new TAstListNodeImpl(pos, { tag, makeVoid() })));
            }
            auto typeNode = new TAstListNodeImpl(pos, items);
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "VariantType", flags), typeNode });
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt11: {
            auto& resourceType = block.GetAlt11().GetRule_type_name_resource1();
            auto tag = TypeNameTag(resourceType.GetRule_type_name_tag3());
            if (!tag) {
                return {};
            }
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "ResourceType", flags), tag });
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt12: {
            auto& taggedType = block.GetAlt12().GetRule_type_name_tagged1();
            auto typeNode = TypeNodeOrBind(taggedType.GetRule_type_name_or_bind3());
            if (!typeNode) {
                return {};
            }
            auto tag = TypeNameTag(taggedType.GetRule_type_name_tag5());
            if (!tag) {
                return {};
            }
            result = new TAstListNodeImpl(pos, { BuildAtom(pos, "TaggedType", flags), typeNode, tag });
            break;
        }
        case TRule_type_name_composite_TBlock1::kAlt13: {
            auto& callableType = block.GetAlt13().GetRule_type_name_callable1();
            TMaybe<std::pair<TVector<TNodePtr>, bool>> requiredArgs, optionalArgs;
            bool namedArgsStarted = false;
            size_t optionalArgsCount = 0;
            if (callableType.HasBlock4()) {
                auto& argList = callableType.GetBlock4().GetRule_callable_arg_list1();
                requiredArgs = CallableArgList(argList, namedArgsStarted);
                if (!requiredArgs) {
                    return {};
                }
                namedArgsStarted = requiredArgs->second;
            }
            if (callableType.HasBlock6()) {
                auto& argList = callableType.GetBlock6().GetRule_callable_arg_list2();
                optionalArgs = CallableArgList(argList, namedArgsStarted);
                if (!optionalArgs) {
                    return {};
                }
                optionalArgsCount = optionalArgs->first.size();
            }
            auto returnType = TypeNodeOrBind(callableType.GetRule_type_name_or_bind9());
            if (!returnType) {
                return {};
            }
            TVector<TNodePtr> items;
            items.push_back(BuildAtom(pos, "CallableType", flags));
            if (optionalArgsCount) {
                items.push_back(makeQuote(new TAstListNodeImpl(pos,
                    { BuildQuotedAtom(pos, ToString(optionalArgsCount), flags) })));
            } else {
                items.push_back(makeQuote(new TAstListNodeImpl(pos, {})));
            }
            items.push_back(makeQuote(new TAstListNodeImpl(pos, { returnType })));
            if (requiredArgs) {
                for (auto& arg: requiredArgs->first) {
                    items.push_back(makeQuote(arg));
                }
            }
            if (optionalArgs) {
                for (auto& arg: optionalArgs->first) {
                    items.push_back(makeQuote(arg));
                }
            }
            result = new TAstListNodeImpl(pos, items);
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    return AddOptionals(result, node.GetBlock2().size());
}

bool Expr(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr& node) {
    TNodePtr exprNode = sqlExpr.Build(node);
    if (!exprNode) {
        return false;
    }
    exprNodes.push_back(exprNode);
    return true;
}

bool ExprList(TSqlExpression& sqlExpr, TVector<TNodePtr>& exprNodes, const TRule_expr_list& node) {
    if (!Expr(sqlExpr, exprNodes, node.GetRule_expr1())) {
        return false;
    }
    for (auto b: node.GetBlock2()) {
        sqlExpr.Token(b.GetToken1());
        if (!Expr(sqlExpr, exprNodes, b.GetRule_expr2())) {
            return false;
        }
    }
    return true;
}

TNodePtr TSqlTranslation::ValueConstructorLiteral(const TRule_value_constructor_literal& node) {
    return BuildLiteralSmartString(Ctx, Token(node.GetToken1()));
}

TNodePtr TSqlTranslation::ValueConstructor(const TRule_value_constructor& node) {
    TSqlCallExpr call(Ctx, Mode);
    if (!call.Init(node)) {
        return {};
    }
    return call.BuildCall();
}

TNodePtr TSqlTranslation::ListLiteral(const TRule_list_literal& node) {
    TVector<TNodePtr> values;
    values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "AsList", TNodeFlags::Default));

    TSqlExpression sqlExpr(Ctx, Mode);
    if (node.HasBlock2() && !ExprList(sqlExpr, values, node.GetBlock2().GetRule_expr_list1())) {
        return nullptr;
    }

    return new TAstListNodeImpl(Ctx.Pos(), std::move(values));
}

TNodePtr TSqlTranslation::DictLiteral(const TRule_dict_literal& node) {
    TVector<TNodePtr> values;
    if (node.HasBlock2()) {
        const auto& list = node.GetBlock2().GetRule_expr_dict_list1();
        const bool isSet = !list.HasBlock2();
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), isSet ? "AsSet" : "AsDict", TNodeFlags::Default));
        TSqlExpression sqlExpr(Ctx, Mode);
        if (isSet) {
            if (!Expr(sqlExpr, values, list.GetRule_expr1())) {
                return nullptr;
            }
        } else {
            TVector<TNodePtr> tupleItems;
            if (!Expr(sqlExpr, tupleItems, list.GetRule_expr1())) {
                return nullptr;
            }

            if (!Expr(sqlExpr, tupleItems, list.GetBlock2().GetRule_expr2())) {
                return nullptr;
            }

            values.push_back(new TTupleNode(Ctx.Pos(), std::move(tupleItems)));
        }

        for (auto& b : list.GetBlock3()) {
            sqlExpr.Token(b.GetToken1());
            const bool isSetCurr = !b.HasBlock3();
            if (isSetCurr != isSet) {
                Error() << "Expected keys/values pair or keys, but got mix of them";
                return nullptr;
            }

            if (isSet) {
                if (!Expr(sqlExpr, values, b.GetRule_expr2())) {
                    return nullptr;
                }
            } else {
                TVector<TNodePtr> tupleItems;
                if (!Expr(sqlExpr, tupleItems, b.GetRule_expr2())) {
                    return nullptr;
                }

                if (!Expr(sqlExpr, tupleItems, b.GetBlock3().GetRule_expr2())) {
                    return nullptr;
                }

                values.push_back(new TTupleNode(Ctx.Pos(), std::move(tupleItems)));
            }
        }
    } else {
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "AsDict", TNodeFlags::Default));
    }

    return new TAstListNodeImpl(Ctx.Pos(), std::move(values));
}

bool TSqlTranslation::StructLiteralItem(TVector<TNodePtr>& labels, const TRule_expr& label, TVector<TNodePtr>& values, const TRule_expr& value) {
    // label expr
    {
        TColumnRefScope scope(Ctx, EColumnRefState::AsStringLiteral, /* topLevel */ false);
        TSqlExpression sqlExpr(Ctx, Mode);
        if (!Expr(sqlExpr, labels, label)) {
            return false;
        }

        TDeferredAtom atom;
        MakeTableFromExpression(Ctx, labels.back(), atom);
        labels.back() = atom.Build();
        if (!labels.back()) {
            return false;
        }
    }

    // value expr
    {
        TSqlExpression sqlExpr(Ctx, Mode);
        if (!Expr(sqlExpr, values, value)) {
            return false;
        }
    }

    return true;
}

TNodePtr TSqlTranslation::StructLiteral(const TRule_struct_literal& node) {
    TVector<TNodePtr> labels;
    TVector<TNodePtr> values;
    TPosition pos = Ctx.TokenPosition(node.GetToken1());
    if (node.HasBlock2()) {
        const auto& list = node.GetBlock2().GetRule_expr_struct_list1();

        if (!StructLiteralItem(labels, list.GetRule_expr1(), values, list.GetRule_expr3())) {
            return {};
        }

        for (auto& b : list.GetBlock4()) {
            if (!StructLiteralItem(labels, b.GetRule_expr2(), values, b.GetRule_expr4())) {
                return {};
            }
        }
    }
    return BuildStructure(pos, values, labels);
}

bool TSqlTranslation::TableHintImpl(const TRule_table_hint& rule, TTableHints& hints) {
    // table_hint:
    //      an_id_hint (EQUALS (type_name_tag | LPAREN type_name_tag (COMMA type_name_tag)* COMMA? RPAREN))?
    //    | (SCHEMA | COLUMNS) EQUALS? type_name_or_bind
    //    | SCHEMA EQUALS? LPAREN (struct_arg_positional (COMMA struct_arg_positional)*)? COMMA? RPAREN
    switch (rule.Alt_case()) {
    case TRule_table_hint::kAltTableHint1: {
        const auto& alt = rule.GetAlt_table_hint1();
        const TString id = Id(alt.GetRule_an_id_hint1(), *this);
        const auto idLower = to_lower(id);
        if (idLower == "schema" || idLower == "columns") {
            Error() << "Expected type after " << to_upper(id);
            return false;
        }
        TVector<TNodePtr> hint_val;
        if (alt.HasBlock2()) {
            auto& tags = alt.GetBlock2().GetBlock2();
            switch (tags.Alt_case()) {
                case TRule_table_hint_TAlt1_TBlock2_TBlock2::kAlt1:
                    hint_val.push_back(TypeNameTag(tags.GetAlt1().GetRule_type_name_tag1()));
                    break;
                case TRule_table_hint_TAlt1_TBlock2_TBlock2::kAlt2: {
                    hint_val.push_back(TypeNameTag(tags.GetAlt2().GetRule_type_name_tag2()));
                    for (auto& tag : tags.GetAlt2().GetBlock3()) {
                        hint_val.push_back(TypeNameTag(tag.GetRule_type_name_tag2()));
                    }
                    break;
                }
                default:
                    Y_FAIL("You should change implementation according to grammar changes");
            }
        }
        hints[id] = hint_val;
        break;
    }

    case TRule_table_hint::kAltTableHint2: {
        const auto& alt2 = rule.GetAlt_table_hint2();
        auto node = TypeNodeOrBind(alt2.GetRule_type_name_or_bind3());
        if (!node) {
            return false;
        }

        hints["user_" + to_lower(alt2.GetToken1().GetValue())] = { node };
        break;
    }

    case TRule_table_hint::kAltTableHint3: {
        const auto& alt = rule.GetAlt_table_hint3();
        TVector<TNodePtr> labels;
        TVector<TNodePtr> structTypeItems;
        if (alt.HasBlock4()) {
            bool warn = false;
            auto processItem = [&](const TRule_struct_arg_positional& arg) {
                // struct_arg_positional:
                //     type_name_tag type_name_or_bind (NOT? NULL)?
                //   | type_name_or_bind AS type_name_tag; //deprecated
                const bool altCurrent = arg.Alt_case() == TRule_struct_arg_positional::kAltStructArgPositional1;
                auto& typeNameOrBind = altCurrent ?
                    arg.GetAlt_struct_arg_positional1().GetRule_type_name_or_bind2() :
                    arg.GetAlt_struct_arg_positional2().GetRule_type_name_or_bind1();
                auto typeNode = TypeNodeOrBind(typeNameOrBind);
                if (!typeNode) {
                    return false;
                }

                auto pos = Ctx.Pos();
                if (!altCurrent && !warn) {
                    Ctx.Warning(pos, TIssuesIds::YQL_DEPRECATED_POSITIONAL_SCHEMA)
                        << "Deprecated syntax for positional schema: please use 'column type' instead of 'type AS column'";
                    warn = true;
                }

                if (altCurrent) {
                    bool notNull = arg.GetAlt_struct_arg_positional1().HasBlock3() && arg.GetAlt_struct_arg_positional1().GetBlock3().HasBlock1();
                    if (!notNull) {
                        typeNode = new TCallNodeImpl(pos, "AsOptionalType", { typeNode });
                    }
                }

                auto& typeNameTag = altCurrent ?
                    arg.GetAlt_struct_arg_positional1().GetRule_type_name_tag1() :
                    arg.GetAlt_struct_arg_positional2().GetRule_type_name_tag3();
                auto tag = TypeNameTag(typeNameTag);
                if (!tag) {
                    return false;
                }

                labels.push_back(tag);
                structTypeItems.push_back(BuildTuple(pos, { tag, typeNode }));
                return true;
            };

            if (!processItem(alt.GetBlock4().GetRule_struct_arg_positional1())) {
                return false;
            }

            for (auto& entry : alt.GetBlock4().GetBlock2()) {
                if (!processItem(entry.GetRule_struct_arg_positional2())) {
                    return false;
                }
            }
        }

        TPosition pos = Ctx.TokenPosition(alt.GetToken1());
        auto labelsTuple = BuildTuple(pos, labels);
        TNodePtr structType = new TCallNodeImpl(pos, "StructType", structTypeItems);

        hints["user_" + to_lower(alt.GetToken1().GetValue())] = { structType, labelsTuple };
        break;
    }

    default:
        Y_FAIL("You should change implementation according to grammar changes");
    }

    return true;
}

TMaybe<TTableHints> TSqlTranslation::TableHintsImpl(const TRule_table_hints& node) {
    TTableHints hints;
    auto& block = node.GetBlock2();
    bool hasErrors = false;
    switch (block.Alt_case()) {
    case TRule_table_hints::TBlock2::kAlt1: {
        hasErrors = !TableHintImpl(block.GetAlt1().GetRule_table_hint1(), hints);
        break;
    }
    case TRule_table_hints::TBlock2::kAlt2: {
        hasErrors = !TableHintImpl(block.GetAlt2().GetRule_table_hint2(), hints);
        for (const auto& x : block.GetAlt2().GetBlock3()) {
            hasErrors = hasErrors || !TableHintImpl(x.GetRule_table_hint2(), hints);
        }

        break;
    }
    default:
        Y_FAIL("You should change implementation according to grammar changes");
    }
    if (hasErrors) {
        return Nothing();
    }

    return hints;
}

bool TSqlTranslation::SimpleTableRefImpl(const TRule_simple_table_ref& node, TTableRef& result) {
    // simple_table_ref: simple_table_ref_core table_hints?;
    if (!SimpleTableRefCoreImpl(node.GetRule_simple_table_ref_core1(), result)) {
        return false;
    }

    TTableHints hints = GetContextHints(Context());
    if (node.HasBlock2()) {
        auto tmp = TableHintsImpl(node.GetBlock2().GetRule_table_hints1());
        if (!tmp) {
            Error() << "Failed to parse table hints";
            return false;
        }

        hints = *tmp;
    }

    if (!hints.empty()) {
        result.Options = BuildInputOptions(Context().Pos(), hints);
    }

    return true;
}

bool TSqlTranslation::SimpleTableRefCoreImpl(const TRule_simple_table_ref_core& node, TTableRef& result) {
    // simple_table_ref_core: ((cluster_expr DOT)? id_or_at) | AT? bind_parameter;
    TString service = Context().Scoped->CurrService;
    TDeferredAtom cluster = Context().Scoped->CurrCluster;
    switch (node.Alt_case()) {
    case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore1: {
        if (node.GetAlt_simple_table_ref_core1().GetRule_object_ref1().HasBlock1()) {
            if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
                Error() << "Cluster should not be used in limited view";
                return false;
            }

            if (!ClusterExpr(node.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetBlock1().GetRule_cluster_expr1(), false, service, cluster)) {
                return false;
            }
        }

        if (cluster.Empty()) {
            Error() << "No cluster name given and no default cluster is selected";
            return false;
        }

        result = TTableRef(Context().MakeName("table"), service, cluster, nullptr);
        auto tableOrAt = Id(node.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2(), *this);
        auto tableAndView = TableKeyImpl(tableOrAt, "", *this);
        result.Keys = BuildTableKey(Context().Pos(), result.Service, result.Cluster,
            TDeferredAtom(Context().Pos(), tableAndView.first), tableAndView.second);
        break;
    }
    case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore2: {
        if (cluster.Empty()) {
            Error() << "No cluster name given and no default cluster is selected";
            return false;
        }

        auto at = node.GetAlt_simple_table_ref_core2().HasBlock1();
        TString bindName;
        if (!NamedNodeImpl(node.GetAlt_simple_table_ref_core2().GetRule_bind_parameter2(), bindName, *this)) {
            return false;
        }
        auto named = GetNamedNode(bindName);
        if (!named) {
            return false;
        }

        TDeferredAtom table;
        MakeTableFromExpression(Context(), named, table);
        result = TTableRef(Context().MakeName("table"), service, cluster, nullptr);
        result.Keys = BuildTableKey(Context().Pos(), result.Service, result.Cluster, table, at ? "@" : "");
        break;
    }
    default:
        Y_FAIL("You should change implementation according to grammar changes");
    }

    return result.Keys != nullptr;
}

bool TSqlTranslation::TopicRefImpl(const TRule_topic_ref& node, TTopicRef& result) {
    TString service = Context().Scoped->CurrService;
    TDeferredAtom cluster = Context().Scoped->CurrCluster;
    if (node.HasBlock1()) {
        if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) {
            Error() << "Cluster should not be used in limited view";
            return false;
        }

        if (!ClusterExpr(node.GetBlock1().GetRule_cluster_expr1(), false, service, cluster)) {
            return false;
        }
    }

    if (cluster.Empty()) {
        Error() << "No cluster name given and no default cluster is selected";
        return false;
    }

    result = TTopicRef(Context().MakeName("topic"), cluster, nullptr);
    auto topic = Id(node.GetRule_an_id2(), *this);
    result.Keys = BuildTopicKey(Context().Pos(), result.Cluster, TDeferredAtom(Context().Pos(), topic));

    return true;
}

bool TSqlCallExpr::Init(const TRule_value_constructor& node) {
    switch (node.Alt_case()) {
        case TRule_value_constructor::kAltValueConstructor1: {
            auto& ctor = node.GetAlt_value_constructor1();
            Func = "Variant";
            TSqlExpression expr(Ctx, Mode);
            if (!Expr(expr, Args, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr5())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr7())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::kAltValueConstructor2: {
            auto& ctor = node.GetAlt_value_constructor2();
            Func = "Enum";
            TSqlExpression expr(Ctx, Mode);
            if (!Expr(expr, Args, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr5())) {
                return false;
            }
            break;
        }
        case TRule_value_constructor::kAltValueConstructor3: {
            auto& ctor = node.GetAlt_value_constructor3();
            Func = "Callable";
            TSqlExpression expr(Ctx, Mode);
            if (!Expr(expr, Args, ctor.GetRule_expr3())) {
                return false;
            }
            if (!Expr(expr, Args, ctor.GetRule_expr5())) {
                return false;
            }
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    PositionalArgs = Args;
    return true;
}

bool TSqlCallExpr::ExtractCallParam(const TRule_external_call_param& node) {
    TString paramName = Id(node.GetRule_an_id1(), *this);
    paramName = to_lower(paramName);

    if (CallConfig.contains(paramName)) {
        Ctx.Error() << "WITH " << to_upper(paramName).Quote()
            << " clause should be specified only once";
        return false;
    }

    const bool optimizeForParam = paramName == "optimize_for";
    const auto columnRefState = optimizeForParam ? EColumnRefState::AsStringLiteral : EColumnRefState::Deny;

    TColumnRefScope scope(Ctx, columnRefState);
    if (optimizeForParam) {
        scope.SetNoColumnErrContext("in external call params");
    }

    TSqlExpression expression(Ctx, Mode);
    auto value = expression.Build(node.GetRule_expr3());
    if (value && optimizeForParam) {
        TDeferredAtom atom;
        MakeTableFromExpression(Ctx, value, atom);
        value = new TCallNodeImpl(Ctx.Pos(), "String", { atom.Build() });
    }

    if (!value) {
        return false;
    }

    CallConfig[paramName] = value;
    return true;
}

bool TSqlCallExpr::ConfigureExternalCall(const TRule_external_call_settings& node) {
    bool success = ExtractCallParam(node.GetRule_external_call_param1());
    for (auto& block: node.GetBlock2()) {
        success = ExtractCallParam(block.GetRule_external_call_param2()) && success;
    }

    return success;
}

bool TSqlCallExpr::Init(const TRule_using_call_expr& node) {
    // using_call_expr: ((an_id_or_type NAMESPACE an_id_or_type) | an_id_expr | bind_parameter | (EXTERNAL FUNCTION)) invoke_expr;
    const auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TRule_using_call_expr::TBlock1::kAlt1: {
            auto& subblock = block.GetAlt1().GetBlock1();
            Module = Id(subblock.GetRule_an_id_or_type1(), *this);
            Func = Id(subblock.GetRule_an_id_or_type3(), *this);
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt2: {
            Func = Id(block.GetAlt2().GetRule_an_id_expr1(), *this);
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt3: {
            TString bindName;
            if (!NamedNodeImpl(block.GetAlt3().GetRule_bind_parameter1(), bindName, *this)) {
                return false;
            }
            Node = GetNamedNode(bindName);
            if (!Node) {
                return false;
            }
            break;
        }
        case TRule_using_call_expr::TBlock1::kAlt4: {
            IsExternalCall = true;
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    YQL_ENSURE(!DistinctAllowed);
    UsingCallExpr = true;
    TColumnRefScope scope(Ctx, EColumnRefState::Allow);
    return Init(node.GetRule_invoke_expr2());
}

void TSqlCallExpr::InitName(const TString& name) {
    Module = "";
    Func = name;
}

void TSqlCallExpr::InitExpr(const TNodePtr& expr) {
    Node = expr;
}

bool TSqlCallExpr::FillArg(const TString& module, const TString& func, size_t& idx, const TRule_named_expr& node) {
    const bool isNamed = node.HasBlock2();

    TMaybe<EColumnRefState> status;
    // TODO: support named args
    if (!isNamed) {
        status = GetFunctionArgColumnStatus(Ctx, module, func, idx);
    }

    TNodePtr expr;
    if (status) {
        TColumnRefScope scope(Ctx, *status, /* isTopLevel = */ false);
        expr = NamedExpr(node);
    } else {
        expr = NamedExpr(node);
    }

    if (!expr) {
        return false;
    }

    Args.emplace_back(std::move(expr));
    if (!isNamed) {
        ++idx;
    }
    return true;
}

bool TSqlCallExpr::FillArgs(const TRule_named_expr_list& node) {
    TString module = Module;
    TString func = Func;
    if (Node && Node->FuncName()) {
        module = Node->ModuleName() ? *Node->ModuleName() : "YQL";
        func = *Node->FuncName();
    }

    size_t idx = 0;
    if (!FillArg(module, func, idx, node.GetRule_named_expr1())) {
        return false;
    }

    for (auto& b: node.GetBlock2()) {
        if (!FillArg(module, func, idx, b.GetRule_named_expr2())) {
            return false;
        }
    }

    return true;
}

bool TSqlCallExpr::Init(const TRule_invoke_expr& node) {
    // invoke_expr: LPAREN (opt_set_quantifier named_expr_list COMMA? | ASTERISK)? RPAREN invoke_expr_tail;
    // invoke_expr_tail:
    //     (null_treatment | filter_clause)? (OVER window_name_or_specification)?
    // ;
    Pos = Ctx.Pos();
    if (node.HasBlock2()) {
        switch (node.GetBlock2().Alt_case()) {
        case TRule_invoke_expr::TBlock2::kAlt1: {
            const auto& alt = node.GetBlock2().GetAlt1();
            TPosition distinctPos;
            if (IsDistinctOptSet(alt.GetRule_opt_set_quantifier1(), distinctPos)) {
                if (!DistinctAllowed) {
                    if (UsingCallExpr) {
                        Ctx.Error(distinctPos) << "DISTINCT can not be used in PROCESS/REDUCE";
                    } else {
                        Ctx.Error(distinctPos) << "DISTINCT can only be used in aggregation functions";
                    }
                    return false;
                }
                YQL_ENSURE(AggMode == EAggregateMode::Normal);
                AggMode = EAggregateMode::Distinct;
                Ctx.IncrementMonCounter("sql_features", "DistinctInCallExpr");
            }
            if (!FillArgs(alt.GetRule_named_expr_list2())) {
                return false;
            }
            for (const auto& arg : Args) {
                if (arg->GetLabel()) {
                    NamedArgs.push_back(arg);
                }
                else {
                    PositionalArgs.push_back(arg);
                    if (!NamedArgs.empty()) {
                        Ctx.Error(arg->GetPos()) << "Unnamed arguments can not follow after named one";
                        return false;
                    }
                }
            }
            break;
        }
        case TRule_invoke_expr::TBlock2::kAlt2:
            if (IsExternalCall) {
                Ctx.Error() << "You should set EXTERNAL FUNCTION type. Example: EXTERNAL FUNCTION('YANDEX-CLOUD', ...)";
            } else {
                Args.push_back(new TAsteriskNode(Pos));
            }
            break;
        default:
            Y_FAIL("You should change implementation according to grammar changes");
        }
    }

    const auto& tail = node.GetRule_invoke_expr_tail4();

    if (tail.HasBlock1()) {
        if (IsExternalCall) {
            Ctx.Error() << "Additional clause after EXTERNAL FUNCTION(...) is not supported";
            return false;
        }

        switch (tail.GetBlock1().Alt_case()) {
        case TRule_invoke_expr_tail::TBlock1::kAlt1: {
            if (!tail.HasBlock2()) {
                Ctx.Error() << "RESPECT/IGNORE NULLS can only be used with window functions";
                return false;
            }
            const auto& alt = tail.GetBlock1().GetAlt1();
            if (alt.GetRule_null_treatment1().Alt_case() == TRule_null_treatment::kAltNullTreatment2) {
                SetIgnoreNulls();
            }
            break;
        }
        case TRule_invoke_expr_tail::TBlock1::kAlt2: {
            Ctx.Error() << "FILTER clause is not supported yet";
            return false;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
        }
    }

    if (tail.HasBlock2()) {
        if (AggMode == EAggregateMode::Distinct) {
            Ctx.Error() << "DISTINCT is not yet supported in window functions";
            return false;
        }
        SetOverWindow();
        auto winRule = tail.GetBlock2().GetRule_window_name_or_specification2();
        switch (winRule.Alt_case()) {
        case TRule_window_name_or_specification::kAltWindowNameOrSpecification1: {
            WindowName = Id(winRule.GetAlt_window_name_or_specification1().GetRule_window_name1().GetRule_an_id_window1(), *this);
            break;
        }
        case TRule_window_name_or_specification::kAltWindowNameOrSpecification2: {
            if (!Ctx.WinSpecsScopes) {
                auto pos = Ctx.TokenPosition(tail.GetBlock2().GetToken1());
                Ctx.Error(pos) << "Window and aggregation functions are not allowed in this context";
                return false;
            }

            TWindowSpecificationPtr spec = WindowSpecification(
                winRule.GetAlt_window_name_or_specification2().GetRule_window_specification1().GetRule_window_specification_details2());
            if (!spec) {
                return false;
            }

            WindowName = Ctx.MakeName("_yql_anonymous_window");
            TWinSpecs& specs = Ctx.WinSpecsScopes.back();
            YQL_ENSURE(!specs.contains(WindowName));
            specs[WindowName] = spec;
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
        }
        Ctx.IncrementMonCounter("sql_features", "WindowFunctionOver");
    }

    return true;
}

void TSqlCallExpr::IncCounters() {
    if (Node) {
        Ctx.IncrementMonCounter("sql_features", "NamedNodeUseApply");
    } else if (!Module.empty()) {
        if (ValidateForCounters(Module)) {
            Ctx.IncrementMonCounter("udf_modules", Module);
            Ctx.IncrementMonCounter("sql_features", "CallUdf");
            if (ValidateForCounters(Func)) {
                auto scriptType = NKikimr::NMiniKQL::ScriptTypeFromStr(Module);
                if (scriptType == NKikimr::NMiniKQL::EScriptType::Unknown) {
                   Ctx.IncrementMonCounter("udf_functions", Module + "." + Func);
                }
            }
        }
    } else if (ValidateForCounters(Func)) {
        Ctx.IncrementMonCounter("sql_builtins", Func);
        Ctx.IncrementMonCounter("sql_features", "CallBuiltin");
    }
}

class TSqlSelect: public TSqlTranslation {
public:
    TSqlSelect(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_select_stmt& node, TPosition& selectPos);
    TSourcePtr Build(const TRule_select_unparenthesized_stmt& node, TPosition& selectPos);

private:
    bool SelectTerm(TVector<TNodePtr>& terms, const TRule_result_column& node);
    bool ValidateSelectColumns(const TVector<TNodePtr>& terms);
    bool ColumnName(TVector<TNodePtr>& keys, const TRule_column_name& node);
    bool ColumnName(TVector<TNodePtr>& keys, const TRule_without_column_name& node);
    template<typename TRule>
    bool ColumnList(TVector<TNodePtr>& keys, const TRule& node);
    bool NamedColumn(TVector<TNodePtr>& columnList, const TRule_named_column& node);
    TSourcePtr SingleSource(const TRule_single_source& node, const TVector<TString>& derivedColumns, TPosition derivedColumnsPos, bool unorderedSubquery);
    TSourcePtr NamedSingleSource(const TRule_named_single_source& node, bool unorderedSubquery);
    bool FlattenByArg(const TString& sourceLabel, TVector<TNodePtr>& flattenByColumns, TVector<TNodePtr>& flattenByExprs, const TRule_flatten_by_arg& node);
    TSourcePtr FlattenSource(const TRule_flatten_source& node);
    TSourcePtr JoinSource(const TRule_join_source& node);
    bool JoinOp(ISource* join, const TRule_join_source::TBlock3& block, TMaybe<TPosition> anyPos);
    TNodePtr JoinExpr(ISource*, const TRule_join_constraint& node);
    TSourcePtr ProcessCore(const TRule_process_core& node, const TWriteSettings& settings, TPosition& selectPos);
    TSourcePtr ReduceCore(const TRule_reduce_core& node, const TWriteSettings& settings, TPosition& selectPos);

    struct TSelectKindPlacement {
        bool IsFirstInSelectOp = false;
        bool IsLastInSelectOp = false;
    };

    TSourcePtr SelectCore(const TRule_select_core& node, const TWriteSettings& settings, TPosition& selectPos,
        TMaybe<TSelectKindPlacement> placement, TVector<TSortSpecificationPtr>& selectOpOrederBy, bool& selectOpAssumeOrderBy);

    bool WindowDefinition(const TRule_window_definition& node, TWinSpecs& winSpecs);
    bool WindowClause(const TRule_window_clause& node, TWinSpecs& winSpecs);

    struct TSelectKindResult {
        TSourcePtr Source;
        TWriteSettings Settings;

        TVector<TSortSpecificationPtr> SelectOpOrderBy;
        bool SelectOpAssumeOrderBy = false;
        TNodePtr SelectOpSkipTake;

        inline explicit operator bool() const {
            return static_cast<bool>(Source);
        }
    };

    bool ValidateLimitOrderByWithSelectOp(TMaybe<TSelectKindPlacement> placement, TStringBuf what);
    bool NeedPassLimitOrderByToUnderlyingSelect(TMaybe<TSelectKindPlacement> placement);

    template<typename TRule>
    TSourcePtr Build(const TRule& node, TPosition pos, TSelectKindResult&& first);


    TSelectKindResult SelectKind(const TRule_select_kind& node, TPosition& selectPos, TMaybe<TSelectKindPlacement> placement);
    TSelectKindResult SelectKind(const TRule_select_kind_partial& node, TPosition& selectPos, TMaybe<TSelectKindPlacement> placement);
    TSelectKindResult SelectKind(const TRule_select_kind_parenthesis& node, TPosition& selectPos, TMaybe<TSelectKindPlacement> placement);
};

class TSqlValues: public TSqlTranslation {
public:
    TSqlValues(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_values_stmt& node, TPosition& valuesPos, const TVector<TString>& derivedColumns = {}, TPosition derivedColumnsPos = TPosition());
protected:
    bool BuildRows(const TRule_values_source_row_list& node, TVector<TVector<TNodePtr>>& rows);

private:
    bool BuildRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
};

class TGroupByClause: public TSqlTranslation {
    enum class EGroupByFeatures {
        Begin,
        Ordinary = Begin,
        Expression,
        Rollup,
        Cube,
        GroupingSet,
        Empty,
        End,
    };
    typedef TEnumBitSet<EGroupByFeatures, static_cast<int>(EGroupByFeatures::Begin), static_cast<int>(EGroupByFeatures::End)> TGroupingSetFeatures;

    class TGroupByClauseCtx: public TSimpleRefCount<TGroupByClauseCtx> {
    public:
        typedef TIntrusivePtr<TGroupByClauseCtx> TPtr;

        TGroupingSetFeatures GroupFeatures;
        TMap<TString, TNodePtr> NodeAliases;
        size_t UnnamedCount = 0;
    };

public:
    TGroupByClause(TContext& ctx, NSQLTranslation::ESqlMode mode, TGroupByClauseCtx::TPtr groupSetContext = {})
        : TSqlTranslation(ctx, mode)
        , GroupSetContext(groupSetContext ? groupSetContext : TGroupByClauseCtx::TPtr(new TGroupByClauseCtx()))
        , CompactGroupBy(false)
    {}

    bool Build(const TRule_group_by_clause& node);
    bool ParseList(const TRule_grouping_element_list& groupingListNode, EGroupByFeatures featureContext);

    void SetFeatures(const TString& field) const;
    TVector<TNodePtr>& Content();
    TMap<TString, TNodePtr>& Aliases();
    TLegacyHoppingWindowSpecPtr GetLegacyHoppingWindow() const;
    bool IsCompactGroupBy() const;
    TString GetSuffix() const;

private:
    TMaybe<TVector<TNodePtr>> MultiplyGroupingSets(const TVector<TNodePtr>& lhs, const TVector<TNodePtr>& rhs) const;
    bool ResolveGroupByAndGrouping();
    bool GroupingElement(const TRule_grouping_element& node, EGroupByFeatures featureContext);
    void FeedCollection(const TNodePtr& elem, TVector<TNodePtr>& collection, bool& hasEmpty) const;
    bool OrdinaryGroupingSet(const TRule_ordinary_grouping_set& node, EGroupByFeatures featureContext);
    bool OrdinaryGroupingSetList(const TRule_ordinary_grouping_set_list& node, EGroupByFeatures featureContext);
    bool HoppingWindow(const TRule_hopping_window_specification& node);

    bool AllowUnnamed(TPosition pos, EGroupByFeatures featureContext);

    TGroupingSetFeatures& Features();
    const TGroupingSetFeatures& Features() const;
    bool AddAlias(const TString& label, const TNodePtr& node);
    TString GenerateGroupByExprName();
    bool IsAutogenerated(const TString* name) const;

    TVector<TNodePtr> GroupBySet;
    TGroupByClauseCtx::TPtr GroupSetContext;
    TLegacyHoppingWindowSpecPtr LegacyHoppingWindowSpec; // stream queries
    static const TString AutogenerateNamePrefix;
    bool CompactGroupBy;
    TString Suffix;
};

const TString TGroupByClause::AutogenerateNamePrefix = "group";

bool ParseNumbers(TContext& ctx, const TString& strOrig, ui64& value, TString& suffix) {
    const auto str = to_lower(strOrig);
    const auto strLen = str.size();
    ui64 base = 10;
    if (strLen > 2 && str[0] == '0') {
        const auto formatChar = str[1];
        if (formatChar == 'x') {
            base = 16;
        } else if (formatChar == 'o') {
            base = 8;
        } else if (formatChar == 'b') {
            base = 2;
        }
    }
    if (strLen > 1) {
        auto iter = str.cend() - 1;
        if (*iter == 'l' || *iter == 's' || *iter == 't' || *iter == 's' || *iter == 'i' ||  *iter == 'b' || *iter == 'n') {
            --iter;
        }
        if (*iter == 'u' || *iter == 'p') {
            --iter;
        }
        suffix = TString(++iter, str.cend());
    }
    value = 0;
    const TString digString(str.begin() + (base == 10 ? 0 : 2), str.end() - suffix.size());
    for (const char& cur: digString) {
        const ui64 curDigit = Char2DigitTable[static_cast<int>(cur)];
        if (curDigit >= base) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", char: '" << cur <<
                "' is out of base: " << base;
            return false;
        }

        ui64 curValue = value;
        value *= base;
        bool overflow = ((value / base) != curValue);
        if (!overflow) {
            curValue = value;
            value += curDigit;
            overflow = value < curValue;
        }

        if (overflow) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << strOrig << ", number limit overflow";
            return false;
        }
    }
    return true;
}

TNodePtr LiteralNumber(TContext& ctx, const TRule_integer& node) {
    const TString intergerString = ctx.Token(node.GetToken1());
    if (to_lower(intergerString).EndsWith("pn")) {
        // TODO: add validation
        return new TLiteralNode(ctx.Pos(), "PgNumeric", intergerString.substr(0, intergerString.size() - 2));
    }

    ui64 value;
    TString suffix;
    if (!ParseNumbers(ctx, intergerString, value, suffix)) {
        return {};
    }

    const bool noSpaceForInt32 = value >> 31;
    const bool noSpaceForInt64 = value >> 63;
    if (suffix ==  "") {
        bool implicitType = true;
        if (noSpaceForInt64) {
            return new TLiteralNumberNode<ui64>(ctx.Pos(), "Uint64", ToString(value), implicitType);
        } else if (noSpaceForInt32) {
            return new TLiteralNumberNode<i64>(ctx.Pos(), "Int64", ToString(value), implicitType);
        }
        return new TLiteralNumberNode<i32>(ctx.Pos(), "Int32", ToString(value), implicitType);
    } else if (suffix == "p") {
        bool implicitType = true;
        if (noSpaceForInt64) {
            ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << intergerString << ", 64 bit signed integer overflow";
            return {};
        } else if (noSpaceForInt32) {
            return new TLiteralNumberNode<i64>(ctx.Pos(), "PgInt8", ToString(value), implicitType);
        }
        return new TLiteralNumberNode<i32>(ctx.Pos(), "PgInt4", ToString(value), implicitType);
    } else if (suffix == "u") {
        return new TLiteralNumberNode<ui32>(ctx.Pos(), "Uint32", ToString(value));
    } else if (suffix == "ul") {
        return new TLiteralNumberNode<ui64>(ctx.Pos(), "Uint64", ToString(value));
    } else if (suffix == "ut") {
        return new TLiteralNumberNode<ui8>(ctx.Pos(), "Uint8", ToString(value));
    } else if (suffix == "t") {
        return new TLiteralNumberNode<i8>(ctx.Pos(), "Int8", ToString(value));
    } else if (suffix == "l") {
        return new TLiteralNumberNode<i64>(ctx.Pos(), "Int64", ToString(value));
    } else if (suffix == "us") {
        return new TLiteralNumberNode<ui16>(ctx.Pos(), "Uint16", ToString(value));
    } else if (suffix == "s") {
        return new TLiteralNumberNode<i16>(ctx.Pos(), "Int16", ToString(value));
    } else if (suffix == "ps") {
        return new TLiteralNumberNode<i16>(ctx.Pos(), "PgInt2", ToString(value));
    } else if (suffix == "pi") {
        return new TLiteralNumberNode<i32>(ctx.Pos(), "PgInt4", ToString(value));
    } else if (suffix == "pb") {
        return new TLiteralNumberNode<i64>(ctx.Pos(), "PgInt8", ToString(value));
    } else {
        ctx.Error(ctx.Pos()) << "Failed to parse number from string: " << intergerString << ", invalid suffix: " << suffix;
        return {};
    }
}

TNodePtr LiteralReal(TContext& ctx, const TRule_real& node) {
    const TString value(ctx.Token(node.GetToken1()));
    YQL_ENSURE(!value.empty());
    auto lower = to_lower(value);
    if (lower.EndsWith("f")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "Float", value.substr(0, value.size()-1));
    } else if (lower.EndsWith("p")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat8", value.substr(0, value.size()-1));
    } else if (lower.EndsWith("pf4")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat4", value.substr(0, value.size()-3));
    } else if (lower.EndsWith("pf8")) {
        return new TLiteralNumberNode<float>(ctx.Pos(), "PgFloat8", value.substr(0, value.size()-3));
    } else if (lower.EndsWith("pn")) {
        return new TLiteralNode(ctx.Pos(), "PgNumeric", value.substr(0, value.size()-2));
    } else {
        return new TLiteralNumberNode<double>(ctx.Pos(), "Double", value);
    }
}

TMaybe<TExprOrIdent> TSqlExpression::LiteralExpr(const TRule_literal_value& node) {
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_literal_value::kAltLiteralValue1: {
            result.Expr = LiteralNumber(Ctx, node.GetAlt_literal_value1().GetRule_integer1());
            break;
        }
        case TRule_literal_value::kAltLiteralValue2: {
            result.Expr = LiteralReal(Ctx, node.GetAlt_literal_value2().GetRule_real1());
            break;
        }
        case TRule_literal_value::kAltLiteralValue3: {
            const TString value(Token(node.GetAlt_literal_value3().GetToken1()));
            return BuildLiteralTypedSmartStringOrId(Ctx, value);
        }
        case TRule_literal_value::kAltLiteralValue5: {
            Token(node.GetAlt_literal_value5().GetToken1());
            result.Expr = BuildLiteralNull(Ctx.Pos());
            break;
        }
        case TRule_literal_value::kAltLiteralValue9: {
            const TString value(to_lower(Token(node.GetAlt_literal_value9().GetRule_bool_value1().GetToken1())));
            result.Expr = BuildLiteralBool(Ctx.Pos(), FromString<bool>(value));
            break;
        }
        case TRule_literal_value::kAltLiteralValue10: {
            result.Expr = BuildEmptyAction(Ctx.Pos());
            break;
        }
        default:
            AltNotImplemented("literal_value", node);
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

template<typename TUnarySubExprType>
TNodePtr TSqlExpression::UnaryExpr(const TUnarySubExprType& node, const TTrailingQuestions& tail) {
    if constexpr (std::is_same_v<TUnarySubExprType, TRule_unary_subexpr>) {
        if (node.Alt_case() == TRule_unary_subexpr::kAltUnarySubexpr1) {
            return UnaryCasualExpr(node.GetAlt_unary_subexpr1().GetRule_unary_casual_subexpr1(), tail);
        } else if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return {};
        } else {
            MaybeUnnamedSmartParenOnTop = false;
            return JsonApiExpr(node.GetAlt_unary_subexpr2().GetRule_json_api_expr1());
        }
    } else {
        MaybeUnnamedSmartParenOnTop = false;
        if (node.Alt_case() == TRule_in_unary_subexpr::kAltInUnarySubexpr1) {
            return UnaryCasualExpr(node.GetAlt_in_unary_subexpr1().GetRule_in_unary_casual_subexpr1(), tail);
        } else if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return {};
        } else {
            return JsonApiExpr(node.GetAlt_in_unary_subexpr2().GetRule_json_api_expr1());
        }
    }
}

TNodePtr TSqlExpression::JsonPathSpecification(const TRule_jsonpath_spec& node) {
    /*
        jsonpath_spec: STRING_VALUE;
    */
    TString value = Token(node.GetToken1());
    TPosition pos = Ctx.Pos();

    auto parsed = StringContent(Ctx, pos, value);
    if (!parsed) {
        return nullptr;
    }
    return new TCallNodeImpl(pos, "Utf8", {BuildQuotedAtom(pos, parsed->Content, parsed->Flags)});
}

TNodePtr TSqlExpression::JsonReturningTypeRule(const TRule_type_name_simple& node) {
    /*
        (RETURNING type_name_simple)?
    */
    return TypeSimple(node, /* onlyDataAllowed */ true);
}

TNodePtr TSqlExpression::JsonInputArg(const TRule_json_common_args& node) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TNodePtr jsonExpr = Build(node.GetRule_expr1());
    if (!jsonExpr || jsonExpr->IsNull()) {
        jsonExpr = new TCallNodeImpl(Ctx.Pos(), "Nothing", {
            new TCallNodeImpl(Ctx.Pos(), "OptionalType", {BuildDataType(Ctx.Pos(), "Json")})
        });
    }

    return jsonExpr;
}

void TSqlExpression::AddJsonVariable(const TRule_json_variable& node, TVector<TNodePtr>& children) {
    /*
        json_variable: expr AS json_variable_name;
    */
    TNodePtr expr;
    TString rawName;
    TPosition namePos = Ctx.Pos();
    ui32 nameFlags = 0;

    expr = Build(node.GetRule_expr1());
    const auto& nameRule = node.GetRule_json_variable_name3();
    switch (nameRule.GetAltCase()) {
        case TRule_json_variable_name::kAltJsonVariableName1:
            rawName = Id(nameRule.GetAlt_json_variable_name1().GetRule_id_expr1(), *this);
            nameFlags = TNodeFlags::ArbitraryContent;
            break;
        case TRule_json_variable_name::kAltJsonVariableName2: {
            const auto& token = nameRule.GetAlt_json_variable_name2().GetToken1();
            namePos = GetPos(token);
            auto parsed = StringContentOrIdContent(Ctx, namePos, token.GetValue());
            if (!parsed) {
                return;
            }
            rawName = parsed->Content;
            nameFlags = parsed->Flags;
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    TNodePtr nameExpr = BuildQuotedAtom(namePos, rawName, nameFlags);
    children.push_back(BuildTuple(namePos, {nameExpr, expr}));
}

void TSqlExpression::AddJsonVariables(const TRule_json_variables& node, TVector<TNodePtr>& children) {
    /*
        json_variables: json_variable (COMMA json_variable)*;
    */
    AddJsonVariable(node.GetRule_json_variable1(), children);
    for (size_t i = 0; i < node.Block2Size(); i++) {
            AddJsonVariable(node.GetBlock2(i).GetRule_json_variable2(), children);
    }
}

TNodePtr TSqlExpression::JsonVariables(const TRule_json_common_args& node) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TVector<TNodePtr> variables;
    TPosition pos = Ctx.Pos();
    if (node.HasBlock4()) {
        const auto& block = node.GetBlock4();
        pos = GetPos(block.GetToken1());
        AddJsonVariables(block.GetRule_json_variables2(), variables);
    }
    return new TCallNodeImpl(pos, "JsonVariables", variables);
}

void TSqlExpression::AddJsonCommonArgs(const TRule_json_common_args& node, TVector<TNodePtr>& children) {
    /*
        json_common_args: expr COMMA jsonpath_spec (PASSING json_variables)?;
    */
    TNodePtr jsonExpr = JsonInputArg(node);
    TNodePtr jsonPath = JsonPathSpecification(node.GetRule_jsonpath_spec3());
    TNodePtr variables = JsonVariables(node);

    children.push_back(jsonExpr);
    children.push_back(jsonPath);
    children.push_back(variables);
}

TNodePtr TSqlExpression::JsonValueCaseHandler(const TRule_json_case_handler& node, EJsonValueHandlerMode& mode) {
    /*
        json_case_handler: ERROR | NULL | (DEFAULT expr);
    */

    switch (node.GetAltCase()) {
        case TRule_json_case_handler::kAltJsonCaseHandler1: {
            const auto pos = GetPos(node.GetAlt_json_case_handler1().GetToken1());
            mode = EJsonValueHandlerMode::Error;
            return new TCallNodeImpl(pos, "Null", {});
        }
        case TRule_json_case_handler::kAltJsonCaseHandler2: {
            const auto pos = GetPos(node.GetAlt_json_case_handler2().GetToken1());
            mode = EJsonValueHandlerMode::DefaultValue;
            return new TCallNodeImpl(pos, "Null", {});
        }
        case TRule_json_case_handler::kAltJsonCaseHandler3:
            mode = EJsonValueHandlerMode::DefaultValue;
            return Build(node.GetAlt_json_case_handler3().GetBlock1().GetRule_expr2());
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

void TSqlExpression::AddJsonValueCaseHandlers(const TRule_json_value& node, TVector<TNodePtr>& children) {
    /*
        json_case_handler*
    */
    if (node.Block5Size() > 2) {
        Ctx.Error() << "Only 1 ON EMPTY and/or 1 ON ERROR clause is expected";
        Ctx.IncrementMonCounter("sql_errors", "JsonValueTooManyHandleClauses");
        return;
    }

    TNodePtr onEmpty;
    EJsonValueHandlerMode onEmptyMode = EJsonValueHandlerMode::DefaultValue;
    TNodePtr onError;
    EJsonValueHandlerMode onErrorMode = EJsonValueHandlerMode::DefaultValue;
    for (size_t i = 0; i < node.Block5Size(); i++) {
        const auto block = node.GetBlock5(i);
        const bool isEmptyClause = to_lower(block.GetToken3().GetValue()) == "empty";

        if (isEmptyClause && onEmpty != nullptr) {
            Ctx.Error() << "Only 1 ON EMPTY clause is expected";
            Ctx.IncrementMonCounter("sql_errors", "JsonValueMultipleOnEmptyClauses");
            return;
        }

        if (!isEmptyClause && onError != nullptr) {
            Ctx.Error() << "Only 1 ON ERROR clause is expected";
            Ctx.IncrementMonCounter("sql_errors", "JsonValueMultipleOnErrorClauses");
            return;
        }

        if (isEmptyClause && onError != nullptr) {
            Ctx.Error() << "ON EMPTY clause must be before ON ERROR clause";
            Ctx.IncrementMonCounter("sql_errors", "JsonValueOnEmptyAfterOnError");
            return;
        }

        EJsonValueHandlerMode currentMode;
        TNodePtr currentHandler = JsonValueCaseHandler(block.GetRule_json_case_handler1(), currentMode);

        if (isEmptyClause) {
            onEmpty = currentHandler;
            onEmptyMode = currentMode;
        } else {
            onError = currentHandler;
            onErrorMode = currentMode;
        }
    }

    if (onEmpty == nullptr) {
        onEmpty = new TCallNodeImpl(Ctx.Pos(), "Null", {});
    }

    if (onError == nullptr) {
        onError = new TCallNodeImpl(Ctx.Pos(), "Null", {});
    }

    children.push_back(BuildQuotedAtom(Ctx.Pos(), ToString(onEmptyMode), TNodeFlags::Default));
    children.push_back(onEmpty);
    children.push_back(BuildQuotedAtom(Ctx.Pos(), ToString(onErrorMode), TNodeFlags::Default));
    children.push_back(onError);
}

TNodePtr TSqlExpression::JsonValueExpr(const TRule_json_value& node) {
    /*
        json_value: JSON_VALUE LPAREN
            json_common_args
            (RETURNING type_name_simple)?
            (json_case_handler ON (EMPTY | ERROR))*
        RPAREN;
    */
    TVector<TNodePtr> children;
    AddJsonCommonArgs(node.GetRule_json_common_args3(), children);
    AddJsonValueCaseHandlers(node, children);

    if (node.HasBlock4()) {
        auto returningType = JsonReturningTypeRule(node.GetBlock4().GetRule_type_name_simple2());
        if (!returningType) {
            return {};
        }
        children.push_back(returningType);
    }

    return new TCallNodeImpl(GetPos(node.GetToken1()), "JsonValue", children);
}

void TSqlExpression::AddJsonExistsHandler(const TRule_json_exists& node, TVector<TNodePtr>& children) {
    /*
        json_exists: JSON_EXISTS LPAREN
            json_common_args
            json_exists_handler?
        RPAREN;
    */
    auto buildJustBool = [&](const TPosition& pos, bool value) {
        return new TCallNodeImpl(pos, "Just", {BuildLiteralBool(pos, value)});
    };

    if (!node.HasBlock4()) {
        children.push_back(buildJustBool(Ctx.Pos(), false));
        return;
    }

    const auto& handlerRule = node.GetBlock4().GetRule_json_exists_handler1();
    const auto& token = handlerRule.GetToken1();
    const auto pos = GetPos(token);
    const auto mode = to_lower(token.GetValue());
    if (mode == "unknown") {
        const auto nothingNode = new TCallNodeImpl(pos, "Nothing", {
            new TCallNodeImpl(pos, "OptionalType", {BuildDataType(pos, "Bool")})
        });
        children.push_back(nothingNode);
    } else if (mode != "error") {
        children.push_back(buildJustBool(pos, FromString<bool>(mode)));
    }
}

TNodePtr TSqlExpression::JsonExistsExpr(const TRule_json_exists& node) {
    /*
        json_exists: JSON_EXISTS LPAREN
            json_common_args
            json_exists_handler?
        RPAREN;
    */
    TVector<TNodePtr> children;
    AddJsonCommonArgs(node.GetRule_json_common_args3(), children);

    AddJsonExistsHandler(node, children);

    return new TCallNodeImpl(GetPos(node.GetToken1()), "JsonExists", children);
}

EJsonQueryWrap TSqlExpression::JsonQueryWrapper(const TRule_json_query& node) {
    /*
        json_query: JSON_QUERY LPAREN
            json_common_args
            (json_query_wrapper WRAPPER)?
            (json_query_handler ON EMPTY)?
            (json_query_handler ON ERROR)?
        RPAREN;
    */
    // default behaviour - no wrapping
    if (!node.HasBlock4()) {
        return EJsonQueryWrap::NoWrap;
    }

    // WITHOUT ARRAY? - no wrapping
    const auto& wrapperRule = node.GetBlock4().GetRule_json_query_wrapper1();
    if (wrapperRule.GetAltCase() == TRule_json_query_wrapper::kAltJsonQueryWrapper1) {
        return EJsonQueryWrap::NoWrap;
    }

    // WITH (CONDITIONAL | UNCONDITIONAL)? ARRAY? - wrapping depends on 2nd token. Default is UNCONDITIONAL
    const auto& withWrapperRule = wrapperRule.GetAlt_json_query_wrapper2().GetBlock1();
    if (!withWrapperRule.HasBlock2()) {
        return EJsonQueryWrap::Wrap;
    }

    const auto& token = withWrapperRule.GetBlock2().GetToken1();
    if (to_lower(token.GetValue()) == "conditional") {
        return EJsonQueryWrap::ConditionalWrap;
    } else {
        return EJsonQueryWrap::Wrap;
    }
}

EJsonQueryHandler TSqlExpression::JsonQueryHandler(const TRule_json_query_handler& node) {
    /*
        json_query_handler: ERROR | NULL | (EMPTY ARRAY) | (EMPTY OBJECT);
    */
    switch (node.GetAltCase()) {
        case TRule_json_query_handler::kAltJsonQueryHandler1:
            return EJsonQueryHandler::Error;
        case TRule_json_query_handler::kAltJsonQueryHandler2:
            return EJsonQueryHandler::Null;
        case TRule_json_query_handler::kAltJsonQueryHandler3:
            return EJsonQueryHandler::EmptyArray;
        case TRule_json_query_handler::kAltJsonQueryHandler4:
            return EJsonQueryHandler::EmptyObject;
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
}

TNodePtr TSqlExpression::JsonQueryExpr(const TRule_json_query& node) {
    /*
        json_query: JSON_QUERY LPAREN
            json_common_args
            (json_query_wrapper WRAPPER)?
            (json_query_handler ON EMPTY)?
            (json_query_handler ON ERROR)?
        RPAREN;
    */

    TVector<TNodePtr> children;
    AddJsonCommonArgs(node.GetRule_json_common_args3(), children);

    auto addChild = [&](TPosition pos, const TString& content) {
        children.push_back(BuildQuotedAtom(pos, content, TNodeFlags::Default));
    };

    const auto wrapMode = JsonQueryWrapper(node);
    addChild(Ctx.Pos(), ToString(wrapMode));

    auto onEmpty = EJsonQueryHandler::Null;
    if (node.HasBlock5()) {
        if (wrapMode != EJsonQueryWrap::NoWrap) {
            Ctx.Error() << "ON EMPTY is prohibited because WRAPPER clause is specified";
            Ctx.IncrementMonCounter("sql_errors", "JsonQueryOnEmptyWithWrapper");
            return nullptr;
        }
        onEmpty = JsonQueryHandler(node.GetBlock5().GetRule_json_query_handler1());
    }
    addChild(Ctx.Pos(), ToString(onEmpty));

    auto onError = EJsonQueryHandler::Null;
    if (node.HasBlock6()) {
        onError = JsonQueryHandler(node.GetBlock6().GetRule_json_query_handler1());
    }
    addChild(Ctx.Pos(), ToString(onError));

    return new TCallNodeImpl(GetPos(node.GetToken1()), "JsonQuery", children);
}

TNodePtr TSqlExpression::JsonApiExpr(const TRule_json_api_expr& node) {
    /*
        json_api_expr: json_value | json_exists | json_query;
    */
    TPosition pos = Ctx.Pos();
    TNodePtr result = nullptr;
    switch (node.GetAltCase()) {
        case TRule_json_api_expr::kAltJsonApiExpr1: {
            const auto& jsonValue = node.GetAlt_json_api_expr1().GetRule_json_value1();
            pos = GetPos(jsonValue.GetToken1());
            result = JsonValueExpr(jsonValue);
            break;
        }
        case TRule_json_api_expr::kAltJsonApiExpr2: {
            const auto& jsonExists = node.GetAlt_json_api_expr2().GetRule_json_exists1();
            pos = GetPos(jsonExists.GetToken1());
            result = JsonExistsExpr(jsonExists);
            break;
        }
        case TRule_json_api_expr::kAltJsonApiExpr3: {
            const auto& jsonQuery = node.GetAlt_json_api_expr3().GetRule_json_query1();
            pos = GetPos(jsonQuery.GetToken1());
            result = JsonQueryExpr(jsonQuery);
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    return result;
}

template<typename TUnaryCasualExprRule>
TNodePtr TSqlExpression::UnaryCasualExpr(const TUnaryCasualExprRule& node, const TTrailingQuestions& tail) {
    // unary_casual_subexpr: (id_expr | atom_expr) unary_subexpr_suffix;
    // OR
    // in_unary_casual_subexpr: (id_expr_in | in_atom_expr) unary_subexpr_suffix;
    // where
    // unary_subexpr_suffix: (key_expr | invoke_expr |(DOT (bind_parameter | DIGITS | id)))* (COLLATE id)?;

    const auto& suffix = node.GetRule_unary_subexpr_suffix2();
    const bool suffixIsEmpty = suffix.GetBlock1().empty() && !suffix.HasBlock2();
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && suffixIsEmpty;
    TString name;
    TNodePtr expr;
    bool typePossible = false;
    auto& block = node.GetBlock1();
    switch (block.Alt_case()) {
        case TUnaryCasualExprRule::TBlock1::kAlt1: {
            MaybeUnnamedSmartParenOnTop = false;
            auto& alt = block.GetAlt1();
            if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                name = Id(alt.GetRule_id_expr1(), *this);
                typePossible = !IsQuotedId(alt.GetRule_id_expr1(), *this);
            } else {
                // type was never possible here
                name = Id(alt.GetRule_id_expr_in1(), *this);
            }
            break;
        }
        case TUnaryCasualExprRule::TBlock1::kAlt2: {
            auto& alt = block.GetAlt2();
            TMaybe<TExprOrIdent> exprOrId;
            if constexpr (std::is_same_v<TUnaryCasualExprRule, TRule_unary_casual_subexpr>) {
                exprOrId = AtomExpr(alt.GetRule_atom_expr1(), suffixIsEmpty ? tail : TTrailingQuestions{});
            } else {
                MaybeUnnamedSmartParenOnTop = false;
                exprOrId = InAtomExpr(alt.GetRule_in_atom_expr1(), suffixIsEmpty ? tail : TTrailingQuestions{});
            }

            if (!exprOrId) {
                Ctx.IncrementMonCounter("sql_errors", "BadAtomExpr");
                return nullptr;
            }
            if (!exprOrId->Expr) {
                name = exprOrId->Ident;
            } else {
                expr = exprOrId->Expr;
            }
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    // bool onlyDots = true;
    bool isColumnRef = !expr;
    bool isFirstElem = true;

    for (auto& b : suffix.GetBlock1()) {
        switch (b.Alt_case()) {
        case TRule_unary_subexpr_suffix::TBlock1::kAlt1: {
            // key_expr
            // onlyDots = false;
            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::kAlt2: {
            // invoke_expr - cannot be a column, function name
            if (isFirstElem) {
                isColumnRef = false;
            }

            // onlyDots = false;
            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::kAlt3: {
            // dot
            break;
        }
        default:
            AltNotImplemented("unary_subexpr_suffix", b);
            return nullptr;
        }

        isFirstElem = false;
    }

    isFirstElem = true;
    TVector<INode::TIdPart> ids;
    INode::TPtr lastExpr;
    if (!isColumnRef) {
        lastExpr = expr;
    } else {
        const bool flexibleTypes = Ctx.FlexibleTypes;
        bool columnOrType = false;
        auto columnRefsState = Ctx.GetColumnReferenceState();
        bool explicitPgType = columnRefsState == EColumnRefState::AsPgType;
        if (explicitPgType && typePossible && suffixIsEmpty) {
            auto pgType = BuildSimpleType(Ctx, Ctx.Pos(), name, false);
            if (pgType && tail.Count) {
                Ctx.Error() << "Optional types are not supported in this context";
                return {};
            }
            return pgType;
        }
        if (auto simpleType = LookupSimpleType(name, flexibleTypes, false); simpleType && typePossible && suffixIsEmpty) {
            if (tail.Count > 0 || columnRefsState == EColumnRefState::Deny || !flexibleTypes) {
                // a type
                return AddOptionals(BuildSimpleType(Ctx, Ctx.Pos(), name, false), tail.Count);
            }
            // type or column: ambiguity will be resolved on type annotation stage
            columnOrType = columnRefsState == EColumnRefState::Allow;
        }
        if (tail.Count) {
            UnexpectedQuestionToken(tail);
            return {};
        }
        if (!Ctx.CheckColumnReference(Ctx.Pos(), name)) {
            return nullptr;
        }

        ids.push_back(columnOrType ? BuildColumnOrType(Ctx.Pos()) : BuildColumn(Ctx.Pos()));
        ids.push_back(name);
    }

    TPosition pos(Ctx.Pos());
    for (auto& b : suffix.GetBlock1()) {
        switch (b.Alt_case()) {
        case TRule_unary_subexpr_suffix::TBlock1::kAlt1: {
            // key_expr
            auto keyExpr = KeyExpr(b.GetAlt1().GetRule_key_expr1());
            if (!keyExpr) {
                Ctx.IncrementMonCounter("sql_errors", "BadKeyExpr");
                return nullptr;
            }

            if (!lastExpr) {
                lastExpr = BuildAccess(pos, ids, false);
                ids.clear();
            }

            ids.push_back(lastExpr);
            ids.push_back(keyExpr);
            lastExpr = BuildAccess(pos, ids, true);
            ids.clear();
            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::kAlt2: {
            // invoke_expr - cannot be a column, function name
            TSqlCallExpr call(Ctx, Mode);
            if (isFirstElem && !name.Empty()) {
                call.AllowDistinct();
                call.InitName(name);
            } else {
                call.InitExpr(lastExpr);
            }

            bool initRet = call.Init(b.GetAlt2().GetRule_invoke_expr1());
            if (initRet) {
                call.IncCounters();
            }

            if (!initRet) {
                return nullptr;
            }

            lastExpr = call.BuildCall();
            if (!lastExpr) {
                return nullptr;
            }

            break;
        }
        case TRule_unary_subexpr_suffix::TBlock1::kAlt3: {
            // dot
            if (lastExpr) {
                ids.push_back(lastExpr);
            }

            auto bb = b.GetAlt3().GetBlock1().GetBlock2();
            switch (bb.Alt_case()) {
                case TRule_unary_subexpr_suffix_TBlock1_TAlt3_TBlock1_TBlock2::kAlt1: {
                    TString named;
                    if (!NamedNodeImpl(bb.GetAlt1().GetRule_bind_parameter1(), named, *this)) {
                        return nullptr;
                    }
                    auto namedNode = GetNamedNode(named);
                    if (!namedNode) {
                        return nullptr;
                    }

                    ids.push_back(named);
                    ids.back().Expr = namedNode;
                    break;
                }
                case TRule_unary_subexpr_suffix_TBlock1_TAlt3_TBlock1_TBlock2::kAlt2: {
                    const TString str(Token(bb.GetAlt2().GetToken1()));
                    ids.push_back(str);
                    break;
                }
                case TRule_unary_subexpr_suffix_TBlock1_TAlt3_TBlock1_TBlock2::kAlt3: {
                    ids.push_back(Id(bb.GetAlt3().GetRule_an_id_or_type1(), *this));
                    break;
                }
                default:
                    Y_FAIL("You should change implementation according to grammar changes");
            }

            if (lastExpr) {
                lastExpr = BuildAccess(pos, ids, false);
                ids.clear();
            }

            break;
        }
        default:
            AltNotImplemented("unary_subexpr_suffix", b);
            return nullptr;
        }

        isFirstElem = false;
    }

    if (!lastExpr) {
        lastExpr = BuildAccess(pos, ids, false);
        ids.clear();
    }

    if (suffix.HasBlock2()) {
        Ctx.IncrementMonCounter("sql_errors", "CollateUnarySubexpr");
        Error() << "unary_subexpr: COLLATE is not implemented yet";
    }

    return lastExpr;
}

TNodePtr TSqlExpression::BindParameterRule(const TRule_bind_parameter& rule, const TTrailingQuestions& tail) {
    TString namedArg;
    if (!NamedNodeImpl(rule, namedArg, *this)) {
        return {};
    }
    if (SmartParenthesisMode == ESmartParenthesis::SqlLambdaParams) {
        Ctx.IncrementMonCounter("sql_features", "LambdaArgument");
        if (tail.Count > 1) {
            Ctx.Error(tail.Pos) << "Expecting at most one '?' token here (for optional lambda parameters), but got " << tail.Count;
            return {};
        }
        return BuildAtom(Ctx.Pos(), namedArg, NYql::TNodeFlags::ArbitraryContent, tail.Count != 0);
    }
    if (tail.Count) {
        UnexpectedQuestionToken(tail);
        return {};
    }
    Ctx.IncrementMonCounter("sql_features", "NamedNodeUseAtom");
    return GetNamedNode(namedArg);
}

TNodePtr TSqlExpression::LambdaRule(const TRule_lambda& rule) {
    const auto& alt = rule;
    const bool isSqlLambda = alt.HasBlock2();
    if (!isSqlLambda) {
        return SmartParenthesis(alt.GetRule_smart_parenthesis1());
    }

    MaybeUnnamedSmartParenOnTop = false;
    TNodePtr parenthesis;
    {
        // we allow column reference here to postpone error and report it with better description in SqlLambdaParams
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        expr.SetSmartParenthesisMode(ESmartParenthesis::SqlLambdaParams);
        parenthesis = expr.SmartParenthesis(alt.GetRule_smart_parenthesis1());
    }
    if (!parenthesis) {
        return {};
    }

    ui32 optionalArgumentsCount = 0;
    TVector<TSymbolNameWithPos> args;
    if (!SqlLambdaParams(parenthesis, args, optionalArgumentsCount)) {
        return {};
    }
    auto bodyBlock = alt.GetBlock2();
    Token(bodyBlock.GetToken1());
    TPosition pos(Ctx.Pos());
    TVector<TNodePtr> exprSeq;
    for (auto& arg: args) {
        arg.Name = PushNamedAtom(arg.Pos, arg.Name);
    }
    bool ret = false;
    TColumnRefScope scope(Ctx, EColumnRefState::Deny);
    scope.SetNoColumnErrContext("in lambda function");
    if (bodyBlock.GetBlock2().HasAlt1()) {
        ret = SqlLambdaExprBody(Ctx, bodyBlock.GetBlock2().GetAlt1().GetBlock1().GetRule_expr2(), exprSeq);
    } else {
        ret = SqlLambdaExprBody(Ctx, bodyBlock.GetBlock2().GetAlt2().GetBlock1().GetRule_lambda_body2(), exprSeq);
    }

    TVector<TString> argNames;
    for (const auto& arg : args) {
        argNames.push_back(arg.Name);
        PopNamedNode(arg.Name);
    }
    if (!ret) {
        return {};
    }

    auto lambdaNode = BuildSqlLambda(pos, std::move(argNames), std::move(exprSeq));
    if (optionalArgumentsCount > 0) {
        lambdaNode = new TCallNodeImpl(pos, "WithOptionalArgs", {
            lambdaNode,
            BuildQuotedAtom(pos, ToString(optionalArgumentsCount), TNodeFlags::Default)
            });
    }

    return lambdaNode;
}

TNodePtr TSqlExpression::CastRule(const TRule_cast_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Cast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx.Pos());
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(rule.GetRule_expr3());
    if (!exprNode) {
        return {};
    }
    auto type = TypeNodeOrBind(rule.GetRule_type_name_or_bind5());
    if (!type) {
        return {};
    }
    return new TCallNodeImpl(pos, "SafeCast", {exprNode, type});
}

TNodePtr TSqlExpression::BitCastRule(const TRule_bitcast_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "BitCast");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TPosition pos(Ctx.Pos());
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(rule.GetRule_expr3());
    if (!exprNode) {
        return {};
    }
    auto type = TypeSimple(rule.GetRule_type_name_simple5(), true);
    if (!type) {
        return {};
    }
    return new TCallNodeImpl(pos, "BitCast", {exprNode, type});
}

TNodePtr TSqlExpression::ExistsRule(const TRule_exists_expr& rule) {
    Ctx.IncrementMonCounter("sql_features", "Exists");

    TPosition pos;
    TSourcePtr source;
    Token(rule.GetToken2());
    switch (rule.GetBlock3().Alt_case()) {
        case TRule_exists_expr::TBlock3::kAlt1: {
            const auto& alt = rule.GetBlock3().GetAlt1().GetRule_select_stmt1();
            TSqlSelect select(Ctx, Mode);
            source = select.Build(alt, pos);
            break;
        }
        case TRule_exists_expr::TBlock3::kAlt2: {
            const auto& alt = rule.GetBlock3().GetAlt2().GetRule_values_stmt1();
            TSqlValues values(Ctx, Mode);
            source = values.Build(alt, pos);
            break;
        }
        default:
            AltNotImplemented("exists_expr", rule.GetBlock3());
    }

    if (!source) {
        Ctx.IncrementMonCounter("sql_errors", "BadSource");
        return nullptr;
    }
    const bool checkExist = true;
    return BuildBuiltinFunc(Ctx, Ctx.Pos(), "ListHasItems", {BuildSourceNode(pos, std::move(source), checkExist)});
}

TNodePtr TSqlExpression::CaseRule(const TRule_case_expr& rule) {
    // case_expr: CASE expr? when_expr+ (ELSE expr)? END;
    // when_expr: WHEN expr THEN expr;
    Ctx.IncrementMonCounter("sql_features", "Case");
    const auto& alt = rule;
    Token(alt.GetToken1());
    TNodePtr elseExpr;
    if (alt.HasBlock4()) {
        Token(alt.GetBlock4().GetToken1());
        TSqlExpression expr(Ctx, Mode);
        elseExpr = expr.Build(alt.GetBlock4().GetRule_expr2());
    } else {
        Ctx.IncrementMonCounter("sql_errors", "ElseIsRequired");
        Error() << "ELSE is required";
        return {};
    }

    TNodePtr caseExpr;
    if (alt.HasBlock2()) {
        TSqlExpression expr(Ctx, Mode);
        caseExpr = expr.Build(alt.GetBlock2().GetRule_expr1());
        if (!caseExpr) {
            return {};
        }
    }

    TVector<TCaseBranch> branches;
    for (size_t i = 0; i < alt.Block3Size(); ++i) {
        branches.emplace_back();
        const auto& block = alt.GetBlock3(i).GetRule_when_expr1();
        Token(block.GetToken1());
        TSqlExpression condExpr(Ctx, Mode);
        branches.back().Pred = condExpr.Build(block.GetRule_expr2());
        if (caseExpr) {
            branches.back().Pred = BuildBinaryOp(Ctx, Ctx.Pos(), "==", caseExpr, branches.back().Pred);
        }
        if (!branches.back().Pred) {
            return {};
        }
        Token(block.GetToken3());
        TSqlExpression thenExpr(Ctx, Mode);
        branches.back().Value = thenExpr.Build(block.GetRule_expr4());
        if (!branches.back().Value) {
            return {};
        }
    }
    auto final = ReduceCaseBranches(branches.begin(), branches.end());
    return BuildBuiltinFunc(Ctx, Ctx.Pos(), "If", { final.Pred, final.Value, elseExpr });
}

TMaybe<TExprOrIdent> TSqlExpression::AtomExpr(const TRule_atom_expr& node, const TTrailingQuestions& tail) {
    // atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | lambda
    //   | cast_expr
    //   | exists_expr
    //   | case_expr
    //   | an_id_or_type NAMESPACE (id_or_type | STRING_VALUE)
    //   | value_constructor
    //   | bitcast_expr
    //   | list_literal
    //   | dict_literal
    //   | struct_literal
    // ;
    if (node.Alt_case() != TRule_atom_expr::kAltAtomExpr2 && tail.Count) {
        UnexpectedQuestionToken(tail);
        return {};
    }
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (node.Alt_case() == TRule_atom_expr::kAltAtomExpr3);
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_atom_expr::kAltAtomExpr1:
            Ctx.IncrementMonCounter("sql_features", "LiteralExpr");
            return LiteralExpr(node.GetAlt_atom_expr1().GetRule_literal_value1());
        case TRule_atom_expr::kAltAtomExpr2:
            result.Expr = BindParameterRule(node.GetAlt_atom_expr2().GetRule_bind_parameter1(), tail);
            break;
        case TRule_atom_expr::kAltAtomExpr3:
            result.Expr = LambdaRule(node.GetAlt_atom_expr3().GetRule_lambda1());
            break;
        case TRule_atom_expr::kAltAtomExpr4:
            result.Expr = CastRule(node.GetAlt_atom_expr4().GetRule_cast_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr5:
            result.Expr = ExistsRule(node.GetAlt_atom_expr5().GetRule_exists_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr6:
            result.Expr = CaseRule(node.GetAlt_atom_expr6().GetRule_case_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr7: {
            const auto& alt = node.GetAlt_atom_expr7();
            TString module(Id(alt.GetRule_an_id_or_type1(), *this));
            TPosition pos(Ctx.Pos());
            TString name;
            switch (alt.GetBlock3().Alt_case()) {
                case TRule_atom_expr::TAlt7::TBlock3::kAlt1:
                    name = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), *this);
                    break;
                case TRule_atom_expr::TAlt7::TBlock3::kAlt2: {
                    name = Token(alt.GetBlock3().GetAlt2().GetToken1());
                    if (Ctx.AnsiQuotedIdentifiers && name.StartsWith('"')) {
                        // same as previous case
                        name = IdContentFromString(Ctx, name);
                    } else {
                        module = "@" + module;
                    }
                    break;
                }
                default:
                    Y_FAIL("Unsigned number: you should change implementation according to grammar changes");
            }
            result.Expr = BuildCallable(pos, module, name, {});
            break;
        }
        case TRule_atom_expr::kAltAtomExpr8: {
            result.Expr = ValueConstructor(node.GetAlt_atom_expr8().GetRule_value_constructor1());
            break;
        }
        case TRule_atom_expr::kAltAtomExpr9:
            result.Expr = BitCastRule(node.GetAlt_atom_expr9().GetRule_bitcast_expr1());
            break;
        case TRule_atom_expr::kAltAtomExpr10:
            result.Expr = ListLiteral(node.GetAlt_atom_expr10().GetRule_list_literal1());
            break;
        case TRule_atom_expr::kAltAtomExpr11:
            result.Expr = DictLiteral(node.GetAlt_atom_expr11().GetRule_dict_literal1());
            break;
        case TRule_atom_expr::kAltAtomExpr12:
            result.Expr = StructLiteral(node.GetAlt_atom_expr12().GetRule_struct_literal1());
            break;
        default:
            AltNotImplemented("atom_expr", node);
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

TMaybe<TExprOrIdent> TSqlExpression::InAtomExpr(const TRule_in_atom_expr& node, const TTrailingQuestions& tail) {
    // in_atom_expr:
    //     literal_value
    //   | bind_parameter
    //   | lambda
    //   | cast_expr
    //   | case_expr
    //   | an_id_or_type NAMESPACE (id_or_type | STRING_VALUE)
    //   | LPAREN select_stmt RPAREN
    //   | value_constructor
    //   | bitcast_expr
    //   | list_literal
    //   | dict_literal
    //   | struct_literal
    // ;
    if (node.Alt_case() != TRule_in_atom_expr::kAltInAtomExpr2 && tail.Count) {
        UnexpectedQuestionToken(tail);
        return {};
    }
    TExprOrIdent result;
    switch (node.Alt_case()) {
        case TRule_in_atom_expr::kAltInAtomExpr1:
            Ctx.IncrementMonCounter("sql_features", "LiteralExpr");
            return LiteralExpr(node.GetAlt_in_atom_expr1().GetRule_literal_value1());
        case TRule_in_atom_expr::kAltInAtomExpr2:
            result.Expr = BindParameterRule(node.GetAlt_in_atom_expr2().GetRule_bind_parameter1(), tail);
            break;
        case TRule_in_atom_expr::kAltInAtomExpr3:
            result.Expr = LambdaRule(node.GetAlt_in_atom_expr3().GetRule_lambda1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr4:
            result.Expr = CastRule(node.GetAlt_in_atom_expr4().GetRule_cast_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr5:
            result.Expr = CaseRule(node.GetAlt_in_atom_expr5().GetRule_case_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr6: {
            const auto& alt = node.GetAlt_in_atom_expr6();
            TString module(Id(alt.GetRule_an_id_or_type1(), *this));
            TPosition pos(Ctx.Pos());
            TString name;
            switch (alt.GetBlock3().Alt_case()) {
            case TRule_in_atom_expr::TAlt6::TBlock3::kAlt1:
                name = Id(alt.GetBlock3().GetAlt1().GetRule_id_or_type1(), *this);
                break;
            case TRule_in_atom_expr::TAlt6::TBlock3::kAlt2: {
                name = Token(alt.GetBlock3().GetAlt2().GetToken1());
                if (Ctx.AnsiQuotedIdentifiers && name.StartsWith('"')) {
                    // same as previous case
                    name = IdContentFromString(Ctx, name);
                } else {
                    module = "@" + module;
                }
                break;
            }
            default:
                Y_FAIL("You should change implementation according to grammar changes");
            }
            result.Expr = BuildCallable(pos, module, name, {});
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr7: {
            Token(node.GetAlt_in_atom_expr7().GetToken1());
            // reset column reference scope (select will reenable it where needed)
            TColumnRefScope scope(Ctx, EColumnRefState::Deny);
            TSqlSelect select(Ctx, Mode);
            TPosition pos;
            auto source = select.Build(node.GetAlt_in_atom_expr7().GetRule_select_stmt2(), pos);
            if (!source) {
                Ctx.IncrementMonCounter("sql_errors", "BadSource");
                return {};
            }
            Ctx.IncrementMonCounter("sql_features", "InSubquery");
            const auto alias = Ctx.MakeName("subquerynode");
            const auto ref = Ctx.MakeName("subquery");
            auto& blocks = Ctx.GetCurrentBlocks();
            blocks.push_back(BuildSubquery(std::move(source), alias, Mode == NSQLTranslation::ESqlMode::SUBQUERY, -1, Ctx.Scoped));
            blocks.back()->SetLabel(ref);
            result.Expr = BuildSubqueryRef(blocks.back(), ref, -1);
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr8: {
            result.Expr = ValueConstructor(node.GetAlt_in_atom_expr8().GetRule_value_constructor1());
            break;
        }
        case TRule_in_atom_expr::kAltInAtomExpr9:
            result.Expr = BitCastRule(node.GetAlt_in_atom_expr9().GetRule_bitcast_expr1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr10:
            result.Expr = ListLiteral(node.GetAlt_in_atom_expr10().GetRule_list_literal1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr11:
            result.Expr = DictLiteral(node.GetAlt_in_atom_expr11().GetRule_dict_literal1());
            break;
        case TRule_in_atom_expr::kAltInAtomExpr12:
            result.Expr = StructLiteral(node.GetAlt_in_atom_expr12().GetRule_struct_literal1());
            break;
        default:
            AltNotImplemented("in_atom_expr", node);
    }
    if (!result.Expr) {
        return {};
    }
    return result;
}

bool TSqlExpression::SqlLambdaParams(const TNodePtr& node, TVector<TSymbolNameWithPos>& args, ui32& optionalArgumentsCount) {
    args.clear();
    optionalArgumentsCount = 0;
    auto errMsg = TStringBuf("Invalid lambda arguments syntax. Lambda arguments should start with '$' as named value.");
    auto tupleNodePtr = dynamic_cast<TTupleNode*>(node.Get());
    if (!tupleNodePtr) {
        Ctx.Error(node->GetPos()) << errMsg;
        return false;
    }
    THashSet<TString> dupArgsChecker;
    for (const auto& argPtr: tupleNodePtr->Elements()) {
        auto contentPtr = argPtr->GetAtomContent();
        if (!contentPtr || !contentPtr->StartsWith("$")) {
            Ctx.Error(argPtr->GetPos()) << errMsg;
            return false;
        }
        if (argPtr->IsOptionalArg()) {
            ++optionalArgumentsCount;
        } else if (optionalArgumentsCount > 0) {
            Ctx.Error(argPtr->GetPos()) << "Non-optional argument can not follow optional one";
            return false;
        }

        if (!IsAnonymousName(*contentPtr) && !dupArgsChecker.insert(*contentPtr).second) {
            Ctx.Error(argPtr->GetPos()) << "Duplicate lambda argument parametr: '" << *contentPtr << "'.";
            return false;
        }
        args.push_back(TSymbolNameWithPos{*contentPtr, argPtr->GetPos()});
    }
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_expr& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    TNodePtr nodeExpr = expr.Build(node);
    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

bool TSqlExpression::SqlLambdaExprBody(TContext& ctx, const TRule_lambda_body& node, TVector<TNodePtr>& exprSeq) {
    TSqlExpression expr(ctx, ctx.Settings.Mode);
    TVector<TString> localNames;
    bool hasError = false;
    for (auto& block: node.GetBlock2()) {
        const auto& rule = block.GetRule_lambda_stmt1();
        switch (rule.Alt_case()) {
            case TRule_lambda_stmt::kAltLambdaStmt1: {
                TVector<TSymbolNameWithPos> names;
                auto nodeExpr = NamedNode(rule.GetAlt_lambda_stmt1().GetRule_named_nodes_stmt1(), names);
                if (!nodeExpr) {
                    hasError = true;
                    continue;
                } else if (nodeExpr->GetSource()) {
                    ctx.Error() << "SELECT is not supported inside lambda body";
                    hasError = true;
                    continue;
                }
                if (names.size() > 1) {
                    auto ref = ctx.MakeName("tie");
                    exprSeq.push_back(nodeExpr->Y("EnsureTupleSize", nodeExpr, nodeExpr->Q(ToString(names.size()))));
                    exprSeq.back()->SetLabel(ref);
                    for (size_t i = 0; i < names.size(); ++i) {
                        TNodePtr nthExpr = nodeExpr->Y("Nth", ref, nodeExpr->Q(ToString(i)));
                        names[i].Name = PushNamedAtom(names[i].Pos, names[i].Name);
                        nthExpr->SetLabel(names[i].Name);
                        localNames.push_back(names[i].Name);
                        exprSeq.push_back(nthExpr);
                    }
                } else {
                    auto& symbol = names.front();
                    symbol.Name = PushNamedAtom(symbol.Pos, symbol.Name);
                    nodeExpr->SetLabel(symbol.Name);
                    localNames.push_back(symbol.Name);
                    exprSeq.push_back(nodeExpr);
                }
                break;
            }
            case TRule_lambda_stmt::kAltLambdaStmt2: {
                if (!ImportStatement(rule.GetAlt_lambda_stmt2().GetRule_import_stmt1(), &localNames)) {
                    hasError = true;
                }
                break;
            }
            default:
                Y_FAIL("SampleClause: does not correspond to grammar changes");
        }
    }

    TNodePtr nodeExpr;
    if (!hasError) {
        nodeExpr = expr.Build(node.GetRule_expr4());
    }

    for (const auto& name : localNames) {
        PopNamedNode(name);
    }

    if (!nodeExpr) {
        return false;
    }
    exprSeq.push_back(nodeExpr);
    return true;
}

TNodePtr TSqlExpression::SubExpr(const TRule_con_subexpr& node, const TTrailingQuestions& tail) {
    // con_subexpr: unary_subexpr | unary_op unary_subexpr;
    switch (node.Alt_case()) {
        case TRule_con_subexpr::kAltConSubexpr1:
            return UnaryExpr(node.GetAlt_con_subexpr1().GetRule_unary_subexpr1(), tail);
        case TRule_con_subexpr::kAltConSubexpr2: {
            MaybeUnnamedSmartParenOnTop = false;
            Ctx.IncrementMonCounter("sql_features", "UnaryOperation");
            TString opName;
            auto token = node.GetAlt_con_subexpr2().GetRule_unary_op1().GetToken1();
            Token(token);
            TPosition pos(Ctx.Pos());
            switch (token.GetId()) {
                case SQLv1LexerTokens::TOKEN_NOT: opName = "Not"; break;
                case SQLv1LexerTokens::TOKEN_PLUS: opName = "Plus"; break;
                case SQLv1LexerTokens::TOKEN_MINUS: opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedMinus" : "Minus"; break;
                case SQLv1LexerTokens::TOKEN_TILDA: opName = "BitNot"; break;
                default:
                    Ctx.IncrementMonCounter("sql_errors", "UnsupportedUnaryOperation");
                    Error() << "Unsupported unary operation: " << token.GetValue();
                    return nullptr;
            }
            Ctx.IncrementMonCounter("sql_unary_operations", opName);
            auto expr = UnaryExpr(node.GetAlt_con_subexpr2().GetRule_unary_subexpr2(), tail);
            return expr ? expr->ApplyUnaryOp(Ctx, pos, opName) : expr;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    return nullptr;
}

TNodePtr TSqlExpression::SubExpr(const TRule_xor_subexpr& node, const TTrailingQuestions& tail) {
    // xor_subexpr: eq_subexpr cond_expr?;
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && !node.HasBlock2();
    TNodePtr res(SubExpr(node.GetRule_eq_subexpr1(), node.HasBlock2() ? TTrailingQuestions{} : tail));
    if (!res) {
        return {};
    }
    TPosition pos(Ctx.Pos());
    if (node.HasBlock2()) {
        auto cond = node.GetBlock2().GetRule_cond_expr1();
        switch (cond.Alt_case()) {
            case TRule_cond_expr::kAltCondExpr1: {
                const auto& matchOp = cond.GetAlt_cond_expr1();
                const bool notMatch = matchOp.HasBlock1();
                const TCiString& opName = Token(matchOp.GetRule_match_op2().GetToken1());
                const auto& pattern = SubExpr(cond.GetAlt_cond_expr1().GetRule_eq_subexpr3(), matchOp.HasBlock4() ? TTrailingQuestions{} : tail);
                if (!pattern) {
                    return {};
                }
                TNodePtr isMatch;
                if (opName == "like" || opName == "ilike") {
                    const TString* escapeLiteral = nullptr;
                    TNodePtr escapeNode;
                    const auto& escaper = BuildUdf(Ctx, pos, "Re2", "PatternFromLike", {});
                    TVector<TNodePtr> escaperArgs({ escaper, pattern });

                    if (matchOp.HasBlock4()) {
                        const auto& escapeBlock = matchOp.GetBlock4();
                        TNodePtr escapeExpr = SubExpr(escapeBlock.GetRule_eq_subexpr2(), tail);
                        if (!escapeExpr) {
                            return {};
                        }
                        escapeLiteral = escapeExpr->GetLiteral("String");
                        escapeNode = escapeExpr;
                        if (escapeLiteral) {
                            Ctx.IncrementMonCounter("sql_features", "LikeEscape");
                            if (escapeLiteral->size() != 1) {
                                Ctx.IncrementMonCounter("sql_errors", "LikeMultiCharEscape");
                                Error() << "ESCAPE clause requires single character argument";
                                return nullptr;
                            }
                            if (escapeLiteral[0] == "%" || escapeLiteral[0] == "_" || escapeLiteral[0] == "\\") {
                                Ctx.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "'%', '_' and '\\' are currently not supported in ESCAPE clause, ";
                                Error() << "please choose any other character";
                                return nullptr;
                            }
                            if (!IsAscii(escapeLiteral->front())) {
                                Ctx.IncrementMonCounter("sql_errors", "LikeUnsupportedEscapeChar");
                                Error() << "Non-ASCII symbols are not supported in ESCAPE clause, ";
                                Error() << "please choose ASCII character";
                                return nullptr;
                            }
                            escaperArgs.push_back(BuildLiteralRawString(pos, *escapeLiteral));
                        } else {
                            Ctx.IncrementMonCounter("sql_errors", "LikeNotLiteralEscape");
                            Error() << "ESCAPE clause requires String literal argument";
                            return nullptr;
                        }
                    }

                    auto re2options = BuildUdf(Ctx, pos, "Re2", "Options", {});
                    if (opName == "ilike") {
                        Ctx.IncrementMonCounter("sql_features", "CaseInsensitiveLike");
                    }
                    auto csModeLiteral = BuildLiteralBool(pos, opName != "ilike");
                    csModeLiteral->SetLabel("CaseSensitive");
                    auto csOption = BuildStructure(pos, { csModeLiteral });
                    auto optionsApply = new TCallNodeImpl(pos, "NamedApply", { re2options, BuildTuple(pos, {}), csOption });

                    const TNodePtr escapedPattern = new TCallNodeImpl(pos, "Apply", { escaperArgs });
                    auto list = new TAstListNodeImpl(pos, { escapedPattern, optionsApply });
                    auto runConfig = new TAstListNodeImpl(pos, { new TAstAtomNodeImpl(pos, "quote", 0), list });

                    const TNodePtr matcher = new TCallNodeImpl(pos, "AssumeStrict", { BuildUdf(Ctx, pos, "Re2", "Match", { runConfig }) });
                    isMatch = new TCallNodeImpl(pos, "Apply", { matcher, res });

                    bool isUtf8 = false;
                    const TString* literalPattern = pattern->GetLiteral("String");
                    if (!literalPattern) {
                        literalPattern = pattern->GetLiteral("Utf8");
                        isUtf8 = literalPattern != nullptr;
                    }

                    if (literalPattern) {
                        bool inEscape = false;
                        TMaybe<char> escape;
                        if (escapeLiteral) {
                            escape = escapeLiteral->front();
                        }

                        bool mayIgnoreCase;
                        TVector<TPatternComponent<char>> components;
                        if (isUtf8) {
                            auto splitResult = SplitPattern(UTF8ToUTF32<false>(*literalPattern), escape, inEscape);
                            for (const auto& component : splitResult) {
                                TPatternComponent<char> converted;
                                converted.IsSimple = component.IsSimple;
                                converted.Prefix = WideToUTF8(component.Prefix);
                                converted.Suffix = WideToUTF8(component.Suffix);
                                components.push_back(std::move(converted));
                            }
                            mayIgnoreCase = ToLowerUTF8(*literalPattern) == ToUpperUTF8(*literalPattern);
                        } else {
                            components = SplitPattern(*literalPattern, escape, inEscape);
                            mayIgnoreCase = WithoutAlpha(*literalPattern);
                        }

                        if (inEscape) {
                            Ctx.IncrementMonCounter("sql_errors", "LikeEscapeSymbolEnd");
                            Error() << "LIKE pattern should not end with escape symbol";
                            return nullptr;
                        }

                        if (opName == "like" || mayIgnoreCase) {
                            // TODO: expand LIKE in optimizers - we can analyze argument types there
                            YQL_ENSURE(!components.empty());
                            const auto& first = components.front();
                            if (components.size() == 1 && first.IsSimple) {
                                // no '%'s and '_'s  in pattern
                                YQL_ENSURE(first.Prefix == first.Suffix);
                                isMatch = BuildBinaryOp(Ctx, pos, "==", res, BuildLiteralRawString(pos, first.Suffix, isUtf8));
                            } else if (!first.Prefix.empty()) {
                                const TString& prefix = first.Prefix;
                                TNodePtr prefixMatch;
                                if (Ctx.EmitStartsWith) {
                                    prefixMatch = BuildBinaryOp(Ctx, pos, "StartsWith", res, BuildLiteralRawString(pos, prefix, isUtf8));
                                } else {
                                    prefixMatch = BuildBinaryOp(Ctx, pos, ">=", res, BuildLiteralRawString(pos, prefix, isUtf8));
                                    auto upperBound = isUtf8 ? NextValidUtf8(prefix) : NextLexicographicString(prefix);
                                    if (upperBound) {
                                        prefixMatch = BuildBinaryOp(
                                            Ctx,
                                            pos,
                                            "And",
                                            prefixMatch,
                                            BuildBinaryOp(Ctx, pos, "<", res, BuildLiteralRawString(pos, TString(*upperBound), isUtf8))
                                        );
                                    }
                                }

                                if (Ctx.AnsiLike && first.IsSimple && components.size() == 2 && components.back().IsSimple) {
                                    const TString& suffix = components.back().Suffix;
                                    // 'prefix%suffix'
                                    if (suffix.empty()) {
                                        isMatch = prefixMatch;
                                    } else {
                                        // len(str) >= len(prefix) + len(suffix) && StartsWith(str, prefix) && EndsWith(str, suffix)
                                        TNodePtr sizePred = BuildBinaryOp(Ctx, pos, ">=",
                                            TNodePtr(new TCallNodeImpl(pos, "Size", { res })),
                                            TNodePtr(new TLiteralNumberNode<ui32>(pos, "Uint32", ToString(prefix.size() + suffix.size()))));
                                        TNodePtr suffixMatch = BuildBinaryOp(Ctx, pos, "EndsWith", res, BuildLiteralRawString(pos, suffix, isUtf8));
                                        isMatch = new TCallNodeImpl(pos, "And", {
                                            sizePred,
                                            prefixMatch,
                                            suffixMatch
                                        });
                                    }
                                } else {
                                    isMatch = BuildBinaryOp(Ctx, pos, "And", prefixMatch, isMatch);
                                }
                            } else if (Ctx.AnsiLike && AllOf(components, [](const auto& comp) { return comp.IsSimple; })) {
                                YQL_ENSURE(first.Prefix.empty());
                                if (components.size() == 3 && components.back().Prefix.empty()) {
                                    // '%foo%'
                                    YQL_ENSURE(!components[1].Prefix.empty());
                                    isMatch = BuildBinaryOp(Ctx, pos, "StringContains", res, BuildLiteralRawString(pos, components[1].Prefix, isUtf8));
                                } else if (components.size() == 2) {
                                    // '%foo'
                                    isMatch = BuildBinaryOp(Ctx, pos, "EndsWith", res, BuildLiteralRawString(pos, components[1].Prefix, isUtf8));
                                }
                            } else if (Ctx.AnsiLike && !components.back().Suffix.empty()) {
                                const TString& suffix = components.back().Suffix;
                                TNodePtr suffixMatch = BuildBinaryOp(Ctx, pos, "EndsWith", res, BuildLiteralRawString(pos, suffix, isUtf8));
                                isMatch = BuildBinaryOp(Ctx, pos, "And", suffixMatch, isMatch);
                            }
                            // TODO: more StringContains/StartsWith/EndsWith cases?
                        }
                    }

                    Ctx.IncrementMonCounter("sql_features", notMatch ? "NotLike" : "Like");

                } else if (opName == "regexp" || opName == "rlike" || opName == "match") {
                    if (matchOp.HasBlock4()) {
                        Ctx.IncrementMonCounter("sql_errors", "RegexpEscape");
                        TString opNameUpper(opName);
                        opNameUpper.to_upper();
                        Error() << opName << " and ESCAPE clauses should not be used together";
                        return nullptr;
                    }

                    if (!Ctx.PragmaRegexUseRe2) {
                        Ctx.Warning(pos, TIssuesIds::CORE_LEGACY_REGEX_ENGINE) << "Legacy regex engine works incorrectly with unicode. Use PRAGMA RegexUseRe2='true';";
                    }

                    const auto& matcher = Ctx.PragmaRegexUseRe2 ?
                        BuildUdf(Ctx, pos, "Re2", opName == "match" ? "Match" : "Grep", {BuildTuple(pos, {pattern, BuildLiteralNull(pos)})}):
                        BuildUdf(Ctx, pos, "Pcre", opName == "match" ? "BacktrackingMatch" : "BacktrackingGrep", { pattern });
                    isMatch = new TCallNodeImpl(pos, "Apply", { matcher, res });
                    if (opName != "match") {
                        Ctx.IncrementMonCounter("sql_features", notMatch ? "NotRegexp" : "Regexp");
                    } else {
                        Ctx.IncrementMonCounter("sql_features", notMatch ? "NotMatch" : "Match");
                    }
                } else {
                    Ctx.IncrementMonCounter("sql_errors", "UnknownMatchOp");
                    AltNotImplemented("match_op", cond);
                    return nullptr;
                }
                return (notMatch && isMatch) ? isMatch->ApplyUnaryOp(Ctx, pos, "Not") : isMatch;
            }
            case TRule_cond_expr::kAltCondExpr2: {
                //   | NOT? IN COMPACT? in_expr
                auto altInExpr = cond.GetAlt_cond_expr2();
                const bool notIn = altInExpr.HasBlock1();
                auto hints = BuildTuple(pos, {});
                bool isCompact = altInExpr.HasBlock3();
                if (!isCompact) {
                    auto sqlHints = Ctx.PullHintForToken(Ctx.TokenPosition(altInExpr.GetToken2()));
                    isCompact = AnyOf(sqlHints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
                }
                if (isCompact) {
                    Ctx.IncrementMonCounter("sql_features", "IsCompactHint");
                    auto sizeHint = BuildTuple(pos, { BuildQuotedAtom(pos, "isCompact", NYql::TNodeFlags::Default) });
                    hints = BuildTuple(pos, { sizeHint });
                }
                TSqlExpression inSubexpr(Ctx, Mode);
                auto inRight = inSubexpr.SqlInExpr(altInExpr.GetRule_in_expr4(), tail);
                auto isIn = BuildBuiltinFunc(Ctx, pos, "In", {res, inRight, hints});
                Ctx.IncrementMonCounter("sql_features", notIn ? "NotIn" : "In");
                return (notIn && isIn) ? isIn->ApplyUnaryOp(Ctx, pos, "Not") : isIn;
            }
            case TRule_cond_expr::kAltCondExpr3: {
                if (tail.Count) {
                    UnexpectedQuestionToken(tail);
                    return {};
                }
                auto altCase = cond.GetAlt_cond_expr3().GetBlock1().Alt_case();
                const bool notNoll =
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt2 ||
                    altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4
                ;

                if (altCase == TRule_cond_expr::TAlt3::TBlock1::kAlt4 &&
                    !cond.GetAlt_cond_expr3().GetBlock1().GetAlt4().HasBlock1())
                {
                    Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_MISSING_IS_BEFORE_NOT_NULL) << "Missing IS keyword before NOT NULL";
                }

                auto isNull = BuildIsNullOp(pos, res);
                Ctx.IncrementMonCounter("sql_features", notNoll ? "NotNull" : "Null");
                return (notNoll && isNull) ? isNull->ApplyUnaryOp(Ctx, pos, "Not") : isNull;
            }
            case TRule_cond_expr::kAltCondExpr4: {
                auto alt = cond.GetAlt_cond_expr4();
                if (alt.HasBlock1()) {
                    Ctx.IncrementMonCounter("sql_features", "NotBetween");
                    return BuildBinaryOp(
                        Ctx,
                        pos,
                        "Or",
                        BuildBinaryOp(Ctx, pos, "<", res, SubExpr(alt.GetRule_eq_subexpr3(), {})),
                        BuildBinaryOp(Ctx, pos, ">", res, SubExpr(alt.GetRule_eq_subexpr5(), tail))
                    );
                } else {
                    Ctx.IncrementMonCounter("sql_features", "Between");
                    return BuildBinaryOp(
                        Ctx,
                        pos,
                        "And",
                        BuildBinaryOp(Ctx, pos, ">=", res, SubExpr(alt.GetRule_eq_subexpr3(), {})),
                        BuildBinaryOp(Ctx, pos, "<=", res, SubExpr(alt.GetRule_eq_subexpr5(), tail))
                    );
                }
            }
            case TRule_cond_expr::kAltCondExpr5: {
                auto alt = cond.GetAlt_cond_expr5();
                auto getNode = [](const TRule_cond_expr::TAlt5::TBlock1& b) -> const TRule_eq_subexpr& { return b.GetRule_eq_subexpr2(); };
                return BinOpList(node.GetRule_eq_subexpr1(), getNode, alt.GetBlock1().begin(), alt.GetBlock1().end(), tail);
            }
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnknownConditionExpr");
                AltNotImplemented("cond_expr", cond);
                return nullptr;
        }
    }
    return res;
}

TNodePtr TSqlExpression::BinOperList(const TString& opName, TVector<TNodePtr>::const_iterator begin, TVector<TNodePtr>::const_iterator end) const {
    TPosition pos(Ctx.Pos());
    const size_t opCount = end - begin;
    Y_VERIFY_DEBUG(opCount >= 2);
    if (opCount == 2) {
        return BuildBinaryOp(Ctx, pos, opName, *begin, *(begin+1));
    } if (opCount == 3) {
        return BuildBinaryOp(Ctx, pos, opName, BuildBinaryOp(Ctx, pos, opName, *begin, *(begin+1)), *(begin+2));
    } else {
        auto mid = begin + opCount / 2;
        return BuildBinaryOp(Ctx, pos, opName, BinOperList(opName, begin, mid), BinOperList(opName, mid, end));
    }
}

TSqlExpression::TCaseBranch TSqlExpression::ReduceCaseBranches(TVector<TCaseBranch>::const_iterator begin, TVector<TCaseBranch>::const_iterator end) const {
    YQL_ENSURE(begin < end);
    const size_t branchCount = end - begin;
    if (branchCount == 1) {
        return *begin;
    }

    auto mid = begin + branchCount / 2;
    auto left = ReduceCaseBranches(begin, mid);
    auto right = ReduceCaseBranches(mid, end);

    TVector<TNodePtr> preds;
    preds.reserve(branchCount);
    for (auto it = begin; it != end; ++it) {
        preds.push_back(it->Pred);
    }

    TCaseBranch result;
    result.Pred = new TCallNodeImpl(Ctx.Pos(), "Or", preds);
    result.Value = BuildBuiltinFunc(Ctx, Ctx.Pos(), "If", { left.Pred, left.Value, right.Value });
    return result;
}

template <typename TNode, typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOper(const TString& opName, const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    if (begin == end) {
        return SubExpr(node, tail);
    }
    // can't have top level smart_parenthesis node if any binary operation is present
    MaybeUnnamedSmartParenOnTop = false;
    Ctx.IncrementMonCounter("sql_binary_operations", opName);
    const size_t listSize = end - begin;
    TVector<TNodePtr> nodes;
    nodes.reserve(1 + listSize);
    nodes.push_back(SubExpr(node, {}));
    for (; begin != end; ++begin) {
        nodes.push_back(SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
    }
    return BinOperList(opName, nodes.begin(), nodes.end());
}

template <typename TNode, typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TNode& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (begin == end);
    TNodePtr partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        Token(begin->GetToken1());
        TPosition pos(Ctx.Pos());
        TString opName;
        auto tokenId = begin->GetToken1().GetId();
        switch (tokenId) {
            case SQLv1LexerTokens::TOKEN_LESS:
                Ctx.IncrementMonCounter("sql_binary_operations", "Less");
                opName = "<";
                break;
            case SQLv1LexerTokens::TOKEN_LESS_OR_EQ:
                opName = "<=";
                Ctx.IncrementMonCounter("sql_binary_operations", "LessOrEq");
                break;
            case SQLv1LexerTokens::TOKEN_GREATER:
                opName = ">";
                Ctx.IncrementMonCounter("sql_binary_operations", "Greater");
                break;
            case SQLv1LexerTokens::TOKEN_GREATER_OR_EQ:
                opName = ">=";
                Ctx.IncrementMonCounter("sql_binary_operations", "GreaterOrEq");
                break;
            case SQLv1LexerTokens::TOKEN_PLUS:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedAdd" : "+";
                Ctx.IncrementMonCounter("sql_binary_operations", "Plus");
                break;
            case SQLv1LexerTokens::TOKEN_MINUS:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedSub" : "-";
                Ctx.IncrementMonCounter("sql_binary_operations", "Minus");
                break;
            case SQLv1LexerTokens::TOKEN_ASTERISK:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedMul" : "*";
                Ctx.IncrementMonCounter("sql_binary_operations", "Multiply");
                break;
            case SQLv1LexerTokens::TOKEN_SLASH:
                opName = "/";
                Ctx.IncrementMonCounter("sql_binary_operations", "Divide");
                if (!Ctx.Scoped->PragmaClassicDivision && partialResult) {
                    partialResult = new TCallNodeImpl(pos, "SafeCast", {std::move(partialResult), BuildDataType(pos, "Double")});
                } else if (Ctx.Scoped->PragmaCheckedOps) {
                    opName = "CheckedDiv";
                }
                break;
            case SQLv1LexerTokens::TOKEN_PERCENT:
                opName = Ctx.Scoped->PragmaCheckedOps ? "CheckedMod" : "%";
                Ctx.IncrementMonCounter("sql_binary_operations", "Mod");
                break;
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnsupportedBinaryOperation");
                Error() << "Unsupported binary operation token: " << tokenId;
                return nullptr;
        }

        partialResult = BuildBinaryOp(Ctx, pos, opName, partialResult, SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
        ++begin;
    }

    return partialResult;
}

template <typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TRule_bit_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (begin == end);
    TNodePtr partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        TString opName;
        switch (begin->GetBlock1().Alt_case()) {
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt1: {
                Token(begin->GetBlock1().GetAlt1().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt1().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_SHIFT_LEFT) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "ShiftLeft";
                Ctx.IncrementMonCounter("sql_binary_operations", "ShiftLeft");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt2: {
                opName = "ShiftRight";
                Ctx.IncrementMonCounter("sql_binary_operations", "ShiftRight");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt3: {
                Token(begin->GetBlock1().GetAlt3().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt3().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_ROT_LEFT) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "RotLeft";
                Ctx.IncrementMonCounter("sql_binary_operations", "RotLeft");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt4: {
                opName = "RotRight";
                Ctx.IncrementMonCounter("sql_binary_operations", "RotRight");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt5: {
                Token(begin->GetBlock1().GetAlt5().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt5().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_AMPERSAND) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "BitAnd";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitAnd");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt6: {
                Token(begin->GetBlock1().GetAlt6().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt6().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_PIPE) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "BitOr";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitOr");
                break;
            }
            case TRule_neq_subexpr_TBlock2_TBlock1::kAlt7: {
                Token(begin->GetBlock1().GetAlt7().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt7().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_CARET) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                opName = "BitXor";
                Ctx.IncrementMonCounter("sql_binary_operations", "BitXor");
                break;
            }
            default:
                Y_FAIL("You should change implementation according to grammar changes");
        }

        partialResult = BuildBinaryOp(Ctx, Ctx.Pos(), opName, partialResult, SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
        ++begin;
    }

    return partialResult;
}

template <typename TGetNode, typename TIter>
TNodePtr TSqlExpression::BinOpList(const TRule_eq_subexpr& node, TGetNode getNode, TIter begin, TIter end, const TTrailingQuestions& tail) {
    MaybeUnnamedSmartParenOnTop = MaybeUnnamedSmartParenOnTop && (begin == end);
    TNodePtr partialResult = SubExpr(node, (begin == end) ? tail : TTrailingQuestions{});
    while (begin != end) {
        Ctx.IncrementMonCounter("sql_features", "BinaryOperation");
        TString opName;
        switch (begin->GetBlock1().Alt_case()) {
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt1: {
                Token(begin->GetBlock1().GetAlt1().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt1().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_EQUALS) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "Equals");
                opName = "==";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt2: {
                Token(begin->GetBlock1().GetAlt2().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt2().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_EQUALS2) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "Equals2");
                opName = "==";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt3: {
                Token(begin->GetBlock1().GetAlt3().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt3().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_NOT_EQUALS) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "NotEquals");
                opName = "!=";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt4: {
                Token(begin->GetBlock1().GetAlt4().GetToken1());
                auto tokenId = begin->GetBlock1().GetAlt4().GetToken1().GetId();
                if (tokenId != SQLv1LexerTokens::TOKEN_NOT_EQUALS2) {
                    Error() << "Unsupported binary operation token: " << tokenId;
                    return {};
                }
                Ctx.IncrementMonCounter("sql_binary_operations", "NotEquals2");
                opName = "!=";
                break;
            }
            case TRule_cond_expr::TAlt5::TBlock1::TBlock1::kAlt5: {
                Token(begin->GetBlock1().GetAlt5().GetRule_distinct_from_op1().GetToken1());
                opName = begin->GetBlock1().GetAlt5().GetRule_distinct_from_op1().HasBlock2() ? "IsNotDistinctFrom" : "IsDistinctFrom";
                Ctx.IncrementMonCounter("sql_binary_operations", opName);
                break;
            }
            default:
                Y_FAIL("You should change implementation according to grammar changes");
        }

        partialResult = BuildBinaryOp(Ctx, Ctx.Pos(), opName, partialResult, SubExpr(getNode(*begin), (begin + 1 == end) ? tail : TTrailingQuestions{}));
        ++begin;
    }

    return partialResult;
}

TNodePtr TSqlExpression::SqlInExpr(const TRule_in_expr& node, const TTrailingQuestions& tail) {
    TSqlExpression expr(Ctx, Mode);
    expr.SetSmartParenthesisMode(TSqlExpression::ESmartParenthesis::InStatement);
    auto result = expr.UnaryExpr(node.GetRule_in_unary_subexpr1(), tail);
    return result;
}

TNodePtr TSqlExpression::SmartParenthesis(const TRule_smart_parenthesis& node) {
    TVector<TNodePtr> exprs;
    Token(node.GetToken1());
    const TPosition pos(Ctx.Pos());
    const bool isTuple = node.HasBlock3();
    bool expectTuple = SmartParenthesisMode == ESmartParenthesis::InStatement;
    EExpr mode = EExpr::Regular;
    if (SmartParenthesisMode == ESmartParenthesis::SqlLambdaParams) {
        mode = EExpr::SqlLambdaParams;
        expectTuple = true;
    }
    if (node.HasBlock2() && !NamedExprList(node.GetBlock2().GetRule_named_expr_list1(), exprs, mode)) {
        return {};
    }

    bool topLevelGroupBy = MaybeUnnamedSmartParenOnTop && SmartParenthesisMode == ESmartParenthesis::GroupBy;

    bool hasAliases = false;
    bool hasUnnamed = false;
    for (const auto& expr: exprs) {
        if (expr->GetLabel()) {
            hasAliases = true;
        } else {
            hasUnnamed = true;
        }
        if (hasAliases && hasUnnamed && !topLevelGroupBy) {
            Ctx.IncrementMonCounter("sql_errors", "AnonymousStructMembers");
            Ctx.Error(pos) << "Structure does not allow anonymous members";
            return nullptr;
        }
    }
    if (exprs.size() == 1 && hasUnnamed && !isTuple && !expectTuple) {
        return exprs.back();
    }
    if (topLevelGroupBy) {
        if (isTuple) {
            Ctx.IncrementMonCounter("sql_errors", "SimpleTupleInGroupBy");
            Token(node.GetBlock3().GetToken1());
            Ctx.Error() << "Unexpected trailing comma in grouping elements list";
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ListOfNamedNode");
        return BuildListOfNamedNodes(pos, std::move(exprs));
    }
    Ctx.IncrementMonCounter("sql_features", hasUnnamed ? "SimpleTuple" : "SimpleStruct");
    return (hasUnnamed || expectTuple || exprs.size() == 0) ? BuildTuple(pos, exprs) : BuildStructure(pos, exprs);
}

TNodePtr TSqlTranslation::NamedNode(const TRule_named_nodes_stmt& rule, TVector<TSymbolNameWithPos>& names) {
    // named_nodes_stmt: bind_parameter_list EQUALS (expr | subselect_stmt);
    // subselect_stmt: (LPAREN select_stmt RPAREN | select_unparenthesized_stmt);
    if (!BindList(rule.GetRule_bind_parameter_list1(), names)) {
        return {};
    }

    TNodePtr nodeExpr = nullptr;
    switch (rule.GetBlock3().Alt_case()) {
    case TRule_named_nodes_stmt::TBlock3::kAlt1: {
        TSqlExpression expr(Ctx, Mode);
        auto result = expr.Build(rule.GetBlock3().GetAlt1().GetRule_expr1());
        return result;
    }

    case TRule_named_nodes_stmt::TBlock3::kAlt2:{
        const auto& subselect_rule = rule.GetBlock3().GetAlt2().GetRule_subselect_stmt1();

        TSqlSelect expr(Ctx, Mode);
        TPosition pos;
        TSourcePtr source = nullptr;
        switch (subselect_rule.GetBlock1().Alt_case()) {
            case TRule_subselect_stmt::TBlock1::kAlt1:
                source = expr.Build(subselect_rule.GetBlock1().GetAlt1().GetRule_select_stmt2(), pos);
                break;

            case TRule_subselect_stmt::TBlock1::kAlt2:
                source = expr.Build(subselect_rule.GetBlock1().GetAlt2().GetRule_select_unparenthesized_stmt1(), pos);
                break;

            default:
                AltNotImplemented("subselect_stmt", subselect_rule.GetBlock1());
                Ctx.IncrementMonCounter("sql_errors", "UnknownNamedNode");
                return nullptr;
        }

        if (!source) {
            return {};
        }

        return BuildSourceNode(pos, std::move(source));
    }

    default:
        AltNotImplemented("named_node", rule.GetBlock3());
        Ctx.IncrementMonCounter("sql_errors", "UnknownNamedNode");
        return nullptr;
    }
}

bool TSqlTranslation::ImportStatement(const TRule_import_stmt& stmt, TVector<TString>* namesPtr) {
    TVector<TString> modulePath;
    if (!ModulePath(stmt.GetRule_module_path2(), modulePath)) {
        return false;
    }
    TVector<TSymbolNameWithPos> names;
    TVector<TSymbolNameWithPos> aliases;
    if (!NamedBindList(stmt.GetRule_named_bind_parameter_list4(), names, aliases)) {
        return false;
    }
    YQL_ENSURE(names.size() == aliases.size());
    const TString moduleAlias = Ctx.AddImport(std::move(modulePath));
    if (!moduleAlias) {
        return false;
    }

    for (size_t i = 0; i < names.size(); ++i) {
        auto& name = names[i];
        auto& alias = aliases[i];

        auto& var = alias.Name ? alias : name;
        if (IsAnonymousName(var.Name)) {
            Ctx.Error(var.Pos) << "Can not import anonymous name " << var.Name;
            return false;
        }

        auto builder = [&](const TString& realName) {
            YQL_ENSURE(realName == var.Name);
            auto atom = BuildQuotedAtom(name.Pos, name.Name);
            return atom->Y("bind", moduleAlias, atom);
        };

        var.Name = PushNamedNode(var.Pos, var.Name, builder);
        if (namesPtr) {
            namesPtr->push_back(var.Name);
        }
    }
    return true;
}

bool TSqlSelect::JoinOp(ISource* join, const TRule_join_source::TBlock3& block, TMaybe<TPosition> anyPos) {
    // block: (join_op (ANY)? flatten_source join_constraint?)
    // join_op:
    //    COMMA
    //  | (NATURAL)? ((LEFT (ONLY | SEMI)? | RIGHT (ONLY | SEMI)? | EXCLUSION | FULL)? (OUTER)? | INNER | CROSS) JOIN
    //;
    const auto& node = block.GetRule_join_op1();
    switch (node.Alt_case()) {
        case TRule_join_op::kAltJoinOp1:
            Ctx.IncrementMonCounter("sql_join_operations", "CartesianProduct");
            Error() << "Cartesian product of tables is not supported. Please use explicit CROSS JOIN";
            return false;
        case TRule_join_op::kAltJoinOp2: {
            auto alt = node.GetAlt_join_op2();
            if (alt.HasBlock1()) {
                Ctx.IncrementMonCounter("sql_join_operations", "Natural");
                Error() << "Natural join is not implemented yet";
                return false;
            }
            TString joinOp("Inner");
            auto hints = Ctx.PullHintForToken(Ctx.TokenPosition(alt.GetToken3()));
            TJoinLinkSettings linkSettings;
            linkSettings.ForceSortedMerge = AnyOf(hints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "merge"; });
            switch (alt.GetBlock2().Alt_case()) {
                case TRule_join_op::TAlt2::TBlock2::kAlt1:
                    if (alt.GetBlock2().GetAlt1().HasBlock1()) {
                        auto block = alt.GetBlock2().GetAlt1().GetBlock1();
                        switch (block.Alt_case()) {
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt1:
                            // left
                            joinOp = Token(block.GetAlt1().GetToken1());
                            if (block.GetAlt1().HasBlock2()) {
                                joinOp += " " + Token(block.GetAlt1().GetBlock2().GetToken1());
                            }
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt2:
                            // right
                            joinOp = Token(block.GetAlt2().GetToken1());
                            if (block.GetAlt2().HasBlock2()) {
                                joinOp += " " + Token(block.GetAlt2().GetBlock2().GetToken1());
                            }

                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt3:
                            // exclusion
                            joinOp = Token(block.GetAlt3().GetToken1());
                            break;
                        case TRule_join_op_TAlt2_TBlock2_TAlt1_TBlock1::kAlt4:
                            // full
                            joinOp = Token(block.GetAlt4().GetToken1());
                            break;
                        default:
                            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation");
                            AltNotImplemented("join_op", node);
                            return false;
                        }
                    }
                    if (alt.GetBlock2().GetAlt1().HasBlock2()) {
                        TString normalizedOp = alt.GetBlock2().GetAlt1().HasBlock1() ? joinOp : "";
                        normalizedOp.to_upper();
                        if (!(normalizedOp == "LEFT" || normalizedOp == "RIGHT" || normalizedOp == "FULL")) {
                            Token(alt.GetBlock2().GetAlt1().GetBlock2().GetToken1());
                            Error() << "Invalid join type: " << normalizedOp << (normalizedOp.empty() ? "" : " ") << "OUTER JOIN. "
                                    << "OUTER keyword is optional and can only be used after LEFT, RIGHT or FULL";
                            Ctx.IncrementMonCounter("sql_errors", "BadJoinType");
                            return false;
                        }
                    }
                    break;
                case TRule_join_op::TAlt2::TBlock2::kAlt2:
                    joinOp = Token(alt.GetBlock2().GetAlt2().GetToken1());
                    break;
                case TRule_join_op::TAlt2::TBlock2::kAlt3:
                    joinOp = Token(alt.GetBlock2().GetAlt3().GetToken1());
                    break;
                default:
                    Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation");
                    AltNotImplemented("join_op", node);
                    return false;
            }

            joinOp = NormalizeJoinOp(joinOp);
            Ctx.IncrementMonCounter("sql_features", "Join");
            Ctx.IncrementMonCounter("sql_join_operations", joinOp);

            TNodePtr joinKeyExpr;
            if (block.HasBlock4()) {
                if (joinOp == "Cross") {
                    Error() << "Cross join should not have ON or USING expression";
                    Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
                    return false;
                }

                joinKeyExpr = JoinExpr(join, block.GetBlock4().GetRule_join_constraint1());
                if (!joinKeyExpr) {
                    Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
                    return false;
                }
            }
            else {
                if (joinOp != "Cross") {
                    Error() << "Expected ON or USING expression";
                    Ctx.IncrementMonCounter("sql_errors", "BadJoinExpr");
                    return false;
                }
            }

            if (joinOp == "Cross" && anyPos) {
                Ctx.Error(*anyPos) << "ANY should not be used with Cross JOIN";
                Ctx.IncrementMonCounter("sql_errors", "BadJoinAny");
                return false;
            }

            Y_VERIFY_DEBUG(join->GetJoin());
            join->GetJoin()->SetupJoin(joinOp, joinKeyExpr, linkSettings);
            break;
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinOperation2");
            AltNotImplemented("join_op", node);
            return false;
    }
    return true;
}

TNodePtr TSqlSelect::JoinExpr(ISource* join, const TRule_join_constraint& node) {
    switch (node.Alt_case()) {
        case TRule_join_constraint::kAltJoinConstraint1: {
            auto& alt = node.GetAlt_join_constraint1();
            Token(alt.GetToken1());
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            TSqlExpression expr(Ctx, Mode);
            return expr.Build(alt.GetRule_expr2());
        }
        case TRule_join_constraint::kAltJoinConstraint2: {
            auto& alt = node.GetAlt_join_constraint2();
            Token(alt.GetToken1());
            TPosition pos(Ctx.Pos());
            TVector<TDeferredAtom> names;
            if (!PureColumnOrNamedListStr(alt.GetRule_pure_column_or_named_list2(), *this, names)) {
                return nullptr;
            }

            Y_VERIFY_DEBUG(join->GetJoin());
            return join->GetJoin()->BuildJoinKeys(Ctx, names);
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownJoinConstraint");
            AltNotImplemented("join_constraint", node);
            break;
    }
    return nullptr;
}

bool TSqlSelect::FlattenByArg(const TString& sourceLabel, TVector<TNodePtr>& flattenByColumns, TVector<TNodePtr>& flattenByExprs,
                              const TRule_flatten_by_arg& node)
{
    // flatten_by_arg:
    //     named_column
    //  |  LPAREN named_expr_list COMMA? RPAREN
    // ;

    flattenByColumns.clear();
    flattenByExprs.clear();

    TVector<TNodePtr> namedExprs;
    switch (node.Alt_case()) {
        case TRule_flatten_by_arg::kAltFlattenByArg1: {
            TVector<TNodePtr> columns;
            if (!NamedColumn(columns, node.GetAlt_flatten_by_arg1().GetRule_named_column1())) {
                return false;
            }
            YQL_ENSURE(columns.size() == 1);
            auto& column = columns.back();
            auto columnNamePtr = column->GetColumnName();
            YQL_ENSURE(columnNamePtr && *columnNamePtr);

            auto sourcePtr = column->GetSourceName();
            const bool isEmptySource = !sourcePtr || !*sourcePtr;
            if (isEmptySource || *sourcePtr == sourceLabel) {
                // select * from T      flatten by x
                // select * from T as s flatten by x
                // select * from T as s flatten by s.x
                flattenByColumns.emplace_back(std::move(column));
            } else {
                // select * from T as s flatten by x.y as z
                if (!column->GetLabel()) {
                    Ctx.Error(column->GetPos()) << "Unnamed expression after FLATTEN BY is not allowed";
                    return false;
                }
                flattenByColumns.emplace_back(BuildColumn(column->GetPos(), column->GetLabel()));

                TVector<INode::TIdPart> ids;
                ids.push_back(BuildColumn(column->GetPos()));
                ids.push_back(*sourcePtr);
                ids.push_back(*columnNamePtr);
                auto node = BuildAccess(column->GetPos(), ids, false);
                node->SetLabel(column->GetLabel());
                flattenByExprs.emplace_back(std::move(node));
            }

            break;
        }
        case TRule_flatten_by_arg::kAltFlattenByArg2: {
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            if (!NamedExprList(node.GetAlt_flatten_by_arg2().GetRule_named_expr_list2(), namedExprs) || Ctx.HasPendingErrors) {
                return false;
            }
            for (auto& namedExprNode : namedExprs) {
                YQL_ENSURE(!namedExprNode->ContentListPtr());

                auto sourcePtr = namedExprNode->GetSourceName();
                const bool isEmptySource = !sourcePtr || !*sourcePtr;
                auto columnNamePtr = namedExprNode->GetColumnName();
                if (columnNamePtr && (isEmptySource || *sourcePtr == sourceLabel)) {
                    namedExprNode->AssumeColumn();
                    flattenByColumns.emplace_back(std::move(namedExprNode));
                } else {
                    auto nodeLabel = namedExprNode->GetLabel();
                    if (!nodeLabel) {
                        Ctx.Error(namedExprNode->GetPos()) << "Unnamed expression after FLATTEN BY is not allowed";
                        return false;
                    }
                    flattenByColumns.emplace_back(BuildColumn(namedExprNode->GetPos(), nodeLabel));
                    flattenByExprs.emplace_back(std::move(namedExprNode));
                }
            }
            break;
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownFlattenByArg");
            AltNotImplemented("flatten_by_arg", node);
            return false;
    }
    return true;
}

TSourcePtr TSqlSelect::FlattenSource(const TRule_flatten_source& node) {
    auto source = NamedSingleSource(node.GetRule_named_single_source1(), true);
    if (!source) {
        return nullptr;
    }
    if (node.HasBlock2()) {
        auto flatten = node.GetBlock2();
        auto flatten2 = flatten.GetBlock2();
        switch (flatten2.Alt_case()) {
        case TRule_flatten_source::TBlock2::TBlock2::kAlt1: {
            TString mode = "auto";
            if (flatten2.GetAlt1().HasBlock1()) {
                mode = to_lower(Token(flatten2.GetAlt1().GetBlock1().GetToken1()));
            }

            TVector<TNodePtr> flattenByColumns;
            TVector<TNodePtr> flattenByExprs;
            if (!FlattenByArg(source->GetLabel(), flattenByColumns, flattenByExprs, flatten2.GetAlt1().GetRule_flatten_by_arg3())) {
                return nullptr;
            }

            Ctx.IncrementMonCounter("sql_features", "FlattenByColumns");
            if (!source->AddExpressions(Ctx, flattenByColumns, EExprSeat::FlattenBy)) {
                return nullptr;
            }

            if (!source->AddExpressions(Ctx, flattenByExprs, EExprSeat::FlattenByExpr)) {
                return nullptr;
            }

            source->SetFlattenByMode(mode);
            break;
        }
        case TRule_flatten_source::TBlock2::TBlock2::kAlt2: {
            Ctx.IncrementMonCounter("sql_features", "FlattenColumns");
            source->MarkFlattenColumns();
            break;
        }

        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownOrdinaryNamedColumn");
            AltNotImplemented("flatten_source", flatten2);
        }
    }
    return source;
}

TSourcePtr TSqlSelect::JoinSource(const TRule_join_source& node) {
    // join_source: (ANY)? flatten_source (join_op (ANY)? flatten_source join_constraint?)*;
    if (node.HasBlock1() && !node.Block3Size()) {
        Error() << "ANY is not allowed without JOIN";
        return nullptr;
    }

    TSourcePtr source(FlattenSource(node.GetRule_flatten_source2()));
    if (!source) {
        return nullptr;
    }

    if (node.Block3Size()) {
        TPosition pos(Ctx.Pos());
        TVector<TSourcePtr> sources;
        TVector<TMaybe<TPosition>> anyPositions;
        TVector<bool> anyFlags;

        sources.emplace_back(std::move(source));
        anyPositions.emplace_back(node.HasBlock1() ? Ctx.TokenPosition(node.GetBlock1().GetToken1()) : TMaybe<TPosition>());
        anyFlags.push_back(bool(anyPositions.back()));

        for (auto& block: node.GetBlock3()) {
            sources.emplace_back(FlattenSource(block.GetRule_flatten_source3()));
            if (!sources.back()) {
                Ctx.IncrementMonCounter("sql_errors", "NoJoinWith");
                return nullptr;
            }

            anyPositions.emplace_back(block.HasBlock2() ? Ctx.TokenPosition(block.GetBlock2().GetToken1()) : TMaybe<TPosition>());
            anyFlags.push_back(bool(anyPositions.back()));
        }

        source = BuildEquiJoin(pos, std::move(sources), std::move(anyFlags), Ctx.Scoped->StrictJoinKeyTypes);
        size_t idx = 1;
        for (auto& block: node.GetBlock3()) {
            YQL_ENSURE(idx < anyPositions.size());
            TMaybe<TPosition> leftAny = (idx == 1) ? anyPositions[0] : Nothing();
            TMaybe<TPosition> rightAny = anyPositions[idx];

            if (!JoinOp(source.Get(), block, leftAny ? leftAny : rightAny)) {
                Ctx.IncrementMonCounter("sql_errors", "NoJoinOp");
                return nullptr;
            }
            ++idx;
        }
    }

    return source;
}

bool TSqlSelect::SelectTerm(TVector<TNodePtr>& terms, const TRule_result_column& node) {
    // result_column:
    //     opt_id_prefix ASTERISK
    //   | expr ((AS an_id) | an_id_pure)?
    // ;
    switch (node.Alt_case()) {
        case TRule_result_column::kAltResultColumn1: {
            auto alt = node.GetAlt_result_column1();

            Token(alt.GetToken2());
            auto idAsteriskQualify = OptIdPrefixAsStr(alt.GetRule_opt_id_prefix1(), *this);
            Ctx.IncrementMonCounter("sql_features", idAsteriskQualify ? "QualifyAsterisk" : "Asterisk");
            terms.push_back(BuildColumn(Ctx.Pos(), "*", idAsteriskQualify));
            break;
        }
        case TRule_result_column::kAltResultColumn2: {
            auto alt = node.GetAlt_result_column2();
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            TSqlExpression expr(Ctx, Mode);
            TNodePtr term(expr.Build(alt.GetRule_expr1()));
            if (!term) {
                Ctx.IncrementMonCounter("sql_errors", "NoTerm");
                return false;
            }
            if (alt.HasBlock2()) {
                TString label;
                bool implicitLabel = false;
                switch (alt.GetBlock2().Alt_case()) {
                    case TRule_result_column_TAlt2_TBlock2::kAlt1:
                        label = Id(alt.GetBlock2().GetAlt1().GetBlock1().GetRule_an_id_or_type2(), *this);
                        break;
                    case TRule_result_column_TAlt2_TBlock2::kAlt2:
                        label = Id(alt.GetBlock2().GetAlt2().GetRule_an_id_as_compat1(), *this);
                        if (!Ctx.AnsiOptionalAs) {
                            // AS is mandatory
                            Ctx.Error() << "Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility";
                            return false;
                        }
                        implicitLabel = true;
                        break;
                    default:
                        Y_FAIL("You should change implementation according to grammar changes");
                }
                term->SetLabel(label, Ctx.Pos());
                term->MarkImplicitLabel(implicitLabel);
            }
            terms.push_back(term);
            break;
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownResultColumn");
            AltNotImplemented("result_column", node);
            return false;
    }
    return true;
}

bool TSqlSelect::ValidateSelectColumns(const TVector<TNodePtr>& terms) {
    TSet<TString> labels;
    TSet<TString> asteriskSources;
    for (const auto& term: terms) {
        const auto& label = term->GetLabel();
        if (!Ctx.PragmaAllowDotInAlias && label.find('.') != TString::npos) {
            Ctx.Error(term->GetPos()) << "Unable to use '.' in column name. Invalid column name: " << label;
            return false;
        }
        if (!label.empty()) {
            if (!labels.insert(label).second) {
                Ctx.Error(term->GetPos()) << "Unable to use duplicate column names. Collision in name: " << label;
                return false;
            }
        }
        if (term->IsAsterisk()) {
            const auto& source = *term->GetSourceName();
            if (source.empty() && terms.ysize() > 1) {
                Ctx.Error(term->GetPos()) << "Unable to use plain '*' with other projection items. Please use qualified asterisk instead: '<table>.*' (<table> can be either table name or table alias).";
                return false;
            } else if (!asteriskSources.insert(source).second) {
                Ctx.Error(term->GetPos()) << "Unable to use twice same quialified asterisk. Invalid source: " << source;
                return false;
            }
        } else if (label.empty()) {
            const auto* column = term->GetColumnName();
            if (column && !column->empty()) {
                const auto& source = *term->GetSourceName();
                const auto usedName = source.empty() ? *column : source + '.' + *column;
                if (!labels.insert(usedName).second) {
                    Ctx.Error(term->GetPos()) << "Unable to use duplicate column names. Collision in name: " << usedName;
                    return false;
                }
            }
        }
    }
    return true;
}

TSourcePtr TSqlSelect::SingleSource(const TRule_single_source& node, const TVector<TString>& derivedColumns, TPosition derivedColumnsPos, bool unorderedSubquery) {
    switch (node.Alt_case()) {
        case TRule_single_source::kAltSingleSource1: {
            const auto& alt = node.GetAlt_single_source1();
            const auto& table_ref = alt.GetRule_table_ref1();

            if (auto maybeSource = AsTableImpl(table_ref)) {
                auto source = *maybeSource;
                if (!source) {
                    return nullptr;
                }

                return source;
            } else {
                TTableRef table;
                if (!TableRefImpl(alt.GetRule_table_ref1(), table, unorderedSubquery)) {
                    return nullptr;
                }

                if (table.Source) {
                    return table.Source;
                }

                TPosition pos(Ctx.Pos());
                Ctx.IncrementMonCounter("sql_select_clusters", table.Cluster.GetLiteral() ? *table.Cluster.GetLiteral() : "unknown");
                return BuildTableSource(pos, table);
            }
        }
        case TRule_single_source::kAltSingleSource2: {
            const auto& alt = node.GetAlt_single_source2();
            Token(alt.GetToken1());
            TSqlSelect innerSelect(Ctx, Mode);
            TPosition pos;
            auto source = innerSelect.Build(alt.GetRule_select_stmt2(), pos);
            if (!source) {
                return nullptr;
            }
            return BuildInnerSource(pos, BuildSourceNode(pos, std::move(source)), Ctx.Scoped->CurrService, Ctx.Scoped->CurrCluster);
        }
        case TRule_single_source::kAltSingleSource3: {
            const auto& alt = node.GetAlt_single_source3();
            TPosition pos;
            return TSqlValues(Ctx, Mode).Build(alt.GetRule_values_stmt2(), pos, derivedColumns, derivedColumnsPos);
        }
        default:
            AltNotImplemented("single_source", node);
            Ctx.IncrementMonCounter("sql_errors", "UnknownSingleSource");
            return nullptr;
    }
}

TSourcePtr TSqlSelect::NamedSingleSource(const TRule_named_single_source& node, bool unorderedSubquery) {
    // named_single_source: single_source (((AS an_id) | an_id_pure) pure_column_list?)? (sample_clause | tablesample_clause)?;
    TVector<TString> derivedColumns;
    TPosition derivedColumnsPos;
    if (node.HasBlock2() && node.GetBlock2().HasBlock2()) {
        const auto& columns = node.GetBlock2().GetBlock2().GetRule_pure_column_list1();
        Token(columns.GetToken1());
        derivedColumnsPos = Ctx.Pos();

        if (node.GetRule_single_source1().Alt_case() != TRule_single_source::kAltSingleSource3) {
            Error() << "Derived column list is only supported for VALUES";
            return nullptr;
        }

        PureColumnListStr(columns, *this, derivedColumns);
    }

    auto singleSource = SingleSource(node.GetRule_single_source1(), derivedColumns, derivedColumnsPos, unorderedSubquery);
    if (!singleSource) {
        return nullptr;
    }
    if (node.HasBlock2()) {
        TString label;
        switch (node.GetBlock2().GetBlock1().Alt_case()) {
            case TRule_named_single_source_TBlock2_TBlock1::kAlt1:
                label = Id(node.GetBlock2().GetBlock1().GetAlt1().GetBlock1().GetRule_an_id2(), *this);
                break;
            case TRule_named_single_source_TBlock2_TBlock1::kAlt2:
                label = Id(node.GetBlock2().GetBlock1().GetAlt2().GetRule_an_id_as_compat1(), *this);
                if (!Ctx.AnsiOptionalAs) {
                    // AS is mandatory
                    Ctx.Error() << "Expecting mandatory AS here. Did you miss comma? Please add PRAGMA AnsiOptionalAs; for ANSI compatibility";
                    return {};
                }
                break;
            default:
                Y_FAIL("You should change implementation according to grammar changes");
        }
        singleSource->SetLabel(label);
    }
    if (node.HasBlock3()) {
        ESampleMode mode = ESampleMode::Auto;
        TSqlExpression expr(Ctx, Mode);
        TNodePtr samplingRateNode;
        TNodePtr samplingSeedNode;
        const auto& sampleBlock = node.GetBlock3();
        TPosition pos;
        switch (sampleBlock.Alt_case()) {
        case TRule_named_single_source::TBlock3::kAlt1:
            {
                const auto& sampleExpr = sampleBlock.GetAlt1().GetRule_sample_clause1().GetRule_expr2();
                samplingRateNode = expr.Build(sampleExpr);
                if (!samplingRateNode) {
                    return nullptr;
                }
                pos = GetPos(sampleBlock.GetAlt1().GetRule_sample_clause1().GetToken1());
                Ctx.IncrementMonCounter("sql_features", "SampleClause");
            }
            break;
        case TRule_named_single_source::TBlock3::kAlt2:
            {
                const auto& tableSampleClause = sampleBlock.GetAlt2().GetRule_tablesample_clause1();
                const auto& modeToken = tableSampleClause.GetRule_sampling_mode2().GetToken1();
                const TCiString& token = Token(modeToken);
                if (token == "system") {
                    mode = ESampleMode::System;
                } else if (token == "bernoulli") {
                    mode = ESampleMode::Bernoulli;
                } else {
                    Ctx.Error(GetPos(modeToken)) << "Unsupported sampling mode: " << token;
                    Ctx.IncrementMonCounter("sql_errors", "UnsupportedSamplingMode");
                    return nullptr;
                }
                const auto& tableSampleExpr = tableSampleClause.GetRule_expr4();
                samplingRateNode = expr.Build(tableSampleExpr);
                if (!samplingRateNode) {
                    return nullptr;
                }
                if (tableSampleClause.HasBlock6()) {
                    const auto& repeatableExpr = tableSampleClause.GetBlock6().GetRule_repeatable_clause1().GetRule_expr3();
                    samplingSeedNode = expr.Build(repeatableExpr);
                    if (!samplingSeedNode) {
                        return nullptr;
                    }
                }
                pos = GetPos(sampleBlock.GetAlt2().GetRule_tablesample_clause1().GetToken1());
                Ctx.IncrementMonCounter("sql_features", "SampleClause");
            }
            break;
        default:
            Y_FAIL("SampleClause: does not corresond to grammar changes");
        }
        if (!singleSource->SetSamplingOptions(Ctx, pos, mode, samplingRateNode, samplingSeedNode)) {
            Ctx.IncrementMonCounter("sql_errors", "IncorrectSampleClause");
            return nullptr;
        }
    }
    return singleSource;
}

bool TSqlSelect::ColumnName(TVector<TNodePtr>& keys, const TRule_column_name& node) {
    const auto sourceName = OptIdPrefixAsStr(node.GetRule_opt_id_prefix1(), *this);
    const auto columnName = Id(node.GetRule_an_id2(), *this);
    if (columnName.empty()) {
        // TDOD: Id() should return TMaybe<TString>
        if (!Ctx.HasPendingErrors) {
            Ctx.Error() << "Empty column name is not allowed";
        }
        return false;
    }
    keys.push_back(BuildColumn(Ctx.Pos(), columnName, sourceName));
    return true;
}

bool TSqlSelect::ColumnName(TVector<TNodePtr>& keys, const TRule_without_column_name& node) {
    // without_column_name: (an_id DOT an_id) | an_id_without;
    TString sourceName;
    TString columnName;
    switch (node.Alt_case()) {
        case TRule_without_column_name::kAltWithoutColumnName1:
            sourceName = Id(node.GetAlt_without_column_name1().GetBlock1().GetRule_an_id1(), *this);
            columnName = Id(node.GetAlt_without_column_name1().GetBlock1().GetRule_an_id3(), *this);
            break;
        case TRule_without_column_name::kAltWithoutColumnName2:
            columnName = Id(node.GetAlt_without_column_name2().GetRule_an_id_without1(), *this);
            break;
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    if (columnName.empty()) {
        // TDOD: Id() should return TMaybe<TString>
        if (!Ctx.HasPendingErrors) {
            Ctx.Error() << "Empty column name is not allowed";
        }
        return false;
    }
    keys.push_back(BuildColumn(Ctx.Pos(), columnName, sourceName));
    return true;
}

template<typename TRule>
bool TSqlSelect::ColumnList(TVector<TNodePtr>& keys, const TRule& node) {
    bool result;
    if constexpr (std::is_same_v<TRule, TRule_column_list>) {
        result = ColumnName(keys, node.GetRule_column_name1());
    } else {
        result = ColumnName(keys, node.GetRule_without_column_name1());
    }

    if (!result) {
        return false;
    }

    for (auto b: node.GetBlock2()) {
        Token(b.GetToken1());
        if constexpr (std::is_same_v<TRule, TRule_column_list>) {
            result = ColumnName(keys, b.GetRule_column_name2());
        } else {
            result = ColumnName(keys, b.GetRule_without_column_name2());
        }
        if (!result) {
            return false;
        }
    }
    return true;
}

bool TSqlSelect::NamedColumn(TVector<TNodePtr>& columnList, const TRule_named_column& node) {
    if (!ColumnName(columnList, node.GetRule_column_name1())) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto label = Id(node.GetBlock2().GetRule_an_id2(), *this);
        columnList.back()->SetLabel(label);
    }
    return true;
}

bool TSqlTranslation::SortSpecification(const TRule_sort_specification& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    bool asc = true;
    TSqlExpression expr(Ctx, Mode);
    TColumnRefScope scope(Ctx, EColumnRefState::Allow);
    TNodePtr exprNode = expr.Build(node.GetRule_expr1());
    if (!exprNode) {
        return false;
    }
    if (node.HasBlock2()) {
        const auto& token = node.GetBlock2().GetToken1();
        Token(token);
        switch (token.GetId()) {
            case SQLv1LexerTokens::TOKEN_ASC:
                Ctx.IncrementMonCounter("sql_features", "OrderByAsc");
                break;
            case SQLv1LexerTokens::TOKEN_DESC:
                asc = false;
                Ctx.IncrementMonCounter("sql_features", "OrderByDesc");
                break;
            default:
                Ctx.IncrementMonCounter("sql_errors", "UnknownOrderBy");
                Error() << "Unsupported direction token: " << token.GetId();
                return false;
        }
    } else {
        Ctx.IncrementMonCounter("sql_features", "OrderByDefault");
    }
    auto sortSpecPtr = MakeIntrusive<TSortSpecification>();
    sortSpecPtr->OrderExpr = exprNode;
    sortSpecPtr->Ascending = asc;
    sortSpecs.emplace_back(sortSpecPtr);
    return true;
}

bool TSqlTranslation::SortSpecificationList(const TRule_sort_specification_list& node, TVector<TSortSpecificationPtr>& sortSpecs) {
    if (!SortSpecification(node.GetRule_sort_specification1(), sortSpecs)) {
        return false;
    }
    for (auto sortSpec: node.GetBlock2()) {
        Token(sortSpec.GetToken1());
        if (!SortSpecification(sortSpec.GetRule_sort_specification2(), sortSpecs)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::IsDistinctOptSet(const TRule_opt_set_quantifier& node) const {
    TPosition pos;
    return node.HasBlock1() && node.GetBlock1().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_DISTINCT;
}

bool TSqlTranslation::IsDistinctOptSet(const TRule_opt_set_quantifier& node, TPosition& distinctPos) const {
    if (node.HasBlock1() && node.GetBlock1().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_DISTINCT) {
        distinctPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
        return true;
    }
    return false;
}

bool TSqlTranslation::RoleNameClause(const TRule_role_name& node, TDeferredAtom& result, bool allowSystemRoles) {
    // role_name: an_id_or_type | bind_parameter;
    switch (node.Alt_case()) {
        case TRule_role_name::kAltRoleName1:
        {
            TString name = Id(node.GetAlt_role_name1().GetRule_an_id_or_type1(), *this);
            result = TDeferredAtom(Ctx.Pos(), name);
            break;
        }
        case TRule_role_name::kAltRoleName2:
        {
            if (!BindParameterClause(node.GetAlt_role_name2().GetRule_bind_parameter1(), result)) {
                return false;
            }
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    if (auto literalName = result.GetLiteral(); literalName && !allowSystemRoles) {
        static const THashSet<TStringBuf> systemRoles = { "current_role", "current_user", "session_user" };
        if (systemRoles.contains(to_lower(*literalName))) {
            Ctx.Error() << "System role " << to_upper(*literalName) << " can not be used here";
            return false;
        }
    }

    return true;
}

bool TSqlTranslation::RoleParameters(const TRule_create_user_option& node, TRoleParameters& result) {
    // create_user_option: ENCRYPTED? PASSWORD expr;
    result = TRoleParameters{};

    TSqlExpression expr(Ctx, Mode);
    TNodePtr password = expr.Build(node.GetRule_expr3());
    if (!password) {
        return false;
    }

    result.IsPasswordEncrypted = node.HasBlock1();
    if (!password->IsNull()) {
        result.Password = MakeAtomFromExpression(Ctx, password);
    }

    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_id& node, TDeferredAtom& result) {
    // permission_id:
    //   CONNECT
    // | LIST
    // | INSERT
    // | MANAGE
    // | DROP
    // | GRANT
    // | MODIFY (TABLES | ATTRIBUTES)
    // | (UPDATE | ERASE) ROW
    // | (REMOVE | DESCRIBE | ALTER) SCHEMA
    // | SELECT (TABLES | ATTRIBUTES | ROW)?
    // | (USE | FULL) LEGACY?
    // | CREATE (DIRECTORY | TABLE | QUEUE)?

    auto handleOneIdentifier = [&result, this] (const auto& permissionNameKeyword) {
        result = TDeferredAtom(Ctx.Pos(), GetIdentifier(*this, permissionNameKeyword).Name);
    };

    auto handleTwoIdentifiers = [&result, this] (const auto& permissionNameKeyword) {
        const auto& token1 = permissionNameKeyword.GetToken1();
        const auto& token2 = permissionNameKeyword.GetToken2();
        TString identifierName = TIdentifier(TPosition(token1.GetColumn(), token1.GetLine()), Identifier(token1)).Name +
                                "_" +
                                TIdentifier(TPosition(token2.GetColumn(), token2.GetLine()), Identifier(token2)).Name;
        result = TDeferredAtom(Ctx.Pos(), identifierName);
    };

    auto handleOneOrTwoIdentifiers = [&result, this] (const auto& permissionNameKeyword) {
        TString identifierName = GetIdentifier(*this, permissionNameKeyword).Name;
        if (permissionNameKeyword.HasBlock2()) {
            identifierName += "_" + GetIdentifier(*this, permissionNameKeyword.GetBlock2()).Name;
        }
        result = TDeferredAtom(Ctx.Pos(), identifierName);
    };

    switch (node.GetAltCase()) {
        case TRule_permission_id::kAltPermissionId1:
        {
            // CONNECT
            handleOneIdentifier(node.GetAlt_permission_id1());
            break;
        }
        case TRule_permission_id::kAltPermissionId2:
        {
            // LIST
            handleOneIdentifier(node.GetAlt_permission_id2());
            break;
        }
        case TRule_permission_id::kAltPermissionId3:
        {
            // INSERT
            handleOneIdentifier(node.GetAlt_permission_id3());
            break;
        }
        case TRule_permission_id::kAltPermissionId4:
        {
            // MANAGE
            handleOneIdentifier(node.GetAlt_permission_id4());
            break;
        }
        case TRule_permission_id::kAltPermissionId5:
        {
            // DROP
            handleOneIdentifier(node.GetAlt_permission_id5());
            break;
        }
        case TRule_permission_id::kAltPermissionId6:
        {
            // GRANT
            handleOneIdentifier(node.GetAlt_permission_id6());
            break;
        }
        case TRule_permission_id::kAltPermissionId7:
        {
            // MODIFY (TABLES | ATTRIBUTES)
            handleTwoIdentifiers(node.GetAlt_permission_id7());
            break;
        }
        case TRule_permission_id::kAltPermissionId8:
        {
            // (UPDATE | ERASE) ROW
            handleTwoIdentifiers(node.GetAlt_permission_id8());
            break;
        }
        case TRule_permission_id::kAltPermissionId9:
        {
            // (REMOVE | DESCRIBE | ALTER) SCHEMA
            handleTwoIdentifiers(node.GetAlt_permission_id9());
            break;
        }
        case TRule_permission_id::kAltPermissionId10:
        {
            // SELECT (TABLES | ATTRIBUTES | ROW)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id10());
            break;
        }
        case TRule_permission_id::kAltPermissionId11:
        {
            // (USE | FULL) LEGACY?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id11());
            break;
        }
        case TRule_permission_id::kAltPermissionId12:
        {
            // CREATE (DIRECTORY | TABLE | QUEUE)?
            handleOneOrTwoIdentifiers(node.GetAlt_permission_id12());
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_name& node, TDeferredAtom& result) {
    // permission_name: permission_id | STRING_VALUE;
    switch (node.Alt_case()) {
        case TRule_permission_name::kAltPermissionName1:
        {
            return PermissionNameClause(node.GetAlt_permission_name1().GetRule_permission_id1(), result);
            break;
        }
        case TRule_permission_name::kAltPermissionName2:
        {
            const TString stringValue(Ctx.Token(node.GetAlt_permission_name2().GetToken1()));
            auto unescaped = StringContent(Ctx, Ctx.Pos(), stringValue);
            if (!unescaped) {
                return false;
            }
            result = TDeferredAtom(Ctx.Pos(), unescaped->Content);
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    return true;
}

bool TSqlTranslation::PermissionNameClause(const TRule_permission_name_target& node, TVector<TDeferredAtom>& result, bool withGrantOption) {
    // permission_name_target: permission_name (COMMA permission_name)* COMMA? | ALL PRIVILEGES?;
    switch (node.Alt_case()) {
        case TRule_permission_name_target::kAltPermissionNameTarget1:
        {
            const auto& permissionNameRule = node.GetAlt_permission_name_target1();
            result.emplace_back();
            if (!PermissionNameClause(permissionNameRule.GetRule_permission_name1(), result.back())) {
                return false;
            }
            for (const auto& item : permissionNameRule.GetBlock2()) {
                result.emplace_back();
                if (!PermissionNameClause(item.GetRule_permission_name2(), result.back())) {
                    return false;
                }
            }
            break;
        }
        case TRule_permission_name_target::kAltPermissionNameTarget2:
        {
            result.emplace_back(Ctx.Pos(), "all_privileges");
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    if (withGrantOption) {
        result.emplace_back(Ctx.Pos(), "grant");
    }
    return true;
}

TSourcePtr TSqlSelect::ProcessCore(const TRule_process_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // PROCESS STREAM? named_single_source (COMMA named_single_source)* (USING using_call_expr (AS an_id)?
    // (WITH external_call_settings)?
    // (WHERE expr)? (HAVING expr)? (ASSUME order_by_clause)?)?

    Token(node.GetToken1());
    TPosition startPos(Ctx.Pos());

    if (!selectPos) {
        selectPos = startPos;
    }

    const bool hasUsing = node.HasBlock5();
    const bool unorderedSubquery = hasUsing;
    TSourcePtr source(NamedSingleSource(node.GetRule_named_single_source3(), unorderedSubquery));
    if (!source) {
        return nullptr;
    }
    if (node.GetBlock4().size()) {
        TVector<TSourcePtr> sources(1, source);
        for (auto& s: node.GetBlock4()) {
            sources.push_back(NamedSingleSource(s.GetRule_named_single_source2(), unorderedSubquery));
            if (!sources.back()) {
                return nullptr;
            }
        }
        auto pos = source->GetPos();
        source = BuildMuxSource(pos, std::move(sources));
    }

    const bool processStream = node.HasBlock2();

    if (!hasUsing) {
        return BuildProcess(startPos, std::move(source), nullptr, false, {}, false, processStream, settings, {});
    }

    const auto& block5 = node.GetBlock5();
    if (block5.HasBlock5()) {
        TSqlExpression expr(Ctx, Mode);
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TNodePtr where = expr.Build(block5.GetBlock5().GetRule_expr2());
        if (!where || !source->AddFilter(Ctx, where)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ProcessWhere");
    } else {
        Ctx.IncrementMonCounter("sql_features", processStream ? "ProcessStream" : "Process");
    }

    if (block5.HasBlock6()) {
        Ctx.Error() << "PROCESS does not allow HAVING yet! You may request it on yql@ maillist.";
        return nullptr;
    }

    bool listCall = false;
    TSqlCallExpr call(Ctx, Mode);
    bool initRet = call.Init(block5.GetRule_using_call_expr2());
    if (initRet) {
        call.IncCounters();
    }

    if (!initRet) {
        return nullptr;
    }

    auto args = call.GetArgs();
    for (auto& arg: args) {
        if (auto placeholder = dynamic_cast<TTableRows*>(arg.Get())) {
            if (listCall) {
                Ctx.Error() << "Only one TableRows() argument is allowed.";
                return nullptr;
            }
            listCall = true;
        }
    }

    if (!call.IsExternal() && block5.HasBlock4()) {
        Ctx.Error() << "PROCESS without USING EXTERNAL FUNCTION doesn't allow WITH block";
        return nullptr;
    }

    if (block5.HasBlock4()) {
        const auto& block54 = block5.GetBlock4();
        if (!call.ConfigureExternalCall(block54.GetRule_external_call_settings2())) {
            return nullptr;
        }
    }

    TSqlCallExpr finalCall(call, args);
    TNodePtr with(finalCall.IsExternal() ? finalCall.BuildCall() : finalCall.BuildUdf(/* forReduce = */ false));
    if (!with) {
        return {};
    }
    args = finalCall.GetArgs();
    if (call.IsExternal())
        listCall = true;

    if (block5.HasBlock3()) {
        with->SetLabel(Id(block5.GetBlock3().GetRule_an_id2(), *this));
    }

    if (call.IsExternal() && block5.HasBlock7()) {
        Ctx.Error() << "PROCESS with USING EXTERNAL FUNCTION doesn't allow ASSUME block";
        return nullptr;
    }

    TVector<TSortSpecificationPtr> assumeOrderBy;
    if (block5.HasBlock7()) {
        if (!OrderByClause(block5.GetBlock7().GetRule_order_by_clause2(), assumeOrderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(assumeOrderBy) ? "AssumeOrderBy" : "AssumeOrderByExpr");
    }

    return BuildProcess(startPos, std::move(source), with, finalCall.IsExternal(), std::move(args), listCall, processStream, settings, assumeOrderBy);
}

TSourcePtr TSqlSelect::ReduceCore(const TRule_reduce_core& node, const TWriteSettings& settings, TPosition& selectPos) {
    // REDUCE named_single_source (COMMA named_single_source)* (PRESORT sort_specification_list)?
    // ON column_list USING ALL? using_call_expr (AS an_id)?
    // (WHERE expr)? (HAVING expr)? (ASSUME order_by_clause)?
    Token(node.GetToken1());
    TPosition startPos(Ctx.Pos());
    if (!selectPos) {
        selectPos = startPos;
    }

    TSourcePtr source(NamedSingleSource(node.GetRule_named_single_source2(), true));
    if (!source) {
        return {};
    }
    if (node.GetBlock3().size()) {
        TVector<TSourcePtr> sources(1, source);
        for (auto& s: node.GetBlock3()) {
            sources.push_back(NamedSingleSource(s.GetRule_named_single_source2(), true));
            if (!sources.back()) {
                return nullptr;
            }
        }
        auto pos = source->GetPos();
        source = BuildMuxSource(pos, std::move(sources));
    }

    TVector<TSortSpecificationPtr> orderBy;
    if (node.HasBlock4()) {
        if (!SortSpecificationList(node.GetBlock4().GetRule_sort_specification_list2(), orderBy)) {
            return {};
        }
    }

    TVector<TNodePtr> keys;
    if (!ColumnList(keys, node.GetRule_column_list6())) {
        return nullptr;
    }

    if (node.HasBlock11()) {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        TNodePtr where = expr.Build(node.GetBlock11().GetRule_expr2());
        if (!where || !source->AddFilter(Ctx, where)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "ReduceWhere");
    } else {
        Ctx.IncrementMonCounter("sql_features", "Reduce");
    }

    TNodePtr having;
    if (node.HasBlock12()) {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        having = expr.Build(node.GetBlock12().GetRule_expr2());
        if (!having) {
            return nullptr;
        }
    }

    bool listCall = false;
    TSqlCallExpr call(Ctx, Mode);
    bool initRet = call.Init(node.GetRule_using_call_expr9());
    if (initRet) {
        call.IncCounters();
    }

    if (!initRet) {
        return nullptr;
    }

    auto args = call.GetArgs();
    for (auto& arg: args) {
        if (auto placeholder = dynamic_cast<TTableRows*>(arg.Get())) {
            if (listCall) {
                Ctx.Error() << "Only one TableRows() argument is allowed.";
                return nullptr;
            }
            listCall = true;
        }
    }

    TSqlCallExpr finalCall(call, args);

    TNodePtr udf(finalCall.BuildUdf(/* forReduce = */ true));
    if (!udf) {
        return {};
    }

    if (node.HasBlock10()) {
        udf->SetLabel(Id(node.GetBlock10().GetRule_an_id2(), *this));
    }

    const auto reduceMode = node.HasBlock8() ? ReduceMode::ByAll : ReduceMode::ByPartition;

    TVector<TSortSpecificationPtr> assumeOrderBy;
    if (node.HasBlock13()) {
        if (!OrderByClause(node.GetBlock13().GetRule_order_by_clause2(), assumeOrderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(assumeOrderBy) ? "AssumeOrderBy" : "AssumeOrderByExpr");
    }

    return BuildReduce(startPos, reduceMode, std::move(source), std::move(orderBy), std::move(keys), std::move(args), udf, having,
        settings, assumeOrderBy, listCall);
}

TSourcePtr TSqlSelect::SelectCore(const TRule_select_core& node, const TWriteSettings& settings, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement, TVector<TSortSpecificationPtr>& selectOpOrederBy, bool& selectOpAssumeOrderBy)
{
    // (FROM join_source)? SELECT STREAM? opt_set_quantifier result_column (COMMA result_column)* COMMA? (WITHOUT column_list)? (FROM join_source)? (WHERE expr)?
    // group_by_clause? (HAVING expr)? window_clause? ext_order_by_clause?
    selectOpOrederBy = {};
    selectOpAssumeOrderBy = false;
    if (node.HasBlock1()) {
        Token(node.GetBlock1().GetToken1());
    } else {
        Token(node.GetToken2());
    }

    TPosition startPos(Ctx.Pos());
    if (!selectPos) {
        selectPos = Ctx.Pos();
    }

    const bool distinct = IsDistinctOptSet(node.GetRule_opt_set_quantifier4());
    if (distinct) {
        Ctx.IncrementMonCounter("sql_features", "DistinctInSelect");
    }

    TSourcePtr source(BuildFakeSource(selectPos, /* missingFrom = */ true, Mode == NSQLTranslation::ESqlMode::SUBQUERY));
    if (node.HasBlock1() && node.HasBlock9()) {
        Token(node.GetBlock9().GetToken1());
        Ctx.IncrementMonCounter("sql_errors", "DoubleFrom");
        Ctx.Error() << "Only one FROM clause is allowed";
        return nullptr;
    }
    if (node.HasBlock1()) {
        source = JoinSource(node.GetBlock1().GetRule_join_source2());
        Ctx.IncrementMonCounter("sql_features", "FromInFront");
    } else if (node.HasBlock9()) {
        source = JoinSource(node.GetBlock9().GetRule_join_source2());
    }
    if (!source) {
        return nullptr;
    }

    const bool selectStream = node.HasBlock3();
    TVector<TNodePtr> without;
    if (node.HasBlock8()) {
        if (!ColumnList(without, node.GetBlock8().GetRule_without_column_list2())) {
            return nullptr;
        }
    }
    if (node.HasBlock10()) {
        auto block = node.GetBlock10();
        Token(block.GetToken1());
        TPosition pos(Ctx.Pos());
        TNodePtr where;
        {
            TColumnRefScope scope(Ctx, EColumnRefState::Allow);
            TSqlExpression expr(Ctx, Mode);
            where = expr.Build(block.GetRule_expr2());
        }
        if (!where) {
            Ctx.IncrementMonCounter("sql_errors", "WhereInvalid");
            return nullptr;
        }
        if (!source->AddFilter(Ctx, where)) {
            Ctx.IncrementMonCounter("sql_errors", "WhereNotSupportedBySource");
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "Where");
    }

    /// \todo merge gtoupByExpr and groupBy in one
    TVector<TNodePtr> groupByExpr, groupBy;
    TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec;
    bool compactGroupBy = false;
    TString groupBySuffix;
    if (node.HasBlock11()) {
        TGroupByClause clause(Ctx, Mode);
        if (!clause.Build(node.GetBlock11().GetRule_group_by_clause1())) {
            return nullptr;
        }
        bool hasHopping = (bool)clause.GetLegacyHoppingWindow();
        for (const auto& exprAlias: clause.Aliases()) {
            YQL_ENSURE(exprAlias.first == exprAlias.second->GetLabel());
            groupByExpr.emplace_back(exprAlias.second);
            hasHopping |= (bool)dynamic_cast<THoppingWindow*>(exprAlias.second.Get());
        }
        groupBy = std::move(clause.Content());
        clause.SetFeatures("sql_features");
        legacyHoppingWindowSpec = clause.GetLegacyHoppingWindow();
        compactGroupBy = clause.IsCompactGroupBy();
        groupBySuffix = clause.GetSuffix();

        if (source->IsStream() && !hasHopping) {
            Ctx.Error() << "Streaming group by query must have a hopping window specification.";
            return nullptr;
        }
    }

    TNodePtr having;
    if (node.HasBlock12()) {
        TSqlExpression expr(Ctx, Mode);
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        having = expr.Build(node.GetBlock12().GetRule_expr2());
        if (!having) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "Having");
    }

    TWinSpecs windowSpec;
    if (node.HasBlock13()) {
        if (source->IsStream()) {
            Ctx.Error() << "WINDOW is not allowed in streaming queries";
            return nullptr;
        }
        if (!WindowClause(node.GetBlock13().GetRule_window_clause1(), windowSpec)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", "WindowClause");
    }

    bool assumeSorted = false;
    TVector<TSortSpecificationPtr> orderBy;
    if (node.HasBlock14()) {
        auto& orderBlock = node.GetBlock14().GetRule_ext_order_by_clause1();
        assumeSorted = orderBlock.HasBlock1();

        Token(orderBlock.GetRule_order_by_clause2().GetToken1());

        if (source->IsStream()) {
            Ctx.Error() << "ORDER BY is not allowed in streaming queries";
            return nullptr;
        }

        if (!ValidateLimitOrderByWithSelectOp(placement, "ORDER BY")) {
            return nullptr;
        }

        if (!OrderByClause(orderBlock.GetRule_order_by_clause2(), orderBy)) {
            return nullptr;
        }
        Ctx.IncrementMonCounter("sql_features", IsColumnsOnly(orderBy)
            ? (assumeSorted ? "AssumeOrderBy" : "OrderBy")
            : (assumeSorted ? "AssumeOrderByExpr" : "OrderByExpr")
        );

        if (!NeedPassLimitOrderByToUnderlyingSelect(placement)) {
            selectOpOrederBy.swap(orderBy);
            std::swap(selectOpAssumeOrderBy, assumeSorted);
        }
    }

    TVector<TNodePtr> terms;
    {
        class TScopedWinSpecs {
        public:
            TScopedWinSpecs(TContext& ctx, TWinSpecs& specs)
                : Ctx(ctx)
            {
                Ctx.WinSpecsScopes.push_back(std::ref(specs));
            }
            ~TScopedWinSpecs() {
                Ctx.WinSpecsScopes.pop_back();
            }
        private:
            TContext& Ctx;
        };


        TScopedWinSpecs scoped(Ctx, windowSpec);
        if (!SelectTerm(terms, node.GetRule_result_column5())) {
            return nullptr;
        }
        for (auto block: node.GetBlock6()) {
            if (!SelectTerm(terms, block.GetRule_result_column2())) {
                return nullptr;
            }
        }

    }
    if (!ValidateSelectColumns(terms)) {
        return nullptr;
    }
    return BuildSelectCore(Ctx, startPos, std::move(source), groupByExpr, groupBy, compactGroupBy, groupBySuffix, assumeSorted, orderBy, having,
        std::move(windowSpec), legacyHoppingWindowSpec, std::move(terms), distinct, std::move(without), selectStream, settings);
}

TString TSqlTranslation::FrameSettingsToString(EFrameSettings settings, bool isUnbounded) {
    TString result;
    switch (settings) {
        case FramePreceding:
            result = "PRECEDING"; break;
        case FrameCurrentRow:
            YQL_ENSURE(!isUnbounded);
            result = "CURRENT ROW"; break;
        case FrameFollowing:
            result = "FOLLOWING"; break;
        default:
            Y_FAIL("Unexpected frame settings");
    }

    return (isUnbounded ? "UNBOUNDED " : "") + result;
}

bool CheckFrameBoundLiteral(TContext& ctx, const TFrameBound& bound, TMaybe<i32>& boundValue) {
    boundValue = {};
    auto node = bound.Bound;
    if (node && node->IsLiteral()) {
        auto type = node->GetLiteralType();
        if (type != "Int32") {
            ctx.Error(node->GetPos()) << "Expecting Int32 as frame bound value, but got " << type << " literal";
            return false;
        }

        i32 value = FromString<i32>(node->GetLiteralValue());
        if (value < 0) {
            ctx.Error(node->GetPos()) << "Expecting non-negative value for frame bound, but got " << value;
            return false;
        }

        boundValue = value;
    }

    return true;
}

bool TSqlTranslation::IsValidFrameSettings(TContext& ctx, const TFrameSpecification& frameSpec, size_t sortSpecSize) {
    const TFrameBound& begin = *frameSpec.FrameBegin;
    const TFrameBound& end = *frameSpec.FrameEnd;

    YQL_ENSURE(begin.Settings != FrameUndefined);
    YQL_ENSURE(end.Settings != FrameUndefined);

    const bool beginUnbounded = !begin.Bound && begin.Settings != FrameCurrentRow;
    const bool endUnbounded = !end.Bound && end.Settings != FrameCurrentRow;

    if (beginUnbounded && begin.Settings == FrameFollowing) {
        ctx.Error(begin.Pos) << "Frame cannot start from " << FrameSettingsToString(begin.Settings, beginUnbounded);
        return false;
    }

    if (endUnbounded && end.Settings == FramePreceding) {
        ctx.Error(end.Pos) << "Frame cannot end with " << FrameSettingsToString(end.Settings, endUnbounded);
        return false;
    }

    if (begin.Settings > end.Settings) {
        ctx.Error(begin.Pos) << "Frame cannot start from " << FrameSettingsToString(begin.Settings, beginUnbounded)
                             << " and end with " << FrameSettingsToString(end.Settings, endUnbounded);
        return false;
    }

    if (frameSpec.FrameType == FrameByRange && sortSpecSize != 1) {
        TStringBuf msg = "RANGE with <offset> PRECEDING/FOLLOWING requires exactly one expression in ORDER BY partition clause";
        if (begin.Bound) {
            ctx.Error(begin.Bound->GetPos()) << msg;
            return false;
        }
        if (end.Bound) {
            ctx.Error(end.Bound->GetPos()) << msg;
            return false;
        }
    }

    TMaybe<i32> beginValue;
    TMaybe<i32> endValue;

    if (frameSpec.FrameType != EFrameType::FrameByRange) {
        if (!CheckFrameBoundLiteral(ctx, begin, beginValue) || !CheckFrameBoundLiteral(ctx, end, endValue)) {
            return false;
        }
    }

    if (beginValue.Defined() && endValue.Defined()) {
        if (begin.Settings == FramePreceding) {
            beginValue = 0 - *beginValue;
        }
        if (end.Settings == FramePreceding) {
            endValue = 0 - *endValue;
        }

        if (*beginValue > *endValue) {
            YQL_ENSURE(begin.Bound);
            ctx.Warning(begin.Bound->GetPos(), TIssuesIds::YQL_EMPTY_WINDOW_FRAME) << "Used frame specification implies empty window frame";
        }
    }

    return true;
}

bool TSqlTranslation::FrameBound(const TRule_window_frame_bound& rule, TFrameBoundPtr& bound) {
    // window_frame_bound:
    //    CURRENT ROW
    //  | (expr | UNBOUNDED) (PRECEDING | FOLLOWING)
    // ;
    bound = new TFrameBound;
    switch (rule.Alt_case()) {
        case TRule_window_frame_bound::kAltWindowFrameBound1:
            bound->Pos = GetPos(rule.GetAlt_window_frame_bound1().GetToken1());
            bound->Settings = FrameCurrentRow;
            break;
        case TRule_window_frame_bound::kAltWindowFrameBound2: {
            auto block = rule.GetAlt_window_frame_bound2().GetBlock1();
            switch (block.Alt_case()) {
                case TRule_window_frame_bound_TAlt2_TBlock1::kAlt1: {
                    TSqlExpression boundExpr(Ctx, Mode);
                    bound->Bound = boundExpr.Build(block.GetAlt1().GetRule_expr1());
                    if (!bound->Bound) {
                        return false;
                    }
                    bound->Pos = bound->Bound->GetPos();
                    break;
                }
                case TRule_window_frame_bound_TAlt2_TBlock1::kAlt2:
                    bound->Pos = GetPos(block.GetAlt2().GetToken1());
                    break;
                default:
                    Y_FAIL("You should change implementation according to grammar changes");
            }

            const TString settingToken = to_lower(Token(rule.GetAlt_window_frame_bound2().GetToken2()));
            if (settingToken == "preceding") {
                bound->Settings = FramePreceding;
            } else if (settingToken == "following") {
                bound->Settings = FrameFollowing;
            } else {
                Y_FAIL("You should change implementation according to grammar changes");
            }
            break;
        }
        default:
            Y_FAIL("FrameClause: frame bound not corresond to grammar changes");
    }
    return true;
}

bool TSqlTranslation::FrameClause(const TRule_window_frame_clause& rule, TFrameSpecificationPtr& frameSpec, size_t sortSpecSize) {
    // window_frame_clause: window_frame_units window_frame_extent window_frame_exclusion?;
    frameSpec = new TFrameSpecification;
    const TString frameUnitStr = to_lower(Token(rule.GetRule_window_frame_units1().GetToken1()));
    if (frameUnitStr == "rows") {
        frameSpec->FrameType = EFrameType::FrameByRows;
    } else if (frameUnitStr == "range") {
        frameSpec->FrameType = EFrameType::FrameByRange;
    } else {
        YQL_ENSURE(frameUnitStr == "groups");
        frameSpec->FrameType = EFrameType::FrameByGroups;
    }

    auto frameExtent = rule.GetRule_window_frame_extent2();
    // window_frame_extent: window_frame_bound | window_frame_between;
    switch (frameExtent.Alt_case()) {
        case TRule_window_frame_extent::kAltWindowFrameExtent1: {
            auto start = frameExtent.GetAlt_window_frame_extent1().GetRule_window_frame_bound1();
            if (!FrameBound(start, frameSpec->FrameBegin)) {
                return false;
            }

            // frame end is CURRENT ROW
            frameSpec->FrameEnd = new TFrameBound;
            frameSpec->FrameEnd->Pos = frameSpec->FrameBegin->Pos;
            frameSpec->FrameEnd->Settings = FrameCurrentRow;
            break;
        }
        case TRule_window_frame_extent::kAltWindowFrameExtent2: {
            // window_frame_between: BETWEEN window_frame_bound AND window_frame_bound;
            auto between = frameExtent.GetAlt_window_frame_extent2().GetRule_window_frame_between1();
            if (!FrameBound(between.GetRule_window_frame_bound2(), frameSpec->FrameBegin)) {
                return false;
            }
            if (!FrameBound(between.GetRule_window_frame_bound4(), frameSpec->FrameEnd)) {
                return false;
            }
            break;
        }
        default:
            Y_FAIL("FrameClause: frame extent not correspond to grammar changes");
    }
    YQL_ENSURE(frameSpec->FrameBegin);
    YQL_ENSURE(frameSpec->FrameEnd);
    if (!IsValidFrameSettings(Ctx, *frameSpec, sortSpecSize)) {
        return false;
    }

    if (rule.HasBlock3()) {
        // window_frame_exclusion: EXCLUDE CURRENT ROW | EXCLUDE GROUP | EXCLUDE TIES | EXCLUDE NO OTHERS;
        switch (rule.GetBlock3().GetRule_window_frame_exclusion1().Alt_case()) {
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion1:
                frameSpec->FrameExclusion = FrameExclCurRow;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion2:
                frameSpec->FrameExclusion = FrameExclGroup;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion3:
                frameSpec->FrameExclusion = FrameExclTies;
                break;
            case TRule_window_frame_exclusion::kAltWindowFrameExclusion4:
                frameSpec->FrameExclusion = FrameExclNone;
                break;
            default:
                Y_FAIL("FrameClause: frame exclusion not correspond to grammar changes");
        }
    }

    if (frameSpec->FrameExclusion != FrameExclNone) {
        Ctx.Error() << "Frame exclusion is not supported yet";
        return false;
    }

    return true;
}

TWindowSpecificationPtr TSqlTranslation::WindowSpecification(const TRule_window_specification_details& rule) {
    /*
    window_specification_details:
        existing_window_name?
        window_partition_clause?
        window_order_clause?
        window_frame_clause?
    */
    TWindowSpecificationPtr winSpecPtr = new TWindowSpecification;
    if (rule.HasBlock1()) {
        Ctx.Error() << "Existing window name is not supported in window specification yet!";
        return {};
    }
    if (rule.HasBlock2()) {
        /*
        window_partition_clause: PARTITION COMPACT? BY named_expr_list;
        */
        auto& partitionClause = rule.GetBlock2().GetRule_window_partition_clause1();
        winSpecPtr->IsCompact = partitionClause.HasBlock2();
        if (!winSpecPtr->IsCompact) {
            auto hints = Ctx.PullHintForToken(Ctx.TokenPosition(partitionClause.GetToken1()));
            winSpecPtr->IsCompact = AnyOf(hints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
        }
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        if (!NamedExprList(partitionClause.GetRule_named_expr_list4(), winSpecPtr->Partitions)) {
            return {};
        }
        // ignore empty unnamed tuples:
        // "PARTITION BY (), foo(x) as y, (), (z)" is allowed and will work exactly the same as
        // "PARTITION BY foo(x) as y, z"
        auto removed = std::remove_if(winSpecPtr->Partitions.begin(), winSpecPtr->Partitions.end(),
            [](const TNodePtr& partitionNode) {
                return !partitionNode->GetLabel() && !partitionNode->GetColumnName() &&
                       dynamic_cast<TTupleNode*>(partitionNode.Get()) != nullptr &&
                       partitionNode->GetTupleSize() == 0;
        });
        winSpecPtr->Partitions.erase(removed, winSpecPtr->Partitions.end());

    }
    if (rule.HasBlock3()) {
        if (!OrderByClause(rule.GetBlock3().GetRule_window_order_clause1().GetRule_order_by_clause1(), winSpecPtr->OrderBy)) {
            return {};
        }
    }
    const bool ordered = !winSpecPtr->OrderBy.empty();
    if (rule.HasBlock4()) {
        if (!FrameClause(rule.GetBlock4().GetRule_window_frame_clause1(), winSpecPtr->Frame, winSpecPtr->OrderBy.size())) {
            return {};
        }
    } else {
        winSpecPtr->Frame = new TFrameSpecification;
        winSpecPtr->Frame->FrameBegin = new TFrameBound;
        winSpecPtr->Frame->FrameEnd = new TFrameBound;
        winSpecPtr->Frame->FrameBegin->Pos = winSpecPtr->Frame->FrameEnd->Pos = Ctx.Pos();
        winSpecPtr->Frame->FrameExclusion = EFrameExclusions::FrameExclNone;

        winSpecPtr->Frame->FrameBegin->Settings = EFrameSettings::FramePreceding;
        if (Ctx.AnsiCurrentRow) {
            // RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            winSpecPtr->Frame->FrameType = EFrameType::FrameByRange;
            winSpecPtr->Frame->FrameEnd->Settings = EFrameSettings::FrameCurrentRow;
        } else if (ordered) {
            // legacy behavior
            // ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            winSpecPtr->Frame->FrameType = EFrameType::FrameByRows;
            winSpecPtr->Frame->FrameEnd->Settings = EFrameSettings::FrameCurrentRow;
        } else {
            // ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            winSpecPtr->Frame->FrameType = EFrameType::FrameByRows;
            winSpecPtr->Frame->FrameEnd->Settings = EFrameSettings::FrameFollowing;
        }
    }

    // Normalize and simplify
    auto replaceCurrentWith = [](TFrameBound& frame, bool preceding, TNodePtr value ) {
        frame.Settings = preceding ? EFrameSettings::FramePreceding : EFrameSettings::FrameFollowing;
        frame.Bound = value;
    };

    const auto frameSpec = winSpecPtr->Frame;
    if (!ordered && frameSpec->FrameType != EFrameType::FrameByRows) {
        // CURRENT ROW -> UNBOUNDED
        if (frameSpec->FrameBegin->Settings == EFrameSettings::FrameCurrentRow) {
            replaceCurrentWith(*frameSpec->FrameBegin, true, nullptr);
        }
        if (frameSpec->FrameEnd->Settings == EFrameSettings::FrameCurrentRow) {
            replaceCurrentWith(*frameSpec->FrameBegin, false, nullptr);
        }
    }

    // RANGE/GROUPS UNBOUNDED -> ROWS UNBOUNDED
    if (frameSpec->FrameBegin->Settings == EFrameSettings::FramePreceding && !frameSpec->FrameBegin->Bound &&
        frameSpec->FrameEnd->Settings == EFrameSettings::FrameFollowing && !frameSpec->FrameEnd->Bound)
    {
        frameSpec->FrameType = EFrameType::FrameByRows;
    }

    if (frameSpec->FrameType != EFrameType::FrameByRange) {
        // replace FrameCurrentRow for ROWS/GROUPS with 0 preceding/following
        // FrameCurrentRow has special meaning ( = first/last peer row)
        if (frameSpec->FrameBegin->Settings == EFrameSettings::FrameCurrentRow) {
            TNodePtr zero = new TLiteralNumberNode<i32>(winSpecPtr->Frame->FrameBegin->Pos, "Int32", "0");
            replaceCurrentWith(*frameSpec->FrameBegin, true, zero);
        }

        if (frameSpec->FrameEnd->Settings == EFrameSettings::FrameCurrentRow) {
            TNodePtr zero = new TLiteralNumberNode<i32>(winSpecPtr->Frame->FrameEnd->Pos, "Int32", "0");
            replaceCurrentWith(*frameSpec->FrameEnd, false, zero);
        }
    }

    return winSpecPtr;
}

bool TSqlSelect::WindowDefinition(const TRule_window_definition& rule, TWinSpecs& winSpecs) {
    const TString windowName = Id(rule.GetRule_new_window_name1().GetRule_window_name1().GetRule_an_id_window1(), *this);
    if (winSpecs.contains(windowName)) {
        Ctx.Error() << "Unable to declare window with same name: " << windowName;
        return false;
    }
    auto windowSpec = WindowSpecification(rule.GetRule_window_specification3().GetRule_window_specification_details2());
    if (!windowSpec) {
        return false;
    }
    winSpecs.emplace(windowName, std::move(windowSpec));
    return true;
}

bool TSqlSelect::WindowClause(const TRule_window_clause& rule, TWinSpecs& winSpecs) {
    auto windowList = rule.GetRule_window_definition_list2();
    if (!WindowDefinition(windowList.GetRule_window_definition1(), winSpecs)) {
        return false;
    }
    for (auto& block: windowList.GetBlock2()) {
        if (!WindowDefinition(block.GetRule_window_definition2(), winSpecs)) {
            return false;
        }
    }
    return true;
}

bool TSqlTranslation::OrderByClause(const TRule_order_by_clause& node, TVector<TSortSpecificationPtr>& orderBy) {
    return SortSpecificationList(node.GetRule_sort_specification_list3(), orderBy);
}

bool TGroupByClause::Build(const TRule_group_by_clause& node) {
    // group_by_clause: GROUP COMPACT? BY opt_set_quantifier grouping_element_list (WITH an_id)?;
    CompactGroupBy = node.HasBlock2();
    if (!CompactGroupBy) {
        auto hints = Ctx.PullHintForToken(Ctx.TokenPosition(node.GetToken1()));
        CompactGroupBy = AnyOf(hints, [](const NSQLTranslation::TSQLHint& hint) { return to_lower(hint.Name) == "compact"; });
    }
    TPosition distinctPos;
    if (IsDistinctOptSet(node.GetRule_opt_set_quantifier4(), distinctPos)) {
        Ctx.Error(distinctPos) << "DISTINCT is not supported in GROUP BY clause yet!";
        Ctx.IncrementMonCounter("sql_errors", "DistinctInGroupByNotSupported");
        return false;
    }
    if (!ParseList(node.GetRule_grouping_element_list5(), EGroupByFeatures::Ordinary)) {
        return false;
    }

    if (node.HasBlock6()) {
        TString mode = Id(node.GetBlock6().GetRule_an_id2(), *this);
        TMaybe<TIssue> normalizeError = NormalizeName(Ctx.Pos(), mode);
        if (!normalizeError.Empty()) {
            Error() << normalizeError->GetMessage();
            Ctx.IncrementMonCounter("sql_errors", "NormalizeGroupByModeError");
            return false;
        }

        if (mode == "combine") {
            Suffix = "Combine";
        } else if (mode == "combinestate") {
            Suffix = "CombineState";
        } else if (mode == "mergestate") {
            Suffix = "MergeState";
        } else if (mode == "finalize") {
            Suffix = "Finalize";
        } else if (mode == "mergefinalize") {
            Suffix = "MergeFinalize";
        } else if (mode == "mergemanyfinalize") {
            Suffix = "MergeManyFinalize";
        } else {
            Ctx.Error() << "Unsupported group by mode: " << mode;
            Ctx.IncrementMonCounter("sql_errors", "GroupByModeUnknown");
            return false;
        }
    }

    if (!ResolveGroupByAndGrouping()) {
        return false;
    }
    return true;
}

bool TGroupByClause::ParseList(const TRule_grouping_element_list& groupingListNode, EGroupByFeatures featureContext) {
    if (!GroupingElement(groupingListNode.GetRule_grouping_element1(), featureContext)) {
        return false;
    }
    for (auto b: groupingListNode.GetBlock2()) {
        if (!GroupingElement(b.GetRule_grouping_element2(), featureContext)) {
            return false;
        }
    }
    return true;
}

void TGroupByClause::SetFeatures(const TString& field) const {
    Ctx.IncrementMonCounter(field, "GroupBy");
    const auto& features = Features();
    if (features.Test(EGroupByFeatures::Ordinary)) {
        Ctx.IncrementMonCounter(field, "GroupByOrdinary");
    }
    if (features.Test(EGroupByFeatures::Expression)) {
        Ctx.IncrementMonCounter(field, "GroupByExpression");
    }
    if (features.Test(EGroupByFeatures::Rollup)) {
        Ctx.IncrementMonCounter(field, "GroupByRollup");
    }
    if (features.Test(EGroupByFeatures::Cube)) {
        Ctx.IncrementMonCounter(field, "GroupByCube");
    }
    if (features.Test(EGroupByFeatures::GroupingSet)) {
        Ctx.IncrementMonCounter(field, "GroupByGroupingSet");
    }
    if (features.Test(EGroupByFeatures::Empty)) {
        Ctx.IncrementMonCounter(field, "GroupByEmpty");
    }
}

TVector<TNodePtr>& TGroupByClause::Content() {
    return GroupBySet;
}

TMap<TString, TNodePtr>& TGroupByClause::Aliases() {
    return GroupSetContext->NodeAliases;
}

TLegacyHoppingWindowSpecPtr TGroupByClause::GetLegacyHoppingWindow() const {
    return LegacyHoppingWindowSpec;
}

bool TGroupByClause::IsCompactGroupBy() const {
    return CompactGroupBy;
}

TString TGroupByClause::GetSuffix() const {
    return Suffix;
}

TMaybe<TVector<TNodePtr>> TGroupByClause::MultiplyGroupingSets(const TVector<TNodePtr>& lhs, const TVector<TNodePtr>& rhs) const {
    TVector<TNodePtr> content;
    for (const auto& leftNode: lhs) {
        auto leftPtr = leftNode->ContentListPtr();
        if (!leftPtr) {
            // TODO: shouldn't happen
            Ctx.Error() << "Unable to multiply grouping sets";
            return {};
        }
        for (const auto& rightNode: rhs) {
            TVector<TNodePtr> mulItem(leftPtr->begin(), leftPtr->end());
            auto rightPtr = rightNode->ContentListPtr();
            if (!rightPtr) {
                // TODO: shouldn't happen
                Ctx.Error() << "Unable to multiply grouping sets";
                return {};
            }
            mulItem.insert(mulItem.end(), rightPtr->begin(), rightPtr->end());
            content.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(mulItem)));
        }
    }
    return content;
}

bool TGroupByClause::ResolveGroupByAndGrouping() {
    auto listPos = std::find_if(GroupBySet.begin(), GroupBySet.end(), [](const TNodePtr& node) {
        return node->ContentListPtr();
    });
    if (listPos == GroupBySet.end()) {
        return true;
    }
    auto curContent = *(*listPos)->ContentListPtr();
    if (listPos != GroupBySet.begin()) {
        TVector<TNodePtr> emulate(GroupBySet.begin(), listPos);
        TVector<TNodePtr> emulateContent(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(emulate)));
        auto mult = MultiplyGroupingSets(emulateContent, curContent);
        if (!mult) {
            return false;
        }
        curContent = *mult;
    }
    for (++listPos; listPos != GroupBySet.end(); ++listPos) {
        auto newElem = (*listPos)->ContentListPtr();
        if (newElem) {
            auto mult = MultiplyGroupingSets(curContent, *newElem);
            if (!mult) {
                return false;
            }
            curContent = *mult;
        } else {
            TVector<TNodePtr> emulate(1, *listPos);
            TVector<TNodePtr> emulateContent(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(emulate)));
            auto mult = MultiplyGroupingSets(curContent, emulateContent);
            if (!mult) {
                return false;
            }
            curContent = *mult;
        }
    }
    TVector<TNodePtr> result(1, BuildListOfNamedNodes(Ctx.Pos(), std::move(curContent)));
    std::swap(result, GroupBySet);
    return true;
}

bool TGroupByClause::GroupingElement(const TRule_grouping_element& node, EGroupByFeatures featureContext) {
    TSourcePtr res;
    TVector<TNodePtr> emptyContent;
    switch (node.Alt_case()) {
        case TRule_grouping_element::kAltGroupingElement1:
            if (!OrdinaryGroupingSet(node.GetAlt_grouping_element1().GetRule_ordinary_grouping_set1(), featureContext)) {
                return false;
            }
            Features().Set(EGroupByFeatures::Ordinary);
            break;
        case TRule_grouping_element::kAltGroupingElement2: {
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.OrdinaryGroupingSetList(node.GetAlt_grouping_element2().GetRule_rollup_list1().GetRule_ordinary_grouping_set_list3(),
                EGroupByFeatures::Rollup))
            {
                return false;
            }
            auto& content = subClause.Content();
            TVector<TNodePtr> collection;
            for (auto limit = content.end(), begin = content.begin(); limit != begin; --limit) {
                TVector<TNodePtr> grouping(begin, limit);
                collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(grouping)));
            }
            collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(emptyContent)));
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Ctx.IncrementMonCounter("sql_features", TStringBuilder() << "GroupByRollup" << content.size());
            Features().Set(EGroupByFeatures::Rollup);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement3: {
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.OrdinaryGroupingSetList(node.GetAlt_grouping_element3().GetRule_cube_list1().GetRule_ordinary_grouping_set_list3(),
                EGroupByFeatures::Cube))
            {
                return false;
            }
            auto& content = subClause.Content();
            if (content.size() > Ctx.PragmaGroupByCubeLimit) {
                Ctx.Error() << "GROUP BY CUBE is allowed only for " << Ctx.PragmaGroupByCubeLimit << " columns, but you use " << content.size();
                return false;
            }
            TVector<TNodePtr> collection;
            for (unsigned mask = (1 << content.size()) - 1; mask > 0; --mask) {
                TVector<TNodePtr> grouping;
                for (unsigned index = 0; index < content.size(); ++index) {
                    if (mask & (1 << index)) {
                        grouping.push_back(content[content.size() - index - 1]);
                    }
                }
                collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(grouping)));
            }
            collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(emptyContent)));
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Ctx.IncrementMonCounter("sql_features", TStringBuilder() << "GroupByCube" << content.size());
            Features().Set(EGroupByFeatures::Cube);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement4: {
            auto listNode = node.GetAlt_grouping_element4().GetRule_grouping_sets_specification1().GetRule_grouping_element_list4();
            TGroupByClause subClause(Ctx, Mode, GroupSetContext);
            if (!subClause.ParseList(listNode, EGroupByFeatures::GroupingSet)) {
                return false;
            }
            auto& content = subClause.Content();
            TVector<TNodePtr> collection;
            bool hasEmpty = false;
            for (auto& elem: content) {
                auto elemContent = elem->ContentListPtr();
                if (elemContent) {
                    if (!elemContent->empty() && elemContent->front()->ContentListPtr()) {
                        for (auto& sub: *elemContent) {
                            FeedCollection(sub, collection, hasEmpty);
                        }
                    } else {
                        FeedCollection(elem, collection, hasEmpty);
                    }
                } else {
                    TVector<TNodePtr> elemList(1, std::move(elem));
                    collection.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(elemList)));
                }
            }
            GroupBySet.push_back(BuildListOfNamedNodes(Ctx.Pos(), std::move(collection)));
            Features().Set(EGroupByFeatures::GroupingSet);
            break;
        }
        case TRule_grouping_element::kAltGroupingElement5: {
            if (!HoppingWindow(node.GetAlt_grouping_element5().GetRule_hopping_window_specification1())) {
                return false;
            }
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    return true;
}

void TGroupByClause::FeedCollection(const TNodePtr& elem, TVector<TNodePtr>& collection, bool& hasEmpty) const {
    auto elemContentPtr = elem->ContentListPtr();
    if (elemContentPtr && elemContentPtr->empty()) {
        if (hasEmpty) {
            return;
        }
        hasEmpty = true;
    }
    collection.push_back(elem);
}

bool TGroupByClause::OrdinaryGroupingSet(const TRule_ordinary_grouping_set& node, EGroupByFeatures featureContext) {
    TNodePtr namedExprNode;
    {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        namedExprNode = NamedExpr(node.GetRule_named_expr1(), EExpr::GroupBy);
    }
    if (!namedExprNode) {
        return false;
    }
    auto nodeLabel = namedExprNode->GetLabel();
    auto contentPtr = namedExprNode->ContentListPtr();
    if (contentPtr) {
        if (nodeLabel && (contentPtr->size() != 1 || contentPtr->front()->GetLabel())) {
            Ctx.Error() << "Unable to use aliases for list of named expressions";
            Ctx.IncrementMonCounter("sql_errors", "GroupByAliasForListOfExpressions");
            return false;
        }
        for (auto& content: *contentPtr) {
            auto label = content->GetLabel();
            if (!label) {
                if (content->GetColumnName()) {
                    namedExprNode->AssumeColumn();
                    continue;
                }

                if (!AllowUnnamed(content->GetPos(), featureContext)) {
                    return false;
                }

                content->SetLabel(label = GenerateGroupByExprName());
            }
            if (!AddAlias(label, content)) {
                return false;
            }
            content = BuildColumn(content->GetPos(), label);
        }
    } else {
        if (!nodeLabel && namedExprNode->GetColumnName()) {
            namedExprNode->AssumeColumn();
        }

        if (!nodeLabel && !namedExprNode->GetColumnName()) {
            if (!AllowUnnamed(namedExprNode->GetPos(), featureContext)) {
                return false;
            }
            namedExprNode->SetLabel(nodeLabel = GenerateGroupByExprName());
        }
        if (nodeLabel) {
            if (!AddAlias(nodeLabel, namedExprNode)) {
                return false;
            }
            namedExprNode = BuildColumn(namedExprNode->GetPos(), nodeLabel);
        }
    }
    GroupBySet.emplace_back(std::move(namedExprNode));
    return true;
}

bool TGroupByClause::OrdinaryGroupingSetList(const TRule_ordinary_grouping_set_list& node, EGroupByFeatures featureContext) {
    if (!OrdinaryGroupingSet(node.GetRule_ordinary_grouping_set1(), featureContext)) {
        return false;
    }
    for (auto& block: node.GetBlock2()) {
        if (!OrdinaryGroupingSet(block.GetRule_ordinary_grouping_set2(), featureContext)) {
            return false;
        }
    }
    return true;
}

bool TGroupByClause::HoppingWindow(const TRule_hopping_window_specification& node) {
    if (LegacyHoppingWindowSpec) {
        Ctx.Error() << "Duplicate hopping window specification.";
        return false;
    }
    LegacyHoppingWindowSpec = new TLegacyHoppingWindowSpec;
    {
        TColumnRefScope scope(Ctx, EColumnRefState::Allow);
        TSqlExpression expr(Ctx, Mode);
        LegacyHoppingWindowSpec->TimeExtractor = expr.Build(node.GetRule_expr3());
        if (!LegacyHoppingWindowSpec->TimeExtractor) {
            return false;
        }
    }
    auto processIntervalParam = [&] (const TRule_expr& rule) -> TNodePtr {
        TSqlExpression expr(Ctx, Mode);
        auto node = expr.Build(rule);
        if (!node) {
            return nullptr;
        }

        auto literal = node->GetLiteral("String");
        if (!literal) {
            return new TAstListNodeImpl(Ctx.Pos(), {
                new TAstAtomNodeImpl(Ctx.Pos(), "EvaluateExpr", TNodeFlags::Default),
                node
            });
        }

        const auto out = NKikimr::NMiniKQL::ValueFromString(NKikimr::NUdf::EDataSlot::Interval, *literal);
        if (!out) {
            Ctx.Error(node->GetPos()) << "Expected interval in ISO 8601 format";
            return nullptr;
        }

        if ('T' == literal->back()) {
            Ctx.Error(node->GetPos()) << "Time prefix 'T' at end of interval constant. The designator 'T' shall be absent if all of the time components are absent.";
            return nullptr;
        }

        return new TAstListNodeImpl(Ctx.Pos(), {
            new TAstAtomNodeImpl(Ctx.Pos(), "Interval", TNodeFlags::Default),
            new TAstListNodeImpl(Ctx.Pos(), {
                new TAstAtomNodeImpl(Ctx.Pos(), "quote", TNodeFlags::Default),
                new TAstAtomNodeImpl(Ctx.Pos(), ToString(out.Get<i64>()), TNodeFlags::Default)
            })
        });
    };

    LegacyHoppingWindowSpec->Hop = processIntervalParam(node.GetRule_expr5());
    if (!LegacyHoppingWindowSpec->Hop) {
        return false;
    }
    LegacyHoppingWindowSpec->Interval = processIntervalParam(node.GetRule_expr7());
    if (!LegacyHoppingWindowSpec->Interval) {
        return false;
    }
    LegacyHoppingWindowSpec->Delay = processIntervalParam(node.GetRule_expr9());
    if (!LegacyHoppingWindowSpec->Delay) {
        return false;
    }
    LegacyHoppingWindowSpec->DataWatermarks = Ctx.PragmaDataWatermarks;

    return true;
}

bool TGroupByClause::AllowUnnamed(TPosition pos, EGroupByFeatures featureContext) {
    TStringBuf feature;
    switch (featureContext) {
        case EGroupByFeatures::Ordinary:
            return true;
        case EGroupByFeatures::Rollup:
            feature = "ROLLUP";
            break;
        case EGroupByFeatures::Cube:
            feature = "CUBE";
            break;
        case EGroupByFeatures::GroupingSet:
            feature = "GROUPING SETS";
            break;
        default:
            YQL_ENSURE(false, "Unknown feature");
    }

    Ctx.Error(pos) << "Unnamed expressions are not supported in " << feature << ". Please use '<expr> AS <name>'.";
    Ctx.IncrementMonCounter("sql_errors", "GroupBySetNoAliasOrColumn");
    return false;
}

TGroupByClause::TGroupingSetFeatures& TGroupByClause::Features() {
    return GroupSetContext->GroupFeatures;
}

const TGroupByClause::TGroupingSetFeatures& TGroupByClause::Features() const {
    return GroupSetContext->GroupFeatures;
}

bool TGroupByClause::AddAlias(const TString& label, const TNodePtr& node) {
    if (Aliases().contains(label)) {
        Ctx.Error() << "Duplicated aliases not allowed";
        Ctx.IncrementMonCounter("sql_errors", "GroupByDuplicateAliases");
        return false;
    }
    Aliases().emplace(label, node);
    return true;
}

TString TGroupByClause::GenerateGroupByExprName() {
    return TStringBuilder() << AutogenerateNamePrefix << GroupSetContext->UnnamedCount++;
}

bool TSqlSelect::ValidateLimitOrderByWithSelectOp(TMaybe<TSelectKindPlacement> placement, TStringBuf what) {
    if (!placement.Defined()) {
        // not in select_op chain
        return true;
    }
    if (!Ctx.AnsiOrderByLimitInUnionAll.Defined()) {
        if (!placement->IsLastInSelectOp) {
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_LIMIT_ORDER_BY_WITH_UNION)
                << what << " will not be allowed here for ANSI compliant UNION ALL.\n"
                << "For details please consult documentation on PRAGMA AnsiOrderByLimitInUnionAll";
        } else {
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_LIMIT_ORDER_BY_WITH_UNION)
                << what << " will be applied to last subquery in UNION ALL, not to entire UNION ALL.\n"
                << "For ANSI compliant behavior please use PRAGMA AnsiOrderByLimitInUnionAll";
        }
        return true;
    }

    if (*Ctx.AnsiOrderByLimitInUnionAll && !placement->IsLastInSelectOp) {
        Ctx.Error() << what << " within UNION ALL is only allowed after last subquery";
        return false;
    }
    return true;
}

bool TSqlSelect::NeedPassLimitOrderByToUnderlyingSelect(TMaybe<TSelectKindPlacement> placement) {
    if (!Ctx.AnsiOrderByLimitInUnionAll.Defined() || !*Ctx.AnsiOrderByLimitInUnionAll) {
        return true;
    }
    return !placement.Defined() || !placement->IsLastInSelectOp;
}

TSqlSelect::TSelectKindResult TSqlSelect::SelectKind(const TRule_select_kind_partial& node, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement)
{
    auto res = SelectKind(node.GetRule_select_kind1(), selectPos, placement);
    if (!res) {
        return {};
    }
    TPosition startPos(Ctx.Pos());
    /// LIMIT INTEGER block
    TNodePtr skipTake;
    if (node.HasBlock2()) {
        auto block = node.GetBlock2();

        Token(block.GetToken1());
        TPosition pos(Ctx.Pos());

        if (!ValidateLimitOrderByWithSelectOp(placement, "LIMIT")) {
            return {};
        }

        TSqlExpression takeExpr(Ctx, Mode);
        auto take = takeExpr.Build(block.GetRule_expr2());
        if (!take) {
            return{};
        }

        TNodePtr skip;
        if (block.HasBlock3()) {
            TSqlExpression skipExpr(Ctx, Mode);
            skip = skipExpr.Build(block.GetBlock3().GetRule_expr2());
            if (!skip) {
                return {};
            }
            if (Token(block.GetBlock3().GetToken1()) == ",") {
                // LIMIT skip, take
                skip.Swap(take);
                Ctx.IncrementMonCounter("sql_features", "LimitSkipTake");
            } else {
                Ctx.IncrementMonCounter("sql_features", "LimitOffset");
            }
        }

        auto st = BuildSkipTake(pos, skip, take);
        if (NeedPassLimitOrderByToUnderlyingSelect(placement)) {
            skipTake = st;
        } else {
            res.SelectOpSkipTake = st;
        }

        Ctx.IncrementMonCounter("sql_features", "Limit");
    }

    res.Source = BuildSelect(startPos, std::move(res.Source), skipTake);
    return res;
}

TSqlSelect::TSelectKindResult TSqlSelect::SelectKind(const TRule_select_kind& node, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement)
{
    const bool discard = node.HasBlock1();
    const bool hasLabel = node.HasBlock3();
    if ((discard || hasLabel) && (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW || Mode == NSQLTranslation::ESqlMode::SUBQUERY)) {
        Ctx.Error() << "DISCARD and INTO RESULT are not allowed in current mode";
        return {};
    }

    if (discard && hasLabel) {
        Ctx.Error() << "DISCARD and INTO RESULT cannot be used at the same time";
        return {};
    }

    if (discard && !selectPos) {
        selectPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
    }

    TWriteSettings settings;
    settings.Discard = discard;
    if (hasLabel) {
        settings.Label = PureColumnOrNamed(node.GetBlock3().GetRule_pure_column_or_named3(), *this);
    }

    TSelectKindResult res;
    if (placement.Defined()) {
        if (placement->IsFirstInSelectOp) {
            res.Settings.Discard = settings.Discard;
        } else if (settings.Discard) {
            auto discardPos = Ctx.TokenPosition(node.GetBlock1().GetToken1());
            if (Ctx.AnsiOrderByLimitInUnionAll.Defined() && *Ctx.AnsiOrderByLimitInUnionAll) {
                Ctx.Error(discardPos) << "DISCARD within UNION ALL is only allowed before first subquery";
                return {};
            }
            Ctx.Warning(discardPos, TIssuesIds::YQL_LIMIT_ORDER_BY_WITH_UNION)
                << "DISCARD will be ignored here. Please use DISCARD before first subquery in UNION ALL if you want to discard entire UNION ALL result";
        }

        if (placement->IsLastInSelectOp) {
            res.Settings.Label = settings.Label;
        } else if (!settings.Label.Empty()) {
            auto labelPos = Ctx.TokenPosition(node.GetBlock3().GetToken1());
            if (Ctx.AnsiOrderByLimitInUnionAll.Defined() && *Ctx.AnsiOrderByLimitInUnionAll) {
                Ctx.Error(labelPos) << "INTO RESULT within UNION ALL is only allowed after last subquery";
                return {};
            }
            Ctx.Warning(labelPos, TIssuesIds::YQL_LIMIT_ORDER_BY_WITH_UNION)
                << "INTO RESULT will be ignored here. Please use INTO RESULT after last subquery in UNION ALL if you want label entire UNION ALL result";
        }

        settings = {};
    }

    switch (node.GetBlock2().Alt_case()) {
        case TRule_select_kind_TBlock2::kAlt1:
            res.Source = ProcessCore(node.GetBlock2().GetAlt1().GetRule_process_core1(), settings, selectPos);
            break;
        case TRule_select_kind_TBlock2::kAlt2:
            res.Source = ReduceCore(node.GetBlock2().GetAlt2().GetRule_reduce_core1(), settings, selectPos);
            break;
        case TRule_select_kind_TBlock2::kAlt3: {
            res.Source = SelectCore(node.GetBlock2().GetAlt3().GetRule_select_core1(), settings, selectPos,
                placement, res.SelectOpOrderBy, res.SelectOpAssumeOrderBy);
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    return res;
}

TSqlSelect::TSelectKindResult TSqlSelect::SelectKind(const TRule_select_kind_parenthesis& node, TPosition& selectPos,
    TMaybe<TSelectKindPlacement> placement)
{
    if (node.Alt_case() == TRule_select_kind_parenthesis::kAltSelectKindParenthesis1) {
        return SelectKind(node.GetAlt_select_kind_parenthesis1().GetRule_select_kind_partial1(), selectPos, placement);
    } else {
        return SelectKind(node.GetAlt_select_kind_parenthesis2().GetRule_select_kind_partial2(), selectPos, {});
    }
}

template<typename TRule>
TSourcePtr TSqlSelect::Build(const TRule& node, TPosition pos, TSelectKindResult&& first) {
    TPosition unionPos = pos; // Position of first select
    TVector<TSourcePtr> sources;
    sources.emplace_back(std::move(first.Source));

    TVector<TSortSpecificationPtr> orderBy;
    TNodePtr skipTake;
    TWriteSettings settings;
    settings.Discard = first.Settings.Discard;
    bool assumeOrderBy = false;
    auto blocks = node.GetBlock2();
    for (int i = 0; i < blocks.size(); ++i) {
        auto& b = blocks[i];
        const bool last = (i + 1 == blocks.size());
        TSelectKindPlacement placement;
        placement.IsLastInSelectOp = last;

        TSelectKindResult next = SelectKind(b.GetRule_select_kind_parenthesis2(), pos, placement);
        if (!next) {
            return nullptr;
        }

        if (last) {
            orderBy = next.SelectOpOrderBy;
            assumeOrderBy = next.SelectOpAssumeOrderBy;
            skipTake = next.SelectOpSkipTake;
            settings.Label = next.Settings.Label;
        }

        switch (b.GetRule_select_op1().Alt_case()) {
            case TRule_select_op::kAltSelectOp1: {
                const bool isUnionAll = b.GetRule_select_op1().GetAlt_select_op1().HasBlock2();
                if (!isUnionAll) {
                    Token(b.GetRule_select_op1().GetAlt_select_op1().GetToken1());
                    Ctx.Error() << "UNION without quantifier ALL is not supported yet. Did you mean UNION ALL?";
                    return nullptr;
                } else {
                    sources.emplace_back(std::move(next.Source));
                }
                break;
            }
            default:
                Ctx.Error() << "INTERSECT and EXCEPT are not implemented yet";
                return nullptr;
        }
    }

    if (sources.size() == 1) {
        return std::move(sources[0]);
    }

    TSourcePtr result;
    if (orderBy) {
        result = BuildUnionAll(unionPos, std::move(sources), {});

        TVector<TNodePtr> groupByExpr;
        TVector<TNodePtr> groupBy;
        bool compactGroupBy = false;
        TString groupBySuffix = "";
        TNodePtr having;
        TWinSpecs winSpecs;
        TLegacyHoppingWindowSpecPtr legacyHoppingWindowSpec;
        bool distinct = false;
        TVector<TNodePtr> without;
        bool stream = false;

        TVector<TNodePtr> terms;
        terms.push_back(BuildColumn(unionPos, "*", ""));

        result = BuildSelectCore(Ctx, unionPos, std::move(result), groupByExpr, groupBy, compactGroupBy, groupBySuffix,
            assumeOrderBy, orderBy, having, std::move(winSpecs), legacyHoppingWindowSpec, std::move(terms),
            distinct, std::move(without), stream, settings);
    } else {
        result = BuildUnionAll(unionPos, std::move(sources), settings);
    }

    if (skipTake || orderBy) {
        result = BuildSelect(unionPos, std::move(result), skipTake);
    }
    return result;
}

TSourcePtr TSqlSelect::Build(const TRule_select_stmt& node, TPosition& selectPos) {
    TMaybe<TSelectKindPlacement> placement;
    if (!node.GetBlock2().empty()) {
        placement.ConstructInPlace();
        placement->IsFirstInSelectOp = true;
    }

    auto res = SelectKind(node.GetRule_select_kind_parenthesis1(), selectPos, placement);
    if (!res) {
        return nullptr;
    }

    return Build(node, selectPos, std::move(res));
}

TSourcePtr TSqlSelect::Build(const TRule_select_unparenthesized_stmt& node, TPosition& selectPos) {
    TMaybe<TSelectKindPlacement> placement;
    if (!node.GetBlock2().empty()) {
        placement.ConstructInPlace();
        placement->IsFirstInSelectOp = true;
    }

    auto res = SelectKind(node.GetRule_select_kind_partial1(), selectPos, placement);
    if (!res) {
        return nullptr;
    }

    return Build(node, selectPos, std::move(res));
}

TSourcePtr TSqlValues::Build(const TRule_values_stmt& node, TPosition& valuesPos, const TVector<TString>& derivedColumns, TPosition derivedColumnsPos) {
    Token(node.GetToken1());
    valuesPos = Ctx.Pos();

    TVector<TVector<TNodePtr>> rows;
    const auto& rowList = node.GetRule_values_source_row_list2();
    if (!BuildRows(rowList, rows)) {
        return nullptr;
    }

    YQL_ENSURE(!rows.empty());
    const size_t columnsCount = rows.back().size();
    if (derivedColumns.size() > columnsCount) {
        Ctx.Error(derivedColumnsPos) << "Derived column list size exceeds column count in VALUES";
        return nullptr;
    }

    auto columns = derivedColumns;
    if (Ctx.WarnUnnamedColumns && columns.size() < columnsCount) {
        Ctx.Warning(valuesPos, TIssuesIds::YQL_UNNAMED_COLUMN)
            << "Autogenerated column names column" << columns.size() << "...column" << columnsCount - 1 << " will be used here";
    }

    while (columns.size() < columnsCount) {
        columns.push_back(TStringBuilder() << "column" << columns.size());
    }

    TVector<TNodePtr> labels;
    for (size_t i = 0; i < columnsCount; ++i) {
        labels.push_back(BuildQuotedAtom(derivedColumnsPos, columns[i]));
    }

    TVector<TNodePtr> items;
    for (auto& row : rows) {
        YQL_ENSURE(!row.empty());
        YQL_ENSURE(row.size() == columnsCount);
        items.push_back(BuildOrderedStructure(row.front()->GetPos(), row, labels));
    }
    auto list = new TCallNodeImpl(valuesPos, "AsList", items);
    list = new TCallNodeImpl(valuesPos, "PersistableRepr", { list });
    list = new TCallNodeImpl(valuesPos, "AssumeColumnOrder", { list, BuildTuple(valuesPos, labels) });
    auto result = BuildNodeSource(valuesPos, list, false);
    result->AllColumns();
    return result;
}

bool TSqlValues::BuildRows(const TRule_values_source_row_list& node, TVector<TVector<TNodePtr>>& rows) {
    rows = TVector<TVector<TNodePtr>> {{}};


    if (!BuildRow(node.GetRule_values_source_row1(), rows.back())) {
        return false;
    }

    const size_t rowSize = rows.back().size();

    for (const auto& valuesSourceRow: node.GetBlock2()) {
        rows.push_back({});
        if (!BuildRow(valuesSourceRow.GetRule_values_source_row2(), rows.back())) {
            return false;
        }
        if (rows.back().size() != rowSize) {
            Token(valuesSourceRow.GetRule_values_source_row2().GetToken1());
            Error() << "All VALUES items should have same size: expecting " << rowSize << ", got " << rows.back().size();
            return false;
        }
    }
    return true;
}

bool TSqlValues::BuildRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow) {
    TSqlExpression sqlExpr(Ctx, Mode);
    return ExprList(sqlExpr, outRow, inRow.GetRule_expr_list2());
}

class TSqlIntoValues: public TSqlValues {
public:
    TSqlIntoValues(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlValues(ctx, mode)
    {
    }

    TSourcePtr Build(const TRule_into_values_source& node, const TString& operationName);

private:
    TSourcePtr ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint,
        const TString& operationName);
};

TSourcePtr TSqlIntoValues::Build(const TRule_into_values_source& node, const TString& operationName) {
    switch (node.Alt_case()) {
        case TRule_into_values_source::kAltIntoValuesSource1: {
            auto alt = node.GetAlt_into_values_source1();
            TVector<TString> columnsHint;
            if (alt.HasBlock1()) {
                PureColumnListStr(alt.GetBlock1().GetRule_pure_column_list1(), *this, columnsHint);
            }
            return ValuesSource(alt.GetRule_values_source2(), columnsHint, operationName);
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "DefaultValuesOrOther");
            AltNotImplemented("into_values_source", node);
            return nullptr;
    }
}

TSourcePtr TSqlIntoValues::ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint,
    const TString& operationName)
{
    Ctx.IncrementMonCounter("sql_features", "ValuesSource");
    TPosition pos(Ctx.Pos());
    switch (node.Alt_case()) {
        case TRule_values_source::kAltValuesSource1: {
            TVector<TVector<TNodePtr>> rows {{}};
            const auto& rowList = node.GetAlt_values_source1().GetRule_values_stmt1().GetRule_values_source_row_list2();
            if (!BuildRows(rowList, rows)) {
                return nullptr;
            }
            return BuildWriteValues(pos, operationName, columnsHint, rows);
        }
        case TRule_values_source::kAltValuesSource2: {
            TSqlSelect select(Ctx, Mode);
            TPosition selectPos;
            auto source = select.Build(node.GetAlt_values_source2().GetRule_select_stmt1(), selectPos);
            if (!source) {
                return nullptr;
            }
            return BuildWriteValues(pos, "UPDATE", columnsHint, std::move(source));
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownValuesSource");
            AltNotImplemented("values_source", node);
            return nullptr;
    }
}

class TSqlIntoTable: public TSqlTranslation {
public:
    TSqlIntoTable(TContext& ctx, NSQLTranslation::ESqlMode mode)
        : TSqlTranslation(ctx, mode)
    {
    }

    TNodePtr Build(const TRule_into_table_stmt& node);

private:
    //bool BuildValuesRow(const TRule_values_source_row& inRow, TVector<TNodePtr>& outRow);
    //TSourcePtr ValuesSource(const TRule_values_source& node, TVector<TString>& columnsHint);
    //TSourcePtr IntoValuesSource(const TRule_into_values_source& node);

    bool ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table, ESQLWriteColumnMode mode,
        const TPosition& pos);
    TString SqlIntoModeStr;
    TString SqlIntoUserModeStr;
};

TNodePtr TSqlIntoTable::Build(const TRule_into_table_stmt& node) {
    static const TMap<TString, ESQLWriteColumnMode> str2Mode = {
        {"InsertInto", ESQLWriteColumnMode::InsertInto},
        {"InsertOrAbortInto", ESQLWriteColumnMode::InsertOrAbortInto},
        {"InsertOrIgnoreInto", ESQLWriteColumnMode::InsertOrIgnoreInto},
        {"InsertOrRevertInto", ESQLWriteColumnMode::InsertOrRevertInto},
        {"UpsertInto", ESQLWriteColumnMode::UpsertInto},
        {"ReplaceInto", ESQLWriteColumnMode::ReplaceInto},
        {"InsertIntoWithTruncate", ESQLWriteColumnMode::InsertIntoWithTruncate}
    };

    auto& modeBlock = node.GetBlock1();

    TVector<TToken> modeTokens;
    switch (modeBlock.Alt_case()) {
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt1:
            modeTokens = {modeBlock.GetAlt1().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt2:
            modeTokens = {
                modeBlock.GetAlt2().GetToken1(),
                modeBlock.GetAlt2().GetToken2(),
                modeBlock.GetAlt2().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt3:
            modeTokens = {
                modeBlock.GetAlt3().GetToken1(),
                modeBlock.GetAlt3().GetToken2(),
                modeBlock.GetAlt3().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt4:
            modeTokens = {
                modeBlock.GetAlt4().GetToken1(),
                modeBlock.GetAlt4().GetToken2(),
                modeBlock.GetAlt4().GetToken3()
            };
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt5:
            modeTokens = {modeBlock.GetAlt5().GetToken1()};
            break;
        case TRule_into_table_stmt_TBlock1::AltCase::kAlt6:
            modeTokens = {modeBlock.GetAlt6().GetToken1()};
            break;
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    TVector<TString> modeStrings;
    modeStrings.reserve(modeTokens.size());
    TVector<TString> userModeStrings;
    userModeStrings.reserve(modeTokens.size());

    for (auto& token : modeTokens) {
        auto tokenStr = Token(token);

        auto modeStr = tokenStr;
        modeStr.to_lower();
        modeStr.to_upper(0, 1);
        modeStrings.push_back(modeStr);

        auto userModeStr = tokenStr;
        userModeStr.to_upper();
        userModeStrings.push_back(userModeStr);
    }

    modeStrings.push_back("Into");
    userModeStrings.push_back("INTO");

    SqlIntoModeStr = JoinRange("", modeStrings.begin(), modeStrings.end());
    SqlIntoUserModeStr = JoinRange(" ", userModeStrings.begin(), userModeStrings.end());

    const auto& intoTableRef = node.GetRule_into_simple_table_ref3();
    const auto& tableRef = intoTableRef.GetRule_simple_table_ref1();
    const auto& tableRefCore = tableRef.GetRule_simple_table_ref_core1();

    auto service = Ctx.Scoped->CurrService;
    auto cluster = Ctx.Scoped->CurrCluster;
    std::pair<bool, TDeferredAtom> nameOrAt;
    bool isBinding = false;
    switch (tableRefCore.Alt_case()) {
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore1: {
            if (tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().HasBlock1()) {
                const auto& clusterExpr = tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetBlock1().GetRule_cluster_expr1();
                bool hasAt = tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2().HasBlock1();
                bool result = !hasAt ?
                    ClusterExprOrBinding(clusterExpr, service, cluster, isBinding) : ClusterExpr(clusterExpr, false, service, cluster);
                if (!result) {
                    return nullptr;
                }
            }

            if (!isBinding && cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return nullptr;
            }

            auto id = Id(tableRefCore.GetAlt_simple_table_ref_core1().GetRule_object_ref1().GetRule_id_or_at2(), *this);
            nameOrAt = std::make_pair(id.first, TDeferredAtom(Ctx.Pos(), id.second));
            break;
        }
        case TRule_simple_table_ref_core::AltCase::kAltSimpleTableRefCore2: {
            auto at = tableRefCore.GetAlt_simple_table_ref_core2().HasBlock1();
            TString name;
            if (!NamedNodeImpl(tableRefCore.GetAlt_simple_table_ref_core2().GetRule_bind_parameter2(), name, *this)) {
                return nullptr;
            }
            auto named = GetNamedNode(name);
            if (!named) {
                return nullptr;
            }

            if (cluster.Empty()) {
                Ctx.Error() << "No cluster name given and no default cluster is selected";
                return nullptr;
            }

            TDeferredAtom table;
            MakeTableFromExpression(Ctx, named, table);
            nameOrAt = std::make_pair(at, table);
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }

    bool withTruncate = false;
    TTableHints tableHints;
    if (tableRef.HasBlock2()) {
        auto hints = TableHintsImpl(tableRef.GetBlock2().GetRule_table_hints1());
        if (!hints) {
            Ctx.Error() << "Failed to parse table hints";
            return nullptr;
        }
        for (const auto& hint : *hints) {
            if (to_upper(hint.first) == "TRUNCATE") {
                withTruncate = true;
            }
        }
        std::erase_if(*hints, [](const auto &hint) { return to_upper(hint.first) == "TRUNCATE"; });
        tableHints = std::move(*hints);
    }

    TVector<TString> eraseColumns;
    if (intoTableRef.HasBlock2()) {
        if (service != StatProviderName) {
            Ctx.Error() << "ERASE BY is unsupported for " << service;
            return nullptr;
        }

        PureColumnListStr(
            intoTableRef.GetBlock2().GetRule_pure_column_list3(), *this, eraseColumns
        );
    }

    if (withTruncate) {
        if (SqlIntoModeStr != "InsertInto") {
            Error() << "Unable " << SqlIntoUserModeStr << " with truncate mode";
            return nullptr;
        }
        SqlIntoModeStr += "WithTruncate";
        SqlIntoUserModeStr += " ... WITH TRUNCATE";
    }
    const auto iterMode = str2Mode.find(SqlIntoModeStr);
    YQL_ENSURE(iterMode != str2Mode.end(), "Invalid sql write mode string: " << SqlIntoModeStr);
    const auto SqlIntoMode = iterMode->second;

    TPosition pos(Ctx.Pos());
    TTableRef table(Ctx.MakeName("table"), service, cluster, nullptr);
    if (isBinding) {
        const TString* binding = nameOrAt.second.GetLiteral();
        YQL_ENSURE(binding);
        YQL_ENSURE(!nameOrAt.first);
        if (!ApplyTableBinding(*binding, table, tableHints)) {
            return nullptr;
        }
    } else {
        table.Keys = BuildTableKey(pos, service, cluster, nameOrAt.second, nameOrAt.first ? "@" : "");
    }

    Ctx.IncrementMonCounter("sql_insert_clusters", table.Cluster.GetLiteral() ? *table.Cluster.GetLiteral() : "unknown");

    auto values = TSqlIntoValues(Ctx, Mode).Build(node.GetRule_into_values_source4(), SqlIntoUserModeStr);
    if (!values) {
        return nullptr;
    }
    if (!ValidateServiceName(node, table, SqlIntoMode, GetPos(modeTokens[0]))) {
        return nullptr;
    }
    Ctx.IncrementMonCounter("sql_features", SqlIntoModeStr);

    return BuildWriteColumns(pos, Ctx.Scoped, table,
                             ToWriteColumnsMode(SqlIntoMode), std::move(values),
                             BuildIntoTableOptions(pos, eraseColumns, tableHints));
}

bool TSqlIntoTable::ValidateServiceName(const TRule_into_table_stmt& node, const TTableRef& table,
    ESQLWriteColumnMode mode, const TPosition& pos) {
    Y_UNUSED(node);
    auto serviceName = table.Service;
    const bool isMapReduce = serviceName == YtProviderName;
    const bool isKikimr = serviceName == KikimrProviderName || serviceName == YdbProviderName;
    const bool isRtmr = serviceName == RtmrProviderName;
    const bool isStat = serviceName == StatProviderName;

    if (!isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertOrAbortInto ||
            mode == ESQLWriteColumnMode::InsertOrIgnoreInto ||
            mode == ESQLWriteColumnMode::InsertOrRevertInto ||
            mode == ESQLWriteColumnMode::UpsertInto && !isStat)
        {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is not supported for " << serviceName << " tables";
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    if (isMapReduce) {
        if (mode == ESQLWriteColumnMode::ReplaceInto) {
            Ctx.Error(pos) << "Meaning of REPLACE INTO has been changed, now you should use INSERT INTO <table> WITH TRUNCATE ... for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", "ReplaceIntoConflictUsage");
            return false;
        }
    } else if (isKikimr) {
        if (mode == ESQLWriteColumnMode::InsertIntoWithTruncate) {
            Ctx.Error(pos) << "INSERT INTO WITH TRUNCATE is not supported for " << serviceName << " tables";
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isRtmr) {
        if (mode != ESQLWriteColumnMode::InsertInto) {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is unsupported for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    } else if (isStat) {
        if (mode != ESQLWriteColumnMode::UpsertInto) {
            Ctx.Error(pos) << SqlIntoUserModeStr << " is unsupported for " << serviceName;
            Ctx.IncrementMonCounter("sql_errors", TStringBuilder() << SqlIntoUserModeStr << "UnsupportedFor" << serviceName);
            return false;
        }
    }

    return true;
}

class TSqlQuery: public TSqlTranslation {
public:
    TSqlQuery(TContext& ctx, NSQLTranslation::ESqlMode mode, bool topLevel)
        : TSqlTranslation(ctx, mode)
        , TopLevel(topLevel)
    {
    }

    TNodePtr Build(const TSQLv1ParserAST& ast);

    bool Statement(TVector<TNodePtr>& blocks, const TRule_sql_stmt_core& core);
private:
    bool DeclareStatement(const TRule_declare_stmt& stmt);
    bool ExportStatement(const TRule_export_stmt& stmt);
    bool AlterTableAction(const TRule_alter_table_action& node, TAlterTableParameters& params);
    bool AlterTableAddColumn(const TRule_alter_table_add_column& node, TAlterTableParameters& params);
    bool AlterTableDropColumn(const TRule_alter_table_drop_column& node, TAlterTableParameters& params);
    bool AlterTableAlterColumn(const TRule_alter_table_alter_column& node, TAlterTableParameters& params);
    bool AlterTableAddFamily(const TRule_family_entry& node, TAlterTableParameters& params);
    bool AlterTableAlterFamily(const TRule_alter_table_alter_column_family& node, TAlterTableParameters& params);
    bool AlterTableSetTableSetting(const TRule_alter_table_set_table_setting_uncompat& node, TAlterTableParameters& params);
    bool AlterTableSetTableSetting(const TRule_alter_table_set_table_setting_compat& node, TAlterTableParameters& params);
    bool AlterTableResetTableSetting(const TRule_alter_table_reset_table_setting& node, TAlterTableParameters& params);
    bool AlterTableAddIndex(const TRule_alter_table_add_index& node, TAlterTableParameters& params);
    void AlterTableDropIndex(const TRule_alter_table_drop_index& node, TAlterTableParameters& params);
    void AlterTableRenameTo(const TRule_alter_table_rename_to& node, TAlterTableParameters& params);
    bool AlterTableAddChangefeed(const TRule_alter_table_add_changefeed& node, TAlterTableParameters& params);
    bool AlterTableAlterChangefeed(const TRule_alter_table_alter_changefeed& node, TAlterTableParameters& params);
    void AlterTableDropChangefeed(const TRule_alter_table_drop_changefeed& node, TAlterTableParameters& params);
    void AlterTableRenameIndexTo(const TRule_alter_table_rename_index_to& node, TAlterTableParameters& params);
    TNodePtr PragmaStatement(const TRule_pragma_stmt& stmt, bool& success);
    void AddStatementToBlocks(TVector<TNodePtr>& blocks, TNodePtr node);

    TNodePtr Build(const TRule_delete_stmt& stmt);

    TNodePtr Build(const TRule_update_stmt& stmt);
    TSourcePtr Build(const TRule_set_clause_choice& stmt);
    bool FillSetClause(const TRule_set_clause& node, TVector<TString>& targetList, TVector<TNodePtr>& values);
    TSourcePtr Build(const TRule_set_clause_list& stmt);
    TSourcePtr Build(const TRule_multiple_column_assignment& stmt);

    template<class TNode>
    void ParseStatementName(const TNode& node, TString& internalStatementName, TString& humanStatementName) {
        internalStatementName.clear();
        humanStatementName.clear();
        const auto& descr = AltDescription(node);
        TVector<TString> parts;
        const auto pos = descr.find(": ");
        Y_VERIFY_DEBUG(pos != TString::npos);
        Split(TString(descr.begin() + pos + 2, descr.end()), "_", parts);
        Y_VERIFY_DEBUG(parts.size() > 1);
        parts.pop_back();
        for (auto& part: parts) {
            part.to_upper(0, 1);
            internalStatementName += part;
            if (!humanStatementName.empty()) {
                humanStatementName += ' ';
            }
            humanStatementName += to_upper(part);
        }
    }

    const bool TopLevel;
};

TNodePtr TSqlTranslation::DoStatement(const TRule_do_stmt& stmt, bool makeLambda, const TVector<TString>& args) {
    switch (stmt.GetBlock2().Alt_case()) {
    case TRule_do_stmt_TBlock2::kAlt1: {
        const auto& callAction = stmt.GetBlock2().GetAlt1().GetRule_call_action1();
        TNodePtr action;
        switch (callAction.GetBlock1().GetAltCase()) {
        case TRule_call_action_TBlock1::kAlt1: {
            TString bindName;
            if (!NamedNodeImpl(callAction.GetBlock1().GetAlt1().GetRule_bind_parameter1(), bindName, *this)) {
                return nullptr;
            }
            action = GetNamedNode(bindName);
            if (!action) {
                return nullptr;
            }
            break;
        }
        case TRule_call_action_TBlock1::kAlt2:
            action = BuildEmptyAction(Ctx.Pos());
            break;
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownDoStmt");
            AltNotImplemented("do_stmt", callAction.GetBlock1());
            return nullptr;
        }

        TVector<TNodePtr> values;
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "Apply", TNodeFlags::Default));
        values.push_back(action);
        values.push_back(new TAstAtomNodeImpl(Ctx.Pos(), "world", TNodeFlags::Default));

        TSqlExpression sqlExpr(Ctx, Mode);
        if (callAction.HasBlock3() && !ExprList(sqlExpr, values, callAction.GetBlock3().GetRule_expr_list1())) {
            return nullptr;
        }

        TNodePtr apply = new TAstListNodeImpl(Ctx.Pos(), std::move(values));
        if (!makeLambda) {
            return BuildDoCall(Ctx.Pos(), apply);
        }

        TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
        params->Add("world");
        for (const auto& arg : args) {
            params->Add(new TAstAtomNodeImpl(Ctx.Pos(), arg, TNodeFlags::ArbitraryContent));
        }

        return BuildDoCall(Ctx.Pos(), BuildLambda(Ctx.Pos(), params, apply));
    }
    case TRule_do_stmt_TBlock2::kAlt2: {
        const auto& inlineAction = stmt.GetBlock2().GetAlt2().GetRule_inline_action1();
        const auto& body = inlineAction.GetRule_define_action_or_subquery_body2();

        auto saveScoped = Ctx.Scoped;
        Ctx.Scoped = MakeIntrusive<TScopedState>();
        Ctx.AllScopes.push_back(Ctx.Scoped);
        *Ctx.Scoped = *saveScoped;
        Ctx.Scoped->Local = TScopedState::TLocal{};
        Ctx.ScopeLevel++;
        TSqlQuery query(Ctx, Ctx.Settings.Mode, false);
        TBlocks innerBlocks;

        const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, body);
        auto ret = hasValidBody ? BuildQuery(Ctx.Pos(), innerBlocks, false, Ctx.Scoped) : nullptr;
        WarnUnusedNodes();
        Ctx.ScopeLevel--;
        Ctx.Scoped = saveScoped;

        if (!ret) {
            return {};
        }

        TNodePtr blockNode = new TAstListNodeImpl(Ctx.Pos());
        blockNode->Add("block");
        blockNode->Add(blockNode->Q(ret));
        if (!makeLambda) {
            return blockNode;
        }

        TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
        params->Add("world");
        for (const auto& arg : args) {
            params->Add(new TAstAtomNodeImpl(Ctx.Pos(), arg, TNodeFlags::ArbitraryContent));
        }

        return BuildLambda(Ctx.Pos(), params, blockNode);
    }
    default:
        Y_FAIL("You should change implementation according to grammar changes");
    }
}

void TSqlQuery::AddStatementToBlocks(TVector<TNodePtr>& blocks, TNodePtr node) {
    blocks.emplace_back(node);
}

static bool AsyncReplicationSettingsEntry(std::map<TString, TNodePtr>& out, const TRule_replication_settings_entry& in, TTranslation& ctx) {
    auto key = Id(in.GetRule_an_id1(), ctx);
    auto value = BuildLiteralSmartString(ctx.Context(), ctx.Token(in.GetToken3()));
    // TODO(ilnaz): validate
    out.emplace(std::move(key), value);
    return true;
}

static bool AsyncReplicationSettings(std::map<TString, TNodePtr>& out, const TRule_replication_settings& in, TTranslation& ctx) {
    if (!AsyncReplicationSettingsEntry(out, in.GetRule_replication_settings_entry1(), ctx)) {
        return false;
    }

    for (auto& block : in.GetBlock2()) {
        if (!AsyncReplicationSettingsEntry(out, block.GetRule_replication_settings_entry2(), ctx)) {
            return false;
        }
    }

    return true;
}

static bool AsyncReplicationTarget(std::vector<std::pair<TString, TString>>& out, const TRule_replication_target& in, TTranslation& ctx) {
    const TString remote = Id(in.GetRule_object_ref1().GetRule_id_or_at2(), ctx).second;
    const TString local = Id(in.GetRule_object_ref3().GetRule_id_or_at2(), ctx).second;
    out.emplace_back(remote, local);
    return true;
}

bool TSqlQuery::Statement(TVector<TNodePtr>& blocks, const TRule_sql_stmt_core& core) {
    TString internalStatementName;
    TString humanStatementName;
    ParseStatementName(core, internalStatementName, humanStatementName);
    const auto& altCase = core.Alt_case();
    if (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW && (altCase >= TRule_sql_stmt_core::kAltSqlStmtCore4 &&
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore13)) {
        Error() << humanStatementName << " statement is not supported in limited views";
        return false;
    }

    if (Mode == NSQLTranslation::ESqlMode::SUBQUERY && (altCase >= TRule_sql_stmt_core::kAltSqlStmtCore4 &&
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore13 && altCase != TRule_sql_stmt_core::kAltSqlStmtCore6 &&
        altCase != TRule_sql_stmt_core::kAltSqlStmtCore17)) {
        Error() << humanStatementName << " statement is not supported in subqueries";
        return false;
    }

    switch (altCase) {
        case TRule_sql_stmt_core::kAltSqlStmtCore1: {
            bool success = false;
            TNodePtr nodeExpr = PragmaStatement(core.GetAlt_sql_stmt_core1().GetRule_pragma_stmt1(), success);
            if (!success) {
                return false;
            }
            if (nodeExpr) {
                AddStatementToBlocks(blocks, nodeExpr);
            }
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore2: {
            Ctx.BodyPart();
            TSqlSelect select(Ctx, Mode);
            TPosition pos;
            auto source = select.Build(core.GetAlt_sql_stmt_core2().GetRule_select_stmt1(), pos);
            if (!source) {
                return false;
            }
            blocks.emplace_back(BuildSelectResult(pos, std::move(source),
                Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW && Mode != NSQLTranslation::ESqlMode::SUBQUERY, Mode == NSQLTranslation::ESqlMode::SUBQUERY,
                Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore3: {
            Ctx.BodyPart();
            TVector<TSymbolNameWithPos> names;
            auto nodeExpr = NamedNode(core.GetAlt_sql_stmt_core3().GetRule_named_nodes_stmt1(), names);
            if (!nodeExpr) {
                return false;
            }
            TVector<TNodePtr> nodes;
            auto subquery = nodeExpr->GetSource();
            if (subquery && Mode == NSQLTranslation::ESqlMode::LIBRARY && Ctx.ScopeLevel == 0) {
                for (size_t i = 0; i < names.size(); ++i) {
                    nodes.push_back(BuildInvalidSubqueryRef(subquery->GetPos()));
                }
            } else if (subquery) {
                const auto alias = Ctx.MakeName("subquerynode");
                const auto ref = Ctx.MakeName("subquery");
                blocks.push_back(BuildSubquery(subquery, alias,
                    Mode == NSQLTranslation::ESqlMode::SUBQUERY, names.size() == 1 ? -1 : names.size(), Ctx.Scoped));
                blocks.back()->SetLabel(ref);

                for (size_t i = 0; i < names.size(); ++i) {
                    nodes.push_back(BuildSubqueryRef(blocks.back(), ref, names.size() == 1 ? -1 : i));
                }
            } else {
                if (names.size() > 1) {
                    auto tupleRes = BuildTupleResult(nodeExpr, names.size());
                    for (size_t i = 0; i < names.size(); ++i) {
                        nodes.push_back(nodeExpr->Y("Nth", tupleRes, nodeExpr->Q(ToString(i))));
                    }
                } else {
                    nodes.push_back(std::move(nodeExpr));
                }
            }

            for (size_t i = 0; i < names.size(); ++i) {
                PushNamedNode(names[i].Pos, names[i].Name, nodes[i]);
            }
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore4: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core4().GetRule_create_table_stmt1();
            const auto& block = rule.GetBlock2();
            ETableType tableType = ETableType::Table;
            if (block.HasAlt2() && block.GetAlt2().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_TABLESTORE) {
                tableType = ETableType::TableStore;
            } else if (block.HasAlt3() && block.GetAlt3().GetToken1().GetId() == SQLv1LexerTokens::TOKEN_EXTERNAL) {
                tableType = ETableType::ExternalTable;
            }

            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref3(), tr)) {
                return false;
            }

            TCreateTableParameters params{.TableType=tableType};
            if (!CreateTableEntry(rule.GetRule_create_table_entry5(), params)) {
                return false;
            }
            for (auto& block: rule.GetBlock6()) {
                if (!CreateTableEntry(block.GetRule_create_table_entry2(), params)) {
                    return false;
                }
            }

            if (rule.HasBlock8()) {
                Context().Error(GetPos(rule.GetBlock8().GetRule_table_inherits1().GetToken1()))
                    << "INHERITS clause is not supported yet";
                return false;
            }

            if (rule.HasBlock9()) {
                if (tableType == ETableType::TableStore) {
                    Context().Error(GetPos(rule.GetBlock9().GetRule_table_partition_by1().GetToken1()))
                        << "PARTITION BY is not supported for TABLESTORE";
                    return false;
                }
                const auto list = rule.GetBlock9().GetRule_table_partition_by1().GetRule_pure_column_list4();
                params.PartitionByColumns.push_back(IdEx(list.GetRule_an_id2(), *this));
                for (auto& node : list.GetBlock3()) {
                    params.PartitionByColumns.push_back(IdEx(node.GetRule_an_id2(), *this));
                }
            }

            if (rule.HasBlock10()) {
                if (!CreateTableSettings(rule.GetBlock10().GetRule_with_table_settings1(), params)) {
                    return false;
                }
            }

            if (rule.HasBlock11()) {
                Context().Error(GetPos(rule.GetBlock11().GetRule_table_tablestore1().GetToken1()))
                    << "TABLESTORE clause is not supported yet";
                return false;
            }

            if (!ValidateExternalTable(params)) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildCreateTable(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore5: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core5().GetRule_drop_table_stmt1();
            const auto& block = rule.GetBlock2();
            ETableType tableType = ETableType::Table;
            if (block.HasAlt2()) {
                tableType = ETableType::TableStore;
            }
            if (block.HasAlt3()) {
                tableType = ETableType::ExternalTable;
            }

            if (rule.HasBlock3()) {
                Context().Error(GetPos(rule.GetToken1())) << "IF EXISTS in " << humanStatementName
                    << " is not supported.";
                return false;
            }
            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref4(), tr)) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildDropTable(Ctx.Pos(), tr, tableType, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore6: {
            const auto& rule = core.GetAlt_sql_stmt_core6().GetRule_use_stmt1();
            Token(rule.GetToken1());
            if (!ClusterExpr(rule.GetRule_cluster_expr2(), true, Ctx.Scoped->CurrService, Ctx.Scoped->CurrCluster)) {
                return false;
            }

            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore7: {
            Ctx.BodyPart();
            TSqlIntoTable intoTable(Ctx, Mode);
            TNodePtr block(intoTable.Build(core.GetAlt_sql_stmt_core7().GetRule_into_table_stmt1()));
            if (!block) {
                return false;
            }
            blocks.emplace_back(block);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore8: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core8().GetRule_commit_stmt1();
            Token(rule.GetToken1());
            blocks.emplace_back(BuildCommitClusters(Ctx.Pos()));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore9: {
            Ctx.BodyPart();
            auto updateNode = Build(core.GetAlt_sql_stmt_core9().GetRule_update_stmt1());
            if (!updateNode) {
                return false;
            }
            AddStatementToBlocks(blocks, updateNode);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore10: {
            Ctx.BodyPart();
            auto deleteNode = Build(core.GetAlt_sql_stmt_core10().GetRule_delete_stmt1());
            if (!deleteNode) {
                return false;
            }
            blocks.emplace_back(deleteNode);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore11: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core11().GetRule_rollback_stmt1();
            Token(rule.GetToken1());
            blocks.emplace_back(BuildRollbackClusters(Ctx.Pos()));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore12:
            if (!DeclareStatement(core.GetAlt_sql_stmt_core12().GetRule_declare_stmt1())) {
                return false;
            }
            break;
        case TRule_sql_stmt_core::kAltSqlStmtCore13:
            if (!ImportStatement(core.GetAlt_sql_stmt_core13().GetRule_import_stmt1())) {
                return false;
            }
            break;
        case TRule_sql_stmt_core::kAltSqlStmtCore14:
            if (!ExportStatement(core.GetAlt_sql_stmt_core14().GetRule_export_stmt1())) {
                return false;
            }
            break;
        case TRule_sql_stmt_core::kAltSqlStmtCore15: {
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core15().GetRule_alter_table_stmt1();
            const bool isTablestore = rule.GetToken2().GetId() == SQLv1LexerTokens::TOKEN_TABLESTORE;
            TTableRef tr;
            if (!SimpleTableRefImpl(rule.GetRule_simple_table_ref3(), tr)) {
                return false;
            }

            TAlterTableParameters params;
            if (isTablestore) {
                params.TableType = ETableType::TableStore;
            }
            if (!AlterTableAction(rule.GetRule_alter_table_action4(), params)) {
                return false;
            }

            for (auto& block : rule.GetBlock5()) {
                if (!AlterTableAction(block.GetRule_alter_table_action2(), params)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterTable(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore16: {
            Ctx.BodyPart();
            auto node = DoStatement(core.GetAlt_sql_stmt_core16().GetRule_do_stmt1(), false);
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore17: {
            Ctx.BodyPart();
            if (!DefineActionOrSubqueryStatement(core.GetAlt_sql_stmt_core17().GetRule_define_action_or_subquery_stmt1())) {
                return false;
            }

            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore18: {
            Ctx.BodyPart();
            auto node = IfStatement(core.GetAlt_sql_stmt_core18().GetRule_if_stmt1());
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore19: {
            Ctx.BodyPart();
            auto node = ForStatement(core.GetAlt_sql_stmt_core19().GetRule_for_stmt1());
            if (!node) {
                return false;
            }

            blocks.push_back(node);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore20: {
            Ctx.BodyPart();
            TSqlValues values(Ctx, Mode);
            TPosition pos;
            auto source = values.Build(core.GetAlt_sql_stmt_core20().GetRule_values_stmt1(), pos, {}, TPosition());
            if (!source) {
                return false;
            }
            blocks.emplace_back(BuildSelectResult(pos, std::move(source),
                Mode != NSQLTranslation::ESqlMode::LIMITED_VIEW && Mode != NSQLTranslation::ESqlMode::SUBQUERY, Mode == NSQLTranslation::ESqlMode::SUBQUERY,
                Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore21: {
            // create_user_stmt: CREATE USER role_name create_user_option?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core21().GetRule_create_user_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            bool allowSystemRoles = false;
            if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                return false;
            }

            TMaybe<TRoleParameters> roleParams;
            if (node.HasBlock4()) {
                roleParams.ConstructInPlace();
                if (!RoleParameters(node.GetBlock4().GetRule_create_user_option1(), *roleParams)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildCreateUser(pos, service, cluster, roleName, roleParams, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore22: {
            // alter_user_stmt: ALTER USER role_name (WITH? create_user_option | RENAME TO role_name);
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core22().GetRule_alter_user_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            {
                bool allowSystemRoles = true;
                if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                    return false;
                }
            }

            TNodePtr stmt;
            switch (node.GetBlock4().Alt_case()) {
                case TRule_alter_user_stmt_TBlock4::kAlt1: {
                    TRoleParameters roleParams;
                    if (!RoleParameters(node.GetBlock4().GetAlt1().GetRule_create_user_option2(), roleParams)) {
                        return false;
                    }
                    stmt = BuildAlterUser(pos, service, cluster, roleName, roleParams, Ctx.Scoped);
                    break;
                }
                case TRule_alter_user_stmt_TBlock4::kAlt2: {
                    TDeferredAtom tgtRoleName;
                    bool allowSystemRoles = false;
                    if (!RoleNameClause(node.GetBlock4().GetAlt2().GetRule_role_name3(), tgtRoleName, allowSystemRoles)) {
                        return false;
                    }
                    stmt = BuildRenameUser(pos, service, cluster, roleName, tgtRoleName,Ctx.Scoped);
                    break;
                }
                default:
                    Y_FAIL("You should change implementation according to grammar changes");
            }

            AddStatementToBlocks(blocks, stmt);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore23: {
            // create_group_stmt: CREATE GROUP role_name;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core23().GetRule_create_group_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            bool allowSystemRoles = false;
            if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildCreateGroup(pos, service, cluster, roleName, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore24: {
            // alter_group_stmt: ALTER GROUP role_name ((ADD|DROP) USER role_name (COMMA role_name)* COMMA? | RENAME TO role_name);
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core24().GetRule_alter_group_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TDeferredAtom roleName;
            {
                bool allowSystemRoles = true;
                if (!RoleNameClause(node.GetRule_role_name3(), roleName, allowSystemRoles)) {
                    return false;
                }
            }

            TNodePtr stmt;
            switch (node.GetBlock4().Alt_case()) {
                case TRule_alter_group_stmt_TBlock4::kAlt1: {
                    auto& addDropNode = node.GetBlock4().GetAlt1();
                    const bool isDrop = addDropNode.GetToken1().GetId() == SQLv1LexerTokens::TOKEN_DROP;
                    TVector<TDeferredAtom> roles;
                    bool allowSystemRoles = false;
                    roles.emplace_back();
                    if (!RoleNameClause(addDropNode.GetRule_role_name3(), roles.back(), allowSystemRoles)) {
                        return false;
                    }

                    for (auto& item : addDropNode.GetBlock4()) {
                        roles.emplace_back();
                        if (!RoleNameClause(item.GetRule_role_name2(), roles.back(), allowSystemRoles)) {
                            return false;
                        }
                    }

                    stmt = BuildAlterGroup(pos, service, cluster, roleName, roles, isDrop, Ctx.Scoped);
                    break;
                }
                case TRule_alter_group_stmt_TBlock4::kAlt2: {
                    TDeferredAtom tgtRoleName;
                    bool allowSystemRoles = false;
                    if (!RoleNameClause(node.GetBlock4().GetAlt2().GetRule_role_name3(), tgtRoleName, allowSystemRoles)) {
                        return false;
                    }
                    stmt = BuildRenameGroup(pos, service, cluster, roleName, tgtRoleName, Ctx.Scoped);
                    break;
                }
                default:
                    Y_FAIL("You should change implementation according to grammar changes");
            }

            AddStatementToBlocks(blocks, stmt);
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore25: {
            // drop_role_stmt: DROP (USER|GROUP) (IF EXISTS)? role_name (COMMA role_name)* COMMA?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core25().GetRule_drop_role_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            const bool isUser = node.GetToken2().GetId() == SQLv1LexerTokens::TOKEN_USER;
            const bool force = node.HasBlock3();

            TVector<TDeferredAtom> roles;
            bool allowSystemRoles = true;
            roles.emplace_back();
            if (!RoleNameClause(node.GetRule_role_name4(), roles.back(), allowSystemRoles)) {
                return false;
            }

            for (auto& item : node.GetBlock5()) {
                roles.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roles.back(), allowSystemRoles)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildDropRoles(pos, service, cluster, roles, isUser, force, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore26: {
            // create_object_stmt: CREATE OBJECT name (TYPE type [WITH k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core26().GetRule_create_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref6().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (node.HasBlock8()) {
                if (!ParseObjectFeatures(kv, node.GetBlock8().GetRule_create_object_features1().GetRule_object_features2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildCreateObjectOperation(Ctx.Pos(), objectId, typeId, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore27: {
            // alter_object_stmt: ALTER OBJECT name (TYPE type [SET k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core27().GetRule_alter_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref6().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (!ParseObjectFeatures(kv, node.GetRule_alter_object_features8().GetRule_object_features2())) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildAlterObjectOperation(Ctx.Pos(), objectId, typeId, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore28: {
            // drop_object_stmt: DROP OBJECT name (TYPE type [WITH k=v,...]);
            auto& node = core.GetAlt_sql_stmt_core28().GetRule_drop_object_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref3().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref3().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref3().GetRule_id_or_at2(), *this).second;
            const TString& typeId = Id(node.GetRule_object_type_ref6().GetRule_an_id_or_type1(), *this);
            std::map<TString, TDeferredAtom> kv;
            if (node.HasBlock8()) {
                if (!ParseObjectFeatures(kv, node.GetBlock8().GetRule_drop_object_features1().GetRule_object_features2())) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildDropObjectOperation(Ctx.Pos(), objectId, typeId, std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore29: {
            // create_external_data_source_stmt: CREATE EXTERNAL DATA SOURCE name WITH (k=v,...);
            auto& node = core.GetAlt_sql_stmt_core29().GetRule_create_external_data_source_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref5().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref5().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref5().GetRule_id_or_at2(), *this).second;
            std::map<TString, TDeferredAtom> kv;
            if (!ParseExternalDataSourceSettings(kv, node.GetRule_with_table_settings6())) {
                return false;
            }

            AddStatementToBlocks(blocks, BuildCreateObjectOperation(Ctx.Pos(), BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId), "EXTERNAL_DATA_SOURCE", std::move(kv), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore30: {
            // drop_external_data_source_stmt: DROP EXTERNAL DATA SOURCE name;
            auto& node = core.GetAlt_sql_stmt_core30().GetRule_drop_external_data_source_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref5().HasBlock1()) {
                if (!ClusterExpr(node.GetRule_object_ref5().GetBlock1().GetRule_cluster_expr1(),
                    false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString& objectId = Id(node.GetRule_object_ref5().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildDropObjectOperation(Ctx.Pos(), BuildTablePath(Ctx.GetPrefixPath(context.ServiceId, context.Cluster), objectId), "EXTERNAL_DATA_SOURCE", {}, context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore31: {
            // create_replication_stmt: CREATE ASYNC REPLICATION
            auto& node = core.GetAlt_sql_stmt_core31().GetRule_create_replication_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                const auto& cluster = node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1();
                if (!ClusterExpr(cluster, false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            std::vector<std::pair<TString, TString>> targets;
            if (!AsyncReplicationTarget(targets, node.GetRule_replication_target6(), *this)) {
                return false;
            }
            for (auto& block : node.GetBlock7()) {
                if (!AsyncReplicationTarget(targets, block.GetRule_replication_target2(), *this)) {
                    return false;
                }
            }

            std::map<TString, TNodePtr> settings;
            if (!AsyncReplicationSettings(settings, node.GetRule_replication_settings10(), *this)) {
                return false;
            }

            const TString id = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildCreateAsyncReplication(Ctx.Pos(), id, std::move(targets), std::move(settings), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore32: {
            // drop_replication_stmt: DROP ASYNC REPLICATION
            auto& node = core.GetAlt_sql_stmt_core32().GetRule_drop_replication_stmt1();
            TObjectOperatorContext context(Ctx.Scoped);
            if (node.GetRule_object_ref4().HasBlock1()) {
                const auto& cluster = node.GetRule_object_ref4().GetBlock1().GetRule_cluster_expr1();
                if (!ClusterExpr(cluster, false, context.ServiceId, context.Cluster)) {
                    return false;
                }
            }

            const TString id = Id(node.GetRule_object_ref4().GetRule_id_or_at2(), *this).second;
            AddStatementToBlocks(blocks, BuildDropAsyncReplication(Ctx.Pos(), id, node.HasBlock5(), context));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore33: {
            Ctx.BodyPart();
            // create_topic_stmt: CREATE TOPIC topic1 (CONSUMER ...)? [WITH (opt1 = val1, ...]?
            auto& rule = core.GetAlt_sql_stmt_core33().GetRule_create_topic_stmt1();
            TTopicRef tr;
            if (!TopicRefImpl(rule.GetRule_topic_ref3(), tr)) {
                return false;
            }

            TCreateTopicParameters params;
            if (rule.HasBlock4()) { //create_topic_entry (consumers)
                auto& entries = rule.GetBlock4().GetRule_create_topic_entries1();
                auto& firstEntry = entries.GetRule_create_topic_entry2();
                if (!CreateTopicEntry(firstEntry, params)) {
                    return false;
                }
                const auto& list = entries.GetBlock3();
                for (auto& node : list) {
                    if (!CreateTopicEntry(node.GetRule_create_topic_entry2(), params)) {
                        return false;
                    }
                }

            }
            if (rule.HasBlock5()) { // with_topic_settings
                auto& topic_settings_node = rule.GetBlock5().GetRule_with_topic_settings1().GetRule_topic_settings3();
                CreateTopicSettings(topic_settings_node, params.TopicSettings);
            }

            AddStatementToBlocks(blocks, BuildCreateTopic(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore34: {
//            alter_topic_stmt: ALTER TOPIC topic_ref alter_topic_action (COMMA alter_topic_action)*;

            Ctx.BodyPart();
            auto& rule = core.GetAlt_sql_stmt_core34().GetRule_alter_topic_stmt1();
            TTopicRef tr;
            if (!TopicRefImpl(rule.GetRule_topic_ref3(), tr)) {
                return false;
            }

            TAlterTopicParameters params;
            auto& firstEntry = rule.GetRule_alter_topic_action4();
            if (!AlterTopicAction(firstEntry, params)) {
                return false;
            }
            const auto& list = rule.GetBlock5();
            for (auto& node : list) {
                if (!AlterTopicAction(node.GetRule_alter_topic_action2(), params)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildAlterTopic(Ctx.Pos(), tr, params, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore35: {
            // drop_topic_stmt: DROP TOPIC
            Ctx.BodyPart();
            const auto& rule = core.GetAlt_sql_stmt_core35().GetRule_drop_topic_stmt1();

            TTopicRef tr;
            if (!TopicRefImpl(rule.GetRule_topic_ref3(), tr)) {
                return false;
            }
            AddStatementToBlocks(blocks, BuildDropTopic(Ctx.Pos(), tr, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore36: {
            // GRANT permission_name_target ON an_id_schema (COMMA an_id_schema)* TO role_name (COMMA role_name)* COMMA? (WITH GRANT OPTION)?;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core36().GetRule_grant_permissions_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TVector<TDeferredAtom> permissions;
            if (!PermissionNameClause(node.GetRule_permission_name_target2(), permissions, node.has_block10())) {
                return false;
            }

            TVector<TDeferredAtom> schemaPathes;
            schemaPathes.emplace_back(Ctx.Pos(), Id(node.GetRule_an_id_schema4(), *this));
            for (const auto& item : node.GetBlock5()) {
                schemaPathes.emplace_back(Ctx.Pos(), Id(item.GetRule_an_id_schema2(), *this));
            }

            TVector<TDeferredAtom> roleNames;
            const bool allowSystemRoles = false;
            roleNames.emplace_back();
            if (!RoleNameClause(node.GetRule_role_name7(), roleNames.back(), allowSystemRoles)) {
                return false;
            }
            for (const auto& item : node.GetBlock8()) {
                roleNames.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roleNames.back(), allowSystemRoles)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildGrantPermissions(pos, service, cluster, permissions, schemaPathes, roleNames, Ctx.Scoped));
            break;
        }
        case TRule_sql_stmt_core::kAltSqlStmtCore37:
        {
            // REVOKE (GRANT OPTION FOR)? permission_name_target ON an_id_schema (COMMA an_id_schema)* FROM role_name (COMMA role_name)*;
            Ctx.BodyPart();
            auto& node = core.GetAlt_sql_stmt_core37().GetRule_revoke_permissions_stmt1();

            Ctx.Token(node.GetToken1());
            const TPosition pos = Ctx.Pos();

            TString service = Ctx.Scoped->CurrService;
            TDeferredAtom cluster = Ctx.Scoped->CurrCluster;
            if (cluster.Empty()) {
                Error() << "USE statement is missing - no default cluster is selected";
                return false;
            }

            TVector<TDeferredAtom> permissions;
            if (!PermissionNameClause(node.GetRule_permission_name_target3(), permissions, node.HasBlock2())) {
                return false;
            }

            TVector<TDeferredAtom> schemaPathes;
            schemaPathes.emplace_back(Ctx.Pos(), Id(node.GetRule_an_id_schema5(), *this));
            for (const auto& item : node.GetBlock6()) {
                schemaPathes.emplace_back(Ctx.Pos(), Id(item.GetRule_an_id_schema2(), *this));
            }

            TVector<TDeferredAtom> roleNames;
            const bool allowSystemRoles = false;
            roleNames.emplace_back();
            if (!RoleNameClause(node.GetRule_role_name8(), roleNames.back(), allowSystemRoles)) {
                return false;
            }
            for (const auto& item : node.GetBlock9()) {
                roleNames.emplace_back();
                if (!RoleNameClause(item.GetRule_role_name2(), roleNames.back(), allowSystemRoles)) {
                    return false;
                }
            }

            AddStatementToBlocks(blocks, BuildRevokePermissions(pos, service, cluster, permissions, schemaPathes, roleNames, Ctx.Scoped));
            break;
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownStatement" + internalStatementName);
            AltNotImplemented("sql_stmt_core", core);
            return false;
    }

    Ctx.IncrementMonCounter("sql_features", internalStatementName);
    return !Ctx.HasPendingErrors;
}

bool TSqlQuery::DeclareStatement(const TRule_declare_stmt& stmt) {
    TNodePtr defaultValue;
    if (stmt.HasBlock5()) {
        TSqlExpression sqlExpr(Ctx, Mode);
        auto exprOrId = sqlExpr.LiteralExpr(stmt.GetBlock5().GetRule_literal_value2());
        if (!exprOrId) {
            return false;
        }
        if (!exprOrId->Expr) {
            Ctx.Error() << "Identifier is not expected here";
            return false;
        }
        defaultValue = exprOrId->Expr;
    }
    if (defaultValue) {
        Error() << "DEFAULT value not supported yet";
        return false;
    }
    if (!Ctx.IsParseHeading()) {
        Error() << "DECLARE statement should be in beginning of query, but it's possible to use PRAGMA or USE before it";
        return false;
    }

    TString varName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter2(), varName, *this)) {
        return false;
    }
    const auto varPos = Ctx.Pos();
    const auto typeNode = TypeNode(stmt.GetRule_type_name4());
    if (!typeNode) {
        return false;
    }
    if (IsAnonymousName(varName)) {
        Ctx.Error(varPos) << "Can not use anonymous name '" << varName << "' in DECLARE statement";
        return false;
    }

    if (Ctx.IsAlreadyDeclared(varName)) {
        Ctx.Warning(varPos, TIssuesIds::YQL_DUPLICATE_DECLARE) << "Duplicate declaration of '" << varName << "' will be ignored";
    } else {
        PushNamedAtom(varPos, varName);
        Ctx.DeclareVariable(varName, typeNode);
    }
    return true;
}

bool TSqlQuery::ExportStatement(const TRule_export_stmt& stmt) {
    if (Mode != NSQLTranslation::ESqlMode::LIBRARY || !TopLevel) {
        Error() << "EXPORT statement should be used only in a library on the top level";
        return false;
    }

    TVector<TSymbolNameWithPos> bindNames;
    if (!BindList(stmt.GetRule_bind_parameter_list2(), bindNames)) {
        return false;
    }

    for (auto& bindName : bindNames) {
        if (!Ctx.AddExport(bindName.Pos, bindName.Name)) {
            return false;
        }
    }
    return true;
}

bool TSqlQuery::AlterTableAction(const TRule_alter_table_action& node, TAlterTableParameters& params) {
    if (params.RenameTo) {
        // rename action is followed by some other actions
        Error() << "RENAME TO can not be used together with another table action";
        return false;
    }

    switch (node.Alt_case()) {
    case TRule_alter_table_action::kAltAlterTableAction1: {
        // ADD COLUMN
        const auto& addRule = node.GetAlt_alter_table_action1().GetRule_alter_table_add_column1();
        if (!AlterTableAddColumn(addRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction2: {
        // DROP COLUMN
        const auto& dropRule = node.GetAlt_alter_table_action2().GetRule_alter_table_drop_column1();
        if (!AlterTableDropColumn(dropRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction3: {
        // ALTER COLUMN
        const auto& alterRule = node.GetAlt_alter_table_action3().GetRule_alter_table_alter_column1();
        if (!AlterTableAlterColumn(alterRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction4: {
        // ADD FAMILY
        const auto& familyEntry = node.GetAlt_alter_table_action4().GetRule_alter_table_add_column_family1()
            .GetRule_family_entry2();
        if (!AlterTableAddFamily(familyEntry, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction5: {
        // ALTER FAMILY
        const auto& alterRule = node.GetAlt_alter_table_action5().GetRule_alter_table_alter_column_family1();
        if (!AlterTableAlterFamily(alterRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction6: {
        // SET (uncompat)
        const auto& setRule = node.GetAlt_alter_table_action6().GetRule_alter_table_set_table_setting_uncompat1();
        if (!AlterTableSetTableSetting(setRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction7: {
        // SET (compat)
        const auto& setRule = node.GetAlt_alter_table_action7().GetRule_alter_table_set_table_setting_compat1();
        if (!AlterTableSetTableSetting(setRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction8: {
        // RESET
        const auto& setRule = node.GetAlt_alter_table_action8().GetRule_alter_table_reset_table_setting1();
        if (!AlterTableResetTableSetting(setRule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction9: {
        // ADD INDEX
        const auto& addIndex = node.GetAlt_alter_table_action9().GetRule_alter_table_add_index1();
        if (!AlterTableAddIndex(addIndex, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction10: {
        // DROP INDEX
        const auto& dropIndex = node.GetAlt_alter_table_action10().GetRule_alter_table_drop_index1();
        AlterTableDropIndex(dropIndex, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction11: {
        // RENAME TO
        if (!params.IsEmpty()) {
            // rename action follows some other actions
            Error() << "RENAME TO can not be used together with another table action";
            return false;
        }

        const auto& renameTo = node.GetAlt_alter_table_action11().GetRule_alter_table_rename_to1();
        AlterTableRenameTo(renameTo, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction12: {
        // ADD CHANGEFEED
        const auto& rule = node.GetAlt_alter_table_action12().GetRule_alter_table_add_changefeed1();
        if (!AlterTableAddChangefeed(rule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction13: {
        // ALTER CHANGEFEED
        const auto& rule = node.GetAlt_alter_table_action13().GetRule_alter_table_alter_changefeed1();
        if (!AlterTableAlterChangefeed(rule, params)) {
            return false;
        }
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction14: {
        // DROP CHANGEFEED
        const auto& rule = node.GetAlt_alter_table_action14().GetRule_alter_table_drop_changefeed1();
        AlterTableDropChangefeed(rule, params);
        break;
    }
    case TRule_alter_table_action::kAltAlterTableAction15: {
        // RENAME INDEX TO
        if (!params.IsEmpty()) {
            // rename action follows some other actions
            Error() << "RENAME INDEX TO can not be used together with another table action";
            return false;
        }

        const auto& renameTo = node.GetAlt_alter_table_action15().GetRule_alter_table_rename_index_to1();
        AlterTableRenameIndexTo(renameTo, params);
        break;
    }

    default:
        AltNotImplemented("alter_table_action", node);
        return false;
    }
    return true;
}

bool TSqlQuery::AlterTableAddColumn(const TRule_alter_table_add_column& node, TAlterTableParameters& params) {
    auto columnSchema = ColumnSchemaImpl(node.GetRule_column_schema3());
    if (!columnSchema) {
        return false;
    }
    if (columnSchema->Families.size() > 1) {
        Ctx.Error() << "Several column families for a single column are not yet supported";
        return false;
    }
    params.AddColumns.push_back(*columnSchema);
    return true;
}

bool TSqlQuery::AlterTableDropColumn(const TRule_alter_table_drop_column& node, TAlterTableParameters& params) {
    TString name = Id(node.GetRule_an_id3(), *this);
    params.DropColumns.push_back(name);
    return true;
}

bool TSqlQuery::AlterTableAlterColumn(const TRule_alter_table_alter_column& node,
        TAlterTableParameters& params)
{
    TString name = Id(node.GetRule_an_id3(), *this);
    const TPosition pos(Context().Pos());
    TVector<TIdentifier> families;
    const auto& familyRelation = node.GetRule_family_relation5();
    families.push_back(IdEx(familyRelation.GetRule_an_id2(), *this));
    params.AlterColumns.emplace_back(pos, name, nullptr, false, families);
    return true;
}

bool TSqlQuery::AlterTableAddFamily(const TRule_family_entry& node, TAlterTableParameters& params) {
    TFamilyEntry family(IdEx(node.GetRule_an_id2(), *this));
    if (!FillFamilySettings(node.GetRule_family_settings3(), family)) {
        return false;
    }
    params.AddColumnFamilies.push_back(family);
    return true;
}

bool TSqlQuery::AlterTableAlterFamily(const TRule_alter_table_alter_column_family& node,
        TAlterTableParameters& params)
{
    TFamilyEntry* entry = nullptr;
    TIdentifier name = IdEx(node.GetRule_an_id3(), *this);
    for (auto& family : params.AlterColumnFamilies) {
        if (family.Name.Name == name.Name) {
            entry = &family;
            break;
        }
    }
    if (!entry) {
        entry = &params.AlterColumnFamilies.emplace_back(name);
    }
    TIdentifier settingName = IdEx(node.GetRule_an_id5(), *this);
    const TRule_family_setting_value& value = node.GetRule_family_setting_value6();
    if (to_lower(settingName.Name) == "data") {
        if (entry->Data) {
            Ctx.Error() << "Redefinition of 'data' setting for column family '" << name.Name
                << "' in one alter";
            return false;
        }
        const TString stringValue(Ctx.Token(value.GetToken1()));
        entry->Data = BuildLiteralSmartString(Ctx, stringValue);
    } else if (to_lower(settingName.Name) == "compression") {
        if (entry->Compression) {
            Ctx.Error() << "Redefinition of 'compression' setting for column family '" << name.Name
                << "' in one alter";
            return false;
        }
        const TString stringValue(Ctx.Token(value.GetToken1()));
        entry->Compression = BuildLiteralSmartString(Ctx, stringValue);
    } else {
        Ctx.Error() << "Unknown table setting: " << settingName.Name;
        return false;
    }
    return true;
}

bool TSqlQuery::AlterTableSetTableSetting(const TRule_alter_table_set_table_setting_uncompat& node,
        TAlterTableParameters& params)
{
    if (!StoreTableSettingsEntry(IdEx(node.GetRule_an_id2(), *this), node.GetRule_table_setting_value3(),
        params.TableSettings, params.TableType, true)) {
        return false;
    }
    return true;
}

bool TSqlQuery::AlterTableSetTableSetting(const TRule_alter_table_set_table_setting_compat& node,
        TAlterTableParameters& params)
{
    const auto& firstEntry = node.GetRule_alter_table_setting_entry3();
    if (!StoreTableSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), firstEntry.GetRule_table_setting_value3(),
        params.TableSettings, params.TableType, true)) {
        return false;
    }
    for (auto& block : node.GetBlock4()) {
        const auto& entry = block.GetRule_alter_table_setting_entry2();
        if (!StoreTableSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), entry.GetRule_table_setting_value3(),
            params.TableSettings, params.TableType, true)) {
            return false;
        }
    }
    return true;
}

bool TSqlQuery::AlterTableResetTableSetting(const TRule_alter_table_reset_table_setting& node,
        TAlterTableParameters& params)
{
    const auto& firstEntry = node.GetRule_an_id3();
    if (!ResetTableSettingsEntry(IdEx(firstEntry, *this), params.TableSettings, params.TableType)) {
        return false;
    }
    for (auto& block : node.GetBlock4()) {
        const auto& entry = block.GetRule_an_id2();
        if (!ResetTableSettingsEntry(IdEx(entry, *this), params.TableSettings, params.TableType)) {
            return false;
        }
    }
    return true;
}

bool TSqlQuery::AlterTableAddIndex(const TRule_alter_table_add_index& node, TAlterTableParameters& params) {
    if (!CreateTableIndex(node.GetRule_table_index2(), *this, params.AddIndexes)) {
        return false;
    }
    return true;
}

void TSqlQuery::AlterTableDropIndex(const TRule_alter_table_drop_index& node, TAlterTableParameters& params) {
    params.DropIndexes.emplace_back(IdEx(node.GetRule_an_id3(), *this));
}

void TSqlQuery::AlterTableRenameTo(const TRule_alter_table_rename_to& node, TAlterTableParameters& params) {
    params.RenameTo = IdEx(node.GetRule_an_id_table3(), *this);
}

void TSqlQuery::AlterTableRenameIndexTo(const TRule_alter_table_rename_index_to& node, TAlterTableParameters& params) {
    auto src = IdEx(node.GetRule_an_id3(), *this);
    auto dst = IdEx(node.GetRule_an_id5(), *this);

    params.RenameIndexTo = std::make_pair(src, dst);
}

bool TSqlQuery::AlterTableAddChangefeed(const TRule_alter_table_add_changefeed& node, TAlterTableParameters& params) {
    TSqlExpression expr(Ctx, Mode);
    return CreateChangefeed(node.GetRule_changefeed2(), expr, params.AddChangefeeds);
}

bool TSqlQuery::AlterTableAlterChangefeed(const TRule_alter_table_alter_changefeed& node, TAlterTableParameters& params) {
    params.AlterChangefeeds.emplace_back(IdEx(node.GetRule_an_id3(), *this));

    const auto& alter = node.GetRule_changefeed_alter_settings4();
    switch (alter.Alt_case()) {
        case TRule_changefeed_alter_settings::kAltChangefeedAlterSettings1: {
            // DISABLE
            params.AlterChangefeeds.back().Disable = true;
            break;
        }
        case TRule_changefeed_alter_settings::kAltChangefeedAlterSettings2: {
            // SET
            const auto& rule = alter.GetAlt_changefeed_alter_settings2().GetRule_changefeed_settings3();
            TSqlExpression expr(Ctx, Mode);
            if (!ChangefeedSettings(rule, expr, params.AlterChangefeeds.back().Settings, true)) {
                return false;
            }
            break;
        }

        default:
            AltNotImplemented("changefeed_alter_settings", alter);
            return false;
    }

    return true;
}

void TSqlQuery::AlterTableDropChangefeed(const TRule_alter_table_drop_changefeed& node, TAlterTableParameters& params) {
    params.DropChangefeeds.emplace_back(IdEx(node.GetRule_an_id3(), *this));
}

TNodePtr TSqlQuery::PragmaStatement(const TRule_pragma_stmt& stmt, bool& success) {
    success = false;
    const TString& prefix = OptIdPrefixAsStr(stmt.GetRule_opt_id_prefix_or_type2(), *this);
    const TString& lowerPrefix = to_lower(prefix);
    const TString pragma(Id(stmt.GetRule_an_id3(), *this));
    TString normalizedPragma(pragma);
    TMaybe<TIssue> normalizeError = NormalizeName(Ctx.Pos(), normalizedPragma);
    if (!normalizeError.Empty()) {
        Error() << normalizeError->GetMessage();
        Ctx.IncrementMonCounter("sql_errors", "NormalizePragmaError");
        return {};
    }

    TVector<TDeferredAtom> values;
    TVector<const TRule_pragma_value*> pragmaValues;
    bool pragmaValueDefault = false;
    if (stmt.GetBlock4().HasAlt1()) {
        pragmaValues.push_back(&stmt.GetBlock4().GetAlt1().GetRule_pragma_value2());
    }
    else if (stmt.GetBlock4().HasAlt2()) {
        pragmaValues.push_back(&stmt.GetBlock4().GetAlt2().GetRule_pragma_value2());
        for (auto& additionalValue : stmt.GetBlock4().GetAlt2().GetBlock3()) {
            pragmaValues.push_back(&additionalValue.GetRule_pragma_value2());
        }
    }

    const bool withConfigure = prefix || normalizedPragma == "file" || normalizedPragma == "folder" || normalizedPragma == "udf";
    static const THashSet<TStringBuf> lexicalScopePragmas = {"classicdivision", "strictjoinkeytypes", "disablestrictjoinkeytypes", "checkedops"};
    const bool hasLexicalScope = withConfigure || lexicalScopePragmas.contains(normalizedPragma);
    for (auto pragmaValue : pragmaValues) {
        if (pragmaValue->HasAlt_pragma_value3()) {
            auto value = Token(pragmaValue->GetAlt_pragma_value3().GetToken1());
            auto parsed = StringContentOrIdContent(Ctx, Ctx.Pos(), value);
            if (!parsed) {
                return {};
            }

            values.push_back(TDeferredAtom(Ctx.Pos(), parsed->Content));
        }
        else if (pragmaValue->HasAlt_pragma_value2()
            && pragmaValue->GetAlt_pragma_value2().GetRule_id1().HasAlt_id2()
            && "default" == to_lower(Id(pragmaValue->GetAlt_pragma_value2().GetRule_id1(), *this)))
        {
            pragmaValueDefault = true;
        }
        else if (withConfigure && pragmaValue->HasAlt_pragma_value5()) {
            TString bindName;
            if (!NamedNodeImpl(pragmaValue->GetAlt_pragma_value5().GetRule_bind_parameter1(), bindName, *this)) {
                return {};
            }
            auto namedNode = GetNamedNode(bindName);
            if (!namedNode) {
                return {};
            }

            TDeferredAtom atom;
            MakeTableFromExpression(Ctx, namedNode, atom);
            values.push_back(atom);
        } else {
            Error() << "Expected string" << (withConfigure ? ", named parameter" : "") << " or 'default' keyword as pragma value for pragma: " << pragma;
            Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
            return {};
        }
    }

    if (prefix.empty()) {
        if (!TopLevel && !hasLexicalScope) {
            Error() << "This pragma '" << pragma << "' is not allowed to be used in actions or subqueries";
            Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
            return{};
        }

        if (normalizedPragma == "refselect") {
            Ctx.PragmaRefSelect = true;
            Ctx.IncrementMonCounter("sql_pragma", "RefSelect");
        } else if (normalizedPragma == "sampleselect") {
            Ctx.PragmaSampleSelect = true;
            Ctx.IncrementMonCounter("sql_pragma", "SampleSelect");
        } else if (normalizedPragma == "allowdotinalias") {
            Ctx.PragmaAllowDotInAlias = true;
            Ctx.IncrementMonCounter("sql_pragma", "AllowDotInAlias");
        } else if (normalizedPragma == "udf") {
            if ((values.size() != 1 && values.size() != 2) || pragmaValueDefault) {
                Error() << "Expected file alias as pragma value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "udf");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "ImportUdfs", values, false);
        } else if (normalizedPragma == "packageversion") {
            if (values.size() != 2 || pragmaValueDefault) {
                Error() << "Expected package name and version";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            ui32 version = 0;
            TString versionString;
            TString packageName;
            if (!values[0].GetLiteral(packageName, Ctx) || !values[1].GetLiteral(versionString, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (!PackageVersionFromString(versionString, version)) {
                Error() << "Unable to parse package version, possible values 0, 1, draft, release";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.SetPackageVersion(packageName, version);
            Ctx.IncrementMonCounter("sql_pragma", "PackageVersion");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "SetPackageVersion", TVector<TDeferredAtom>{ values[0], TDeferredAtom(values[1].Build()->GetPos(), ToString(version)) }, false);
        } else if (normalizedPragma == "file") {
            if (values.size() < 2U || values.size() > 3U || pragmaValueDefault) {
                Error() << "Expected file alias, url and optional token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "file");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "AddFileByUrl", values, false);
        } else if (normalizedPragma == "folder") {
            if (values.size() < 2U || values.size() > 3U || pragmaValueDefault) {
                Error() << "Expected folder alias, url and optional token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "folder");
            success = true;
            return BuildPragma(Ctx.Pos(), TString(ConfigProviderName), "AddFolderByUrl", values, false);
        } else if (normalizedPragma == "library") {
            if (values.size() < 1) {
                Error() << "Expected non-empty file alias";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }
            if (values.size() > 3) {
                Error() << "Expected file alias and optional url and token name as pragma values";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }

            TString alias;
            if (!values.front().GetLiteral(alias, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return{};
            }

            TContext::TLibraryStuff library;
            std::get<TPosition>(library) = values.front().Build()->GetPos();
            if (values.size() > 1) {
                auto& first = std::get<1U>(library);
                first.emplace();
                first->second = values[1].Build()->GetPos();
                if (!values[1].GetLiteral(first->first, Ctx)) {
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return{};
                }

                TSet<TString> names;
                SubstParameters(first->first, Nothing(), &names);
                for (const auto& name : names) {
                    auto namedNode = GetNamedNode(name);
                    if (!namedNode) {
                        return{};
                    }
                }
                if (values.size() > 2) {
                    auto& second = std::get<2U>(library);
                    second.emplace();
                    second->second = values[2].Build()->GetPos();
                    if (!values[2].GetLiteral(second->first, Ctx)) {
                        Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                        return{};
                    }
                }
            }

            Ctx.Libraries[alias] = std::move(library);
            Ctx.IncrementMonCounter("sql_pragma", "library");
        } else if (normalizedPragma == "directread") {
            Ctx.PragmaDirectRead = true;
            Ctx.IncrementMonCounter("sql_pragma", "DirectRead");
        } else if (normalizedPragma == "equijoin") {
            Ctx.IncrementMonCounter("sql_pragma", "EquiJoin");
        } else if (normalizedPragma == "autocommit") {
            Ctx.PragmaAutoCommit = true;
            Ctx.IncrementMonCounter("sql_pragma", "AutoCommit");
        } else if (normalizedPragma == "usetableprefixforeach") {
            Ctx.PragmaUseTablePrefixForEach = true;
            Ctx.IncrementMonCounter("sql_pragma", "UseTablePrefixForEach");
        } else if (normalizedPragma == "tablepathprefix") {
            TString value;
            TMaybe<TString> arg;

            if (values.size() == 1 || values.size() == 2) {
                if (!values.front().GetLiteral(value, Ctx)) {
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                    return {};
                }

                if (values.size() == 2) {
                    arg = value;
                    if (!values.back().GetLiteral(value, Ctx)) {
                        Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                        return {};
                    }
                }

                if (!Ctx.SetPathPrefix(value, arg)) {
                    return {};
                }
            } else {
                Error() << "Expected path prefix or tuple of (Provider, PathPrefix) or"
                        << " (Cluster, PathPrefix) as pragma value";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "PathPrefix");
        } else if (normalizedPragma == "groupbylimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaGroupByLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "GroupByLimit");
        } else if (normalizedPragma == "groupbycubelimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.PragmaGroupByCubeLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "GroupByCubeLimit");
        } else if (normalizedPragma == "simplecolumns") {
            Ctx.SimpleColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "SimpleColumns");
        } else if (normalizedPragma == "disablesimplecolumns") {
            Ctx.SimpleColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableSimpleColumns");
        } else if (normalizedPragma == "coalescejoinkeysonqualifiedall") {
            Ctx.CoalesceJoinKeysOnQualifiedAll = true;
            Ctx.IncrementMonCounter("sql_pragma", "CoalesceJoinKeysOnQualifiedAll");
        } else if (normalizedPragma == "disablecoalescejoinkeysonqualifiedall") {
            Ctx.CoalesceJoinKeysOnQualifiedAll = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableCoalesceJoinKeysOnQualifiedAll");
        } else if (normalizedPragma == "resultrowslimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.ResultRowsLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "ResultRowsLimit");
        } else if (normalizedPragma == "resultsizelimit") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.ResultSizeLimit)) {
                Error() << "Expected unsigned integer literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.IncrementMonCounter("sql_pragma", "ResultSizeLimit");
        } else if (normalizedPragma == "warning") {
            if (values.size() != 2U || values.front().Empty() || values.back().Empty()) {
                Error() << "Expected arguments <action>, <issueId> for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TString action;
            TString codePattern;
            if (!values[0].GetLiteral(action, Ctx) || !values[1].GetLiteral(codePattern, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            TWarningRule rule;
            TString parseError;
            auto parseResult = TWarningRule::ParseFrom(codePattern, action, rule, parseError);
            switch (parseResult) {
                case TWarningRule::EParseResult::PARSE_OK:
                    break;
                case TWarningRule::EParseResult::PARSE_PATTERN_FAIL:
                case TWarningRule::EParseResult::PARSE_ACTION_FAIL:
                    Ctx.Error() << parseError;
                    return {};
                default:
                    Y_ENSURE(false, "Unknown parse result");
            }

            Ctx.WarningPolicy.AddRule(rule);
            if (rule.GetPattern() == "*" && rule.GetAction() == EWarningAction::ERROR) {
                // Keep 'unused symbol' warning as warning unless explicitly set to error
                Ctx.SetWarningPolicyFor(TIssuesIds::YQL_UNUSED_SYMBOL, EWarningAction::DEFAULT);
            }

            Ctx.IncrementMonCounter("sql_pragma", "warning");
        } else if (normalizedPragma == "greetings") {
            if (values.size() > 1) {
                Error() << "Multiple arguments are not expected for " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (values.empty()) {
                values.emplace_back(TDeferredAtom(Ctx.Pos(), "Hello, world! And best wishes from the YQL Team!"));
            }

            TString arg;
            if (!values.front().GetLiteral(arg, Ctx)) {
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Info(Ctx.Pos()) << arg;
        } else if (normalizedPragma == "warningmsg") {
            if (values.size() != 1 || !values[0].GetLiteral()) {
                Error() << "Expected string literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_PRAGMA_WARNING_MSG) << *values[0].GetLiteral();
        } else if (normalizedPragma == "errormsg") {
            if (values.size() != 1 || !values[0].GetLiteral()) {
                Error() << "Expected string literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.Error(Ctx.Pos()) << *values[0].GetLiteral();
        } else if (normalizedPragma == "classicdivision") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.Scoped->PragmaClassicDivision)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "ClassicDivision");
        } else if (normalizedPragma == "checkedops") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.Scoped->PragmaCheckedOps)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "CheckedOps");
        } else if (normalizedPragma == "disableunordered") {
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_PRAGMA)
                << "Use of deprecated DisableUnordered pragma. It will be dropped soon";
        } else if (normalizedPragma == "pullupflatmapoverjoin") {
            Ctx.PragmaPullUpFlatMapOverJoin = true;
            Ctx.IncrementMonCounter("sql_pragma", "PullUpFlatMapOverJoin");
        } else if (normalizedPragma == "disablepullupflatmapoverjoin") {
            Ctx.PragmaPullUpFlatMapOverJoin = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisablePullUpFlatMapOverJoin");
        } else if (normalizedPragma == "allowunnamedcolumns") {
            Ctx.WarnUnnamedColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "AllowUnnamedColumns");
        } else if (normalizedPragma == "warnunnamedcolumns") {
            Ctx.WarnUnnamedColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "WarnUnnamedColumns");
        } else if (normalizedPragma == "discoverymode") {
            Ctx.DiscoveryMode = true;
            Ctx.IncrementMonCounter("sql_pragma", "DiscoveryMode");
        } else if (normalizedPragma == "enablesystemcolumns") {
            if (values.size() != 1 || !values[0].GetLiteral() || !TryFromString(*values[0].GetLiteral(), Ctx.EnableSystemColumns)) {
                Error() << "Expected boolean literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "EnableSystemColumns");
        } else if (normalizedPragma == "ansiinforemptyornullableitemscollections") {
            Ctx.AnsiInForEmptyOrNullableItemsCollections = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiInForEmptyOrNullableItemsCollections");
        } else if (normalizedPragma == "disableansiinforemptyornullableitemscollections") {
            Ctx.AnsiInForEmptyOrNullableItemsCollections = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiInForEmptyOrNullableItemsCollections");
        } else if (normalizedPragma == "dqengine") {
            Ctx.IncrementMonCounter("sql_pragma", "DqEngine");
            if (values.size() != 1 || !values[0].GetLiteral()
                || ! (*values[0].GetLiteral() == "disable" || *values[0].GetLiteral() == "auto" || *values[0].GetLiteral() == "force"))
            {
                Error() << "Expected `disable|auto|force' argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            if (*values[0].GetLiteral() == "disable") {
                Ctx.DqEngineEnable = false;
                Ctx.DqEngineForce = false;
            } else if (*values[0].GetLiteral() == "force") {
                Ctx.DqEngineEnable = true;
                Ctx.DqEngineForce = true;
            } else if (*values[0].GetLiteral() == "auto") {
                Ctx.DqEngineEnable = true;
                Ctx.DqEngineForce = false;
            }
        } else if (normalizedPragma == "ansirankfornullablekeys") {
            Ctx.AnsiRankForNullableKeys = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiRankForNullableKeys");
        } else if (normalizedPragma == "disableansirankfornullablekeys") {
            Ctx.AnsiRankForNullableKeys = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiRankForNullableKeys");
        } else if (normalizedPragma == "ansiorderbylimitinunionall") {
            Ctx.AnsiOrderByLimitInUnionAll = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiOrderByLimitInUnionAll");
        } else if (!Ctx.EnforceAnsiOrderByLimitInUnionAll && normalizedPragma == "disableansiorderbylimitinunionall") {
            Ctx.AnsiOrderByLimitInUnionAll = false;
            Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_PRAGMA)
                << "Use of deprecated DisableAnsiOrderByLimitInUnionAll pragma. It will be dropped soon";
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiOrderByLimitInUnionAll");
        } else if (normalizedPragma == "ansioptionalas") {
            Ctx.AnsiOptionalAs = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiOptionalAs");
        } else if (normalizedPragma == "disableansioptionalas") {
            Ctx.AnsiOptionalAs = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiOptionalAs");
        } else if (normalizedPragma == "warnonansialiasshadowing") {
            Ctx.WarnOnAnsiAliasShadowing = true;
            Ctx.IncrementMonCounter("sql_pragma", "WarnOnAnsiAliasShadowing");
        } else if (normalizedPragma == "disablewarnonansialiasshadowing") {
            Ctx.WarnOnAnsiAliasShadowing = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableWarnOnAnsiAliasShadowing");
        } else if (normalizedPragma == "regexusere2") {
            if (values.size() != 1U || !values.front().GetLiteral() || !TryFromString(*values.front().GetLiteral(), Ctx.PragmaRegexUseRe2)) {
                Error() << "Expected 'true' or 'false' for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            Ctx.IncrementMonCounter("sql_pragma", "RegexUseRe2");
        } else if (normalizedPragma == "jsonqueryreturnsjsondocument") {
            Ctx.JsonQueryReturnsJsonDocument = true;
            Ctx.IncrementMonCounter("sql_pragma", "JsonQueryReturnsJsonDocument");
        } else if (normalizedPragma == "disablejsonqueryreturnsjsondocument") {
            Ctx.JsonQueryReturnsJsonDocument = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableJsonQueryReturnsJsonDocument");
        } else if (normalizedPragma == "orderedcolumns") {
            Ctx.OrderedColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "OrderedColumns");
        } else if (normalizedPragma == "disableorderedcolumns") {
            Ctx.OrderedColumns = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableOrderedColumns");
        } else if (normalizedPragma == "positionalunionall") {
            Ctx.PositionalUnionAll = true;
            // PositionalUnionAll implies OrderedColumns
            Ctx.OrderedColumns = true;
            Ctx.IncrementMonCounter("sql_pragma", "PositionalUnionAll");
        } else if (normalizedPragma == "pqreadby") {
            if (values.size() != 1 || !values[0].GetLiteral()) {
                Error() << "Expected string literal as a single argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            // special guard to raise error on situation:
            // use cluster1;
            // pragma PqReadPqBy="cluster2";
            const TString* currentClusterLiteral = Ctx.Scoped->CurrCluster.GetLiteral();
            if (currentClusterLiteral && *values[0].GetLiteral() != "dq" && *currentClusterLiteral != *values[0].GetLiteral()) {
                Error() << "Cluster in PqReadPqBy pragma differs from cluster specified in USE statement: " << *values[0].GetLiteral() << " != " << *currentClusterLiteral;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            Ctx.PqReadByRtmrCluster = *values[0].GetLiteral();
            Ctx.IncrementMonCounter("sql_pragma", "PqReadBy");
        } else if (normalizedPragma == "bogousstaringroupbyoverjoin") {
            Ctx.BogousStarInGroupByOverJoin = true;
            Ctx.IncrementMonCounter("sql_pragma", "BogousStarInGroupByOverJoin");
        } else if (normalizedPragma == "strictjoinkeytypes") {
            Ctx.Scoped->StrictJoinKeyTypes = true;
            Ctx.IncrementMonCounter("sql_pragma", "StrictJoinKeyTypes");
        } else if (normalizedPragma == "disablestrictjoinkeytypes") {
            Ctx.Scoped->StrictJoinKeyTypes = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableStrictJoinKeyTypes");
        } else if (normalizedPragma == "unorderedsubqueries") {
            Ctx.UnorderedSubqueries = true;
            Ctx.IncrementMonCounter("sql_pragma", "UnorderedSubqueries");
        } else if (normalizedPragma == "disableunorderedsubqueries") {
            Ctx.UnorderedSubqueries = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableUnorderedSubqueries");
        } else if (normalizedPragma == "datawatermarks") {
            if (values.size() != 1 || !values[0].GetLiteral()
                || ! (*values[0].GetLiteral() == "enable" || *values[0].GetLiteral() == "disable"))
            {
                Error() << "Expected `enable|disable' argument for: " << pragma;
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

            if (*values[0].GetLiteral() == "enable") {
                Ctx.PragmaDataWatermarks = true;
            } else if (*values[0].GetLiteral() == "disable") {
                Ctx.PragmaDataWatermarks = false;
            }

            Ctx.IncrementMonCounter("sql_pragma", "DataWatermarks");
        } else if (normalizedPragma == "flexibletypes") {
            Ctx.FlexibleTypes = true;
            Ctx.IncrementMonCounter("sql_pragma", "FlexibleTypes");
        } else if (normalizedPragma == "disableflexibletypes") {
            Ctx.FlexibleTypes = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableFlexibleTypes");
        } else if (normalizedPragma == "ansicurrentrow") {
            Ctx.AnsiCurrentRow = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiCurrentRow");
        } else if (normalizedPragma == "disableansicurrentrow") {
            Ctx.AnsiCurrentRow = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiCurrentRow");
        } else if (normalizedPragma == "emitaggapply") {
            Ctx.EmitAggApply = true;
            Ctx.IncrementMonCounter("sql_pragma", "EmitAggApply");
        } else if (normalizedPragma == "disableemitaggapply") {
            Ctx.EmitAggApply = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableEmitAggApply");
        } else if (normalizedPragma == "useblocks") {
            Ctx.UseBlocks = true;
            Ctx.IncrementMonCounter("sql_pragma", "UseBlocks");
        } else if (normalizedPragma == "disableuseblocks") {
            Ctx.UseBlocks = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableUseBlocks");
        } else if (normalizedPragma == "ansilike") {
            Ctx.AnsiLike = true;
            Ctx.IncrementMonCounter("sql_pragma", "AnsiLike");
        } else if (normalizedPragma == "disableansilike") {
            Ctx.AnsiLike = false;
            Ctx.IncrementMonCounter("sql_pragma", "DisableAnsiLike");
        } else {
            Error() << "Unknown pragma: " << pragma;
            Ctx.IncrementMonCounter("sql_errors", "UnknownPragma");
            return {};
        }
    } else {
        if (lowerPrefix == "yson") {
            if (!TopLevel) {
                Error() << "This pragma '" << pragma << "' is not allowed to be used in actions";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
            if (normalizedPragma == "fast") {
                Ctx.Warning(Ctx.Pos(), TIssuesIds::YQL_DEPRECATED_PRAGMA)
                    << "Use of deprecated yson.Fast pragma. It will be dropped soon";
                success = true;
                return {};
            } else if (normalizedPragma == "autoconvert") {
                Ctx.PragmaYsonAutoConvert = true;
                success = true;
                return {};
            } else if (normalizedPragma == "strict") {
                if (values.size() == 0U) {
                    Ctx.PragmaYsonStrict = true;
                    success = true;
                } else if (values.size() == 1U && values.front().GetLiteral() && TryFromString(*values.front().GetLiteral(), Ctx.PragmaYsonStrict)) {
                    success = true;
                } else {
                    Error() << "Expected 'true', 'false' or no parameter for: " << pragma;
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                }
                return {};
            } else if (normalizedPragma == "disablestrict") {
                if (values.size() == 0U) {
                    Ctx.PragmaYsonStrict = false;
                    success = true;
                    return {};
                }
                bool pragmaYsonDisableStrict;
                if (values.size() == 1U && values.front().GetLiteral() && TryFromString(*values.front().GetLiteral(), pragmaYsonDisableStrict)) {
                    Ctx.PragmaYsonStrict = !pragmaYsonDisableStrict;
                    success = true;
                } else {
                    Error() << "Expected 'true', 'false' or no parameter for: " << pragma;
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                }
                return {};
            } else if (normalizedPragma == "casttostring" || normalizedPragma == "disablecasttostring") {
                const bool allow = normalizedPragma == "casttostring";
                if (values.size() == 0U) {
                    Ctx.YsonCastToString = allow;
                    success = true;
                    return {};
                }
                bool pragmaYsonCastToString;
                if (values.size() == 1U && values.front().GetLiteral() && TryFromString(*values.front().GetLiteral(), pragmaYsonCastToString)) {
                    Ctx.PragmaYsonStrict = allow ? pragmaYsonCastToString : !pragmaYsonCastToString;
                    success = true;
                } else {
                    Error() << "Expected 'true', 'false' or no parameter for: " << pragma;
                    Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                }
                return {};
            } else {
                Error() << "Unknown pragma: '" << pragma << "'";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }

        } else if (std::find(Providers.cbegin(), Providers.cend(), lowerPrefix) == Providers.cend()) {
            if (!Ctx.HasCluster(prefix)) {
                Error() << "Unknown pragma prefix: " << prefix << ", please use cluster name or one of provider " <<
                    JoinRange(", ", Providers.cbegin(), Providers.cend());
                Ctx.IncrementMonCounter("sql_errors", "UnknownPragma");
                return {};
            }
        }

        if (normalizedPragma != "flags" && normalizedPragma != "packageversion") {
            if (values.size() > 1) {
                Error() << "Expected at most one value in the pragma";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
        } else {
            if (pragmaValueDefault || values.size() < 1) {
                Error() << "Expected at least one value in the pragma";
                Ctx.IncrementMonCounter("sql_errors", "BadPragmaValue");
                return {};
            }
        }

        success = true;
        Ctx.IncrementMonCounter("sql_pragma", pragma);
        return BuildPragma(Ctx.Pos(), lowerPrefix, normalizedPragma, values, pragmaValueDefault);
    }
    success = true;
    return {};
}

TNodePtr TSqlQuery::Build(const TRule_delete_stmt& stmt) {
    TTableRef table;
    if (!SimpleTableRefImpl(stmt.GetRule_simple_table_ref3(), table)) {
        return nullptr;
    }

    const bool isKikimr = table.Service == KikimrProviderName;
    if (!isKikimr) {
        Ctx.Error(GetPos(stmt.GetToken1())) << "DELETE is unsupported for " << table.Service;
        return nullptr;
    }

    TSourcePtr source = BuildTableSource(Ctx.Pos(), table);

    if (stmt.HasBlock4()) {
        switch (stmt.GetBlock4().Alt_case()) {
            case TRule_delete_stmt_TBlock4::kAlt1: {
                const auto& alt = stmt.GetBlock4().GetAlt1();

                TColumnRefScope scope(Ctx, EColumnRefState::Allow);
                TSqlExpression sqlExpr(Ctx, Mode);
                auto whereExpr = sqlExpr.Build(alt.GetRule_expr2());
                if (!whereExpr) {
                    return nullptr;
                }
                source->AddFilter(Ctx, whereExpr);
                break;
            }

            case TRule_delete_stmt_TBlock4::kAlt2: {
                const auto& alt = stmt.GetBlock4().GetAlt2();

                auto values = TSqlIntoValues(Ctx, Mode).Build(alt.GetRule_into_values_source2(), "DELETE ON");
                if (!values) {
                    return nullptr;
                }

                return BuildWriteColumns(Ctx.Pos(), Ctx.Scoped, table, EWriteColumnMode::DeleteOn, std::move(values));
            }

            default:
                return nullptr;
        }
    }

    return BuildDelete(Ctx.Pos(), Ctx.Scoped, table, std::move(source));
}

TNodePtr TSqlQuery::Build(const TRule_update_stmt& stmt) {
    TTableRef table;
    if (!SimpleTableRefImpl(stmt.GetRule_simple_table_ref2(), table)) {
        return nullptr;
    }

    const bool isKikimr = table.Service == KikimrProviderName;

    if (!isKikimr) {
        Ctx.Error(GetPos(stmt.GetToken1())) << "UPDATE is unsupported for " << table.Service;
        return nullptr;
    }

    switch (stmt.GetBlock3().Alt_case()) {
        case TRule_update_stmt_TBlock3::kAlt1: {
            const auto& alt = stmt.GetBlock3().GetAlt1();
            TSourcePtr values = Build(alt.GetRule_set_clause_choice2());
            auto source = BuildTableSource(Ctx.Pos(), table);

            if (alt.HasBlock3()) {
                TColumnRefScope scope(Ctx, EColumnRefState::Allow);
                TSqlExpression sqlExpr(Ctx, Mode);
                auto whereExpr = sqlExpr.Build(alt.GetBlock3().GetRule_expr2());
                if (!whereExpr) {
                    return nullptr;
                }
                source->AddFilter(Ctx, whereExpr);
            }

            return BuildUpdateColumns(Ctx.Pos(), Ctx.Scoped, table, std::move(values), std::move(source));
        }

        case TRule_update_stmt_TBlock3::kAlt2: {
            const auto& alt = stmt.GetBlock3().GetAlt2();

            auto values = TSqlIntoValues(Ctx, Mode).Build(alt.GetRule_into_values_source2(), "UPDATE ON");
            if (!values) {
                return nullptr;
            }

            return BuildWriteColumns(Ctx.Pos(), Ctx.Scoped, table, EWriteColumnMode::UpdateOn, std::move(values));
        }

        default:
            return nullptr;
    }
}

TSourcePtr TSqlQuery::Build(const TRule_set_clause_choice& stmt) {
    switch (stmt.Alt_case()) {
        case TRule_set_clause_choice::kAltSetClauseChoice1:
            return Build(stmt.GetAlt_set_clause_choice1().GetRule_set_clause_list1());
        case TRule_set_clause_choice::kAltSetClauseChoice2:
            return Build(stmt.GetAlt_set_clause_choice2().GetRule_multiple_column_assignment1());
        default:
            AltNotImplemented("set_clause_choice", stmt);
            return nullptr;
    }
}

bool TSqlQuery::FillSetClause(const TRule_set_clause& node, TVector<TString>& targetList, TVector<TNodePtr>& values) {
    targetList.push_back(ColumnNameAsSingleStr(*this, node.GetRule_set_target1().GetRule_column_name1()));
    TColumnRefScope scope(Ctx, EColumnRefState::Allow);
    TSqlExpression sqlExpr(Ctx, Mode);
    if (!Expr(sqlExpr, values, node.GetRule_expr3())) {
        return false;
    }
    return true;
}

TSourcePtr TSqlQuery::Build(const TRule_set_clause_list& stmt) {
    TVector<TString> targetList;
    TVector<TNodePtr> values;
    const TPosition pos(Ctx.Pos());
    if (!FillSetClause(stmt.GetRule_set_clause1(), targetList, values)) {
        return nullptr;
    }
    for (auto& block: stmt.GetBlock2()) {
        if (!FillSetClause(block.GetRule_set_clause2(), targetList, values)) {
            return nullptr;
        }
    }
    Y_VERIFY_DEBUG(targetList.size() == values.size());
    return BuildUpdateValues(pos, targetList, values);
}

TSourcePtr TSqlQuery::Build(const TRule_multiple_column_assignment& stmt) {
    TVector<TString> targetList;
    FillTargetList(*this, stmt.GetRule_set_target_list1(), targetList);
    auto simpleValuesNode = stmt.GetRule_simple_values_source4();
    const TPosition pos(Ctx.Pos());
    switch (simpleValuesNode.Alt_case()) {
        case TRule_simple_values_source::kAltSimpleValuesSource1: {
            TVector<TNodePtr> values;
            TSqlExpression sqlExpr(Ctx, Mode);
            if (!ExprList(sqlExpr, values, simpleValuesNode.GetAlt_simple_values_source1().GetRule_expr_list1())) {
                return nullptr;
            }
            return BuildUpdateValues(pos, targetList, values);
        }
        case TRule_simple_values_source::kAltSimpleValuesSource2: {
            TSqlSelect select(Ctx, Mode);
            TPosition selectPos;
            auto source = select.Build(simpleValuesNode.GetAlt_simple_values_source2().GetRule_select_stmt1(), selectPos);
            if (!source) {
                return nullptr;
            }
            return BuildWriteValues(pos, "UPDATE", targetList, std::move(source));
        }
        default:
            Ctx.IncrementMonCounter("sql_errors", "UnknownSimpleValuesSourceAlt");
            AltNotImplemented("simple_values_source", simpleValuesNode);
            return nullptr;
    }
}

TNodePtr TSqlQuery::Build(const TSQLv1ParserAST& ast) {
    if (Mode == NSQLTranslation::ESqlMode::QUERY) {
        // inject externally declared named expressions
        for (auto [name, type] : Ctx.Settings.DeclaredNamedExprs) {
            if (name.empty()) {
                Error() << "Empty names for externally declared expressions are not allowed";
                return nullptr;
            }
            TString varName = "$" + name;
            if (IsAnonymousName(varName)) {
                Error() << "Externally declared name '" << name << "' is anonymous";
                return nullptr;
            }

            auto parsed = ParseType(type, *Ctx.Pool, Ctx.Issues, Ctx.Pos());
            if (!parsed) {
                Error() << "Failed to parse type for externally declared name '" << name << "'";
                return nullptr;
            }

            TNodePtr typeNode = BuildBuiltinFunc(Ctx, Ctx.Pos(), "ParseType", { BuildLiteralRawString(Ctx.Pos(), type) });
            PushNamedAtom(Ctx.Pos(), varName);
            // no duplicates are possible at this stage
            bool isWeak = true;
            Ctx.DeclareVariable(varName, typeNode, isWeak);
            // avoid 'Symbol is not used' warning for externally declared expression
            YQL_ENSURE(GetNamedNode(varName));
        }
    }

    const auto& query = ast.GetRule_sql_query();
    TVector<TNodePtr> blocks;
    Ctx.PushCurrentBlocks(&blocks);
    Y_DEFER {
        Ctx.PopCurrentBlocks();
    };
    if (query.Alt_case() == TRule_sql_query::kAltSqlQuery1) {
        const auto& statements = query.GetAlt_sql_query1().GetRule_sql_stmt_list1();
        if (!Statement(blocks, statements.GetRule_sql_stmt2().GetRule_sql_stmt_core2())) {
            return nullptr;
        }
        for (auto block: statements.GetBlock3()) {
            if (!Statement(blocks, block.GetRule_sql_stmt2().GetRule_sql_stmt_core2())) {
                return nullptr;
            }
        }
    }

    ui32 topLevelSelects = 0;
    for (auto& block : blocks) {
        if (block->IsSelect()) {
            ++topLevelSelects;
        }
    }

    if ((Mode == NSQLTranslation::ESqlMode::SUBQUERY || Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW) && topLevelSelects != 1) {
        Error() << "Strictly one select/process/reduce statement must be used in the "
            << (Mode == NSQLTranslation::ESqlMode::LIMITED_VIEW ? "view" : "subquery");
        return nullptr;
    }

    if (!Ctx.PragmaAutoCommit && Ctx.Settings.EndOfQueryCommit && IsQueryMode(Mode)) {
        AddStatementToBlocks(blocks, BuildCommitClusters(Ctx.Pos()));
    }

    auto result = BuildQuery(Ctx.Pos(), blocks, true, Ctx.Scoped);
    WarnUnusedNodes();
    return result;
}

bool TSqlTranslation::DefineActionOrSubqueryBody(TSqlQuery& query, TBlocks& blocks, const TRule_define_action_or_subquery_body& body) {
    if (body.HasBlock2()) {
        Ctx.PushCurrentBlocks(&blocks);
        Y_DEFER {
            Ctx.PopCurrentBlocks();
        };
        if (!query.Statement(blocks, body.GetBlock2().GetRule_sql_stmt_core1())) {
            return false;
        }

        for (const auto& nestedStmtItem : body.GetBlock2().GetBlock2()) {
            const auto& nestedStmt = nestedStmtItem.GetRule_sql_stmt_core2();
            if (!query.Statement(blocks, nestedStmt)) {
                return false;
            }
        }
    }

    return true;
}

bool TSqlTranslation::DefineActionOrSubqueryStatement(const TRule_define_action_or_subquery_stmt& stmt) {
    auto kind = Ctx.Token(stmt.GetToken2());
    const bool isSubquery = to_lower(kind) == "subquery";
    if (!isSubquery && Mode == NSQLTranslation::ESqlMode::SUBQUERY) {
        Error() << "Definition of actions is not allowed in the subquery";
        return false;
    }

    TString actionName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter3(), actionName, *this)) {
        return false;
    }
    if (IsAnonymousName(actionName)) {
        Error() << "Can not use anonymous name '" << actionName << "' as " << to_upper(kind) << " name";
        return false;
    }
    TPosition actionNamePos = Ctx.Pos();

    TVector<TSymbolNameWithPos> argNames;
    ui32 optionalArgumentsCount = 0;
    if (stmt.HasBlock5() && !ActionOrSubqueryArgs(stmt.GetBlock5().GetRule_action_or_subquery_args1(), argNames, optionalArgumentsCount)) {
        return false;
    }

    auto saveScoped = Ctx.Scoped;
    Ctx.Scoped = MakeIntrusive<TScopedState>();
    Ctx.AllScopes.push_back(Ctx.Scoped);
    *Ctx.Scoped = *saveScoped;
    Ctx.Scoped->Local = TScopedState::TLocal{};
    Ctx.ScopeLevel++;

    for (auto& arg : argNames) {
        arg.Name = PushNamedAtom(arg.Pos, arg.Name);
    }

    auto saveMode = Ctx.Settings.Mode;
    if (isSubquery) {
        Ctx.Settings.Mode = NSQLTranslation::ESqlMode::SUBQUERY;
    }

    TSqlQuery query(Ctx, Ctx.Settings.Mode, false);
    TBlocks innerBlocks;
    const bool hasValidBody = DefineActionOrSubqueryBody(query, innerBlocks, stmt.GetRule_define_action_or_subquery_body8());

    ui32 topLevelSelects = 0;
    for (auto& block : innerBlocks) {
        if (block->IsSelect()) {
            ++topLevelSelects;
        }
    }

    if (isSubquery && topLevelSelects != 1) {
        Error() << "Strictly one select/process/reduce statement must be used in the subquery, got: " << topLevelSelects;
        return false;
    }

    auto ret = hasValidBody ? BuildQuery(Ctx.Pos(), innerBlocks, false, Ctx.Scoped) : nullptr;
    WarnUnusedNodes();
    Ctx.Scoped = saveScoped;
    Ctx.ScopeLevel--;
    Ctx.Settings.Mode = saveMode;

    if (!ret) {
        return false;
    }

    TNodePtr blockNode = new TAstListNodeImpl(Ctx.Pos());
    blockNode->Add("block");
    blockNode->Add(blockNode->Q(ret));

    TNodePtr params = new TAstListNodeImpl(Ctx.Pos());
    params->Add("world");
    for (const auto& arg : argNames) {
        params->Add(BuildAtom(arg.Pos, arg.Name));
    }

    auto lambda = BuildLambda(Ctx.Pos(), params, blockNode);
    if (optionalArgumentsCount > 0) {
        lambda = new TCallNodeImpl(Ctx.Pos(), "WithOptionalArgs", {
            lambda,
            BuildQuotedAtom(Ctx.Pos(), ToString(optionalArgumentsCount), TNodeFlags::Default)
            });
    }

    PushNamedNode(actionNamePos, actionName, lambda);
    return true;
}

TNodePtr TSqlTranslation::IfStatement(const TRule_if_stmt& stmt) {
    bool isEvaluate = stmt.HasBlock1();
    TSqlExpression expr(Ctx, Mode);
    auto exprNode = expr.Build(stmt.GetRule_expr3());
    if (!exprNode) {
        return {};
    }

    auto thenNode = DoStatement(stmt.GetRule_do_stmt4(), isEvaluate);
    if (!thenNode) {
        return {};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock5()) {
        elseNode = DoStatement(stmt.GetBlock5().GetRule_do_stmt2(), isEvaluate);
        if (!elseNode) {
            return {};
        }
    }

    return BuildWorldIfNode(Ctx.Pos(), exprNode, thenNode, elseNode, isEvaluate);
}

TNodePtr TSqlTranslation::ForStatement(const TRule_for_stmt& stmt) {
    bool isEvaluate = stmt.HasBlock1();
    TSqlExpression expr(Ctx, Mode);
    TString itemArgName;
    if (!NamedNodeImpl(stmt.GetRule_bind_parameter3(), itemArgName, *this)) {
        return {};
    }
    TPosition itemArgNamePos = Ctx.Pos();

    auto exprNode = expr.Build(stmt.GetRule_expr5());
    if (!exprNode) {
        return{};
    }

    itemArgName = PushNamedAtom(itemArgNamePos, itemArgName);
    auto bodyNode = DoStatement(stmt.GetRule_do_stmt6(), true, { itemArgName });
    PopNamedNode(itemArgName);
    if (!bodyNode) {
        return{};
    }

    TNodePtr elseNode;
    if (stmt.HasBlock7()) {
        elseNode = DoStatement(stmt.GetBlock7().GetRule_do_stmt2(), true);
        if (!elseNode) {
            return{};
        }
    }

    return BuildWorldForNode(Ctx.Pos(), exprNode, bodyNode, elseNode, isEvaluate);
}

TAstNode* SqlASTToYql(const google::protobuf::Message& protoAst, TContext& ctx) {
    const google::protobuf::Descriptor* d = protoAst.GetDescriptor();
    if (d && d->name() != "TSQLv1ParserAST") {
        ctx.Error() << "Invalid AST structure: " << d->name() << ", expected TSQLv1ParserAST";
        return nullptr;
    }
    TSqlQuery query(ctx, ctx.Settings.Mode, true);
    TNodePtr node(query.Build(static_cast<const TSQLv1ParserAST&>(protoAst)));
    try {
        if (node && node->Init(ctx, nullptr)) {
            return node->Translate(ctx);
        }
    } catch (const NProtoAST::TTooManyErrors&) {
        // do not add error issue, no room for it
    }

    return nullptr;
}

void SqlASTToYqlImpl(NYql::TAstParseResult& res, const google::protobuf::Message& protoAst,
        TContext& ctx) {
    YQL_ENSURE(!ctx.Issues.Size());
    res.Root = SqlASTToYql(protoAst, ctx);
    res.Pool = std::move(ctx.Pool);
    if (!res.Root) {
        if (ctx.Issues.Size()) {
            ctx.IncrementMonCounter("sql_errors", "AstToYqlError");
        } else {
            ctx.IncrementMonCounter("sql_errors", "AstToYqlSilentError");
            ctx.Error() << "Error occurred on parse SQL query, but no error is collected" <<
                ", please send this request over bug report into YQL interface or write on yql@ maillist";
        }
    } else {
        ctx.WarnUnusedHints();
    }
}

NYql::TAstParseResult SqlASTToYql(const google::protobuf::Message& protoAst,
    const NSQLTranslation::TSQLHints& hints,
    const NSQLTranslation::TTranslationSettings& settings)
{
    YQL_ENSURE(IsQueryMode(settings.Mode));
    TAstParseResult res;
    TContext ctx(settings, hints, res.Issues);
    SqlASTToYqlImpl(res, protoAst, ctx);
    return res;
}

NYql::TAstParseResult SqlToYql(const TString& query, const NSQLTranslation::TTranslationSettings& settings, NYql::TWarningRules* warningRules)
{
    TAstParseResult res;
    const TString queryName = "query";

    NSQLTranslation::TSQLHints hints;
    auto lexer = MakeLexer(settings.AnsiLexer);
    YQL_ENSURE(lexer);
    if (!CollectSqlHints(*lexer, query, queryName, settings.File, hints, res.Issues, settings.MaxErrors)) {
        return res;
    }

    TContext ctx(settings, hints, res.Issues);
    NSQLTranslation::TErrorCollectorOverIssues collector(res.Issues, settings.MaxErrors, settings.File);

    google::protobuf::Message* ast(SqlAST(query, queryName, collector, settings.AnsiLexer, settings.Arena));
    if (ast) {
        SqlASTToYqlImpl(res, *ast, ctx);
    } else {
        ctx.IncrementMonCounter("sql_errors", "AstError");
    }
    if (warningRules) {
        *warningRules = ctx.WarningPolicy.GetRules();
        ctx.WarningPolicy.Clear();
    }
    return res;
}

bool TSqlTranslation::BindParameterClause(const TRule_bind_parameter& node, TDeferredAtom& result) {
    TString paramName;
    if (!NamedNodeImpl(node, paramName, *this)) {
        return false;
    }
    auto named = GetNamedNode(paramName);
    if (!named) {
        return false;
    }

    result = MakeAtomFromExpression(Ctx, named);
    return true;
}

bool TSqlTranslation::ObjectFeatureValueClause(const TRule_object_feature_value& node, TDeferredAtom& result) {
    // object_feature_value: an_id_or_type | bind_parameter;
    switch (node.Alt_case()) {
        case TRule_object_feature_value::kAltObjectFeatureValue1:
        {
            TString name = Id(node.GetAlt_object_feature_value1().GetRule_an_id_or_type1(), *this);
            result = TDeferredAtom(Ctx.Pos(), name);
            break;
        }
        case TRule_object_feature_value::kAltObjectFeatureValue2:
        {
            if (!BindParameterClause(node.GetAlt_object_feature_value2().GetRule_bind_parameter1(), result)) {
                return false;
            }
            break;
        }
        default:
            Y_FAIL("You should change implementation according to grammar changes");
    }
    return true;
}

bool TSqlTranslation::AddObjectFeature(std::map<TString, TDeferredAtom>& result, const TRule_object_feature& feature) {
    if (feature.has_alt_object_feature1()) {
        auto& kv = feature.GetAlt_object_feature1().GetRule_object_feature_kv1();
        const TString& key = Id(kv.GetRule_an_id_or_type1(), *this);
        auto& ruleValue = kv.GetRule_object_feature_value3();
        TDeferredAtom value;
        if (!ObjectFeatureValueClause(ruleValue, value)) {
            return false;
        }
        result[key] = value;
    } else if (feature.has_alt_object_feature2()) {
        result[Id(feature.GetAlt_object_feature2().GetRule_object_feature_flag1().GetRule_an_id_or_type1(), *this)] = TDeferredAtom();
    }
    return true;
}

bool TSqlTranslation::ParseObjectFeatures(std::map<TString, TDeferredAtom>& result, const TRule_object_features& features) {
    if (features.has_alt_object_features1()) {
        if (!AddObjectFeature(result, features.alt_object_features1().GetRule_object_feature1())) {
            return false;
        }

    } else if (features.has_alt_object_features2()) {
        if (!AddObjectFeature(result, features.alt_object_features2().GetRule_object_feature2())) {
            return false;
        }
        for (auto&& i : features.alt_object_features2().GetBlock3()) {
            if (!AddObjectFeature(result, i.GetRule_object_feature2())) {
                return false;
            }
        }
    } else {
        return false;
    }
    return true;
}

bool TSqlTranslation::StoreDataSourceSettingsEntry(const TIdentifier& id, const TRule_table_setting_value* value, std::map<TString, TDeferredAtom>& result) {
    YQL_ENSURE(value);

    const TString key = to_lower(id.Name);
    if (result.find(key) != result.end()) {
        Ctx.Error() << to_upper(key) << " duplicate keys";
        return false;
    }

    if (IsIn({"source_type", "installation", "location", "auth_method"}, key)) {
        if (!StoreString(*value, result[key], Ctx, to_upper(key))) {
            return false;
        }
    } else {
        Ctx.Error() << "Unknown external data source setting: " << id.Name;
        return false;
    }
    return true;
}

bool TSqlTranslation::ParseExternalDataSourceSettings(std::map<TString, TDeferredAtom>& result, const TRule_with_table_settings& settingsNode) {
    const auto& firstEntry = settingsNode.GetRule_table_settings_entry3();
    if (!StoreDataSourceSettingsEntry(IdEx(firstEntry.GetRule_an_id1(), *this), &firstEntry.GetRule_table_setting_value3(),
            result)) {
        return false;
    }
    for (auto& block : settingsNode.GetBlock4()) {
        const auto& entry = block.GetRule_table_settings_entry2();
        if (!StoreDataSourceSettingsEntry(IdEx(entry.GetRule_an_id1(), *this), &entry.GetRule_table_setting_value3(), result)) {
            return false;
        }
    }

    if (result.find("source_type") == result.end()) {
        Ctx.Error() << "SOURCE_TYPE requires key";
        return false;
    }
    if (result.find("auth_method") == result.end()) {
        Ctx.Error() << "AUTH_METHOD requires key";
        return false;
    }
    if (result.find("installation") == result.end() && result.find("location") == result.end()) {
        Ctx.Error() << "INSTALLATION or LOCATION must be specified";
        return false;
    }
    return true;
}

bool TSqlTranslation::ValidateExternalTable(const TCreateTableParameters& params) {
    if (params.TableType != ETableType::ExternalTable) {
        return true;
    }

    if (!params.TableSettings.DataSourcePath) {
        Ctx.Error() << "DATA_SOURCE requires key";
        return false;
    }

    if (!params.TableSettings.Location) {
        Ctx.Error() << "LOCATION requires key";
        return false;
    }

    if (params.PkColumns) {
        Ctx.Error() << "PRIMARY KEY is not supported for external table";
        return false;
    }

    return true;
}

} // namespace NSQLTranslationV1
