#include "proto_parser.h"

#include <yql/essentials/utils/yql_panic.h>

#include <yql/essentials/parser/proto_ast/antlr3/proto_ast_antlr3.h>
#include <yql/essentials/parser/proto_ast/antlr4/proto_ast_antlr4.h>
#include <yql/essentials/parser/proto_ast/collect_issues/collect_issues.h>
#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1/SQLv1Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi/SQLv1Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi/SQLv1Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Parser.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Lexer.h>
#include <yql/essentials/parser/proto_ast/gen/v1_ansi_antlr4/SQLv1Antlr4Parser.h>

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split/SQLv1Parser.pb.main.h>

#include <library/cpp/protobuf/util/simple_reflection.h>
#include <util/generic/algorithm.h>

#if defined(_tsan_enabled_)
#include <util/system/mutex.h>
#endif

using namespace NYql;

namespace NSQLTranslationV1 {


#if defined(_tsan_enabled_)
    TMutex SanitizerSQLTranslationMutex;
#endif

using namespace NSQLv1Generated;

void ValidateMessagesImpl(const google::protobuf::Message* msg1, const google::protobuf::Message* msg2, bool hasNonAscii) {
    YQL_ENSURE(!msg1 == !msg2);
    if (!msg1) {
        return;
    }

    YQL_ENSURE(msg1->GetDescriptor() == msg2->GetDescriptor());
    const auto descr = msg1->GetDescriptor();
    if (descr == NSQLv1Generated::TToken::GetDescriptor()) {
        const auto& token1 = dynamic_cast<const NSQLv1Generated::TToken&>(*msg1);
        const auto& token2 = dynamic_cast<const NSQLv1Generated::TToken&>(*msg2);
        const bool isEof1 = token1.GetId() == Max<ui32>();
        const bool isEof2 = token2.GetId() == Max<ui32>();
        YQL_ENSURE(isEof1 == isEof2);
        YQL_ENSURE(token1.GetValue() == token2.GetValue());
        if (!isEof1) {
            YQL_ENSURE(token1.GetLine() == token2.GetLine());
            if (!hasNonAscii) {
                YQL_ENSURE(token1.GetColumn() == token2.GetColumn());
            }
        }

        return;
    }

    for (int i = 0; i < descr->field_count(); ++i) {
        const NProtoBuf::FieldDescriptor* fd = descr->field(i);
        NProtoBuf::TConstField field1(*msg1, fd);
        NProtoBuf::TConstField field2(*msg2, fd);
        YQL_ENSURE(field1.IsMessage() == field2.IsMessage());
        if (field1.IsMessage()) {
            YQL_ENSURE(field1.Size() == field2.Size());
            for (size_t j = 0; j < field1.Size(); ++j) {
                ValidateMessagesImpl(field1.template Get<NProtoBuf::Message>(j), field2.template Get<NProtoBuf::Message>(j), hasNonAscii);
            }
        }
    }
}

void ValidateMessages(const TString& query, const google::protobuf::Message* msg1, const google::protobuf::Message* msg2) {
    const bool hasNonAscii = AnyOf(query, [](char c) { return !isascii(c);});
    return ValidateMessagesImpl(msg1, msg2, hasNonAscii);
}

google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, TIssues& err,
    size_t maxErrors, bool ansiLexer, bool anlr4Parser, bool testAntlr4, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    NSQLTranslation::TErrorCollectorOverIssues collector(err, maxErrors, queryName);
    if (ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder3<NALPAnsi::SQLv1Parser, NALPAnsi::SQLv1Lexer> builder(query, queryName, arena);
        auto res = builder.BuildAST(collector);
        if (testAntlr4) {
            NProtoAST::TProtoASTBuilder4<NALPAnsiAntlr4::SQLv1Antlr4Parser, NALPAnsiAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
            auto res2 = builder.BuildAST(collector);
            ValidateMessages(query, res, res2);
        }

        return res;
    } else if (!ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder3<NALPDefault::SQLv1Parser, NALPDefault::SQLv1Lexer> builder(query, queryName, arena);
        auto res = builder.BuildAST(collector);
        if (testAntlr4) {
            NProtoAST::TProtoASTBuilder4<NALPDefaultAntlr4::SQLv1Antlr4Parser, NALPDefaultAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
            auto res2 = builder.BuildAST(collector);
            ValidateMessages(query, res, res2);
        }

        return res;
    } else if (ansiLexer && anlr4Parser) {
        NProtoAST::TProtoASTBuilder4<NALPAnsiAntlr4::SQLv1Antlr4Parser, NALPAnsiAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(collector);
    } else {
        NProtoAST::TProtoASTBuilder4<NALPDefaultAntlr4::SQLv1Antlr4Parser, NALPDefaultAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(collector);
    }
}

google::protobuf::Message* SqlAST(const TString& query, const TString& queryName, NProtoAST::IErrorCollector& err,
    bool ansiLexer, bool anlr4Parser, bool testAntlr4, google::protobuf::Arena* arena) {
    YQL_ENSURE(arena);
#if defined(_tsan_enabled_)
    TGuard<TMutex> grd(SanitizerSQLTranslationMutex);
#endif
    if (ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder3<NALPAnsi::SQLv1Parser, NALPAnsi::SQLv1Lexer> builder(query, queryName, arena);
        auto res = builder.BuildAST(err);
        if (testAntlr4) {
            NProtoAST::TProtoASTBuilder4<NALPAnsiAntlr4::SQLv1Antlr4Parser, NALPAnsiAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
            auto res2 = builder.BuildAST(err);
            ValidateMessages(query, res, res2);
        }

        return res;
    } else if (!ansiLexer && !anlr4Parser) {
        NProtoAST::TProtoASTBuilder3<NALPDefault::SQLv1Parser, NALPDefault::SQLv1Lexer> builder(query, queryName, arena);
        auto res = builder.BuildAST(err);
        if (testAntlr4) {
            NProtoAST::TProtoASTBuilder4<NALPDefaultAntlr4::SQLv1Antlr4Parser, NALPDefaultAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
            auto res2 =  builder.BuildAST(err);
            ValidateMessages(query, res, res2);
        }

        return res;
    } else if (ansiLexer && anlr4Parser) {
        NProtoAST::TProtoASTBuilder4<NALPAnsiAntlr4::SQLv1Antlr4Parser, NALPAnsiAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    } else {
        NProtoAST::TProtoASTBuilder4<NALPDefaultAntlr4::SQLv1Antlr4Parser, NALPDefaultAntlr4::SQLv1Antlr4Lexer> builder(query, queryName, arena);
        return builder.BuildAST(err);
    }
}

} // namespace NSQLTranslationV1
