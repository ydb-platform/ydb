#pragma once

#include "reflection.h"

#include <yql/essentials/parser/proto_ast/gen/v1_proto_split_antlr4/SQLv1Antlr4Parser.pb.main.h>

#include <google/protobuf/message.h>

#include <util/generic/string.h>

namespace NSQLTranslationV1 {

using namespace NSQLv1Generated;

struct TStatementName {
    TString Internal;
    TString Human;

    static TStatementName FromAltDescription(const TString& alt);

    template <typename T>
    static TStatementName From(const T& node) {
        return FromAltDescription(AltDescription(node));
    }
};

TVector<TStatementName> StatementNames(const TRule_sql_query& rule);

} // namespace NSQLTranslationV1
