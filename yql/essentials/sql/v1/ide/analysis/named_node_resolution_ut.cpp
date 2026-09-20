#include <yql/essentials/sql/v1/ide/analysis/named_node_resolution.h>

#include <yql/essentials/sql/v1/ide/pure_ast/parser.h>

#include <yql/essentials/utils/string/trim_indent.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLPureAST;
using NYql::TrimIndent;

Y_UNIT_TEST_SUITE(NamedNodeTests) {

INamedNodes::TPtr Resolve(TString query) {
    query = TrimIndent(query);
    return ResolveNamedNodes(MakeParser(/* isAnsiLexer = */ false)->Parse(query), {});
}

void DumpEntries(
    IOutputStream& out,
    const INamedNodes& nodes,
    const INamedNodeScope& scope,
    size_t depth)
{
    const TString indent(depth, ' ');
    for (const INamedNodeScope::TEntry& entry : scope.Entries()) {
        std::visit([&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TNamedNodeRef>) {
                INamedNodeDef::TPtr def = nodes.Definition(value);
                UNIT_ASSERT_C(def, "Reference is missing its named-node definition");
                out << indent << value << " refers to " << def->Decl() << '\n';
            } else if constexpr (std::is_same_v<T, INamedNodeDef::TPtr>) {
                out << indent << value->Decl() << " definition\n";
            } else {
                if (value->Owner().IsWildcard()) {
                    out << indent << "anonymous scope\n";
                } else {
                    out << indent << "scope of " << value->Owner() << '\n';
                }
                DumpEntries(out, nodes, *value, depth + 1);
            }
        }, entry);
    }
}

void Test(TString query, TString expected) {
    expected = TrimIndent(expected + '\n');
    auto nodes = Resolve(std::move(query));

    TStringStream stream;
    DumpEntries(stream, *nodes, *nodes->TopLevel(), 0);
    UNIT_ASSERT_NO_DIFF(stream.Str(), expected);
}

Y_UNIT_TEST(SingleDefinition) {
    Test(R"sql(
        $x = 'hello';
        #
    )sql", R"(
        1:0:x definition
    )");
}

Y_UNIT_TEST(MultipleDistinctDefinitions) {
    Test(R"sql(
        $a = 'first';
        $b = 'second';
        #
    )sql", R"(
        1:0:a definition
        2:0:b definition
    )");
}

Y_UNIT_TEST(RedefinedVariableAppearsOnce) {
    Test(R"sql(
        $x = 'first';
        $x = 'second';
        #
    )sql", R"(
        1:0:x definition
        2:0:x definition
    )");
}

Y_UNIT_TEST(SelfReferenceWithNoPreviousDefinition) {
    Test(R"sql(
        $logs = (FROM $logs SELECT a);
        #
    )sql", R"(
        1:0:logs definition
    )");
}

Y_UNIT_TEST(RedefinitionWithSelfReference) {
    Test(R"sql(
        $base = '/home';
        $source = $base || '/yql';
        $source = $source || '/1';
        SELECT # FROM $source;
    )sql", R"(
        1:0:base definition
        2:0:source definition
        2:10:base refers to 1:0:base
        3:0:source definition
        3:10:source refers to 2:0:source
        4:14:source refers to 3:0:source
    )");
}

Y_UNIT_TEST(ReferencesResolveToCorrectDefinition) {
    Test(R"sql(
        $base = 'home/yql/tutorial';
        $logs = $base || '/users';
        $logs = (FROM $logs SELECT a);
        #
    )sql", R"(
        1:0:base definition
        2:0:logs definition
        2:8:base refers to 1:0:base
        3:0:logs definition
        3:14:logs refers to 2:0:logs
    )");
}

Y_UNIT_TEST(Declare) {
    Test(R"sql(
        DECLARE $x AS String;
        #
    )sql", R"(
        1:8:x definition
    )");
}

Y_UNIT_TEST(Import) {
    Test(R"sql(
        IMPORT math SYMBOLS $sqrt, $pow;
        #
    )sql", R"(
        1:20:sqrt definition
        1:27:pow definition
    )");
}

Y_UNIT_TEST(MultiBinding) {
    Test(R"sql(
        $first, $second, $_ = AsTuple(1, 2, 3);
        #
    )sql", R"(
        1:0:first definition
        1:8:second definition
    )");
}

Y_UNIT_TEST(DefineAction) {
    Test(R"sql(
        DEFINE ACTION $greet($name) AS
            SELECT "Hello, " || $name || "!";
        END DEFINE;
        #
    )sql", R"(
        1:14:greet definition
        scope of 1:14:greet
         1:21:name definition
         2:24:name refers to 1:21:name
    )");
}

Y_UNIT_TEST(EvaluateFor) {
    Test(R"sql(
        $init = 0;
        EVALUATE FOR $i IN AsList(1, 2, 3) DO BEGIN
            $acc = $init;
            #
        END DO;
    )sql", R"(
        1:0:init definition
        anonymous scope
         2:13:i definition
         3:4:acc definition
         3:11:init refers to 1:0:init
    )");
}

Y_UNIT_TEST(LambdaUncurryIll) {
    Test(R"sql(
        $f = ($a, $b) -> $a + $b;
        #
    )sql", R"(
        1:0:f definition
        anonymous scope
         1:6:a definition
         1:10:b definition
    )");
}

Y_UNIT_TEST(LambdaUncurryWell) {
    Test(R"sql(
        $f = ($a, $b) -> ($a + $b);
        #
    )sql", R"(
        1:0:f definition
        scope of 1:0:f
         1:6:a definition
         1:10:b definition
         1:18:a refers to 1:6:a
         1:23:b refers to 1:10:b
    )");
}

Y_UNIT_TEST(LambdaCurryIll) {
    Test(R"sql(
        $f = ($a) -> (($b) -> ($a + # + $b));
    )sql", R"(
        1:0:f definition
        scope of 1:0:f
         1:6:a definition
         anonymous scope
          1:15:b definition
          1:23:a refers to 1:6:a
          1:32:b refers to 1:15:b
    )");
}

Y_UNIT_TEST(LambdaCurryWell) {
    Test(R"sql(
        $f = ($a) -> (($b) -> ($a + $b));
    )sql", R"(
        1:0:f definition
        scope of 1:0:f
         1:6:a definition
         anonymous scope
          1:15:b definition
          1:23:a refers to 1:6:a
          1:28:b refers to 1:15:b
    )");
}

Y_UNIT_TEST(LambdaCurryBodyIll) {
    Test(R"sql(
        $f = ($a) -> (($b) -> { RETURN $a + # + $b; });
    )sql", R"(
        1:0:f definition
        scope of 1:0:f
         1:6:a definition
         anonymous scope
          1:15:b definition
          1:31:a refers to 1:6:a
          1:40:b refers to 1:15:b
    )");
}

Y_UNIT_TEST(LambdaMap) {
    Test(R"sql(
        $result = ListMap(lst, ($x) -> (#));
    )sql", R"(
        1:0:result definition
        anonymous scope
         1:24:x definition
    )");
}

Y_UNIT_TEST(IncompleteUnaryExpression) {
    Test(R"sql(
        $x = +;
    )sql", R"(
        1:0:x definition
    )");
}

Y_UNIT_TEST(ReferenceChain) {
    Test(R"sql(
        $a = 'base';
        $b = $a || '/level1';
        $c = $b || '/level2';
        #
    )sql", R"(
        1:0:a definition
        2:0:b definition
        2:5:a refers to 1:0:a
        3:0:c definition
        3:5:b refers to 2:0:b
    )");
}

Y_UNIT_TEST(NamedSubquerySelect) {
    Test(R"sql(
        $subquery = (SELECT * FROM example.`/data`);
        SELECT # FROM $subquery;
    )sql", R"(
        1:0:subquery definition
        2:14:subquery refers to 1:0:subquery
    )");
}

Y_UNIT_TEST(IndirectNamedNode) {
    Test(R"sql(
        $cluster = 'ex' || 'am' || "ple";
        $product = "yql";
        $seq = "1";
        $source = "/home/" || $product || "/" || $seq;
        SELECT # FROM $cluster.$source;
    )sql", R"(
        1:0:cluster definition
        2:0:product definition
        3:0:seq definition
        4:0:source definition
        4:22:product refers to 2:0:product
        4:41:seq refers to 3:0:seq
        5:14:cluster refers to 1:0:cluster
        5:23:source refers to 4:0:source
    )");
}

Y_UNIT_TEST(LambdaParameterShadowsOuterDefinition) {
    Test(R"sql(
        $x = 'outer';
        $f = ($x) -> ($x);
        $after = $x;
    )sql", R"(
        1:0:x definition
        2:0:f definition
        scope of 2:0:f
         2:6:x definition
         2:14:x refers to 2:6:x
        3:0:after definition
        3:9:x refers to 1:0:x
    )");
}

Y_UNIT_TEST(ForIteratorIsVisibleOnlyInBody) {
    Test(R"sql(
        $i = AsList(1);
        FOR $i IN $i DO BEGIN
            $body = $i;
        END DO ELSE DO BEGIN
            $fallback = $i;
        END DO;
        $after = $i;
    )sql", R"(
        1:0:i definition
        2:10:i refers to 1:0:i
        anonymous scope
         2:4:i definition
         3:4:body definition
         3:12:i refers to 2:4:i
        anonymous scope
         5:4:fallback definition
         5:16:i refers to 1:0:i
        7:0:after definition
        7:9:i refers to 1:0:i
    )");
}

Y_UNIT_TEST(TopLevelLambdaWithStatementBody) {
    Test(R"sql(
        $outer = 1;
        $f = ($x) -> {
            $local = $outer + $x;
            RETURN $local;
        };
    )sql", R"(
        1:0:outer definition
        2:0:f definition
        scope of 2:0:f
         2:6:x definition
         3:4:local definition
         3:13:outer refers to 1:0:outer
         3:22:x refers to 2:6:x
         4:11:local refers to 3:4:local
    )");
}

Y_UNIT_TEST(NestedLambdaWithStatementBody) {
    Test(R"sql(
        $outer = 1;
        $mapped = ListMap($items, ($x) -> {
            $local = $outer + $x;
            RETURN $local;
        });
    )sql", R"(
        1:0:outer definition
        2:0:mapped definition
        anonymous scope
         2:27:x definition
         3:4:local definition
         3:13:outer refers to 1:0:outer
         3:22:x refers to 2:27:x
         4:11:local refers to 3:4:local
    )");
}

Y_UNIT_TEST(IfBranchesHaveSeparateScopes) {
    Test(R"sql(
        $condition = true;
        EVALUATE IF $condition DO BEGIN
            $then = $condition;
        END DO ELSE DO BEGIN
            $otherwise = $condition;
        END DO;
        $after = $condition;
    )sql", R"(
        1:0:condition definition
        2:12:condition refers to 1:0:condition
        anonymous scope
         3:4:then definition
         3:12:condition refers to 1:0:condition
        anonymous scope
         5:4:otherwise definition
         5:17:condition refers to 1:0:condition
        7:0:after definition
        7:9:condition refers to 1:0:condition
    )");
}

Y_UNIT_TEST(DefineSubqueryScope) {
    Test(R"sql(
        $outer = 1;
        DEFINE SUBQUERY $get($arg) AS
            SELECT $arg, $outer;
        END DEFINE;
        $result = $get();
    )sql", R"(
        1:0:outer definition
        2:16:get definition
        scope of 2:16:get
         2:21:arg definition
         3:11:arg refers to 2:21:arg
         3:17:outer refers to 1:0:outer
        5:0:result definition
        5:10:get refers to 2:16:get
    )");
}

Y_UNIT_TEST(DefineActionScope) {
    Test(R"sql(
        $outer = 1;
        DEFINE ACTION $set($arg) AS
            $local = $outer + $arg;
        END DEFINE;
        DO $set($outer);
    )sql", R"(
        1:0:outer definition
        2:14:set definition
        scope of 2:14:set
         2:19:arg definition
         3:4:local definition
         3:13:outer refers to 1:0:outer
         3:22:arg refers to 2:19:arg
        5:3:set refers to 2:14:set
        5:8:outer refers to 1:0:outer
    )");
}

Y_UNIT_TEST(CombinedNestedScopes) {
    Test(R"sql(
        $source = AsList(1, 2);
        $offset = 10;
        $transform = ($item) -> {
            $value = $item + $offset;
            RETURN $value;
        };
        DEFINE ACTION $run($items) AS
            EVALUATE FOR $item IN $items DO BEGIN
                $mapped = ListMap($item, ($nested) -> ($transform($nested)));
            END DO;
        END DEFINE;
        DO $run($source);
    )sql", R"(
        1:0:source definition
        2:0:offset definition
        3:0:transform definition
        scope of 3:0:transform
         3:14:item definition
         4:4:value definition
         4:13:item refers to 3:14:item
         4:21:offset refers to 2:0:offset
         5:11:value refers to 4:4:value
        7:14:run definition
        scope of 7:14:run
         7:19:items definition
         8:26:items refers to 7:19:items
         anonymous scope
          8:17:item definition
          9:8:mapped definition
          9:26:item refers to 8:17:item
          anonymous scope
           9:34:nested definition
           9:47:transform refers to 3:0:transform
           9:58:nested refers to 9:34:nested
        12:3:run refers to 7:14:run
        12:8:source refers to 1:0:source
    )");
}

Y_UNIT_TEST(MultiBindingInitializerUsesPreviousDefinitions) {
    Test(R"sql(
        $x = 'outer';
        $x, $y = AsTuple($x, $x);
        $after = $x;
    )sql", R"(
        1:0:x definition
        2:0:x definition
        2:4:y definition
        2:17:x refers to 1:0:x
        2:21:x refers to 1:0:x
        3:0:after definition
        3:9:x refers to 2:0:x
    )");
}

Y_UNIT_TEST(SiblingScopeDefinitionsDoNotLeak) {
    Test(R"sql(
        $x = 'outer';
        EVALUATE IF true DO BEGIN
            $x = 'then';
            $then = $x;
        END DO ELSE DO BEGIN
            $otherwise = $x;
        END DO;
        $after = $x;
    )sql", R"(
        1:0:x definition
        anonymous scope
         3:4:x definition
         4:4:then definition
         4:12:x refers to 3:4:x
        anonymous scope
         6:4:otherwise definition
         6:17:x refers to 1:0:x
        8:0:after definition
        8:9:x refers to 1:0:x
    )");
}

Y_UNIT_TEST(NamedAndAnonymousSiblingScopesKeepSourceOrder) {
    Test(R"sql(
        $mapped = ListMap($items, ($x) -> ($x));
        $named = ($y) -> ($y);
        DO BEGIN
            $local = 1;
        END DO;
    )sql", R"(
        1:0:mapped definition
        anonymous scope
         1:27:x definition
         1:35:x refers to 1:27:x
        2:0:named definition
        scope of 2:0:named
         2:10:y definition
         2:18:y refers to 2:10:y
        anonymous scope
         4:4:local definition
    )");
}

Y_UNIT_TEST(WildcardActionHasAnonymousScope) {
    Test(R"sql(
        DEFINE ACTION $_($arg) AS
            $local = $arg;
        END DEFINE;
    )sql", R"(
        anonymous scope
         1:17:arg definition
         2:4:local definition
         2:13:arg refers to 1:17:arg
    )");
}

Y_UNIT_TEST(Recursion) {
    Test(R"sql(
        $x = $x;
        $x, $y = $x + $y;
        DEFINE ACTION $x($y) AS
            DO $x($y);
        END DEFINE;
    )sql", R"(
        1:0:x definition
        2:0:x definition
        2:4:y definition
        2:9:x refers to 1:0:x
        3:14:x definition
        scope of 3:14:x
         3:17:y definition
         4:7:x refers to 2:0:x
         4:10:y refers to 3:17:y
    )");
}

Y_UNIT_TEST(CreateView) {
    Test(R"sql(
        $x, $y = (1, 1);
        CREATE VIEW tmp AS DO BEGIN
            $y = 1;
            $x = $y;
            SELECT 1;
        END DO;
        $z = $y;
    )sql", R"(
        1:0:x definition
        1:4:y definition
        anonymous scope
         3:4:y definition
         4:4:x definition
         4:9:y refers to 3:4:y
        7:0:z definition
        7:5:y refers to 1:4:y
    )");
}

Y_UNIT_TEST(CreateViewBodyFreeVariable) {
    Test(R"sql(
        $outer = 1;
        CREATE VIEW tmp AS DO BEGIN
            SELECT $outer;
        END DO;
    )sql", R"(
        1:0:outer definition
        anonymous scope
    )");
}

Y_UNIT_TEST(CreateViewSelectFreeVariable) {
    Test(R"sql(
        $outer = 1;
        CREATE VIEW tmp AS
            SELECT $outer;
    )sql", R"(
        1:0:outer definition
        anonymous scope
         3:11:outer refers to 1:0:outer
    )");
}

Y_UNIT_TEST(LambdaArgument0) {
    Test(R"sql(
        $x = 1;
        $f = () -> (1);
        $f = () -> ($x);
    )sql", R"(
        1:0:x definition
        2:0:f definition
        scope of 2:0:f
        3:0:f definition
        scope of 3:0:f
         3:12:x refers to 1:0:x
    )");
}

Y_UNIT_TEST(LambdaArgument1) {
    Test(R"sql(
        $x = 1;
        $f = ($x) -> (($x)($x));
    )sql", R"(
        1:0:x definition
        2:0:f definition
        scope of 2:0:f
         2:6:x definition
         2:15:x refers to 2:6:x
         2:19:x refers to 2:6:x
    )");
}

Y_UNIT_TEST(LambdaArgument2) {
    Test(R"sql(
        $x = 1;
        $f = ($x) -> ($x)($x);
    )sql", R"(
        1:0:x definition
        2:0:f definition
        anonymous scope
         2:6:x definition
         2:14:x refers to 2:6:x
        2:18:x refers to 1:0:x
    )");
}

Y_UNIT_TEST(LambdaArgument3) {
    Test(R"sql(
        $x = 1;
        $f = (($x) -> ($x))($x);
    )sql", R"(
        1:0:x definition
        2:0:f definition
        anonymous scope
         2:7:x definition
         2:15:x refers to 2:7:x
        2:20:x refers to 1:0:x
    )");
}

Y_UNIT_TEST(LambdaArgument4) {
    Test(R"sql(
        $x = 1;
        $f = ($x) -> ($x)($x)($x);
    )sql", R"(
        1:0:x definition
        2:0:f definition
        anonymous scope
         2:6:x definition
         2:14:x refers to 2:6:x
        2:18:x refers to 1:0:x
        2:22:x refers to 1:0:x
    )");
}

Y_UNIT_TEST(LambdaArgumentOpt) {
    Test(R"sql(
        $h = ($x, $y?) -> ($x + ($y ?? 0));
    )sql", R"(
        1:0:h definition
        scope of 1:0:h
         1:6:x definition
         1:10:y definition
         1:19:x refers to 1:6:x
         1:25:y refers to 1:10:y
    )");
}

Y_UNIT_TEST(DefinitionAcceptsReferencesButNotDeclarations) {
    INamedNodes::TPtr nodes = Resolve(R"sql(
        $x = 1;
        $y = $x;
    )sql");

    INamedNodeDef::TPtr definition;
    TMaybe<TNamedNodeRef> reference;
    for (const INamedNodeScope::TEntry& entry : nodes->TopLevel()->Entries()) {
        if (const auto* def = std::get_if<INamedNodeDef::TPtr>(&entry);
            def != nullptr && (*def)->Decl().Name == "x") {
            definition = *def;
        } else if (const auto* ref = std::get_if<TNamedNodeRef>(&entry)) {
            reference = *ref;
        }
    }

    UNIT_ASSERT(definition);
    UNIT_ASSERT(reference);
    UNIT_ASSERT(nodes->Declaration(definition->Decl()) == definition);
    UNIT_ASSERT(!nodes->Declaration(*reference));
    UNIT_ASSERT(!nodes->Definition(definition->Decl()));
    UNIT_ASSERT(nodes->Definition(*reference) == definition);
    UNIT_ASSERT_VALUES_EQUAL(definition->References().size(), 1);
    UNIT_ASSERT_VALUES_EQUAL(definition->References().front(), *reference);
}

} // Y_UNIT_TEST_SUITE(NamedNodeTests)
