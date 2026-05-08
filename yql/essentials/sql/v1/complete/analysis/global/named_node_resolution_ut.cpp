#include "named_node_resolution.h"
#include "parser.h"

#include <yql/essentials/sql/v1/complete/core/input.h>
#include <yql/essentials/utils/string/trim_indent.h>

#include <library/cpp/yson/node/node.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NSQLComplete;
using NYql::TrimIndent;

Y_UNIT_TEST_SUITE(NamedNodeTests) {

void Test(TString query, TString expected) {
    query = TrimIndent(query);
    expected = TrimIndent(expected + '\n');

    auto parser = MakeParser(/* isAnsiLexer = */ false);
    auto nodes = ResolveNamedNodes(parser->Parse(SharpedInput(query)), {});

    TStringStream stream;
    nodes->Dump(stream);
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
        1:6:a definition
        1:10:b definition
        1:22:b refers to 1:10:b
    )");
}

Y_UNIT_TEST(LambdaUncurryWell) {
    Test(R"sql(
        $f = ($a, $b) -> ($a + $b);
        #
    )sql", R"(
        1:0:f definition
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
        1:6:a definition
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
        1:6:a definition
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
        1:6:a definition
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
        1:24:x definition
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

} // Y_UNIT_TEST_SUITE(NamedNodeTests)
