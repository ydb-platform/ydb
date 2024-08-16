#include "ut/util.h"

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yql/parser/pg_wrapper/interface/parser.h>

#include <util/system/tempfile.h>

using namespace NSQLTranslation;

Y_UNIT_TEST_SUITE(PgSqlParsingOnly) {
    Y_UNIT_TEST(Locking) {
        auto res = PgSqlToYql("SELECT 1 FROM plato.Input FOR UPDATE");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().find("locking") != TString::npos);
    }

    Y_UNIT_TEST(InsertStmt) {
        auto res = PgSqlToYql("INSERT INTO plato.Input VALUES (1, 1)");
        UNIT_ASSERT(res.Root);
    }

    Y_UNIT_TEST(InsertStmt_DefaultValues) {
        auto res = PgSqlToYql("INSERT INTO plato.Input DEFAULT VALUES");
        UNIT_ASSERT(res.Root);

        const NYql::TAstNode* writeNode = nullptr;
        VisitAstNodes(*res.Root, [&writeNode] (const NYql::TAstNode& node) {
            const bool isWriteNode = node.IsList() && node.GetChildrenCount() > 0
                && node.GetChild(0)->IsAtom() && node.GetChild(0)->GetContent() == "Write!";
            if (isWriteNode) {
                writeNode = &node;
            }
        });

        UNIT_ASSERT(writeNode);
        UNIT_ASSERT(writeNode->GetChildrenCount() > 5);
        const auto optionsQListNode = writeNode->GetChild(5);
        UNIT_ASSERT(optionsQListNode->ToString().Contains("'default_values"));
    }

    Y_UNIT_TEST(InsertStmt_Returning) {
        auto res = PgSqlToYql("INSERT INTO plato.Input VALUES (1, 1) RETURNING *");
        UNIT_ASSERT(res.Root);
        const NYql::TAstNode* writeNode = nullptr;
        VisitAstNodes(*res.Root, [&writeNode] (const NYql::TAstNode& node) {
            const bool isWriteNode = node.IsList() && node.GetChildrenCount() > 0
                && node.GetChild(0)->IsAtom() && node.GetChild(0)->GetContent() == "Write!";
            if (isWriteNode) {
                writeNode = &node;
            }
        });
        UNIT_ASSERT(writeNode);
        UNIT_ASSERT(writeNode->GetChildrenCount() > 5);
        const auto optionsQListNode = writeNode->GetChild(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            optionsQListNode->ToString(),
            R"('('('mode 'append) '('returning '((PgResultItem '"" (Void) (lambda '() (PgStar)))))))"
        );
    }

    Y_UNIT_TEST(DeleteStmt) {
        auto res = PgSqlToYql("DELETE FROM plato.Input");
        UNIT_ASSERT(res.Root);
        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let read0 (Read! world (DataSource '"yt" '"plato") (Key '('table (String '"input"))) (Void) '()))
            (let world (Left! read0))
            (let world (Write! world (DataSink '"yt" '"plato") (Key '('table (String '"input"))) (Void) '('('pg_delete (PgSelect '('('set_items '((PgSetItem '('('result '((PgResultItem '"" (Void) (lambda '() (PgStar))))) '('from '('((Right! read0) '"input" '()))) '('join_ops '('('('push)))))))) '('set_ops '('push))))) '('mode 'delete))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(DeleteStmt_Returning) {
        auto res = PgSqlToYql("DELETE FROM plato.Input RETURNING name, price AS new_price");
        UNIT_ASSERT(res.Root);
        const NYql::TAstNode* writeNode = nullptr;
        VisitAstNodes(*res.Root, [&writeNode] (const NYql::TAstNode& node) {
            const bool isWriteNode = node.IsList() && node.GetChildrenCount() > 0
                && node.GetChild(0)->IsAtom() && node.GetChild(0)->GetContent() == "Write!";
            if (isWriteNode) {
                writeNode = &node;
            }
        });
        UNIT_ASSERT(writeNode);
        UNIT_ASSERT(writeNode->GetChildrenCount() > 5);
        const auto optionsQListNode = writeNode->GetChild(5);
        UNIT_ASSERT_STRINGS_EQUAL(
            optionsQListNode->GetChild(1)->GetChild(2)->ToString(),
            R"('('returning '((PgResultItem '"name" (Void) (lambda '() (PgColumnRef '"name"))) (PgResultItem '"new_price" (Void) (lambda '() (PgColumnRef '"price"))))))"
        );
    }

    Y_UNIT_TEST(CreateTableStmt_Basic) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '())) '('b (PgType 'text) '('columnConstraints '())))))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_SystemColumns) {
        auto res = PgSqlToYql("CREATE TABLE t(XMIN int)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().find("system column") != TString::npos);
    }

    Y_UNIT_TEST(CreateTableStmt_NotNull) {
        auto res = PgSqlToYql("CREATE TABLE t (a int NOT NULL, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('not_null)))) '('b (PgType 'text) '('columnConstraints '())))))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_JustPK) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('not_null)))) '('b (PgType 'text) '('columnConstraints '())))) '('primarykey '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_Default) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY, b int DEFAULT 0)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('not_null)))) '('b (PgType 'int4) '('columnConstraints '('('default (PgConst '0 (PgType 'int4)))))))) '('primarykey '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_PKAndNotNull) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY NOT NULL, b text)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('not_null)))) '('b (PgType 'text) '('columnConstraints '())))) '('primarykey '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_PKAndOtherNotNull) {
        auto res = PgSqlToYql("CREATE TABLE t (a int PRIMARY KEY, b text NOT NULL)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('not_null)))) '('b (PgType 'text) '('columnConstraints '('('not_null)))))) '('primarykey '('a)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_TableLevelPK) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, b text NOT NULL, PRIMARY KEY (a, b))");
        UNIT_ASSERT(res.Root);

        TString program = R"(
        (
            (let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('not_null)))) '('b (PgType 'text) '('columnConstraints '('('not_null)))))) '('primarykey '('a 'b)))))
            (let world (CommitAll! world))
            (return world)
        )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_RepeatingColumnNames) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, a text)");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().find("duplicate") != TString::npos);
    }

    Y_UNIT_TEST(CreateTableStmt_PKHasColumnsNotBelongingToTable_Fails) {
        auto res = PgSqlToYql("CREATE TABLE t (a int, primary key(b))");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().find("PK column does not belong to table") != TString::npos);
    }

    Y_UNIT_TEST(CreateTableStmt_AliasSerialToIntType) {
        auto res = PgSqlToYql("CREATE TABLE t (a SerIAL)");
        UNIT_ASSERT(res.Root);

        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '('('a (PgType 'int4) '('columnConstraints '('('serial)))))))))
                (let world (CommitAll! world))
                (return world))
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateTableStmt_Temp) {
        auto res = PgSqlToYql("create temp table t ()");
        UNIT_ASSERT(res.Root);

        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"t"))) (Void) '('('mode 'create) '('columns '()) '('temporary))))
                (let world (CommitAll! world))
                (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(CreateSeqStmt) {
        auto res = PgSqlToYql(
            "CREATE TEMP SEQUENCE IF NOT EXISTS seq AS integer START WITH 10 INCREMENT BY 2 NO MINVALUE NO MAXVALUE CACHE 3;");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());

        TString program = R"(
            ((let world (Configure! world (DataSource 'config) 'OrderedColumns))
            (let world (Write! world (DataSink '"kikimr" '"")
            (Key '('pgObject (String '"seq") (String 'pgSequence))) (Void) '(
                '('mode 'create_if_not_exists) '('temporary) '('"as" '"int4")
                '('"start" '10) '('"increment" '2) '('"cache" '3))))
            (let world (CommitAll! world)) (return world))
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(DropSequenceStmt) {
        auto res = PgSqlToYql("DROP SEQUENCE IF EXISTS seq;");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns)) (let world (Write! world (DataSink '"kikimr" '"") (Key '('pgObject (String '"seq") (String 'pgSequence))) (Void) '('('mode 'drop_if_exists))))
                (let world (CommitAll! world))
                (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(AlterSequenceStmt) {
        auto res = PgSqlToYql("ALTER SEQUENCE IF EXISTS seq AS integer START WITH 10 INCREMENT BY 2 NO MINVALUE NO MAXVALUE CACHE 3;");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let world (Write! world (DataSink '"kikimr" '"")
                 (Key '('pgObject (String '"seq") (String 'pgSequence)))
                 (Void) '('('mode 'alter_if_exists) '('"as" '"int4") '('"start" '10) '('"increment" '2) '('"cache" '3))))
                 (let world (CommitAll! world)) (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(AlterTableSetDefaultNextvalStmt) {
        auto res = PgSqlToYql("ALTER TABLE public.t ALTER COLUMN id SET DEFAULT nextval('seq');");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns)) 
                (let world (Write! world (DataSink '"kikimr" '"") 
                    (Key '('tablescheme (String '"t"))) (Void) '('('mode 'alter) '('actions '('('alterColumns '('('"id" '('setDefault '('nextval 'seq)))))))))) 
                (let world (CommitAll! world)) (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(AlterTableStmtWithCast) {
        auto res = PgSqlToYql("ALTER TABLE public.t ALTER COLUMN id SET DEFAULT nextval('seq'::regclass);");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns)) 
                (let world (Write! world (DataSink '"kikimr" '"") 
                    (Key '('tablescheme (String '"t"))) (Void) '('('mode 'alter) '('actions '('('alterColumns '('('"id" '('setDefault '('nextval 'seq)))))))))) 
                (let world (CommitAll! world)) (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(AlterTableDropDefaultStmt) {
        auto res = PgSqlToYql("ALTER TABLE public.t ALTER COLUMN id DROP DEFAULT;");
        UNIT_ASSERT_C(res.Root, res.Issues.ToString());
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns)) 
                (let world (Write! world (DataSink '"kikimr" '"") 
                    (Key '('tablescheme (String '"t"))) (Void) '('('mode 'alter) '('actions '('('alterColumns '('('"id" '('setDefault '('Null))))))))))
                (let world (CommitAll! world)) (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(VariableShowStmt) {
        auto res = PgSqlToYql("Show server_version_num");
        UNIT_ASSERT(res.Root);

        TString program = fmt::format(R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let output (PgSelect '('('set_items '((PgSetItem '('('result '((PgResultItem '"server_version_num" (Void) (lambda '() (PgConst '"{}" (PgType 'text)))))))))) '('set_ops '('push)))))
                (let result_sink (DataSink 'result))
                (let world (Write! world result_sink (Key) output '('('type) '('autoref))))
                (let world (Commit! world result_sink))
                (let world (CommitAll! world))
                (return world)
            )
        )", NYql::GetPostgresServerVersionNum());
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    TMap<TString, TString> GetParamNameToPgType(const NYql::TAstNode& root) {
        TMap<TString, TString> actualParamToType;

        VisitAstNodes(root, [&actualParamToType] (const NYql::TAstNode& node) {
            bool isDeclareNode =
                node.IsListOfSize(3) && node.GetChild(0)->IsAtom()
                && node.GetChild(0)->GetContent() == "declare";
            if (isDeclareNode) {
                const auto varNameNode = node.GetChild(1);
                UNIT_ASSERT(varNameNode->IsAtom());
                const auto varName = varNameNode->GetContent();

                const auto varTypeNode = node.GetChild(2);
                UNIT_ASSERT(varTypeNode->IsListOfSize(2));
                UNIT_ASSERT(varTypeNode->GetChild(0)->GetContent() == "PgType");
                actualParamToType[TString(varName)] = varTypeNode->GetChild(1)->ToString();
            }
        });

        return actualParamToType;
    }

    Y_UNIT_TEST(ParamRef_IntAndPoint) {
        TTranslationSettings settings;

        settings.PgParameterTypeOids = {NYql::NPg::LookupType("int4").TypeId, NYql::NPg::LookupType("point").TypeId};
        auto res = SqlToYqlWithMode(
            R"(select $1 as "x", $2 as "y")",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        TMap<TString, TString> expectedParamToType {
            {"$p1", "'int4"},
            {"$p2", "'point"},
        };
        UNIT_ASSERT(res.Root);
        const auto actualParamToTypes = GetParamNameToPgType(*res.Root);
        UNIT_ASSERT(expectedParamToType.size() == actualParamToTypes.size());
        UNIT_ASSERT_EQUAL(expectedParamToType, actualParamToTypes);
    }

    Y_UNIT_TEST(ParamRef_IntUnknownInt) {
        TTranslationSettings settings;
        settings.PgParameterTypeOids = {NYql::NPg::LookupType("int4").TypeId, NYql::NPg::LookupType("unknown").TypeId, NYql::NPg::LookupType("int4").TypeId};
        auto res = SqlToYqlWithMode(
            R"(select $1 as "x", $2 as "y", $3 as "z")",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        TMap<TString, TString> expectedParamToType {
            {"$p1", "'int4"},
            {"$p2", "'unknown"},
            {"$p3", "'int4"},
        };
        UNIT_ASSERT(res.Root);
        const auto actualParamToTypes = GetParamNameToPgType(*res.Root);
        UNIT_ASSERT(expectedParamToType.size() == actualParamToTypes.size());
        UNIT_ASSERT_EQUAL(expectedParamToType, actualParamToTypes);
    }

    Y_UNIT_TEST(ParamRef_NoTypeOids) {
        TTranslationSettings settings;
        settings.PgParameterTypeOids = {};
        auto res = PgSqlToYql(R"(select $1 as "x", $2 as "y", $3 as "z")");
        TMap<TString, TString> expectedParamToType {
            {"$p1", "'unknown"},
            {"$p2", "'unknown"},
            {"$p3", "'unknown"},
        };
        UNIT_ASSERT(res.Root);
        auto actualParamToTypes = GetParamNameToPgType(*res.Root);
        UNIT_ASSERT_VALUES_EQUAL(expectedParamToType, actualParamToTypes);
    }

    Y_UNIT_TEST(DropTableStmt) {
        auto res = PgSqlToYql("drop table plato.Input");
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let world (Write! world (DataSink '"yt" '"plato") (Key '('tablescheme (String '"input"))) (Void) '('('mode 'drop))))
                (let world (CommitAll! world))
                (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(DropTableStmtMultiple) {
        auto res = PgSqlToYql("DROP TABLE FakeTable1, FakeTable2");
        TString program = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"FakeTable1"))) (Void) '('('mode 'drop))))
                (let world (Write! world (DataSink '"kikimr" '"") (Key '('tablescheme (String '"FakeTable2"))) (Void) '('('mode 'drop))))
                (let world (CommitAll! world))
                (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(program);
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(DropTableUnknownClusterStmt) {
        auto res = PgSqlToYql("drop table if exists pub.t");
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT_C(issue.GetMessage().find("Unknown cluster: pub") != TString::npos, res.Issues.ToString());
    }

    Y_UNIT_TEST(PublicSchemeRemove) {
        auto res = PgSqlToYql("DROP TABLE IF EXISTS public.t; CREATE TABLE public.t(id INT PRIMARY KEY, foo INT);\
INSERT INTO public.t VALUES(1, 2);\
UPDATE public.t SET foo = 3 WHERE id == 1;\
DELETE FROM public.t WHERE id == 1;\
SELECT COUNT(*) FROM public.t;");
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Root->ToString().find("public") == TString::npos);
    }

    Y_UNIT_TEST(UpdateStmt) {
        auto res = PgSqlToYql("UPDATE plato.Input SET kind = 'test' where kind = 'testtest'");
        TString updateStmtProg = R"(
            (
                (let world (Configure! world (DataSource 'config) 'OrderedColumns))
                (let read0 (Read! world (DataSource '"yt" '"plato") (Key '('table (String '"input"))) (Void) '()))
                (let world (Left! read0))
                (let world (block '((let update_select
                (PgSelect '(
                    '('set_items '((PgSetItem
                        '('('emit_pg_star)
                        '('result '((PgResultItem '"" (Void) (lambda '() (PgStar)))
                            (PgResultItem '"kind" (Void) (lambda '() (PgConst '"test" (PgType 'unknown))))))
                        '('from '('((Right! read0) '"input" '())))
                        '('join_ops '('('('push))))
                        '('where (PgWhere (Void) (lambda '() (PgOp '"=" (PgColumnRef '"kind") (PgConst '"testtest" (PgType 'unknown)))))) '('unknowns_allowed)))))
                        '('set_ops '('push)))
                    )
                )
                (let sink (DataSink '"yt" '"plato"))
                (let key (Key '('table (String '"input"))))
                (return (Write! world sink key (Void) '('('pg_update update_select) '('mode 'update)))))))
                (let world (CommitAll! world))
                (return world)
            )
        )";
        const auto expectedAst = NYql::ParseAst(updateStmtProg);

        UNIT_ASSERT_C(res.Issues.Empty(), "Failed to parse statement, issues: " + res.Issues.ToString());
        UNIT_ASSERT_C(res.Root, "Failed to parse statement, root is nullptr");
        UNIT_ASSERT_STRINGS_EQUAL(res.Root->ToString(), expectedAst.Root->ToString());
    }

    Y_UNIT_TEST(BlockEngine) {
        auto res = PgSqlToYql("set blockEngine='auto'; select 1;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_STRING_CONTAINS(res.Root->ToString(), "(let world (Configure! world (DataSource 'config) 'BlockEngine 'auto))");

        res = PgSqlToYql("set Blockengine='force'; select 1;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT_STRING_CONTAINS(res.Root->ToString(), "(let world (Configure! world (DataSource 'config) 'BlockEngine 'force))");

        res = PgSqlToYql("set BlockEngine='disable'; select 1;");
        UNIT_ASSERT(res.Root);
        UNIT_ASSERT(!res.Root->ToString().Contains("BlockEngine"));

        res = PgSqlToYql("set BlockEngine='foo'; select 1;");
        UNIT_ASSERT(!res.Root);
        UNIT_ASSERT_EQUAL(res.Issues.Size(), 1);

        auto issue = *(res.Issues.begin());
        UNIT_ASSERT(issue.GetMessage().Contains("VariableSetStmt, not supported BlockEngine option value: foo"));
    }

    Y_UNIT_TEST(SetConfig_SearchPath) {
        TTranslationSettings settings;
        settings.GUCSettings = std::make_shared<TGUCSettings>();
        settings.ClusterMapping["pg_catalog"] = NYql::PgProviderName;
        settings.DefaultCluster = "";

        auto res = SqlToYqlWithMode(
            R"(select set_config('search_path', 'pg_catalog', false);)",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::ToCerr,
            false,
            settings);
        UNIT_ASSERT_C(res.IsOk(), res.Issues.ToString());
        UNIT_ASSERT(res.Root);

        res = SqlToYqlWithMode(
            R"(select oid,
typinput::int4 as typinput,
typname,
typnamespace,
typtype
from pg_type)",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Root);

        res = SqlToYqlWithMode(
            R"(select oid,
typinput::int4 as typinput,
typname,
typnamespace,
typtype
from pg_catalog.pg_type)",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Root);

        res = SqlToYqlWithMode(
            R"(select set_config('search_path', 'public', false);)",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Root);

        res = SqlToYqlWithMode(
            R"(select * from pg_type;)",
            NSQLTranslation::ESqlMode::QUERY,
            10,
            {},
            EDebugOutput::None,
            false,
            settings);
        UNIT_ASSERT(res.IsOk());
        UNIT_ASSERT(res.Root);
    }
}

Y_UNIT_TEST_SUITE(PgExtensions) {
    using namespace NYql;

    Y_UNIT_TEST(Empty) {
        NPg::ClearExtensions();
        UNIT_ASSERT_VALUES_EQUAL(NPg::ExportExtensions(), "");
        NPg::ImportExtensions("", true, nullptr);
    }

    Y_UNIT_TEST(ProcsAndType) {
        NPg::ClearExtensions();
        if (NPg::AreAllFunctionsAllowed()) {
            return;
        }

        NPg::TExtensionDesc desc;
        TTempFileHandle h;
        TStringBuf sql = R"(
            CREATE OR REPLACE FUNCTION mytype_in(cstring)
                RETURNS mytype
                AS '$libdir/MyExt','mytype_in_func'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE OR REPLACE FUNCTION mytype_out(mytype)
                RETURNS cstring
                AS '$libdir/MyExt','mytype_out_func'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE TYPE mytype (
                alignment = double,
                internallength = 65,
                input = mytype_in,
                output = mytype_out
            );
        )";

        h.Write(sql.Data(), sql.Size());
        desc.Name = "MyExt";
        desc.InstallName = "$libdir/MyExt";
        desc.SqlPaths.push_back(h.Name());
        NPg::RegisterExtensions({desc}, true, *NSQLTranslationPG::CreateExtensionSqlParser(), nullptr);
        auto validate = [&]() {
            const auto& type = NPg::LookupType("mytype");
            UNIT_ASSERT_VALUES_EQUAL(type.Category, 'U');
            UNIT_ASSERT_VALUES_EQUAL(type.TypeLen, 65);
            UNIT_ASSERT_VALUES_EQUAL(type.TypeAlign, 'd');
            const auto& arrType = NPg::LookupType("_mytype");
            UNIT_ASSERT_VALUES_EQUAL(arrType.ElementTypeId, type.TypeId);
            const auto& inProc = NPg::LookupProc("mytype_in", { NPg::LookupType("cstring").TypeId });
            UNIT_ASSERT_VALUES_EQUAL(inProc.ArgTypes.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(inProc.Src, "mytype_in_func");
            UNIT_ASSERT(inProc.IsStrict);
            const auto& outProc = NPg::LookupProc("mytype_out", { NPg::LookupType("mytype").TypeId });
            UNIT_ASSERT_VALUES_EQUAL(outProc.ArgTypes.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(outProc.Src, "mytype_out_func");
            UNIT_ASSERT(outProc.IsStrict);
            UNIT_ASSERT_VALUES_EQUAL(type.InFuncId, inProc.ProcId);
            UNIT_ASSERT_VALUES_EQUAL(type.OutFuncId, outProc.ProcId);
        };

        validate();
        auto exported = NPg::ExportExtensions();
        NPg::ClearExtensions();
        NPg::ImportExtensions(exported, true, nullptr);
        validate();
    }

    Y_UNIT_TEST(InsertValues) {
        NPg::ClearExtensions();
        NPg::TExtensionDesc desc;
        TTempFileHandle h;
        TStringBuf sql = R"(
            CREATE TABLE mytable(
                foo int4,
                bar text,
                baz double
            );

            INSERT INTO mytable(bar, foo, baz)
            VALUES ('a', 1, null),('b', null, -3.4);
        )";

        h.Write(sql.Data(), sql.Size());
        desc.Name = "MyExt";
        desc.InstallName = "$libdir/MyExt";
        desc.SqlPaths.push_back(h.Name());
        NPg::RegisterExtensions({desc}, true, *NSQLTranslationPG::CreateExtensionSqlParser(), nullptr);
        auto validate = [&]() {
            const auto& table = NPg::LookupStaticTable({"pg_catalog","mytable"});
            UNIT_ASSERT(table.Kind == NPg::ERelKind::Relation);
            size_t remap[2];
            size_t rowStep;
            const auto& data = *NPg::ReadTable({"pg_catalog", "mytable"}, {"foo", "bar"}, remap, rowStep);
            UNIT_ASSERT_VALUES_EQUAL(rowStep, 3);
            UNIT_ASSERT_VALUES_EQUAL(data.size(), 2 * rowStep);
            UNIT_ASSERT_VALUES_EQUAL(data[rowStep * 0 + remap[0]], "1");
            UNIT_ASSERT_VALUES_EQUAL(data[rowStep * 0 + remap[1]], "a");
            UNIT_ASSERT(!data[rowStep * 1 + remap[0]].Defined());
            UNIT_ASSERT_VALUES_EQUAL(data[rowStep * 1 + remap[1]], "b");
        };

        validate();
        auto exported = NPg::ExportExtensions();
        NPg::ClearExtensions();
        NPg::ImportExtensions(exported, true, nullptr);
        validate();
    }

    Y_UNIT_TEST(Casts) {
        NPg::ClearExtensions();
        if (NPg::AreAllFunctionsAllowed()) {
            return;
        }

        NPg::TExtensionDesc desc;
        TTempFileHandle h;
        TStringBuf sql = R"(
            CREATE TYPE foo (
                alignment = double,
                internallength = variable
            );

            CREATE TYPE bar (
                alignment = double,
                internallength = variable
            );

            CREATE OR REPLACE FUNCTION bar(foo)
                RETURNS bar
                AS '$libdir/MyExt','foo_to_bar'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE CAST (foo AS bar) WITH FUNCTION bar(foo);
        )";

        h.Write(sql.Data(), sql.Size());
        desc.Name = "MyExt";
        desc.InstallName = "$libdir/MyExt";
        desc.SqlPaths.push_back(h.Name());
        NPg::RegisterExtensions({desc}, true, *NSQLTranslationPG::CreateExtensionSqlParser(), nullptr);
        auto validate = [&]() {
            auto sourceId = NPg::LookupType("foo").TypeId;
            auto targetId = NPg::LookupType("bar").TypeId;
            UNIT_ASSERT(NPg::HasCast(sourceId, targetId));
            const auto& cast = NPg::LookupCast(sourceId, targetId);
            UNIT_ASSERT_VALUES_EQUAL(cast.SourceId, sourceId);
            UNIT_ASSERT_VALUES_EQUAL(cast.TargetId, targetId);
            UNIT_ASSERT_VALUES_EQUAL((ui32)cast.Method, (ui32)NPg::ECastMethod::Function);
            UNIT_ASSERT_VALUES_EQUAL(cast.CoercionCode, NPg::ECoercionCode::Explicit);
            UNIT_ASSERT_VALUES_EQUAL(cast.FunctionId, NPg::LookupProc("bar",{sourceId}).ProcId);
        };

        validate();
        auto exported = NPg::ExportExtensions();
        NPg::ClearExtensions();
        NPg::ImportExtensions(exported, true, nullptr);
        validate();
    }

    Y_UNIT_TEST(Operators) {
        NPg::ClearExtensions();
        if (NPg::AreAllFunctionsAllowed()) {
            return;
        }

        NPg::TExtensionDesc desc;
        TTempFileHandle h;
        TStringBuf sql = R"(
            CREATE TYPE foo (
                alignment = double,
                internallength = variable
            );

            CREATE OR REPLACE FUNCTION foo_lt(foo, foo)
                RETURNS bool
                AS '$libdir/MyExt','foo_lt'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE OR REPLACE FUNCTION foo_le(foo, foo)
                RETURNS bool
                AS '$libdir/MyExt','foo_le'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE OR REPLACE FUNCTION foo_gt(foo, foo)
                RETURNS bool
                AS '$libdir/MyExt','foo_gt'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE OR REPLACE FUNCTION foo_ge(foo, foo)
                RETURNS bool
                AS '$libdir/MyExt','foo_ge'
                LANGUAGE 'c' IMMUTABLE STRICT PARALLEL SAFE;

            CREATE OPERATOR < (
                LEFTARG = foo, RIGHTARG = foo, PROCEDURE = foo_lt,
                COMMUTATOR = '>', NEGATOR = '>='
            );

            CREATE OPERATOR <= (
                LEFTARG = foo, RIGHTARG = foo, PROCEDURE = foo_le,
                COMMUTATOR = '>=', NEGATOR = '>'
            );

            CREATE OPERATOR > (
                LEFTARG = foo, RIGHTARG = foo, PROCEDURE = foo_gt,
                COMMUTATOR = '<', NEGATOR = '<='
            );

            CREATE OPERATOR >= (
                LEFTARG = foo, RIGHTARG = foo, PROCEDURE = foo_ge,
                COMMUTATOR = '<=', NEGATOR = '<'
            );
        )";

        h.Write(sql.Data(), sql.Size());
        desc.Name = "MyExt";
        desc.InstallName = "$libdir/MyExt";
        desc.SqlPaths.push_back(h.Name());
        NPg::RegisterExtensions({desc}, true, *NSQLTranslationPG::CreateExtensionSqlParser(), nullptr);
        auto validate = [&]() {
            auto typeId = NPg::LookupType("foo").TypeId;
            TVector<ui32> args { typeId, typeId };
            auto lessProcId = NPg::LookupProc("foo_lt", args).ProcId;
            auto lessOrEqualProcId = NPg::LookupProc("foo_le", args).ProcId;
            auto greaterProcId = NPg::LookupProc("foo_gt", args).ProcId;
            auto greaterOrEqualProcId = NPg::LookupProc("foo_ge", args).ProcId;

            const auto& lessOp = NPg::LookupOper("<", args);
            const auto& lessOrEqualOp = NPg::LookupOper("<=", args);
            const auto& greaterOp = NPg::LookupOper(">", args);
            const auto& greaterOrEqualOp = NPg::LookupOper(">=", args);

            UNIT_ASSERT_VALUES_EQUAL(lessOp.Name, "<");
            UNIT_ASSERT_VALUES_EQUAL(lessOp.LeftType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(lessOp.RightType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(lessOp.ProcId, lessProcId);
            UNIT_ASSERT_VALUES_EQUAL(lessOp.ComId, greaterOp.OperId);
            UNIT_ASSERT_VALUES_EQUAL(lessOp.NegateId, greaterOrEqualOp.OperId);

            UNIT_ASSERT_VALUES_EQUAL(lessOrEqualOp.Name, "<=");
            UNIT_ASSERT_VALUES_EQUAL(lessOrEqualOp.LeftType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(lessOrEqualOp.RightType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(lessOrEqualOp.ProcId, lessOrEqualProcId);
            UNIT_ASSERT_VALUES_EQUAL(lessOrEqualOp.ComId, greaterOrEqualOp.OperId);
            UNIT_ASSERT_VALUES_EQUAL(lessOrEqualOp.NegateId, greaterOp.OperId);

            UNIT_ASSERT_VALUES_EQUAL(greaterOp.Name, ">");
            UNIT_ASSERT_VALUES_EQUAL(greaterOp.LeftType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOp.RightType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOp.ProcId, greaterProcId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOp.ComId, lessOp.OperId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOp.NegateId, lessOrEqualOp.OperId);

            UNIT_ASSERT_VALUES_EQUAL(greaterOrEqualOp.Name, ">=");
            UNIT_ASSERT_VALUES_EQUAL(greaterOrEqualOp.LeftType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOrEqualOp.RightType, typeId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOrEqualOp.ProcId, greaterOrEqualProcId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOrEqualOp.ComId, lessOrEqualOp.OperId);
            UNIT_ASSERT_VALUES_EQUAL(greaterOrEqualOp.NegateId, lessOp.OperId);
        };

        validate();
        auto exported = NPg::ExportExtensions();
        NPg::ClearExtensions();
        NPg::ImportExtensions(exported, true, nullptr);
        validate();
    }
}
