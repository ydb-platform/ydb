#include "ut/util.h"

#include <library/cpp/testing/unittest/registar.h>

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
