#include "pg_dump_parser.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NYdb;
using namespace NYdb::NConsoleClient;

Y_UNIT_TEST_SUITE(PgDumpParserTests) {
    TString ParseDump(const TString& data) {
        TStringStream in, out;
        TPgDumpParser parser(out, true);

        in << data;
        parser.Prepare(in);
        in.clear();
        in << data;
        parser.WritePgDump(in);
        return out.Str();
    }

    TString ParseDumpFixedString(const TString& data) {
        TStringStream out;
        TFixedStringStream in(data);
        TPgDumpParser parser(out, true);

        parser.Prepare(in);
        in.MovePointer();
        parser.WritePgDump(in);
        return out.Str();
    }

    Y_UNIT_TEST(RemovePublicScheme) {
        TString data =
            "INSERT INTO public.pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(1, 1, 0, '                                                                                    ');\n"
            "INSERT INTO public.pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(2, 1, 0, '                                                                                    ');\n"
            "INSERT INTO public.pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(3, 1, 0, '                                                                                    ');\n"
            "INSERT INTO public.pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(4, 1, 0, '                                                                                    ');\n";
        TString result =
            "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(1, 1, 0, '                                                                                    ');\n"
            "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(2, 1, 0, '                                                                                    ');\n"
            "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(3, 1, 0, '                                                                                    ');\n"
            "INSERT INTO pgbench_accounts (aid, bid, abalance, filler) VALUES\n"
            "(4, 1, 0, '                                                                                    ');\n";
        UNIT_ASSERT_EQUAL(ParseDump(data), result);
        UNIT_ASSERT_EQUAL(ParseDumpFixedString(data), result);
    }

    Y_UNIT_TEST(PgCatalogAndAlterComment) {
        TString data =
            "SET client_encoding = 'UTF8';\n"
            "SET standard_conforming_strings = on;\n"
            "SELECT pg_catalog.set_config('search_path', '', false);\n"
            "SET check_function_bodies = false;\n"
            "ALTER TABLE public.pgbench_accounts OWNER TO root;\n";
        TString result =
            "SET client_encoding = 'UTF8';\n"
            "SET standard_conforming_strings = on;\n"
            "-- SELECT pg_catalog.set_config('search_path', '', false);\n"
            "SET check_function_bodies = false;\n"
            "-- ALTER TABLE public.pgbench_accounts OWNER TO root;\n";
        UNIT_ASSERT_EQUAL(ParseDump(data), result);
        UNIT_ASSERT_EQUAL(ParseDumpFixedString(data), result);
    }

    Y_UNIT_TEST(CreateTablePrimaryKeys) {
        TString data =
            "CREATE TABLE public.pgbench_accounts (\n"
            "    aid integer NOT NULL,\n"
            "    bid integer,\n"
            "    abalance integer,\n"
            "    filler character(84)\n"
            ")\n"
            "WITH (fillfactor='100');\n"
            "CREATE TABLE public.pgbench_branches (\n"
            "    bid integer NOT NULL,\n"
            "    bbalance integer,\n"
            "    filler character(88)\n"
            ")\n"
            "WITH (fillfactor='100');\n"
            "CREATE TABLE public.pgbench_history (\n"
            "    tid integer,\n"
            "    bid integer,\n"
            "    aid integer,\n"
            "    delta integer,\n"
            "    mtime timestamp without time zone,\n"
            "    filler character(22)\n"
            ");\n"
            "--\n"
            "ALTER TABLE ONLY public.pgbench_accounts\n"
            "    ADD CONSTRAINT pgbench_accounts_pkey PRIMARY KEY (aid);\n"
            "ALTER TABLE ONLY public.pgbench_branches\n"
            "    ADD CONSTRAINT pgbench_branches_pkey PRIMARY KEY (bid);\n"
            "ALTER TABLE ONLY public.pgbench_accounts\n"
            "    ADD CONSTRAINT c_widget_field_6 FOREIGN KEY (value_sysmapid) REFERENCES public.sysmaps(sysmapid) ON DELETE CASCADE;\n";
        TString result = 
            "CREATE TABLE pgbench_accounts (\n"
            "    aid integer NOT NULL,\n"
            "    bid integer,\n"
            "    abalance integer,\n"
            "    filler character(84),\n"
            "    PRIMARY KEY(aid)\n"
            ");\n"
            "CREATE TABLE pgbench_branches (\n"
            "    bid integer NOT NULL,\n"
            "    bbalance integer,\n"
            "    filler character(88),\n"
            "    PRIMARY KEY(bid)\n"
            ");\n"
            "CREATE TABLE pgbench_history (\n"
            "    tid integer,\n"
            "    bid integer,\n"
            "    aid integer,\n"
            "    delta integer,\n"
            "    mtime timestamp without time zone,\n"
            "    filler character(22),\n"
            "    __ydb_stub_id BIGSERIAL PRIMARY KEY\n"
            ");\n"
            "--\n"
            "-- ALTER TABLE ONLY public.pgbench_accounts\n"
            "--     ADD CONSTRAINT pgbench_accounts_pkey PRIMARY KEY (aid);\n"
            "-- ALTER TABLE ONLY public.pgbench_branches\n"
            "--     ADD CONSTRAINT pgbench_branches_pkey PRIMARY KEY (bid);\n"
            "-- ALTER TABLE ONLY public.pgbench_accounts\n"
            "--     ADD CONSTRAINT c_widget_field_6 FOREIGN KEY (value_sysmapid) REFERENCES public.sysmaps(sysmapid) ON DELETE CASCADE;\n";
        UNIT_ASSERT_EQUAL(ParseDump(data), result);
        UNIT_ASSERT_EQUAL(ParseDumpFixedString(data), result);
    }
}
