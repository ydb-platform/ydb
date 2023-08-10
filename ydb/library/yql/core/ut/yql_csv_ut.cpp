#include "yql_csv.h"
#include <ydb/library/yql/core/ut_common/yql_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/mem.h>

using namespace NYql;
using namespace NUtils;

TVector<TString> ReadOneLineCsv(const TString& csv)
{
    TMemoryInput memIn(csv);
    TCsvInputStream in(memIn);
    return in.ReadLine();
}

TVector<TString> ReadOneLineCsvData(const TStringBuf& csvdata)
{
    TCsvInputBuffer in(csvdata);
    return in.ReadLine();
}


template<typename T>
TString WriteCsv(T&& functor) {
    TStringStream ss;
    TCsvOutputStream out(ss);
    functor(out);
    return ss.Str();
}

Y_UNIT_TEST_SUITE(TYqlUtils) {

    Y_UNIT_TEST(CsvReadTest) {
        {
            TVector<TString> columns = ReadOneLineCsv("");
            UNIT_ASSERT_EQUAL(columns.size(), 0);
        }

        {
            TVector<TString> columns = ReadOneLineCsv("\"\"");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("one");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("\"one\"");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("one;two;three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("\"one\";\"two\";\"three\"");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("\"one\";two;three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("one;\"two\";three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("one;two;\"three\"");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsv(";two;three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("one;two;");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsv(";");
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("\"\";");
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsv(";\"\"");
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsv("\"ab;cd\"");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "ab;cd");
        }

        {
            TVector<TString> columns = ReadOneLineCsv(
                    "\\\";"
                    "\"\\\"\";"
                    "\\\"\\\";"
                    "\"\\\"\\\"\";"
                    "\\\"\\\"\\\"\\\";"
                    "\"\\\"\\\"\\\"\\\"\";"
                    "\\\\\";"
                    "\"\\\\\"\";"
                    "\\\\\"\\\\\";"
                    "\"\\\\\"\\\\\"\";"
                    "\\\\\"\\\\\"\\\\\"\\\\\";"
                    "\"\\\\\"\\\\\"\\\\\"\\\\\"\"");
            UNIT_ASSERT_EQUAL(columns.size(), 12);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "\"\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[3], "\"\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[4], "\"\"\"\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[5], "\"\"\"\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[6], "\\\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[7], "\\\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[8], "\\\"\\\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[9], "\\\"\\\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[10], "\\\"\\\"\\\"\\\"");
            UNIT_ASSERT_STRINGS_EQUAL(columns[11], "\\\"\\\"\\\"\\\"");
        }
    }

    Y_UNIT_TEST(CsvReadFromBufferTest) {
        {
            TVector<TString> columns = ReadOneLineCsvData("");
            UNIT_ASSERT_EQUAL(columns.size(), 0);
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("\"\"");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("one");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("\"one\"");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("one;two;three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("\"one\";\"two\";\"three\"");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("\"one\";two;three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("one;\"two\";three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("one;two;\"three\"");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData(";two;three");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "three");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("one;two;");
            UNIT_ASSERT_EQUAL(columns.size(), 3);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "one");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "two");
            UNIT_ASSERT_STRINGS_EQUAL(columns[2], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData(";");
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("\"\";");
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData(";\"\"");
            UNIT_ASSERT_EQUAL(columns.size(), 2);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "");
            UNIT_ASSERT_STRINGS_EQUAL(columns[1], "");
        }

        {
            TVector<TString> columns = ReadOneLineCsvData("\"ab;cd\"");
            UNIT_ASSERT_EQUAL(columns.size(), 1);
            UNIT_ASSERT_STRINGS_EQUAL(columns[0], "ab;cd");
        }
    }

    Y_UNIT_TEST(CsvWriteTest) {
        {
            TString str = WriteCsv([](TCsvOutputStream& out) {
                    out << "";
            });
            UNIT_ASSERT_STRINGS_EQUAL(str, "\"\"");
        }

        {
            TString str = WriteCsv([](TCsvOutputStream& out) {
                    out << "one";
            });
            UNIT_ASSERT_STRINGS_EQUAL(str, "\"one\"");
        }

        {
            TString str = WriteCsv([](TCsvOutputStream& out) {
                    out << "one" << "two";
            });
            UNIT_ASSERT_STRINGS_EQUAL(str, "\"one\";\"two\"");
        }

        {
            TString str = WriteCsv([](TCsvOutputStream& out) {
                    out << "one" << "two" << Endl
                        << 1 << 2;
            });
            UNIT_ASSERT_STRINGS_EQUAL(str, "\"one\";\"two\"\n"
                                           "\"1\";\"2\"");
        }
    }
}

