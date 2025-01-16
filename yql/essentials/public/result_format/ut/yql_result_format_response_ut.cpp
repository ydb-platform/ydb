#include <yql/essentials/public/result_format/yql_result_format_response.h>

#include <library/cpp/yson/node/node_io.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYql::NResult {

Y_UNIT_TEST_SUITE(ParseResponse) {
    Y_UNIT_TEST(Empty) {
        auto response = NYT::NodeFromYsonString("[]");
        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 0);
    }

    Y_UNIT_TEST(One) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
    }

    Y_UNIT_TEST(Two) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                ]
            };
            {
                Write = [
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 2);
    }

    Y_UNIT_TEST(Label) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Label = foo;
                Write = [
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT(res.Label.Defined());
        UNIT_ASSERT_VALUES_EQUAL(*res.Label, "foo");
    }

    Y_UNIT_TEST(Position) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Position = {
                    File = "<main>";
                    Row = 1;
                    Column = 7;
                };
                Write = [
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT(res.Position.Defined());
        UNIT_ASSERT_VALUES_EQUAL(res.Position->File, "<main>");
        UNIT_ASSERT_VALUES_EQUAL(res.Position->Row, 1);
        UNIT_ASSERT_VALUES_EQUAL(res.Position->Column, 7);
    }

    Y_UNIT_TEST(Truncated) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Truncated = %true;
                Write = [
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT(res.IsTruncated);
    }

    Y_UNIT_TEST(Unordered) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Unordered = %true;
                Write = [
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT(res.IsUnordered);
    }

    Y_UNIT_TEST(WriteOne) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write.Data), "\"data\"");
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write.Type), "\"type\"");
    }

    Y_UNIT_TEST(WriteTwo) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data1;
                        Type = type1;
                    };
                    {
                        Data = data2;
                        Type = type2;
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 2);
        const auto& write1 = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write1.Data), "\"data1\"");
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write1.Type), "\"type1\"");
        const auto& write2 = res.Writes[1];
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write2.Data), "\"data2\"");
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write2.Type), "\"type2\"");
    }

    Y_UNIT_TEST(WriteTruncated) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                        Truncated = %true;
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT(write.IsTruncated);
        UNIT_ASSERT(!res.IsTruncated);
    }

    Y_UNIT_TEST(WriteNoType) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Truncated = %true;
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write.Data), "\"data\"");
        UNIT_ASSERT(!write.Type);
    }

    Y_UNIT_TEST(WriteNoData) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Type = type;
                        Ref = [];
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(NYT::NodeToCanonicalYsonString(*write.Type), "\"type\"");
        UNIT_ASSERT(!write.Data);
    }

    Y_UNIT_TEST(RefsEmpty) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                        Ref = [];
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(write.Refs.size(), 0);
    }

    Y_UNIT_TEST(RefsOne) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                        Ref = [
                            {
                                "Reference" = [];
                                "Remove" = %true;
                            }
                        ];
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(write.Refs.size(), 1);
        const auto& ref = write.Refs[0];
        UNIT_ASSERT_VALUES_EQUAL(ref.Reference.size(), 0);
        UNIT_ASSERT(ref.Remove);
    }

    Y_UNIT_TEST(RefsTwo) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                        Ref = [
                            {
                                "Reference" = [];
                                "Remove" = %true;
                            };
                            {
                                "Reference" = [];
                                "Remove" = %true;
                            }
                        ];
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(write.Refs.size(), 2);
        const auto& ref1 = write.Refs[0];
        UNIT_ASSERT_VALUES_EQUAL(ref1.Reference.size(), 0);
        UNIT_ASSERT(ref1.Remove);
        const auto& ref2 = write.Refs[1];
        UNIT_ASSERT_VALUES_EQUAL(ref2.Reference.size(), 0);
        UNIT_ASSERT(ref2.Remove);
    }

    Y_UNIT_TEST(RefsComponents) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                        Ref = [
                            {
                                "Reference" = ["foo";"bar"];
                                "Remove" = %false;
                            }
                        ];
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(write.Refs.size(), 1);
        const auto& ref = write.Refs[0];
        UNIT_ASSERT_VALUES_EQUAL(ref.Reference.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(ref.Reference[0], "foo");
        UNIT_ASSERT_VALUES_EQUAL(ref.Reference[1], "bar");
        UNIT_ASSERT(!ref.Remove);
    }

    Y_UNIT_TEST(RefsColumns) {
        auto response = NYT::NodeFromYsonString(R"([
            {
                Write = [
                    {
                        Data = data;
                        Type = type;
                        Ref = [
                            {
                                "Columns" = ["col1";"col2"];
                                "Reference" = [];
                                "Remove" = %false;
                            }
                        ];
                    }
                ]
            };
        ])");

        auto resList = ParseResponse(response);
        UNIT_ASSERT_VALUES_EQUAL(resList.size(), 1);
        const auto& res = resList[0];
        UNIT_ASSERT_VALUES_EQUAL(res.Writes.size(), 1);
        const auto& write = res.Writes[0];
        UNIT_ASSERT_VALUES_EQUAL(write.Refs.size(), 1);
        const auto& ref = write.Refs[0];
        UNIT_ASSERT(ref.Columns.Defined());
        UNIT_ASSERT_VALUES_EQUAL(ref.Columns->size(), 2);
        UNIT_ASSERT_VALUES_EQUAL((*ref.Columns)[0], "col1");
        UNIT_ASSERT_VALUES_EQUAL((*ref.Columns)[1], "col2");
    }
}

}


