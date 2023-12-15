#include <util/stream/str.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/system/types.h>

#include <ydb/library/yaml_config/validator/validator_builder.h>
#include <ydb/library/yaml_config/validator/validator.h>
#include <ydb/library/yaml_config/validator/validator_checks.h>

namespace NKikimr {

using namespace NYamlConfig::NValidator;
using TIssue = TValidationResult::TIssue;

bool HasOnlyThisIssues(TValidationResult result, TVector<TIssue> issues) {
    if (result.Issues.size() != issues.size()) {
        Cerr << "Issue counts are differend. List of actul issues:" << Endl;
        Cerr << result;
        Cerr << "------------- List of Expected Issues: " << Endl;
        Cerr << TValidationResult(issues);
        Cerr << "------------- End of issue List" << Endl;
        return false;
    }
    Sort(result.Issues);
    Sort(issues);
    for (size_t i = 0; i < issues.size(); ++i) {
        if (result.Issues[i] != issues[i]) {
            Cerr << "Issues are differend. List of actul issues:" << Endl;
            Cerr << result;
            Cerr << "------------- List of Expected Issues: " << Endl;
            Cerr << TValidationResult(issues);
            Cerr << "------------- End of issue List" << Endl;
            return false;
        }
    }
    return true;
}

bool Valid(TValidationResult result) {
    if (result.Ok()) return true;

    Cerr << "List of issues:" << Endl;
    Cerr << result;
    Cerr << "------------- End of issue list: " << Endl;
    return false;
}

Y_UNIT_TEST_SUITE(Checks) {
    Y_UNIT_TEST(BasicIntChecks) {
        TInt64Builder b;
        b.AddCheck("Value must be divisible by 2", [](auto& c) {
            c.Expect(c.Node() % 2 == 0);
        }).AddCheck("Value must be divisible by 3", [](auto& c) {
            c.Expect(c.Node().Value() % 3 == 0);
        });
        auto v = b.CreateValidator();

        Y_ENSURE(HasOnlyThisIssues(v.Validate("1"), {
            {"/", "Value must be divisible by 3"},
            {"/", "Value must be divisible by 2"}
            }));
        Y_ENSURE(HasOnlyThisIssues(v.Validate("2"), {{"/", "Value must be divisible by 3"}}));
        Y_ENSURE(HasOnlyThisIssues(v.Validate("3"), {{"/", "Value must be divisible by 2"}}));
        Y_ENSURE(Valid(v.Validate("6")));
    }

    Y_UNIT_TEST(BasicStringChecks) {
        NYamlConfig::NValidator::TStringBuilder b;
        b.AddCheck("Value length must be divisible by 2", [](auto& c) {
            c.Expect(c.Node().Value().length() % 2 == 0);
        }).AddCheck("Value length must be divisible by 3", [](auto& c) {
            c.Expect(c.Node().Value().length() % 3 == 0);
        });
        auto v = b.CreateValidator();

        Y_ENSURE(HasOnlyThisIssues(v.Validate("1"), {
            {"/", "Value length must be divisible by 3"},
            {"/", "Value length must be divisible by 2"}
            }));
        Y_ENSURE(HasOnlyThisIssues(v.Validate("22"), {{"/", "Value length must be divisible by 3"}}));
        Y_ENSURE(HasOnlyThisIssues(v.Validate("333"), {{"/", "Value length must be divisible by 2"}}));
        Y_ENSURE(Valid(v.Validate("666666")));
    }

    Y_UNIT_TEST(IntArrayValidation) {
        TArrayBuilder b;
        b.Item(TInt64Builder().Range(0, 10))
        .AddCheck("Array length must be greater than 5", [](auto& c) { 
            c.Expect(c.Node().Length() >= 5);
        }).AddCheck("Xor of all array elements must be 0", [](auto& c) {
            i64 totalXor = 0;
            for (int i = 0; i < c.Node().Length(); ++i) {
                totalXor ^= c.Node()[i].Int64().Value(); // or just c[i]?
            }
            c.Expect(totalXor == 0);
        });
        auto v = b.CreateValidator();

        Y_ENSURE(HasOnlyThisIssues(v.Validate("[]"), {{"/", "Array length must be greater than 5"}}));

        Y_ENSURE(HasOnlyThisIssues(v.Validate("[1]"), {
            {"/", "Array length must be greater than 5"},
            {"/", "Xor of all array elements must be 0"}
            }));
        Y_ENSURE(HasOnlyThisIssues(v.Validate("[-1]"), {
            {"/0", "Value must be greater or equal to min value(i.e >= 0)"}
            }));

        Y_ENSURE(Valid(v.Validate("[4,5,6,4,5,6]")));
        Y_ENSURE(Valid(v.Validate("[1,2,3,4,5,6,7]")));
    }

    Y_UNIT_TEST(MapValidation) {
        // field1 -> must have field2
        // field2 -> must have field1

        // field3 -> must have int_arr of 3 even non-negative elements with total sum <= 100
        // field4 -> must not have int_arr

        auto optionalInt = [](TInt64Builder& b) {
            b.Optional();
        };
        auto evenInt = [](TInt64CheckContext& c) {
            c.Expect(c.Node().Value() % 2 == 0);
        };
        auto arrLengthIs3 = [](TArrayCheckContext& c) {
            c.Expect(c.Node().Length() == 3);
        };
        auto sumIsLessOrEqualTo100 = [](TArrayCheckContext& c) {
            i64 sum = 0;
            for (int i = 0; i < c.Node().Length(); ++i) {
                sum += c.Node()[i].Int64().Value();
            }
            c.Expect(sum <= 100);
        };
        auto field1AndField2MustBeBothPresentOrAbsent = [](TMapCheckContext& c) {
            c.Expect(c.Node().Has("field1") == c.Node().Has("field2"));
        };
        auto arrayPresenceCheck = [](TMapCheckContext& c) {
            if (c.Node().Has("field3")) {
                c.Expect(c.Node().Has("int_arr"), "If you have field3, than you must have int_arr");
            }
            if (c.Node().Has("field4")) {
                c.Expect(!c.Node().Has("int_arr"), "If you have field4, than you must not have int_arr");
            }
        };

        auto v = 
        TMapBuilder()
            .Int64("field1", optionalInt)
            .Int64("field2", optionalInt)
            .Int64("field3", optionalInt)
            .Int64("field4", optionalInt)
            .Array("int_arr", [=](auto& b) {
                b.Optional()
                .Int64Item([=](auto& b) {
                    b.Min(0)
                    .AddCheck("Value must be even", evenInt);
                })
                .AddCheck("Array length must be 3", arrLengthIs3)
                .AddCheck("Sum of all array elements must be less or equal to 100", sumIsLessOrEqualTo100);
            })
            .AddCheck(
                "field1 and field2 either both must be present or both must be absent",
                field1AndField2MustBeBothPresentOrAbsent)
            .AddCheck("int_arr precense", arrayPresenceCheck)
            .CreateValidator();

        {
            TString yaml =
                "{}";

            Y_ENSURE(Valid(v.Validate(yaml)));
        }

        {
            TString yaml =
                "{\n"
                    "field1: 0,\n"
                    "field2: 0,\n"
                    "field3: 0,\n"
                    // "field4: 0,\n"
                    "int_arr: [2,4,6]\n"
                "}";
            Y_ENSURE(Valid(v.Validate(yaml)));
        }
        
        {
            TString yaml =
                "{\n"
                    "field1: 0,\n"
                    "field2: 0,\n"
                    "field3: 0,\n"
                    // "field4: 0,\n"
                    "int_arr: [-1,4,601,2]\n"
                "}";
            Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
                {"/int_arr/0", "Value must be greater or equal to min value(i.e >= 0)"},
                {"/int_arr/2", "Value must be even"}
            }));
        }

        {
            TString yaml =
                "{\n"
                    // "field1: 0,\n"
                    "field2: 0,\n"
                    "field3: 0,\n"
                    "field4: 0,\n"
                    "int_arr: [2,4,600,4]\n"
                "}";
            Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
                {"/int_arr", "Array length must be 3"},
                {"/int_arr", "Sum of all array elements must be less or equal to 100"}
            }));
        }
        
        {
            TString yaml =
                "{\n"
                    // "field1: 0,\n"
                    "field2: 0,\n"
                    "field3: 0,\n"
                    "field4: 0,\n"
                    "int_arr: [2,4,4]\n"
                "}";
            Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
                {"/", "Check \"int_arr precense\" failed: If you have field4, than you must not have int_arr"},
                {"/", "field1 and field2 either both must be present or both must be absent"},
            }));
        }
    }

    //TODO: make Some check tests for generic node(and
    //before that make ability to somehow work with this nodes)

    Y_UNIT_TEST(ErrorInCheck) {
        auto v = 
        TMapBuilder()
            .Bool("flag")
            .Map("a", [](auto& b) {
                b.Optional()
                .Map("b", [](auto& b) {
                    b.Int64("c");
                });
            })
            .Map("c", [](auto& b) {
                b.Optional()
                .Map("b", [](auto& b) {
                    b.Int64("a");
                });
            })
            .AddCheck("must have even a/b/c if flag is true. Else must have odd c/a/b", [](auto& c) {
                if (c.Node()["flag"].Bool()) {
                    c.Expect(c.Node()["a"].Map()["b"].Map()["c"].Int64().Value() % 2 == 0);
                } else {
                    c.Expect(c.Node()["c"].Map()["b"].Map()["a"].Int64().Value() % 2 == 1);
                }
            })
        .CreateValidator();

        const char* yaml = 
        "{"
        "   flag: true"
        "}";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/", "Check \"must have even a/b/c if flag is true. Else must have odd c/a/b\" failed: Node \"/a/b/c\" is not presented"},
        }));

        yaml = 
        "{"
        "   flag: true,"
        "   a: {"
        "       b: {"
        "           c: 0"
        "       }"
        "   }"
        "}";

        Y_ENSURE(Valid(v.Validate(yaml)));
        
        yaml = 
        "{"
        "   flag: true,"
        "   a: {"
        "       b: {"
        "           c: 1"
        "       }"
        "   }"
        "}";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/", "must have even a/b/c if flag is true. Else must have odd c/a/b"},
        }));
        
        yaml = 
        "{"
        "   flag: faLse,"
        "   a: {"
        "       b: {"
        "           c: 0"
        "       }"
        "   }"
        "}";

        Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
            {"/", "Check \"must have even a/b/c if flag is true. Else must have odd c/a/b\" failed: Node \"/c/b/a\" is not presented"},
        }));
    }

    Y_UNIT_TEST(OpaqueMaps) {
        auto v =
        TMapBuilder([](auto& b) {
            b.Opaque()
            .Int64("depth", [](auto& b) {
                b.Range(0, 3);
            });
        })
        .AddCheck("field/field/.../field level must equal /depth and must be a scalar", [](auto& c) {
            int remaining_depth = c.Node()["depth"].Int64();
            int cur_depth = 0;
            TNodeWrapper node = c.Node();
            while (remaining_depth > 0) {
                if (!node.IsMap()) {
                    c.Fail("field \"field\" on level " + ToString<i64>(cur_depth) + " must be a map");
                }
                node = node.Map()["field"];
                remaining_depth--;
                cur_depth++;
            }
            if (cur_depth == 0) {
                c.Expect(!node.Map()["field"].Exists(), "If depth == 0, field \"field\" must not be there");
            } else {
                c.Assert(node.Exists(), "field \"field\" on level " + ToString<i64>(cur_depth) + " must exist");
                c.Expect(node.IsScalar(), "field \"field\" on level " + ToString<i64>(cur_depth) + " must be a scalar");
            }
        })
        .CreateValidator();

        const char* yaml = 
        "depth: 0\n"
        "field: value\n";

        UNIT_ASSERT(HasOnlyThisIssues(v.Validate(yaml), {{
            "/", "Check \"field/field/.../field level must equal /depth and must be a scalar\" failed: If depth == 0, field \"field\" must not be there"
        }}));
        
        yaml = 
        "depth: 1\n"
        "field: value\n";

        UNIT_ASSERT(Valid(v.Validate(yaml)));

        yaml = 
        "depth: 1\n";

        UNIT_ASSERT(HasOnlyThisIssues(v.Validate(yaml), {{
            "/", "Check \"field/field/.../field level must equal /depth and must be a scalar\" failed: field \"field\" on level 1 must exist"
        }}));

        yaml = 
        "depth: 2\n"
        "field:\n"
        "  field: value\n";

        UNIT_ASSERT(Valid(v.Validate(yaml)));

        yaml = 
        "depth: 2\n"
        "field:\n"
        "  field: []\n";

        UNIT_ASSERT(HasOnlyThisIssues(v.Validate(yaml), {{
            "/", "Check \"field/field/.../field level must equal /depth and must be a scalar\" failed: field \"field\" on level 2 must be a scalar"
        }}));

        yaml = 
        "depth: 2\n"
        "field:\n"
        "  field:\n"
        "    field: value\n";

        UNIT_ASSERT(HasOnlyThisIssues(v.Validate(yaml), {{
            "/", "Check \"field/field/.../field level must equal /depth and must be a scalar\" failed: field \"field\" on level 2 must be a scalar"
        }}));
    }
}

} // namespace NKikimr
