#include <util/stream/str.h>

#include <library/cpp/testing/unittest/registar.h>

#include <ydb/library/yaml_config/validator/validator_builder.h>
#include <ydb/library/yaml_config/validator/validator.h>

namespace NKikimr {

using namespace NYamlConfig::NValidator;
using TIssue = TValidationResult::TIssue;

bool HasIssue(const TValidationResult& result, const TIssue& issue) {
    for (const TIssue& i : result.Issues) {
        if (i == issue) {
            return true;
        }
    }
    Cerr << "Doesn't have specified issue. The followin issues are presented:" << Endl;
    Cerr << result;
    Cerr << "------------- End of issue List" << Endl;
    return false;
}

bool HasOneIssue(const TValidationResult& result, const TIssue& issue) {
    if (result.Issues.size() != 1) {
        Cerr << "Has " << result.Issues.size() << " errors" << Endl;
        return false;
    }
    return HasIssue(result, issue);
}

bool HasOnlyThisIssues(TValidationResult result, TVector<TIssue> issues) {
    if (result.Issues.size() != issues.size()) {
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

bool ContainsProblem(const TValidationResult& result, const TString& problem) {
    for (const TIssue& i : result.Issues) {
        if (i.Problem.Contains(problem)) {
            return true;
        }
    }
    Cerr << "Problem '" << problem << "'wasn't found" << Endl;
    Cerr << "List of issues:" << Endl;
    Cerr << result;
    return false;
}

Y_UNIT_TEST_SUITE(Validator) {
    Y_UNIT_TEST(IntValidation) {
        TInt64Builder b;
        b.Range(0, 10);
        auto v = b.CreateValidator();

        Y_ENSURE(HasOneIssue(v.Validate("{}"), {"/", "Node must be Scalar(Int64)"}));
        Y_ENSURE(HasOneIssue(v.Validate("1a"), {"/", "Can't convert value to Int64"}));
        Y_ENSURE(HasOneIssue(v.Validate("11"), {"/", "Value must be less or equal to max value(i.e <= 10)"}));
        Y_ENSURE(HasOneIssue(v.Validate("-1"), {"/", "Value must be greater or equal to min value(i.e >= 0)"}));
        Y_ENSURE(v.Validate("5").Ok());
    }

    Y_UNIT_TEST(BoolValidation) {
        TBoolBuilder b;
        auto v = b.CreateValidator();

        Y_ENSURE(HasOneIssue(v.Validate("{}"), {"/", "Node must be Scalar(Bool)"}));
        Y_ENSURE(HasOneIssue(v.Validate("a"), {"/", "Value must be either true or false"}));
        Y_ENSURE(HasOneIssue(v.Validate("1"), {"/", "Value must be either true or false"}));
        Y_ENSURE(HasOneIssue(v.Validate("yes"), {"/", "Value must be either true or false"}));

        Y_ENSURE(v.Validate("true").Ok());
        Y_ENSURE(v.Validate("False").Ok());
        Y_ENSURE(v.Validate("TrUe").Ok());
    }

    Y_UNIT_TEST(StringValidation) {
        NYamlConfig::NValidator::TStringBuilder b;
        auto v = b.CreateValidator();

        Y_ENSURE(HasOneIssue(v.Validate("{}"), {"/", "Node must be Scalar(String)"}));

        Y_ENSURE(v.Validate("text").Ok());
        Y_ENSURE(v.Validate("123").Ok());
    }

    Y_UNIT_TEST(IntArrayValidation) {
        TArrayBuilder b;
        b.Item(TInt64Builder().Range(0, 10));
        // or
        // b.Int64Item([](auto& b) { b.Range(0, 10); });
        auto v = b.CreateValidator();

        Y_ENSURE(HasOneIssue(v.Validate("{}"), {"/", "Node must be Array"}));
        Y_ENSURE(HasOneIssue(v.Validate("[{}]"), {"/0", "Node must be Scalar(Int64)"}));
        Y_ENSURE(HasOneIssue(v.Validate("[1a]"), {"/0", "Can't convert value to Int64"}));
        Y_ENSURE(HasOneIssue(v.Validate("[11]"), {"/0", "Value must be less or equal to max value(i.e <= 10)"}));
        Y_ENSURE(HasOneIssue(v.Validate("[-1]"), {"/0", "Value must be greater or equal to min value(i.e >= 0)"}));

        Y_ENSURE(HasOnlyThisIssues(v.Validate("[{}, 1a, 11, -1, 5]"), {
            {"/0", "Node must be Scalar(Int64)"},
            {"/1", "Can't convert value to Int64"},
            {"/2", "Value must be less or equal to max value(i.e <= 10)"},
            {"/3", "Value must be greater or equal to min value(i.e >= 0)"},
            }));

        Y_ENSURE(v.Validate("[1, 2, 3, 3, 3]").Ok());
        Y_ENSURE(v.Validate("[0, 10, 5]").Ok());
    }

    // Also can add nullable field support
    Y_UNIT_TEST(MapValidation) {
        auto v =
        TMapBuilder()
            .Int64("required_int")
            .Int64("int", [](auto& b) {
                b.Optional()
                .Range(0, 10);
            })
            .Bool("bool")
            .Array("int_array", [](auto& b) {
                b.Int64Item([](auto& b){ b.Range(0, 10); });
            })
            .CreateValidator();

        Y_ENSURE(HasOneIssue(v.Validate("213"), {"/", "Node must be Map"}));

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "int: 5,\n"
                    "bool: true,\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(v.Validate(yaml).Ok());
        }

        {
            TString yaml =
                "{\n"
                    "int: 5,\n"
                    "bool: true,\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(HasOneIssue(v.Validate(yaml), {"/required_int", "Node is required"}));
        }

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "bool: true,\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(v.Validate(yaml).Ok());
        }

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "int: -1,\n"
                    "bool: true,\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(HasOneIssue(v.Validate(yaml), {"/int", "Value must be greater or equal to min value(i.e >= 0)"}));
        }

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "int: 11,\n"
                    "bool: true,\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(HasOneIssue(v.Validate(yaml), {"/int", "Value must be less or equal to max value(i.e <= 10)"}));
        }

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "int: 5,\n"
                    "bool: {},\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(HasOneIssue(v.Validate(yaml), {"/bool", "Node must be Scalar(Bool)"}));
        }

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "int: 5,\n"
                    "bool: not_bool,\n"
                    "int_array: [1,2,3,4,5]\n"
                "}";
            Y_ENSURE(HasOneIssue(v.Validate(yaml), {"/bool", "Value must be either true or false"}));
        }

        {
            TString yaml =
                "{\n"
                    "required_int: 5,\n"
                    "int: 5,\n"
                    "bool: true,\n"
                    "int_array: [{}, 1a, 11, -1, 5]\n"
                "}";
            Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
                {"/int_array/0", "Node must be Scalar(Int64)"},
                {"/int_array/1", "Can't convert value to Int64"},
                {"/int_array/2", "Value must be less or equal to max value(i.e <= 10)"},
                {"/int_array/3", "Value must be greater or equal to min value(i.e >= 0)"},
            }));
        }

        {
            TString yaml =
                "{\n"
                    "int: -1,\n"
                    "bool: not_bool,\n"
                    "int_array: [{}, 1a, 11, -1, 5]\n"
                "}";
            Y_ENSURE(HasOnlyThisIssues(v.Validate(yaml), {
                {"/required_int", "Node is required"},
                {"/int", "Value must be greater or equal to min value(i.e >= 0)"},
                {"/bool", "Value must be either true or false"},
                {"/int_array/0", "Node must be Scalar(Int64)"},
                {"/int_array/1", "Can't convert value to Int64"},
                {"/int_array/2", "Value must be less or equal to max value(i.e <= 10)"},
                {"/int_array/3", "Value must be greater or equal to min value(i.e >= 0)"},
            }));
        }
    }

    Y_UNIT_TEST(MultitypeNodeValidation) {
        TGenericBuilder b;
        b.CanBeBool();
        b.CanBeInt64([](auto& b) { b.Range(0, 10); });
        b.CanBeMap([](auto& b){
            b.Int64("int1")
            .Int64("int2");
        });

        auto v = b.CreateValidator();

        Y_ENSURE(v.Validate("true").Ok());
        Y_ENSURE(v.Validate("FaLsE").Ok());
        Y_ENSURE(v.Validate("0").Ok());
        Y_ENSURE(v.Validate("10").Ok());
        Y_ENSURE(v.Validate("{int1: 1, int2: 2}").Ok());

        // should change error mesage somehow
        Y_ENSURE(HasOneIssue(v.Validate("akaka"), {
            "/", "There was errors in all scenarios:\n"
                 "  1) Value must be either true or false\n"
                 "  2) Can't convert value to Int64\n"
                 "  3) Node must be Map"
        }));
    }

    Y_UNIT_TEST(OpaqueMaps) {
        auto b =
        TMapBuilder()
            .Int64("int")
            .NotOpaque();

        auto notOpaque = b.CreateValidator();

        b.Opaque();
        auto opaque = b.CreateValidator();

        Y_ENSURE(notOpaque.Validate("{int: 1}").Ok());
        Y_ENSURE(HasOneIssue(notOpaque.Validate("{}"), {"/int", "Node is required"}));
        Y_ENSURE(HasOneIssue(
            notOpaque.Validate("{int: 1, int2: 2}"),
            {"/int2", "Unexpected node"}));

        Y_ENSURE(opaque.Validate("{int: 1}").Ok());
        Y_ENSURE(HasOneIssue(opaque.Validate("{}"), {"/int", "Node is required"}));
        Y_ENSURE(opaque.Validate("{int: 1, int2: 2}").Ok());
    }

    Y_UNIT_TEST(Enums) {
        auto v = TEnumBuilder({"a", "b", "c"}).CreateValidator();

        Y_ENSURE(v.Validate("a").Ok());
        Y_ENSURE(v.Validate("b").Ok());
        Y_ENSURE(v.Validate("c").Ok());

        Y_ENSURE(HasOneIssue(
            v.Validate("d"),
            {"/", "Node value must be one of the following: a, b, c. (it was \"d\")"}));
    }
}

} // namespace NKikimr
