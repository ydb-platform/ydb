#include <library/cpp/testing/unittest/gtest.h>
#include <library/cpp/resource/resource.h>

#include <library/cpp/type_info/type_info.h>
#include <library/cpp/type_info/type_io.h>

#include <util/string/strip.h>
#include <util/string/split.h>

using namespace NTi;

std::vector<std::vector<TString>> ParseData(TStringBuf data, int expectedFieldsCount) {
    TString noComments;
    {
        TMemoryInput in(data);
        TString line;
        while (in.ReadLine(line)) {
            if (StripString(line).StartsWith('#')) {
                continue;
            }
            noComments += line;
        }
    }

    std::vector<std::vector<TString>> result;
    for (TStringBuf record : StringSplitter(noComments).SplitByString(";;")) {
        record = StripString(record);
        if (record.Empty()) {
            continue;
        }
        std::vector<TString> fields;
        for (TStringBuf field : StringSplitter(record).SplitByString("::")) {
            fields.emplace_back(StripString(field));
        }
        if (static_cast<int>(fields.size()) != expectedFieldsCount) {
            ythrow yexception() << "Unexpected field count expected: " << expectedFieldsCount << " actual: " << fields.size();
        }
        result.push_back(fields);
    }
    return result;
}

TEST(TestData, GoodTypes) {
    auto records = ParseData(NResource::Find("/good"), 2);

    for (const auto& record : records) {
        const auto& typeYson = record.at(0);
        const auto& typeText = record.at(1);
        TString context = TStringBuilder()
            << "text: " << typeText << Endl
            << "yson: " << typeYson << Endl;
        auto wrapError = [&] (const std::exception& ex) {
            return yexception() << "Unexpected error: " << ex.what() << '\n' << context;
        };

        TTypePtr type;
        try {
            type = NIo::DeserializeYson(*HeapFactory(), typeYson);
        } catch (const std::exception& ex) {
            ythrow wrapError(ex);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(ToString(*type), typeText, context);

        TTypePtr type2;
        try {
            auto yson2 = NIo::SerializeYson(type.Get(), true);
            type2 = NIo::DeserializeYson(*HeapFactory(), yson2);
        } catch (const std::exception& ex) {
            ythrow wrapError(ex);
        }
        UNIT_ASSERT_VALUES_EQUAL_C(*type, *type2, context);
    }
}

TEST(TestData, BadTypes) {
    auto records = ParseData(NResource::Find("/bad"), 3);

    for (const auto& record : records) {
        const auto& typeYson = record.at(0);
        const auto& exceptionMessage = record.at(1);

        TString context = TStringBuilder()
            << "exception: " << exceptionMessage << Endl
            << "yson: " << typeYson << Endl;
        UNIT_ASSERT_EXCEPTION_CONTAINS_C(
            NIo::DeserializeYson(*HeapFactory(), typeYson),
            yexception,
            exceptionMessage,
            context);
    }
}