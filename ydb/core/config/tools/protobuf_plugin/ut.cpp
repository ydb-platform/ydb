#include <ydb/core/config/tools/protobuf_plugin/ut/protos/config_root_test.pb.h>
#include <ydb/core/config/tools/protobuf_plugin/ut/protos/copy_to_test.pb.h>
#include <ydb/core/config/tools/protobuf_plugin/ut/protos/as_map_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

void FillSourceMessage(NKikimrConfig::SourceMessage& source) {
    source.SetStringField1("string1");
    source.SetStringField2("string2");
    source.SetStringField3("string3");
    source.SetStringField4("string4");
    source.AddStringField5("string5-1");
    source.AddStringField5("string5-2");
    source.AddStringField6("string6-1");
    source.AddStringField6("string6-2");
    source.AddStringField6("string6-3");
    source.AddStringField7("string7-1");
    source.AddStringField7("string7-2");
    source.AddStringField7("string7-3");
    source.AddStringField7("string7-4");
    source.AddStringField8("string8-1");
    source.AddStringField8("string8-2");
    source.AddStringField8("string8-3");
    source.AddStringField8("string8-4");
    source.AddStringField8("string8-5");

    source.SetIntField(1);

    source.MutableComplexMessage1()->SetStringField("cm1-string");
    source.MutableComplexMessage2()->SetStringField("cm2-string");
    source.MutableComplexMessage3()->SetStringField("cm3-string");
    source.MutableComplexMessage4()->SetStringField("cm4-string");
    source.AddComplexMessage5()->SetStringField("cm5-string-1");
    source.AddComplexMessage5()->SetStringField("cm5-string-2");
    source.AddComplexMessage6()->SetStringField("cm6-string-1");
    source.AddComplexMessage6()->SetStringField("cm6-string-2");
    source.AddComplexMessage6()->SetStringField("cm6-string-3");
    source.AddComplexMessage7()->SetStringField("cm7-string-1");
    source.AddComplexMessage7()->SetStringField("cm7-string-2");
    source.AddComplexMessage7()->SetStringField("cm7-string-3");
    source.AddComplexMessage7()->SetStringField("cm7-string-4");
    source.AddComplexMessage8()->SetStringField("cm8-string-1");
    source.AddComplexMessage8()->SetStringField("cm8-string-2");
    source.AddComplexMessage8()->SetStringField("cm8-string-3");
    source.AddComplexMessage8()->SetStringField("cm8-string-4");
    source.AddComplexMessage8()->SetStringField("cm8-string-5");
}

}

Y_UNIT_TEST_SUITE(ValidationTests) {
    Y_UNIT_TEST(CanDispatchByTag) {
        NKikimrConfig::ActualConfigMessage msg;

        msg.MutableField21();

        auto [has1, get1, mut1] = NKikimrConfig::ActualConfigMessage::GetFieldAccessorsByFieldTag(NKikimrConfig::ActualConfigMessage::TField1FieldTag{});
        auto [has21, get21, mut21] = NKikimrConfig::ActualConfigMessage::GetFieldAccessorsByFieldTag(NKikimrConfig::ActualConfigMessage::TField21FieldTag{});

        Y_UNUSED(get21, get1);

        UNIT_ASSERT(!(msg.*has1)());
        UNIT_ASSERT((msg.*has21)());

        (msg.*mut1)();

        UNIT_ASSERT(msg.HasField1());
        UNIT_ASSERT((msg.*has1)());
    }

    Y_UNIT_TEST(CanCopyTo) {
        NKikimrConfig::SourceMessage source;
        NKikimrConfig::FirstSinkMessage firstSink;
        NKikimrConfig::SecondSinkMessage secondSink;

        source.CopyToFirstSinkMessage(firstSink);
        source.CopyToSecondSinkMessage(secondSink);

        UNIT_ASSERT(!source.HasStringField1());
        UNIT_ASSERT(!source.HasStringField2());
        UNIT_ASSERT(!source.HasStringField3());
        UNIT_ASSERT(!source.HasStringField4());
        UNIT_ASSERT(!source.StringField5Size());
        UNIT_ASSERT(!source.StringField6Size());
        UNIT_ASSERT(!source.StringField7Size());
        UNIT_ASSERT(!source.StringField8Size());
        UNIT_ASSERT(!source.HasComplexMessage1());
        UNIT_ASSERT(!source.HasComplexMessage2());
        UNIT_ASSERT(!source.HasComplexMessage3());
        UNIT_ASSERT(!source.HasComplexMessage4());
        UNIT_ASSERT(!source.ComplexMessage5Size());
        UNIT_ASSERT(!source.ComplexMessage6Size());
        UNIT_ASSERT(!source.ComplexMessage7Size());
        UNIT_ASSERT(!source.ComplexMessage8Size());

        UNIT_ASSERT(!firstSink.HasStringField2());
        UNIT_ASSERT(!firstSink.HasStringField4());
        UNIT_ASSERT(!firstSink.StringField6Size());
        UNIT_ASSERT(!firstSink.StringField8Size());
        UNIT_ASSERT(!firstSink.HasComplexMessage2());
        UNIT_ASSERT(!firstSink.HasComplexMessage4());
        UNIT_ASSERT(!firstSink.ComplexMessage6Size());
        UNIT_ASSERT(!firstSink.ComplexMessage8Size());

        UNIT_ASSERT(!secondSink.HasStringField3());
        UNIT_ASSERT(!secondSink.HasStringField4());
        UNIT_ASSERT(!secondSink.StringField7Size());
        UNIT_ASSERT(!secondSink.StringField8Size());
        UNIT_ASSERT(!secondSink.HasComplexMessage3());
        UNIT_ASSERT(!secondSink.HasComplexMessage4());
        UNIT_ASSERT(!secondSink.ComplexMessage7Size());
        UNIT_ASSERT(!secondSink.ComplexMessage8Size());

        FillSourceMessage(source);

        firstSink.SetIntField(2);
        secondSink.SetIntField(3);

        TString expectedSource = source.ShortDebugString();

        source.CopyToFirstSinkMessage(firstSink);

        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField2(), source.GetStringField2());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField4(), source.GetStringField4());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.StringField6Size(), source.StringField6Size());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField6(0), source.GetStringField6(0));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField6(1), source.GetStringField6(1));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField6(2), source.GetStringField6(2));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.StringField8Size(), source.StringField8Size());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField8(0), source.GetStringField8(0));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField8(1), source.GetStringField8(1));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField8(2), source.GetStringField8(2));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField8(3), source.GetStringField8(3));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetStringField8(4), source.GetStringField8(4));
        UNIT_ASSERT_VALUES_EQUAL(firstSink.ComplexMessage6Size(), source.ComplexMessage6Size());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage6(0).ShortDebugString(), source.GetComplexMessage6(0).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage6(1).ShortDebugString(), source.GetComplexMessage6(1).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage6(2).ShortDebugString(), source.GetComplexMessage6(2).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.ComplexMessage8Size(), source.ComplexMessage8Size());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage8(0).ShortDebugString(), source.GetComplexMessage8(0).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage8(1).ShortDebugString(), source.GetComplexMessage8(1).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage8(2).ShortDebugString(), source.GetComplexMessage8(2).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage8(3).ShortDebugString(), source.GetComplexMessage8(3).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetComplexMessage8(4).ShortDebugString(), source.GetComplexMessage8(4).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(firstSink.GetIntField(), 2);

        UNIT_ASSERT_VALUES_EQUAL(expectedSource, source.ShortDebugString());

        TString expectedFirstSink = firstSink.ShortDebugString();

        source.CopyToSecondSinkMessage(secondSink);

        UNIT_ASSERT_VALUES_EQUAL(expectedSource, source.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(expectedFirstSink, firstSink.ShortDebugString());

        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField3(), source.GetStringField3());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField4(), source.GetStringField4());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.StringField7Size(), source.StringField7Size());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField7(0), source.GetStringField7(0));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField7(1), source.GetStringField7(1));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField7(2), source.GetStringField7(2));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField7(3), source.GetStringField7(3));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.StringField8Size(), source.StringField8Size());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField8(0), source.GetStringField8(0));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField8(1), source.GetStringField8(1));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField8(2), source.GetStringField8(2));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField8(3), source.GetStringField8(3));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetStringField8(4), source.GetStringField8(4));
        UNIT_ASSERT_VALUES_EQUAL(secondSink.ComplexMessage7Size(), source.ComplexMessage7Size());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage7(0).ShortDebugString(), source.GetComplexMessage7(0).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage7(1).ShortDebugString(), source.GetComplexMessage7(1).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage7(2).ShortDebugString(), source.GetComplexMessage7(2).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage7(3).ShortDebugString(), source.GetComplexMessage7(3).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.ComplexMessage8Size(), source.ComplexMessage8Size());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage8(0).ShortDebugString(), source.GetComplexMessage8(0).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage8(1).ShortDebugString(), source.GetComplexMessage8(1).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage8(2).ShortDebugString(), source.GetComplexMessage8(2).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage8(3).ShortDebugString(), source.GetComplexMessage8(3).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetComplexMessage8(4).ShortDebugString(), source.GetComplexMessage8(4).ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(secondSink.GetIntField(), 3);
    }

    Y_UNIT_TEST(MapType) {
        NKikimrConfig::MessageWithMap msg;
        TString nameBase = "Entry";
        for (int i = 1; i <= 10; ++i) {
            TString name = nameBase + ToString(i);
            UNIT_ASSERT_VALUES_EQUAL(msg.GetSomeMap(name).size(), 0);
        }

        UNIT_CHECK_GENERATED_EXCEPTION(msg.GetSomeMap("otherName"), std::out_of_range);
        UNIT_CHECK_GENERATED_EXCEPTION(msg.GetSomeMap("Entry0"), std::out_of_range);

        UNIT_ASSERT(!msg.Entry1Size());
        UNIT_ASSERT(!msg.Entry2Size());
        UNIT_ASSERT(!msg.Entry3Size());
        UNIT_ASSERT(!msg.Entry4Size());
        UNIT_ASSERT(!msg.Entry5Size());
        UNIT_ASSERT(!msg.Entry6Size());
        UNIT_ASSERT(!msg.Entry7Size());
        UNIT_ASSERT(!msg.Entry8Size());
        UNIT_ASSERT(!msg.Entry9Size());
        UNIT_ASSERT(!msg.Entry10Size());

        for (int i = 1; i <= 10; ++i) {
            TString name = nameBase + ToString(i);
            msg.AddSomeMap(name)->SetStr(ToString(i));
        }

        UNIT_CHECK_GENERATED_EXCEPTION(msg.AddSomeMap("otherName"), std::out_of_range);

        UNIT_ASSERT_VALUES_EQUAL(msg.Entry1Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry1(0).GetStr(), "1");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry2Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry2(0).GetStr(), "2");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry3Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry3(0).GetStr(), "3");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry4Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry4(0).GetStr(), "4");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry5Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry5(0).GetStr(), "5");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry6Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry6(0).GetStr(), "6");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry7Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry7(0).GetStr(), "7");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry8Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry8(0).GetStr(), "8");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry9Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry9(0).GetStr(), "9");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry10Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry10(0).GetStr(), "10");

        for (int i = 1; i <= 10; ++i) {
            TString name = nameBase + ToString(i);
            msg.MutableSomeMap(name)->Add()->SetStr(ToString(i * i));
        }

        UNIT_CHECK_GENERATED_EXCEPTION(msg.MutableSomeMap("otherName"), std::out_of_range);

        UNIT_ASSERT_VALUES_EQUAL(msg.Entry1Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry1(1).GetStr(), "1");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry2Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry2(1).GetStr(), "4");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry3Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry3(1).GetStr(), "9");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry4Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry4(1).GetStr(), "16");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry5Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry5(1).GetStr(), "25");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry6Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry6(1).GetStr(), "36");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry7Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry7(1).GetStr(), "49");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry8Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry8(1).GetStr(), "64");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry9Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry9(1).GetStr(), "81");
        UNIT_ASSERT_VALUES_EQUAL(msg.Entry10Size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(msg.GetEntry10(1).GetStr(), "100");
    }

    Y_UNIT_TEST(AdvancedCopyTo) {
        NKikimrConfig::SourceMessage source;
        NKikimrConfig::ThirdSinkMessage thirdSink;
        NKikimrConfig::FourthSinkMessage fourthSink;

        FillSourceMessage(source);

        source.CopyToThirdSinkMessage(thirdSink);
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField2(), "string2");
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField4(), "string4");
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.StringField8Size(), 5);
        for (ui32 i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField8(i), Sprintf("test-string8-%d", i + 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.RenamedStringField1Size(), 3);
        for (ui32 i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedStringField1(i), Sprintf("string6-%d", i + 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.RenamedStringField2Size(), 4);
        for (ui32 i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedStringField2(i), Sprintf("string7-%d", i + 1));
        }
        UNIT_ASSERT(thirdSink.HasRenamedComplexField1());
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedComplexField1().ShortDebugString(), source.GetComplexMessage2().ShortDebugString());

        UNIT_CHECK_GENERATED_EXCEPTION(source.CopyToFourthSinkMessage(fourthSink), std::exception);

        source.MutableComplexMessage2()->SetIntField(100);

        source.CopyToFourthSinkMessage(fourthSink);

        UNIT_ASSERT_VALUES_EQUAL(fourthSink.GetStringField4(), "string4");
        UNIT_ASSERT(fourthSink.HasComplexMessage2());
        UNIT_ASSERT_VALUES_EQUAL(fourthSink.GetComplexMessage2().GetStringField(), "142");
        UNIT_ASSERT_VALUES_EQUAL(fourthSink.GetComplexMessage2().GetIntField(), 142);

        thirdSink.SetStringField2("changed");
        thirdSink.SetRenamedStringField1(0, "unchanged");
        source.CopyToThirdSinkMessage(thirdSink);

        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField2(), "string2");
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField4(), "string4");
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.StringField8Size(), 10);
        for (ui32 i = 0; i < 5; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField8(i), Sprintf("test-string8-%d", i + 1));
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetStringField8(i + 5), Sprintf("test-string8-%d", i + 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.RenamedStringField1Size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedStringField1(0), "unchanged");
        for (ui32 i = 1; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedStringField1(i), Sprintf("string6-%d", i + 1));
        }
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.RenamedStringField2Size(), 8);
        for (ui32 i = 0; i < 4; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedStringField2(i), Sprintf("string7-%d", i + 1));
            UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedStringField2(i + 4), Sprintf("string7-%d", i + 1));
        }
        UNIT_ASSERT(thirdSink.HasRenamedComplexField1());
        UNIT_ASSERT_VALUES_EQUAL(thirdSink.GetRenamedComplexField1().GetStringField(), source.GetComplexMessage2().GetStringField());
        UNIT_ASSERT(!thirdSink.GetRenamedComplexField1().HasIntField());
    }

    Y_UNIT_TEST(HasReservedPaths) {
        NKikimrConfig::ActualConfigMessage msg;

        const auto& reserved = msg.GetReservedChildrenPaths();

        UNIT_ASSERT(reserved.contains("/field1/another_reserved_field"));
        UNIT_ASSERT(reserved.contains("/field21/another_reserved_field"));
        UNIT_ASSERT(reserved.contains("/root_reserved_field"));
    }
}

template <>
ui32 NKikimrConfig::SomeComplexMessage::TransformIntFieldToIntFieldForAnotherComplexMessage<ui32, ui32>(const ui32* in) {
    if (in) {
        return *in + 42;
    }
    return 42;
}

template <>
TString NKikimrConfig::SomeComplexMessage::TransformIntFieldToStringFieldForAnotherComplexMessage<ui32, TString>(const ui32* in) {
    if (in) {
        return ToString(*in + 42);
    }
    throw std::exception();
}

template<>
TString NKikimrConfig::SourceMessage::TransformStringField8ToStringField8ForThirdSinkMessage<const TString, TString>(const TString* const in) {
    if (in) {
        return TString("test-") + *in;
    }
    return "default";
}
