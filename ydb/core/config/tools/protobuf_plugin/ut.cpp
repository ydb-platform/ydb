#include <ydb/core/config/tools/protobuf_plugin/ut/protos/config_root_test.pb.h>
#include <ydb/core/config/tools/protobuf_plugin/ut/protos/copy_to_test.pb.h>

#include <library/cpp/testing/unittest/registar.h>

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
}
