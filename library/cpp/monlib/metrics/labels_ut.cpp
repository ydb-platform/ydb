#include "labels.h"

#include <library/cpp/testing/unittest/registar.h>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(TLabelsTest) {
    TLabel pSolomon("project", "solomon");
    TLabel pKikimr("project", "kikimr");

    Y_UNIT_TEST(Equals) {
        UNIT_ASSERT(pSolomon == TLabel("project", "solomon"));

        UNIT_ASSERT_STRINGS_EQUAL(pSolomon.Name(), "project");
        UNIT_ASSERT_STRINGS_EQUAL(pSolomon.Value(), "solomon");

        UNIT_ASSERT(pSolomon != pKikimr);
    }

    Y_UNIT_TEST(ToString) {
        UNIT_ASSERT_STRINGS_EQUAL(pSolomon.ToString(), "project=solomon");
        UNIT_ASSERT_STRINGS_EQUAL(pKikimr.ToString(), "project=kikimr");
    }

    Y_UNIT_TEST(FromString) {
        auto pYql = TLabel::FromString("project=yql");
        UNIT_ASSERT_EQUAL(pYql, TLabel("project", "yql"));

        UNIT_ASSERT_EQUAL(TLabel::FromString("k=v"), TLabel("k", "v"));
        UNIT_ASSERT_EQUAL(TLabel::FromString("k=v "), TLabel("k", "v"));
        UNIT_ASSERT_EQUAL(TLabel::FromString("k= v"), TLabel("k", "v"));
        UNIT_ASSERT_EQUAL(TLabel::FromString("k =v"), TLabel("k", "v"));
        UNIT_ASSERT_EQUAL(TLabel::FromString(" k=v"), TLabel("k", "v"));
        UNIT_ASSERT_EQUAL(TLabel::FromString(" k = v "), TLabel("k", "v"));

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLabel::FromString(""),
            yexception,
            "invalid label string format");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLabel::FromString("k v"),
            yexception,
            "invalid label string format");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLabel::FromString(" =v"),
            yexception,
            "label name cannot be empty");

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            TLabel::FromString("k= "),
            yexception,
            "label value cannot be empty");
    }

    Y_UNIT_TEST(TryFromString) {
        TLabel pYql;
        UNIT_ASSERT(TLabel::TryFromString("project=yql", pYql));
        UNIT_ASSERT_EQUAL(pYql, TLabel("project", "yql"));

        {
            TLabel label;
            UNIT_ASSERT(TLabel::TryFromString("k=v", label));
            UNIT_ASSERT_EQUAL(label, TLabel("k", "v"));
        }
        {
            TLabel label;
            UNIT_ASSERT(TLabel::TryFromString("k=v ", label));
            UNIT_ASSERT_EQUAL(label, TLabel("k", "v"));
        }
        {
            TLabel label;
            UNIT_ASSERT(TLabel::TryFromString("k= v", label));
            UNIT_ASSERT_EQUAL(label, TLabel("k", "v"));
        }
        {
            TLabel label;
            UNIT_ASSERT(TLabel::TryFromString("k =v", label));
            UNIT_ASSERT_EQUAL(label, TLabel("k", "v"));
        }
        {
            TLabel label;
            UNIT_ASSERT(TLabel::TryFromString(" k=v", label));
            UNIT_ASSERT_EQUAL(label, TLabel("k", "v"));
        }
        {
            TLabel label;
            UNIT_ASSERT(TLabel::TryFromString(" k = v ", label));
            UNIT_ASSERT_EQUAL(label, TLabel("k", "v"));
        }
    }

    Y_UNIT_TEST(Labels) {
        TLabels labels;
        UNIT_ASSERT(labels.Add(TStringBuf("name1"), TStringBuf("value1")));
        UNIT_ASSERT(labels.Size() == 1);
        UNIT_ASSERT(labels.Has(TStringBuf("name1")));
        {
            auto l = labels.Find("name1");
            UNIT_ASSERT(l.Defined());
            UNIT_ASSERT_STRINGS_EQUAL(l->Name(), "name1");
            UNIT_ASSERT_STRINGS_EQUAL(l->Value(), "value1");
        }
        {
            auto l = labels.Find("name2");
            UNIT_ASSERT(!l.Defined());
        }

        // duplicated name
        UNIT_ASSERT(!labels.Add(TStringBuf("name1"), TStringBuf("value2")));
        UNIT_ASSERT(labels.Size() == 1);

        UNIT_ASSERT(labels.Add(TStringBuf("name2"), TStringBuf("value2")));
        UNIT_ASSERT(labels.Size() == 2);
        UNIT_ASSERT(labels.Has(TStringBuf("name2")));
        {
            auto l = labels.Find("name2");
            UNIT_ASSERT(l.Defined());
            UNIT_ASSERT_STRINGS_EQUAL(l->Name(), "name2");
            UNIT_ASSERT_STRINGS_EQUAL(l->Value(), "value2");
        }

        UNIT_ASSERT_EQUAL(labels[0], TLabel("name1", "value1"));
        UNIT_ASSERT_EQUAL(labels[1], TLabel("name2", "value2"));

        TVector<TLabel> labelsCopy;
        for (auto&& label : labels) {
            labelsCopy.emplace_back(label.Name(), label.Value());
        }

        UNIT_ASSERT_EQUAL(labelsCopy, TVector<TLabel>({
                                          {"name1", "value1"},
                                          {"name2", "value2"},
                                      }));
    }

    Y_UNIT_TEST(Hash) {
        TLabel label("name", "value");
        UNIT_ASSERT_EQUAL(ULL(2378153472115172159), label.Hash());

        {
            TLabels labels = {{"name", "value"}};
            UNIT_ASSERT_EQUAL(ULL(5420514431458887014), labels.Hash());
        }
        {
            TLabels labels = {{"name1", "value1"}, {"name2", "value2"}};
            UNIT_ASSERT_EQUAL(ULL(2226975250396609813), labels.Hash());
        }
    }

    Y_UNIT_TEST(MakeEmptyLabels) {
        {
            auto labels = MakeLabels<TString>();
            UNIT_ASSERT(labels);
            UNIT_ASSERT(labels->Empty());
            UNIT_ASSERT_VALUES_EQUAL(labels->Size(), 0);
        }
        {
            auto labels = MakeLabels<TStringBuf>();
            UNIT_ASSERT(labels);
            UNIT_ASSERT(labels->Empty());
            UNIT_ASSERT_VALUES_EQUAL(labels->Size(), 0);
        }
    }

    Y_UNIT_TEST(MakeLabelsFromInitializerList) {
        auto labels = MakeLabels<TString>({{"my", "label"}});
        UNIT_ASSERT(labels);
        UNIT_ASSERT(!labels->Empty());
        UNIT_ASSERT_VALUES_EQUAL(labels->Size(), 1);

        UNIT_ASSERT(labels->Has("my"));

        auto label = labels->Get("my");
        UNIT_ASSERT(label.has_value());
        UNIT_ASSERT_STRINGS_EQUAL((*label)->Name(), "my");
        UNIT_ASSERT_STRINGS_EQUAL((*label)->Value(), "label");
    }

    Y_UNIT_TEST(MakeLabelsFromOtherLabel) {
        auto labels = MakeLabels({{"my", "label"}});
        UNIT_ASSERT(labels);
        UNIT_ASSERT(!labels->Empty());
        UNIT_ASSERT_VALUES_EQUAL(labels->Size(), 1);

        UNIT_ASSERT(labels->Has("my"));

        auto label = labels->Get("my");
        UNIT_ASSERT(label.has_value());
        UNIT_ASSERT_STRINGS_EQUAL((*label)->Name(), "my");
        UNIT_ASSERT_STRINGS_EQUAL((*label)->Value(), "label");
    }
}
