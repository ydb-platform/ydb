#include <yql/essentials/providers/common/config/yql_config_qplayer.h>
#include <yql/essentials/core/qplayer/storage/memory/yql_qstorage_memory.h>

#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/string/split.h>

namespace NYql::NCommon {

namespace {

struct TTestAttr {
    TString Name;
    TString Value;
    bool WithActivation = false;
    bool MustBeActivated = true;

    const TString& GetName() const {
        return Name;
    }

    bool HasActivation() const {
        return WithActivation;
    }

    bool SerializeToString(TString* out) const {
        *out = Name + "|" + Value + "|" + (WithActivation ? "1" : "0");
        return true;
    }

    bool ParseFromString(const TString& data) {
        TVector<TStringBuf> fields;
        StringSplitter(data).Split('|').AddTo(&fields);
        if (fields.size() != 3) {
            return false;
        }
        Name = TString(fields[0]);
        Value = TString(fields[1]);
        WithActivation = (fields[2] == "1");
        return true;
    }
};

auto MakeActivationFilter() {
    return [](const TTestAttr& attr) {
        return attr.MustBeActivated;
    };
}

TQContext MakeWriteContext(IQStoragePtr storage) {
    return TQContext(storage->MakeWriter("op", {}));
}

TQContext MakeReadContext(IQStoragePtr storage) {
    return TQContext(storage->MakeReader("op", {}));
}

struct TExpectedFlag {
    TString Name;
    TString Value;
};

TVector<TExpectedFlag> ToExpected(const TVector<TTestAttr>& flags) {
    TVector<TExpectedFlag> result;
    for (const auto& flag : flags) {
        result.push_back({.Name = flag.Name, .Value = flag.Value});
    }
    return result;
}

} // namespace

Y_UNIT_TEST_SUITE(TActivationFlagsQPlayerTest) {

struct TCaptureReplayCase {
    TVector<TTestAttr> Source;
    TVector<TExpectedFlag> Expected;
};

Y_UNIT_TEST(CaptureAndReplayProduceSameFlags) {
    const TVector<TCaptureReplayCase> cases = {
        {
            .Source = {
                TTestAttr{.Name = "plain_a", .Value = "val_a", .WithActivation = false, .MustBeActivated = true},
                TTestAttr{.Name = "activated_b", .Value = "val_b", .WithActivation = true, .MustBeActivated = true},
                TTestAttr{.Name = "skipped_c", .Value = "val_c", .WithActivation = true, .MustBeActivated = false},
                TTestAttr{.Name = "plain_d", .Value = "val_d", .WithActivation = false, .MustBeActivated = true},
            },
            .Expected = {{.Name = "plain_a", .Value = "val_a"}, {.Name = "activated_b", .Value = "val_b"}, {.Name = "plain_d", .Value = "val_d"}},
        },
        {
            .Source = {
                TTestAttr{.Name = "plain_a", .Value = "val_a", .WithActivation = false, .MustBeActivated = true},
                TTestAttr{.Name = "skipped_b", .Value = "val_b", .WithActivation = true, .MustBeActivated = false},
            },
            .Expected = {{.Name = "plain_a", .Value = "val_a"}},
        },
        {
            .Source = {
                TTestAttr{.Name = "only_activated", .Value = "val_x", .WithActivation = true, .MustBeActivated = true},
            },
            .Expected = {{.Name = "only_activated", .Value = "val_x"}},
        },
        {
            .Source = {
                TTestAttr{.Name = "plain_only", .Value = "val_y", .WithActivation = false, .MustBeActivated = true},
            },
            .Expected = {{.Name = "plain_only", .Value = "val_y"}},
        },
    };

    for (const auto& testCase : cases) {
        auto storage = MakeMemoryQStorage();

        TVector<TExpectedFlag> capturedExpected;
        {
            auto writeContext = MakeWriteContext(storage);
            auto capturedFlags = SelectAndSaveActivatedFlags<TTestAttr>(
                "label", writeContext, testCase.Source, MakeActivationFilter(), /*hasProviderName=*/true);
            capturedExpected = ToExpected(capturedFlags);
            writeContext.GetWriter()->Commit().GetValueSync();
        }

        UNIT_ASSERT_VALUES_EQUAL(testCase.Expected.size(), capturedExpected.size());
        for (size_t idx = 0; idx < testCase.Expected.size(); ++idx) {
            UNIT_ASSERT_VALUES_EQUAL(testCase.Expected[idx].Name, capturedExpected[idx].Name);
            UNIT_ASSERT_VALUES_EQUAL(testCase.Expected[idx].Value, capturedExpected[idx].Value);
        }

        auto readContext = MakeReadContext(storage);
        auto replayedFlags = SelectAndSaveActivatedFlags<TTestAttr>(
            "label", readContext, testCase.Source, MakeActivationFilter(), false);
        auto replayedActual = ToExpected(replayedFlags);

        UNIT_ASSERT_VALUES_EQUAL(capturedExpected.size(), replayedActual.size());
        for (size_t idx = 0; idx < capturedExpected.size(); ++idx) {
            UNIT_ASSERT_VALUES_EQUAL(capturedExpected[idx].Name, replayedActual[idx].Name);
            UNIT_ASSERT_VALUES_EQUAL(capturedExpected[idx].Value, replayedActual[idx].Value);
        }
    }
}

Y_UNIT_TEST(OldFormatFlagsReturnedAsIs) {
    auto storage = MakeMemoryQStorage();
    {
        TTestAttr plainAttr = {.Name = "plain_a", .Value = "value_a", .WithActivation = false};
        TTestAttr activatedAttr = {.Name = "activated_b", .Value = "value_b", .WithActivation = true};
        auto oldFormatNode = NYT::TNode::CreateMap();
        TString serializedPlain;
        TString serializedActivated;
        Y_ENSURE(plainAttr.SerializeToString(&serializedPlain));
        Y_ENSURE(activatedAttr.SerializeToString(&serializedActivated));
        oldFormatNode[plainAttr.Name] = serializedPlain;
        oldFormatNode[activatedAttr.Name] = serializedActivated;
        auto yson = NYT::NodeToYsonString(oldFormatNode, NYT::NYson::EYsonFormat::Binary);
        auto writer = storage->MakeWriter("op", {});
        writer->Put({.Component = TString(NPrivate::QplayerActivationComponent), .Label = "label"}, yson).GetValueSync();
        writer->Commit().GetValueSync();
    }

    const TVector<TTestAttr> source = {
        TTestAttr{.Name = "plain_a", .Value = "value_a", .WithActivation = false},
        TTestAttr{.Name = "activated_b", .Value = "value_b_other", .WithActivation = true},
        TTestAttr{.Name = "not_in_store", .Value = "value_x", .WithActivation = true},
    };

    auto readContext = MakeReadContext(storage);
    auto replayedFlags = SelectAndSaveActivatedFlags<TTestAttr>(
        "label", readContext, source, MakeActivationFilter(), false);

    UNIT_ASSERT_VALUES_EQUAL(2U, replayedFlags.size());
    THashMap<TString, TString> replayedByName;
    for (const auto& flag : replayedFlags) {
        replayedByName[flag.Name] = flag.Value;
    }
    UNIT_ASSERT_VALUES_EQUAL("value_a", replayedByName["plain_a"]);
    UNIT_ASSERT_VALUES_EQUAL("value_b", replayedByName["activated_b"]);
}

Y_UNIT_TEST(FilterCalledOncePerItem) {
    const TVector<TTestAttr> source = {
        TTestAttr{.Name = "plain_a", .Value = "value_a", .WithActivation = false},
        TTestAttr{.Name = "activated_b", .Value = "value_b", .WithActivation = true},
        TTestAttr{.Name = "activated_c", .Value = "value_c", .WithActivation = true},
    };
    int filterCallCount = 0;
    auto countingFilter = [&](const TTestAttr&) {
        ++filterCallCount;
        return true;
    };

    TQContext emptyContext;
    SelectAndSaveActivatedFlags<TTestAttr>("label", emptyContext, source, countingFilter, false);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(source.size()), filterCallCount);
}

} // Y_UNIT_TEST_SUITE(TActivationFlagsQPlayerTest)

} // namespace NYql::NCommon
