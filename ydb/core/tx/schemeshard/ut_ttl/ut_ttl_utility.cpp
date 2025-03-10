#include <ydb/core/tx/schemeshard/common/validation.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;
using namespace NSchemeShard;

Y_UNIT_TEST_SUITE(TSchemeShardTTLUtility) {
    void TestValidateTiers(const std::vector<NKikimrSchemeOp::TTTLSettings::TTier>& tiers, const TConclusionStatus& expectedResult) {
        google::protobuf::RepeatedPtrField<NKikimrSchemeOp::TTTLSettings_TTier> input;
        for (const auto& tier : tiers) {
            input.Add()->CopyFrom(tier);
        }

        TString error;
        UNIT_ASSERT_VALUES_EQUAL(NValidation::TTTLValidator::ValidateTiers(input, error), expectedResult.IsSuccess());
        if (expectedResult.IsFail()) {
            UNIT_ASSERT_STRING_CONTAINS(error, expectedResult.GetErrorMessage());
        }
    }

    Y_UNIT_TEST(ValidateTiers) {
        NKikimrSchemeOp::TTTLSettings::TTier tierNoAction;
        tierNoAction.SetApplyAfterSeconds(60);
        NKikimrSchemeOp::TTTLSettings::TTier tierNoDuration;
        tierNoDuration.MutableDelete();
        auto makeDeleteTier = [](const ui32 seconds) {
            NKikimrSchemeOp::TTTLSettings::TTier tier;
            tier.MutableDelete();
            tier.SetApplyAfterSeconds(seconds);
            return tier;
        };
        auto makeEvictTier = [](const ui32 seconds) {
            NKikimrSchemeOp::TTTLSettings::TTier tier;
            tier.MutableEvictToExternalStorage()->SetStorage("/Root/abc");
            tier.SetApplyAfterSeconds(seconds);
            return tier;
        };

        TestValidateTiers({ tierNoAction }, TConclusionStatus::Fail("Tier 0: missing Action"));
        TestValidateTiers({ tierNoDuration }, TConclusionStatus::Fail("Tier 0: missing ApplyAfterSeconds"));
        TestValidateTiers({ makeDeleteTier(1) }, TConclusionStatus::Success());
        TestValidateTiers({ makeEvictTier(1) }, TConclusionStatus::Success());
        TestValidateTiers({ makeEvictTier(1), makeDeleteTier(2) }, TConclusionStatus::Success());
        TestValidateTiers({ makeEvictTier(1), makeEvictTier(2), makeDeleteTier(3) }, TConclusionStatus::Success());
        TestValidateTiers({ makeEvictTier(1), makeEvictTier(2) }, TConclusionStatus::Success());
        TestValidateTiers({ makeEvictTier(2), makeEvictTier(1) }, TConclusionStatus::Fail("Tiers in the sequence must have increasing ApplyAfterSeconds: 2 (tier 0) >= 1 (tier 1)"));
        TestValidateTiers({ makeDeleteTier(1), makeEvictTier(2) }, TConclusionStatus::Fail("Tier 0: only the last tier in TTL settings can have Delete action"));
        TestValidateTiers({ makeDeleteTier(1), makeDeleteTier(2) }, TConclusionStatus::Fail("Tier 0: only the last tier in TTL settings can have Delete action"));
    }

    void ValidateGetExpireAfter(const NKikimrSchemeOp::TTTLSettings::TEnabled& ttlSettings, const bool allowNonDeleteTiers, const TConclusion<TDuration>& expectedResult) {
        auto result = GetExpireAfter(ttlSettings, allowNonDeleteTiers);
        UNIT_ASSERT_VALUES_EQUAL(result.IsSuccess(), expectedResult.IsSuccess());
        if (expectedResult.IsFail()) {
            UNIT_ASSERT_STRING_CONTAINS(result.GetErrorMessage(), expectedResult.GetErrorMessage());
        }
    }

    Y_UNIT_TEST(GetExpireAfter) {
        NKikimrSchemeOp::TTTLSettings::TTier evictTier;
        evictTier.MutableEvictToExternalStorage()->SetStorage("/Root/abc");
        evictTier.SetApplyAfterSeconds(1800);
        NKikimrSchemeOp::TTTLSettings::TTier deleteTier;
        deleteTier.MutableDelete();
        deleteTier.SetApplyAfterSeconds(3600);

        {
            NKikimrSchemeOp::TTTLSettings::TEnabled input;
            ValidateGetExpireAfter(input, true, TDuration::Zero());
            ValidateGetExpireAfter(input, false, TDuration::Zero());
        }
        {
            NKikimrSchemeOp::TTTLSettings::TEnabled input;
            input.SetExpireAfterSeconds(60);
            ValidateGetExpireAfter(input, true, TDuration::Seconds(60));
            ValidateGetExpireAfter(input, false, TDuration::Seconds(60));
        }
        {
            NKikimrSchemeOp::TTTLSettings::TEnabled input;
            *input.AddTiers() = deleteTier;
            ValidateGetExpireAfter(input, true, TDuration::Seconds(3600));
            ValidateGetExpireAfter(input, false, TDuration::Seconds(3600));
        }
        {
            NKikimrSchemeOp::TTTLSettings::TEnabled input;
            *input.AddTiers() = evictTier;
            *input.AddTiers() = deleteTier;
            ValidateGetExpireAfter(input, true, TDuration::Seconds(3600));
            ValidateGetExpireAfter(input, false, TConclusionStatus::Fail("Only DELETE via TTL is allowed for row-oriented tables"));
        }
        {
            NKikimrSchemeOp::TTTLSettings::TEnabled input;
            *input.AddTiers() = evictTier;
            ValidateGetExpireAfter(input, true, TConclusionStatus::Fail("TTL settings does not contain DELETE action"));
            ValidateGetExpireAfter(input, false, TConclusionStatus::Fail("Only DELETE via TTL is allowed for row-oriented tables"));
        }
    }
}
