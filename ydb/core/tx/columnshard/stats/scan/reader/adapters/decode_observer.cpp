#include "decode_observer.h"

#include <ydb/core/formats/arrow/save_load/loader.h>
#include <ydb/core/formats/arrow/serializer/native.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NStats::NScan {
namespace {

class TDecodeScope: public NArrow::NAccessor::IDecodeScope {
private:
    TOperationScope Scope;
    TDecodeObserver& Observer;
    TOperationNode* const PreviousParent;
    const TCallHandle PreviousCall;
    bool ParentRestored = false;

    void RestoreParent() noexcept {
        if (!ParentRestored) {
            Observer.SetParent(PreviousParent);
            Observer.SetParentCall(PreviousCall);
            ParentRestored = true;
        }
    }

public:
    TDecodeScope(TOperationScope scope, TDecodeObserver& observer)
        : Scope(std::move(scope))
        , Observer(observer)
        , PreviousParent(observer.GetParent())
        , PreviousCall(observer.GetParentCall())
    {
        Observer.SetParent(Scope.GetNode());
        Observer.SetParentCall(Scope.GetHandle());
    }

    ~TDecodeScope() override {
        RestoreParent();
    }

    void Finish(std::optional<ui64> outputRows) override {
        Scope.Observe([&](auto& observation) {
            auto* rows = &observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows);
            rows->SetApplicability(NColumnShardStats::APPLICABLE);
            if (outputRows) {
                rows->SetValue(*outputRows);
            } else {
                rows->MutableCoverage()->SetMissingReason(NColumnShardStats::UNAVAILABLE);
            }
        });
        RestoreParent();
        Scope.Finish();
    }
};

}   // namespace

std::unique_ptr<NArrow::NAccessor::IDecodeScope> TDecodeObserver::OnDecodeBegin(
    const NArrow::NAccessor::TColumnLoader& loader, ui64 encodedBytes, std::optional<ui64> rows) {
    TString codec;
    if (const auto* native = dynamic_cast<const NArrow::NSerialization::TNativeSerializer*>(loader.GetSerializer().GetObjectPtr().get())) {
        codec = arrow::util::Codec::GetCodecAsString(native->GetCodecType());
    }
    const auto& fieldName = loader.GetField()->name();
    const TString name(fieldName.data(), fieldName.size());
    auto& parent = Parent ? *Parent : LocalStats.GetRoot();
    const TString key = TStringBuilder() << "Decode/" << loader.GetColumnId() << "/" << codec << "/" << name;
    auto* node = parent.FindChild(key);
    if (!node) {
        NColumnShardStats::TOperation definition;
        definition.SetKey(key);
        definition.SetName("Decode");
        definition.MutableDecode()->AddColumns(name);
        if (!codec.empty()) {
            definition.MutableDecode()->SetCodec(codec);
        }
        auto* statistics = definition.MutableStatistics();
        statistics->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        statistics->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        statistics->MutableInputBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
        statistics->MutableOutputBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
        statistics->SetInputRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
        node = LocalStats.AddChild(parent, definition);
        if (!node) {
            return {};
        }
    }
    auto scope = LocalStats.Measure(node, ParentCall);
    if (!scope) {
        return {};
    }
    scope.Observe([&](auto& observation) {
        if (rows) {
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(*rows);
        } else {
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows)
                .MutableCoverage()
                ->SetMissingReason(NColumnShardStats::UNAVAILABLE);
        }
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputBytes).SetValue(encodedBytes);
        observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputBytes)
            .MutableCoverage()
            ->SetMissingReason(NColumnShardStats::NOT_REPORTED);
    });
    return std::make_unique<TDecodeScope>(std::move(scope), *this);
}

}   // namespace NKikimr::NOlap::NStats::NScan
