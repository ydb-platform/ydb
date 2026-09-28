#include "yql_activation_groups.h"

#include "yql_config_qplayer.h"

#include <yql/essentials/providers/common/activation/yql_activation.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <util/generic/hash_set.h>

#include <ranges>

namespace NYql::NCommon {

namespace {

constexpr TStringBuf ActivationGroupsLabel = "ActivationGroups";

struct TActivationGroupReference {
    google::protobuf::Message* Parent = nullptr;
    const google::protobuf::FieldDescriptor* ContainingField = nullptr;
    int ItemIndex = -1;
    TActivationPercentage* Activation = nullptr;
    TString Name;
};

struct TValidatedActivationGroups {
    TVector<TActivationGroupReference> References;
    THashSet<TString> ReferencedNames;
};

TActivationPercentage* FindActivationGroup(google::protobuf::Message& message) {
    const auto* descriptor = message.GetDescriptor();
    const auto* activationField = descriptor->FindFieldByName("Activation");
    if (!activationField || activationField->is_repeated() ||
        activationField->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE ||
        activationField->message_type() != TActivationPercentage::descriptor())
    {
        return nullptr;
    }

    const auto* reflection = message.GetReflection();
    if (!reflection->HasField(message, activationField)) {
        return nullptr;
    }

    auto* activation = static_cast<TActivationPercentage*>(reflection->MutableMessage(&message, activationField));
    return activation->HasActivationGroup() ? activation : nullptr;
}

template <typename TVisitor>
void VisitMessages(
    google::protobuf::Message* message,
    google::protobuf::Message* parent,
    const google::protobuf::FieldDescriptor* containingField,
    int itemIndex,
    const TVisitor& visitor)
{
    visitor(message, parent, containingField, itemIndex);

    const auto* descriptor = message->GetDescriptor();
    const auto* reflection = message->GetReflection();

    for (int index = 0; index < descriptor->field_count(); ++index) {
        const auto* field = descriptor->field(index);
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }

        if (field->is_repeated()) {
            for (int repeatedIndex = 0; repeatedIndex < reflection->FieldSize(*message, field); ++repeatedIndex) {
                VisitMessages(
                    reflection->MutableRepeatedMessage(message, field, repeatedIndex),
                    message,
                    field,
                    repeatedIndex,
                    visitor);
            }
        } else if (reflection->HasField(*message, field)) {
            VisitMessages(reflection->MutableMessage(message, field), message, field, -1, visitor);
        }
    }
}

TValidatedActivationGroups ValidateActivationGroups(TGatewaysConfig& gateways) {
    const NConfig::TActivationGroupRegistry activationGroups(gateways);

    TValidatedActivationGroups validated;
    VisitMessages(
        &gateways,
        /*parent=*/nullptr,
        /*containingField=*/nullptr,
        /*itemIndex=*/-1,
        [&](google::protobuf::Message* message, google::protobuf::Message* parent,
            const google::protobuf::FieldDescriptor* containingField, int itemIndex) {
            auto* activation = FindActivationGroup(*message);
            if (!activation) {
                return;
            }

            const auto name = activation->GetActivationGroup();
            activationGroups.Resolve(*activation);
            validated.ReferencedNames.insert(name);
            validated.References.push_back({parent, containingField, itemIndex, activation, name});
        });

    return validated;
}

void SetActivatedMarker(const TActivationGroupReference& reference) {
    reference.Activation->Clear();
    reference.Activation->SetPercentage(100);
    reference.Activation->SetExcludeRobots(false);
}

void RemoveRepeatedMessage(
    google::protobuf::Message* message,
    const google::protobuf::FieldDescriptor* field,
    int itemIndex)
{
    const auto* reflection = message->GetReflection();
    const int size = reflection->FieldSize(*message, field);
    for (int index = itemIndex; index + 1 < size; ++index) {
        reflection->SwapElements(message, field, index, index + 1);
    }
    reflection->RemoveLast(message, field);
}

void ApplySelectedGroups(
    const TValidatedActivationGroups& validated,
    const THashSet<TString>& selectedNames)
{
    for (const auto& reference : std::ranges::reverse_view(validated.References)) {
        if (selectedNames.contains(reference.Name)) {
            SetActivatedMarker(reference);
        } else if (reference.ItemIndex >= 0) {
            RemoveRepeatedMessage(reference.Parent, reference.ContainingField, reference.ItemIndex);
        } else {
            reference.Parent->GetReflection()->ClearField(reference.Parent, reference.ContainingField);
        }
    }
}

} // namespace

TVector<TString> ApplyActivationGroupsInplace(
    TGatewaysConfig& gateways,
    const TString& username,
    const TCredentials::TPtr& credentials,
    const TQContext& qContext)
{
    // Validate before SelectAndSaveActivatedFlags can write a decision to QPlayer.
    const auto validated = ValidateActivationGroups(gateways);
    if (gateways.ActivationGroupSize() == 0) {
        return {};
    }

    // QPlayer replay returns the stored selection without invoking the filter.
    // Use the returned groups as the source of truth.
    const auto filter = NConfig::MakeActivationFilter<TActivationGroup>(username, credentials);
    const auto selectedGroups = SelectAndSaveActivatedFlags<TActivationGroup>(
        TString(ActivationGroupsLabel),
        qContext,
        gateways.GetActivationGroup(),
        filter,
        /*hasProviderName=*/true);

    THashSet<TString> selectedNames;
    for (const auto& group : selectedGroups) {
        selectedNames.insert(group.GetName());
    }

    ApplySelectedGroups(validated, selectedNames);

    TVector<TString> activatedGroups;
    for (const auto& group : gateways.GetActivationGroup()) {
        const auto& name = group.GetName();
        if (selectedNames.contains(name) && validated.ReferencedNames.contains(name)) {
            activatedGroups.push_back(name);
        }
    }

    gateways.ClearActivationGroup();
    return activatedGroups;
}

} // namespace NYql::NCommon
