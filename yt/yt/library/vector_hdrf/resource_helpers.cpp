#include "resource_helpers.h"

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/library/numeric/serialize/fixed_point_number.h>

namespace NYT::NVectorHdrf {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TJobResources ToJobResources(const TJobResourcesConfig& config, TJobResources defaultValue)
{
    if (config.UserSlots) {
        defaultValue.SetUserSlots(*config.UserSlots);
    }
    if (config.Cpu) {
        defaultValue.SetCpu(*config.Cpu);
    }
    if (config.Network) {
        defaultValue.SetNetwork(*config.Network);
    }
    if (config.Memory) {
        defaultValue.SetMemory(*config.Memory);
    }
    if (config.Gpu) {
        defaultValue.SetGpu(*config.Gpu);
    }
    return defaultValue;
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TJobResources& resources, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
    #define XX(name, Name) .Item(#name).Value(resources.Get##Name())
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
        .EndMap();
}

void Deserialize(TJobResources& resources, INodePtr node)
{
    auto mapNode = node->AsMap();
    #define XX(name, Name) \
        if (auto child = mapNode->FindChild(#name)) { \
            auto value = resources.Get##Name(); \
            Deserialize(value, child); \
            resources.Set##Name(value); \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void FormatValue(TStringBuilderBase* builder, const TJobResources& resources, TStringBuf /* format */)
{
    builder->AppendFormat(
        "{UserSlots: %v, Cpu: %v, Gpu: %v, Memory: %vMB, Network: %v}",
        resources.GetUserSlots(),
        resources.GetCpu(),
        resources.GetGpu(),
        resources.GetMemory() / 1_MB,
        resources.GetNetwork());
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TResourceVolume& volume, NYson::IYsonConsumer* consumer)
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            #define XX(name, Name) .Item(#name).Value(volume.Get##Name())
            ITERATE_JOB_RESOURCES(XX)
            #undef XX
        .EndMap();
}

void Deserialize(TResourceVolume& volume, INodePtr node)
{
    auto mapNode = node->AsMap();
    #define XX(name, Name) \
        if (auto child = mapNode->FindChild(#name)) { \
            auto value = volume.Get##Name(); \
            Deserialize(value, child); \
            volume.Set##Name(value); \
        }
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
}

void Deserialize(TResourceVolume& volume, TYsonPullParserCursor* cursor)
{
    Deserialize(volume, ExtractTo<INodePtr>(cursor));
}

void Serialize(const TResourceVector& resourceVector, IYsonConsumer* consumer)
{
    auto fluent = NYTree::BuildYsonFluently(consumer).BeginMap();
    for (int index = 0; index < ResourceCount; ++index) {
        fluent
            .Item(FormatEnum(TResourceVector::GetResourceTypeById(index)))
            .Value(resourceVector[index]);
    }
    fluent.EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TResourceVolume& volume, TStringBuf /* format */)
{
    builder->AppendFormat(
        "{UserSlots: %.2f, Cpu: %v, Gpu: %.2f, Memory: %.2fMBs, Network: %.2f}",
        volume.GetUserSlots(),
        volume.GetCpu(),
        volume.GetGpu(),
        volume.GetMemory() / 1_MB,
        volume.GetNetwork());
}

TString ToString(const TResourceVolume& volume)
{
    return ToStringViaBuilder(volume);
}

void FormatValue(TStringBuilderBase* builder, const TResourceVector& resourceVector, TStringBuf format)
{
    auto getResourceSuffix = [] (EJobResourceType resourceType) {
        const auto& resourceNames = TEnumTraits<EJobResourceType>::GetDomainNames();
        switch (resourceType) {
            case EJobResourceType::UserSlots:
                // S is for Slots.
                return 'S';

            default:
                return resourceNames[ToUnderlying(resourceType)][0];
        }
    };

    builder->AppendChar('[');
    bool isFirst = true;
    for (auto resourceType : TEnumTraits<EJobResourceType>::GetDomainValues()) {
        if (!isFirst) {
            builder->AppendChar(' ');
        }
        isFirst = false;

        FormatValue(builder, resourceVector[resourceType], format);
        builder->AppendChar(getResourceSuffix(resourceType));
    }
    builder->AppendChar(']');
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf
