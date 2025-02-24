#pragma once

#include "retro_span_base.h"

namespace NRetro {

class TRetroSpanDSProxyRequest : public TTypedRetroSpan<ERetroSpanType::DSProxyRequest> {
public:
    TRetroSpanDSProxyRequest(TInstant start, ui32 groupId)
        : TTyped(start)
        , GroupId(groupId)
    {}

public:
    void AddPartNode(ui32 nodeId) {
        if (PartCount < MaxPartsInBlob) {
            PartNodes[PartCount++] = nodeId;
        }
    }

    std::vector<ui32> GetSubrequestNodeIds() const override {
        std::vector<ui32> nodeIds;
        for (ui32 i = 0; i < PartCount; ++i) {
            nodeIds.push_back(PartNodes[i]);
        }
        return nodeIds;
    }

    ui32 GetGroupId() const {
        return GroupId;
    }

    TString GetName() const override {
        return "DSProxyRequest";
    }

    void FillWilsonSpanAttributes(NWilson::TSpan* span) const override {
        if (span) {
            span->Attribute("GroupId", ToString(GroupId));
            span->Attribute("Subrequests", ToString(PartCount));
        }
    }

private:
    constexpr static ui32 MaxPartsInBlob = 18;

    ui32 GroupId;
    ui32 PartNodes[MaxPartsInBlob];
    ui8 PartCount = 0;
};

class TRetroSpanBackpressureInFlight : public TTypedRetroSpan<ERetroSpanType::BackpressureInFlight> {
public:
    TRetroSpanBackpressureInFlight(TFullSpanId parentId, TInstant start)
        : TTyped(parentId, start)
    {}

    TString GetName() const override {
        return "BackpressureInFlight";
    }
};

/// SizeOf

// TODO: separate implementation
constexpr static ui32 SizeOfRetroSpan(ERetroSpanType type) {
    switch (type) {
#define SIZEOF_CASE(type)                           \
            case ERetroSpanType::type:              \
                return sizeof(TRetroSpan##type);

        SIZEOF_CASE(DSProxyRequest);
        SIZEOF_CASE(BackpressureInFlight);

#undef SIZEOF_CASE
        default:
            Y_ABORT_S("Unknown retrospan type# " << static_cast<ui32>(type));
    }
    return 0;
}

} // namespace NRetro
