#include "read_ops.h"

using namespace NYsonPull;
using namespace NYsonPull::NReadOps;

namespace {
    bool TrySkipValueUntil(EEventType end, TReader& reader) {
        const auto& event = reader.NextEvent();
        if (event.Type() == end) {
            return false;
        }
        SkipCurrentValue(event, reader);
        return true;
    }

    bool TrySkipKeyValueUntil(EEventType end, TReader& reader) {
        const auto& event = reader.NextEvent();
        if (event.Type() == end) {
            return false;
        }
        Expect(event, EEventType::Key);
        SkipValue(reader);
        return true;
    }
}

void NYsonPull::NReadOps::SkipCurrentValue(const TEvent& event, TReader& reader) {
    switch (event.Type()) {
        case EEventType::BeginList:
            while (TrySkipValueUntil(EEventType::EndList, reader)) {
            }
            return;

        case EEventType::BeginMap:
            while (TrySkipKeyValueUntil(EEventType::EndMap, reader)) {
            }
            return;

        case EEventType::BeginAttributes:
            while (TrySkipKeyValueUntil(EEventType::EndAttributes, reader)) {
            }
            // attributes after attributes are disallowed in TReader
            SkipValue(reader);
            return;

        case EEventType::Scalar:
            return;

        default:
            throw yexception() << "Unexpected event: " << event;
    }
}

void NYsonPull::NReadOps::SkipValue(TReader& reader) {
    const auto& event = reader.NextEvent();
    SkipCurrentValue(event, reader);
}

void NYsonPull::NReadOps::SkipControlRecords(TReader& reader) {
    const auto* event = &reader.LastEvent();
    while (event->Type() == EEventType::BeginAttributes) {
        SkipCurrentValue(*event, reader);
        event = &reader.NextEvent();
    }
    Expect(*event, EEventType::BeginMap);
}
