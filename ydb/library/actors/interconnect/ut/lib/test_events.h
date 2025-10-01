#pragma once

#include <ydb/library/actors/interconnect/ut/protos/interconnect_test.pb.h>

namespace NActors {
    enum {
        EvTest = EventSpaceBegin(TEvents::ES_PRIVATE),
        EvTestChan,
        EvTestSmall,
        EvTestLarge,
        EvTestResponse,
        EvTestStartPolling,
    };

    struct TEvTest : TEventPB<TEvTest, NInterconnectTest::TEvTest, EvTest> {
        TEvTest() = default;

        explicit TEvTest(ui64 sequenceNumber) {
            Record.SetSequenceNumber(sequenceNumber);
        }

        TEvTest(ui64 sequenceNumber, const TString& payload) {
            Record.SetSequenceNumber(sequenceNumber);
            Record.SetPayload(payload);
        }
    };

    struct TEvTestLarge : TEventPB<TEvTestLarge, NInterconnectTest::TEvTestLarge, EvTestLarge> {
        TEvTestLarge() = default;

        TEvTestLarge(ui64 sequenceNumber, const TString& payload) {
            Record.SetSequenceNumber(sequenceNumber);
            Record.SetPayload(payload);
        }
    };

    struct TEvTestSmall : TEventPB<TEvTestSmall, NInterconnectTest::TEvTestSmall, EvTestSmall> {
        TEvTestSmall() = default;

        TEvTestSmall(ui64 sequenceNumber, const TString& payload) {
            Record.SetSequenceNumber(sequenceNumber);
            Record.SetPayload(payload);
        }
    };

    struct TEvTestResponse : TEventPB<TEvTestResponse, NInterconnectTest::TEvTestResponse, EvTestResponse> {
        TEvTestResponse() = default;

        TEvTestResponse(ui64 confirmedSequenceNumber) {
            Record.SetConfirmedSequenceNumber(confirmedSequenceNumber);
        }
    };

    struct TEvTestStartPolling : TEventPB<TEvTestStartPolling, NInterconnectTest::TEvTestStartPolling, EvTestStartPolling> {
        TEvTestStartPolling() = default;
    };

}
