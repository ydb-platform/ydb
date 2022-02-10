#pragma once

#include "reader.h"

#include <util/generic/maybe.h>
#include <util/generic/bt_exception.h>
#include <util/generic/yexception.h>
#include <util/system/yassert.h>

/** Imperative recursive-descent parsing helpers.
 *
 *  These functions help verify conditions and advance parser state.
 *  For aggregate parsing functions, common precondition is to require Begin{X}
 *  event prior to function invocation. Thus, parsers are composable by calling
 *  sub-parser after dispatching on opening event, e.g.:
 *
 *    if (reader.LastEvent().Type() == EEventType::BeginMap) {
 *        ReadSomeMap(reader)
 *    }
 *
 */

namespace NYsonPull {
    namespace NReadOps {
        class TExpectationFailure: public TWithBackTrace<yexception> {
        };

        inline void Expect(const TEvent& got, EEventType expected) {
            Y_ENSURE_EX(
                got.Type() == expected,
                TExpectationFailure() << "expected " << expected << ", got " << got);
        }

        inline void Expect(const TScalar& got, EScalarType expected) {
            Y_ENSURE_EX(
                got.Type() == expected,
                TExpectationFailure() << "expected scalar " << expected << ", got " << got);
        }

        // ExpectBegin{X} functions verify that last event WAS X
        // SkipBegin{X} functions verify that next event WILL BE X and CONSUME it

        inline void ExpectBeginStream(TReader& reader) {
            Expect(reader.LastEvent(), EEventType::BeginStream);
        }

        inline void SkipBeginStream(TReader& reader) {
            Expect(reader.NextEvent(), EEventType::BeginStream);
        }

        inline void ExpectBeginMap(TReader& reader) {
            Expect(reader.LastEvent(), EEventType::BeginMap);
        }

        inline void SkipBeginMap(TReader& reader) {
            Expect(reader.NextEvent(), EEventType::BeginMap);
        }

        inline void ExpectBeginList(TReader& reader) {
            Expect(reader.LastEvent(), EEventType::BeginList);
        }

        inline void SkipBeginList(TReader& reader) {
            Expect(reader.NextEvent(), EEventType::BeginList);
        }

        inline bool ReadListItem(TReader& reader) {
            return reader.NextEvent().Type() != EEventType::EndList;
        }

        inline TMaybe<TStringBuf> ReadKey(TReader& reader) {
            const auto& event = reader.NextEvent();
            switch (event.Type()) {
                case EEventType::Key:
                    return event.AsString();
                case EEventType::EndMap:
                    return Nothing();
                default:
                    ythrow yexception() << "Unexpected event: " << event;
            }
        }

        template <typename T = const TScalar&>
        inline T ReadScalar(TReader& reader);

        template <>
        inline const TScalar& ReadScalar<const TScalar&>(TReader& reader) {
            const auto& event = reader.NextEvent();
            Expect(event, EEventType::Scalar);
            return event.AsScalar();
        }

        template <>
        inline i64 ReadScalar<i64>(TReader& reader) {
            const auto& scalar = ReadScalar(reader);
            Expect(scalar, EScalarType::Int64);
            return scalar.AsInt64();
        }

        template <>
        inline ui64 ReadScalar<ui64>(TReader& reader) {
            const auto& scalar = ReadScalar(reader);
            Expect(scalar, EScalarType::UInt64);
            return scalar.AsUInt64();
        }

        template <>
        inline double ReadScalar<double>(TReader& reader) {
            const auto& scalar = ReadScalar(reader);
            Expect(scalar, EScalarType::Float64);
            return scalar.AsFloat64();
        }

        template <>
        inline TStringBuf ReadScalar<TStringBuf>(TReader& reader) {
            const auto& scalar = ReadScalar(reader);
            Expect(scalar, EScalarType::String);
            return scalar.AsString();
        }

        template <>
        inline TString ReadScalar<TString>(TReader& reader) {
            return TString(ReadScalar<TStringBuf>(reader));
        }

        template <>
        inline bool ReadScalar<bool>(TReader& reader) {
            const auto& scalar = ReadScalar(reader);
            Expect(scalar, EScalarType::Boolean);
            return scalar.AsBoolean();
        }

        // Skip value that was already started with `event`
        void SkipCurrentValue(const TEvent& event, TReader& reader);

        // Skip value that starts at `reader.next_event()`
        void SkipValue(TReader& reader);

        // Skip values with attributes, wait for map value
        void SkipControlRecords(TReader& reader);
    }
}
