#pragma once

#include <google/protobuf/util/message_differencer.h>
#include <util/generic/string.h>

#include <gmock/gmock.h>

#include <ostream>

namespace NGTest {
    // Protobuf message compare options
    struct TProtoCompareOptions {
        using EScope = google::protobuf::util::MessageDifferencer::Scope;
        using EMessageFieldComparison = google::protobuf::util::MessageDifferencer::MessageFieldComparison;
        using ERepeatedFieldComparison = google::protobuf::util::MessageDifferencer::RepeatedFieldComparison;
        using EFloatComparison = google::protobuf::util::DefaultFieldComparator::FloatComparison;

        // Sets the scope of the comparison
        EScope Scope = EScope::FULL;
        // Unset fields comparison method
        EMessageFieldComparison MessageFieldComparison = EMessageFieldComparison::EQUIVALENT;
        // Sets the type of comparison for repeated field
        ERepeatedFieldComparison RepeatedFieldComparison = ERepeatedFieldComparison::AS_LIST;
        // Floats and doubles are comparison method
        EFloatComparison FloatComparison = EFloatComparison::EXACT;
        // if true comparator will treat to float or double fields are equal when both are NaN
        bool TreatNanAsEqual = true;
    };

    // Implements the protobuf message equality matcher, which matches two messages using MessageDifferencer.
    template <typename T>
    class TEqualsProtoMatcher {
    public:
        TEqualsProtoMatcher(T expected, const TProtoCompareOptions& options)
            : Expected_(expected)
            , Options_(options)
        {
        }
        TEqualsProtoMatcher(const TEqualsProtoMatcher& other) = default;
        TEqualsProtoMatcher(TEqualsProtoMatcher&& other) = default;

        TEqualsProtoMatcher& operator=(const TEqualsProtoMatcher& other) = delete;
        TEqualsProtoMatcher& operator=(TEqualsProtoMatcher&& other) = delete;

        template <class X>
        operator ::testing::Matcher<X>() const {
            return ::testing::MakeMatcher(new TImpl<X>(Expected_, Options_));
        }

    private:
        // Implements the protobuf message equality matcher as a Matcher<X>.
        template <class X>
        class TImpl : public ::testing::MatcherInterface<X> {
        public:
            TImpl(T expected, const TProtoCompareOptions& options)
                : Expected_(expected)
                , Options_(options)
            {
            }

            bool MatchAndExplain(X actual, ::testing::MatchResultListener* listener) const override {
                google::protobuf::util::DefaultFieldComparator cmp;
                cmp.set_float_comparison(Options_.FloatComparison);
                cmp.set_treat_nan_as_equal(Options_.TreatNanAsEqual);

                google::protobuf::util::MessageDifferencer md;
                md.set_scope(Options_.Scope);
                md.set_message_field_comparison(Options_.MessageFieldComparison);
                md.set_repeated_field_comparison(Options_.RepeatedFieldComparison);
                md.set_field_comparator(&cmp);

                TString diff;
                md.ReportDifferencesToString(&diff);

                if (!md.Compare(actual, ::testing::internal::Unwrap(Expected_))) {
                    if (listener->IsInterested()) {
                        *listener << diff.c_str();
                    }
                    return false;
                } else {
                    if (listener->IsInterested()) {
                        *listener << "is equal.";
                    }
                    return true;
                }
            }

            void DescribeTo(::std::ostream* os) const override {
                *os << "message is equal to ";
                ::testing::internal::UniversalPrint(::testing::internal::Unwrap(Expected_), os);
            }

            void DescribeNegationTo(::std::ostream* os) const override {
                *os << "message isn't equal to ";
                ::testing::internal::UniversalPrint(::testing::internal::Unwrap(Expected_), os);
            }

        private:
            const T Expected_;
            const TProtoCompareOptions Options_;
        };

    private:
        const T Expected_;
        const TProtoCompareOptions Options_;
    };

    /**
     * Matcher that checks that protobuf messages are equal.
     *
     * Example:
     *
     * ```c++
     * EXPECT_THAT(actualMessage, EqualsProto(std::ref(expectedMessage));
     * ```
     * NOTE: according to the documentation there is a similar matcher in the internal gtest implementation of Google
     * and it may appear in the opensource in course of this `https://github.com/google/googletest/issues/1761`
     */

    template <class T>
    TEqualsProtoMatcher<T> EqualsProto(T expected, const TProtoCompareOptions& options = {}) {
        return {expected, options};
    }

    template <typename T>
    TEqualsProtoMatcher<T> EqualsProtoStrict(T expected) {
        return EqualsProto(expected, TProtoCompareOptions{
            // Either both fields must be present, or none of them (unset != default value)
            .MessageFieldComparison = TProtoCompareOptions::EMessageFieldComparison::EQUAL,
        });
    }
}
