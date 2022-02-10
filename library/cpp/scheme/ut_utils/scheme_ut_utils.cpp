#include "scheme_ut_utils.h"

#include <library/cpp/colorizer/colors.h>

#include <util/stream/str.h>

namespace NSc {
    namespace NUt {
        NSc::TValue AssertFromJson(TStringBuf val) {
            try {
                return TValue::FromJsonThrow(val);
            } catch (const TSchemeParseException& e) {
                TStringStream s;
                NColorizer::TColors colors;
                s << "\n"
                  << colors.YellowColor() << "Reason:" << colors.OldColor() << "\n"
                  << e.Reason;
                s << "\n"
                  << colors.YellowColor() << "Where:" << colors.OldColor() << "\n"
                  << val.SubStr(0, e.Offset) << colors.RedColor() << val.SubStr(e.Offset) << colors.OldColor() << "\n";
                UNIT_FAIL_IMPL("could not parse json", s.Str());
                return NSc::Null();
            } catch (const yexception& e) {
                TStringStream s;
                s << '\n'
                  << val;
                UNIT_FAIL_IMPL("could not parse json", s.Str());
                return NSc::Null();
            }
        }

        void AssertScheme(const TValue& expected, const TValue& actual) {
            UNIT_ASSERT_JSON_EQ_JSON(actual, expected);
        }

        void AssertSchemeJson(TStringBuf expected, const NSc::TValue& actual) {
            UNIT_ASSERT_JSON_EQ_JSON(actual, expected);
        }

        void AssertJsonJson(TStringBuf expected, TStringBuf actual) {
            UNIT_ASSERT_JSON_EQ_JSON(actual, expected);
        }
    }
}
