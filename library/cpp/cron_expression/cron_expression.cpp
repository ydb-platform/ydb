#include "cron_expression.h"

#include <util/generic/bitmap.h>
#include <util/string/ascii.h>
#include <util/string/join.h>
#include <util/string/split.h>

class TCronExpression::TImpl {
private:
    static constexpr uint32_t CRON_MAX_SECONDS = 60;
    static constexpr uint32_t CRON_MAX_MINUTES = 60;
    static constexpr uint32_t CRON_MAX_HOURS = 24;
    static constexpr uint32_t CRON_MAX_DAYS_OF_MONTH = 31;
    static constexpr uint32_t CRON_MAX_DAYS_OF_WEEK = 7;
    static constexpr uint32_t CRON_MAX_MONTHS = 12;
    static constexpr uint32_t CRON_MIN_YEARS = 1970;
    static constexpr uint32_t CRON_MAX_YEARS = 2200;
    static constexpr uint32_t CRON_MAX_YEARS_DIFF = 4;

    static constexpr uint32_t WEEK_DAYS = 7;
    static constexpr uint32_t YEARS_GAP_LENGTH = 231;

    enum class ECronField {
        CF_SECOND,
        CF_MINUTE,
        CF_HOUR_OF_DAY,
        CF_DAY_OF_WEEK,
        CF_DAY_OF_MONTH,
        CF_MONTH,
        CF_YEAR,
        CF_NEXT
    };

    enum class ETokenType {
        TT_ASTERISK,
        TT_QUESTION,
        TT_NUMBER,
        TT_COMMA,
        TT_SLASH,
        TT_L,
        TT_W,
        TT_HASH,
        TT_MINUS,
        TT_WS,
        TT_EOF,
        TT_INVALID
    };

    static constexpr std::array<TStringBuf, 7> DAYS_ARR = {"SUN"sv, "MON"sv, "TUE"sv, "WED"sv, "THU"sv, "FRI"sv, "SAT"sv};
    static constexpr std::array<TStringBuf, 12> CF_MONTHS_ARR = {"JAN"sv, "FEB"sv, "MAR"sv, "APR"sv, "MAY"sv, "JUN"sv, "JUL"sv, "AUG"sv, "SEP"sv, "OCT"sv, "NOV"sv, "DEC"sv};

    static constexpr TStringBuf ErrorWS = "Fields - expected whitespace separator"sv;
    static constexpr TStringBuf ErrorOutOfRange = "Idx out of range"sv;
    static constexpr TStringBuf ErrorUnknownField = "Unknown field"sv;
    static constexpr TStringBuf ErrorDateNotExists = "Requested date does not exist"sv;

    class TCronExpr {
    private:
        TBitMap<60, uint8_t> Seconds;
        TBitMap<60, uint8_t> Minutes;
        TBitMap<24, uint8_t> Hours;

        // Sunday can be represented both as 0 and 7
        TBitMap<8, uint8_t> DaysOfWeek;
        TBitMap<31, uint8_t> DaysOfMonth;
        TBitMap<12, uint8_t> Months;
        int8_t DayInMonth = 0;

        // 0 last day of the month
        // 1 last weekday of the month
        // 2 closest weekday to day in month
        uint8_t Flags = 0;
        TBitMap<YEARS_GAP_LENGTH, uint8_t> Years;

    private:

        template <size_t N>
        static TMaybe<uint32_t> NextSetBit(const TBitMap<N, uint8_t>& bits, uint32_t max, uint32_t fromIndex, uint32_t offset) {
            if (fromIndex < offset || max < offset) {
                ythrow yexception() << ErrorOutOfRange;
            }
            fromIndex -= offset;
            max -= offset;
            if (bits.Get(fromIndex)) {
                return fromIndex + offset;
            }
            uint8_t nextBit = bits.NextNonZeroBit(fromIndex);
            if (nextBit < max) {
                return nextBit + offset;
            }
            return Nothing();
        }

        template <size_t N>
        static TMaybe<uint32_t> PrevSetBit(const TBitMap<N, uint8_t>& bits, uint32_t fromIndex, uint32_t toIndex, uint32_t offset) {
            if (fromIndex < offset || toIndex < offset) {
                ythrow yexception() << ErrorOutOfRange;
            }
            fromIndex -= offset;
            toIndex -= offset;
            for (; fromIndex + 1 > toIndex; --fromIndex) {
                if (bits.Get(fromIndex)) {
                    return fromIndex + offset;
                }
                if (fromIndex == 0) {
                    return Nothing();
                }
            }
            return Nothing();
        }

    public:
        void SetBit(ECronField field, uint32_t idx) {
            switch(field) {
                case ECronField::CF_SECOND: {
                    Seconds.Set(idx);
                    break;
                }
                case ECronField::CF_MINUTE: {
                    Minutes.Set(idx);
                    break;
                }
                case ECronField::CF_HOUR_OF_DAY: {
                    Hours.Set(idx);
                    break;
                }
                case ECronField::CF_DAY_OF_WEEK: {
                    DaysOfWeek.Set(idx);
                    break;
                }
                case ECronField::CF_DAY_OF_MONTH: {
                    if (idx < 1) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    DaysOfMonth.Set(idx - 1);
                    break;
                }
                case ECronField::CF_MONTH: {
                    if (idx < 1) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    Months.Set(idx - 1);
                    break;
                }
                case ECronField::CF_YEAR: {
                    if (idx < 1970) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    Years.Set(idx - 1970);
                    break;
                }
                default:
                    ythrow yexception() << ErrorUnknownField;
            }
        }

        void DelBit(ECronField field, uint32_t idx) {
            switch(field) {
                case ECronField::CF_SECOND: {
                    Seconds.Reset(idx);
                    break;
                }
                case ECronField::CF_MINUTE: {
                    Minutes.Reset(idx);
                    break;
                }
                case ECronField::CF_HOUR_OF_DAY: {
                    Hours.Reset(idx);
                    break;
                }
                case ECronField::CF_DAY_OF_WEEK: {
                    DaysOfWeek.Reset(idx);
                    break;
                }
                case ECronField::CF_DAY_OF_MONTH: {
                    if (idx < 1) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    DaysOfMonth.Reset(idx - 1);
                    break;
                }
                case ECronField::CF_MONTH: {
                    if (idx < 1) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    Months.Reset(idx - 1);
                    break;
                }
                case ECronField::CF_YEAR: {
                    if (idx < 1970) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    Years.Reset(idx - 1970);
                    break;
                }
                default:
                    ythrow yexception() << ErrorUnknownField;
            }
        }

        uint8_t GetBit(ECronField field, uint32_t idx) {
            switch(field) {
                case ECronField::CF_SECOND: {
                    return Seconds.Get(idx);
                }
                case ECronField::CF_MINUTE: {
                    return Minutes.Get(idx);
                }
                case ECronField::CF_HOUR_OF_DAY: {
                    return Hours.Get(idx);
                }
                case ECronField::CF_DAY_OF_WEEK: {
                    return DaysOfWeek.Get(idx);
                }
                case ECronField::CF_DAY_OF_MONTH: {
                    if (idx < 1) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    return DaysOfMonth.Get(idx - 1);
                }
                case ECronField::CF_MONTH: {
                    if (idx < 1) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    return Months.Get(idx - 1);
                }
                case ECronField::CF_YEAR: {
                    if (idx < 1970) {
                        ythrow yexception() << ErrorOutOfRange;
                    }
                    return Years.Get(idx - 1970);
                }
                default: {
                    ythrow yexception() << ErrorUnknownField;
                }
            }
        }

        TMaybe<uint32_t> FindNextSetBit(ECronField field, uint32_t max, uint32_t fromIndex) {
            switch(field) {
                case ECronField::CF_SECOND: {
                    return NextSetBit(Seconds, max, fromIndex, 0);
                }
                case ECronField::CF_MINUTE: {
                    return NextSetBit(Minutes, max, fromIndex, 0);
                }
                case ECronField::CF_HOUR_OF_DAY: {
                    return NextSetBit(Hours, max, fromIndex, 0);
                }
                case ECronField::CF_DAY_OF_WEEK: {
                    return NextSetBit(DaysOfWeek, max, fromIndex, 0);
                }
                case ECronField::CF_DAY_OF_MONTH: {
                    return NextSetBit(DaysOfMonth, max, fromIndex, 1);
                }
                case ECronField::CF_MONTH: {
                    return NextSetBit(Months, max, fromIndex, 1);
                }
                case ECronField::CF_YEAR: {
                    return NextSetBit(Years, max, fromIndex, 1970);
                }
                default:
                    ythrow yexception() << ErrorUnknownField;
            }
        }

        TMaybe<uint32_t> FindPrevSetBit(ECronField field, uint32_t fromIndex, uint32_t toIndex) {
            switch(field) {
                case ECronField::CF_SECOND: {
                    return PrevSetBit(Seconds, fromIndex, toIndex, 0);
                }
                case ECronField::CF_MINUTE: {
                    return PrevSetBit(Minutes, fromIndex, toIndex, 0);
                }
                case ECronField::CF_HOUR_OF_DAY: {
                    return PrevSetBit(Hours, fromIndex, toIndex, 0);
                }
                case ECronField::CF_DAY_OF_WEEK: {
                    return PrevSetBit(DaysOfWeek, fromIndex, toIndex, 0);
                }
                case ECronField::CF_DAY_OF_MONTH: {
                    return PrevSetBit(DaysOfMonth, fromIndex, toIndex, 1);
                }
                case ECronField::CF_MONTH: {
                    return PrevSetBit(Months, fromIndex, toIndex, 1);
                }
                case ECronField::CF_YEAR: {
                    return PrevSetBit(Years, fromIndex, toIndex, 1970);
                }
                default:
                    ythrow yexception() << ErrorUnknownField;
            }
        }

        uint8_t GetFlags() {
            return Flags;
        }

        void SetFlag(uint32_t idx) {
            Y_ENSURE(8 > idx, ErrorOutOfRange);
            Flags |= static_cast<uint8_t>(1 << idx);
        }

        int8_t GetDayInMonth() {
            return DayInMonth;
        }

        void SetDayInMonth(int8_t dim) {
            DayInMonth = dim;
        }

        void AddToDayInMonth(int8_t dim) {
            DayInMonth += dim;
        }
    };

    struct TParserContext {
        TStringBuf input;
        ETokenType Type = ETokenType::TT_INVALID;
        ECronField FieldType = ECronField::CF_SECOND;
        int32_t Value = 0;
        uint32_t Min = 0;
        uint32_t Max = 0;
        bool FixDow = false;
        bool LDow = false;
    };

private:
    TCronExpr Target_;

    static uint32_t GetWeekday(const NDatetime::TCivilSecond& calendar) {
        return (static_cast<int32_t>(NDatetime::GetWeekday(calendar)) + 1) % 7;
    }

    static uint32_t GetField(const NDatetime::TCivilSecond& date, ECronField field) {
        switch (field) {
            case ECronField::CF_SECOND:
                return date.second();
            case ECronField::CF_MINUTE:
                return date.minute();
            case ECronField::CF_HOUR_OF_DAY:
                return date.hour();
            case ECronField::CF_DAY_OF_WEEK:
                return GetWeekday(date);
            case ECronField::CF_DAY_OF_MONTH:
                return date.day();
            case ECronField::CF_MONTH:
                return date.month();
            case ECronField::CF_YEAR:
                return date.year();
            default:
                ythrow yexception() << ErrorUnknownField;
        }
    }

    static uint32_t LastDayOfMonth(uint32_t month, uint32_t year, bool isWeekday) {
        NDatetime::TCivilSecond calendar(year, month + 1, 0);
        uint32_t day = calendar.day();

        if (isWeekday) {
            uint32_t weekday = GetWeekday(calendar);
            if (weekday == 0) {
                if (day < 2) {
                    ythrow yexception() << ErrorOutOfRange;
                }
                day -= 2;
            } else if (weekday == 6) {
                if (day < 1) {
                    ythrow yexception() << ErrorOutOfRange;
                }
                day -= 1;
            }
        }
        return day;
    }

    static uint32_t ClosestWeekday(uint32_t dayOfMonth, uint32_t month, uint32_t year) {
        NDatetime::TCivilSecond calendar;

        if (dayOfMonth > LastDayOfMonth(month, year, false)) {
            ythrow yexception() << "Day of month out of range";
        }

        calendar = NDatetime::TCivilSecond(year, month, dayOfMonth);

        uint32_t wday = GetWeekday(calendar);

        if (wday == 0) {
            if (dayOfMonth == LastDayOfMonth(month, year, 0)) {
                dayOfMonth -= 2;
            } else {
                dayOfMonth += 1;
            }

        } else if (wday == 6) {
            if (dayOfMonth == 1) {
                dayOfMonth += 2;
            } else {
                dayOfMonth -= 1;
            }
        }

        return dayOfMonth;
    }

    static void SetField(NDatetime::TCivilSecond& calendar, ECronField field, int32_t value) {
        switch (field) {
            case ECronField::CF_SECOND:
                calendar = NDatetime::TCivilSecond(calendar.year(), calendar.month(), calendar.day(), calendar.hour(), calendar.minute(), value);
                return;
            case ECronField::CF_MINUTE:
                calendar = NDatetime::TCivilSecond(calendar.year(), calendar.month(), calendar.day(), calendar.hour(), value, calendar.second());
                return;
            case ECronField::CF_HOUR_OF_DAY:
                calendar = NDatetime::TCivilSecond(calendar.year(), calendar.month(), calendar.day(), value, calendar.minute(), calendar.second());
                return;
            case ECronField::CF_DAY_OF_MONTH:
                calendar = NDatetime::TCivilSecond(calendar.year(), calendar.month(), value, calendar.hour(), calendar.minute(), calendar.second());
                return;
            case ECronField::CF_MONTH: {
                int32_t maxDays = LastDayOfMonth(value, calendar.year(), false);
                if (calendar.day() > maxDays) {
                    calendar = NDatetime::TCivilSecond(calendar.year(), calendar.month(), maxDays, calendar.hour(), calendar.minute(), calendar.second());
                }
                calendar = NDatetime::TCivilSecond(calendar.year(), value, calendar.day(), calendar.hour(), calendar.minute(), calendar.second());
                return;
            }
            case ECronField::CF_YEAR:
                calendar = NDatetime::TCivilSecond(value, calendar.month(), calendar.day(), calendar.hour(), calendar.minute(), calendar.second());
                return;
            default:
                ythrow yexception() << ErrorUnknownField;
        }
    }

    static void AddToField(NDatetime::TCivilSecond& calendar, ECronField field, int32_t value) {
        SetField(calendar, field, static_cast<int32_t>(GetField(calendar, field)) + value);
    }

    static void ResetMin(NDatetime::TCivilSecond& calendar, ECronField field) {
        if (field == ECronField::CF_DAY_OF_MONTH || field == ECronField::CF_MONTH) {
            SetField(calendar, field, 1);
        } else {
            SetField(calendar, field, 0);
        }
    }

    static void ResetMax(NDatetime::TCivilSecond& calendar, ECronField field) {
        switch (field) {
            case ECronField::CF_SECOND:
                SetField(calendar, field, CRON_MAX_SECONDS - 1);
                break;
            case ECronField::CF_MINUTE:
                SetField(calendar, field, CRON_MAX_MINUTES - 1);
                break;
            case ECronField::CF_HOUR_OF_DAY:
                SetField(calendar, field, CRON_MAX_HOURS  - 1);
                break;
            case ECronField::CF_DAY_OF_MONTH:
                SetField(calendar, field, LastDayOfMonth(calendar.month(), calendar.year(), 0));
                break;
            default:
                break;
        }
    }

    static void ResetAllMin(NDatetime::TCivilSecond& calendar, TBitMap<7, uint8_t>& fields) {
        for (int32_t i = 0; i < 7; ++i) {

            if (fields.Get(i)) {
                ResetMin(calendar, ECronField(i));
            }
        }
    }

    static void ResetAllMax(NDatetime::TCivilSecond& calendar, TBitMap<7, uint8_t>& fields) {
        for (int32_t i = 0; i < 7; ++i) {
            if (fields.Get(i)) {
                ResetMax(calendar, ECronField(i));
            }
        }
    }

    template <size_t N>
    static TMaybe<uint32_t> MatchOrdinals(TStringBuf str, std::array<TStringBuf, N> arr) {
        size_t arrLen = arr.size();

        for (size_t i = 0; i < arrLen; ++i) {
            if (AsciiHasPrefixIgnoreCase(str, arr[i])) {
                return i;
            }
        }
        return Nothing();
    }

    static void TokenNext(TParserContext& context) {
        auto input = context.input;
        context.Type = ETokenType::TT_INVALID;
        context.Value = 0;

        if (context.input.empty() || context.input.front() == '\0') {
            context.Type = ETokenType::TT_EOF;
        } else if (IsAsciiSpace(context.input.front())) {

            while (!context.input.empty() && IsAsciiSpace(context.input.front())) {
                context.input.Skip(1);
            }
            context.Type = ETokenType::TT_WS;

        } else if (IsAsciiDigit(context.input.front())) {

            while (!context.input.empty() && IsAsciiDigit(context.input.front())) {
                context.Value = context.Value * 10 + (context.input.front() - '0');
                context.input.Skip(1);
            }
            context.Type = ETokenType::TT_NUMBER;

        } else {
            bool isAlpha = IsAsciiAlpha(input.front());

            if (isAlpha) {
                while (!input.empty() && IsAsciiAlpha(input.front())) {
                    input.Skip(1);
                }
                context.Value = MatchOrdinals(context.input, DAYS_ARR).GetOrElse(-1);

                if (context.Value < 0) {
                    context.Value = MatchOrdinals(context.input, CF_MONTHS_ARR).GetOrElse(-1) + 1;
                    if (context.Value == 0) {
                        context.Value = -1;
                    }
                }

                if (context.Value >= 0) {
                    context.input = input;
                    context.Type = ETokenType::TT_NUMBER;
                }
            }

            if (!isAlpha || context.Value < 0) {
                switch (context.input.front()) {
                    case '*':
                        context.Type = ETokenType::TT_ASTERISK;
                        break;
                    case '?':
                        context.Type = ETokenType::TT_QUESTION;
                        break;
                    case ',':
                        context.Type = ETokenType::TT_COMMA;
                        break;
                    case '/':
                        context.Type = ETokenType::TT_SLASH;
                        break;
                    case 'L':
                        context.Type = ETokenType::TT_L;
                        break;
                    case 'W':
                        context.Type = ETokenType::TT_W;
                        break;
                    case '#':
                        context.Type = ETokenType::TT_HASH;
                        break;
                    case '-':
                        context.Type = ETokenType::TT_MINUS;
                        break;
                }
                context.input.Skip(1);
            }
        }

        if (ETokenType::TT_INVALID == context.Type) {
            ythrow yexception() << ErrorUnknownField;
        }
    }

    static int32_t Number(TParserContext& context) {
        int32_t value = 0;

        switch (context.Type) {
            case ETokenType::TT_MINUS:
                TokenNext(context);

                if (ETokenType::TT_NUMBER == context.Type) {
                    value = -context.Value;
                    TokenNext(context);
                } else {
                    ythrow yexception() << "Number '-' follows with number";
                }
                break;
            case ETokenType::TT_NUMBER:
                value = context.Value;
                TokenNext(context);
                break;
            default:
                ythrow yexception() << "Number - error";
        }

        return value;
    }

    static uint32_t Frequency(TParserContext& context, uint32_t delta, uint32_t& to, bool range) {
        switch (context.Type) {
            case ETokenType::TT_SLASH: {
                TokenNext(context);
                if (ETokenType::TT_NUMBER == context.Type) {
                    delta = context.Value;
                    if (delta < 1) {
                        ythrow yexception() << "Frequency - needs to be at least 1";
                    }
                    if (!range) {
                        to = context.Max - 1;
                    }
                    TokenNext(context);
                } else {
                    ythrow yexception() << "Frequency - '/' follows with number";
                }
                break;
            }
            case ETokenType::TT_COMMA:
            case ETokenType::TT_WS:
            case ETokenType::TT_EOF:
                break;
            default:
                ythrow yexception() << "Frequency - error";
        }
        return delta;
    }

    uint32_t Range(TParserContext& context, uint32_t& from, uint32_t to) {
        switch (context.Type) {
            case ETokenType::TT_HASH: {

                if (ECronField::CF_DAY_OF_WEEK == context.FieldType) {
                    TokenNext(context);

                    if (Target_.GetDayInMonth()) {
                        ythrow yexception() << "Nth-day - support for specifying multiple '#' segments is not implemented";
                    }

                    Target_.SetDayInMonth(Number(context));

                    if (Target_.GetDayInMonth() > 5 || Target_.GetDayInMonth() < -5) {
                        ythrow yexception() << "Nth-day - '#' can follow only with -5..5";
                    }
                } else {
                    ythrow yexception() << "Nth-day - '#' allowed only for day of week";
                }
                break;
            }
            case ETokenType::TT_MINUS: {
                TokenNext(context);

                if (ETokenType::TT_NUMBER == context.Type) {
                    to = context.Value;
                    TokenNext(context);
                } else {
                    ythrow yexception() << "Range '-' follows with number";
                }
                break;
            }
            case ETokenType::TT_W: {
                Target_.SetDayInMonth(static_cast<int8_t>(to));
                from = context.Min;

                for (uint32_t i = 1; i <= 5; ++i) {
                    Target_.SetBit(ECronField::CF_DAY_OF_WEEK, i);
                }
                Target_.SetFlag(2);
                to = context.Max - 1;
                TokenNext(context);
                break;
            }
            case ETokenType::TT_L: {
                if (ECronField::CF_DAY_OF_WEEK == context.FieldType) {
                    Target_.SetDayInMonth(-1);
                    TokenNext(context);
                } else {
                    ythrow yexception() << "Range - 'L' allowed only for day of week";
                }
                break;
            }
            case ETokenType::TT_WS:
            case ETokenType::TT_SLASH:
            case ETokenType::TT_COMMA:
            case ETokenType::TT_EOF:
                break;
            default:
                ythrow yexception() << "Range - error";
        }

        return to;
    }

    void Segment(TParserContext& context) {
        uint32_t from = context.Min;
        Y_ENSURE(context.Max > 0);
        uint32_t to = context.Max - 1;
        uint32_t delta = 1;
        bool isLW = false;

        switch (context.Type) {
            case ETokenType::TT_ASTERISK: {
                TokenNext(context);
                delta = Frequency(context, delta, to, false);
                break;
            }
            case ETokenType::TT_NUMBER: {
                from = context.Value;
                TokenNext(context);
                to = Range(context, from, from);
                delta = Frequency(context, delta, to, from != to);
                break;
            }
            case ETokenType::TT_L: {
                TokenNext(context);

                switch (context.FieldType) {
                    case ECronField::CF_DAY_OF_MONTH: {
                        Target_.SetDayInMonth(-1);

                        switch (context.Type) {
                            case ETokenType::TT_MINUS:
                            case ETokenType::TT_NUMBER: {
                                Target_.AddToDayInMonth(Number(context));
                                break;
                            }
                            case ETokenType::TT_W: {
                                if (ECronField::CF_DAY_OF_MONTH == context.FieldType) {
                                    TokenNext(context);

                                    for (uint32_t i = 1; i <= 5; ++i) {
                                        Target_.SetBit(ECronField::CF_DAY_OF_WEEK, i);
                                    }

                                    Target_.SetFlag(1);
                                    context.FixDow = true;
                                    isLW = true;
                                } else {
                                    ythrow yexception() << "Offset - 'W' allowed only for day of month";
                                }
                                break;
                            }
                            case ETokenType::TT_COMMA:
                            case ETokenType::TT_WS:
                            case ETokenType::TT_EOF:
                                break;
                            default:
                                ythrow yexception() << "Offset - error";
                        }
                        /* Note 0..6 and not 1..7*/
                        if (!isLW) {
                            Target_.SetFlag(0);
                            context.FixDow = true;
                            context.LDow = true;
                        }
                        break;
                    }
                    case ECronField::CF_DAY_OF_WEEK:
                        from = to = 0;
                        break;
                    default:
                        ythrow yexception() << "Segment 'L' allowed only for day of month and day of week";
                }
                break;
            }
            case ETokenType::TT_W: {
                if (ECronField::CF_DAY_OF_MONTH != context.FieldType) {
                    ythrow yexception() << "Segment 'W' allowed only for day of month";
                }
                for (uint32_t i = 1; i <= 5; ++i) {
                    Target_.SetBit(ECronField::CF_DAY_OF_WEEK, i);
                }
                TokenNext(context);
                context.FixDow = true;
                break;
            }
            case ETokenType::TT_QUESTION: {
                TokenNext(context);
                break;
            }
            default:
                ythrow yexception() << "Segment - error";
        }

        if (ECronField::CF_DAY_OF_WEEK == context.FieldType && context.FixDow && !context.LDow) {
            return;
        }

        if (from < context.Min || to < context.Min) {
            ythrow yexception() << "Range - specified range is less than minimum";
        }

        if (from >= context.Max || to >= context.Max) {
            ythrow yexception() << "Range - specified range exceeds maximum";
        }

        if (from > to && ECronField::CF_DAY_OF_WEEK != context.FieldType) {
            ythrow yexception() << "Range - specified range start exceeds range end";
        }

        if (from > to && ECronField::CF_DAY_OF_WEEK == context.FieldType) {
            for (; from < 7; from += delta) {
                Target_.SetBit(context.FieldType, from);
            }
            for (from %= 7; from <= to; from += delta) {
                Target_.SetBit(context.FieldType, from);
            }
        }

        for (; from <= to; from += delta) {
            Target_.SetBit(context.FieldType, from);
        }

        if (ECronField::CF_DAY_OF_WEEK == context.FieldType && Target_.GetBit(ECronField::CF_DAY_OF_WEEK, 7)) {

            // Sunday can be represented as 0 or 7
            Target_.SetBit(ECronField::CF_DAY_OF_WEEK, 0);
            Target_.DelBit(ECronField::CF_DAY_OF_WEEK, 7);
        }
        return;
    }

    void Field(TParserContext& context) {
        Segment(context);
        switch (context.Type) {
            case ETokenType::TT_COMMA:
                TokenNext(context);
                Field(context);
                break;
            case ETokenType::TT_WS:
            case ETokenType::TT_EOF:
                break;
            default:
                ythrow yexception() << "FieldRest - error";
        }
        return;
    }

    void FieldWrapper(TParserContext& context, ECronField FieldType, uint32_t min, uint32_t max) {
        context.FieldType = FieldType;
        context.Min = min;
        context.Max = max;

        Field(context);
    }

    void Fields(TParserContext& context, uint32_t len) {
        TokenNext(context);

        if (len < 6) {
            Target_.SetBit(ECronField::CF_SECOND, 0);
        } else {
            FieldWrapper(context, ECronField::CF_SECOND, 0, CRON_MAX_SECONDS);

            if (ETokenType::TT_WS == context.Type) {
                TokenNext(context);
            } else {
                ythrow yexception() << ErrorWS;
            }
        }

        FieldWrapper(context, ECronField::CF_MINUTE, 0, CRON_MAX_MINUTES);
        if (ETokenType::TT_WS == context.Type) {
            TokenNext(context);
        } else {
            ythrow yexception() << ErrorWS;
        }

        FieldWrapper(context, ECronField::CF_HOUR_OF_DAY, 0, CRON_MAX_HOURS);
        if (ETokenType::TT_WS == context.Type) {
            TokenNext(context);
        } else {
            ythrow yexception() << ErrorWS;
        }

        FieldWrapper(context, ECronField::CF_DAY_OF_MONTH, 1, CRON_MAX_DAYS_OF_MONTH + 1);
        if (ETokenType::TT_WS == context.Type) {
            TokenNext(context);
        } else {
            ythrow yexception() << ErrorWS;
        }

        FieldWrapper(context, ECronField::CF_MONTH, 1, CRON_MAX_MONTHS + 1);
        if (ETokenType::TT_WS == context.Type) {
            TokenNext(context);
        } else {
            ythrow yexception() << ErrorWS;
        }

        FieldWrapper(context, ECronField::CF_DAY_OF_WEEK, 0, CRON_MAX_DAYS_OF_WEEK + 1);
        if (len < 7) {
            Target_.SetBit(ECronField::CF_YEAR, 1970 + YEARS_GAP_LENGTH - 1);
        } else {
            if (ETokenType::TT_WS == context.Type) {
                TokenNext(context);
            } else {
                ythrow yexception() << ErrorWS;
            }

            FieldWrapper(context, ECronField::CF_YEAR, CRON_MIN_YEARS, CRON_MAX_YEARS);
        }
        return;
    }

    /////////////////////////////////////////////////

    TMaybe<uint32_t> FindNextPrev(uint32_t max, uint32_t value, uint32_t min, NDatetime::TCivilSecond& calendar, ECronField field, ECronField nextField, TBitMap<7, uint8_t>& lowerOrders, int32_t offset) {
        TMaybe<uint32_t> nextValue = (offset > 0 ? Target_.FindNextSetBit(field, max, value) : Target_.FindPrevSetBit(field, value, min));

        // roll under if needed
        if (nextValue.Empty()) {
            if (nextField == ECronField::CF_NEXT) {
                ythrow yexception() << ErrorDateNotExists;
            }
            if (offset > 0) {
                ResetMax(calendar, field);
            } else {
                ResetMin(calendar, field);
            }
            AddToField(calendar, nextField, offset);

            nextValue = offset > 0 ? Target_.FindNextSetBit(field, max, min) : Target_.FindPrevSetBit(field, max - 1, value);
        }

        if (nextValue.Empty() || nextValue != value) {
            if (offset > 0) {
                ResetAllMin(calendar, lowerOrders);
            } else {
                ResetAllMax(calendar, lowerOrders);
            }
            if (nextValue.Empty()) {
                SetField(calendar, field, 0);
                return 0;
            } else {
                SetField(calendar, field, nextValue.GetRef());
            }

        }
        return nextValue;
    }

    bool FindDayCondition(const NDatetime::TCivilSecond& calendar, int8_t dim, uint32_t dom, uint32_t dow, uint8_t flags, TMaybe<uint32_t> day) {
        if (day.Empty()) {
            if ((!flags && dim < 0) || flags & 1) {
                day = LastDayOfMonth(calendar.month(), calendar.year(), 0);
            } else if (flags & 2) {
                day = LastDayOfMonth(calendar.month(), calendar.year(), 1);
            } else if (flags & 4) {
                Y_ENSURE(dim >= 0);
                day = ClosestWeekday(dim, calendar.month(), calendar.year());
            }
        }

        if (!Target_.GetBit(ECronField::CF_DAY_OF_MONTH, dom) || !Target_.GetBit(ECronField::CF_DAY_OF_WEEK, dow)) {
            return true;
        }

        int64_t dayValue = day.Empty() ? 0 : (day.GetRef() + 1);
        int64_t domValue = static_cast<int64_t>(dom);

        if (flags) {
            if ((flags & 3) && domValue != dayValue + dim) {
                return true;
            }

            if ((flags & 4) && domValue != dayValue - 1) {
                return true;
            }
        } else {
            if (dim < 0 && (domValue < dayValue + static_cast<int64_t>(WEEK_DAYS) * dim || domValue >= dayValue + static_cast<int64_t>(WEEK_DAYS) * (dim + 1))) {
                return true;
            }

            if (dim > 0 && (domValue < static_cast<int64_t>(WEEK_DAYS) * (dim - 1) + 1 || domValue >= static_cast<int64_t>(WEEK_DAYS) * dim + 1)) {
                return true;
            }
        }
        return false;
    }

    TMaybe<int32_t> FindDay(NDatetime::TCivilSecond& calendar, int8_t dim, uint32_t dom, uint32_t dow, uint8_t flags, TBitMap<7, uint8_t>& resets, int32_t offset) {
        TMaybe<uint32_t> day = Nothing();
        uint32_t year = calendar.year();
        uint32_t month = calendar.month();
        uint32_t count = 0;
        uint32_t max = 366;

        while (FindDayCondition(calendar, dim, dom, dow, flags, day) && count++ < max) {
            if (offset > 0) {
                ResetAllMin(calendar, resets);
            } else {
                ResetAllMax(calendar, resets);
            }

            AddToField(calendar, ECronField::CF_DAY_OF_MONTH, offset);

            dom = calendar.day();
            dow = GetWeekday(calendar);

            if (year != calendar.year()) {
                year = calendar.year();
                day = Nothing(); /* This should not be needed unless there is as single day month in libc. */
            }

            if (month != static_cast<uint32_t>(calendar.month())) {
                month = calendar.month();
                day = Nothing();
                Y_ENSURE(day.Empty());
            }
        }
        return dom;
    }

    void DoNextPrev(NDatetime::TCivilSecond& calendar, int32_t offset) {
        uint32_t baseYear = calendar.year();
        uint32_t value = 0;
        TMaybe<uint32_t> updateValue = Nothing();
        TBitMap<7, uint8_t> resets(0);

        for (;;) {
            resets.Clear();

            value = GetField(calendar, ECronField::CF_SECOND);
            updateValue = FindNextPrev(CRON_MAX_SECONDS, value, 0, calendar, ECronField::CF_SECOND, ECronField::CF_MINUTE, resets, offset);

            if (updateValue.Empty()) {
                ythrow yexception() << ErrorDateNotExists;
            }

            if (value == updateValue) {
                resets.Set(static_cast<uint32_t>(ECronField::CF_SECOND));
            } else {
                continue;
            }

            value = GetField(calendar, ECronField::CF_MINUTE);
            updateValue = FindNextPrev(CRON_MAX_MINUTES, value, 0, calendar, ECronField::CF_MINUTE, ECronField::CF_HOUR_OF_DAY, resets, offset);

            if (updateValue.Empty()) {
                ythrow yexception() << ErrorDateNotExists;
            }

            if (value == updateValue) {
                resets.Set(static_cast<uint32_t>(ECronField::CF_MINUTE));
            } else {
                continue;
            }

            value = GetField(calendar, ECronField::CF_HOUR_OF_DAY);
            updateValue = FindNextPrev(CRON_MAX_HOURS, value, 0, calendar, ECronField::CF_HOUR_OF_DAY, ECronField::CF_DAY_OF_MONTH, resets, offset);

            if (updateValue.Empty()) {
                ythrow yexception() << ErrorDateNotExists;
            }

            if (value == updateValue) {
                resets.Set(static_cast<uint32_t>(ECronField::CF_HOUR_OF_DAY));
            } else {
                continue;
            }

            value = GetField(calendar, ECronField::CF_DAY_OF_MONTH);
            updateValue = FindDay(calendar, Target_.GetDayInMonth(), value, GetWeekday(calendar), Target_.GetFlags(), resets, offset);
            if (updateValue.Empty()) {
                ythrow yexception() << ErrorDateNotExists;
            }

            if (value == updateValue) {
                resets.Set(static_cast<uint32_t>(ECronField::CF_DAY_OF_MONTH));
            } else {
                continue;
            }

            value = GetField(calendar, ECronField::CF_MONTH);
            updateValue = FindNextPrev(CRON_MAX_MONTHS, value, 1, calendar, ECronField::CF_MONTH, ECronField::CF_YEAR, resets, offset);

            if (updateValue.Empty()) {
                ythrow yexception() << ErrorDateNotExists;
            }

            if (value != updateValue) {
                if (static_cast<uint32_t>(std::abs(calendar.year() - baseYear)) > CRON_MAX_YEARS_DIFF) {
                    break;
                }
                continue;
            }

            if (Target_.GetBit(ECronField::CF_YEAR, 1970 + YEARS_GAP_LENGTH - 1)) {
                break;
            } else {
                resets.Set(static_cast<uint32_t>(ECronField::CF_MONTH));
            }

            value = GetField(calendar, ECronField::CF_YEAR);
            updateValue = FindNextPrev(CRON_MAX_YEARS, value, CRON_MIN_YEARS, calendar, ECronField::CF_YEAR, ECronField::CF_NEXT, resets, offset);

            if (updateValue.Empty()) {
                ythrow yexception() << ErrorDateNotExists;
            }

            if (value == updateValue) {
                break;
            }
        }
    }

    NDatetime::TCivilSecond Cron(NDatetime::TCivilSecond original, int32_t offset) {
        NDatetime::TCivilSecond calendar;
        calendar = original;

        DoNextPrev(calendar, offset);

        if (calendar == original) {
            /* We arrived at the original timestamp - round up to the next whole second and try again... */
            AddToField(calendar, ECronField::CF_SECOND, offset);
            DoNextPrev(calendar, offset);
        }

        if (
            !Target_.GetBit(ECronField::CF_SECOND, calendar.second()) ||
            !Target_.GetBit(ECronField::CF_MINUTE, calendar.minute()) ||
            !Target_.GetBit(ECronField::CF_HOUR_OF_DAY, calendar.hour()) ||
            !Target_.GetBit(ECronField::CF_DAY_OF_MONTH, calendar.day()) ||
            !Target_.GetBit(ECronField::CF_MONTH, calendar.month()) ||
            !(Target_.GetBit(ECronField::CF_YEAR, 1970 + YEARS_GAP_LENGTH - 1) || Target_.GetBit(ECronField::CF_YEAR, calendar.year())) ||
            !Target_.GetBit(ECronField::CF_DAY_OF_WEEK, GetWeekday(calendar))
        ) {
            ythrow yexception() << ErrorDateNotExists;
        }

        return calendar;
    }

    void CronParseExpr(TStringBuf expression) {
        TParserContext context;
        if (expression.empty()) {
            ythrow yexception() << "Invalid empty expression";
        }

        if ('@' == expression.front()) {
            if (expression == "@annually"sv || expression == "@yearly"sv) {
                expression = "0 0 0 1 1 *"sv;
            } else if (expression == "@monthly"sv) {
                expression = "0 0 0 1 * *"sv;
            } else if (expression == "@weekly"sv) {
                expression = "0 0 0 * * 0"sv;
            } else if (expression == "@daily"sv || expression == "@midnight"sv) {
                expression = "0 0 0 * * *"sv;
            } else if (expression == "@hourly"sv) {
                expression = "0 0 * * * *"sv;
            } else if (expression == "@minutely"sv) {
                expression = "0 * * * * *"sv;
            } else if (expression == "@secondly"sv) {
                expression = "* * * * * * *"sv;
            } else if (expression == "@reboot"sv) {
                ythrow yexception() << "@reboot not implemented";
            } else {
                ythrow yexception() << "Unknown typed cron";
            }
        }
        uint32_t len = StringSplitter(expression).Split(' ').SkipEmpty().Count();
        if (len < 5 || len > 7) {
            ythrow yexception() << Join(" ", "Invalid number of fields, expression must consist of 5-7 fields but has:"sv, len);
        }

        context.input = expression;
        Fields(context, len);
        return;
    }

public:

    NDatetime::TCivilSecond FindCronNext(NDatetime::TCivilSecond date) {
        return Cron(date, +1);
    }

    NDatetime::TCivilSecond FindCronPrev(NDatetime::TCivilSecond date) {
        return Cron(date, -1);
    }

    TImpl(const TStringBuf cronUnparsedExpr) {
        CronParseExpr(cronUnparsedExpr);
    }
};

TCronExpression::TCronExpression(const TStringBuf cronUnparsedExpr)
    : Impl(MakeHolder<TImpl>(cronUnparsedExpr))
{
}

TCronExpression::~TCronExpression() = default;

NDatetime::TCivilSecond TCronExpression::CronNext(NDatetime::TCivilSecond date) {
    return Impl->FindCronNext(date);
}

NDatetime::TCivilSecond TCronExpression::CronPrev(NDatetime::TCivilSecond date) {
    return Impl->FindCronPrev(date);
}
