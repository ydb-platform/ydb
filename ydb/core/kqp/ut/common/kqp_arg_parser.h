#pragma once

#include <cassert>
#include <chrono>
#include <cstdint>
#include <map>
#include <sstream>
#include <string>
#include <type_traits>
#include <regex>


namespace NKikimr::NKqp {


class TArgs {
public:
    template <typename TValue>
    class TRangedValueIter {
    public:
        TRangedValueIter(TValue current, TValue end, TValue step)
            : Current_(current)
            , End_(end)
            , Step_(step)
        {
        }

        TValue operator*() const {
            return Current_;
        }

        TRangedValueIter& operator++() {
            Current_ += Step_;
            if (Current_ >= End_) {
                Current_ = End_;
            }

            return *this;
        }

        bool operator!=(TRangedValueIter other) const {
            assert(Step_ == other.Step_);
            return Current_ != other.Current_;
        }

    private:
        TValue Current_;
        TValue End_;
        TValue Step_;
    };

    template <typename TValue>
    class TRangedValue {
    public:
        TRangedValue(TValue from, TValue to, TValue step)
            : IsRange_(true)
            , From_(from)
            , To_(to)
            , Step_(step)
        {
        }

        TRangedValue(TValue from)
            : IsRange_(false)
            , From_(from)
            , To_(from)
            , Step_(1)
        {
        }

        bool IsRange() const {
            return IsRange_;
        }

        TRangedValueIter<TValue> end() const {
            TValue End = To_ + 1; // immediately after the last
            return TRangedValueIter<TValue>{End, End, Step_};
        }

        TRangedValueIter<TValue> begin() const {
            return TRangedValueIter{From_, *end(), Step_};
        }

        TValue GetValue() const {
            return From_;
        }

        TValue GetFirst() const {
            return From_;
        }

        TValue GetLast() const {
            return To_;
        }

        TValue GetStep() const {
            return Step_;
        }

    private:
        bool IsRange_;

        TValue From_;
        TValue To_;
        TValue Step_;
    };

public:
    TArgs(std::string input)
        : Values_(ParseMap(input))
    {
    }

    template <typename TValue>
    auto GetArg(std::string key) {
        return ParseRangedValue<TValue>(Values_[key]);
    }

    bool HasArg(std::string key) {
        return Values_.contains(key);
    }


private:
    std::map<std::string, std::string> Values_;

private:
    static void LTrim(std::string &input) {
        input.erase(input.begin(), std::find_if(input.begin(), input.end(), [](unsigned char ch) {
            return !std::isspace(ch);
        }));
    }

    static void RTrim(std::string &input) {
        input.erase(std::find_if(input.rbegin(), input.rend(), [](unsigned char ch) {
            return !std::isspace(ch);
        }).base(), input.end());
    }

    static void Trim(std::string &input) {
        LTrim(input);
        RTrim(input);
    }

    static std::map<std::string, std::string> ParseMap(const std::string& input, char delimiter = ';') {
        std::map<std::string, std::string> result;
        std::stringstream ss(input);

        std::string entry;
        while (std::getline(ss, entry, delimiter)) {
            // each entry looks like key value pair, e.g. "N=5"
            Trim(entry);
            size_t pos = entry.find('=');

            if (pos != std::string::npos) {
                std::string key = entry.substr(0, pos);
                std::string value = entry.substr(pos + 1);
                Trim(value);
                result[std::move(key)] = std::move(value);
            }
        }

        return result;
    }

    template <typename TValue>
    static auto ParseRangedValue(const std::string& input) {
        // Check if it contains ".."
        size_t dotdot = input.find("..");

        if (dotdot == std::string::npos) {
            // parse fixed value
            auto value = ParseValue<TValue>(input);
            return TRangedValue<decltype(value)>{value};
        } else {
            // parse ranged (with step or without)
            size_t comma = input.find(',');

            auto to = ParseValue<TValue>(input.substr(dotdot + 2));
            if (comma != std::string::npos && comma < dotdot) {
                // parse ranges like "0.1,0.2..1.0"
                auto first = ParseValue<TValue>(input.substr(0, comma));
                auto second = ParseValue<TValue>(input.substr(comma + 1, dotdot - comma - 1));
                auto step = second - first;
                return TRangedValue<decltype(first)>{first, to, step};
            }

            // parse ranges like "1..100"
            auto first = ParseValue<TValue>(input.substr(0, dotdot));
            return TRangedValue<decltype(first)>{first, to, /*default step=*/1};
        }
    }

    template <typename TValue>
    static auto ParseValue(const std::string& input) {
        if constexpr (std::is_same_v<TValue, double>) {
            return std::stod(input);
        } else if constexpr (std::is_same_v<TValue, uint64_t>) {
            return static_cast<uint64_t>(std::stoull(input));
        } else if constexpr (std::is_same_v<TValue, int64_t>) {
            return static_cast<int64_t>(std::stoll(input));
        } else if constexpr (std::is_same_v<TValue, std::string>) {
            return input;
        } else if constexpr (std::is_same_v<TValue, std::chrono::nanoseconds>) {
            return static_cast<ui64>(ParseDuration(input).count());
        } else {
            static_assert(false, "Unhandled type");
        }
    }

    static std::chrono::nanoseconds ParseDuration(const std::string& input) {
        std::regex pattern(R"((\d+(?:\.\d+)?)\s*(ns|us|ms|s|m|h))");
        std::smatch match;

        if (!std::regex_match(input, match, pattern)) {
            throw std::invalid_argument("Invalid duration format");
        }

        double value = std::stod(match[1]);
        std::string unit = match[2];

        if (unit == "ns") return std::chrono::nanoseconds(static_cast<uint64_t>(value));
        if (unit == "us") return std::chrono::microseconds(static_cast<uint64_t>(value));
        if (unit == "ms") return std::chrono::milliseconds(static_cast<uint64_t>(value));
        if (unit == "s")  return std::chrono::seconds(static_cast<uint64_t>(value));
        if (unit == "m")  return std::chrono::minutes(static_cast<uint64_t>(value));
        if (unit == "h")  return std::chrono::hours(static_cast<uint64_t>(value));

        throw std::invalid_argument("Unknown unit");
    }

};


}
