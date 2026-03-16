/*
 * (C) Copyright 2005- ECMWF.
 *
 * This software is licensed under the terms of the Apache Licence Version 2.0
 * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
 * virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
 */

#pragma once

#include <chrono>
#include <string>
#include <vector>
#include <stdexcept>
#include <array>
#include <unordered_map>
#include <algorithm>

namespace eccodes {

template <typename T> using Minutes   = std::chrono::duration<T, std::ratio<60>>;
template <typename T> using Hours     = std::chrono::duration<T, std::ratio<3600>>;
template <typename T> using Days      = std::chrono::duration<T, std::ratio<86400>>;
template <typename T> using Months    = std::chrono::duration<T, std::ratio<2592000>>;
template <typename T> using Years     = std::chrono::duration<T, std::ratio<31536000>>;
template <typename T> using Years10   = std::chrono::duration<T, std::ratio<315360000>>;
template <typename T> using Years30   = std::chrono::duration<T, std::ratio<946080000>>;
template <typename T> using Centuries = std::chrono::duration<T, std::ratio<3153600000>>;
template <typename T> using Hours3    = std::chrono::duration<T, std::ratio<10800>>;
template <typename T> using Hours6    = std::chrono::duration<T, std::ratio<21600>>;
template <typename T> using Hours12   = std::chrono::duration<T, std::ratio<43200>>;
template <typename T> using Seconds   = std::chrono::duration<T, std::ratio<1>>;
template <typename T> using Minutes15 = std::chrono::duration<T, std::ratio<900>>;
template <typename T> using Minutes30 = std::chrono::duration<T, std::ratio<1800>>;
template <typename T> using Missing   = std::chrono::duration<T, std::ratio<0>>;



class Unit;
template <typename T> Seconds<T> to_seconds(long value, const Unit& unit);
template <typename T> T from_seconds(Seconds<T> seconds, const Unit& unit);

class Unit {
public:
    enum class Value {
        MINUTE = 0,
        HOUR = 1,
        DAY = 2,
        MONTH = 3,
        YEAR = 4,
        YEARS10 = 5,
        YEARS30 = 6,
        CENTURY = 7,
        HOURS3 = 10,
        HOURS6 = 11,
        HOURS12 = 12,
        SECOND = 13,
        MINUTES15 = 14,
        MINUTES30 = 15,
        MISSING = 255,
    };

    Unit() : internal_value_(Value::HOUR) {}

    explicit Unit(Value unit_value) : internal_value_(unit_value) {}

    explicit Unit(const std::string& unit_value) {
        try {
            internal_value_ = get_converter().name_to_unit(unit_value);
        } catch (std::exception& e) {
            throw std::runtime_error(std::string{"Unit not found "} + e.what());
        }
    }

    explicit Unit(long unit_value) {
        try {
            internal_value_ = get_converter().long_to_unit(unit_value);
        } catch (std::exception& e) {
            throw std::runtime_error(std::string{"Unit not found "} + e.what());
        }
    }

    bool operator>(const Unit& other) const {return get_converter().unit_to_duration(internal_value_) > get_converter().unit_to_duration(other.internal_value_);}
    bool operator==(const Value value) const {return get_converter().unit_to_duration(internal_value_) == get_converter().unit_to_duration(value);}
    bool operator==(const Unit& unit) const {return get_converter().unit_to_duration(internal_value_) == get_converter().unit_to_duration(unit.internal_value_);}
    bool operator!=(const Unit& unit) const {return !(*this == unit);}
    bool operator!=(const Value value) const {return !(*this == value);}

    Unit& operator=(const Value value) {
        internal_value_ = value;
        return *this;
    }

    template <typename T> T value() const;
    static std::vector<Value> grib_selected_units;
    static std::vector<Value> complete_unit_order_;

    static std::vector<Unit> list_supported_units() {
        std::vector<Unit> result;
        result.reserve(32);
        for (const auto& val : complete_unit_order_) {
            if (val == Value::MISSING)
                continue;
            result.push_back(Unit(val));
        }

        return result;
    }

private:
    class Map {
    public:
        Map() {
            for (const auto& entry : tab_) {
                // unit_value <-> unit_name
                name_to_value_[entry.unit_name] = entry.unit_value;
                value_to_name_[entry.unit_value] = entry.unit_name;

                // unit_value <-> duration in seconds
                value_to_duration_[entry.unit_value] = entry.duration;
                duration_to_value_[entry.duration] = entry.unit_value;

                // unit_value <-> wmo_code
                value_to_long_[entry.unit_value] = static_cast<long>(entry.unit_value);
                long_to_value_[static_cast<long>(entry.unit_value)] = entry.unit_value;
            }
        }

        // wmo_code <-> unit_name
        std::string unit_to_name(const Value& unit_value) const {return value_to_name_.at(unit_value);}
        Value name_to_unit(const std::string& name) const {return name_to_value_.at(name);}

        // unit_value <-> duration
        uint64_t unit_to_duration(const Value& unit_value) const {return value_to_duration_.at(unit_value);}
        Value duration_to_unit(long duration) const {return duration_to_value_.at(duration);}

        // wmo_code <-> unit_name
        long unit_to_long(const Value& unit_value) const {return value_to_long_.at(unit_value);}
        Value long_to_unit(long wmo_code) const {return long_to_value_.at(wmo_code);}

    private:
        struct Entry {
            Value unit_value;
            std::string unit_name;
            uint64_t duration;
        };

        const std::array<Entry, 15> tab_ = {{
            Entry{Value::MISSING   , "MISSING" , 0},
            Entry{Value::SECOND    , "s"       , 1},
            Entry{Value::MINUTE    , "m"       , 60},
            Entry{Value::MINUTES15 , "15m"     , 900},
            Entry{Value::MINUTES30 , "30m"     , 1800},
            Entry{Value::HOUR      , "h"       , 3600},
            Entry{Value::HOURS3    , "3h"      , 10800},
            Entry{Value::HOURS6    , "6h"      , 21600},
            Entry{Value::HOURS12   , "12h"     , 43200},
            Entry{Value::DAY       , "D"       , 86400},
            Entry{Value::MONTH     , "M"       , 2592000},
            Entry{Value::YEAR      , "Y"       , 31536000},
            Entry{Value::YEARS10   , "10Y"     , 315360000},
            Entry{Value::YEARS30   , "30Y"     , 946080000},
            Entry{Value::CENTURY   , "C"       , 3153600000},
        }};

        std::unordered_map<std::string, Value> name_to_value_;
        std::unordered_map<Value, std::string> value_to_name_;

        std::unordered_map<Value, long> value_to_long_;
        std::unordered_map<long, Value> long_to_value_;

        std::unordered_map<Value, uint64_t> value_to_duration_;
        std::unordered_map<uint64_t, Value> duration_to_value_;
    };


    Value internal_value_;
public:
    static Map& get_converter() {
        static Map map_;
        return map_;
    }
};


template <typename T>
Seconds<T> to_seconds(long value, const Unit& unit) {
    Seconds<T> seconds;
    switch (unit.value<Unit::Value>()) {
        case Unit::Value::SECOND: seconds = Seconds<T>(value); break;
        case Unit::Value::MINUTE: seconds = Minutes<T>(value); break;
        case Unit::Value::MINUTES15: seconds = Minutes15<T>(value); break;
        case Unit::Value::MINUTES30: seconds = Minutes30<T>(value); break;
        case Unit::Value::HOUR: seconds = Hours<T>(value); break;
        case Unit::Value::HOURS3: seconds = Hours3<T>(value); break;
        case Unit::Value::HOURS6: seconds = Hours6<T>(value); break;
        case Unit::Value::HOURS12: seconds = Hours12<T>(value); break;
        case Unit::Value::DAY: seconds = Days<T>(value); break;
        case Unit::Value::MONTH: seconds = Months<T>(value); break;
        case Unit::Value::YEAR: seconds = Years<T>(value); break;
        case Unit::Value::YEARS10: seconds = Years10<T>(value); break;
        case Unit::Value::YEARS30: seconds = Years30<T>(value); break;
        case Unit::Value::CENTURY: seconds = Centuries<T>(value); break;
        default:
            std::string msg = "Unknown unit: " + unit.value<std::string>();
            throw std::runtime_error(msg);
    }
    return seconds;
}


template <typename T>
T from_seconds(Seconds<T> seconds, const Unit& unit) {
    T value;
    switch (unit.value<Unit::Value>()) {
        case Unit::Value::SECOND: value = std::chrono::duration_cast<Seconds<T>>(seconds).count(); break;
        case Unit::Value::MINUTE: value = std::chrono::duration_cast<Minutes<T>>(seconds).count(); break;
        case Unit::Value::MINUTES15: value = std::chrono::duration_cast<Minutes15<T>>(seconds).count(); break;
        case Unit::Value::MINUTES30: value = std::chrono::duration_cast<Minutes30<T>>(seconds).count(); break;
        case Unit::Value::HOUR: value = std::chrono::duration_cast<Hours<T>>(seconds).count(); break;
        case Unit::Value::HOURS3: value = std::chrono::duration_cast<Hours3<T>>(seconds).count(); break;
        case Unit::Value::HOURS6: value = std::chrono::duration_cast<Hours6<T>>(seconds).count(); break;
        case Unit::Value::HOURS12: value = std::chrono::duration_cast<Hours12<T>>(seconds).count(); break;
        case Unit::Value::DAY: value = std::chrono::duration_cast<Days<T>>(seconds).count(); break;
        case Unit::Value::MONTH: value = std::chrono::duration_cast<Months<T>>(seconds).count(); break;
        case Unit::Value::YEAR: value = std::chrono::duration_cast<Years<T>>(seconds).count(); break;
        case Unit::Value::YEARS10: value = std::chrono::duration_cast<Years10<T>>(seconds).count(); break;
        case Unit::Value::YEARS30: value = std::chrono::duration_cast<Years30<T>>(seconds).count(); break;
        case Unit::Value::CENTURY: value = std::chrono::duration_cast<Centuries<T>>(seconds).count(); break;
        default:
            std::string msg = "Unknown unit: " + unit.value<std::string>();
            throw std::runtime_error(msg);
    }
    return value;
}

} // namespace eccodes
