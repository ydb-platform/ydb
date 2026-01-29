#pragma once

#include <algorithm>
#include <cassert>
#include <cctype>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>


namespace NKikimr::NKqp {

class TParamsMap {
public:
    TParamsMap(std::vector<std::pair<std::string, std::string>> data)
        : Data_(std::move(data))
    {
    }

    void Set(const std::string& key, const std::string& value) {
        auto it = Find(key);
        if (it != Data_.end()) {
            it->second = value;
        } else {
            Data_.emplace_back(key, value);
        }
    }

    bool Has(const std::string& key) const {
        return Find(key) != Data_.end();
    }

    template <typename TValue>
    TValue GetValue(const std::string& key, TValue defaultValue) const {
        auto it = Find(key);
        if (it == Data_.end()) {
            return defaultValue;
        }
        try {
            return Parse<TValue>(it->second);
        } catch (...) {
            return defaultValue;
        }
    }

    template <typename TValue = std::string>
    TValue GetValue(const std::string& key) const {
        auto it = Find(key);
        if (it == Data_.end()) {
            throw std::out_of_range("Key not found: " + key);
        }
        return Parse<TValue>(it->second);
    }

    using TUnderlyingStorage = std::vector<std::pair<std::string, std::string>>;

    TUnderlyingStorage& GetData() {
        return Data_;
    }

    const TUnderlyingStorage& GetData() const {
        return Data_;
    }

private:
    TUnderlyingStorage Data_;

    template<typename T> struct is_chrono_duration : std::false_type {};
    template<typename Rep, typename Period>
    struct is_chrono_duration<std::chrono::duration<Rep, Period>> : std::true_type {};

    TUnderlyingStorage::const_iterator Find(const std::string& key) const {
        return std::find_if(Data_.begin(), Data_.end(),
            [&key](const auto& pair) { return pair.first == key; });
    }

    TUnderlyingStorage::iterator Find(const std::string& key) {
        return std::find_if(Data_.begin(), Data_.end(),
            [&key](const auto& pair) { return pair.first == key; });
    }

    template <typename TValue>
    static TValue ParseDuration(const std::string& s) {
        using namespace std::chrono;
        duration<double, std::nano> totalNs(0);

        const char* ptr = s.c_str();
        char* endptr = nullptr;

        while (*ptr) {
            // 1. Skip whitespace
            while (*ptr && std::isspace(static_cast<unsigned char>(*ptr))) {
                ptr ++;
            }

            if (!*ptr) {
                break;
            }

            // 2. Parse Number (handles 100, 1.5, .5)
            double val = std::strtod(ptr, &endptr);

            if (ptr == endptr) {
                // We encountered text without a number (e.g., just "ms"?) or garbage
                throw std::invalid_argument("Invalid duration format in: " + s);
            }
            ptr = endptr;

            // 3. Skip whitespace between number and valid suffix (e.g. "1 h")
            while (*ptr && std::isspace(static_cast<unsigned char>(*ptr))) {
                ptr ++;
            }

            // 4. Parse suffix (ns, ms, s, etc...)
            std::string suffix;
            while (*ptr && std::isalpha(static_cast<unsigned char>(*ptr))) {
                suffix += *ptr;
                ptr ++;
            }

            duration<double, std::nano> segmentNs;
            if (suffix == "ns") {
                segmentNs = duration<double, std::nano>(val);
            }
            else if (suffix == "us") {
                segmentNs = duration<double, std::micro>(val);
            }
            else if (suffix == "ms") {
                segmentNs = duration<double, std::milli>(val);
            }
            else if (suffix == "s") {
                segmentNs = duration<double>(val);
            }
            else if (suffix == "m") {
                segmentNs = duration<double, std::ratio<60>>(val);
            }
            else if (suffix == "h") {
                segmentNs = duration<double, std::ratio<3600>>(val);
            }
            else if (suffix == "d") {
                segmentNs = duration<double, std::ratio<86400>>(val);
            }
            else if (suffix.empty()) {
                segmentNs = duration<double>(val);
            }
            else {
                throw std::invalid_argument("Unknown suffix: " + suffix);
            }

            totalNs += segmentNs;
        }

        return duration_cast<TValue>(totalNs);
    }

    template <typename TValue>
    static TValue Parse(const std::string& value) {
        if constexpr (std::is_same_v<TValue, std::string>) {
            return value;
        }
        else if constexpr (std::is_same_v<TValue, bool>) {
            return value == "true" || value == "1" || value == "on";
        }
        else if constexpr (std::is_integral_v<TValue>) {
            if constexpr (std::is_unsigned_v<TValue>) {
                return static_cast<TValue>(std::stoull(value));
            }
            else {
                return static_cast<TValue>(std::stoll(value));
            }
        }
        else if constexpr (std::is_floating_point_v<TValue>) {
            return static_cast<TValue>(std::stod(value));
        }
        else if constexpr (is_chrono_duration<TValue>::value) {
            return ParseDuration<TValue>(value);
        }
        else {
            return TValue(value);
        }
    }
};

class TTupleParser {
public:
    using TTable = std::vector<TParamsMap>;

    TTupleParser(std::string input)
        : Input_(std::move(input))
        , Pos_(0)
    {
    }

    // TopLevel = Expression
    TTable Parse() {
        TTable table = ParseExpression();
        TTable updated;

        // Finalize: Strip leading '+' checks that were used for merge logic
        uint32_t idx = 0;
        for (auto& row : table) {
            for (auto& [key, value] : row.GetData()) {
                if (!value.empty() && (value[0] == '+' || value[0] == '?')) {
                    value = value.substr(1);
                }
            }


            updated.push_back(MergeRows(TParamsMap::TUnderlyingStorage{{"idx", std::to_string(idx++)}}, row));
        }

        return updated;
    }

private:
    std::string Input_;
    uint64_t Pos_;

    using TOptionalParser = std::optional<TTable> (TTupleParser::*)();
    using TParser = TTable (TTupleParser::*)();
    using TOperation = TTable (TTupleParser::*)(const TTable& a, const TTable& b);

    // Expression = Term { '+' Term }
    TTable ParseExpression() {
        return ParseMany(&TTupleParser::ParseTerm, &TTupleParser::OpSum, '+');
    }

    // Term = Factor { '*' Factor }
    TTable ParseTerm() {
        return ParseMany(&TTupleParser::ParseFactor, &TTupleParser::OpProduct, '*');
    }

    // Factor = Parens | Tuple
    TTable ParseFactor() {
        return ParseOr(&TTupleParser::ParseParens, &TTupleParser::ParseTuple);
    }

    // Factor = '(' Expression ')'
    std::optional<TTable> ParseParens() {
        return ParseInside(&TTupleParser::ParseExpression, '(', ')');
    }

    // Tuple = KeyDef { ';' KeyDef }
    TTable ParseTuple() {
        return ParseMany(&TTupleParser::ParseKeyDef, &TTupleParser::OpProduct, ';');
    }

    // KeyDef = Identifier '=' ValueList
    TTable ParseKeyDef() {
        std::string key = ParseIdentifier();
        Ensure('=');

        std::vector<std::string> values = ParseValueList();

        TTable result;
        result.reserve(values.size());

        for (const auto& value : values) {
            result.push_back(TParamsMap::TUnderlyingStorage{{key, value}});
        }
        return result;
    }

    // ValueList = ValueEntry { ',' ValueEntry }
    std::vector<std::string> ParseValueList() {
        std::string raw = ParseString([](char symbol) {
            return symbol != ';' && symbol != ')';
        });

        if (raw.empty()) {
            throw std::runtime_error("Empty value at " + std::to_string(Pos_));
        }

        // Check if this is a range expression
        size_t dotdot = raw.find("..");
        if (dotdot != std::string::npos) {
            return ExpandRange(raw);
        }

        return Split(raw, ",");
    }

    std::string ParseIdentifier() {
        return ParseString([](char symbol) {
            return std::isalnum(symbol) || symbol == '_' || symbol == '-';
        });
    }

    std::vector<std::string> ExpandRange(const std::string& input) {
        try {
            auto parts = Split(input, ",");

            if (parts.empty()) {
                throw std::runtime_error("Empty range expression");
            }

            bool isFloat = ShouldFormatAsFloat(parts);
            auto range = Split(parts[parts.size() - 1], "..");

            if (parts.size() <= 2 || range.size() != 2) {
                double first = std::stod(parts[0]);
                double start = parts.size() == 2 ? std::stod(range[0]) : first;
                double end = std::stod(range[range.size() - 1]);
                double step = first != start ? (start - first) : (start <= end) ? 1.0 : -1.0;

                return GenerateSequence(std::stod(parts[0]), std::stod(range[range.size() - 1]), step, isFloat);
            }

            throw std::runtime_error(
                "Invalid range format: only 'X..Y' or 'X,Y..Z' allowed, got " +
                std::to_string(parts.size()) + " parts"
            );

        } catch (const std::exception& e) {
            throw std::runtime_error("Invalid range format: " + input + " (" + e.what() + ")");
        }
    }

    static bool ShouldFormatAsFloat(const std::vector<std::string>& parts) {
        for (const auto& part : parts) {
            // Count dots, excluding the ".." operator
            size_t dotPos = part.find('.');
            size_t dotDotPos = part.find("..");

            // Has a decimal point that's not part of ".."
            if (dotPos != std::string::npos &&
                (dotDotPos == std::string::npos || dotPos != dotDotPos)) {
                return true;
            }
        }
        return false;
    }

    static std::vector<std::string> GenerateSequence(double start, double end, double step, bool isFloat) {
        std::vector<std::string> result;

        if (std::abs(step) < 1e-9) {
            throw std::runtime_error("Step size cannot be zero");
        }

        if (std::abs((end - start) / step) > 100000) {
            throw std::runtime_error("Range too large");
        }

        double current = start;
        double epsilon = 1e-9 * std::abs(step);

        while ((step > 0 && current <= end + epsilon) ||
                (step < 0 && current >= end - epsilon)) {

            result.push_back(FormatNumber(current, isFloat));
            current += step;
        }

        return result;
    }

    static std::string FormatNumber(double value, bool isFloat) {
        if (isFloat) {
            std::string s = std::to_string(value);
            // Trim trailing zeroes
            s.erase(s.find_last_not_of('0') + 1, std::string::npos);
            if (!s.empty() && s.back() == '.') s.pop_back();
            return s;
        } else {
            return std::to_string((long long)std::round(value));
        }
    }

    // =========================== Table Operations ===========================

    TTable OpSum(const TTable& a, const TTable& b) {
        TTable res = a;
        res.insert(res.end(), b.begin(), b.end());
        return res;
    }

    TTable OpProduct(const TTable& a, const TTable& b) {
        if (a.empty()) return b;
        if (b.empty()) return a;

        TTable res;
        res.reserve(a.size() * b.size());

        for (const auto& rowA : a) {
            for (const auto& rowB : b) {
                res.push_back(MergeRows(rowA, rowB));
            }
        }
        return res;
    }

    static TParamsMap MergeRows(TParamsMap dest, const TParamsMap& src) {
        for (const auto& [key, value] : src.GetData()) {
            auto it = std::find_if(
                dest.GetData().begin(), dest.GetData().end(),
                [&key](const auto& element) {
                    return element.first == key;
                }
            );

            if (it == dest.GetData().end()) {
                dest.GetData().push_back({key, value});
                continue;
            }

            // Handle collision
            if (!value.empty() && value[0] == '+') {
                // Value begins with '+' --- it should be appended
                // in case of collision instead of the default overwrite
                if (!it->second.empty()) {
                    it->second += "," + value.substr(1);
                } else {
                    it->second = value.substr(1);
                }
            } else if (it->second.empty() && value[0] == '?') {
                it->second = value.substr(1);
            } else if (value.empty() || value[0] != '?') {
                it->second = value;
            }
        }
        return dest;
    }

    // =========================== Parsing Primitives =========================

    TTable ParseMany(TParser parser, TOperation operation, char delimiter) {
        TTable left = (this->*parser)();
        while (Match(delimiter)) {
            TTable right = (this->*parser)();
            left = (this->*operation)(left, std::move(right));
        }
        return left;

    }

    TTable ParseOr(TOptionalParser choice0, TParser choice1) {
        std::optional<TTable> parsed0 = (this->*choice0)();
        if (parsed0) {
            return *parsed0;
        }

        return (this->*choice1)();
    }

    std::optional<TTable> ParseInside(TParser parser, char lhs, char rhs) {
        uint64_t start = Pos_;
        if (!Match(lhs)) {
            Pos_ = start;
            return std::nullopt;
        }

        TTable value = (this->*parser)();

        // If lhs matches, then rhs is required, this depends on grammar, but
        // it greatly improves errors if grammar is appropriate (like in this case)
        Ensure(rhs);

        return value;
    }

    template <typename TCondition>
    std::string ParseString(TCondition condition) {
        SkipSpaces();

        size_t start = Pos_;
        while (Pos_ < Input_.size() && condition(Input_[Pos_])) {
            ++ Pos_;
        }

        if (start == Pos_) {
            throw std::runtime_error("Expected string at " + std::to_string(Pos_));
        }

        std::string value = Input_.substr(start, Pos_ - start);
        Trim(value);

        return value;
    }

    // =========================== Parsing Primitives =========================

    char Peek() {
        if (Pos_ >= Input_.size()) {
            return 0;
        }

        return Input_[Pos_];
    }

    void Consume() {
        if (Pos_ < Input_.size()) {
            ++ Pos_;
        }
    }

    bool Match(char symbol) {
        SkipSpaces();
        if (Peek() == symbol) {
            Consume();
            return true;
        }
        return false;
    }

    void Ensure(char c) {
        if (!Match(c)) {
            throw std::runtime_error("Expected: '" + std::to_string(c) + "'");
        }
    }

    // =========================== String Processing ==========================

    std::vector<std::string> Split(const std::string& input, const std::string& delimiter) {
        std::vector<std::string> tokens;
        size_t start = 0;
        size_t end = 0;

        auto addToken = [&](std::string token) {
            Trim(token);
            if (!token.empty()) {
                tokens.push_back(token);
            }
        };

        while ((end = input.find(delimiter, start)) != std::string::npos) {
            std::string token = input.substr(start, end - start);
            addToken(token);
            start = end + delimiter.length();
        }

        // Last token
        addToken(input.substr(start));
        return tokens;
    }

    void SkipSpaces() {
        while (Pos_ < Input_.size()) {
            if (std::isspace(Input_[Pos_])) {
                ++ Pos_;
            } else if (Pos_ + 1 < Input_.size() && Input_[Pos_] == '/' && Input_[Pos_ + 1] == '/') {
                // Skip comment: advance to end of line
                Pos_ += 2;
                while (Pos_ < Input_.size() && Input_[Pos_] != '\n') {
                    ++ Pos_;
                }
                // The '\n' will be consumed by the whitespace check on next iteration
            } else {
                break;
            }
        }
    }

    static void Trim(std::string& s) {
        s.erase(s.begin(), std::find_if(s.begin(), s.end(), [](unsigned char ch) { return !std::isspace(ch); }));
        s.erase(std::find_if(s.rbegin(), s.rend(), [](unsigned char ch) { return !std::isspace(ch); }).base(), s.end());
    }

};

void PrintTable(const NKikimr::NKqp::TTupleParser::TTable& table) {
    if (table.empty()) {
        std::cout << "Empty Table" << std::endl;
        return;
    }

    std::vector<std::string> headers;
    std::set<std::string> seenHeaders;

    for (const auto& row : table) {
        for (const auto& [key, value] : row.GetData()) {
            if (seenHeaders.insert(key).second) {
                headers.push_back(key);
            }
        }
    }

    // Calculate widths of each column
    std::map<std::string, size_t> columnWidths;
    for (const auto& header : headers) {
        columnWidths[header] = header.length();
    }

    for (const auto& row : table) {
        for (const auto& [key, value] : row.GetData()) {
            columnWidths[key] = std::max(columnWidths[key], value.length());
        }
    }

    for (auto& [key, width] : columnWidths) {
        width += 2; // space from both sides
    }

    size_t totalWidth = 1;
    for (const auto& header : headers) {
        totalWidth += columnWidths[header] + 3;
    }

    // Print headers
    std::cout << "|";
    for (const auto& h : headers) {
        std::cout << " " << std::setw(columnWidths[h]) << std::left << h << " |";
    }
    std::cout << "\n";
    std::cout << std::string(totalWidth, '-') << "\n";

    // Print rows
    for (const auto& row : table) {
        std::cout << "|";
        for (const auto& header : headers) {
            // Find value for this header
            std::string displayedValue;
            for (const auto& [key, value] : row.GetData()) {
                if (key == header) {
                    displayedValue = value;
                    break;
                }
            }
            std::cout << " " << std::setw(columnWidths[header]) << std::left
                      << displayedValue << " |";
        }
        std::cout << "\n";
    }
    std::cout << "\n";
}

} // namespace NKikimr::NKqp
