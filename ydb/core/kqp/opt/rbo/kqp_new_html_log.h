#pragma once

#include <cassert>
#include <fstream>
#include <string>
#include <vector>

namespace NKikimr {
namespace NKqp {

struct PlanNode {
    std::string label;
    std::vector<std::pair<std::string, std::string>> metadata;
    std::vector<std::string> stats;
    std::vector<PlanNode> children;

    PlanNode() = default;
    explicit PlanNode(const std::string& lbl) : label(lbl) {}

    PlanNode& addMeta(const std::string& key, const std::string& value) {
        metadata.emplace_back(key, value);
        return *this;
    }

    PlanNode& setStat(size_t col, const std::string& value) {
        if (stats.size() <= col) stats.resize(col + 1);
        stats[col] = value;
        return *this;
    }

    PlanNode& addChild(const std::string& lbl) {
        children.emplace_back(lbl);
        return children.back();
    }

    PlanNode& addChild(const PlanNode& node) {
        children.push_back(node);
        return children.back();
    }

    bool hasAnyMeta() const {
        if (!metadata.empty()) return true;
        for (const auto& c : children) if (c.hasAnyMeta()) return true;
        return false;
    }

    bool hasAnyStats() const {
        for (const auto& s : stats) if (!s.empty()) return true;
        for (const auto& c : children) if (c.hasAnyStats()) return true;
        return false;
    }
};

struct InfoSection {
    enum Type { TABLE, HTML, TEXT };
    Type type = TEXT;
    std::string title;
    std::vector<std::pair<std::string, std::string>> tableRows;
    std::string content;

    static InfoSection makeTable(const std::string& t,
                                 const std::vector<std::pair<std::string, std::string>>& rows) {
        InfoSection s; s.type = TABLE; s.title = t; s.tableRows = rows; return s;
    }
    static InfoSection makeHTML(const std::string& t, const std::string& html) {
        InfoSection s; s.type = HTML; s.title = t; s.content = html; return s;
    }
    static InfoSection makeText(const std::string& t, const std::string& text) {
        InfoSection s; s.type = TEXT; s.title = t; s.content = text; return s;
    }
};

class OptimizerTraceHTML {
public:
    struct RuleApplication {
        std::string ruleName;
        PlanNode tree;
        std::vector<InfoSection> info;
    };

    struct Stage {
        std::string name;
        std::vector<RuleApplication> rules;
    };

    void setStatColumns(const std::vector<std::string>& cols) { statColumns_ = cols; }
    void addStage(const std::string& name) { stages_.push_back({name, {}}); }

    RuleApplication& addRule(const std::string& ruleName, const PlanNode& tree) {
        assert(!stages_.empty());
        stages_.back().rules.push_back({ruleName, tree, {}});
        return stages_.back().rules.back();
    }

    void addInfoToLastRule(const InfoSection& section) {
        assert(!stages_.empty() && !stages_.back().rules.empty());
        stages_.back().rules.back().info.push_back(section);
    }

    bool generateHTML(const std::string& filename) const;

private:
    std::vector<Stage> stages_;
    std::vector<std::string> statColumns_;

    static std::string esc(const std::string& s);
    static std::string jsEsc(const std::string& s);
    void emitTreeJSON(std::ofstream& out, const PlanNode& node) const;
    void emitInfoJSON(std::ofstream& out, const std::vector<InfoSection>& sections) const;
    void emitTitleBar(std::ofstream& out, size_t si, size_t gi, size_t ri,
                      size_t ruleNum, const std::string& name,
                      bool hasMeta, bool hasStats, bool hasInfo,
                      bool isFirst, bool isLast) const;
};

} // namespace NKqp
} // namespace NKikimr
