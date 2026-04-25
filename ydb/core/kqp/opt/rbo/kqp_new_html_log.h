#ifndef OPTIMIZER_TRACE_HTML_H
#define OPTIMIZER_TRACE_HTML_H

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <cassert>

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
        for (const auto& c : children)
            if (c.hasAnyMeta()) return true;
        return false;
    }

    bool hasAnyStats() const {
        for (const auto& s : stats)
            if (!s.empty()) return true;
        for (const auto& c : children)
            if (c.hasAnyStats()) return true;
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
        InfoSection s;
        s.type = TABLE;
        s.title = t;
        s.tableRows = rows;
        return s;
    }
    static InfoSection makeHTML(const std::string& t, const std::string& html) {
        InfoSection s;
        s.type = HTML;
        s.title = t;
        s.content = html;
        return s;
    }
    static InfoSection makeText(const std::string& t, const std::string& text) {
        InfoSection s;
        s.type = TEXT;
        s.title = t;
        s.content = text;
        return s;
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

    bool generateHTML(const std::string& filename) const {
        std::ofstream out(filename);
        if (!out.is_open()) return false;

        struct GroupInfo {
            std::string name;
            std::vector<size_t> ruleIndices;
        };
        std::vector<std::vector<GroupInfo>> stageGroups;
        for (const auto& stage : stages_) {
            std::vector<GroupInfo> groups;
            for (size_t j = 0; j < stage.rules.size(); ) {
                GroupInfo g;
                g.name = stage.rules[j].ruleName;
                g.ruleIndices.push_back(j);
                size_t k = j + 1;
                while (k < stage.rules.size() && stage.rules[k].ruleName == g.name) {
                    g.ruleIndices.push_back(k);
                    k++;
                }
                groups.push_back(g);
                j = k;
            }
            stageGroups.push_back(groups);
        }

        /* ═══════════ HTML head + CSS ═══════════ */
        out << R"RAWCSS(<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>Optimizer Trace</title>
<style>

* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100vh; overflow: hidden; }

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, sans-serif;
    font-size: 14px;
    background: #e8eaef;
    color: #1c1e24;
    display: flex;
    flex-direction: column;
}

/* ─── Top bar ─── */
.top-bar {
    flex-shrink: 0;
    padding: 10px 16px 8px 16px;
    background: #e8eaef;
}

h1 {
    font-size: 17px;
    font-weight: 600;
    display: inline-block;
    margin-right: 14px;
}

.controls {
    display: flex;
    align-items: center;
    gap: 6px 12px;
    flex-wrap: nowrap;
}

.controls label {
    font-size: 12px;
    color: #555;
    cursor: pointer;
    white-space: nowrap;
}

.ctrl-btn {
    font-size: 11px;
    padding: 3px 8px;
    border: 1px solid #b0b4bc;
    border-radius: 3px;
    background: #d6d9e0;
    color: #3a3d44;
    cursor: pointer;
    transition: background 0.1s;
    white-space: nowrap;
    text-align: center;
}
.ctrl-btn:hover { background: #cbcfd7; }
.ctrl-btn.inactive { opacity: 0.35; pointer-events: none; }
.ctrl-btn.diff-active {
    background: #d05060;
    border-color: #b44050;
    color: #fff;
}
.ctrl-btn.diff-active:hover { background: #c04858; }

.search-box {
    font-size: 12px;
    padding: 3px 8px;
    border: 1px solid #b0b4bc;
    border-radius: 3px;
    width: 220px;
    min-width: 220px;
    outline: none;
    background: #fff;
    color: #1c1e24;
}
.search-box:focus {
    border-color: #5a8eee;
    box-shadow: 0 0 0 2px rgba(90, 142, 238, 0.18);
}

.search-info {
    font-size: 11px;
    color: #888;
    white-space: nowrap;
}

/* ─── Trace container ─── */
.trace {
    flex: 1;
    min-height: 0;
    display: flex;
    overflow-x: auto;
    overflow-y: hidden;
    padding: 0 16px 12px 16px;
    align-items: stretch;
}

/* ─── Stage ─── */
.stage-wrap {
    display: flex;
    align-items: stretch;
    margin-right: 3px;
    flex-shrink: 0;
}

/* Stage collapsed sidebar */
.stage-col {
    width: 28px;
    min-width: 28px;
    background: #b8bcc6;
    border: 1px solid #a0a4ae;
    border-radius: 4px;
    cursor: pointer;
    user-select: none;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding-top: 6px;
    transition: background 0.1s;
    flex-shrink: 0;
}
.stage-col:hover { background: #aeb2bc; }

.stage-col-arrow {
    font-size: 9px;
    color: #6a6e78;
    margin-bottom: 4px;
    flex-shrink: 0;
}

.stage-col-text {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 12px;
    font-weight: 600;
    color: #444850;
    white-space: nowrap;
    padding: 4px 0;
}

.stage-col-count {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 10px;
    color: #7a7e88;
    white-space: nowrap;
    padding: 2px 0;
    margin-top: 2px;
}

/* Empty stage styling */
.stage-wrap.stage-empty .stage-col {
    background: #d0d3da;
    border-color: #bbbec6;
}
.stage-wrap.stage-empty .stage-col:hover { background: #c6c9d0; }
.stage-wrap.stage-empty .stage-col-text { color: #999; }
.stage-wrap.stage-empty .stage-col-arrow { color: #aaa; }
.stage-wrap.stage-empty .stage-hdr {
    background: #d0d3da;
    border-color: #bbbec6;
    color: #999;
}

/* Stage expanded column */
.stage-exp {
    display: flex;
    flex-direction: column;
    min-width: 0;
    min-height: 0;
}

/* Stage header bar — darkest level */
.stage-hdr {
    height: 34px;
    min-height: 34px;
    max-height: 34px;
    display: flex;
    align-items: center;
    padding: 0 10px;
    background: #b8bcc6;
    border: 1px solid #a0a4ae;
    border-bottom: none;
    border-radius: 3px 3px 0 0;
    font-size: 12px;
    font-weight: 600;
    color: #2c2f36;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    overflow: hidden;
    transition: background 0.1s;
    flex-shrink: 0;
}
.stage-hdr:hover { background: #aeb2bc; }

.stage-hdr-arrow {
    margin-right: 6px;
    font-size: 8px;
    color: #6a6e78;
}

.stage-hdr-count {
    font-weight: 400;
    color: #6a6e78;
    margin-left: 6px;
    font-size: 11px;
}

.stage-toggle-btn {
    font-size: 10px;
    padding: 1px 5px;
    border: 1px solid #a0a4ae;
    border-radius: 3px;
    background: #a8acb6;
    color: #444;
    cursor: pointer;
    transition: background 0.1s;
    margin-right: 6px;
    flex-shrink: 0;
    line-height: 1.3;
}
.stage-toggle-btn:hover {
    background: #9ea2ac;
    color: #222;
}

/* Rules row container */
.rules-row {
    display: flex;
    align-items: stretch;
    flex: 1;
    min-height: 0;
    overflow: hidden;
    border: 1px solid #a0a4ae;
    border-top: none;
    border-radius: 0 0 3px 3px;
    background: #cdd0d8;
    gap: 1px;
}

/* ─── Unified small button ─── */
.tbtn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 18px;
    min-height: 18px;
    max-height: 18px;
    border: 1px solid #c0c4cc;
    border-radius: 3px;
    background: #e6e8ec;
    color: #666;
    cursor: pointer;
    transition: background 0.1s;
    line-height: 1;
    flex-shrink: 0;
    padding: 0;
    vertical-align: middle;
}
.tbtn:hover { background: #d8dade; color: #222; }
.tbtn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }
.tbtn.nav { width: 22px; font-size: 13px; }
.tbtn.icon { min-width: 22px; font-size: 11px; padding: 0 4px; }
.tbtn.icon.active { background: #5a8eee; border-color: #4a78d8; color: #fff; }
.tbtn.ab { width: 18px; font-size: 10px; font-weight: 700; color: #888; }
.tbtn.ab:hover { background: #d4d6dc; color: #555; }
.tbtn.ab.selected-a { background: #4a8ad4; border-color: #3a78c0; color: #fff; }
.tbtn.ab.selected-b { background: #d04a70; border-color: #b83a60; color: #fff; }

/* ─── Rule cell ─── */
.rule-cell {
    display: flex;
    flex-direction: column;
    overflow: hidden;
    flex-shrink: 0;
    border: none;
}

.rule-cell.expanded {
    width: 520px;
    min-width: 520px;
    max-width: 520px;
    background: #fff;
}
.rule-cell.expanded .rule-col-wrap { display: none; }
.rule-cell.expanded .rule-exp-wrap {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
}

.rule-cell.collapsed {
    width: 16px;
    min-width: 16px;
    max-width: 16px;
    background: #e4e6ec;
    cursor: pointer;
}
.rule-cell.collapsed:hover { background: #dcdee4; }
.rule-cell.collapsed .rule-col-wrap {
    display: flex;
    flex-direction: column;
    height: 100%;
    align-items: center;
}
.rule-cell.collapsed .rule-exp-wrap { display: none; }

/* ─── Rule title bar — lightest level ─── */
.rule-title-bar {
    display: flex;
    align-items: center;
    height: 32px;
    min-height: 32px;
    max-height: 32px;
    border-bottom: 1px solid #e0e2e8;
    background: #f0f2f5;
    flex-shrink: 0;
    padding: 0 6px 0 0;
}

.rule-title {
    padding: 0 6px;
    font-size: 12px;
    font-weight: 400;
    color: #1c1e24;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    line-height: 32px;
}

.rule-num {
    font-weight: 400;
    color: #a0a4ae;
    font-size: 10px;
    margin-right: 3px;
}

.title-left {
    display: flex;
    align-items: center;
    flex: 1;
    min-width: 0;
    gap: 3px;
    overflow: hidden;
}

.title-right {
    display: flex;
    align-items: center;
    gap: 3px;
    flex-shrink: 0;
    margin-left: 4px;
}

.rule-col-wrap { padding-top: 32px; }

.rule-col-num {
    font-size: 8px;
    color: #bbb;
    flex-shrink: 0;
    text-align: center;
    width: 100%;
    padding-bottom: 2px;
}

.rule-col-text {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 12px;
    font-weight: 400;
    color: #555;
    white-space: nowrap;
    padding: 4px 1px;
}

/* ─── Rule content ─── */
.rule-content {
    flex: 1;
    min-height: 0;
    display: flex;
    flex-direction: column;
    overflow: hidden;
}

.rule-tree-wrap {
    flex: 1;
    overflow: auto;
    min-height: 0;
    padding: 8px 12px;
}

/* ─── Info panel ─── */
.rule-info-panel {
    display: none;
    border-top: 2px solid #d0d2d8;
    overflow: auto;
    max-height: 45%;
    flex-shrink: 0;
    background: #f8f9fb;
    padding: 8px 12px;
}
.rule-info-panel.visible { display: block; }

.info-section { margin-bottom: 10px; }

.info-section-title {
    font-size: 11px;
    font-weight: 600;
    color: #555;
    margin-bottom: 4px;
    padding-bottom: 2px;
    border-bottom: 1px solid #e4e6ea;
}

.info-table {
    font-size: 12px;
    border-collapse: collapse;
    width: 100%;
}
.info-table td {
    padding: 2px 8px 2px 0;
    vertical-align: top;
    border-bottom: 1px solid #eef0f3;
}
.info-table td:first-child {
    font-weight: 500;
    color: #555;
    white-space: nowrap;
    width: 1%;
}

.info-text {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px;
    white-space: pre-wrap;
    line-height: 1.5;
    color: #333;
}

.info-html {
    font-size: 12px;
    line-height: 1.5;
}

/* ─── Tree ─── */
.tree-root {
    white-space: nowrap;
    min-width: min-content;
}

/* Stats header */
.tree-stats-header {
    display: none;
    padding: 0 0 4px 0;
    margin-bottom: 4px;
    border-bottom: 1px solid #e4e6ea;
}
.tree-stats-header.visible { display: flex; }

.stat-header-cell {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 10px;
    font-weight: 600;
    color: #888;
    width: 72px;
    min-width: 72px;
    text-align: right;
    padding-right: 8px;
    flex-shrink: 0;
}

/* Tree node row */
.tree-node-row {
    display: flex;
    align-items: center;
    min-height: 22px;
    padding: 0;
    border-radius: 2px;
}
.tree-node-row:hover { background: rgba(0, 0, 0, 0.025); }

.stat-val-cell {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px;
    color: #888;
    width: 72px;
    min-width: 72px;
    text-align: right;
    padding-right: 8px;
    flex-shrink: 0;
    display: none;
}
.stats-active .stat-val-cell { display: block; }
.stats-active .tree-stats-header { display: flex; }

/* ─── CSS connector cells ───
   Each .tree-c is a 20×22 inline-block with positioned lines inside. */
.tree-c {
    position: relative;
    display: inline-block;
    width: 20px;
    min-width: 20px;
    height: 22px;
    flex-shrink: 0;
    pointer-events: none;
}

/* Vertical line: full height (continuing pipe) */
.tree-c.pipe::before,
.tree-c.branch::before {
    content: '';
    position: absolute;
    left: 9px;
    top: 0;
    bottom: 0;
    border-left: 1px solid #cdd0d6;
}

/* Vertical line: top to center only (L-shape) */
.tree-c.elbow::before {
    content: '';
    position: absolute;
    left: 9px;
    top: 0;
    height: 11px;
    border-left: 1px solid #cdd0d6;
}

/* Horizontal branch */
.tree-c.branch::after,
.tree-c.elbow::after {
    content: '';
    position: absolute;
    left: 9px;
    top: 11px;
    width: 11px;
    border-top: 1px solid #cdd0d6;
}

/* Connector cells in metadata rows (shorter height) */
.tree-cm {
    position: relative;
    display: inline-block;
    width: 20px;
    min-width: 20px;
    height: 18px;
    flex-shrink: 0;
    pointer-events: none;
}
.tree-cm.pipe::before {
    content: '';
    position: absolute;
    left: 9px;
    top: 0;
    bottom: 0;
    border-left: 1px solid #cdd0d6;
}

.tree-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 16px;
    min-width: 16px;
    height: 16px;
    font-size: 8px;
    color: #999;
    cursor: pointer;
    flex-shrink: 0;
    border-radius: 2px;
    transition: color 0.1s;
    user-select: none;
}
.tree-toggle:hover { color: #444; background: rgba(0, 0, 0, 0.06); }
.tree-toggle.leaf { cursor: default; color: transparent; pointer-events: none; }

.tree-label {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 22px;
    color: #1c1e24;
    padding-left: 2px;
    white-space: nowrap;
}
.tree-label.clickable { cursor: pointer; }
.tree-label.clickable:hover {
    text-decoration: underline;
    text-decoration-color: #ccc;
}

/* Per-node metadata row */
.tree-meta-row {
    display: flex;
    align-items: center;
    min-height: 18px;
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px;
    line-height: 18px;
}

.tree-meta-pad {
    display: inline-block;
    width: 18px;
    flex-shrink: 0;
}

.tree-meta-key { color: #888; font-weight: 500; white-space: nowrap; }
.tree-meta-val { color: #555; white-space: nowrap; }

.tree-meta-block { }
.tree-meta-block.collapsed { display: none; }
.tree-children-block { }

/* ─── Diff coloring — flat, no nesting accumulation ─── */
.diff-mark-add > .tree-node-row { background: #d4edda; }
.diff-mark-add > .tree-node-row .tree-label { color: #1a7f37; }
.diff-mark-rem > .tree-node-row { background: #f8d7da; }
.diff-mark-rem > .tree-node-row .tree-label {
    color: #c62828;
    text-decoration: line-through;
    text-decoration-color: #c62828;
}

/* ─── Group cell ─── */
.group-cell {
    display: flex;
    flex-direction: column;
    overflow: hidden;
    flex-shrink: 0;
    border: none;
}

/* Group collapsed — medium level, subtle neutral gray */
.group-cell.g-collapsed {
    width: 24px;
    min-width: 24px;
    max-width: 24px;
    background: #c8ccd4;
    cursor: pointer;
}
.group-cell.g-collapsed:hover { background: #bec2ca; }
.group-cell.g-collapsed .group-col-wrap {
    display: flex;
    flex-direction: column;
    height: 100%;
    align-items: center;
    padding-top: 32px;
}
.group-cell.g-collapsed .group-exp-wrap { display: none; }

.group-col-count {
    font-size: 8px;
    color: #6a6e78;
    flex-shrink: 0;
    font-weight: 600;
    text-align: center;
    width: 100%;
    padding-bottom: 2px;
}

.group-col-text {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 11px;
    font-weight: 600;
    color: #4a4e58;
    white-space: nowrap;
    padding: 4px 2px;
}

.group-cell.g-expanded { background: transparent; }
.group-cell.g-expanded .group-col-wrap { display: none; }
.group-cell.g-expanded .group-exp-wrap {
    display: flex;
    flex-direction: column;
    flex: 1;
    min-height: 0;
}

/* Group title bar — between stage and rule */
.group-title-bar {
    display: flex;
    align-items: center;
    height: 16px;
    min-height: 16px;
    max-height: 16px;
    padding: 0 4px;
    background: #c2c6ce;
    flex-shrink: 0;
}

.group-toggle-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 21px;
    min-width: 21px;
    font-size: 8px;
    padding: 0;
    border: none;
    background: none;
    color: #5a5e68;
    cursor: pointer;
    transition: color 0.1s;
    flex-shrink: 0;
    line-height: 14px;
}
.group-toggle-btn:hover { color: #222; }

.group-title {
    flex: 1;
    font-size: 11px;
    font-weight: 600;
    color: #3a3e46;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    padding: 0 4px;
    line-height: 16px;
}

.group-nav {
    display: flex;
    gap: 2px;
    flex-shrink: 0;
}

.group-nav-btn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 12px;
    font-size: 10px;
    border: 1px solid #a8acb4;
    border-radius: 2px;
    background: #b4b8c0;
    color: #4a4e58;
    cursor: pointer;
    transition: background 0.1s;
    line-height: 1;
    padding: 0;
    flex-shrink: 0;
}
.group-nav-btn:hover { background: #a8acb6; color: #222; }
.group-nav-btn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }

.group-rules-area {
    display: flex;
    flex: 1;
    min-height: 0;
    align-items: stretch;
    overflow: hidden;
    background: #cdd0d8;
    gap: 1px;
}

/* Inside groups: compact button/title sizes */
.group-rules-area .rule-title-bar {
    height: 16px;
    min-height: 16px;
    max-height: 16px;
    padding: 0 4px 0 6px;
}
.group-rules-area .rule-title {
    font-size: 11px;
    line-height: 16px;
    padding: 0 2px 0 0;
}
.group-rules-area .rule-col-text { font-size: 11px; }
.group-rules-area .rule-num {
    display: inline-block;
    min-width: 20px;
    text-align: right;
    font-size: 9px;
    padding-right: 6px;
}
.group-rules-area .title-left { gap: 2px; }
.group-rules-area .title-right { gap: 2px; margin-left: 2px; }
.group-rules-area .tbtn {
    height: 12px;
    min-height: 12px;
    max-height: 12px;
    border-radius: 2px;
    font-size: 8px;
}
.group-rules-area .tbtn.nav { width: 18px; font-size: 9px; }
.group-rules-area .tbtn.icon { min-width: 14px; font-size: 8px; padding: 0 2px; }
.group-rules-area .tbtn.ab { width: 14px; font-size: 7px; }
.group-rules-area .rule-col-wrap { padding-top: 16px; }

/* Flash animation */
@keyframes cellFlash {
    0%   { filter: brightness(1.5); }
    100% { filter: brightness(1); }
}
.flash-cell { animation: cellFlash 0.2s ease-out; }

mark {
    background: #fff176;
    color: #1c1e24;
    border-radius: 2px;
    padding: 0 1px;
}


/* ═══════════════════════════
   Dark mode
   ═══════════════════════════ */

body.darkmode {
    background: #16181e;
    color: #cdd0d8;
}
body.darkmode .top-bar { background: #16181e; }

body.darkmode .ctrl-btn {
    background: #2a2d36;
    border-color: #3e424c;
    color: #b0b4bc;
}
body.darkmode .ctrl-btn:hover { background: #34383e; }
body.darkmode .ctrl-btn.diff-active {
    background: #a83848;
    border-color: #8c2e3c;
    color: #fff;
}
body.darkmode .ctrl-btn.diff-active:hover { background: #983040; }
body.darkmode .controls label { color: #888; }

body.darkmode .search-box {
    background: #22252c;
    border-color: #3e424c;
    color: #cdd0d8;
}
body.darkmode .search-box:focus {
    border-color: #5a8eee;
    box-shadow: 0 0 0 2px rgba(90, 142, 238, 0.12);
}

/* Stage — darkest level in dark mode */
body.darkmode .stage-col {
    background: #1e2028;
    border-color: #2e3038;
}
body.darkmode .stage-col:hover { background: #262830; }
body.darkmode .stage-col-text { color: #8a8e98; }
body.darkmode .stage-col-arrow { color: #5a5e68; }
body.darkmode .stage-col-count { color: #5a5e68; }

body.darkmode .stage-wrap.stage-empty .stage-col {
    background: #1a1c22;
    border-color: #2a2c34;
}
body.darkmode .stage-wrap.stage-empty .stage-col:hover { background: #1e2028; }
body.darkmode .stage-wrap.stage-empty .stage-col-text { color: #4a4e56; }
body.darkmode .stage-wrap.stage-empty .stage-hdr {
    background: #1a1c22;
    border-color: #2a2c34;
    color: #4a4e56;
}

body.darkmode .stage-hdr {
    background: #1e2028;
    border-color: #2e3038;
    color: #a0a4ae;
}
body.darkmode .stage-hdr:hover { background: #262830; }
body.darkmode .stage-hdr-count { color: #5a5e68; }
body.darkmode .stage-hdr-arrow { color: #5a5e68; }

body.darkmode .stage-toggle-btn {
    background: #262830;
    border-color: #3a3e48;
    color: #8a8e98;
}
body.darkmode .stage-toggle-btn:hover {
    background: #303440;
    color: #ccc;
}

body.darkmode .rules-row {
    border-color: #2e3038;
    background: #1a1c22;
}

/* Group — medium level in dark mode */
body.darkmode .group-cell.g-collapsed {
    background: #252830;
}
body.darkmode .group-cell.g-collapsed:hover { background: #2c2f38; }
body.darkmode .group-col-text { color: #8a8e98; }
body.darkmode .group-col-count { color: #6a6e78; }
body.darkmode .group-title-bar { background: #252830; }
body.darkmode .group-title { color: #9a9ea8; }
body.darkmode .group-toggle-btn { color: #7a7e88; }
body.darkmode .group-toggle-btn:hover { color: #ccc; }
body.darkmode .group-nav-btn {
    background: #2a2d36;
    border-color: #3a3e48;
    color: #8a8e98;
}
body.darkmode .group-nav-btn:hover {
    background: #343840;
    color: #ccc;
}
body.darkmode .group-rules-area { background: #1a1c22; }

/* Rule — lightest level in dark mode */
body.darkmode .rule-cell.expanded { background: #1c1e26; }
body.darkmode .rule-cell.collapsed { background: #22252e; }
body.darkmode .rule-cell.collapsed:hover { background: #282b34; }
body.darkmode .rule-col-text { color: #9a9ea8; }
body.darkmode .rule-col-num { color: #4a4e56; }

body.darkmode .rule-title-bar {
    background: #2a2d36;
    border-bottom-color: #34383e;
}
body.darkmode .rule-title { color: #b8bcc6; }
body.darkmode .rule-num { color: #5a5e68; }

body.darkmode .tbtn {
    background: #2a2d36;
    border-color: #3e424c;
    color: #7a7e88;
}
body.darkmode .tbtn:hover { background: #34383e; color: #ccc; }
body.darkmode .tbtn.icon.active {
    background: #3a70c8;
    border-color: #2e60b0;
    color: #fff;
}
body.darkmode .tbtn.ab { color: #6a6e78; }
body.darkmode .tbtn.ab:hover { background: #34383e; color: #a0a4ae; }
body.darkmode .tbtn.ab.selected-a {
    background: #3068b8;
    border-color: #2858a0;
    color: #fff;
}
body.darkmode .tbtn.ab.selected-b {
    background: #b03858;
    border-color: #982e48;
    color: #fff;
}

body.darkmode .rule-info-panel {
    background: #1a1c22;
    border-top-color: #3a3e48;
}
body.darkmode .info-section-title {
    color: #888;
    border-bottom-color: #2e3038;
}
body.darkmode .info-table td { border-bottom-color: #262830; }
body.darkmode .info-table td:first-child { color: #7a7e88; }
body.darkmode .info-text { color: #b0b4bc; }

body.darkmode .tree-node-row:hover { background: rgba(255, 255, 255, 0.02); }

body.darkmode .tree-c.pipe::before,
body.darkmode .tree-c.branch::before,
body.darkmode .tree-c.elbow::before { border-left-color: #34383e; }
body.darkmode .tree-c.branch::after,
body.darkmode .tree-c.elbow::after { border-top-color: #34383e; }
body.darkmode .tree-cm.pipe::before { border-left-color: #34383e; }

body.darkmode .tree-toggle { color: #5a5e68; }
body.darkmode .tree-toggle:hover {
    color: #bbb;
    background: rgba(255, 255, 255, 0.05);
}
body.darkmode .tree-label { color: #cdd0d8; }
body.darkmode .tree-label.clickable:hover { text-decoration-color: #444; }
body.darkmode .tree-meta-key { color: #6a6e78; }
body.darkmode .tree-meta-val { color: #9a9ea8; }
body.darkmode .stat-val-cell { color: #6a6e78; }
body.darkmode .stat-header-cell { color: #5a5e68; }
body.darkmode .tree-stats-header { border-bottom-color: #2e3038; }
body.darkmode mark { background: #5a5520; color: #eee8a0; }

body.darkmode .diff-mark-add > .tree-node-row { background: #1a3328; }
body.darkmode .diff-mark-add > .tree-node-row .tree-label { color: #4ad864; }
body.darkmode .diff-mark-rem > .tree-node-row { background: #3d1f24; }
body.darkmode .diff-mark-rem > .tree-node-row .tree-label { color: #f04848; }

</style>
<script>
)RAWCSS";

        /* ═══════════ JS data ═══════════ */
        size_t stageCount = stages_.size();
        out << "var stageCount = " << stageCount << ";\n";

        out << "var stageRuleCounts = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << stages_[i].rules.size();
        }
        out << "];\n";

        out << "var stageNames = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "\"" << jsEsc(stages_[i].name) << "\"";
        }
        out << "];\n";

        out << "var groups = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t g = 0; g < stageGroups[i].size(); g++) {
                if (g) out << ", ";
                const auto& grp = stageGroups[i][g];
                out << "{name: \"" << jsEsc(grp.name) << "\", ri: [";
                for (size_t k = 0; k < grp.ruleIndices.size(); k++) {
                    if (k) out << ", ";
                    out << grp.ruleIndices[k];
                }
                out << "]}";
            }
            out << "]";
        }
        out << "];\n";

        out << "var ruleNames = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t j = 0; j < stages_[i].rules.size(); j++) {
                if (j) out << ", ";
                out << "\"" << jsEsc(stages_[i].rules[j].ruleName) << "\"";
            }
            out << "]";
        }
        out << "];\n";

        out << "var statColumns = [";
        for (size_t i = 0; i < statColumns_.size(); i++) {
            if (i) out << ", ";
            out << "\"" << jsEsc(statColumns_[i]) << "\"";
        }
        out << "];\n";
        out << "var statColCount = " << statColumns_.size() << ";\n";

        out << "var planTrees = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t j = 0; j < stages_[i].rules.size(); j++) {
                if (j) out << ", ";
                emitTreeJSON(out, stages_[i].rules[j].tree);
            }
            out << "]";
        }
        out << "];\n";

        out << "var ruleInfo = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t j = 0; j < stages_[i].rules.size(); j++) {
                if (j) out << ", ";
                emitInfoJSON(out, stages_[i].rules[j].info);
            }
            out << "]";
        }
        out << "];\n";

        out << "var ruleHasMeta = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t j = 0; j < stages_[i].rules.size(); j++) {
                if (j) out << ", ";
                out << (stages_[i].rules[j].tree.hasAnyMeta() ? 1 : 0);
            }
            out << "]";
        }
        out << "];\n";

        out << "var ruleHasStats = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t j = 0; j < stages_[i].rules.size(); j++) {
                if (j) out << ", ";
                out << (stages_[i].rules[j].tree.hasAnyStats() ? 1 : 0);
            }
            out << "]";
        }
        out << "];\n";

        out << "var ruleHasInfo = [";
        for (size_t i = 0; i < stageCount; i++) {
            if (i) out << ", ";
            out << "[";
            for (size_t j = 0; j < stages_[i].rules.size(); j++) {
                if (j) out << ", ";
                out << (!stages_[i].rules[j].info.empty() ? 1 : 0);
            }
            out << "]";
        }
        out << "];\n\n";

        /* ═══════════ JS logic ═══════════ */
        out << R"RAWJS(

/* ══════════════════════════════════════════════════════════════
   Flat index for global navigation across all rules
   ══════════════════════════════════════════════════════════════ */

var allRules = [];
for (var si = 0; si < stageCount; si++) {
    for (var gi = 0; gi < groups[si].length; gi++) {
        var grp = groups[si][gi];
        for (var ri = 0; ri < grp.ri.length; ri++) {
            allRules.push({
                stageIdx: si,
                groupIdx: gi,
                ruleIdx: ri,
                rawRuleIdx: grp.ri[ri]
            });
        }
    }
}

function findFlatIndex(si, gi, ri) {
    for (var i = 0; i < allRules.length; i++) {
        if (allRules[i].stageIdx === si &&
            allRules[i].groupIdx === gi &&
            allRules[i].ruleIdx === ri) {
            return i;
        }
    }
    return -1;
}


/* ══════════════════════════════════════════════════════════════
   Layout state (open/closed for stages, groups, rules)
   ══════════════════════════════════════════════════════════════ */

var stageOpen = [];
var groupOpen = [];
var ruleOpen = [];
var ruleMetaAll = [];
var ruleStats = [];
var ruleInfoOpen = [];
var searchActive = false;

for (var si = 0; si < stageCount; si++) {
    stageOpen.push(stageRuleCounts[si] > 0);
    groupOpen.push([]);
    ruleOpen.push([]);
    ruleMetaAll.push([]);
    ruleStats.push([]);
    ruleInfoOpen.push([]);

    for (var gi = 0; gi < groups[si].length; gi++) {
        var count = groups[si][gi].ri.length;
        var isMulti = count > 1;

        groupOpen[si].push(!isMulti);
        ruleOpen[si].push([]);
        ruleMetaAll[si].push([]);
        ruleStats[si].push([]);
        ruleInfoOpen[si].push([]);

        for (var ri = 0; ri < count; ri++) {
            ruleOpen[si][gi].push(isMulti ? (ri === count - 1) : true);
            ruleMetaAll[si][gi].push(false);
            ruleStats[si][gi].push(false);
            ruleInfoOpen[si][gi].push(false);
        }
    }
}


/* ══════════════════════════════════════════════════════════════
   HTML helpers
   ══════════════════════════════════════════════════════════════ */

function htmlEscape(text) {
    return text
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#39;');
}

function htmlHighlight(text, query) {
    if (!query) return htmlEscape(text);
    var lowerText = text.toLowerCase();
    var lowerQuery = query.toLowerCase();
    var result = '';
    var pos = 0;
    while (pos < text.length) {
        var idx = lowerText.indexOf(lowerQuery, pos);
        if (idx === -1) {
            result += htmlEscape(text.substring(pos));
            break;
        }
        result += htmlEscape(text.substring(pos, idx));
        result += '<mark>' + htmlEscape(text.substring(idx, idx + query.length)) + '</mark>';
        pos = idx + query.length;
    }
    return result;
}


/* ══════════════════════════════════════════════════════════════
   Plan tree rendering (normal mode, with CSS connector cells)
   ══════════════════════════════════════════════════════════════

   Connector cells (.tree-c) are 20×22px inline-blocks with
   ::before/::after drawing continuous lines.

   Metadata rows get .tree-cm cells (20×18px) that continue
   vertical lines through metadata and children columns.
   ══════════════════════════════════════════════════════════════ */

function renderTree(container, tree, ruleKey, showMeta, showStats, query) {
    var headerHtml = '';
    if (statColCount > 0) {
        headerHtml += '<div class="tree-stats-header' +
                      (showStats ? ' visible' : '') +
                      '" id="stathdr-' + ruleKey + '">';
        for (var col = 0; col < statColCount; col++) {
            headerHtml += '<span class="stat-header-cell">' +
                          htmlEscape(statColumns[col]) + '</span>';
        }
        headerHtml += '<span style="width:16px;flex-shrink:0;display:inline-block"></span>';
        headerHtml += '<span style="font-size:10px;color:#aaa;font-weight:600;padding-left:2px">Plan</span>';
        headerHtml += '</div>';
    }

    container.innerHTML = headerHtml +
        '<div class="tree-root' + (showStats ? ' stats-active' : '') +
        '" id="treeroot-' + ruleKey + '">' +
        renderRootNode(tree, ruleKey, '0', showMeta, showStats, query) +
        '</div>';
}

/**
 * Renders the root node (no connector from parent).
 *
 * @param {Object} node    - plan tree node {l, m, s, c}
 * @param {string} rk      - rule key for unique IDs
 * @param {string} pid     - path-based ID segment
 * @param {boolean} showMeta
 * @param {boolean} showStats
 * @param {string} query   - search highlight term
 */
function renderRootNode(node, rk, pid, showMeta, showStats, query) {
    if (!node || !node.l) return '';

    var hasChildren = node.c && node.c.length > 0;
    var hasMeta = node.m && node.m.length > 0;
    var nodeId = 'tn-' + rk + '-' + pid;
    var html = '<div id="' + nodeId + '">';

    /* Label row */
    html += '<div class="tree-node-row">';
    html += renderStatCells(node, showStats, query);

    if (hasChildren) {
        html += '<span class="tree-toggle" id="' + nodeId + '-tog"' +
                ' onclick="treeToggle(event,\x27' + nodeId + '\x27)">\u25BC</span>';
    } else {
        html += '<span class="tree-toggle leaf">\u2022</span>';
    }

    html += renderLabelSpan(node, nodeId, hasMeta, query);
    html += '</div>';

    /* Metadata block */
    if (hasMeta) {
        html += renderMetaBlock(node, nodeId, showMeta, query, [], hasChildren);
    }

    /* Children */
    if (hasChildren) {
        html += '<div class="tree-children-block" id="' + nodeId + '-ch">';
        for (var i = 0; i < node.c.length; i++) {
            var isLast = (i === node.c.length - 1);
            var childPrefix = [isLast ? 'space' : 'pipe'];
            html += renderChildNode(node.c[i], rk, pid + '-' + i, showMeta, showStats, query,
                                    [], childPrefix, isLast);
        }
        html += '</div>';
    }

    html += '</div>';
    return html;
}

/**
 * Renders a child node with its branch/elbow connector prepended.
 */
function renderChildNode(node, rk, pid, showMeta, showStats, query,
                         parentPrefix, childPrefix, isLast) {
    if (!node || !node.l) return '';

    var hasChildren = node.c && node.c.length > 0;
    var hasMeta = node.m && node.m.length > 0;
    var nodeId = 'tn-' + rk + '-' + pid;
    var branchType = isLast ? 'elbow' : 'branch';

    var html = '<div id="' + nodeId + '">';

    /* Label row */
    html += '<div class="tree-node-row">';
    html += renderStatCells(node, showStats, query);

    for (var i = 0; i < parentPrefix.length; i++) {
        html += '<span class="tree-c ' + parentPrefix[i] + '"></span>';
    }
    html += '<span class="tree-c ' + branchType + '"></span>';

    if (hasChildren) {
        html += '<span class="tree-toggle" id="' + nodeId + '-tog"' +
                ' onclick="treeToggle(event,\x27' + nodeId + '\x27)">\u25BC</span>';
    } else {
        html += '<span class="tree-toggle leaf">\u2022</span>';
    }

    html += renderLabelSpan(node, nodeId, hasMeta, query);
    html += '</div>';

    /* Metadata block */
    if (hasMeta) {
        var metaPrefix = parentPrefix.slice();
        metaPrefix.push(isLast ? 'space' : 'pipe');
        html += renderMetaBlock(node, nodeId, showMeta, query, metaPrefix, hasChildren);
    }

    /* Children */
    if (hasChildren) {
        html += '<div class="tree-children-block" id="' + nodeId + '-ch">';
        for (var i = 0; i < node.c.length; i++) {
            var childIsLast = (i === node.c.length - 1);
            var grandPrefix = childPrefix.slice();
            grandPrefix.push(childIsLast ? 'space' : 'pipe');
            html += renderChildNode(node.c[i], rk, pid + '-' + i, showMeta, showStats, query,
                                    childPrefix, grandPrefix, childIsLast);
        }
        html += '</div>';
    }

    html += '</div>';
    return html;
}

/** Render stat value cells for a node row. */
function renderStatCells(node, showStats, query) {
    if (statColCount === 0) return '';
    var html = '';
    for (var col = 0; col < statColCount; col++) {
        var val = (node.s && col < node.s.length && node.s[col])
                  ? htmlHighlight(node.s[col], query) : '';
        html += '<span class="stat-val-cell">' + val + '</span>';
    }
    return html;
}

/** Render the label span (clickable if metadata present). */
function renderLabelSpan(node, nodeId, hasMeta, query) {
    if (hasMeta) {
        return '<span class="tree-label clickable"' +
               ' onclick="treeMetaToggle(event,\x27' + nodeId + '\x27)">' +
               htmlHighlight(node.l, query) + '</span>';
    }
    return '<span class="tree-label">' + htmlHighlight(node.l, query) + '</span>';
}

/** Render a metadata block for a node. */
function renderMetaBlock(node, nodeId, showMeta, query, ancestorPrefixes, hasChildren) {
    var html = '<div class="tree-meta-block' + (showMeta ? '' : ' collapsed') +
               '" id="' + nodeId + '-meta">';

    for (var i = 0; i < node.m.length; i++) {
        html += '<div class="tree-meta-row">';

        /* Stat placeholders */
        if (statColCount > 0) {
            for (var col = 0; col < statColCount; col++) {
                html += '<span class="stat-val-cell"></span>';
            }
        }

        /* Ancestor connectors */
        for (var j = 0; j < ancestorPrefixes.length; j++) {
            html += '<span class="tree-cm ' + ancestorPrefixes[j] + '"></span>';
        }

        /* Children column continuation */
        if (hasChildren) {
            html += '<span class="tree-cm pipe"></span>';
        } else {
            html += '<span class="tree-cm"></span>';
        }

        html += '<span class="tree-meta-pad"></span>';
        html += '<span class="tree-meta-key">' + htmlHighlight(node.m[i][0], query) + ':</span> ';
        html += '<span class="tree-meta-val">' + htmlHighlight(node.m[i][1], query) + '</span>';
        html += '</div>';
    }

    html += '</div>';
    return html;
}

/** Toggle tree node expand/collapse. */
function treeToggle(event, nodeId) {
    event.stopPropagation();
    var childBlock = document.getElementById(nodeId + '-ch');
    var toggleEl = document.getElementById(nodeId + '-tog');
    if (!childBlock || !toggleEl) return;

    if (childBlock.style.display === 'none') {
        childBlock.style.display = '';
        toggleEl.textContent = '\u25BC';
    } else {
        childBlock.style.display = 'none';
        toggleEl.textContent = '\u25B6';
    }
}

/** Toggle metadata visibility for a single tree node. */
function treeMetaToggle(event, nodeId) {
    event.stopPropagation();
    var metaBlock = document.getElementById(nodeId + '-meta');
    if (metaBlock) {
        metaBlock.classList.toggle('collapsed');
    }
}


/* ══════════════════════════════════════════════════════════════
   Rule-level actions (metadata, stats, info toggles)
   ══════════════════════════════════════════════════════════════ */

function rerenderRuleTree(si, gi, ri, query) {
    var rawIdx = groups[si][gi].ri[ri];
    var ruleKey = si + '-' + gi + '-' + ri;
    var container = document.getElementById('tree-' + ruleKey);
    if (!container) return;

    renderTree(container, planTrees[si][rawIdx], ruleKey,
               ruleMetaAll[si][gi][ri], ruleStats[si][gi][ri], query || '');
}

function toggleRuleMeta(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleMetaAll[si][gi][ri] = !ruleMetaAll[si][gi][ri];
    rerenderRuleTree(si, gi, ri, currentSearchQuery());
    var btn = document.getElementById('metabtn-' + si + '-' + gi + '-' + ri);
    if (btn) btn.classList.toggle('active', ruleMetaAll[si][gi][ri]);
}

function toggleRuleStats(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleStats[si][gi][ri] = !ruleStats[si][gi][ri];
    rerenderRuleTree(si, gi, ri, currentSearchQuery());
    var btn = document.getElementById('statbtn-' + si + '-' + gi + '-' + ri);
    if (btn) btn.classList.toggle('active', ruleStats[si][gi][ri]);
}

function toggleRuleInfo(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleInfoOpen[si][gi][ri] = !ruleInfoOpen[si][gi][ri];
    var panel = document.getElementById('info-' + si + '-' + gi + '-' + ri);
    var btn = document.getElementById('infobtn-' + si + '-' + gi + '-' + ri);
    if (panel) panel.classList.toggle('visible', ruleInfoOpen[si][gi][ri]);
    if (btn) btn.classList.toggle('active', ruleInfoOpen[si][gi][ri]);
}

function renderInfoPanel(container, sections) {
    if (!sections || !sections.length) {
        container.innerHTML = '<div style="color:#999;font-size:11px;font-style:italic">No info</div>';
        return;
    }
    var html = '';
    for (var i = 0; i < sections.length; i++) {
        var section = sections[i];
        html += '<div class="info-section">';

        if (section.title) {
            html += '<div class="info-section-title">' + htmlEscape(section.title) + '</div>';
        }

        if (section.type === 'table' && section.rows) {
            html += '<table class="info-table">';
            for (var r = 0; r < section.rows.length; r++) {
                html += '<tr><td>' + htmlEscape(section.rows[r][0]) +
                        '</td><td>' + htmlEscape(section.rows[r][1]) + '</td></tr>';
            }
            html += '</table>';
        } else if (section.type === 'html') {
            html += '<div class="info-html">' + section.content + '</div>';
        } else if (section.type === 'text') {
            html += '<div class="info-text">' + htmlEscape(section.content) + '</div>';
        }

        html += '</div>';
    }
    container.innerHTML = html;
}


/* ══════════════════════════════════════════════════════════════
   Display updates (expand/collapse for stage, group, rule)
   ══════════════════════════════════════════════════════════════ */

function updateStageDisplay(si) {
    var colEl = document.getElementById('stage-col-' + si);
    var expEl = document.getElementById('stage-exp-' + si);
    if (!colEl || !expEl) return;
    colEl.style.display = stageOpen[si] ? 'none' : 'flex';
    expEl.style.display = stageOpen[si] ? 'flex' : 'none';
}

function updateGroupDisplay(si, gi) {
    if (groups[si][gi].ri.length <= 1) return;
    var el = document.getElementById('group-' + si + '-' + gi);
    if (el) {
        el.className = 'group-cell ' + (groupOpen[si][gi] ? 'g-expanded' : 'g-collapsed');
    }
}

function updateRuleDisplay(si, gi, ri) {
    var el = document.getElementById('rule-' + si + '-' + gi + '-' + ri);
    if (el) {
        el.className = 'rule-cell ' + (ruleOpen[si][gi][ri] ? 'expanded' : 'collapsed');
    }
}

function updateAllDisplays() {
    for (var si = 0; si < stageCount; si++) {
        updateStageDisplay(si);
        for (var gi = 0; gi < groups[si].length; gi++) {
            updateGroupDisplay(si, gi);
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                updateRuleDisplay(si, gi, ri);
            }
        }
    }
}


/* ══════════════════════════════════════════════════════════════
   Flash animation
   ══════════════════════════════════════════════════════════════ */

var justExpandedByNav = null;

function flashRuleCell(si, gi, ri) {
    var id = 'rule-' + si + '-' + gi + '-' + ri;
    if (justExpandedByNav === id) {
        justExpandedByNav = null;
        return;
    }
    justExpandedByNav = null;
    var el = document.getElementById(id);
    if (!el) return;
    el.classList.remove('flash-cell');
    void el.offsetWidth;
    el.classList.add('flash-cell');
}


/* ══════════════════════════════════════════════════════════════
   Stage / Group / Rule interactions
   ══════════════════════════════════════════════════════════════ */

function toggleStage(si) {
    stageOpen[si] = !stageOpen[si];
    updateStageDisplay(si);
}

function toggleStageRules(si, ev) {
    ev.stopPropagation();
    if (!stageRuleCounts[si]) return;

    /* Check if any rule is currently open in this stage */
    var anyOpen = false;
    for (var gi = 0; gi < groups[si].length && !anyOpen; gi++) {
        var count = groups[si][gi].ri.length;
        if (count > 1) {
            if (groupOpen[si][gi]) {
                for (var ri = 0; ri < count; ri++) {
                    if (ruleOpen[si][gi][ri]) { anyOpen = true; break; }
                }
            }
        } else {
            if (ruleOpen[si][gi][0]) { anyOpen = true; }
        }
    }

    /* Toggle all groups/rules in stage */
    for (var gi = 0; gi < groups[si].length; gi++) {
        var count = groups[si][gi].ri.length;
        if (anyOpen) {
            if (count > 1) groupOpen[si][gi] = false;
            for (var ri = 0; ri < count; ri++) ruleOpen[si][gi][ri] = false;
        } else {
            if (count > 1) groupOpen[si][gi] = true;
            for (var ri = 0; ri < count; ri++) ruleOpen[si][gi][ri] = true;
        }
        updateGroupDisplay(si, gi);
        for (var ri = 0; ri < count; ri++) {
            updateRuleDisplay(si, gi, ri);
        }
    }
}

function toggleGroup(si, gi, ev) {
    if (ev) ev.stopPropagation();
    var count = groups[si][gi].ri.length;
    if (count <= 1) return;

    groupOpen[si][gi] = !groupOpen[si][gi];

    if (groupOpen[si][gi]) {
        /* When opening a group, show only the last rule by default */
        for (var ri = 0; ri < count; ri++) {
            ruleOpen[si][gi][ri] = (ri === count - 1);
            updateRuleDisplay(si, gi, ri);
        }
    }
    updateGroupDisplay(si, gi);
}

function toggleGroupRules(si, gi, ev) {
    ev.stopPropagation();
    var count = groups[si][gi].ri.length;
    if (count <= 1 || !groupOpen[si][gi]) return;

    var anyOpen = false;
    for (var ri = 0; ri < count; ri++) {
        if (ruleOpen[si][gi][ri]) { anyOpen = true; break; }
    }
    for (var ri = 0; ri < count; ri++) {
        ruleOpen[si][gi][ri] = !anyOpen;
        updateRuleDisplay(si, gi, ri);
    }
}

function onCollapsedGroupClick(si, gi, ev) {
    ev.stopPropagation();
    if (!groupOpen[si][gi]) toggleGroup(si, gi, null);
}

function toggleRule(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleOpen[si][gi][ri] = !ruleOpen[si][gi][ri];
    updateRuleDisplay(si, gi, ri);
}

function onCollapsedRuleClick(si, gi, ri, ev) {
    ev.stopPropagation();
    if (!ruleOpen[si][gi][ri]) toggleRule(si, gi, ri, null);
}


/* ══════════════════════════════════════════════════════════════
   Navigation
   ══════════════════════════════════════════════════════════════ */

function ensureRuleVisible(si, gi, ri) {
    var wasAlreadyVisible = true;

    if (!stageOpen[si]) {
        stageOpen[si] = true;
        updateStageDisplay(si);
        wasAlreadyVisible = false;
    }

    var count = groups[si][gi].ri.length;
    if (count > 1 && !groupOpen[si][gi]) {
        groupOpen[si][gi] = true;
        for (var r = 0; r < count; r++) {
            ruleOpen[si][gi][r] = (r === count - 1);
            updateRuleDisplay(si, gi, r);
        }
        updateGroupDisplay(si, gi);
        if (ri !== count - 1) wasAlreadyVisible = false;
    }

    if (!ruleOpen[si][gi][ri]) {
        ruleOpen[si][gi][ri] = true;
        updateRuleDisplay(si, gi, ri);
        wasAlreadyVisible = false;
    }

    return wasAlreadyVisible;
}

function scrollToFlash(si, gi, ri, wasVisible) {
    var id = 'rule-' + si + '-' + gi + '-' + ri;
    var el = document.getElementById(id);
    if (!el) return;

    if (!wasVisible) justExpandedByNav = id;
    el.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
    setTimeout(function() { flashRuleCell(si, gi, ri); }, 80);
}

function navigatePrev(si, gi, ri, ev) {
    ev.stopPropagation();
    var idx = findFlatIndex(si, gi, ri);
    if (idx <= 0) return;
    var target = allRules[idx - 1];
    var wasVisible = ensureRuleVisible(target.stageIdx, target.groupIdx, target.ruleIdx);
    scrollToFlash(target.stageIdx, target.groupIdx, target.ruleIdx, wasVisible);
}

function navigateNext(si, gi, ri, ev) {
    ev.stopPropagation();
    var idx = findFlatIndex(si, gi, ri);
    if (idx >= allRules.length - 1) return;
    var target = allRules[idx + 1];
    var wasVisible = ensureRuleVisible(target.stageIdx, target.groupIdx, target.ruleIdx);
    scrollToFlash(target.stageIdx, target.groupIdx, target.ruleIdx, wasVisible);
}

function groupNavigatePrev(si, gi, ev) {
    ev.stopPropagation();
    var idx = findFlatIndex(si, gi, 0);
    if (idx <= 0) return;
    var target = allRules[idx - 1];
    var wasVisible = ensureRuleVisible(target.stageIdx, target.groupIdx, target.ruleIdx);
    scrollToFlash(target.stageIdx, target.groupIdx, target.ruleIdx, wasVisible);
}

function groupNavigateNext(si, gi, ev) {
    ev.stopPropagation();
    var count = groups[si][gi].ri.length;
    var idx = findFlatIndex(si, gi, count - 1);
    if (idx >= allRules.length - 1) return;
    var target = allRules[idx + 1];
    var wasVisible = ensureRuleVisible(target.stageIdx, target.groupIdx, target.ruleIdx);
    scrollToFlash(target.stageIdx, target.groupIdx, target.ruleIdx, wasVisible);
}


/* ══════════════════════════════════════════════════════════════
   Global cycle (expand/collapse all)
   ══════════════════════════════════════════════════════════════ */

var globalCycleState = 3;
var cycleLabels = ['Expand Stages', 'Expand Rules', 'Expand All', 'Collapse All'];

function cycleGlobalState() {
    globalCycleState = (globalCycleState + 1) % 4;

    for (var si = 0; si < stageCount; si++) {
        if (!stageRuleCounts[si]) continue;

        switch (globalCycleState) {
        case 0: /* All collapsed */
            stageOpen[si] = false;
            for (var gi = 0; gi < groups[si].length; gi++) {
                groupOpen[si][gi] = false;
                for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                    ruleOpen[si][gi][ri] = false;
                }
            }
            break;

        case 1: /* Stages open, groups/rules collapsed */
            stageOpen[si] = true;
            for (var gi = 0; gi < groups[si].length; gi++) {
                groupOpen[si][gi] = false;
                for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                    ruleOpen[si][gi][ri] = false;
                }
            }
            break;

        case 2: /* Stages + groups open, last rule in each group expanded */
            stageOpen[si] = true;
            for (var gi = 0; gi < groups[si].length; gi++) {
                var count = groups[si][gi].ri.length;
                groupOpen[si][gi] = true;
                for (var ri = 0; ri < count; ri++) {
                    ruleOpen[si][gi][ri] = (count > 1 ? ri === count - 1 : true);
                }
            }
            break;

        case 3: /* Everything expanded */
            stageOpen[si] = true;
            for (var gi = 0; gi < groups[si].length; gi++) {
                groupOpen[si][gi] = true;
                for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                    ruleOpen[si][gi][ri] = true;
                }
            }
            break;
        }
    }

    updateAllDisplays();
    document.getElementById('cycle-button').textContent = cycleLabels[globalCycleState];
}

function toggleDarkMode() {
    document.body.classList.toggle('darkmode');
}


/* ══════════════════════════════════════════════════════════════
   Layout state save/restore (for search and diff)
   ══════════════════════════════════════════════════════════════ */

function saveLayoutState() {
    return {
        stageOpen: stageOpen.slice(),
        groupOpen: groupOpen.map(function(arr) { return arr.slice(); }),
        ruleOpen: ruleOpen.map(function(arr) {
            return arr.map(function(inner) { return inner.slice(); });
        })
    };
}

function applyLayoutState(state) {
    stageOpen = state.stageOpen;
    groupOpen = state.groupOpen;
    ruleOpen = state.ruleOpen;
}

function currentSearchQuery() {
    var box = document.getElementById('search-box');
    return (searchActive && box) ? box.value.trim() : '';
}


/* ══════════════════════════════════════════════════════════════
   Search
   ══════════════════════════════════════════════════════════════ */

var savedSearchState = null;

function doSearch() {
    var query = document.getElementById('search-box').value.trim();
    var infoEl = document.getElementById('search-info');

    if (!query) {
        if (searchActive && savedSearchState) {
            applyLayoutState(savedSearchState);
            savedSearchState = null;
            searchActive = false;
            rerenderAllTrees('');
            updateAllDisplays();
        }
        infoEl.textContent = '';
        return;
    }

    if (!searchActive) {
        savedSearchState = saveLayoutState();
        searchActive = true;
    }

    var lowerQuery = query.toLowerCase();
    var matches = 0;

    /* Collapse everything first */
    for (var si = 0; si < stageCount; si++) {
        stageOpen[si] = false;
        for (var gi = 0; gi < groups[si].length; gi++) {
            groupOpen[si][gi] = false;
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                ruleOpen[si][gi][ri] = false;
            }
        }
    }

    /* Expand matching rules */
    for (var si = 0; si < stageCount; si++) {
        for (var gi = 0; gi < groups[si].length; gi++) {
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                var rawIdx = groups[si][gi].ri[ri];
                var nameMatch = ruleNames[si][rawIdx].toLowerCase().indexOf(lowerQuery) !== -1;
                var treeMatch = treeContainsText(planTrees[si][rawIdx], lowerQuery);

                if (nameMatch || treeMatch) {
                    stageOpen[si] = true;
                    if (groups[si][gi].ri.length > 1) groupOpen[si][gi] = true;
                    ruleOpen[si][gi][ri] = true;
                    ruleMetaAll[si][gi][ri] = treeMatch;
                    rerenderRuleTree(si, gi, ri, query);
                    matches++;
                } else {
                    rerenderRuleTree(si, gi, ri, '');
                }
            }
        }
    }

    infoEl.textContent = matches
        ? matches + ' match' + (matches > 1 ? 'es' : '')
        : 'no matches';
    updateAllDisplays();
}

function treeContainsText(node, lowerQuery) {
    if (!node || !node.l) return false;
    if (node.l.toLowerCase().indexOf(lowerQuery) !== -1) return true;
    if (node.m) {
        for (var i = 0; i < node.m.length; i++) {
            if (node.m[i][0].toLowerCase().indexOf(lowerQuery) !== -1) return true;
            if (node.m[i][1].toLowerCase().indexOf(lowerQuery) !== -1) return true;
        }
    }
    if (node.c) {
        for (var i = 0; i < node.c.length; i++) {
            if (treeContainsText(node.c[i], lowerQuery)) return true;
        }
    }
    return false;
}

function rerenderAllTrees(query) {
    for (var si = 0; si < stageCount; si++) {
        for (var gi = 0; gi < groups[si].length; gi++) {
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                rerenderRuleTree(si, gi, ri, query);
            }
        }
    }
}


/* ══════════════════════════════════════════════════════════════
   A/B Diff — selection and UI
   ══════════════════════════════════════════════════════════════ */

var diffA = null;
var diffB = null;
var savedDiffState = null;

function updateDiffButton() {
    var btn = document.getElementById('diff-button');
    if (!btn) return;
    if (savedDiffState) {
        btn.textContent = 'Close \u2206-view';
        btn.className = 'ctrl-btn diff-active';
    } else if (diffA || diffB) {
        btn.textContent = 'Reset \uD83C\uDD70\uD83C\uDD71';
        btn.className = 'ctrl-btn';
    } else {
        btn.textContent = 'Reset \uD83C\uDD70\uD83C\uDD71';
        btn.className = 'ctrl-btn inactive';
    }
}

function onDiffButtonClick() {
    if (savedDiffState) {
        closeDiffFull();
    } else if (diffA || diffB) {
        clearDiffSelection('both');
        diffA = null;
        diffB = null;
        updateDiffButton();
    }
}

function setDiffA(si, gi, ri, ev) {
    ev.stopPropagation();

    if (diffA && diffA.si === si && diffA.gi === gi && diffA.ri === ri) {
        /* Deselect A */
        var wasActive = !!savedDiffState;
        if (wasActive) {
            exitDiffKeepOther('b');
        } else {
            clearDiffSelection('a');
            diffA = null;
        }
        updateDiffButton();
        return;
    }

    clearDiffSelection('a');
    diffA = { si: si, gi: gi, ri: ri };
    var btn = document.getElementById('abtn-' + si + '-' + gi + '-' + ri);
    if (btn) btn.classList.add('selected-a');

    if (diffB) showDiffInline();
    updateDiffButton();
}

function setDiffB(si, gi, ri, ev) {
    ev.stopPropagation();

    if (diffB && diffB.si === si && diffB.gi === gi && diffB.ri === ri) {
        /* Deselect B */
        var wasActive = !!savedDiffState;
        if (wasActive) {
            exitDiffKeepOther('a');
        } else {
            clearDiffSelection('b');
            diffB = null;
        }
        updateDiffButton();
        return;
    }

    clearDiffSelection('b');
    diffB = { si: si, gi: gi, ri: ri };
    var btn = document.getElementById('bbtn-' + si + '-' + gi + '-' + ri);
    if (btn) btn.classList.add('selected-b');

    if (diffA) showDiffInline();
    updateDiffButton();
}

function clearDiffSelection(which) {
    if ((which === 'a' || which === 'both') && diffA) {
        var btn = document.getElementById('abtn-' + diffA.si + '-' + diffA.gi + '-' + diffA.ri);
        if (btn) btn.classList.remove('selected-a');
        if (which === 'a') diffA = null;
    }
    if ((which === 'b' || which === 'both') && diffB) {
        var btn = document.getElementById('bbtn-' + diffB.si + '-' + diffB.gi + '-' + diffB.ri);
        if (btn) btn.classList.remove('selected-b');
        if (which === 'b') diffB = null;
    }
}

/** Close diff entirely, clearing both selections. */
function closeDiffFull() {
    if (savedDiffState) {
        applyLayoutState(savedDiffState);
        savedDiffState = null;
    }
    clearDiffSelection('both');
    diffA = null;
    diffB = null;
    rerenderAllTrees(currentSearchQuery());
    updateAllDisplays();
    updateDiffButton();
}

/** Exit diff but keep one selection (the 'other' side). */
function exitDiffKeepOther(keepWhich) {
    if (savedDiffState) {
        applyLayoutState(savedDiffState);
        savedDiffState = null;
    }
    if (keepWhich === 'a') {
        clearDiffSelection('b');
        diffB = null;
    } else {
        clearDiffSelection('a');
        diffA = null;
    }
    rerenderAllTrees(currentSearchQuery());
    updateAllDisplays();
}

/** Compute and display the diff between A and B. */
function showDiffInline() {
    if (!diffA || !diffB) return;
    if (!savedDiffState) savedDiffState = saveLayoutState();

    /* Collapse everything, then open only A and B */
    for (var si = 0; si < stageCount; si++) {
        stageOpen[si] = false;
        for (var gi = 0; gi < groups[si].length; gi++) {
            groupOpen[si][gi] = false;
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                ruleOpen[si][gi][ri] = false;
            }
        }
    }

    var targets = [diffA, diffB];
    for (var t = 0; t < 2; t++) {
        var d = targets[t];
        stageOpen[d.si] = true;
        if (groups[d.si][d.gi].ri.length > 1) groupOpen[d.si][d.gi] = true;
        ruleOpen[d.si][d.gi][d.ri] = true;
    }
    updateAllDisplays();

    /* Compute diff */
    var rawA = groups[diffA.si][diffA.gi].ri[diffA.ri];
    var rawB = groups[diffB.si][diffB.gi].ri[diffB.ri];
    var diffResult = TreeDiff.diff(planTrees[diffA.si][rawA], planTrees[diffB.si][rawB]);

    /* Render side A */
    var keyA = diffA.si + '-' + diffA.gi + '-' + diffA.ri;
    var containerA = document.getElementById('tree-' + keyA);
    if (containerA) {
        containerA.innerHTML = '<div class="tree-root">' +
            DiffRenderer.renderSide(diffResult, keyA, '0', 'a', [], true) +
            '</div>';
    }

    /* Render side B */
    var keyB = diffB.si + '-' + diffB.gi + '-' + diffB.ri;
    var containerB = document.getElementById('tree-' + keyB);
    if (containerB) {
        containerB.innerHTML = '<div class="tree-root">' +
            DiffRenderer.renderSide(diffResult, keyB, '0', 'b', [], true) +
            '</div>';
    }

    updateDiffButton();
}


/* ══════════════════════════════════════════════════════════════
   Tree Diff Module
   ══════════════════════════════════════════════════════════════

   Clean, self-contained module for computing structural diffs
   between two plan trees.

   Input:  two plan tree nodes (format: {l: string, c: [{...}], ...})
   Output: array of DiffNode objects:
       {
           status:   'same' | 'added' | 'removed',
           label:    string,
           original: reference to source plan node (for added/removed),
           children: [DiffNode, ...]
       }

   The algorithm:
   1. Basic structural diff using LCS matching on child labels.
   2. Move detection pass: finds removed subtrees that match
      added subtrees (by label + structural similarity), then
      re-diffs their children so only the moved node itself is
      highlighted, not its entire subtree.
   ══════════════════════════════════════════════════════════════ */

var TreeDiff = (function() {

    /**
     * Main entry point. Diffs two plan trees and returns an
     * array of diff nodes with move detection applied.
     */
    function diff(nodeA, nodeB) {
        var result = basicDiff(nodeA, nodeB);
        detectMoves(result);
        return result;
    }

    /**
     * Basic structural diff using LCS on child labels.
     * Returns an array of diff nodes (typically length 1 if
     * root labels match, or 2 if they differ).
     */
    function basicDiff(nodeA, nodeB) {
        if (!nodeA && !nodeB) return [];
        if (!nodeA) return [markEntireSubtree(nodeB, 'added')];
        if (!nodeB) return [markEntireSubtree(nodeA, 'removed')];

        /* Root labels differ: both trees are fully changed */
        if (nodeA.l !== nodeB.l) {
            return [
                markEntireSubtree(nodeA, 'removed'),
                markEntireSubtree(nodeB, 'added')
            ];
        }

        /* Root labels match: diff the children */
        var resultNode = {
            status: 'same',
            label: nodeA.l,
            original: null,
            children: []
        };

        var childrenA = nodeA.c || [];
        var childrenB = nodeB.c || [];
        var editOps = computeLCSEditOps(childrenA, childrenB);

        for (var i = 0; i < editOps.length; i++) {
            var op = editOps[i];
            if (op.type === 'match') {
                var subDiff = basicDiff(childrenA[op.idxA], childrenB[op.idxB]);
                for (var s = 0; s < subDiff.length; s++) {
                    resultNode.children.push(subDiff[s]);
                }
            } else if (op.type === 'remove') {
                resultNode.children.push(markEntireSubtree(childrenA[op.idxA], 'removed'));
            } else if (op.type === 'add') {
                resultNode.children.push(markEntireSubtree(childrenB[op.idxB], 'added'));
            }
        }

        return [resultNode];
    }

    /**
     * Mark an entire subtree as added or removed.
     * Stores a reference to the original plan node for move detection.
     */
    function markEntireSubtree(planNode, status) {
        var diffNode = {
            status: status,
            label: planNode.l,
            original: planNode,
            children: []
        };
        if (planNode.c) {
            for (var i = 0; i < planNode.c.length; i++) {
                diffNode.children.push(markEntireSubtree(planNode.c[i], status));
            }
        }
        return diffNode;
    }

    /**
     * Compute LCS-based edit operations between two child arrays.
     * Children are matched by their label (.l property).
     * Returns an ordered array of operations:
     *   {type:'match', idxA, idxB}, {type:'remove', idxA}, {type:'add', idxB}
     */
    function computeLCSEditOps(childrenA, childrenB) {
        var n = childrenA.length;
        var m = childrenB.length;

        /* Build DP table */
        var dp = [];
        for (var i = 0; i <= n; i++) {
            dp[i] = [];
            for (var j = 0; j <= m; j++) {
                dp[i][j] = 0;
            }
        }
        for (var i = 1; i <= n; i++) {
            for (var j = 1; j <= m; j++) {
                if (childrenA[i - 1].l === childrenB[j - 1].l) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        /* Backtrack to find matched pairs */
        var matched = [];
        var i = n, j = m;
        while (i > 0 && j > 0) {
            if (childrenA[i - 1].l === childrenB[j - 1].l) {
                matched.unshift({ type: 'match', idxA: i - 1, idxB: j - 1 });
                i--;
                j--;
            } else if (dp[i - 1][j] >= dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }

        /* Build ordered edit operations */
        var ops = [];
        var ai = 0, bi = 0;
        for (var mi = 0; mi < matched.length; mi++) {
            while (ai < matched[mi].idxA) {
                ops.push({ type: 'remove', idxA: ai });
                ai++;
            }
            while (bi < matched[mi].idxB) {
                ops.push({ type: 'add', idxB: bi });
                bi++;
            }
            ops.push(matched[mi]);
            ai = matched[mi].idxA + 1;
            bi = matched[mi].idxB + 1;
        }
        while (ai < n) {
            ops.push({ type: 'remove', idxA: ai });
            ai++;
        }
        while (bi < m) {
            ops.push({ type: 'add', idxB: bi });
            bi++;
        }
        return ops;
    }


    /* ── Move detection ──────────────────────────────────────
       After the basic diff, some removed subtrees may match
       added subtrees elsewhere (i.e. a node was moved, not
       deleted and recreated). We detect these by:

       1. Collecting all fully-marked "removed" and "added"
          subtrees from the diff tree.
       2. Matching them by label + structural similarity.
       3. For each matched pair, re-diffing their children
          so only actual structural changes are highlighted,
          not the entire moved subtree.

       The top-level node keeps its 'removed'/'added' status
       to show where the move happened.
       ────────────────────────────────────────────────────── */

    /**
     * Top-level move detection. Modifies diffNodes in place.
     */
    function detectMoves(diffNodes) {
        var removedList = [];
        var addedList = [];
        collectFullyMarkedSubtrees(diffNodes, removedList, addedList);

        if (removedList.length === 0 || addedList.length === 0) return;

        /* Greedy matching: for each removed node, find the best
           matching added node by label + similarity score */
        var usedAddedIndices = {};

        for (var ri = 0; ri < removedList.length; ri++) {
            var removedNode = removedList[ri];
            var bestAddedIdx = -1;
            var bestScore = 0;

            for (var ai = 0; ai < addedList.length; ai++) {
                if (usedAddedIndices[ai]) continue;
                var addedNode = addedList[ai];

                if (removedNode.label !== addedNode.label) continue;

                var score = computeSubtreeSimilarity(
                    removedNode.original, addedNode.original
                );
                if (score > bestScore) {
                    bestScore = score;
                    bestAddedIdx = ai;
                }
            }

            /* Require at least the root label matches (score >= 1) */
            if (bestAddedIdx >= 0 && bestScore >= 1) {
                var addedNode = addedList[bestAddedIdx];
                usedAddedIndices[bestAddedIdx] = true;

                /* Re-diff the subtrees properly.
                   Since labels match, basicDiff returns a single
                   node with status 'same' and diffed children. */
                var subDiff = basicDiff(removedNode.original, addedNode.original);

                if (subDiff.length === 1 && subDiff[0].status === 'same') {
                    /* Replace uniformly-marked children with
                       properly diffed children */
                    removedNode.children = subDiff[0].children;
                    addedNode.children = subDiff[0].children;
                }
                /* The top-level status stays 'removed'/'added'
                   to visually indicate the insertion/deletion point */
            }
        }

        /* Recurse: the re-diffed children may themselves contain
           new removed/added pairs that are moves */
        for (var i = 0; i < diffNodes.length; i++) {
            if (diffNodes[i].children && diffNodes[i].children.length > 0) {
                detectMoves(diffNodes[i].children);
            }
        }
    }

    /**
     * Collect all top-level fully-marked subtrees (where every
     * descendant has the same status). These are candidates for
     * move detection.
     *
     * A node is "fully marked" if:
     *   - Its status is 'added' or 'removed', AND
     *   - All its descendants have the same status
     *     (which is true by construction from markEntireSubtree,
     *      unless a previous move-detection pass has modified it)
     */
    function collectFullyMarkedSubtrees(diffNodes, removedList, addedList) {
        for (var i = 0; i < diffNodes.length; i++) {
            var node = diffNodes[i];
            if (node.status === 'removed' && isFullyMarked(node, 'removed')) {
                removedList.push(node);
            } else if (node.status === 'added' && isFullyMarked(node, 'added')) {
                addedList.push(node);
            } else if (node.status === 'same' && node.children) {
                /* Only recurse into 'same' nodes — we want top-level
                   marked subtrees, not nested ones */
                collectFullyMarkedSubtrees(node.children, removedList, addedList);
            }
        }
    }

    /**
     * Check whether every node in the subtree has the given status.
     */
    function isFullyMarked(diffNode, status) {
        if (diffNode.status !== status) return false;
        if (diffNode.children) {
            for (var i = 0; i < diffNode.children.length; i++) {
                if (!isFullyMarked(diffNode.children[i], status)) return false;
            }
        }
        return true;
    }

    /**
     * Compute a similarity score between two plan tree nodes.
     * Returns the count of label matches in the overlapping structure.
     * Used for ranking move candidates.
     */
    function computeSubtreeSimilarity(planA, planB) {
        if (!planA || !planB) return 0;
        if (planA.l !== planB.l) return 0;

        var score = 1; /* Root labels match */

        var childrenA = planA.c || [];
        var childrenB = planB.c || [];

        if (childrenA.length === 0 && childrenB.length === 0) return score;

        /* Use LCS to find matching children and sum their similarity */
        var n = childrenA.length;
        var m = childrenB.length;
        var dp = [];
        for (var i = 0; i <= n; i++) {
            dp[i] = [];
            for (var j = 0; j <= m; j++) {
                dp[i][j] = 0;
            }
        }
        for (var i = 1; i <= n; i++) {
            for (var j = 1; j <= m; j++) {
                if (childrenA[i - 1].l === childrenB[j - 1].l) {
                    dp[i][j] = dp[i - 1][j - 1] + 1;
                } else {
                    dp[i][j] = Math.max(dp[i - 1][j], dp[i][j - 1]);
                }
            }
        }

        /* Backtrack to find matched children */
        var i = n, j = m;
        while (i > 0 && j > 0) {
            if (childrenA[i - 1].l === childrenB[j - 1].l) {
                score += computeSubtreeSimilarity(childrenA[i - 1], childrenB[j - 1]);
                i--;
                j--;
            } else if (dp[i - 1][j] >= dp[i][j - 1]) {
                i--;
            } else {
                j--;
            }
        }

        return score;
    }

    /* Public API */
    return {
        diff: diff
    };

})();


/* ══════════════════════════════════════════════════════════════
   Diff Renderer Module
   ══════════════════════════════════════════════════════════════

   Renders a diff result (array of DiffNode) for one side (A or B)
   using CSS connector cells, matching the normal tree rendering
   style.

   Side 'a' shows 'same' and 'removed' nodes.
   Side 'b' shows 'same' and 'added' nodes.
   ══════════════════════════════════════════════════════════════ */

var DiffRenderer = (function() {

    /**
     * Filter diff nodes to those visible on the given side.
     */
    function filterVisible(nodes, side) {
        var result = [];
        for (var i = 0; i < nodes.length; i++) {
            var node = nodes[i];
            if (side === 'a' && node.status === 'added') continue;
            if (side === 'b' && node.status === 'removed') continue;
            result.push(node);
        }
        return result;
    }

    /**
     * Get the CSS class for a diff node's status.
     */
    function diffClass(status) {
        if (status === 'added') return ' diff-mark-add';
        if (status === 'removed') return ' diff-mark-rem';
        return '';
    }

    /**
     * Render the visible nodes for one side of the diff.
     *
     * @param {Array}   nodes        - array of DiffNode
     * @param {string}  ruleKey      - for unique DOM IDs
     * @param {string}  basePid      - path ID prefix
     * @param {string}  side         - 'a' or 'b'
     * @param {Array}   parentPrefix - ancestor connector types
     * @param {boolean} isRoot       - true for the top level
     */
    function renderSide(nodes, ruleKey, basePid, side, parentPrefix, isRoot) {
        var visible = filterVisible(nodes, side);
        var html = '';

        if (isRoot) {
            for (var i = 0; i < visible.length; i++) {
                html += renderDiffRootNode(
                    visible[i], ruleKey, basePid + '-' + i, side
                );
            }
        } else {
            for (var i = 0; i < visible.length; i++) {
                var isLast = (i === visible.length - 1);
                var childPrefix = parentPrefix.slice();
                childPrefix.push(isLast ? 'space' : 'pipe');
                html += renderDiffChildNode(
                    visible[i], ruleKey, basePid + '-' + i, side,
                    parentPrefix, childPrefix, isLast
                );
            }
        }
        return html;
    }

    /**
     * Render a root-level diff node (no branch connector).
     */
    function renderDiffRootNode(node, ruleKey, pid, side) {
        var visibleChildren = filterVisible(node.children || [], side);
        var nodeId = 'tn-' + ruleKey + '-' + pid;

        var html = '<div class="' + diffClass(node.status).trim() + '" id="' + nodeId + '">';

        /* Label row */
        html += '<div class="tree-node-row">';
        if (visibleChildren.length > 0) {
            html += '<span class="tree-toggle" id="' + nodeId + '-tog"' +
                    ' onclick="treeToggle(event,\x27' + nodeId + '\x27)">\u25BC</span>';
        } else {
            html += '<span class="tree-toggle leaf">\u2022</span>';
        }
        html += '<span class="tree-label">' + htmlEscape(node.label || '') + '</span>';
        html += '</div>';

        /* Children */
        if (visibleChildren.length > 0) {
            html += '<div class="tree-children-block" id="' + nodeId + '-ch">';
            html += renderSide(node.children, ruleKey, pid, side, [], false);
            html += '</div>';
        }

        html += '</div>';
        return html;
    }

    /**
     * Render a child diff node with branch/elbow connector.
     */
    function renderDiffChildNode(node, ruleKey, pid, side,
                                  parentPrefix, childPrefix, isLast) {
        var visibleChildren = filterVisible(node.children || [], side);
        var branchType = isLast ? 'elbow' : 'branch';
        var nodeId = 'tn-' + ruleKey + '-' + pid;

        var html = '<div class="' + diffClass(node.status).trim() + '" id="' + nodeId + '">';

        /* Label row */
        html += '<div class="tree-node-row">';
        for (var i = 0; i < parentPrefix.length; i++) {
            html += '<span class="tree-c ' + parentPrefix[i] + '"></span>';
        }
        html += '<span class="tree-c ' + branchType + '"></span>';

        if (visibleChildren.length > 0) {
            html += '<span class="tree-toggle" id="' + nodeId + '-tog"' +
                    ' onclick="treeToggle(event,\x27' + nodeId + '\x27)">\u25BC</span>';
        } else {
            html += '<span class="tree-toggle leaf">\u2022</span>';
        }
        html += '<span class="tree-label">' + htmlEscape(node.label || '') + '</span>';
        html += '</div>';

        /* Children */
        if (visibleChildren.length > 0) {
            html += '<div class="tree-children-block" id="' + nodeId + '-ch">';
            html += renderSide(node.children, ruleKey, pid, side, childPrefix, false);
            html += '</div>';
        }

        html += '</div>';
        return html;
    }

    /* Public API */
    return {
        renderSide: renderSide
    };

})();


/* ══════════════════════════════════════════════════════════════
   Horizontal wheel scroll
   ══════════════════════════════════════════════════════════════ */

function initWheelScroll() {
    var traceEl = document.querySelector('.trace');
    if (!traceEl) return;
    traceEl.addEventListener('wheel', function(event) {
        if (event.target.closest &&
            (event.target.closest('.rule-tree-wrap') ||
             event.target.closest('.rule-info-panel'))) {
            return;
        }
        if (Math.abs(event.deltaY) > Math.abs(event.deltaX)) {
            traceEl.scrollLeft += event.deltaY;
            event.preventDefault();
        }
    }, { passive: false });
}


/* ══════════════════════════════════════════════════════════════
   Initialization
   ══════════════════════════════════════════════════════════════ */

window.addEventListener('DOMContentLoaded', function() {
    initWheelScroll();

    for (var si = 0; si < stageCount; si++) {
        for (var gi = 0; gi < groups[si].length; gi++) {
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                rerenderRuleTree(si, gi, ri, '');

                var rawIdx = groups[si][gi].ri[ri];
                var infoContainer = document.getElementById(
                    'info-content-' + si + '-' + gi + '-' + ri
                );
                if (infoContainer) {
                    renderInfoPanel(infoContainer, ruleInfo[si][rawIdx]);
                }
            }
        }
    }

    var darkCheckbox = document.querySelector('.controls input[type="checkbox"]');
    if (darkCheckbox && !darkCheckbox.checked) {
        document.body.classList.remove('darkmode');
    }

    updateDiffButton();
});

</script>
</head><body class="darkmode">
)RAWJS";

        /* ═══════════ Controls ═══════════ */
        out << "<div class=\"top-bar\"><div class=\"controls\">\n";
        out << "  <h1>SQL Optimizer Trace</h1>\n";
        out << "  <button class=\"ctrl-btn inactive\" id=\"diff-button\" "
            << "onclick=\"onDiffButtonClick()\" style=\"min-width:90px\">"
            << "Reset \xF0\x9F\x85\xB0\xF0\x9F\x85\xB1</button>\n";
        out << "  <button class=\"ctrl-btn\" id=\"cycle-button\" "
            << "onclick=\"cycleGlobalState()\" style=\"min-width:120px\">"
            << "Collapse All</button>\n";
        out << "  <label><input type=\"checkbox\" checked onclick=\"toggleDarkMode()\" "
            << "style=\"cursor:pointer\"> Dark Theme</label>\n";
        out << "  <input class=\"search-box\" id=\"search-box\" type=\"text\" "
            << "placeholder=\"Search plans\xe2\x80\xa6\" oninput=\"doSearch()\">\n";
        out << "  <span class=\"search-info\" id=\"search-info\"></span>\n";
        out << "</div></div>\n\n";

        /* ═══════════ Trace ═══════════ */
        out << "<div class=\"trace\">\n";

        size_t totalRules = 0;
        for (const auto& s : stages_) totalRules += s.rules.size();

        for (size_t i = 0; i < stages_.size(); i++) {
            const Stage& stage = stages_[i];
            const auto& grps = stageGroups[i];
            bool empty = stage.rules.empty();

            out << "<div class=\"stage-wrap"
                << (empty ? " stage-empty" : "") << "\">\n";

            /* Collapsed stage sidebar */
            out << "  <div class=\"stage-col\" id=\"stage-col-" << i
                << "\" style=\"display:" << (empty ? "flex" : "none")
                << "\" onclick=\"toggleStage(" << i << ")\">"
                << "<div class=\"stage-col-arrow\">&#9654;</div>"
                << "<div class=\"stage-col-text\">" << esc(stage.name) << "</div>"
                << "<div class=\"stage-col-count\">(" << stage.rules.size() << ")</div>"
                << "</div>\n";

            /* Expanded stage */
            out << "  <div class=\"stage-exp\" id=\"stage-exp-" << i
                << "\" style=\"display:" << (empty ? "none" : "flex") << "\">\n";

            /* Stage header */
            out << "    <div class=\"stage-hdr\" onclick=\"toggleStage(" << i << ")\">";
            if (!empty) {
                out << "<button class=\"stage-toggle-btn\" "
                    << "onclick=\"toggleStageRules(" << i << ",event)\">&#9776;</button>";
            }
            out << "<span class=\"stage-hdr-arrow\">&#9660;</span>"
                << esc(stage.name)
                << "<span class=\"stage-hdr-count\">(" << stage.rules.size() << ")</span>"
                << "</div>\n";

            /* Rules row */
            out << "    <div class=\"rules-row\">\n";

            if (empty) {
                out << "      <div class=\"rule-cell expanded\" "
                    << "style=\"color:#666;font-style:italic;\">"
                    << "<div class=\"rule-exp-wrap\">"
                    << "<div class=\"rule-title-bar\" style=\"cursor:default;\">"
                    << "<div class=\"title-left\">"
                    << "<div class=\"rule-title\" style=\"cursor:default;color:#888;\">"
                    << "no rules</div></div></div>"
                    << "<div class=\"rule-content\"><div class=\"rule-tree-wrap\">"
                    << "<span style=\"color:#999;font-size:12px;\">no rules applied</span>"
                    << "</div></div></div></div>\n";
            }

            size_t flatBase = 0;
            for (size_t si2 = 0; si2 < i; si2++) {
                flatBase += stages_[si2].rules.size();
            }
            size_t ruleNum = 0;

            for (size_t g = 0; g < grps.size(); g++) {
                const auto& grp = grps[g];
                size_t cnt = grp.ruleIndices.size();

                if (cnt == 1) {
                    /* Single-rule group (no group wrapper needed) */
                    size_t j = grp.ruleIndices[0];
                    ruleNum++;
                    const auto& rule = stage.rules[j];
                    size_t flat = flatBase + j;
                    bool isFirst = (flat == 0);
                    bool isLast = (flat == totalRules - 1);
                    bool hasInfo = !rule.info.empty();
                    bool hasMeta = rule.tree.hasAnyMeta();
                    bool hasStats = rule.tree.hasAnyStats() && !statColumns_.empty();

                    out << "      <div class=\"rule-cell expanded\" id=\"rule-"
                        << i << "-" << g << "-0\" onclick=\"onCollapsedRuleClick("
                        << i << "," << g << ",0,event)\">\n";

                    out << "        <div class=\"rule-col-wrap\">"
                        << "<div class=\"rule-col-num\">" << ruleNum << "</div>"
                        << "<div class=\"rule-col-text\">" << esc(rule.ruleName) << "</div>"
                        << "</div>\n";

                    out << "        <div class=\"rule-exp-wrap\">\n";
                    emitTitleBar(out, i, g, 0, ruleNum, rule.ruleName,
                                 hasMeta, hasStats, hasInfo, isFirst, isLast);

                    out << "          <div class=\"rule-content\">\n";
                    out << "            <div class=\"rule-tree-wrap\" id=\"tree-"
                        << i << "-" << g << "-0\"></div>\n";

                    if (hasInfo) {
                        out << "            <div class=\"rule-info-panel\" id=\"info-"
                            << i << "-" << g << "-0\">"
                            << "<div id=\"info-content-" << i << "-" << g << "-0\"></div>"
                            << "</div>\n";
                    }

                    out << "          </div>\n";
                    out << "        </div>\n";
                    out << "      </div>\n";

                } else {
                    /* Multi-rule group */
                    size_t flatFirst = flatBase + grp.ruleIndices[0];
                    bool groupIsFirst = (flatFirst == 0);
                    bool groupIsLast = ((flatFirst + cnt - 1) == totalRules - 1);

                    out << "      <div class=\"group-cell g-collapsed\" id=\"group-"
                        << i << "-" << g << "\" onclick=\"onCollapsedGroupClick("
                        << i << "," << g << ",event)\">\n";

                    /* Group collapsed sidebar */
                    out << "        <div class=\"group-col-wrap\">"
                        << "<div class=\"group-col-count\">&times;" << cnt << "</div>"
                        << "<div class=\"group-col-text\">" << esc(grp.name) << "</div>"
                        << "</div>\n";

                    /* Group expanded content */
                    out << "        <div class=\"group-exp-wrap\">\n";

                    /* Group title bar */
                    out << "          <div class=\"group-title-bar\">\n";
                    out << "            <button class=\"group-toggle-btn\" "
                        << "onclick=\"toggleGroupRules(" << i << "," << g << ",event)\">"
                        << "&#9776;</button>\n";
                    out << "            <div class=\"group-title\" onclick=\"toggleGroup("
                        << i << "," << g << ",event)\">"
                        << esc(grp.name) << " (&times;" << cnt << ")</div>\n";
                    out << "            <div class=\"group-nav\">"
                        << "<button class=\"group-nav-btn\" onclick=\"groupNavigatePrev("
                        << i << "," << g << ",event)\""
                        << (groupIsFirst ? " disabled" : "") << ">&#8592;</button>"
                        << "<button class=\"group-nav-btn\" onclick=\"groupNavigateNext("
                        << i << "," << g << ",event)\""
                        << (groupIsLast ? " disabled" : "") << ">&#8594;</button>"
                        << "</div>\n";
                    out << "          </div>\n";

                    /* Group rules area */
                    out << "          <div class=\"group-rules-area\">\n";

                    for (size_t k = 0; k < cnt; k++) {
                        size_t j = grp.ruleIndices[k];
                        ruleNum++;
                        const auto& rule = stage.rules[j];
                        size_t flat = flatBase + j;
                        bool isFirst = (flat == 0);
                        bool isLast = (flat == totalRules - 1);
                        bool startExpanded = (k == cnt - 1);
                        bool hasInfo = !rule.info.empty();
                        bool hasMeta = rule.tree.hasAnyMeta();
                        bool hasStats = rule.tree.hasAnyStats() && !statColumns_.empty();

                        out << "            <div class=\"rule-cell "
                            << (startExpanded ? "expanded" : "collapsed")
                            << "\" id=\"rule-" << i << "-" << g << "-" << k
                            << "\" onclick=\"onCollapsedRuleClick("
                            << i << "," << g << "," << k << ",event)\">\n";

                        out << "              <div class=\"rule-col-wrap\">"
                            << "<div class=\"rule-col-num\">" << ruleNum << "</div>"
                            << "<div class=\"rule-col-text\">" << esc(rule.ruleName) << "</div>"
                            << "</div>\n";

                        out << "              <div class=\"rule-exp-wrap\">\n                ";
                        emitTitleBar(out, i, g, k, ruleNum, rule.ruleName,
                                     hasMeta, hasStats, hasInfo, isFirst, isLast);

                        out << "                <div class=\"rule-content\">\n";
                        out << "                  <div class=\"rule-tree-wrap\" id=\"tree-"
                            << i << "-" << g << "-" << k << "\"></div>\n";

                        if (hasInfo) {
                            out << "                  <div class=\"rule-info-panel\" id=\"info-"
                                << i << "-" << g << "-" << k << "\">"
                                << "<div id=\"info-content-"
                                << i << "-" << g << "-" << k << "\"></div>"
                                << "</div>\n";
                        }

                        out << "                </div>\n";
                        out << "              </div>\n";
                        out << "            </div>\n";
                    }

                    out << "          </div>\n";
                    out << "        </div>\n";
                    out << "      </div>\n";
                }
            }

            out << "    </div>\n";
            out << "  </div>\n";
            out << "</div>\n";
        }

        out << "</div>\n</body></html>\n";
        out.close();
        return true;
    }

private:
    std::vector<Stage> stages_;
    std::vector<std::string> statColumns_;

    /** HTML-escape a string for safe embedding in HTML content. */
    static std::string esc(const std::string& s) {
        std::string result;
        result.reserve(s.size() + s.size() / 8);
        for (char c : s) {
            switch (c) {
                case '&':  result += "&amp;";  break;
                case '<':  result += "&lt;";   break;
                case '>':  result += "&gt;";   break;
                case '"':  result += "&quot;"; break;
                case '\'': result += "&#39;";  break;
                default:   result += c;
            }
        }
        return result;
    }

    /** Escape a string for safe embedding in a JS string literal. */
    static std::string jsEsc(const std::string& s) {
        std::string result;
        result.reserve(s.size() + s.size() / 8);
        for (char c : s) {
            switch (c) {
                case '\\': result += "\\\\"; break;
                case '"':  result += "\\\""; break;
                case '\'': result += "\\'";  break;
                case '\n': result += "\\n";  break;
                case '\r': result += "\\r";  break;
                case '\t': result += "\\t";  break;
                default:   result += c;
            }
        }
        return result;
    }

    /** Emit a plan tree node as a JSON object to the output stream. */
    void emitTreeJSON(std::ofstream& out, const PlanNode& node) const {
        out << "{l: \"" << jsEsc(node.label) << "\", m: [";
        for (size_t i = 0; i < node.metadata.size(); i++) {
            if (i) out << ", ";
            out << "[\"" << jsEsc(node.metadata[i].first)
                << "\", \"" << jsEsc(node.metadata[i].second) << "\"]";
        }
        out << "]";

        if (!node.stats.empty()) {
            out << ", s: [";
            for (size_t i = 0; i < node.stats.size(); i++) {
                if (i) out << ", ";
                out << "\"" << jsEsc(node.stats[i]) << "\"";
            }
            out << "]";
        }

        out << ", c: [";
        for (size_t i = 0; i < node.children.size(); i++) {
            if (i) out << ", ";
            emitTreeJSON(out, node.children[i]);
        }
        out << "]}";
    }

    /** Emit an array of InfoSection as a JSON array to the output stream. */
    void emitInfoJSON(std::ofstream& out, const std::vector<InfoSection>& sections) const {
        out << "[";
        for (size_t i = 0; i < sections.size(); i++) {
            if (i) out << ", ";
            const auto& s = sections[i];
            out << "{";
            switch (s.type) {
                case InfoSection::TABLE:
                    out << "type: \"table\", title: \"" << jsEsc(s.title) << "\", rows: [";
                    for (size_t r = 0; r < s.tableRows.size(); r++) {
                        if (r) out << ", ";
                        out << "[\"" << jsEsc(s.tableRows[r].first)
                            << "\", \"" << jsEsc(s.tableRows[r].second) << "\"]";
                    }
                    out << "]";
                    break;
                case InfoSection::HTML:
                    out << "type: \"html\", title: \"" << jsEsc(s.title)
                        << "\", content: \"" << jsEsc(s.content) << "\"";
                    break;
                case InfoSection::TEXT:
                    out << "type: \"text\", title: \"" << jsEsc(s.title)
                        << "\", content: \"" << jsEsc(s.content) << "\"";
                    break;
            }
            out << "}";
        }
        out << "]";
    }

    /** Emit the title bar HTML for a rule cell. */
    void emitTitleBar(std::ofstream& out, size_t si, size_t gi, size_t ri,
                      size_t ruleNum, const std::string& name,
                      bool hasMeta, bool hasStats, bool hasInfo,
                      bool isFirst, bool isLast) const {
        out << "<div class=\"rule-title-bar\">\n";
        out << "  <div class=\"title-left\">";

        out << "<div class=\"rule-title\" onclick=\"toggleRule("
            << si << "," << gi << "," << ri << ",event)\">"
            << "<span class=\"rule-num\">" << ruleNum << ".</span>"
            << esc(name) << "</div>";

        /* Feature icons — only shown when data exists */
        if (hasMeta) {
            out << "<button class=\"tbtn icon\" id=\"metabtn-"
                << si << "-" << gi << "-" << ri
                << "\" onclick=\"toggleRuleMeta("
                << si << "," << gi << "," << ri
                << ",event)\" title=\"Toggle metadata\">"
                << "\xe2\x93\x98</button>";
        }
        if (hasStats) {
            out << "<button class=\"tbtn icon\" id=\"statbtn-"
                << si << "-" << gi << "-" << ri
                << "\" onclick=\"toggleRuleStats("
                << si << "," << gi << "," << ri
                << ",event)\" title=\"Toggle statistics\">"
                << "\xce\xa3</button>";
        }
        if (hasInfo) {
            out << "<button class=\"tbtn icon\" id=\"infobtn-"
                << si << "-" << gi << "-" << ri
                << "\" onclick=\"toggleRuleInfo("
                << si << "," << gi << "," << ri
                << ",event)\" title=\"Toggle info\">"
                << "\xe2\x8b\xae</button>";
        }

        out << "</div>";

        out << "<div class=\"title-right\">";
        out << "<button class=\"tbtn ab\" id=\"abtn-"
            << si << "-" << gi << "-" << ri
            << "\" onclick=\"setDiffA("
            << si << "," << gi << "," << ri << ",event)\">A</button>";
        out << "<button class=\"tbtn ab\" id=\"bbtn-"
            << si << "-" << gi << "-" << ri
            << "\" onclick=\"setDiffB("
            << si << "," << gi << "," << ri << ",event)\">B</button>";
        out << "<button class=\"tbtn nav\" onclick=\"navigatePrev("
            << si << "," << gi << "," << ri << ",event)\""
            << (isFirst ? " disabled" : "") << ">&#8592;</button>";
        out << "<button class=\"tbtn nav\" onclick=\"navigateNext("
            << si << "," << gi << "," << ri << ",event)\""
            << (isLast ? " disabled" : "") << ">&#8594;</button>";
        out << "</div>\n</div>\n";
    }
};

} // namespace NKqp
} // namespace NKikimr

#endif // OPTIMIZER_TRACE_HTML_H

