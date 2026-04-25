#include "kqp_new_html_log.h"

#include <sstream>

namespace NKikimr {
namespace NKqp {

bool OptimizerTraceHTML::generateHTML(const std::string& filename) const {
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
                g.ruleIndices.push_back(k); k++;
            }
            groups.push_back(g);
            j = k;
        }
        stageGroups.push_back(groups);
    }

    /* ═══════════ CSS ═══════════ */
    out << R"RAWCSS(<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<title>Optimizer Trace</title>
<style>

* { box-sizing: border-box; margin: 0; padding: 0; }
html, body { height: 100vh; overflow: hidden; }

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, sans-serif;
    font-size: 14px; background: #eaecf0; color: #24292f;
    display: flex; flex-direction: column;
}

/* ── Top bar ── */
.top-bar { flex-shrink: 0; padding: 10px 16px 8px 16px; background: #eaecf0; }
h1 { font-size: 17px; font-weight: 600; display: inline-block; margin-right: 14px; }

.controls { display: flex; align-items: center; gap: 6px 12px; flex-wrap: nowrap; }
.controls label { font-size: 12px; color: #666; cursor: pointer; white-space: nowrap; }

.ctrl-btn {
    font-size: 11px; padding: 3px 8px; border: 1px solid #bbb; border-radius: 3px;
    background: #dfe1e5; color: #333; cursor: pointer; transition: background 0.1s;
    white-space: nowrap; text-align: center;
}
.ctrl-btn:hover { background: #cfd2d7; }
.ctrl-btn.inactive { opacity: 0.4; pointer-events: none; }
.ctrl-btn.diff-active { background: #e06070; border-color: #c04050; color: #fff; }
.ctrl-btn.diff-active:hover { background: #d05060; }

.search-box {
    font-size: 12px; padding: 3px 8px; border: 1px solid #bbb; border-radius: 3px;
    width: 220px; min-width: 220px; outline: none; background: #fff; color: #24292f;
}
.search-box:focus { border-color: #6a9bff; box-shadow: 0 0 0 2px rgba(106,155,255,0.2); }
.search-info { font-size: 11px; color: #888; white-space: nowrap; }

/* ── Trace ── */
.trace {
    flex: 1; min-height: 0; display: flex; overflow-x: auto; overflow-y: hidden;
    padding: 0 16px 12px 16px; align-items: stretch;
}

/* ── Stage ── */
.stage-wrap { display: flex; align-items: stretch; margin-right: 3px; flex-shrink: 0; }
.stage-col {
    width: 28px; min-width: 28px; background: #d8dbe0; border: 1px solid #b8bcc2;
    border-radius: 4px; cursor: pointer; user-select: none; display: flex;
    flex-direction: column; align-items: center; padding-top: 6px;
    transition: background 0.1s; flex-shrink: 0;
}
.stage-col:hover { background: #cdd0d6; }
.stage-col-arrow { font-size: 9px; color: #778; margin-bottom: 4px; flex-shrink: 0; }
.stage-col-text {
    writing-mode: vertical-lr; transform: rotate(180deg); font-size: 12px;
    font-weight: 600; color: #555; white-space: nowrap; padding: 4px 0;
}
.stage-col-count {
    writing-mode: vertical-lr; transform: rotate(180deg); font-size: 10px;
    color: #999; white-space: nowrap; padding: 2px 0; margin-top: 2px;
}
.stage-wrap.stage-empty .stage-col { background: #e0e2e6; border-color: #c8cbcf; }
.stage-wrap.stage-empty .stage-col:hover { background: #d5d8dd; }
.stage-wrap.stage-empty .stage-col-text { color: #aaa; }
.stage-wrap.stage-empty .stage-col-arrow { color: #bbb; }
.stage-wrap.stage-empty .stage-hdr { background: #e0e2e6; border-color: #c8cbcf; color: #aaa; }

.stage-exp { display: flex; flex-direction: column; min-width: 0; min-height: 0; }
.stage-hdr {
    height: 30px; min-height: 30px; max-height: 30px; display: flex;
    align-items: center; padding: 0 10px; background: #d5d8de;
    border: 1px solid #b8bcc2; border-bottom: none; border-radius: 3px 3px 0 0;
    font-size: 12px; font-weight: 600; color: #444; cursor: pointer;
    user-select: none; white-space: nowrap; overflow: hidden;
    transition: background 0.1s; flex-shrink: 0;
}
.stage-hdr:hover { background: #cacdd4; }
.stage-hdr-arrow { margin-right: 6px; font-size: 8px; color: #778; }
.stage-hdr-count { font-weight: 400; color: #999; margin-left: 6px; font-size: 11px; }

.stage-toggle-btn {
    font-size: 10px; padding: 1px 5px; border: 1px solid #b8bcc2; border-radius: 3px;
    background: #c5c8cf; color: #666; cursor: pointer; transition: background 0.1s;
    margin-right: 6px; flex-shrink: 0; line-height: 1.3;
}
.stage-toggle-btn:hover { background: #b8bcc4; color: #333; }

.rules-row {
    display: flex; align-items: stretch; flex: 1; min-height: 0; overflow: hidden;
    border: 1px solid #b8bcc2; border-top: none; border-radius: 0 0 3px 3px;
    background: #d5d8de; gap: 1px;
}

/* ── Unified button ── */
.tbtn {
    display: inline-flex; align-items: center; justify-content: center;
    height: 18px; min-height: 18px; max-height: 18px;
    border: 1px solid #ccc; border-radius: 3px;
    background: #eaecef; color: #666; cursor: pointer; transition: background 0.1s;
    line-height: 1; flex-shrink: 0; padding: 0; vertical-align: middle;
}
.tbtn:hover { background: #dcdee2; color: #222; }
.tbtn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }
.tbtn.nav { width: 22px; font-size: 13px; }
.tbtn.icon { min-width: 22px; font-size: 11px; padding: 0 4px; }
.tbtn.icon.active { background: #6a9bff; border-color: #5080e0; color: #fff; }
.tbtn.ab { width: 18px; font-size: 10px; font-weight: 700; color: #888; }
.tbtn.ab:hover { background: #d8dade; color: #555; }
.tbtn.ab.selected-a { background: #4a90d9; border-color: #3a7ec5; color: #fff; }
.tbtn.ab.selected-b { background: #d94a7a; border-color: #c53a6a; color: #fff; }

/* ── Rule cell ── */
.rule-cell {
    display: flex; flex-direction: column; overflow: hidden; flex-shrink: 0; border: none;
}
.rule-cell.expanded {
    width: 520px; min-width: 520px; max-width: 520px; background: #fff;
}
.rule-cell.expanded .rule-col-wrap { display: none; }
.rule-cell.expanded .rule-exp-wrap {
    display: flex; flex-direction: column; flex: 1; min-height: 0;
}
.rule-cell.collapsed {
    width: 16px; min-width: 16px; max-width: 16px; background: #eef0f3; cursor: pointer;
}
.rule-cell.collapsed:hover { background: #e4e6ea; }
.rule-cell.collapsed .rule-col-wrap {
    display: flex; flex-direction: column; height: 100%; align-items: center;
}
.rule-cell.collapsed .rule-exp-wrap { display: none; }

/* ── Rule title bar ── */
.rule-title-bar {
    display: flex; align-items: center; height: 28px; min-height: 28px; max-height: 28px;
    border-bottom: 1px solid #e0e2e6; background: #f2f4f6; flex-shrink: 0; padding: 0 6px 0 0;
}
.rule-title {
    padding: 0 6px; font-size: 12px; font-weight: 400; color: #24292f;
    cursor: pointer; user-select: none; white-space: nowrap; overflow: hidden;
    text-overflow: ellipsis; line-height: 28px;
}
.rule-num { font-weight: 400; color: #aaa; font-size: 10px; margin-right: 3px; }

.title-left {
    display: flex; align-items: center; flex: 1; min-width: 0; gap: 3px; overflow: hidden;
}
.title-right {
    display: flex; align-items: center; gap: 3px; flex-shrink: 0; margin-left: 4px;
}

.rule-col-wrap { padding-top: 28px; }
.rule-col-num {
    font-size: 8px; color: #bbb; flex-shrink: 0; text-align: center;
    width: 100%; padding-bottom: 2px;
}
.rule-col-text {
    writing-mode: vertical-lr; transform: rotate(180deg); font-size: 12px;
    font-weight: 400; color: #555; white-space: nowrap; padding: 4px 1px;
}

/* ── Rule content ── */
.rule-content {
    flex: 1; min-height: 0; display: flex; flex-direction: column; overflow: hidden;
}
.rule-tree-wrap { flex: 1; overflow: auto; min-height: 0; padding: 8px 12px; }

/* ── Info panel ── */
.rule-info-panel {
    display: none; border-top: 2px solid #d0d2d6; overflow: auto;
    max-height: 45%; flex-shrink: 0; background: #fafbfc; padding: 8px 12px;
}
.rule-info-panel.visible { display: block; }
.info-section { margin-bottom: 10px; }
.info-section-title {
    font-size: 11px; font-weight: 600; color: #555; margin-bottom: 4px;
    padding-bottom: 2px; border-bottom: 1px solid #e8eaed;
}
.info-table { font-size: 12px; border-collapse: collapse; width: 100%; }
.info-table td { padding: 2px 8px 2px 0; vertical-align: top; border-bottom: 1px solid #f0f1f3; }
.info-table td:first-child { font-weight: 500; color: #555; white-space: nowrap; width: 1%; }
.info-text {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px; white-space: pre-wrap; line-height: 1.5; color: #333;
}
.info-html { font-size: 12px; line-height: 1.5; }

/* ── Tree ── */
.tree-root { white-space: nowrap; min-width: min-content; }

/* Stats header */
.tree-stats-header {
    display: none; padding: 0 0 4px 0; margin-bottom: 4px; border-bottom: 1px solid #e8eaed;
}
.tree-stats-header.visible { display: flex; }
.stat-header-cell {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 10px; font-weight: 600; color: #888;
    width: 72px; min-width: 72px; text-align: right; padding-right: 8px; flex-shrink: 0;
}

/* Each tree "row" is a flex row */
.tree-node-row {
    display: flex; align-items: center; min-height: 22px; padding: 0;
    border-radius: 2px;
}
.tree-node-row:hover { background: rgba(0,0,0,0.025); }

.stat-val-cell {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px; color: #888; width: 72px; min-width: 72px;
    text-align: right; padding-right: 8px; flex-shrink: 0; display: none;
}
.stats-active .stat-val-cell { display: block; }
.stats-active .tree-stats-header { display: flex; }

/* ── CSS connector cells ──
   Each .tree-c is a 20x22 inline-block with positioned lines inside. */
.tree-c {
    position: relative; display: inline-block;
    width: 20px; min-width: 20px; height: 22px;
    flex-shrink: 0; pointer-events: none;
}
/* Vertical line: full height */
.tree-c.pipe::before, .tree-c.branch::before {
    content: ''; position: absolute; left: 9px; top: 0; bottom: 0;
    border-left: 1px solid #d0d2d6;
}
/* Vertical line: top to center only (L-shape) */
.tree-c.elbow::before {
    content: ''; position: absolute; left: 9px; top: 0; height: 11px;
    border-left: 1px solid #d0d2d6;
}
/* Horizontal branch */
.tree-c.branch::after, .tree-c.elbow::after {
    content: ''; position: absolute; left: 9px; top: 11px; width: 11px;
    border-top: 1px solid #d0d2d6;
}

/* Connector cells in metadata rows (shorter height) */
.tree-cm {
    position: relative; display: inline-block;
    width: 20px; min-width: 20px; height: 18px;
    flex-shrink: 0; pointer-events: none;
}
.tree-cm.pipe::before {
    content: ''; position: absolute; left: 9px; top: 0; bottom: 0;
    border-left: 1px solid #d0d2d6;
}

.tree-toggle {
    display: inline-flex; align-items: center; justify-content: center;
    width: 16px; min-width: 16px; height: 16px; font-size: 8px; color: #999;
    cursor: pointer; flex-shrink: 0; border-radius: 2px;
    transition: color 0.1s; user-select: none;
}
.tree-toggle:hover { color: #444; background: rgba(0,0,0,0.06); }
.tree-toggle.leaf { cursor: default; color: transparent; pointer-events: none; }

.tree-label {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 12px; line-height: 22px; color: #24292f; padding-left: 2px;
    white-space: nowrap;
}
.tree-label.clickable { cursor: pointer; }
.tree-label.clickable:hover { text-decoration: underline; text-decoration-color: #ccc; }

/* Per-node metadata row */
.tree-meta-row {
    display: flex; align-items: center; min-height: 18px;
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px; line-height: 18px;
}
.tree-meta-pad { display: inline-block; width: 18px; flex-shrink: 0; }
.tree-meta-key { color: #888; font-weight: 500; white-space: nowrap; }
.tree-meta-val { color: #555; white-space: nowrap; }

.tree-meta-block { }
.tree-meta-block.collapsed { display: none; }

.tree-children-block { }

/* ── Diff coloring — flat, no nesting accumulation ── */
.diff-mark-add > .tree-node-row { background: #d4edda; }
.diff-mark-add > .tree-node-row .tree-label { color: #22863a; }
.diff-mark-rem > .tree-node-row { background: #f8d7da; }
.diff-mark-rem > .tree-node-row .tree-label { color: #cb2431; text-decoration: line-through; text-decoration-color: #cb2431; }

/* ── Group cell ── */
.group-cell {
    display: flex; flex-direction: column; overflow: hidden; flex-shrink: 0; border: none;
}
.group-cell.g-collapsed {
    width: 24px; min-width: 24px; max-width: 24px; background: #d0d3db; cursor: pointer;
}
.group-cell.g-collapsed:hover { background: #c5c8d2; }
.group-cell.g-collapsed .group-col-wrap {
    display: flex; flex-direction: column; height: 100%; align-items: center; padding-top: 28px;
}
.group-cell.g-collapsed .group-exp-wrap { display: none; }
.group-col-count {
    font-size: 8px; color: #777; flex-shrink: 0; font-weight: 600;
    text-align: center; width: 100%; padding-bottom: 2px;
}
.group-col-text {
    writing-mode: vertical-lr; transform: rotate(180deg); font-size: 11px;
    font-weight: 600; color: #555; white-space: nowrap; padding: 4px 2px;
}
.group-cell.g-expanded { background: transparent; }
.group-cell.g-expanded .group-col-wrap { display: none; }
.group-cell.g-expanded .group-exp-wrap {
    display: flex; flex-direction: column; flex: 1; min-height: 0;
}
.group-title-bar {
    display: flex; align-items: center; height: 14px; min-height: 14px;
    max-height: 14px; padding: 0 6px; background: #cdd0d8; flex-shrink: 0;
}
.group-toggle-btn {
    display: inline-flex; align-items: center; justify-content: center;
    width: 22px; min-width: 22px; font-size: 8px; padding: 0;
    border: none; background: none; color: #777; cursor: pointer;
    transition: color 0.1s; flex-shrink: 0; line-height: 14px;
}
.group-toggle-btn:hover { color: #333; }
.group-title {
    flex: 1; font-size: 11px; font-weight: 400; color: #666; cursor: pointer;
    user-select: none; white-space: nowrap; overflow: hidden;
    text-overflow: ellipsis; padding: 0 4px; line-height: 14px;
}
.group-nav { display: flex; gap: 2px; flex-shrink: 0; }
.group-nav-btn {
    display: inline-flex; align-items: center; justify-content: center;
    width: 18px; height: 12px; font-size: 10px; border: 1px solid #b4b8c0;
    border-radius: 2px; background: #bfc3cb; color: #666; cursor: pointer;
    transition: background 0.1s; line-height: 1; padding: 0; flex-shrink: 0;
}
.group-nav-btn:hover { background: #b0b5be; color: #333; }
.group-nav-btn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }

.group-rules-area {
    display: flex; flex: 1; min-height: 0; align-items: stretch;
    overflow: hidden; background: #d5d8de; gap: 1px;
}

/* Inside groups: uniform 12px button height */
.group-rules-area .rule-title-bar {
    height: 14px; min-height: 14px; max-height: 14px; padding: 0 4px 0 6px;
}
.group-rules-area .rule-title { font-size: 11px; line-height: 14px; padding: 0 4px 0 0; }
.group-rules-area .rule-col-text { font-size: 11px; }
.group-rules-area .rule-num {
    display: inline-block; min-width: 22px; text-align: right;
    font-size: 9px; padding-right: 4px;
}
.group-rules-area .title-left { gap: 2px; }
.group-rules-area .title-right { gap: 2px; margin-left: 2px; }
.group-rules-area .tbtn {
    height: 12px; min-height: 12px; max-height: 12px; border-radius: 2px; font-size: 8px;
}
.group-rules-area .tbtn.nav { width: 14px; font-size: 9px; }
.group-rules-area .tbtn.icon { min-width: 14px; font-size: 8px; padding: 0 2px; }
.group-rules-area .tbtn.ab { width: 14px; font-size: 7px; }
.group-rules-area .rule-col-wrap { padding-top: 14px; }

/* Flash */
@keyframes cellFlash { 0%{filter:brightness(1.5)} 100%{filter:brightness(1)} }
.flash-cell { animation: cellFlash 0.2s ease-out; }
mark { background: #fff176; color: #24292f; border-radius: 2px; padding: 0 1px; }

/* ═══════ Dark mode ═══════ */
body.darkmode { background: #1a1c22; color: #d4d7de; }
body.darkmode .top-bar { background: #1a1c22; }
body.darkmode .ctrl-btn { background: #2e3038; border-color: #4a4e58; color: #bbb; }
body.darkmode .ctrl-btn:hover { background: #383c44; }
body.darkmode .ctrl-btn.diff-active { background: #b04050; border-color: #903040; color: #fff; }
body.darkmode .ctrl-btn.diff-active:hover { background: #a03848; }
body.darkmode .controls label { color: #888; }
body.darkmode .search-box { background: #262930; border-color: #4a4e58; color: #d4d7de; }
body.darkmode .search-box:focus { border-color: #6a9bff; box-shadow: 0 0 0 2px rgba(106,155,255,0.15); }

body.darkmode .stage-col { background: #282b32; border-color: #3e4248; }
body.darkmode .stage-col:hover { background: #303440; }
body.darkmode .stage-col-text { color: #999; }
body.darkmode .stage-col-arrow { color: #777; }
body.darkmode .stage-col-count { color: #666; }
body.darkmode .stage-wrap.stage-empty .stage-col { background: #222428; border-color: #363840; }
body.darkmode .stage-wrap.stage-empty .stage-col:hover { background: #2a2c32; }
body.darkmode .stage-wrap.stage-empty .stage-col-text { color: #555; }
body.darkmode .stage-wrap.stage-empty .stage-hdr { background: #222428; border-color: #363840; color: #555; }
body.darkmode .stage-hdr { background: #252830; border-color: #3e4248; color: #b0b4bc; }
body.darkmode .stage-hdr:hover { background: #2c3038; }
body.darkmode .stage-hdr-count { color: #777; }
body.darkmode .stage-hdr-arrow { color: #777; }
body.darkmode .stage-toggle-btn { background: #2e3038; border-color: #4a4e58; color: #999; }
body.darkmode .stage-toggle-btn:hover { background: #383c44; color: #ddd; }
body.darkmode .rules-row { border-color: #3e4248; background: #252830; }

body.darkmode .rule-cell.expanded { background: #1e2028; }
body.darkmode .rule-cell.collapsed { background: #24262c; }
body.darkmode .rule-cell.collapsed:hover { background: #2a2c34; }
body.darkmode .rule-col-text { color: #999; }
body.darkmode .rule-col-num { color: #555; }
body.darkmode .rule-title-bar { background: #282b34; border-bottom-color: #363840; }
body.darkmode .rule-title { color: #ccd0d8; }
body.darkmode .rule-num { color: #666; }

body.darkmode .tbtn { background: #2e3038; border-color: #4a4e58; color: #888; }
body.darkmode .tbtn:hover { background: #383c44; color: #ddd; }
body.darkmode .tbtn.icon.active { background: #4a7fd9; border-color: #3a6cc5; color: #fff; }
body.darkmode .tbtn.ab { color: #777; }
body.darkmode .tbtn.ab:hover { background: #383c44; color: #aaa; }
body.darkmode .tbtn.ab.selected-a { background: #3a70c0; border-color: #2e60b0; color: #fff; }
body.darkmode .tbtn.ab.selected-b { background: #c03a60; border-color: #a82e50; color: #fff; }

body.darkmode .rule-info-panel { background: #1c1e24; border-top-color: #4a4e58; }
body.darkmode .info-section-title { color: #999; border-bottom-color: #363840; }
body.darkmode .info-table td { border-bottom-color: #2a2c34; }
body.darkmode .info-table td:first-child { color: #888; }
body.darkmode .info-text { color: #bbb; }

body.darkmode .tree-node-row:hover { background: rgba(255,255,255,0.025); }
body.darkmode .tree-c.pipe::before, body.darkmode .tree-c.branch::before,
body.darkmode .tree-c.elbow::before { border-left-color: #3e4248; }
body.darkmode .tree-c.branch::after, body.darkmode .tree-c.elbow::after { border-top-color: #3e4248; }
body.darkmode .tree-cm.pipe::before { border-left-color: #3e4248; }
body.darkmode .tree-toggle { color: #777; }
body.darkmode .tree-toggle:hover { color: #ccc; background: rgba(255,255,255,0.06); }
body.darkmode .tree-label { color: #d4d7de; }
body.darkmode .tree-label.clickable:hover { text-decoration-color: #555; }
body.darkmode .tree-meta-key { color: #777; }
body.darkmode .tree-meta-val { color: #aaa; }
body.darkmode .stat-val-cell { color: #777; }
body.darkmode .stat-header-cell { color: #666; }
body.darkmode .tree-stats-header { border-bottom-color: #363840; }
body.darkmode mark { background: #665d20; color: #f0e8b0; }

body.darkmode .diff-mark-add > .tree-node-row { background: #1a3328; }
body.darkmode .diff-mark-add > .tree-node-row .tree-label { color: #56d364; }
body.darkmode .diff-mark-rem > .tree-node-row { background: #3d1f24; }
body.darkmode .diff-mark-rem > .tree-node-row .tree-label { color: #f85149; }

body.darkmode .group-cell.g-collapsed { background: #282c38; }
body.darkmode .group-cell.g-collapsed:hover { background: #303448; }
body.darkmode .group-col-text { color: #7882a0; }
body.darkmode .group-col-count { color: #6a7090; }
body.darkmode .group-title-bar { background: #262a36; }
body.darkmode .group-title { color: #7080a0; }
body.darkmode .group-toggle-btn { color: #7080a0; }
body.darkmode .group-toggle-btn:hover { color: #ccc; }
body.darkmode .group-nav-btn { background: #2a3040; border-color: #404860; color: #7080a0; }
body.darkmode .group-nav-btn:hover { background: #344058; color: #ccc; }
body.darkmode .group-rules-area { background: #252830; }

</style>
<script>
)RAWCSS";

    /* ═══════════ JS data ═══════════ */
    size_t stageCount = stages_.size();
    out << "var stageCount=" << stageCount << ";\n";
    out << "var stageRuleCounts=[";
    for (size_t i = 0; i < stageCount; i++) { if (i) out << ","; out << stages_[i].rules.size(); }
    out << "];\n";
    out << "var stageNames=[";
    for (size_t i = 0; i < stageCount; i++) { if (i) out << ","; out << "\"" << jsEsc(stages_[i].name) << "\""; }
    out << "];\n";
    out << "var groups=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t g = 0; g < stageGroups[i].size(); g++) {
            if (g) out << ",";
            const auto& grp = stageGroups[i][g];
            out << "{name:\"" << jsEsc(grp.name) << "\",ri:[";
            for (size_t k = 0; k < grp.ruleIndices.size(); k++) { if (k) out << ","; out << grp.ruleIndices[k]; }
            out << "]}";
        }
        out << "]";
    }
    out << "];\n";
    out << "var ruleNames=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t j = 0; j < stages_[i].rules.size(); j++) { if (j) out << ","; out << "\"" << jsEsc(stages_[i].rules[j].ruleName) << "\""; }
        out << "]";
    }
    out << "];\n";
    out << "var statColumns=[";
    for (size_t i = 0; i < statColumns_.size(); i++) { if (i) out << ","; out << "\"" << jsEsc(statColumns_[i]) << "\""; }
    out << "];\nvar statColCount=" << statColumns_.size() << ";\n";
    out << "var planTrees=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t j = 0; j < stages_[i].rules.size(); j++) { if (j) out << ","; emitTreeJSON(out, stages_[i].rules[j].tree); }
        out << "]";
    }
    out << "];\n";
    out << "var ruleInfo=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t j = 0; j < stages_[i].rules.size(); j++) { if (j) out << ","; emitInfoJSON(out, stages_[i].rules[j].info); }
        out << "]";
    }
    out << "];\n";
    out << "var ruleHasMeta=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t j = 0; j < stages_[i].rules.size(); j++) { if (j) out << ","; out << (stages_[i].rules[j].tree.hasAnyMeta()?1:0); }
        out << "]";
    }
    out << "];\n";
    out << "var ruleHasStats=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t j = 0; j < stages_[i].rules.size(); j++) { if (j) out << ","; out << (stages_[i].rules[j].tree.hasAnyStats()?1:0); }
        out << "]";
    }
    out << "];\n";
    out << "var ruleHasInfo=[";
    for (size_t i = 0; i < stageCount; i++) {
        if (i) out << ","; out << "[";
        for (size_t j = 0; j < stages_[i].rules.size(); j++) { if (j) out << ","; out << (!stages_[i].rules[j].info.empty()?1:0); }
        out << "]";
    }
    out << "];\n\n";

    /* ═══════════ JS logic ═══════════ */
    out << R"RAWJS(

var allRules=[];
for(var si=0;si<stageCount;si++)
    for(var gi=0;gi<groups[si].length;gi++){
        var grp=groups[si][gi];
        for(var ri=0;ri<grp.ri.length;ri++)
            allRules.push({stageIdx:si,groupIdx:gi,ruleIdx:ri,rawRuleIdx:grp.ri[ri]});
    }
function findFlatIndex(si,gi,ri){
    for(var i=0;i<allRules.length;i++)
        if(allRules[i].stageIdx===si&&allRules[i].groupIdx===gi&&allRules[i].ruleIdx===ri)return i;
    return -1;
}

/* ── State ── */
var stageOpen=[], groupOpen=[], ruleOpen=[];
var ruleMetaAll=[], ruleStats=[], ruleInfoOpen=[];
var searchActive=false;

for(var si=0;si<stageCount;si++){
    stageOpen.push(stageRuleCounts[si]>0);
    groupOpen.push([]); ruleOpen.push([]);
    ruleMetaAll.push([]); ruleStats.push([]); ruleInfoOpen.push([]);
    for(var gi=0;gi<groups[si].length;gi++){
        var cnt=groups[si][gi].ri.length, isMulti=cnt>1;
        groupOpen[si].push(!isMulti);
        ruleOpen[si].push([]); ruleMetaAll[si].push([]);
        ruleStats[si].push([]); ruleInfoOpen[si].push([]);
        for(var ri=0;ri<cnt;ri++){
            ruleOpen[si][gi].push(isMulti?(ri===cnt-1):true);
            ruleMetaAll[si][gi].push(false);
            ruleStats[si][gi].push(false);
            ruleInfoOpen[si][gi].push(false);
        }
    }
}

/* ── HTML helpers ── */
function htmlEscape(t){
    return t.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')
            .replace(/"/g,'&quot;').replace(/'/g,'&#39;');
}
function htmlHighlight(text,query){
    if(!query)return htmlEscape(text);
    var lt=text.toLowerCase(),lq=query.toLowerCase(),r='',p=0;
    while(p<text.length){
        var idx=lt.indexOf(lq,p);
        if(idx===-1){r+=htmlEscape(text.substring(p));break;}
        r+=htmlEscape(text.substring(p,idx));
        r+='<mark>'+htmlEscape(text.substring(idx,idx+query.length))+'</mark>';
        p=idx+query.length;
    }
    return r;
}

function renderTree(container, tree, ruleKey, showMeta, showStats, query) {
    var hdr = '';
    if (statColCount > 0) {
        hdr = '<div class="tree-stats-header' + (showStats ? ' visible' : '') +
              '" id="stathdr-' + ruleKey + '">';
        for (var c = 0; c < statColCount; c++)
            hdr += '<span class="stat-header-cell">' + htmlEscape(statColumns[c]) + '</span>';
        hdr += '<span style="width:16px;flex-shrink:0;display:inline-block"></span>';
        hdr += '<span style="font-size:10px;color:#aaa;font-weight:600;padding-left:2px">Plan</span>';
        hdr += '</div>';
    }
    container.innerHTML = hdr +
        '<div class="tree-root' + (showStats ? ' stats-active' : '') +
        '" id="treeroot-' + ruleKey + '">' +
        renderNode(tree, ruleKey, '0', showMeta, showStats, query, [], true) +
        '</div>';
}

function renderNode(node, rk, pid, showMeta, showStats, q, prefix, isRoot) {
    if (!node || !node.l) return '';

    var hasKids = node.c && node.c.length > 0;
    var hasMeta = node.m && node.m.length > 0;
    var nid = 'tn-' + rk + '-' + pid;

    var html = '<div id="' + nid + '">';

    html += '<div class="tree-node-row">';

    if (statColCount > 0) {
        for (var c = 0; c < statColCount; c++) {
            var val = (node.s && c < node.s.length && node.s[c]) ? htmlHighlight(node.s[c], q) : '';
            html += '<span class="stat-val-cell">' + val + '</span>';
        }
    }

    for (var i = 0; i < prefix.length; i++)
        html += '<span class="tree-c ' + prefix[i] + '"></span>';

    if (hasKids)
        html += '<span class="tree-toggle" id="' + nid + '-tog" onclick="treeToggle(event,\x27' + nid + '\x27)">▼</span>';
    else
        html += '<span class="tree-toggle leaf">•</span>';

    if (hasMeta)
        html += '<span class="tree-label clickable" onclick="treeMetaToggle(event,\x27' + nid + '\x27)">' + htmlHighlight(node.l, q) + '</span>';
    else
        html += '<span class="tree-label">' + htmlHighlight(node.l, q) + '</span>';

    html += '</div>';

    if (hasMeta) {
        html += '<div class="tree-meta-block' + (showMeta ? '' : ' collapsed') + '" id="' + nid + '-meta">';
        for (var i = 0; i < node.m.length; i++) {
            html += '<div class="tree-meta-row">';
            if (statColCount > 0)
                for (var c = 0; c < statColCount; c++)
                    html += '<span class="stat-val-cell"></span>';
            for (var j = 0; j < prefix.length; j++)
                html += '<span class="tree-cm ' + prefix[j] + '"></span>';
            if (hasKids)
                html += '<span class="tree-cm pipe"></span>';
            else
                html += '<span class="tree-cm"></span>';
            html += '<span class="tree-meta-pad"></span>';
            html += '<span class="tree-meta-key">' + htmlHighlight(node.m[i][0], q) + ':</span> ';
            html += '<span class="tree-meta-val">' + htmlHighlight(node.m[i][1], q) + '</span>';
            html += '</div>';
        }
        html += '</div>';
    }

    if (hasKids) {
        html += '<div class="tree-children-block" id="' + nid + '-ch">';
        for (var i = 0; i < node.c.length; i++) {
            var isLast = (i === node.c.length - 1);
            var childPrefix = prefix.slice();
            childPrefix.push(isLast ? 'space' : 'pipe');
            html += renderNodeWithBranch(node.c[i], rk, pid + '-' + i, showMeta, showStats, q,
                                          prefix, childPrefix, isLast);
        }
        html += '</div>';
    }

    html += '</div>';
    return html;
}

function renderNodeWithBranch(node, rk, pid, showMeta, showStats, q,
                               parentPrefix, childPrefix, isLast) {
    if (!node || !node.l) return '';

    var hasKids = node.c && node.c.length > 0;
    var hasMeta = node.m && node.m.length > 0;
    var nid = 'tn-' + rk + '-' + pid;
    var branchType = isLast ? 'elbow' : 'branch';

    var html = '<div id="' + nid + '">';

    html += '<div class="tree-node-row">';

    if (statColCount > 0)
        for (var c = 0; c < statColCount; c++) {
            var val = (node.s && c < node.s.length && node.s[c]) ? htmlHighlight(node.s[c], q) : '';
            html += '<span class="stat-val-cell">' + val + '</span>';
        }

    for (var i = 0; i < parentPrefix.length; i++)
        html += '<span class="tree-c ' + parentPrefix[i] + '"></span>';
    html += '<span class="tree-c ' + branchType + '"></span>';

    if (hasKids)
        html += '<span class="tree-toggle" id="' + nid + '-tog" onclick="treeToggle(event,\x27' + nid + '\x27)">▼</span>';
    else
        html += '<span class="tree-toggle leaf">•</span>';

    if (hasMeta)
        html += '<span class="tree-label clickable" onclick="treeMetaToggle(event,\x27' + nid + '\x27)">' + htmlHighlight(node.l, q) + '</span>';
    else
        html += '<span class="tree-label">' + htmlHighlight(node.l, q) + '</span>';

    html += '</div>';

    if (hasMeta) {
        var myContType = isLast ? 'space' : 'pipe';

        html += '<div class="tree-meta-block' + (showMeta ? '' : ' collapsed') + '" id="' + nid + '-meta">';
        for (var i = 0; i < node.m.length; i++) {
            html += '<div class="tree-meta-row">';
            if (statColCount > 0)
                for (var c = 0; c < statColCount; c++)
                    html += '<span class="stat-val-cell"></span>';
            for (var j = 0; j < parentPrefix.length; j++)
                html += '<span class="tree-cm ' + parentPrefix[j] + '"></span>';
            html += '<span class="tree-cm ' + myContType + '"></span>';
            if (hasKids)
                html += '<span class="tree-cm pipe"></span>';
            else
                html += '<span class="tree-cm"></span>';
            html += '<span class="tree-meta-pad"></span>';
            html += '<span class="tree-meta-key">' + htmlHighlight(node.m[i][0], q) + ':</span> ';
            html += '<span class="tree-meta-val">' + htmlHighlight(node.m[i][1], q) + '</span>';
            html += '</div>';
        }
        html += '</div>';
    }

    if (hasKids) {
        html += '<div class="tree-children-block" id="' + nid + '-ch">';
        for (var i = 0; i < node.c.length; i++) {
            var childIsLast = (i === node.c.length - 1);
            var grandPrefix = childPrefix.slice();
            grandPrefix.push(childIsLast ? 'space' : 'pipe');
            html += renderNodeWithBranch(node.c[i], rk, pid + '-' + i, showMeta, showStats, q,
                                          childPrefix, grandPrefix, childIsLast);
        }
        html += '</div>';
    }

    html += '</div>';
    return html;
}

function treeToggle(event, nid) {
    event.stopPropagation();
    var ch = document.getElementById(nid + '-ch');
    var tog = document.getElementById(nid + '-tog');
    if (!ch || !tog) return;
    if (ch.style.display === 'none') { ch.style.display = ''; tog.textContent = '▼'; }
    else { ch.style.display = 'none'; tog.textContent = '▶'; }
}

function treeMetaToggle(event, nid) {
    event.stopPropagation();
    var m = document.getElementById(nid + '-meta');
    if (m) m.classList.toggle('collapsed');
}


/* ── Rule actions ── */
function rerenderRuleTree(si, gi, ri, query) {
    var rawIdx = groups[si][gi].ri[ri];
    var rk = si + '-' + gi + '-' + ri;
    var c = document.getElementById('tree-' + rk);
    if (!c) return;
    renderTree(c, planTrees[si][rawIdx], rk,
               ruleMetaAll[si][gi][ri], ruleStats[si][gi][ri], query || '');
}

function toggleRuleMeta(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleMetaAll[si][gi][ri] = !ruleMetaAll[si][gi][ri];
    rerenderRuleTree(si, gi, ri, currentSearchQuery());
    var b = document.getElementById('metabtn-' + si + '-' + gi + '-' + ri);
    if (b) b.classList.toggle('active', ruleMetaAll[si][gi][ri]);
}

function toggleRuleStats(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleStats[si][gi][ri] = !ruleStats[si][gi][ri];
    rerenderRuleTree(si, gi, ri, currentSearchQuery());
    var b = document.getElementById('statbtn-' + si + '-' + gi + '-' + ri);
    if (b) b.classList.toggle('active', ruleStats[si][gi][ri]);
}

function toggleRuleInfo(si, gi, ri, ev) {
    if (ev) ev.stopPropagation();
    ruleInfoOpen[si][gi][ri] = !ruleInfoOpen[si][gi][ri];
    var p = document.getElementById('info-' + si + '-' + gi + '-' + ri);
    var b = document.getElementById('infobtn-' + si + '-' + gi + '-' + ri);
    if (p) p.classList.toggle('visible', ruleInfoOpen[si][gi][ri]);
    if (b) b.classList.toggle('active', ruleInfoOpen[si][gi][ri]);
}

function renderInfoPanel(container, sections) {
    if (!sections || !sections.length) {
        container.innerHTML = '<div style="color:#999;font-size:11px;font-style:italic">No info</div>'; return;
    }
    var h = '';
    for (var i = 0; i < sections.length; i++) {
        var s = sections[i];
        h += '<div class="info-section">';
        if (s.title) h += '<div class="info-section-title">' + htmlEscape(s.title) + '</div>';
        if (s.type === 'table' && s.rows) {
            h += '<table class="info-table">';
            for (var r = 0; r < s.rows.length; r++)
                h += '<tr><td>' + htmlEscape(s.rows[r][0]) + '</td><td>' + htmlEscape(s.rows[r][1]) + '</td></tr>';
            h += '</table>';
        } else if (s.type === 'html') h += '<div class="info-html">' + s.content + '</div>';
        else if (s.type === 'text') h += '<div class="info-text">' + htmlEscape(s.content) + '</div>';
        h += '</div>';
    }
    container.innerHTML = h;
}


/* ── Display ── */
function updateStageDisplay(si) {
    var c = document.getElementById('stage-col-' + si), e = document.getElementById('stage-exp-' + si);
    if (!c || !e) return;
    c.style.display = stageOpen[si] ? 'none' : 'flex';
    e.style.display = stageOpen[si] ? 'flex' : 'none';
}
function updateGroupDisplay(si, gi) {
    if (groups[si][gi].ri.length <= 1) return;
    var el = document.getElementById('group-' + si + '-' + gi);
    if (el) el.className = 'group-cell ' + (groupOpen[si][gi] ? 'g-expanded' : 'g-collapsed');
}
function updateRuleDisplay(si, gi, ri) {
    var el = document.getElementById('rule-' + si + '-' + gi + '-' + ri);
    if (el) el.className = 'rule-cell ' + (ruleOpen[si][gi][ri] ? 'expanded' : 'collapsed');
}
function updateAllDisplays() {
    for (var si = 0; si < stageCount; si++) {
        updateStageDisplay(si);
        for (var gi = 0; gi < groups[si].length; gi++) {
            updateGroupDisplay(si, gi);
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) updateRuleDisplay(si, gi, ri);
        }
    }
}

/* Flash */
var justExpandedByNav = null;
function flashRuleCell(si, gi, ri) {
    var id = 'rule-' + si + '-' + gi + '-' + ri;
    if (justExpandedByNav === id) { justExpandedByNav = null; return; }
    justExpandedByNav = null;
    var el = document.getElementById(id); if (!el) return;
    el.classList.remove('flash-cell'); void el.offsetWidth; el.classList.add('flash-cell');
}

/* ── Stage/Group/Rule interactions ── */
function toggleStage(si) { stageOpen[si] = !stageOpen[si]; updateStageDisplay(si); }
function toggleStageRules(si, ev) {
    ev.stopPropagation(); if (!stageRuleCounts[si]) return;
    var any = false;
    outer: for (var gi = 0; gi < groups[si].length; gi++) {
        var c = groups[si][gi].ri.length;
        if (c > 1) { if (groupOpen[si][gi]) for (var ri = 0; ri < c; ri++) if (ruleOpen[si][gi][ri]) { any = true; break outer; } }
        else if (ruleOpen[si][gi][0]) { any = true; break; }
    }
    for (var gi = 0; gi < groups[si].length; gi++) {
        var c = groups[si][gi].ri.length;
        if (any) { if (c > 1) groupOpen[si][gi] = false; for (var ri = 0; ri < c; ri++) ruleOpen[si][gi][ri] = false; }
        else { if (c > 1) groupOpen[si][gi] = true; for (var ri = 0; ri < c; ri++) ruleOpen[si][gi][ri] = true; }
        updateGroupDisplay(si, gi);
        for (var ri = 0; ri < c; ri++) updateRuleDisplay(si, gi, ri);
    }
}
function toggleGroup(si, gi, ev) {
    if (ev) ev.stopPropagation();
    var c = groups[si][gi].ri.length; if (c <= 1) return;
    groupOpen[si][gi] = !groupOpen[si][gi];
    if (groupOpen[si][gi])
        for (var ri = 0; ri < c; ri++) { ruleOpen[si][gi][ri] = (ri === c - 1); updateRuleDisplay(si, gi, ri); }
    updateGroupDisplay(si, gi);
}
function toggleGroupRules(si, gi, ev) {
    ev.stopPropagation();
    var c = groups[si][gi].ri.length;
    if (c <= 1 || !groupOpen[si][gi]) return;
    var any = false;
    for (var ri = 0; ri < c; ri++) if (ruleOpen[si][gi][ri]) { any = true; break; }
    for (var ri = 0; ri < c; ri++) { ruleOpen[si][gi][ri] = !any; updateRuleDisplay(si, gi, ri); }
}
function onCollapsedGroupClick(si, gi, ev) { ev.stopPropagation(); if (!groupOpen[si][gi]) toggleGroup(si, gi, null); }
function toggleRule(si, gi, ri, ev) { if (ev) ev.stopPropagation(); ruleOpen[si][gi][ri] = !ruleOpen[si][gi][ri]; updateRuleDisplay(si, gi, ri); }
function onCollapsedRuleClick(si, gi, ri, ev) { ev.stopPropagation(); if (!ruleOpen[si][gi][ri]) toggleRule(si, gi, ri, null); }

/* ── Navigation ── */
function ensureRuleVisible(si, gi, ri) {
    var v = true;
    if (!stageOpen[si]) { stageOpen[si] = true; updateStageDisplay(si); v = false; }
    var c = groups[si][gi].ri.length;
    if (c > 1 && !groupOpen[si][gi]) {
        groupOpen[si][gi] = true;
        for (var r = 0; r < c; r++) { ruleOpen[si][gi][r] = (r === c - 1); updateRuleDisplay(si, gi, r); }
        updateGroupDisplay(si, gi);
        if (ri !== c - 1) v = false;
    }
    if (!ruleOpen[si][gi][ri]) { ruleOpen[si][gi][ri] = true; updateRuleDisplay(si, gi, ri); v = false; }
    return v;
}
function scrollToFlash(si, gi, ri, v) {
    var id = 'rule-' + si + '-' + gi + '-' + ri;
    var el = document.getElementById(id); if (!el) return;
    if (!v) justExpandedByNav = id;
    el.scrollIntoView({ behavior: 'smooth', inline: 'center', block: 'nearest' });
    setTimeout(function() { flashRuleCell(si, gi, ri); }, 80);
}
function navigatePrev(si,gi,ri,ev) { ev.stopPropagation(); var i=findFlatIndex(si,gi,ri); if(i<=0)return; var t=allRules[i-1]; scrollToFlash(t.stageIdx,t.groupIdx,t.ruleIdx,ensureRuleVisible(t.stageIdx,t.groupIdx,t.ruleIdx)); }
function navigateNext(si,gi,ri,ev) { ev.stopPropagation(); var i=findFlatIndex(si,gi,ri); if(i>=allRules.length-1)return; var t=allRules[i+1]; scrollToFlash(t.stageIdx,t.groupIdx,t.ruleIdx,ensureRuleVisible(t.stageIdx,t.groupIdx,t.ruleIdx)); }
function groupNavigatePrev(si,gi,ev) { ev.stopPropagation(); var i=findFlatIndex(si,gi,0); if(i<=0)return; var t=allRules[i-1]; scrollToFlash(t.stageIdx,t.groupIdx,t.ruleIdx,ensureRuleVisible(t.stageIdx,t.groupIdx,t.ruleIdx)); }
function groupNavigateNext(si,gi,ev) { ev.stopPropagation(); var c=groups[si][gi].ri.length; var i=findFlatIndex(si,gi,c-1); if(i>=allRules.length-1)return; var t=allRules[i+1]; scrollToFlash(t.stageIdx,t.groupIdx,t.ruleIdx,ensureRuleVisible(t.stageIdx,t.groupIdx,t.ruleIdx)); }

/* ── Global cycle ── */
var globalCycleState=3;
var cycleLabels=['Expand Stages','Expand Rules','Expand All','Collapse All'];
function cycleGlobalState(){
    globalCycleState=(globalCycleState+1)%4;
    for(var si=0;si<stageCount;si++){
        if(!stageRuleCounts[si])continue;
        switch(globalCycleState){
        case 0: stageOpen[si]=false; for(var gi=0;gi<groups[si].length;gi++){groupOpen[si][gi]=false;for(var ri=0;ri<groups[si][gi].ri.length;ri++)ruleOpen[si][gi][ri]=false;}break;
        case 1: stageOpen[si]=true; for(var gi=0;gi<groups[si].length;gi++){groupOpen[si][gi]=false;for(var ri=0;ri<groups[si][gi].ri.length;ri++)ruleOpen[si][gi][ri]=false;}break;
        case 2: stageOpen[si]=true; for(var gi=0;gi<groups[si].length;gi++){var c=groups[si][gi].ri.length;groupOpen[si][gi]=true;for(var ri=0;ri<c;ri++)ruleOpen[si][gi][ri]=(c>1?ri===c-1:true);}break;
        case 3: stageOpen[si]=true; for(var gi=0;gi<groups[si].length;gi++){groupOpen[si][gi]=true;for(var ri=0;ri<groups[si][gi].ri.length;ri++)ruleOpen[si][gi][ri]=true;}break;
        }
    }
    updateAllDisplays();
    document.getElementById('cycle-button').textContent=cycleLabels[globalCycleState];
}

function toggleDarkMode(){document.body.classList.toggle('darkmode');}

/* ── Search ── */
var savedState=null;
function currentSearchQuery(){var b=document.getElementById('search-box');return(searchActive&&b)?b.value.trim():'';}
function saveLayoutState(){
    return{so:stageOpen.slice(),go:groupOpen.map(function(a){return a.slice();}),
           ro:ruleOpen.map(function(a){return a.map(function(b){return b.slice();});})};
}
function applyLayoutState(s){stageOpen=s.so;groupOpen=s.go;ruleOpen=s.ro;}

function doSearch(){
    var query=document.getElementById('search-box').value.trim();
    var info=document.getElementById('search-info');
    if(!query){
        if(searchActive&&savedState){applyLayoutState(savedState);savedState=null;searchActive=false;rerenderAllTrees('');updateAllDisplays();}
        info.textContent='';return;
    }
    if(!searchActive){savedState=saveLayoutState();searchActive=true;}
    var lq=query.toLowerCase(),matches=0;
    for(var si=0;si<stageCount;si++){stageOpen[si]=false;for(var gi=0;gi<groups[si].length;gi++){groupOpen[si][gi]=false;for(var ri=0;ri<groups[si][gi].ri.length;ri++)ruleOpen[si][gi][ri]=false;}}
    for(var si=0;si<stageCount;si++)for(var gi=0;gi<groups[si].length;gi++)for(var ri=0;ri<groups[si][gi].ri.length;ri++){
        var raw=groups[si][gi].ri[ri];
        var nm=ruleNames[si][raw].toLowerCase().indexOf(lq)!==-1;
        var tm=treeContains(planTrees[si][raw],lq);
        if(nm||tm){stageOpen[si]=true;if(groups[si][gi].ri.length>1)groupOpen[si][gi]=true;ruleOpen[si][gi][ri]=true;ruleMetaAll[si][gi][ri]=tm;rerenderRuleTree(si,gi,ri,query);matches++;}
        else rerenderRuleTree(si,gi,ri,'');
    }
    info.textContent=matches?matches+' match'+(matches>1?'es':''):'no matches';
    updateAllDisplays();
}
function treeContains(n,lq){
    if(!n||!n.l)return false;
    if(n.l.toLowerCase().indexOf(lq)!==-1)return true;
    if(n.m)for(var i=0;i<n.m.length;i++){if(n.m[i][0].toLowerCase().indexOf(lq)!==-1)return true;if(n.m[i][1].toLowerCase().indexOf(lq)!==-1)return true;}
    if(n.c)for(var i=0;i<n.c.length;i++)if(treeContains(n.c[i],lq))return true;
    return false;
}
function rerenderAllTrees(q){for(var si=0;si<stageCount;si++)for(var gi=0;gi<groups[si].length;gi++)for(var ri=0;ri<groups[si][gi].ri.length;ri++)rerenderRuleTree(si,gi,ri,q);}


/* ══════════════════════════════
   A/B Diff — inline
   ══════════════════════════════ */
var diffA=null, diffB=null, diffSavedState=null;

function updateDiffButton(){
    var b=document.getElementById('diff-button'); if(!b)return;
    if(diffSavedState){b.textContent='Exit Diff';b.className='ctrl-btn diff-active';}
    else if(diffA||diffB){b.textContent='Reset A|B';b.className='ctrl-btn';}
    else{b.textContent='Reset A|B';b.className='ctrl-btn inactive';}
}

function onDiffButtonClick(){
    if(diffSavedState) closeDiffFull();
    else if(diffA||diffB){ clearDiffSel('both'); diffA=null; diffB=null; updateDiffButton(); }
}

function setDiffA(si,gi,ri,ev){
    ev.stopPropagation();
    if(diffA&&diffA.si===si&&diffA.gi===gi&&diffA.ri===ri){
        var wasActive = !!diffSavedState;
        if(wasActive) exitDiffKeepOther('b');
        else { clearDiffSel('a'); diffA=null; }
        updateDiffButton(); return;
    }
    clearDiffSel('a'); diffA={si:si,gi:gi,ri:ri};
    var btn=document.getElementById('abtn-'+si+'-'+gi+'-'+ri);
    if(btn)btn.classList.add('selected-a');
    if(diffB) showDiffInline();
    updateDiffButton();
}

function setDiffB(si,gi,ri,ev){
    ev.stopPropagation();
    if(diffB&&diffB.si===si&&diffB.gi===gi&&diffB.ri===ri){
        var wasActive = !!diffSavedState;
        if(wasActive) exitDiffKeepOther('a');
        else { clearDiffSel('b'); diffB=null; }
        updateDiffButton(); return;
    }
    clearDiffSel('b'); diffB={si:si,gi:gi,ri:ri};
    var btn=document.getElementById('bbtn-'+si+'-'+gi+'-'+ri);
    if(btn)btn.classList.add('selected-b');
    if(diffA) showDiffInline();
    updateDiffButton();
}

function clearDiffSel(which){
    if((which==='a'||which==='both')&&diffA){
        var b=document.getElementById('abtn-'+diffA.si+'-'+diffA.gi+'-'+diffA.ri);
        if(b)b.classList.remove('selected-a');
        if(which==='a')diffA=null;
    }
    if((which==='b'||which==='both')&&diffB){
        var b=document.getElementById('bbtn-'+diffB.si+'-'+diffB.gi+'-'+diffB.ri);
        if(b)b.classList.remove('selected-b');
        if(which==='b')diffB=null;
    }
}

function closeDiffFull(){
    if(diffSavedState){applyLayoutState(diffSavedState);diffSavedState=null;}
    clearDiffSel('both'); diffA=null; diffB=null;
    rerenderAllTrees(currentSearchQuery()); updateAllDisplays(); updateDiffButton();
}

function exitDiffKeepOther(keepWhich){
    if(diffSavedState){applyLayoutState(diffSavedState);diffSavedState=null;}
    if(keepWhich==='a'){ clearDiffSel('b'); diffB=null; }
    else { clearDiffSel('a'); diffA=null; }
    rerenderAllTrees(currentSearchQuery()); updateAllDisplays();
}

function showDiffInline(){
    if(!diffA||!diffB)return;
    if(!diffSavedState) diffSavedState=saveLayoutState();

    for(var si=0;si<stageCount;si++){stageOpen[si]=false;for(var gi=0;gi<groups[si].length;gi++){groupOpen[si][gi]=false;for(var ri=0;ri<groups[si][gi].ri.length;ri++)ruleOpen[si][gi][ri]=false;}}
    var targets=[diffA,diffB];
    for(var t=0;t<2;t++){var d=targets[t];stageOpen[d.si]=true;if(groups[d.si][d.gi].ri.length>1)groupOpen[d.si][d.gi]=true;ruleOpen[d.si][d.gi][d.ri]=true;}
    updateAllDisplays();

    var rawA=groups[diffA.si][diffA.gi].ri[diffA.ri];
    var rawB=groups[diffB.si][diffB.gi].ri[diffB.ri];
    var diffs=computeTreeDiff(planTrees[diffA.si][rawA], planTrees[diffB.si][rawB]);

    var kA=diffA.si+'-'+diffA.gi+'-'+diffA.ri;
    var cA=document.getElementById('tree-'+kA);
    if(cA) cA.innerHTML='<div class="tree-root">'+renderDiffSide(diffs,kA,'0','a',[],true)+'</div>';

    var kB=diffB.si+'-'+diffB.gi+'-'+diffB.ri;
    var cB=document.getElementById('tree-'+kB);
    if(cB) cB.innerHTML='<div class="tree-root">'+renderDiffSide(diffs,kB,'0','b',[],true)+'</div>';

    updateDiffButton();
}

function computeTreeDiff(a, b) {
    if (!a && !b) return [];
    if (!a) return [markAllDiff(b, 'added')];
    if (!b) return [markAllDiff(a, 'removed')];
    if (a.l !== b.l) return [markAllDiff(a, 'removed'), markAllDiff(b, 'added')];

    var result = { status: 'same', label: a.l, children: [] };
    var ac = a.c || [], bc = b.c || [];
    var ops = lcsMatch(ac, bc);
    for (var i = 0; i < ops.length; i++) {
        var op = ops[i];
        if (op.type === 'match') {
            var sub = computeTreeDiff(ac[op.a], bc[op.b]);
            for (var s = 0; s < sub.length; s++) result.children.push(sub[s]);
        } else if (op.type === 'remove') {
            result.children.push(markAllDiff(ac[op.a], 'removed'));
        } else {
            result.children.push(markAllDiff(bc[op.b], 'added'));
        }
    }
    return [result];
}

function markAllDiff(node, status) {
    var r = { status: status, label: node.l, children: [] };
    if (node.c) for (var i = 0; i < node.c.length; i++) r.children.push(markAllDiff(node.c[i], status));
    return r;
}

function lcsMatch(ac, bc) {
    var n=ac.length, m=bc.length, dp=[];
    for(var i=0;i<=n;i++){dp[i]=[];for(var j=0;j<=m;j++)dp[i][j]=0;}
    for(var i=1;i<=n;i++)for(var j=1;j<=m;j++)
        dp[i][j]=ac[i-1].l===bc[j-1].l?dp[i-1][j-1]+1:Math.max(dp[i-1][j],dp[i][j-1]);
    var matched=[],i=n,j=m;
    while(i>0&&j>0){if(ac[i-1].l===bc[j-1].l){matched.unshift({type:'match',a:i-1,b:j-1});i--;j--;}else if(dp[i-1][j]>=dp[i][j-1])i--;else j--;}
    var ops=[],ai=0,bi=0;
    for(var mi=0;mi<matched.length;mi++){
        while(ai<matched[mi].a)ops.push({type:'remove',a:ai++});
        while(bi<matched[mi].b)ops.push({type:'add',b:bi++});
        ops.push(matched[mi]);ai=matched[mi].a+1;bi=matched[mi].b+1;
    }
    while(ai<n)ops.push({type:'remove',a:ai++});
    while(bi<m)ops.push({type:'add',b:bi++});
    return ops;
}

function renderDiffSide(nodes, rk, basePid, side, parentPrefix, isRoot) {
    var html = '';
    var visible = [];
    for (var i = 0; i < nodes.length; i++) {
        var n = nodes[i];
        if (side === 'a' && n.status === 'added') continue;
        if (side === 'b' && n.status === 'removed') continue;
        visible.push(n);
    }

    if (isRoot) {
        for (var i = 0; i < visible.length; i++)
            html += renderDiffNode(visible[i], rk, basePid + '-' + i, side, [], true);
    } else {
        for (var i = 0; i < visible.length; i++) {
            var isLast = (i === visible.length - 1);
            var childPrefix = parentPrefix.slice();
            childPrefix.push(isLast ? 'space' : 'pipe');
            html += renderDiffNodeWithBranch(visible[i], rk, basePid + '-' + i, side, parentPrefix, childPrefix, isLast);
        }
    }
    return html;
}

function renderDiffNode(node, rk, pid, side, prefix, isRoot) {
    var diffCls = '';
    if (node.status === 'added') diffCls = ' diff-mark-add';
    if (node.status === 'removed') diffCls = ' diff-mark-rem';

    var visKids = getVisibleDiffChildren(node, side);
    var nid = 'tn-' + rk + '-' + pid;
    var html = '<div class="' + diffCls + '" id="' + nid + '">';
    html += '<div class="tree-node-row">';
    for (var i = 0; i < prefix.length; i++)
        html += '<span class="tree-c ' + prefix[i] + '"></span>';
    html += '<span class="tree-toggle leaf">•</span>';
    html += '<span class="tree-label">' + htmlEscape(node.label || '') + '</span>';
    html += '</div>';

    if (visKids.length > 0)
        html += renderDiffSide(node.children, rk, pid, side, prefix, false);

    html += '</div>';
    return html;
}

function renderDiffNodeWithBranch(node, rk, pid, side, parentPrefix, childPrefix, isLast) {
    var diffCls = '';
    if (node.status === 'added') diffCls = ' diff-mark-add';
    if (node.status === 'removed') diffCls = ' diff-mark-rem';

    var visKids = getVisibleDiffChildren(node, side);
    var brType = isLast ? 'elbow' : 'branch';
    var nid = 'tn-' + rk + '-' + pid;
    var html = '<div class="' + diffCls + '" id="' + nid + '">';
    html += '<div class="tree-node-row">';
    for (var i = 0; i < parentPrefix.length; i++)
        html += '<span class="tree-c ' + parentPrefix[i] + '"></span>';
    html += '<span class="tree-c ' + brType + '"></span>';
    html += '<span class="tree-toggle leaf">•</span>';
    html += '<span class="tree-label">' + htmlEscape(node.label || '') + '</span>';
    html += '</div>';

    if (visKids.length > 0)
        html += renderDiffSide(node.children, rk, pid, side, childPrefix, false);

    html += '</div>';
    return html;
}

function getVisibleDiffChildren(node, side) {
    if (!node.children) return [];
    var v = [];
    for (var i = 0; i < node.children.length; i++) {
        if (side === 'a' && node.children[i].status === 'added') continue;
        if (side === 'b' && node.children[i].status === 'removed') continue;
        v.push(node.children[i]);
    }
    return v;
}


/* ── Wheel scroll ── */
function initWheelScroll(){
    var t=document.querySelector('.trace'); if(!t)return;
    t.addEventListener('wheel',function(e){
        if(e.target.closest&&(e.target.closest('.rule-tree-wrap')||e.target.closest('.rule-info-panel')))return;
        if(Math.abs(e.deltaY)>Math.abs(e.deltaX)){t.scrollLeft+=e.deltaY;e.preventDefault();}
    },{passive:false});
}

/* ── Init ── */
window.addEventListener('DOMContentLoaded',function(){
    initWheelScroll();
    for(var si=0;si<stageCount;si++)for(var gi=0;gi<groups[si].length;gi++)for(var ri=0;ri<groups[si][gi].ri.length;ri++){
        rerenderRuleTree(si,gi,ri,'');
        var raw=groups[si][gi].ri[ri];
        var ic=document.getElementById('info-content-'+si+'-'+gi+'-'+ri);
        if(ic)renderInfoPanel(ic,ruleInfo[si][raw]);
    }
    var cb=document.querySelector('.controls input[type="checkbox"]');
    if(cb&&!cb.checked)document.body.classList.remove('darkmode');
    updateDiffButton();
});

</script>
</head><body class="darkmode">
)RAWJS";

    /* ═══════════ Controls ═══════════ */
    out << "<div class=\"top-bar\"><div class=\"controls\">\n";
    out << "  <h1>SQL Optimizer Trace</h1>\n";
    out << "  <button class=\"ctrl-btn inactive\" id=\"diff-button\" onclick=\"onDiffButtonClick()\" style=\"min-width:90px\">Reset A|B</button>\n";
    out << "  <button class=\"ctrl-btn\" id=\"cycle-button\" onclick=\"cycleGlobalState()\" style=\"min-width:120px\">Collapse All</button>\n";
    out << "  <label><input type=\"checkbox\" checked onclick=\"toggleDarkMode()\" style=\"cursor:pointer\"> dark</label>\n";
    out << "  <input class=\"search-box\" id=\"search-box\" type=\"text\" placeholder=\"Search plans…\" oninput=\"doSearch()\">\n";
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

        out << "<div class=\"stage-wrap" << (empty?" stage-empty":"") << "\">\n";
        out << "  <div class=\"stage-col\" id=\"stage-col-" << i
            << "\" style=\"display:" << (empty?"flex":"none")
            << "\" onclick=\"toggleStage(" << i << ")\">"
            << "<div class=\"stage-col-arrow\">&#9654;</div>"
            << "<div class=\"stage-col-text\">" << esc(stage.name) << "</div>"
            << "<div class=\"stage-col-count\">(" << stage.rules.size() << ")</div></div>\n";

        out << "  <div class=\"stage-exp\" id=\"stage-exp-" << i
            << "\" style=\"display:" << (empty?"none":"flex") << "\">\n";
        out << "    <div class=\"stage-hdr\" onclick=\"toggleStage(" << i << ")\">";
        if (!empty) out << "<button class=\"stage-toggle-btn\" onclick=\"toggleStageRules(" << i << ",event)\">&#9776;</button>";
        out << "<span class=\"stage-hdr-arrow\">&#9660;</span>" << esc(stage.name)
            << "<span class=\"stage-hdr-count\">(" << stage.rules.size() << ")</span></div>\n";
        out << "    <div class=\"rules-row\">\n";

        if (empty) {
            out << "      <div class=\"rule-cell expanded\" style=\"color:#666;font-style:italic;\">"
                << "<div class=\"rule-exp-wrap\"><div class=\"rule-title-bar\" style=\"cursor:default;\">"
                << "<div class=\"title-left\"><div class=\"rule-title\" style=\"cursor:default;color:#888;\">no rules</div></div>"
                << "</div><div class=\"rule-content\"><div class=\"rule-tree-wrap\">"
                << "<span style=\"color:#999;font-size:12px;\">no rules applied</span></div></div></div></div>\n";
        }

        size_t flatBase = 0;
        for (size_t si2 = 0; si2 < i; si2++) flatBase += stages_[si2].rules.size();
        size_t ruleNum = 0;

        for (size_t g = 0; g < grps.size(); g++) {
            const auto& grp = grps[g];
            size_t cnt = grp.ruleIndices.size();

            if (cnt == 1) {
                size_t j = grp.ruleIndices[0]; ruleNum++;
                const auto& rule = stage.rules[j];
                size_t flat = flatBase+j;
                bool isF=(flat==0), isL=(flat==totalRules-1);
                bool hasInfo=!rule.info.empty();
                bool hasMeta=rule.tree.hasAnyMeta();
                bool hasStats=rule.tree.hasAnyStats()&&!statColumns_.empty();

                out << "      <div class=\"rule-cell expanded\" id=\"rule-"
                    << i << "-" << g << "-0\" onclick=\"onCollapsedRuleClick(" << i << "," << g << ",0,event)\">\n";
                out << "        <div class=\"rule-col-wrap\"><div class=\"rule-col-num\">" << ruleNum
                    << "</div><div class=\"rule-col-text\">" << esc(rule.ruleName) << "</div></div>\n";
                out << "        <div class=\"rule-exp-wrap\">\n";
                emitTitleBar(out, i, g, 0, ruleNum, rule.ruleName, hasMeta, hasStats, hasInfo, isF, isL);
                out << "          <div class=\"rule-content\">\n";
                out << "            <div class=\"rule-tree-wrap\" id=\"tree-" << i << "-" << g << "-0\"></div>\n";
                if (hasInfo) out << "            <div class=\"rule-info-panel\" id=\"info-" << i << "-" << g << "-0\"><div id=\"info-content-" << i << "-" << g << "-0\"></div></div>\n";
                out << "          </div>\n";
                out << "        </div>\n      </div>\n";

            } else {
                size_t flatFirst = flatBase + grp.ruleIndices[0];
                bool grpF=(flatFirst==0), grpL=((flatFirst+cnt-1)==totalRules-1);

                out << "      <div class=\"group-cell g-collapsed\" id=\"group-" << i << "-" << g
                    << "\" onclick=\"onCollapsedGroupClick(" << i << "," << g << ",event)\">\n";
                out << "        <div class=\"group-col-wrap\"><div class=\"group-col-count\">&times;" << cnt
                    << "</div><div class=\"group-col-text\">" << esc(grp.name) << "</div></div>\n";
                out << "        <div class=\"group-exp-wrap\">\n";
                out << "          <div class=\"group-title-bar\">\n";
                out << "            <button class=\"group-toggle-btn\" onclick=\"toggleGroupRules(" << i << "," << g << ",event)\">&#9776;</button>\n";
                out << "            <div class=\"group-title\" onclick=\"toggleGroup(" << i << "," << g << ",event)\">"
                    << esc(grp.name) << " (&times;" << cnt << ")</div>\n";
                out << "            <div class=\"group-nav\">"
                    << "<button class=\"group-nav-btn\" onclick=\"groupNavigatePrev(" << i << "," << g << ",event)\"" << (grpF?" disabled":"") << ">&#8592;</button>"
                    << "<button class=\"group-nav-btn\" onclick=\"groupNavigateNext(" << i << "," << g << ",event)\"" << (grpL?" disabled":"") << ">&#8594;</button></div>\n";
                out << "          </div>\n";
                out << "          <div class=\"group-rules-area\">\n";

                for (size_t k = 0; k < cnt; k++) {
                    size_t j = grp.ruleIndices[k]; ruleNum++;
                    const auto& rule = stage.rules[j];
                    size_t flat = flatBase+j;
                    bool isF=(flat==0), isL=(flat==totalRules-1);
                    bool startExp=(k==cnt-1);
                    bool hasInfo=!rule.info.empty();
                    bool hasMeta=rule.tree.hasAnyMeta();
                    bool hasStats=rule.tree.hasAnyStats()&&!statColumns_.empty();

                    out << "            <div class=\"rule-cell " << (startExp?"expanded":"collapsed")
                        << "\" id=\"rule-" << i << "-" << g << "-" << k
                        << "\" onclick=\"onCollapsedRuleClick(" << i << "," << g << "," << k << ",event)\">\n";
                    out << "              <div class=\"rule-col-wrap\"><div class=\"rule-col-num\">" << ruleNum
                        << "</div><div class=\"rule-col-text\">" << esc(rule.ruleName) << "</div></div>\n";
                    out << "              <div class=\"rule-exp-wrap\">\n                ";
                    emitTitleBar(out, i, g, k, ruleNum, rule.ruleName, hasMeta, hasStats, hasInfo, isF, isL);
                    out << "                <div class=\"rule-content\">\n";
                    out << "                  <div class=\"rule-tree-wrap\" id=\"tree-" << i << "-" << g << "-" << k << "\"></div>\n";
                    if (hasInfo) out << "                  <div class=\"rule-info-panel\" id=\"info-" << i << "-" << g << "-" << k << "\"><div id=\"info-content-" << i << "-" << g << "-" << k << "\"></div></div>\n";
                    out << "                </div>\n";
                    out << "              </div>\n            </div>\n";
                }
                out << "          </div>\n        </div>\n      </div>\n";
            }
        }
        out << "    </div>\n  </div>\n</div>\n";
    }
    out << "</div>\n</body></html>\n";
    out.close();
    return true;
}

std::string OptimizerTraceHTML::esc(const std::string& s) {
    std::string r; r.reserve(s.size()+s.size()/8);
    for (char c : s) switch(c) {
        case '&': r+="&amp;"; break; case '<': r+="&lt;"; break;
        case '>': r+="&gt;"; break; case '"': r+="&quot;"; break;
        case '\'': r+="&#39;"; break; default: r+=c;
    }
    return r;
}

std::string OptimizerTraceHTML::jsEsc(const std::string& s) {
    std::string r; r.reserve(s.size()+s.size()/8);
    for (char c : s) switch(c) {
        case '\\': r+="\\\\"; break; case '"': r+="\\\""; break;
        case '\'': r+="\\'"; break; case '\n': r+="\\n"; break;
        case '\r': r+="\\r"; break; case '\t': r+="\\t"; break;
        default: r+=c;
    }
    return r;
}

void OptimizerTraceHTML::emitTreeJSON(std::ofstream& out, const PlanNode& node) const {
    out << "{l:\"" << jsEsc(node.label) << "\",m:[";
    for (size_t i = 0; i < node.metadata.size(); i++) {
        if (i) out << ",";
        out << "[\"" << jsEsc(node.metadata[i].first) << "\",\"" << jsEsc(node.metadata[i].second) << "\"]";
    }
    out << "]";
    if (!node.stats.empty()) {
        out << ",s:[";
        for (size_t i = 0; i < node.stats.size(); i++) { if (i) out << ","; out << "\"" << jsEsc(node.stats[i]) << "\""; }
        out << "]";
    }
    out << ",c:[";
    for (size_t i = 0; i < node.children.size(); i++) { if (i) out << ","; emitTreeJSON(out, node.children[i]); }
    out << "]}";
}

void OptimizerTraceHTML::emitInfoJSON(std::ofstream& out, const std::vector<InfoSection>& sections) const {
    out << "[";
    for (size_t i = 0; i < sections.size(); i++) {
        if (i) out << ",";
        const auto& s = sections[i];
        out << "{";
        switch (s.type) {
            case InfoSection::TABLE:
                out << "type:\"table\",title:\"" << jsEsc(s.title) << "\",rows:[";
                for (size_t r = 0; r < s.tableRows.size(); r++) {
                    if (r) out << ",";
                    out << "[\"" << jsEsc(s.tableRows[r].first) << "\",\"" << jsEsc(s.tableRows[r].second) << "\"]";
                }
                out << "]"; break;
            case InfoSection::HTML:
                out << "type:\"html\",title:\"" << jsEsc(s.title) << "\",content:\"" << jsEsc(s.content) << "\""; break;
            case InfoSection::TEXT:
                out << "type:\"text\",title:\"" << jsEsc(s.title) << "\",content:\"" << jsEsc(s.content) << "\""; break;
        }
        out << "}";
    }
    out << "]";
}

void OptimizerTraceHTML::emitTitleBar(std::ofstream& out, size_t si, size_t gi, size_t ri,
                  size_t ruleNum, const std::string& name,
                  bool hasMeta, bool hasStats, bool hasInfo,
                  bool isFirst, bool isLast) const {
    out << "<div class=\"rule-title-bar\">\n";
    out << "  <div class=\"title-left\">";
    out << "<div class=\"rule-title\" onclick=\"toggleRule(" << si << "," << gi << "," << ri << ",event)\">"
        << "<span class=\"rule-num\">" << ruleNum << ".</span>" << esc(name) << "</div>";
    if (hasMeta)
        out << "<button class=\"tbtn icon\" id=\"metabtn-" << si << "-" << gi << "-" << ri
            << "\" onclick=\"toggleRuleMeta(" << si << "," << gi << "," << ri << ",event)\" title=\"Toggle metadata\">"
            << "\xe2\x93\x98</button>";
    if (hasStats)
        out << "<button class=\"tbtn icon\" id=\"statbtn-" << si << "-" << gi << "-" << ri
            << "\" onclick=\"toggleRuleStats(" << si << "," << gi << "," << ri << ",event)\" title=\"Toggle statistics\">"
            << "\xe2\x96\xa4</button>";
    if (hasInfo)
        out << "<button class=\"tbtn icon\" id=\"infobtn-" << si << "-" << gi << "-" << ri
            << "\" onclick=\"toggleRuleInfo(" << si << "," << gi << "," << ri << ",event)\" title=\"Toggle info\">"
            << "\xe2\x93\x98</button>";
    out << "</div>";
    out << "<div class=\"title-right\">";
    out << "<button class=\"tbtn ab\" id=\"abtn-" << si << "-" << gi << "-" << ri
        << "\" onclick=\"setDiffA(" << si << "," << gi << "," << ri << ",event)\">A</button>";
    out << "<button class=\"tbtn ab\" id=\"bbtn-" << si << "-" << gi << "-" << ri
        << "\" onclick=\"setDiffB(" << si << "," << gi << "," << ri << ",event)\">B</button>";
    out << "<button class=\"tbtn nav\" onclick=\"navigatePrev(" << si << "," << gi << "," << ri << ",event)\""
        << (isFirst?" disabled":"") << ">&#8592;</button>";
    out << "<button class=\"tbtn nav\" onclick=\"navigateNext(" << si << "," << gi << "," << ri << ",event)\""
        << (isLast?" disabled":"") << ">&#8594;</button>";
    out << "</div>\n</div>\n";
}

} // namespace NKqp
} // namespace NKikimr
