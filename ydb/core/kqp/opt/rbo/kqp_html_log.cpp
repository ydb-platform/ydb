#include "kqp_html_log.h"

#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NKikimr {
namespace NKqp {

void TKqpOptimizerTraceHtml::AddStage(const TString& name) {
    Stages_.push_back(TStage{.Name = name, .Rules = {}});
}

void TKqpOptimizerTraceHtml::AddRule(const TString& ruleName, const TString& planText) {
    if (Stages_.empty()) {
        return;
    }
    Stages_.back().Rules.push_back(TRuleApplication{.RuleName = ruleName, .PlanText = planText});
}

bool TKqpOptimizerTraceHtml::GenerateHtml(const TString& filename) const {
    try {
        TFileOutput out(filename);
        out << R"(<!DOCTYPE html>
<html><head>
<meta charset="UTF-8">
<style>

* { box-sizing: border-box; margin: 0; padding: 0; }

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, sans-serif;
    font-size: 14px;
    background: #f5f6f8;
    color: #24292f;
    padding: 16px;
}

h1 {
    font-size: 17px;
    font-weight: 600;
    margin-bottom: 12px;
    display: inline-block;
    margin-right: 16px;
}

.controls {
    margin-bottom: 14px;
}

.controls label {
    font-size: 12px;
    color: #666;
    cursor: pointer;
    margin-left: 12px;
}

/* ── Trace row ── */
.trace {
    display: inline-flex;
    align-items: stretch;
    min-height: calc(100vh - 90px);
}

/* ══════════════════════════════════════════════════════
   Stage wrapper: always present, always inline-flex
   ══════════════════════════════════════════════════════ */
.stage-wrap {
    display: inline-flex;
    align-items: stretch;
    margin-right: 3px;
}

/* ── Stage: collapsed vertical bar ── */
.stage-wrap .stage-col {
    width: 28px;
    min-width: 28px;
    background: #c8cdd5;
    border: 2px solid #a8aeb8;
    border-radius: 4px;
    cursor: pointer;
    user-select: none;
    display: flex;
    flex-direction: column;
    align-items: center;
    padding-top: 6px;
    transition: background 0.1s;
}

.stage-col:hover {
    background: #bbc1ca;
}

.stage-col-arrow {
    font-size: 9px;
    color: #556;
    margin-bottom: 4px;
    flex-shrink: 0;
}

.stage-col-text {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 12px;
    font-weight: 600;
    color: #3a3f4a;
    white-space: nowrap;
    padding: 4px 0;
}

/* ── Stage: expanded (header + rules) ── */
.stage-wrap .stage-exp {
    display: flex;
    flex-direction: column;
    min-width: 0;
}

.stage-hdr {
    height: 30px;
    min-height: 30px;
    max-height: 30px;
    display: flex;
    align-items: center;
    padding: 0 10px;
    background: #dfe3e8;
    border: 1px solid #b0b6be;
    border-bottom: none;
    border-radius: 3px 3px 0 0;
    font-size: 12px;
    font-weight: 600;
    color: #374151;
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    overflow: hidden;
    transition: background 0.1s;
}

.stage-hdr:hover {
    background: #d0d5db;
}

.stage-hdr-arrow {
    margin-right: 6px;
    font-size: 8px;
    color: #556;
}

.stage-hdr-count {
    font-weight: 400;
    color: #888;
    margin-left: 6px;
    font-size: 11px;
}

/* ── Rules row within a stage ── */
.rules-row {
    display: flex;
    align-items: stretch;
    flex: 1;
    min-height: 0;
}

/* ── Rule cell ── */
.rule-cell {
    display: flex;
    flex-direction: column;
    border: 1px solid #c8ccd2;
    overflow: hidden;
    transition: background 0.1s;
}

/* Expanded rule */
.rule-cell.expanded {
    width: 520px;
    min-width: 520px;
    max-width: 520px;
    background: #fff;
}

.rule-cell.expanded .rule-col-wrap { display: none; }
.rule-cell.expanded .rule-exp-wrap { display: flex; flex-direction: column; flex: 1; min-height: 0; }

/* Collapsed rule */
.rule-cell.collapsed {
    width: 16px;
    min-width: 16px;
    max-width: 16px;
    background: #eef0f3;
    cursor: pointer;
    border-color: #d8dbe0;
}

.rule-cell.collapsed:hover {
    background: #e4e7eb;
}

.rule-cell.collapsed .rule-col-wrap { display: flex; flex-direction: column; height: 100%; }
.rule-cell.collapsed .rule-exp-wrap { display: none; }

.rule-col-text {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 12px;
    color: #555;
    white-space: nowrap;
    padding: 8px 2px;
}

/* ── Rule expanded content ── */
.rule-title {
    padding: 8px 14px;
    font-size: 13px;
    font-weight: 600;
    color: #24292f;
    cursor: pointer;
    user-select: none;
    border-bottom: 1px solid #e8eaed;
    background: #f8f9fb;
    flex-shrink: 0;
    transition: background 0.1s;
}

.rule-title:hover {
    background: #eef0f4;
}

.rule-pre-wrap {
    flex: 1;
    overflow: auto;
    min-height: 0;
}

pre {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 1.6;
    -moz-tab-size: 4;
    -o-tab-size:   4;
    tab-size:      4;
    white-space: pre;
    margin: 0;
    padding: 12px 16px;
    color: #24292f;
}

/* ── Dark mode ── */
body.darkmode {
    background: #1b1d23;
    color: #d4d7de;
}

body.darkmode .stage-col {
    background: #3a3d45;
    border-color: #555;
}
body.darkmode .stage-col:hover { background: #4a4d55; }
body.darkmode .stage-col-text { color: #bbb; }
body.darkmode .stage-col-arrow { color: #aaa; }

body.darkmode .stage-hdr {
    background: #2d3039;
    border-color: #555;
    color: #ccd0d8;
}
body.darkmode .stage-hdr:hover { background: #363940; }
body.darkmode .stage-hdr-count { color: #888; }
body.darkmode .stage-hdr-arrow { color: #aaa; }

body.darkmode .rule-cell {
    border-color: #444;
}
body.darkmode .rule-cell.expanded {
    background: #22252b;
}
body.darkmode .rule-cell.collapsed {
    background: #2a2d33;
    border-color: #3a3d44;
}
body.darkmode .rule-cell.collapsed:hover {
    background: #34373e;
}
body.darkmode .rule-col-text { color: #999; }

body.darkmode .rule-title {
    color: #d4d7de;
    background: #282b32;
    border-bottom-color: #3a3d44;
}
body.darkmode .rule-title:hover { background: #30343b; }

body.darkmode pre { color: #d4d7de; }

</style>

<script>
)";

        out << "var stageCount = " << Stages_.size() << ";\n";
        out << "var stageRuleCounts = [";
        for (size_t i = 0; i < Stages_.size(); i++) {
            if (i) {
                out << ",";
            }
            out << Stages_[i].Rules.size();
        }
        out << "];\n\n";

        out << R"(
var stageOpen = [];
var ruleOpen = [];

for (var i = 0; i < stageCount; i++) {
    stageOpen.push(true);
    ruleOpen.push([]);
    for (var j = 0; j < stageRuleCounts[i]; j++) {
        ruleOpen[i].push(true);
    }
}

function toggleStage(idx) {
    stageOpen[idx] = !stageOpen[idx];
    var col = document.getElementById('stage-' + idx + '-col');
    var exp = document.getElementById('stage-' + idx + '-exp');
    if (stageOpen[idx]) {
        col.style.display = 'none';
        exp.style.display = 'flex';
    } else {
        col.style.display = 'flex';
        exp.style.display = 'none';
    }
}

function toggleRule(si, ri) {
    ruleOpen[si][ri] = !ruleOpen[si][ri];
    var cell = document.getElementById('s' + si + 'r' + ri);
    cell.className = 'rule-cell ' + (ruleOpen[si][ri] ? 'expanded' : 'collapsed');
}

function onCollapsedRuleClick(si, ri, evt) {
    evt.stopPropagation();
    if (!ruleOpen[si][ri]) {
        toggleRule(si, ri);
    }
}

function toggleDarkMode() {
    document.body.classList.toggle('darkmode');
}
</script>

</head><body>
)";

        out << "<div class=\"controls\">\n";
        out << "  <h1>KQP New RBO trace</h1>\n";
        out << "  <label><input type=\"checkbox\" onclick=\"toggleDarkMode()\" style=\"cursor:pointer\"> dark mode</label>\n";
        out << "</div>\n\n";

        out << "<div class=\"trace\">\n";

        for (size_t i = 0; i < Stages_.size(); i++) {
            const TStage& stage = Stages_[i];

            out << "<div class=\"stage-wrap\">\n";

            out << "  <div class=\"stage-col\" id=\"stage-" << i
                << "-col\" style=\"display:none\" onclick=\"toggleStage(" << i << ")\">"
                << "<div class=\"stage-col-arrow\">&#9654;</div>"
                << "<div class=\"stage-col-text\">" << Esc(stage.Name) << "</div>"
                << "</div>\n";

            out << "  <div class=\"stage-exp\" id=\"stage-" << i << "-exp\" style=\"display:flex\">\n";

            out << "    <div class=\"stage-hdr\" onclick=\"toggleStage(" << i << ")\">"
                << "<span class=\"stage-hdr-arrow\">&#9660;</span>" << Esc(stage.Name)
                << "<span class=\"stage-hdr-count\">(" << stage.Rules.size() << ")</span>"
                << "</div>\n";

            out << "    <div class=\"rules-row\">\n";

            if (stage.Rules.empty()) {
                out << "      <div class=\"rule-cell expanded\" style=\"color:#999;font-style:italic;\">"
                    << "<div class=\"rule-exp-wrap\"><div class=\"rule-title\" style=\"cursor:default;\">no rules</div>"
                    << "<div class=\"rule-pre-wrap\"><pre style=\"color:#999;\">no rules applied</pre></div>"
                    << "</div></div>\n";
            }

            for (size_t j = 0; j < stage.Rules.size(); j++) {
                const TRuleApplication& rule = stage.Rules[j];
                TString id = TStringBuilder() << "s" << i << "r" << j;

                out << "      <div class=\"rule-cell expanded\" id=\"" << id
                    << "\" onclick=\"onCollapsedRuleClick(" << i << "," << j << ",event)\">\n";

                out << "        <div class=\"rule-col-wrap\">"
                    << "<div class=\"rule-col-text\">" << Esc(rule.RuleName) << "</div>"
                    << "</div>\n";

                out << "        <div class=\"rule-exp-wrap\">\n";
                out << "          <div class=\"rule-title\" onclick=\"event.stopPropagation();toggleRule(" << i << "," << j
                    << ")\">" << Esc(rule.RuleName) << "</div>\n";
                out << "          <div class=\"rule-pre-wrap\"><pre>" << Esc(rule.PlanText) << "</pre></div>\n";
                out << "        </div>\n";

                out << "      </div>\n";
            }

            out << "    </div>\n";
            out << "  </div>\n";

            out << "</div>\n";
        }

        out << "</div>\n";
        out << "</body></html>\n";
        out.Finish();
        return true;
    } catch (...) {
        return false;
    }
}

TString TKqpOptimizerTraceHtml::Esc(TStringBuf s) {
    TString r;
    r.reserve(s.size() + s.size() / 8);
    for (char c : s) {
        switch (c) {
            case '&':
                r += "&amp;";
                break;
            case '<':
                r += "&lt;";
                break;
            case '>':
                r += "&gt;";
                break;
            case '"':
                r += "&quot;";
                break;
            case '\'':
                r += "&#39;";
                break;
            default:
                r += c;
        }
    }
    return r;
}

} // namespace NKqp
} // namespace NKikimr
