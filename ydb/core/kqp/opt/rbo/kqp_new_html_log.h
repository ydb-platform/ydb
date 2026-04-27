// optimizer_trace_html.h
#ifndef OPTIMIZER_TRACE_HTML_H
#define OPTIMIZER_TRACE_HTML_H

#include <string>
#include <vector>
#include <fstream>
#include <sstream>
#include <cassert>
#include <cstddef>
#include <utility>

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

/* ═══════════════════════════
   Theme palettes

   To make a new theme, copy one block and change only these variables.
   The rest of the logger uses semantic tokens, not hard-coded colors.
   ═══════════════════════════ */

:root,
body.theme-stone-light {
    color-scheme: light;
    --bg-page: #f0eee9;
    --bg-page-top: #fbfaf7;
    --text-strong: #1f1c18;
    --text-main: #2a251f;
    --text-muted: #4a453d;
    --text-soft: #6f6659;
    --text-disabled: #8a8073;
    --text-on-accent: #fffefb;
    --surface-rule: #fffefb;
    --surface-rule-collapsed: #e8e3da;
    --surface-panel: #faf8f3;
    --surface-control: #e8e3da;
    --surface-control-hover: #ddd6ca;
    --surface-stage: #bdb5a7;
    --surface-stage-hover: #b2a999;
    --surface-stage-empty: #d9d3c8;
    --surface-empty-card: #ede8de;
    --text-empty-title: #3a342d;
    --text-empty-muted: #6f6659;
    --empty-dot: #8a642a;
    --surface-group: #b1a898;
    --surface-group-hover: #a69c8b;
    --surface-group-bar: #aba291;
    --surface-group-rules: #a69c8b;
    --surface-row: #c8c1b6;
    --border-strong: #8f8576;
    --border-main: #9e9485;
    --border-soft: #d8d1c7;
    --border-subtle: #e7e1d8;
    --connector: #776f63;
    --leaf-dot: #6b6257;
    --hover-row: rgba(42, 37, 31, 0.07);
    --focus: #8a642a;
    --focus-ring: rgba(138, 100, 42, 0.22);
    --accent-blue: #2f7d72;
    --accent-blue-border: #24675d;
    --accent-red: #c5475a;
    --accent-red-border: #a93647;
    --select-a: #3a7d72;
    --select-a-border: #2f665d;
    --select-b: #c55365;
    --select-b-border: #a94052;
    --diff-add-bg: #dcefd9;
    --diff-add-text: #2f6f35;
    --diff-rem-bg: #f4d9dc;
    --diff-rem-text: #a9343f;
    --mark-bg: #f4d35e;
    --mark-text: #1f1c18;
}

body.theme-stone-dark {
    color-scheme: dark;
    --bg-page: #11100d;
    --bg-page-top: #171511;
    --text-strong: #f7f2e9;
    --text-main: #e2d9ca;
    --text-muted: #b9ad9b;
    --text-soft: #968a78;
    --text-disabled: #71685b;
    --text-on-accent: #11100d;
    --surface-rule: #1b1915;
    --surface-rule-collapsed: #26231d;
    --surface-panel: #28241e;
    --surface-control: #2d2921;
    --surface-control-hover: #393328;
    --surface-stage: #242017;
    --surface-stage-hover: #2f2a1f;
    --surface-stage-empty: #191713;
    --surface-empty-card: #222018;
    --text-empty-title: #e6dccb;
    --text-empty-muted: #a99b87;
    --empty-dot: #d0a35b;
    --surface-group: #2c271e;
    --surface-group-hover: #383126;
    --surface-group-bar: #342e23;
    --surface-group-rules: #15130f;
    --surface-row: #15130f;
    --border-strong: #554b3b;
    --border-main: #6d614f;
    --border-soft: #41382d;
    --border-subtle: #363026;
    --connector: #817563;
    --leaf-dot: #a99b87;
    --hover-row: rgba(242, 233, 218, 0.075);
    --focus: #d0a35b;
    --focus-ring: rgba(208, 163, 91, 0.22);
    --accent-blue: #55a394;
    --accent-blue-border: #438a7d;
    --accent-red: #d56670;
    --accent-red-border: #b84c56;
    --select-a: #4d9b8d;
    --select-a-border: #3d8175;
    --select-b: #c95a73;
    --select-b-border: #ad465e;
    --diff-add-bg: #183c2c;
    --diff-add-text: #7be199;
    --diff-rem-bg: #4a2228;
    --diff-rem-text: #ff8588;
    --mark-bg: #796323;
    --mark-text: #fff0a3;
}

body.theme-slate-light {
    color-scheme: light;
    --bg-page: #edf1f7;
    --bg-page-top: #f8fafc;
    --text-strong: #111827;
    --text-main: #1f2937;
    --text-muted: #4b5565;
    --text-soft: #647084;
    --text-disabled: #7d8797;
    --text-on-accent: #ffffff;
    --surface-rule: #ffffff;
    --surface-rule-collapsed: #e2e7f0;
    --surface-panel: #f8fafc;
    --surface-control: #e7ebf2;
    --surface-control-hover: #d9e0ea;
    --surface-stage: #b6c0cf;
    --surface-stage-hover: #abb6c7;
    --surface-stage-empty: #d4dbe5;
    --surface-empty-card: #e8edf5;
    --text-empty-title: #273244;
    --text-empty-muted: #647084;
    --empty-dot: #2f6fe4;
    --surface-group: #adbacb;
    --surface-group-hover: #a2afc2;
    --surface-group-bar: #a8b4c6;
    --surface-group-rules: #a2afc2;
    --surface-row: #c0cad8;
    --border-strong: #8794a8;
    --border-main: #9aa6b6;
    --border-soft: #d2d9e3;
    --border-subtle: #e5e9f0;
    --connector: #748198;
    --leaf-dot: #647084;
    --hover-row: rgba(31, 41, 55, 0.06);
    --focus: #2f6fe4;
    --focus-ring: rgba(47, 111, 228, 0.20);
    --accent-blue: #2563eb;
    --accent-blue-border: #1d4ed8;
    --accent-red: #d63d57;
    --accent-red-border: #b72f46;
    --select-a: #2f74d0;
    --select-a-border: #1f5fb6;
    --select-b: #d13f68;
    --select-b-border: #b82f56;
    --diff-add-bg: #d8f3df;
    --diff-add-text: #176f36;
    --diff-rem-bg: #fde0e4;
    --diff-rem-text: #b4232d;
    --mark-bg: #ffe66d;
    --mark-text: #111827;
}

body.theme-slate-dark {
    color-scheme: dark;
    --bg-page: #0e131b;
    --bg-page-top: #121824;
    --text-strong: #f3f7fb;
    --text-main: #d9e2ee;
    --text-muted: #aeb9c9;
    --text-soft: #8b98ac;
    --text-disabled: #667287;
    --text-on-accent: #0e131b;
    --surface-rule: #18202c;
    --surface-rule-collapsed: #202b3b;
    --surface-panel: #222d3e;
    --surface-control: #263246;
    --surface-control-hover: #303d52;
    --surface-stage: #1d2738;
    --surface-stage-hover: #27344a;
    --surface-stage-empty: #151c28;
    --surface-empty-card: #1a2433;
    --text-empty-title: #e4edf8;
    --text-empty-muted: #9aa8bd;
    --empty-dot: #6da3ff;
    --surface-group: #223044;
    --surface-group-hover: #2b3a51;
    --surface-group-bar: #28364b;
    --surface-group-rules: #111926;
    --surface-row: #111926;
    --border-strong: #40506a;
    --border-main: #53647e;
    --border-soft: #39485f;
    --border-subtle: #303d51;
    --connector: #5f708c;
    --leaf-dot: #8b9bb6;
    --hover-row: rgba(222, 233, 247, 0.075);
    --focus: #6da3ff;
    --focus-ring: rgba(109, 163, 255, 0.22);
    --accent-blue: #4d8cff;
    --accent-blue-border: #3473e5;
    --accent-red: #d64d62;
    --accent-red-border: #b53a4e;
    --select-a: #3f7fdd;
    --select-a-border: #2d68be;
    --select-b: #d0527a;
    --select-b-border: #b94868;
    --diff-add-bg: #143e2b;
    --diff-add-text: #75e58f;
    --diff-rem-bg: #4b2029;
    --diff-rem-text: #ff7a7a;
    --mark-bg: #6f6425;
    --mark-text: #fff3a6;
}

body.theme-yandex-light {
    color-scheme: light;
    --bg-page: #f5f5f5;
    --bg-page-top: #ffffff;
    --text-strong: #000000;
    --text-main: #1f1f1f;
    --text-muted: #4d4d4d;
    --text-soft: #707070;
    --text-disabled: #9d9d9d;
    --text-on-accent: #1a1a1a;
    --surface-rule: #ffffff;
    --surface-rule-collapsed: #eeeeee;
    --surface-panel: #fafafa;
    --surface-control: #eeeeee;
    --surface-control-hover: #e3e3e3;
    --surface-stage: #d0d0d0;
    --surface-stage-hover: #c6c6c6;
    --surface-stage-empty: #e2e2e2;
    --surface-empty-card: #eeeeee;
    --text-empty-title: #1f1f1f;
    --text-empty-muted: #707070;
    --empty-dot: #ffcc00;
    --surface-group: #c7c7c7;
    --surface-group-hover: #bdbdbd;
    --surface-group-bar: #c1c1c1;
    --surface-group-rules: #bdbdbd;
    --surface-row: #d8d8d8;
    --border-strong: #8c8c8c;
    --border-main: #a8a8a8;
    --border-soft: #d6d6d6;
    --border-subtle: #e6e6e6;
    --connector: #6e6e6e;
    --leaf-dot: #5c5c5c;
    --hover-row: rgba(0, 0, 0, 0.06);
    --focus: #ffcc00;
    --focus-ring: rgba(255, 204, 0, 0.35);
    --accent-blue: #ffcc00;
    --accent-blue-border: #e5b800;
    --accent-red: #ff7070;
    --accent-red-border: #e45f5f;
    --select-a: #ffcc00;
    --select-a-border: #e5b800;
    --select-b: #ff7070;
    --select-b-border: #e45f5f;
    --diff-add-bg: #ddf3dc;
    --diff-add-text: #236b2b;
    --diff-rem-bg: #ffe0e0;
    --diff-rem-text: #a72828;
    --mark-bg: #ffdc4d;
    --mark-text: #1a1a1a;
}

body.theme-yandex-dark {
    color-scheme: dark;
    --bg-page: #111111;
    --bg-page-top: #171717;
    --text-strong: #f5f5f5;
    --text-main: #e0e0e0;
    --text-muted: #b8b8b8;
    --text-soft: #909090;
    --text-disabled: #6f6f6f;
    --text-on-accent: #1a1a1a;
    --surface-rule: #1d1d1d;
    --surface-rule-collapsed: #282828;
    --surface-panel: #252525;
    --surface-control: #2b2b2b;
    --surface-control-hover: #373737;
    --surface-stage: #242424;
    --surface-stage-hover: #303030;
    --surface-stage-empty: #191919;
    --surface-empty-card: #202020;
    --text-empty-title: #f0f0f0;
    --text-empty-muted: #a8a8a8;
    --empty-dot: #ffcc00;
    --surface-group: #2b2b2b;
    --surface-group-hover: #363636;
    --surface-group-bar: #323232;
    --surface-group-rules: #151515;
    --surface-row: #151515;
    --border-strong: #555555;
    --border-main: #6b6b6b;
    --border-soft: #444444;
    --border-subtle: #343434;
    --connector: #777777;
    --leaf-dot: #a0a0a0;
    --hover-row: rgba(255, 255, 255, 0.075);
    --focus: #ffcc00;
    --focus-ring: rgba(255, 204, 0, 0.26);
    --accent-blue: #ffcc00;
    --accent-blue-border: #d8ad00;
    --accent-red: #ff7070;
    --accent-red-border: #df5c5c;
    --select-a: #ffcc00;
    --select-a-border: #d8ad00;
    --select-b: #ff7070;
    --select-b-border: #df5c5c;
    --diff-add-bg: #1f3a2a;
    --diff-add-text: #7edc91;
    --diff-rem-bg: #492729;
    --diff-rem-text: #ff8585;
    --mark-bg: #806a16;
    --mark-text: #fff0a3;
}

body.theme-monokai-pro {
    color-scheme: dark;
    --bg-page: #221f22;
    --bg-page-top: #2d2a2e;
    --text-strong: #fcfcfa;
    --text-main: #f0ede8;
    --text-muted: #c8c3bd;
    --text-soft: #9c969e;
    --text-disabled: #6f6973;
    --text-on-accent: #2d2a2e;
    --surface-rule: #2d2a2e;
    --surface-rule-collapsed: #3a363d;
    --surface-panel: #343136;
    --surface-control: #3b383e;
    --surface-control-hover: #47434a;
    --surface-stage: #39353c;
    --surface-stage-hover: #45404a;
    --surface-stage-empty: #28252a;
    --surface-empty-card: #302d33;
    --text-empty-title: #f0ede8;
    --text-empty-muted: #aaa2ad;
    --empty-dot: #ffd866;
    --surface-group: #453d45;
    --surface-group-hover: #514850;
    --surface-group-bar: #4a424a;
    --surface-group-rules: #242126;
    --surface-row: #242126;
    --border-strong: #5e5763;
    --border-main: #726a78;
    --border-soft: #4c4650;
    --border-subtle: #3e3942;
    --connector: #8b858e;
    --leaf-dot: #b2abb6;
    --hover-row: rgba(252, 252, 250, 0.075);
    --focus: #ffd866;
    --focus-ring: rgba(255, 216, 102, 0.24);
    --accent-blue: #78dce8;
    --accent-blue-border: #5bc7d4;
    --accent-red: #ff6188;
    --accent-red-border: #e85278;
    --select-a: #78dce8;
    --select-a-border: #5bc7d4;
    --select-b: #ff6188;
    --select-b-border: #e85278;
    --diff-add-bg: #344936;
    --diff-add-text: #a9dc76;
    --diff-rem-bg: #4a2732;
    --diff-rem-text: #ff8ba7;
    --mark-bg: #ffd866;
    --mark-text: #2d2a2e;
}

body.theme-doom-old-hope {
    color-scheme: dark;
    --bg-page: #1c1d21;
    --bg-page-top: #22242b;
    --text-strong: #f4f1de;
    --text-main: #d6d6d6;
    --text-muted: #b7bcc9;
    --text-soft: #8b92a5;
    --text-disabled: #626878;
    --text-on-accent: #1c1d21;
    --surface-rule: #24262d;
    --surface-rule-collapsed: #30333d;
    --surface-panel: #2b2d35;
    --surface-control: #30333c;
    --surface-control-hover: #3a3d49;
    --surface-stage: #303241;
    --surface-stage-hover: #3a3d4e;
    --surface-stage-empty: #202229;
    --surface-empty-card: #292b34;
    --text-empty-title: #f4f1de;
    --text-empty-muted: #aab0bf;
    --empty-dot: #f3c969;
    --surface-group: #36384a;
    --surface-group-hover: #414458;
    --surface-group-bar: #3b3e51;
    --surface-group-rules: #191b21;
    --surface-row: #191b21;
    --border-strong: #54596d;
    --border-main: #666c82;
    --border-soft: #3f4352;
    --border-subtle: #343744;
    --connector: #8f96a8;
    --leaf-dot: #aab0bf;
    --hover-row: rgba(244, 241, 222, 0.075);
    --focus: #f3c969;
    --focus-ring: rgba(243, 201, 105, 0.24);
    --accent-blue: #4fd6be;
    --accent-blue-border: #39bfa8;
    --accent-red: #ff6077;
    --accent-red-border: #e24f64;
    --select-a: #4fd6be;
    --select-a-border: #39bfa8;
    --select-b: #ff6077;
    --select-b-border: #e24f64;
    --diff-add-bg: #2d442b;
    --diff-add-text: #a1d655;
    --diff-rem-bg: #472631;
    --diff-rem-text: #ff8496;
    --mark-bg: #f3c969;
    --mark-text: #1c1d21;
}

body.theme-modus-operandi {
    color-scheme: light;
    --bg-page: #ffffff;
    --bg-page-top: #ffffff;
    --text-strong: #000000;
    --text-main: #000000;
    --text-muted: #404040;
    --text-soft: #606060;
    --text-disabled: #8a8a8a;
    --text-on-accent: #ffffff;
    --surface-rule: #ffffff;
    --surface-rule-collapsed: #eeeeee;
    --surface-panel: #f7f7f7;
    --surface-control: #eeeeee;
    --surface-control-hover: #e0e0e0;
    --surface-stage: #d0d0d0;
    --surface-stage-hover: #c4c4c4;
    --surface-stage-empty: #e5e5e5;
    --surface-empty-card: #f0f0f0;
    --text-empty-title: #000000;
    --text-empty-muted: #505050;
    --empty-dot: #0031a9;
    --surface-group: #c2c2c2;
    --surface-group-hover: #b7b7b7;
    --surface-group-bar: #bbbbbb;
    --surface-group-rules: #b7b7b7;
    --surface-row: #dadada;
    --border-strong: #777777;
    --border-main: #999999;
    --border-soft: #d0d0d0;
    --border-subtle: #e4e4e4;
    --connector: #666666;
    --leaf-dot: #555555;
    --hover-row: rgba(0, 0, 0, 0.06);
    --focus: #0031a9;
    --focus-ring: rgba(0, 49, 169, 0.18);
    --accent-blue: #0031a9;
    --accent-blue-border: #002580;
    --accent-red: #a60000;
    --accent-red-border: #7f0000;
    --select-a: #0031a9;
    --select-a-border: #002580;
    --select-b: #a60000;
    --select-b-border: #7f0000;
    --diff-add-bg: #d7f5dc;
    --diff-add-text: #005e00;
    --diff-rem-bg: #ffd7d7;
    --diff-rem-text: #a60000;
    --mark-bg: #f2e900;
    --mark-text: #000000;
}

body.theme-modus-vivendi {
    color-scheme: dark;
    --bg-page: #000000;
    --bg-page-top: #0a0a0a;
    --text-strong: #ffffff;
    --text-main: #ffffff;
    --text-muted: #c6c6c6;
    --text-soft: #9f9f9f;
    --text-disabled: #707070;
    --text-on-accent: #000000;
    --surface-rule: #000000;
    --surface-rule-collapsed: #1a1a1a;
    --surface-panel: #1e1e1e;
    --surface-control: #222222;
    --surface-control-hover: #303030;
    --surface-stage: #222222;
    --surface-stage-hover: #303030;
    --surface-stage-empty: #111111;
    --surface-empty-card: #181818;
    --text-empty-title: #ffffff;
    --text-empty-muted: #bcbcbc;
    --empty-dot: #2fafff;
    --surface-group: #2a2a2a;
    --surface-group-hover: #383838;
    --surface-group-bar: #333333;
    --surface-group-rules: #101010;
    --surface-row: #101010;
    --border-strong: #5f5f5f;
    --border-main: #7a7a7a;
    --border-soft: #3a3a3a;
    --border-subtle: #2c2c2c;
    --connector: #9a9a9a;
    --leaf-dot: #bcbcbc;
    --hover-row: rgba(255, 255, 255, 0.08);
    --focus: #2fafff;
    --focus-ring: rgba(47, 175, 255, 0.26);
    --accent-blue: #2fafff;
    --accent-blue-border: #1b95dc;
    --accent-red: #ff5f59;
    --accent-red-border: #d94a45;
    --select-a: #2fafff;
    --select-a-border: #1b95dc;
    --select-b: #ff5f59;
    --select-b-border: #d94a45;
    --diff-add-bg: #173b17;
    --diff-add-text: #44bc44;
    --diff-rem-bg: #4a1f1f;
    --diff-rem-text: #ff7f86;
    --mark-bg: #d0bc00;
    --mark-text: #000000;
}

body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, sans-serif;
    font-size: 14px;
    background: var(--bg-page);
    color: var(--text-main);
    display: flex;
    flex-direction: column;
}

/* ─── Top bar ─── */
.top-bar {
    flex-shrink: 0;
    padding: 10px 16px 8px 16px;
    background: var(--bg-page-top);
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
    color: var(--text-muted);
    cursor: pointer;
    white-space: nowrap;
}

.ctrl-btn {
    font-size: 11px;
    padding: 3px 8px;
    border: 1px solid var(--border-main);
    border-radius: 3px;
    background: var(--surface-control);
    color: var(--text-main);
    cursor: pointer;
    transition: background 0.1s;
    white-space: nowrap;
    text-align: center;
}
.ctrl-btn:hover { background: var(--surface-control-hover); }
.ctrl-btn.inactive { opacity: 0.35; pointer-events: none; }
.ctrl-btn.diff-active {
    background: var(--accent-red);
    border-color: var(--accent-red-border);
    color: var(--text-on-accent);
}
.ctrl-btn.diff-active:hover { filter: brightness(0.96); }

.search-box {
    font-size: 12px;
    padding: 3px 8px;
    border: 1px solid var(--border-main);
    border-radius: 3px;
    width: 220px;
    min-width: 220px;
    outline: none;
    background: var(--surface-rule);
    color: var(--text-main);
}
.search-box:focus {
    border-color: var(--focus);
    box-shadow: 0 0 0 2px var(--focus-ring);
}

.search-info {
    font-size: 11px;
    color: var(--text-soft);
    white-space: nowrap;
}

.theme-select {
    font-size: 12px;
    padding: 3px 22px 3px 8px;
    border: 1px solid var(--border-main);
    border-radius: 3px;
    outline: none;
    background: var(--surface-rule);
    color: var(--text-main);
    cursor: pointer;
}
.theme-select:focus {
    border-color: var(--border-main);
    box-shadow: none;
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
    background: var(--surface-stage);
    border: 1px solid var(--border-strong);
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
.stage-col:hover { background: var(--surface-stage-hover); }

.stage-col-arrow {
    font-size: 9px;
    color: var(--text-muted);
    margin-bottom: 4px;
    flex-shrink: 0;
}

.stage-col-text {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 12px;
    font-weight: 600;
    color: var(--text-main);
    white-space: nowrap;
    padding: 4px 0;
}

.stage-col-count {
    writing-mode: vertical-lr;
    transform: rotate(180deg);
    font-size: 10px;
    color: var(--text-muted);
    white-space: nowrap;
    padding: 2px 0;
    margin-top: 2px;
}

/* Empty stage styling */
.stage-wrap.stage-empty .stage-col {
    background: var(--surface-stage-empty);
    border-color: var(--border-main);
}
.stage-wrap.stage-empty .stage-col:hover { background: var(--surface-control-hover); }
.stage-wrap.stage-empty .stage-col-text { color: var(--text-muted); }
.stage-wrap.stage-empty .stage-col-arrow { color: var(--text-soft); }
.stage-wrap.stage-empty .stage-hdr {
    background: var(--surface-stage-empty);
    border-color: var(--border-main);
    color: var(--text-muted);
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
    background: var(--surface-stage);
    border: 1px solid var(--border-strong);
    border-bottom: none;
    border-radius: 3px 3px 0 0;
    font-size: 12px;
    font-weight: 600;
    color: var(--text-main);
    cursor: pointer;
    user-select: none;
    white-space: nowrap;
    overflow: hidden;
    transition: background 0.1s;
    flex-shrink: 0;
}
.stage-hdr:hover { background: var(--surface-stage-hover); }

.stage-hdr-arrow {
    margin-right: 6px;
    font-size: 8px;
    color: var(--text-muted);
}

.stage-hdr-count {
    font-weight: 400;
    color: var(--text-muted);
    margin-left: 6px;
    font-size: 11px;
}

.stage-toggle-btn {
    font-size: 10px;
    padding: 1px 5px;
    border: 1px solid var(--border-strong);
    border-radius: 3px;
    background: var(--surface-group-hover);
    color: var(--text-main);
    cursor: pointer;
    transition: background 0.1s;
    margin-right: 6px;
    flex-shrink: 0;
    line-height: 1.3;
}
.stage-toggle-btn:hover {
    background: var(--surface-stage-hover);
    color: var(--text-strong);
}

/* Rules row container */
.rules-row {
    display: flex;
    align-items: stretch;
    flex: 1;
    min-height: 0;
    overflow: hidden;
    border: 1px solid var(--border-strong);
    border-top: none;
    border-radius: 0 0 3px 3px;
    background: var(--surface-row);
    gap: 1px;
}

.empty-rules-row {
    align-items: center;
    justify-content: center;
    gap: 0;
    padding: 14px;
    background: var(--surface-stage-empty);
}

.empty-stage-card {
    width: 360px;
    min-width: 360px;
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 15px 18px;
    border-radius: 6px;
    background: var(--surface-empty-card);
    cursor: default;
    user-select: none;
}

.empty-stage-dot {
    width: 6px;
    height: 6px;
    border-radius: 999px;
    background: var(--empty-dot);
    flex-shrink: 0;
}

.empty-stage-copy {
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 3px;
}

.empty-stage-title {
    color: var(--text-empty-title);
    font-size: 12px;
    font-weight: 600;
    line-height: 1.2;
}

.empty-stage-subtitle {
    color: var(--text-empty-muted);
    font-size: 11px;
    line-height: 1.25;
}

/* ─── Unified small button ─── */
.tbtn {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    height: 18px;
    min-height: 18px;
    max-height: 18px;
    border: 1px solid var(--border-main);
    border-radius: 3px;
    background: var(--surface-control);
    color: var(--text-muted);
    cursor: pointer;
    transition: background 0.1s;
    line-height: 1;
    flex-shrink: 0;
    padding: 0;
    vertical-align: middle;
}
.tbtn:hover { background: var(--surface-control-hover); color: var(--text-strong); }
.tbtn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }
.tbtn.nav { width: 22px; font-size: 13px; }
.tbtn.icon { min-width: 22px; font-size: 11px; padding: 0 4px; }
.tbtn.icon.active { background: var(--accent-blue); border-color: var(--accent-blue-border); color: var(--text-on-accent); }
.tbtn.ab { width: 18px; font-size: 10px; font-weight: 700; color: var(--text-soft); }
.tbtn.ab:hover { background: var(--surface-control-hover); color: var(--text-main); }
.tbtn.ab.selected-a { background: var(--select-a); border-color: var(--select-a-border); color: var(--text-on-accent); }
.tbtn.ab.selected-b { background: var(--select-b); border-color: var(--select-b-border); color: var(--text-on-accent); }

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
    background: var(--surface-rule);
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
    background: var(--surface-rule-collapsed);
    cursor: pointer;
}
.rule-cell.collapsed:hover { background: var(--surface-control-hover); }
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
    border-bottom: 1px solid var(--border-subtle);
    background: var(--surface-panel);
    flex-shrink: 0;
    padding: 0 6px 0 0;
    cursor: pointer;
    user-select: none;
}

.rule-title {
    padding: 0 6px;
    font-size: 12px;
    font-weight: 400;
    color: var(--text-strong);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    line-height: 32px;
}

.rule-num {
    font-weight: 400;
    color: var(--text-soft);
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
    color: var(--text-soft);
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
    color: var(--text-muted);
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
    border-top: 2px solid var(--border-soft);
    overflow: auto;
    max-height: 45%;
    flex-shrink: 0;
    background: var(--surface-panel);
    padding: 8px 12px;
}
.rule-info-panel.visible { display: block; }

.info-section { margin-bottom: 10px; }

.info-section-title {
    font-size: 11px;
    font-weight: 600;
    color: var(--text-muted);
    margin-bottom: 4px;
    padding-bottom: 2px;
    border-bottom: 1px solid var(--border-subtle);
}

.info-table {
    font-size: 12px;
    border-collapse: collapse;
    width: 100%;
}
.info-table td {
    padding: 2px 8px 2px 0;
    vertical-align: top;
    border-bottom: 1px solid var(--border-subtle);
}
.info-table td:first-child {
    font-weight: 500;
    color: var(--text-muted);
    white-space: nowrap;
    width: 1%;
}

.info-text {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px;
    white-space: pre-wrap;
    line-height: 1.5;
    color: var(--text-main);
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
    border-bottom: 1px solid var(--border-subtle);
}
.tree-stats-header.visible { display: flex; }

.stat-header-cell {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 10px;
    font-weight: 600;
    color: var(--text-soft);
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
.tree-node-row:hover { background: var(--hover-row); }

.stat-val-cell {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 11px;
    color: var(--text-soft);
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
    border-left: 1px solid var(--connector);
}

/* Vertical line: top to center only (L-shape) */
.tree-c.elbow::before {
    content: '';
    position: absolute;
    left: 9px;
    top: 0;
    height: 11px;
    border-left: 1px solid var(--connector);
}

/* Horizontal branch */
.tree-c.branch::after,
.tree-c.elbow::after {
    content: '';
    position: absolute;
    left: 9px;
    top: 11px;
    width: 11px;
    border-top: 1px solid var(--connector);
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
    border-left: 1px solid var(--connector);
}

.tree-toggle {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 16px;
    min-width: 16px;
    height: 16px;
    font-size: 8px;
    color: var(--text-soft);
    cursor: pointer;
    flex-shrink: 0;
    border-radius: 2px;
    transition: color 0.1s;
    user-select: none;
}
.tree-toggle:hover { color: var(--text-strong); background: var(--hover-row); }
.tree-toggle.leaf {
    cursor: default;
    color: var(--leaf-dot);
    pointer-events: none;
}

.tree-label {
    font-family: "JetBrains Mono", Menlo, Consolas, monospace;
    font-size: 12px;
    line-height: 22px;
    color: var(--text-strong);
    padding-left: 2px;
    white-space: nowrap;
}
.tree-label.clickable { cursor: pointer; }
.tree-label.clickable:hover {
    text-decoration: underline;
    text-decoration-color: var(--connector);
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

.tree-meta-key { color: var(--text-soft); font-weight: 500; white-space: nowrap; }
.tree-meta-val { color: var(--text-muted); white-space: nowrap; }

.tree-meta-block { }
.tree-meta-block.collapsed { display: none; }
.tree-children-block { }

/* ─── Diff coloring — flat, no nesting accumulation ─── */
.diff-mark-add > .tree-node-row { background: var(--diff-add-bg); }
.diff-mark-add > .tree-node-row .tree-label { color: var(--diff-add-text); }
.diff-mark-rem > .tree-node-row { background: var(--diff-rem-bg); }
.diff-mark-rem > .tree-node-row .tree-label {
    color: var(--diff-rem-text);
    text-decoration: line-through;
    text-decoration-color: var(--diff-rem-text);
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
    background: var(--surface-group);
    cursor: pointer;
}
.group-cell.g-collapsed:hover { background: var(--surface-group-hover); }
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
    color: var(--text-muted);
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
    color: var(--text-main);
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
    background: var(--surface-group-bar);
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
    color: var(--text-muted);
    cursor: pointer;
    transition: color 0.1s;
    flex-shrink: 0;
    line-height: 14px;
}
.group-toggle-btn:hover { color: var(--text-strong); }

.group-title {
    flex: 1;
    font-size: 11px;
    font-weight: 600;
    color: var(--text-main);
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
    border: 1px solid var(--border-main);
    border-radius: 2px;
    background: var(--surface-group-hover);
    color: var(--text-main);
    cursor: pointer;
    transition: background 0.1s;
    line-height: 1;
    padding: 0;
    flex-shrink: 0;
}
.group-nav-btn:hover { background: var(--surface-stage-hover); color: var(--text-strong); }
.group-nav-btn:disabled { opacity: 0.3; cursor: default; pointer-events: none; }

.group-rules-area {
    display: flex;
    flex: 1;
    min-height: 0;
    align-items: stretch;
    overflow: hidden;
    background: var(--surface-group-rules);
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
    background: var(--mark-bg);
    color: var(--mark-text);
    border-radius: 2px;
    padding: 0 1px;
}

.tree-plan-label {
    font-size: 10px;
    color: var(--text-soft);
    font-weight: 600;
    padding-left: 2px;
}

.empty-note,
.info-empty {
    color: var(--text-soft);
    font-size: 11px;
    font-style: italic;
}

.empty-note { font-size: 12px; }

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
var renderedTreeState = [];
var searchActive = false;

for (var si = 0; si < stageCount; si++) {
    stageOpen.push(stageRuleCounts[si] > 0);
    groupOpen.push([]);
    ruleOpen.push([]);
    ruleMetaAll.push([]);
    ruleStats.push([]);
    ruleInfoOpen.push([]);
    renderedTreeState.push([]);

    for (var gi = 0; gi < groups[si].length; gi++) {
        var count = groups[si][gi].ri.length;
        var isMulti = count > 1;

        groupOpen[si].push(!isMulti);
        ruleOpen[si].push([]);
        ruleMetaAll[si].push([]);
        ruleStats[si].push([]);
        ruleInfoOpen[si].push([]);
        renderedTreeState[si].push([]);

        for (var ri = 0; ri < count; ri++) {
            ruleOpen[si][gi].push(isMulti ? (ri === count - 1) : true);
            ruleMetaAll[si][gi].push(false);
            ruleStats[si][gi].push(false);
            ruleInfoOpen[si][gi].push(false);
            renderedTreeState[si][gi].push(null);
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
        headerHtml += '<span class="tree-plan-label">Plan</span>';
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

function rerenderRuleTree(si, gi, ri, query, force) {
    var rawIdx = groups[si][gi].ri[ri];
    var ruleKey = si + '-' + gi + '-' + ri;
    var container = document.getElementById('tree-' + ruleKey);
    if (!container) return;

    query = query || '';
    var showMeta = ruleMetaAll[si][gi][ri];
    var showStats = ruleStats[si][gi][ri];
    var prev = renderedTreeState[si][gi][ri];
    if (!force && prev &&
        prev.query === query &&
        prev.showMeta === showMeta &&
        prev.showStats === showStats) {
        return;
    }

    renderTree(container, planTrees[si][rawIdx], ruleKey,
               showMeta, showStats, query);
    renderedTreeState[si][gi][ri] = {
        query: query,
        showMeta: showMeta,
        showStats: showStats
    };
}

function invalidateRuleTree(si, gi, ri) {
    if (renderedTreeState[si] &&
        renderedTreeState[si][gi]) {
        renderedTreeState[si][gi][ri] = null;
    }
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
        container.innerHTML = '<div class="info-empty">No info</div>';
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
        if (!stageRuleCounts[si]) {
            if (globalCycleState === 0) stageOpen[si] = false;
            continue;
        }

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

var themeNames = [
    'stone-dark', 'stone-light',
    'slate-dark', 'slate-light',
    'yandex-light', 'yandex-dark',
    'monokai-pro',
    'doom-old-hope',
    'modus-operandi', 'modus-vivendi'
];
var defaultTheme = 'stone-dark';

function applyTheme(theme, persist) {
    if (themeNames.indexOf(theme) === -1) theme = defaultTheme;

    for (var i = 0; i < themeNames.length; i++) {
        document.body.classList.remove('theme-' + themeNames[i]);
    }
    document.body.classList.add('theme-' + theme);

    var select = document.getElementById('theme-select');
    if (select) select.value = theme;

    if (persist !== false) {
        try { window.localStorage.setItem('optimizerTraceTheme', theme); } catch (e) {}
    }
}

function initTheme() {
    var savedTheme = null;
    try { savedTheme = window.localStorage.getItem('optimizerTraceTheme'); } catch (e) {}

    var currentTheme = defaultTheme;
    for (var i = 0; i < themeNames.length; i++) {
        if (document.body.classList.contains('theme-' + themeNames[i])) {
            currentTheme = themeNames[i];
            break;
        }
    }

    applyTheme(savedTheme || currentTheme, false);
}

function setTheme(theme) {
    applyTheme(theme, true);
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
var searchFrame = null;
var ruleSearchText = null;
var ruleTreeSearchText = null;

function scheduleSearch() {
    if (searchFrame !== null) return;

    var schedule = window.requestAnimationFrame || function(fn) {
        return setTimeout(fn, 0);
    };
    searchFrame = schedule(function() {
        searchFrame = null;
        doSearch();
    });
}

function warmSearchIndex() {
    if (ruleSearchText && ruleTreeSearchText) return;

    if (window.requestIdleCallback) {
        window.requestIdleCallback(function() { ensureSearchIndex(); });
    } else {
        setTimeout(function() { ensureSearchIndex(); }, 0);
    }
}

function ensureSearchIndex() {
    if (ruleSearchText && ruleTreeSearchText) return;

    ruleSearchText = [];
    ruleTreeSearchText = [];

    for (var si = 0; si < stageCount; si++) {
        ruleSearchText[si] = [];
        ruleTreeSearchText[si] = [];

        for (var ri = 0; ri < planTrees[si].length; ri++) {
            var treeText = buildTreeSearchText(planTrees[si][ri]);
            ruleTreeSearchText[si][ri] = treeText;
            ruleSearchText[si][ri] = (
                String(ruleNames[si][ri] || '') + '\n' + treeText
            ).toLowerCase();
        }
    }
}

function buildTreeSearchText(node) {
    var parts = [];
    collectTreeSearchText(node, parts);
    return parts.join('\n').toLowerCase();
}

function collectTreeSearchText(node, parts) {
    if (!node || !node.l) return;
    parts.push(String(node.l));

    if (node.m) {
        for (var i = 0; i < node.m.length; i++) {
            parts.push(String(node.m[i][0] || ''));
            parts.push(String(node.m[i][1] || ''));
        }
    }

    if (node.c) {
        for (var j = 0; j < node.c.length; j++) {
            collectTreeSearchText(node.c[j], parts);
        }
    }
}

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

    ensureSearchIndex();

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
                var treeMatch = ruleTreeSearchText[si][rawIdx].indexOf(lowerQuery) !== -1;
                var ruleMatch = ruleSearchText[si][rawIdx].indexOf(lowerQuery) !== -1;

                if (ruleMatch) {
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

function rerenderAllTrees(query, force) {
    for (var si = 0; si < stageCount; si++) {
        for (var gi = 0; gi < groups[si].length; gi++) {
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                rerenderRuleTree(si, gi, ri, query, force);
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
    rerenderAllTrees(currentSearchQuery(), true);
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
    rerenderAllTrees(currentSearchQuery(), true);
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
        invalidateRuleTree(diffA.si, diffA.gi, diffA.ri);
    }

    /* Render side B */
    var keyB = diffB.si + '-' + diffB.gi + '-' + diffB.ri;
    var containerB = document.getElementById('tree-' + keyB);
    if (containerB) {
        containerB.innerHTML = '<div class="tree-root">' +
            DiffRenderer.renderSide(diffResult, keyB, '0', 'b', [], true) +
            '</div>';
        invalidateRuleTree(diffB.si, diffB.gi, diffB.ri);
    }

    updateDiffButton();
}


/* ══════════════════════════════════════════════════════════════
   Tree Diff Module
   ══════════════════════════════════════════════════════════════

   Clean, self-contained module for computing structural diffs
   between two plan trees.

   Input:  two plan tree nodes (format: {l: string, c: [{...}], ...})
   Output: side-specific arrays of DiffNode objects:
       {
           status:   'same' | 'added' | 'removed',
           label:    string,
           children: [DiffNode, ...]
       }

   The algorithm:
   1. Render side-specific diff trees, so each side keeps its
      real physical tree shape.
   2. Match ordinary siblings by LCS on child labels.
   3. Detect moved unary wrapper operators. If removing a wrapper
      from one side makes it structurally equal to the other side,
      only the wrapper row is marked removed/added.
   ══════════════════════════════════════════════════════════════ */

var TreeDiff = (function() {

    var movableUnaryOperators = {
        Aggregate: true,
        Distinct: true,
        Filter: true,
        Limit: true,
        Map: true,
        Project: true,
        Sort: true,
        Window: true
    };

    /**
     * Main entry point. Returns side-specific diff trees:
     *   { sideSpecific: true, a: [DiffNode], b: [DiffNode] }
     */
    function diff(nodeA, nodeB) {
        return sideDiff(nodeA, nodeB);
    }

    /**
     * Diff two nodes while preserving each side's real tree shape.
     */
    function sideDiff(nodeA, nodeB) {
        if (!nodeA && !nodeB) return makeSideResult([], []);
        if (!nodeA) return makeSideResult([], [markEntireSubtree(nodeB, 'added')]);
        if (!nodeB) return makeSideResult([markEntireSubtree(nodeA, 'removed')], []);

        if (nodeA.l === nodeB.l) {
            var childDiff = diffChildren(nodeA.c || [], nodeB.c || []);
            return makeSideResult(
                [makeDiffNode(nodeA.l, 'same', childDiff.a)],
                [makeDiffNode(nodeB.l, 'same', childDiff.b)]
            );
        }

        var wrapperMove = tryMovedWrapperDiff(nodeA, nodeB);
        if (wrapperMove) return wrapperMove;

        return makeSideResult(
            [markEntireSubtree(nodeA, 'removed')],
            [markEntireSubtree(nodeB, 'added')]
        );
    }

    function makeSideResult(nodesA, nodesB) {
        return { sideSpecific: true, a: nodesA, b: nodesB };
    }

    function makeDiffNode(label, status, children) {
        return {
            status: status,
            label: label,
            original: null,
            children: children || []
        };
    }

    /**
     * Diff sibling lists. Matched labels recurse normally; unmatched
     * remove/add runs get a second pass for moved wrappers.
     */
    function diffChildren(childrenA, childrenB) {
        var editOps = computeLCSEditOps(childrenA, childrenB);
        var resultA = [];
        var resultB = [];

        for (var i = 0; i < editOps.length; i++) {
            var op = editOps[i];
            if (op.type === 'match') {
                var matchedDiff = sideDiff(childrenA[op.idxA], childrenB[op.idxB]);
                appendAll(resultA, matchedDiff.a);
                appendAll(resultB, matchedDiff.b);
                continue;
            }

            var removed = [];
            var added = [];
            while (i < editOps.length && editOps[i].type !== 'match') {
                if (editOps[i].type === 'remove') {
                    removed.push(childrenA[editOps[i].idxA]);
                } else {
                    added.push(childrenB[editOps[i].idxB]);
                }
                i++;
            }
            i--;

            var runDiff = diffUnmatchedRun(removed, added);
            appendAll(resultA, runDiff.a);
            appendAll(resultB, runDiff.b);
        }

        return makeSideResult(resultA, resultB);
    }

    function appendAll(target, items) {
        for (var i = 0; i < items.length; i++) target.push(items[i]);
    }

    /**
     * Pair unmatched remove/add nodes when they are probably the same
     * logical subtree in a new position. This keeps moved operators
     * readable as one removed row and one added row.
     */
    function diffUnmatchedRun(removed, added) {
        var resultA = [];
        var resultB = [];
        var pairForRemoved = {};
        var pairForAdded = {};

        for (var ri = 0; ri < removed.length; ri++) {
            var bestPair = null;
            var bestAddedIdx = -1;
            for (var ai = 0; ai < added.length; ai++) {
                if (pairForAdded[ai]) continue;

                var candidate = tryUnmatchedPairDiff(removed[ri], added[ai]);
                if (candidate) {
                    bestPair = candidate;
                    bestAddedIdx = ai;
                    break;
                }
            }
            if (bestPair) {
                pairForRemoved[ri] = bestPair;
                pairForAdded[bestAddedIdx] = bestPair;
            }
        }

        for (var r = 0; r < removed.length; r++) {
            if (pairForRemoved[r]) {
                appendAll(resultA, pairForRemoved[r].a);
            } else {
                resultA.push(markEntireSubtree(removed[r], 'removed'));
            }
        }

        for (var a = 0; a < added.length; a++) {
            if (pairForAdded[a]) {
                appendAll(resultB, pairForAdded[a].b);
            } else {
                resultB.push(markEntireSubtree(added[a], 'added'));
            }
        }

        return makeSideResult(resultA, resultB);
    }

    function tryUnmatchedPairDiff(nodeA, nodeB) {
        if (nodeA && nodeB && nodeA.l === nodeB.l) {
            var childDiff = diffChildren(nodeA.c || [], nodeB.c || []);
            return makeSideResult(
                [makeDiffNode(nodeA.l, 'removed', childDiff.a)],
                [makeDiffNode(nodeB.l, 'added', childDiff.b)]
            );
        }

        var wrapperMove = tryMovedWrapperDiff(nodeA, nodeB);
        if (wrapperMove) return wrapperMove;

        return null;
    }

    /**
     * Mark an entire subtree as added or removed.
     */
    function markEntireSubtree(planNode, status) {
        var diffNode = makeDiffNode(planNode.l, status, []);
        if (planNode.c) {
            for (var i = 0; i < planNode.c.length; i++) {
                diffNode.children.push(markEntireSubtree(planNode.c[i], status));
            }
        }
        return diffNode;
    }

    function cloneAsSame(planNode) {
        var diffNode = makeDiffNode(planNode.l, 'same', []);
        var children = planNode.c || [];
        for (var i = 0; i < children.length; i++) {
            diffNode.children.push(cloneAsSame(children[i]));
        }
        return diffNode;
    }

    /**
     * Detect a unary wrapper move:
     *   X(W(Y))  <->  W(X(Y))
     * and the multi-child equivalent:
     *   W(Join(A,B))  <->  Join(W(A),B)
     */
    function tryMovedWrapperDiff(nodeA, nodeB) {
        if (!nodeA || !nodeB) return null;

        if (isMovableUnaryWrapper(nodeB)) {
            var movedUp = tryTopWrapperAgainstNested(nodeA, nodeB, 'removed', 'added');
            if (movedUp) return movedUp;
        }

        if (isMovableUnaryWrapper(nodeA)) {
            var movedDown = tryTopWrapperAgainstNested(nodeB, nodeA, 'added', 'removed');
            if (movedDown) {
                return makeSideResult(movedDown.b, movedDown.a);
            }
        }

        return null;
    }

    function tryTopWrapperAgainstNested(treeWithNestedWrapper, topWrapper,
                                        nestedStatus, topStatus) {
        var paths = findWrapperPaths(treeWithNestedWrapper, topWrapper.l);
        var topPayload = (topWrapper.c || [])[0];
        if (!topPayload) return null;

        for (var i = 0; i < paths.length; i++) {
            var normalized = removeWrapperAtPath(treeWithNestedWrapper, paths[i]);
            if (sameStructure(normalized, topPayload)) {
                return makeSideResult(
                    [cloneWithWrapperStatus(treeWithNestedWrapper, paths[i], nestedStatus)],
                    [makeDiffNode(topWrapper.l, topStatus, [cloneAsSame(topPayload)])]
                );
            }
        }
        return null;
    }

    function findWrapperPaths(node, label) {
        var paths = [];
        collectWrapperPaths(node, label, [], paths);
        return paths;
    }

    function collectWrapperPaths(node, label, path, paths) {
        if (!node) return;
        if (node.l === label && isMovableUnaryWrapper(node)) {
            paths.push(path.slice());
        }

        var children = node.c || [];
        for (var i = 0; i < children.length; i++) {
            path.push(i);
            collectWrapperPaths(children[i], label, path, paths);
            path.pop();
        }
    }

    function cloneWithWrapperStatus(node, path, status) {
        if (path.length === 0) {
            return makeDiffNode(node.l, status, (node.c || []).map(cloneAsSame));
        }

        var children = [];
        var childIndex = path[0];
        var rest = path.slice(1);
        var sourceChildren = node.c || [];
        for (var i = 0; i < sourceChildren.length; i++) {
            if (i === childIndex) {
                children.push(cloneWithWrapperStatus(sourceChildren[i], rest, status));
            } else {
                children.push(cloneAsSame(sourceChildren[i]));
            }
        }
        return makeDiffNode(node.l, 'same', children);
    }

    function removeWrapperAtPath(node, path) {
        if (path.length === 0) {
            return clonePlanNode((node.c || [])[0]);
        }

        var clone = { l: node.l, c: [] };
        var childIndex = path[0];
        var rest = path.slice(1);
        var children = node.c || [];
        for (var i = 0; i < children.length; i++) {
            if (i === childIndex) {
                clone.c.push(removeWrapperAtPath(children[i], rest));
            } else {
                clone.c.push(clonePlanNode(children[i]));
            }
        }
        return clone;
    }

    function clonePlanNode(node) {
        var clone = { l: node.l, c: [] };
        var children = node.c || [];
        for (var i = 0; i < children.length; i++) {
            clone.c.push(clonePlanNode(children[i]));
        }
        return clone;
    }

    function sameStructure(nodeA, nodeB) {
        if (!nodeA || !nodeB) return false;
        if (nodeA.l !== nodeB.l) return false;

        var childrenA = nodeA.c || [];
        var childrenB = nodeB.c || [];
        if (childrenA.length !== childrenB.length) return false;

        for (var i = 0; i < childrenA.length; i++) {
            if (!sameStructure(childrenA[i], childrenB[i])) return false;
        }
        return true;
    }

    function isMovableUnaryWrapper(node) {
        var children = node.c || [];
        return children.length === 1 && !!movableUnaryOperators[operatorName(node.l)];
    }

    function operatorName(label) {
        var match = String(label || '').match(/^[^\s\[]+/);
        return match ? match[0] : '';
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
        if (nodes && nodes.sideSpecific) {
            nodes = nodes[side] || [];
        }

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
    initTheme();
    initWheelScroll();

    for (var si = 0; si < stageCount; si++) {
        for (var gi = 0; gi < groups[si].length; gi++) {
            for (var ri = 0; ri < groups[si][gi].ri.length; ri++) {
                rerenderRuleTree(si, gi, ri, '', true);

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

    updateDiffButton();
    warmSearchIndex();
});

</script>
</head><body class="theme-stone-dark">
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
        out << "  <label for=\"theme-select\">Theme</label>\n";
        out << "  <select class=\"theme-select\" id=\"theme-select\" "
            << "onchange=\"setTheme(this.value)\">\n";
        out << "    <option value=\"stone-dark\" selected>Stone Dark</option>\n";
        out << "    <option value=\"stone-light\">Stone Light</option>\n";
        out << "    <option value=\"slate-dark\">Slate Dark</option>\n";
        out << "    <option value=\"slate-light\">Slate Light</option>\n";
        out << "    <option value=\"yandex-light\">Yandex Light</option>\n";
        out << "    <option value=\"yandex-dark\">Yandex Dark</option>\n";
        out << "    <option value=\"monokai-pro\">Monokai Pro</option>\n";
        out << "    <option value=\"doom-old-hope\">Doom Old Hope</option>\n";
        out << "    <option value=\"modus-operandi\">Modus Operandi</option>\n";
        out << "    <option value=\"modus-vivendi\">Modus Vivendi</option>\n";
        out << "  </select>\n";
        out << "  <input class=\"search-box\" id=\"search-box\" type=\"text\" "
            << "placeholder=\"Search plans\xe2\x80\xa6\" oninput=\"scheduleSearch()\">\n";
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
            out << "    <div class=\"rules-row"
                << (empty ? " empty-rules-row" : "") << "\">\n";

            if (empty) {
                out << "      <div class=\"empty-stage-card\">"
                    << "<span class=\"empty-stage-dot\" aria-hidden=\"true\"></span>"
                    << "<div class=\"empty-stage-copy\">"
                    << "<div class=\"empty-stage-title\">No rules in this stage</div>"
                    << "<div class=\"empty-stage-subtitle\">Optimizer did not apply rules here</div>"
                    << "</div></div>\n";
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
        out << "<div class=\"rule-title-bar\" onclick=\"toggleRule("
            << si << "," << gi << "," << ri << ",event)\">\n";
        out << "  <div class=\"title-left\">";

        out << "<div class=\"rule-title\">"
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
