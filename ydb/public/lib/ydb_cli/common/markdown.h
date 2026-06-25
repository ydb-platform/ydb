#pragma once

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/strbuf.h>

namespace NYdb::NConsoleClient {

// Render Markdown-ish text as an FTXUI element tree for terminal output: prose words are reflowed
// with inline bold ("**...**") styled and Markdown headings bolded, tables become real bordered
// tables, and fenced code blocks become highlighted panels. Inline markup that can't be styled in
// place (italic, blockquotes) is reduced to plain text; single backticks are kept verbatim (the
// model uses them for YQL identifiers).
ftxui::Element MarkdownToElement(TStringBuf text);

} // namespace NYdb::NConsoleClient
