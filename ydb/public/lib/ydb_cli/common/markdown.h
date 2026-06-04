#pragma once

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>

#include <util/generic/strbuf.h>

namespace NYdb::NConsoleClient {

// Render Markdown-ish text as an FTXUI element tree for terminal output: prose words are reflowed
// with inline bold ("**...**") and inline code ("`...`") styled, Markdown headings are bolded,
// tables become real bordered tables, and fenced code blocks become highlighted panels. Inline
// markup that can't be styled in place (italic, blockquotes) is reduced to plain text.
ftxui::Element MarkdownToElement(TStringBuf text);

} // namespace NYdb::NConsoleClient
