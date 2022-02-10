#pragma once

#include <util/generic/string.h>
#include <util/generic/strbuf.h>

TString IndentText(TStringBuf text, TStringBuf indent = TStringBuf("    "));
