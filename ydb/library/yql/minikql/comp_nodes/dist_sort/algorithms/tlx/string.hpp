/*******************************************************************************
 * tlx/string.hpp
 *
 * Part of tlx - http://panthema.net/tlx
 *
 * Copyright (C) 2017 Timo Bingmann <tb@panthema.net>
 *
 * All rights reserved. Published under the Boost Software License, Version 1.0
 ******************************************************************************/

#ifndef TLX_STRING_HEADER
#define TLX_STRING_HEADER

//! \defgroup tlx_string String Algorithms
//! Simple string manipulations

/*[[[perl
my %exclude = ("tlx/string/appendline.hpp" => 1,
               "tlx/string/ssprintf_generic.hpp" => 1);
print "#include <$_>\n"
    foreach grep {!$exclude{$_}} sort glob("tlx/string/"."*.hpp");
]]]*/
#include <tlx/string/base64.hpp>
#include <tlx/string/bitdump.hpp>
#include <tlx/string/compare_icase.hpp>
#include <tlx/string/contains.hpp>
#include <tlx/string/contains_word.hpp>
#include <tlx/string/ends_with.hpp>
#include <tlx/string/equal_icase.hpp>
#include <tlx/string/erase_all.hpp>
#include <tlx/string/escape_html.hpp>
#include <tlx/string/escape_uri.hpp>
#include <tlx/string/expand_environment_variables.hpp>
#include <tlx/string/extract_between.hpp>
#include <tlx/string/format_iec_units.hpp>
#include <tlx/string/format_si_iec_units.hpp>
#include <tlx/string/format_si_units.hpp>
#include <tlx/string/hash_djb2.hpp>
#include <tlx/string/hash_sdbm.hpp>
#include <tlx/string/hexdump.hpp>
#include <tlx/string/index_of.hpp>
#include <tlx/string/join.hpp>
#include <tlx/string/join_generic.hpp>
#include <tlx/string/join_quoted.hpp>
#include <tlx/string/less_icase.hpp>
#include <tlx/string/levenshtein.hpp>
#include <tlx/string/pad.hpp>
#include <tlx/string/parse_si_iec_units.hpp>
#include <tlx/string/parse_uri.hpp>
#include <tlx/string/parse_uri_form_data.hpp>
#include <tlx/string/replace.hpp>
#include <tlx/string/split.hpp>
#include <tlx/string/split_quoted.hpp>
#include <tlx/string/split_view.hpp>
#include <tlx/string/split_words.hpp>
#include <tlx/string/ssprintf.hpp>
#include <tlx/string/starts_with.hpp>
#include <tlx/string/to_lower.hpp>
#include <tlx/string/to_upper.hpp>
#include <tlx/string/trim.hpp>
#include <tlx/string/union_words.hpp>
#include <tlx/string/word_wrap.hpp>
// [[[end]]]

#endif // !TLX_STRING_HEADER

/******************************************************************************/
