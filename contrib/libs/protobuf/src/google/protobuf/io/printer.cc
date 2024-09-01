// Protocol Buffers - Google's data interchange format
// Copyright 2008 Google Inc.  All rights reserved.
// https://developers.google.com/protocol-buffers/
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
//     * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//     * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

// Author: kenton@google.com (Kenton Varda)
//  Based on original Protocol Buffers design by
//  Sanjay Ghemawat, Jeff Dean, and others.

#include "google/protobuf/io/printer.h"

#include <stdlib.h>

#include <cstddef>
#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "y_absl/container/flat_hash_map.h"
#include "y_absl/log/absl_check.h"
#include "y_absl/log/absl_log.h"
#include "y_absl/strings/ascii.h"
#include "y_absl/strings/escaping.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/str_format.h"
#include "y_absl/strings/str_split.h"
#include "y_absl/strings/string_view.h"
#include "y_absl/strings/strip.h"
#include "y_absl/types/optional.h"
#include "y_absl/types/span.h"
#include "y_absl/types/variant.h"

namespace google {
namespace protobuf {
namespace io {
namespace {
template <typename T>
y_absl::optional<T> LookupInFrameStack(
    y_absl::string_view var,
    y_absl::Span<std::function<y_absl::optional<T>(y_absl::string_view)>> frames) {
  for (size_t i = frames.size(); i >= 1; --i) {
    auto val = frames[i - 1](var);
    if (val.has_value()) {
      return val;
    }
  }
  return y_absl::nullopt;
}
}  // namespace

struct Printer::Format {
  struct Chunk {
    // The chunk's text; if this is a variable, it does not include the $...$.
    y_absl::string_view text;

    // Whether or not this is a variable name, i.e., a $...$.
    bool is_var;
  };

  struct Line {
    // Chunks to emit, split along $ and annotates as to whether it is a
    // variable name.
    std::vector<Chunk> chunks;

    // The indentation for this chunk.
    size_t indent;
  };

  std::vector<Line> lines;

  // Whether this is a multiline raw string, according to internal heuristics.
  bool is_raw_string = false;
};

Printer::Format Printer::TokenizeFormat(y_absl::string_view format_string,
                                        const PrintOptions& options) {
  Format format;
  size_t raw_string_indent = 0;
  if (options.strip_raw_string_indentation) {
    // We are processing a call that looks like
    //
    // p->Emit(R"cc(
    //   class Foo {
    //     int x, y, z;
    //   };
    // )cc");
    //
    // or
    //
    // p->Emit(R"cc(
    //
    //   class Foo {
    //     int x, y, z;
    //   };
    // )cc");
    //
    // To compute the indent, we need:
    //   1. Iterate over each line.
    //   2. Find the first line that contains non-whitespace characters.
    //   3. Count the number of leading spaces on that line.
    //
    // The following pairs of loops assume the current line is the first line
    // with non-whitespace characters; if we consume all the spaces and
    // then immediately hit a newline, this means this wasn't the right line and
    // we should start over.
    //
    // Note that the very first character *must* be a newline; that is how we
    // detect that this is a multi-line raw string template, and as such this is
    // a while loop, not a do/while loop.

    y_absl::string_view orig = format_string;
    while (y_absl::ConsumePrefix(&format_string, "\n")) {
      raw_string_indent = 0;
      format.is_raw_string = true;
      while (y_absl::ConsumePrefix(&format_string, " ")) {
        ++raw_string_indent;
      }
    }

    // If we consume the entire string, this probably wasn't a raw string and
    // was probably something like a couple of explicit newlines.
    if (format_string.empty()) {
      format_string = orig;
      format.is_raw_string = false;
      raw_string_indent = 0;
    }
  }

  // We now split the remaining format string into lines and discard:
  //   1. A trailing Printer-discarded comment, if this is a raw string.
  //
  //   2. All leading spaces to compute that line's indent.
  //      We do not do this for the first line, so that Emit("  ") works
  //      correctly. We do this *regardless* of whether we are processing
  //      a raw string, because existing non-raw-string calls to cpp::Formatter
  //      rely on this. There is a test that validates this behavior.
  //
  //   3. Set the indent for that line to max(0, line_indent -
  //      raw_string_indent), if this is not a raw string.
  //
  //   4. Trailing empty lines, if we know this is a raw string, except for
  //      a single extra newline at the end.
  //
  // Each line is itself split into chunks along the variable delimiters, e.g.
  // $...$.
  bool is_first = true;
  for (y_absl::string_view line_text : y_absl::StrSplit(format_string, '\n')) {
    if (format.is_raw_string) {
      size_t comment_index = line_text.find(options_.ignored_comment_start);
      line_text = line_text.substr(0, comment_index);
    }

    size_t line_indent = 0;
    while (!is_first && y_absl::ConsumePrefix(&line_text, " ")) {
      ++line_indent;
    }
    is_first = false;

    format.lines.emplace_back();
    auto& line = format.lines.back();
    line.indent =
        line_indent > raw_string_indent ? line_indent - raw_string_indent : 0;

    bool is_var = false;
    size_t total_len = 0;
    for (y_absl::string_view chunk :
         y_absl::StrSplit(line_text, options_.variable_delimiter)) {
      // The special _start and _end variables should actually glom the next
      // chunk into themselves, so as to be of the form _start$foo and _end$foo.
      if (!line.chunks.empty() && !is_var) {
        auto& prev = line.chunks.back();
        if (prev.text == "_start" || prev.text == "_end") {
          // The +1 below is to account for the $ in between them.
          // This string is safe, because prev.text and chunk are contiguous
          // by construction.
          prev.text = y_absl::string_view(prev.text.data(),
                                        prev.text.size() + 1 + chunk.size());

          // Account for the foo$ part of $_start$foo$.
          total_len += chunk.size() + 1;
          continue;
        }
      }

      if (is_var || !chunk.empty()) {
        line.chunks.push_back(Format::Chunk{chunk, is_var});
      }

      total_len += chunk.size();
      if (is_var) {
        // This accounts for the $s around a variable.
        total_len += 2;
      }

      is_var = !is_var;
    }

    // To ensure there are no unclosed $...$, we check that the computed length
    // above equals the actual length of the string. If it's off, that means
    // that there are missing or extra $ characters.
    Validate(total_len == line_text.size(), options, [&line] {
      if (line.chunks.empty()) {
        return TProtoStringType("wrong number of variable delimiters");
      }

      return y_absl::StrFormat("unclosed variable name: `%s`",
                             y_absl::CHexEscape(line.chunks.back().text));
    });

    // Trim any empty, non-variable chunks.
    while (!line.chunks.empty()) {
      auto& last = line.chunks.back();
      if (last.is_var || !last.text.empty()) {
        break;
      }

      line.chunks.pop_back();
    }
  }

  // Discard any trailing newlines (i.e., lines which contain no chunks.)
  if (format.is_raw_string) {
    while (!format.lines.empty() && format.lines.back().chunks.empty()) {
      format.lines.pop_back();
    }
  }

#if 0  // Use this to aid debugging tokenization.
  LOG(INFO) << "--- " << format.lines.size() << " lines";
  for (size_t i = 0; i < format.lines.size(); ++i) {
    const auto& line = format.lines[i];

    auto log_line = y_absl::StrFormat("[\" \" x %d]", line.indent);
    for (const auto& chunk : line.chunks) {
      y_absl::StrAppendFormat(&log_line, " %s\"%s\"", chunk.is_var ? "$" : "",
                            y_absl::CHexEscape(chunk.text));
    }
    LOG(INFO) << log_line;
  }
  LOG(INFO) << "---";
#endif

  return format;
}

constexpr y_absl::string_view Printer::kProtocCodegenTrace;

Printer::Printer(ZeroCopyOutputStream* output, Options options)
    : sink_(output), options_(options) {
  if (!options_.enable_codegen_trace.has_value()) {
    // Trace-by-default is threaded through via an env var, rather than a
    // global, so that child processes can pick it up as well. The flag
    // --enable_codegen_trace setenv()'s this in protoc's startup code.
    static const bool kEnableCodegenTrace =
        ::getenv(kProtocCodegenTrace.data()) != nullptr;
    options_.enable_codegen_trace = kEnableCodegenTrace;
  }
}

y_absl::string_view Printer::LookupVar(y_absl::string_view var) {
  auto result = LookupInFrameStack(var, y_absl::MakeSpan(var_lookups_));
  Y_ABSL_CHECK(result.has_value()) << "could not find " << var;

  auto* view = result->AsString();
  Y_ABSL_CHECK(view != nullptr)
      << "could not find " << var << "; found callback instead";

  return *view;
}

bool Printer::Validate(bool cond, Printer::PrintOptions opts,
                       y_absl::FunctionRef<TProtoStringType()> message) {
  if (!cond) {
    if (opts.checks_are_debug_only) {
      Y_ABSL_DLOG(FATAL) << message();
    } else {
      Y_ABSL_LOG(FATAL) << message();
    }
  }
  return cond;
}

bool Printer::Validate(bool cond, Printer::PrintOptions opts,
                       y_absl::string_view message) {
  return Validate(cond, opts, [=] { return TProtoStringType(message); });
}

// This function is outlined to isolate the use of
// Y_ABSL_CHECK into the .cc file.
void Printer::Outdent() {
  PrintOptions opts;
  opts.checks_are_debug_only = true;
  if (!Validate(indent_ >= options_.spaces_per_indent, opts,
                "Outdent() without matching Indent()")) {
    return;
  }
  indent_ -= options_.spaces_per_indent;
}

void Printer::Emit(y_absl::Span<const Sub> vars, y_absl::string_view format,
                   SourceLocation loc) {
  PrintOptions opts;
  opts.strip_raw_string_indentation = true;
  opts.loc = loc;

  auto defs = WithDefs(vars, /*allow_callbacks=*/true);

  PrintImpl(format, {}, opts);
}

y_absl::optional<std::pair<size_t, size_t>> Printer::GetSubstitutionRange(
    y_absl::string_view varname, PrintOptions opts) {
  auto it = substitutions_.find(varname);
  if (!Validate(it != substitutions_.end(), opts, [varname] {
        return y_absl::StrCat("undefined variable in annotation: ", varname);
      })) {
    return y_absl::nullopt;
  }

  std::pair<size_t, size_t> range = it->second;
  if (!Validate(range.first <= range.second, opts, [range, varname] {
        return y_absl::StrFormat(
            "variable used for annotation used multiple times: %s (%d..%d)",
            varname, range.first, range.second);
      })) {
    return y_absl::nullopt;
  }

  return range;
}

void Printer::Annotate(y_absl::string_view begin_varname,
                       y_absl::string_view end_varname,
                       y_absl::string_view file_path,
                       const std::vector<int>& path) {
  if (options_.annotation_collector == nullptr) {
    return;
  }

  PrintOptions opts;
  opts.checks_are_debug_only = true;
  auto begin = GetSubstitutionRange(begin_varname, opts);
  auto end = GetSubstitutionRange(end_varname, opts);
  if (!begin.has_value() || !end.has_value()) {
    return;
  }
  if (begin->first > end->second) {
    Y_ABSL_DLOG(FATAL) << "annotation has negative length from " << begin_varname
                     << " to " << end_varname;
    return;
  }
  options_.annotation_collector->AddAnnotation(begin->first, end->second,
                                               TProtoStringType(file_path), path);
}

void Printer::WriteRaw(const char* data, size_t size) {
  if (failed_ || size == 0) {
    return;
  }

  if (at_start_of_line_ && data[0] != '\n') {
    IndentIfAtStart();
    if (failed_) {
      return;
    }

    // Fix up empty variables (e.g., "{") that should be annotated as
    // coming after the indent.
    for (const TProtoStringType& var : line_start_variables_) {
      auto& pair = substitutions_[var];
      pair.first += indent_;
      pair.second += indent_;
    }
  }

  // If we're going to write any data, clear line_start_variables_, since
  // we've either updated them in the block above or they no longer refer to
  // the current line.
  line_start_variables_.clear();

  sink_.Append(data, size);
  failed_ |= sink_.failed();
}

void Printer::IndentIfAtStart() {
  if (!at_start_of_line_) {
    return;
  }

  for (size_t i = 0; i < indent_; ++i) {
    sink_.Write(" ");
  }
  at_start_of_line_ = false;
}

void Printer::PrintCodegenTrace(y_absl::optional<SourceLocation> loc) {
  if (!options_.enable_codegen_trace.value_or(false) || !loc.has_value()) {
    return;
  }

  if (!at_start_of_line_) {
    at_start_of_line_ = true;
    line_start_variables_.clear();
    sink_.Write("\n");
  }

  PrintRaw(y_absl::StrFormat("%s @%s:%d\n", options_.comment_start,
                           loc->file_name(), loc->line()));
  at_start_of_line_ = true;
}

bool Printer::ValidateIndexLookupInBounds(size_t index,
                                          size_t current_arg_index,
                                          size_t args_len, PrintOptions opts) {
  if (!Validate(index < args_len, opts, [this, index] {
        return y_absl::StrFormat("annotation %c{%d%c is out of bounds",
                               options_.variable_delimiter, index + 1,
                               options_.variable_delimiter);
      })) {
    return false;
  }
  if (!Validate(
          index <= current_arg_index, opts, [this, index, current_arg_index] {
            return y_absl::StrFormat(
                "annotation arg must be in correct order as given; expected "
                "%c{%d%c but got %c{%d%c",
                options_.variable_delimiter, current_arg_index + 1,
                options_.variable_delimiter, options_.variable_delimiter,
                index + 1, options_.variable_delimiter);
          })) {
    return false;
  }
  return true;
}

void Printer::PrintImpl(y_absl::string_view format,
                        y_absl::Span<const TProtoStringType> args, PrintOptions opts) {
  // Inside of this function, we set indentation as we print new lines from
  // the format string. No matter how we exit this function, we should fix up
  // the indent to what it was before we entered; a cleanup makes it easy to
  // avoid this mistake.
  size_t original_indent = indent_;
  auto unindent =
      y_absl::MakeCleanup([this, original_indent] { indent_ = original_indent; });

  y_absl::string_view original = format;

  line_start_variables_.clear();

  if (opts.use_substitution_map) {
    substitutions_.clear();
  }

  auto fmt = TokenizeFormat(format, opts);
  PrintCodegenTrace(opts.loc);

  size_t arg_index = 0;
  bool skip_next_newline = false;
  std::vector<AnnotationCollector::Annotation> annot_stack;
  std::vector<std::pair<y_absl::string_view, size_t>> annot_records;
  for (size_t line_idx = 0; line_idx < fmt.lines.size(); ++line_idx) {
    const auto& line = fmt.lines[line_idx];

    // We only print a newline for lines that follow the first; a loop iteration
    // can also hint that we should not emit another newline through the
    // `skip_next_newline` variable.
    //
    // We also assume that double newlines are undesirable, so we
    // do not emit a newline if we are at the beginning of a line, *unless* the
    // previous format line is actually empty. This behavior is specific to
    // raw strings.
    if (line_idx > 0) {
      bool prev_was_empty = fmt.lines[line_idx - 1].chunks.empty();
      bool should_skip_newline =
          skip_next_newline ||
          (fmt.is_raw_string && (at_start_of_line_ && !prev_was_empty));
      if (!should_skip_newline) {
        line_start_variables_.clear();
        sink_.Write("\n");
        at_start_of_line_ = true;
      }
    }
    skip_next_newline = false;

    indent_ = original_indent + line.indent;

    for (size_t chunk_idx = 0; chunk_idx < line.chunks.size(); ++chunk_idx) {
      auto chunk = line.chunks[chunk_idx];

      if (!chunk.is_var) {
        PrintRaw(chunk.text);
        continue;
      }

      if (chunk.text.empty()) {
        // `$$` is an escape for just `$`.
        WriteRaw(&options_.variable_delimiter, 1);
        continue;
      }

      // If we get this far, we can conclude the chunk is a substitution
      // variable; we rename the `chunk` variable to make this clear below.
      y_absl::string_view var = chunk.text;
      if (opts.use_curly_brace_substitutions &&
          y_absl::ConsumePrefix(&var, "{")) {
        if (!Validate(var.size() == 1u, opts,
                      "expected single-digit variable")) {
          continue;
        }

        if (!Validate(y_absl::ascii_isdigit(var[0]), opts,
                      "expected digit after {")) {
          continue;
        }

        size_t idx = var[0] - '1';
        if (!ValidateIndexLookupInBounds(idx, arg_index, args.size(), opts)) {
          continue;
        }

        if (idx == arg_index) {
          ++arg_index;
        }

        IndentIfAtStart();
        annot_stack.push_back({{sink_.bytes_written(), 0}, args[idx]});
        continue;
      }

      if (opts.use_curly_brace_substitutions &&
          y_absl::ConsumePrefix(&var, "}")) {
        // The rest of var is actually ignored, and this is apparently
        // public API now. Oops?
        if (!Validate(!annot_stack.empty(), opts,
                      "unexpected end of annotation")) {
          continue;
        }

        annot_stack.back().first.second = sink_.bytes_written();
        if (options_.annotation_collector != nullptr) {
          options_.annotation_collector->AddAnnotationNew(annot_stack.back());
        }
        annot_stack.pop_back();
        continue;
      }

      y_absl::string_view prefix, suffix;
      if (opts.strip_spaces_around_vars) {
        var = y_absl::StripLeadingAsciiWhitespace(var);
        prefix = chunk.text.substr(0, chunk.text.size() - var.size());
        var = y_absl::StripTrailingAsciiWhitespace(var);
        suffix = chunk.text.substr(prefix.size() + var.size());
      }

      if (!Validate(!var.empty(), opts, "unexpected empty variable")) {
        continue;
      }

      bool is_start = y_absl::ConsumePrefix(&var, "_start$");
      bool is_end = y_absl::ConsumePrefix(&var, "_end$");
      if (opts.use_annotation_frames && (is_start || is_end)) {
        if (is_start) {
          IndentIfAtStart();
          annot_records.push_back({var, sink_.bytes_written()});

          // Skip all whitespace immediately after a _start.
          ++chunk_idx;
          if (chunk_idx < line.chunks.size()) {
            y_absl::string_view text = line.chunks[chunk_idx].text;
            while (y_absl::ConsumePrefix(&text, " ")) {
            }
            PrintRaw(text);
          }
        } else {
          // If a line consisted *only* of an _end, this will likely result in
          // a blank line if we do not zap the newline after it, so we do that
          // here.
          if (line.chunks.size() == 1) {
            skip_next_newline = true;
          }

          auto record_var = annot_records.back();
          annot_records.pop_back();

          if (!Validate(record_var.first == var, opts, [record_var, var] {
                return y_absl::StrFormat(
                    "_start and _end variables must match, but got %s and %s, "
                    "respectively",
                    record_var.first, var);
              })) {
            continue;
          }

          y_absl::optional<AnnotationRecord> record =
              LookupInFrameStack(var, y_absl::MakeSpan(annotation_lookups_));

          if (!Validate(record.has_value(), opts, [var] {
                return y_absl::StrCat("undefined annotation variable: \"",
                                    y_absl::CHexEscape(var), "\"");
              })) {
            continue;
          }

          if (options_.annotation_collector != nullptr) {
            options_.annotation_collector->AddAnnotation(
                record_var.second, sink_.bytes_written(), record->file_path,
                record->path, record->semantic);
          }
        }

        continue;
      }

      y_absl::optional<ValueView> sub;
      y_absl::optional<AnnotationRecord> same_name_record;
      if (opts.allow_digit_substitutions && y_absl::ascii_isdigit(var[0])) {
        if (!Validate(var.size() == 1u, opts,
                      "expected single-digit variable")) {
          continue;
        }

        size_t idx = var[0] - '1';
        if (!ValidateIndexLookupInBounds(idx, arg_index, args.size(), opts)) {
          continue;
        }
        if (idx == arg_index) {
          ++arg_index;
        }
        sub = args[idx];
      } else {
        sub = LookupInFrameStack(var, y_absl::MakeSpan(var_lookups_));

        if (opts.use_annotation_frames) {
          same_name_record =
              LookupInFrameStack(var, y_absl::MakeSpan(annotation_lookups_));
        }
      }

      // By returning here in case of empty we also skip possible spaces inside
      // the $...$, i.e. "void$ dllexpor$ f();" -> "void f();" in the empty
      // case.
      if (!Validate(sub.has_value(), opts, [var] {
            return y_absl::StrCat("undefined variable: \"", y_absl::CHexEscape(var),
                                "\"");
          })) {
        continue;
      }

      size_t range_start = sink_.bytes_written();
      size_t range_end = sink_.bytes_written();

      if (const y_absl::string_view* str = sub->AsString()) {
        if (at_start_of_line_ && str->empty()) {
          line_start_variables_.emplace_back(var);
        }

        if (!str->empty()) {
          // If `sub` is empty, we do not print the spaces around it.
          PrintRaw(prefix);
          PrintRaw(*str);
          range_end = sink_.bytes_written();
          range_start = range_end - str->size();
          PrintRaw(suffix);
        }
      } else {
        const ValueView::Callback* fnc = sub->AsCallback();
        Y_ABSL_CHECK(fnc != nullptr);

        Validate(
            prefix.empty() && suffix.empty(), opts,
            "substitution that resolves to callback cannot contain whitespace");

        range_start = sink_.bytes_written();
        Y_ABSL_CHECK((*fnc)())
            << "recursive call encountered while evaluating \"" << var << "\"";
        range_end = sink_.bytes_written();
      }

      // If we just evaluated a value which specifies end-of-line consume-after
      // characters, and we're at the start of a line, that means we finished
      // with a newline.
      //
      // We trim a single end-of-line `consume_after` character in this case.
      //
      // This helps callback formatting "work as expected" with respect to forms
      // like
      //
      //   class Foo {
      //     $methods$;
      //   };
      //
      // Without this post-processing, it would turn into
      //
      //   class Foo {
      //     void Bar() {};
      //   };
      //
      // in many cases. Without the `;`, clang-format may format the template
      // incorrectly.
      auto next_idx = chunk_idx + 1;
      if (!sub->consume_after.empty() && next_idx < line.chunks.size() &&
          !line.chunks[next_idx].is_var) {
        chunk_idx = next_idx;

        y_absl::string_view text = line.chunks[chunk_idx].text;
        for (char c : sub->consume_after) {
          if (y_absl::ConsumePrefix(&text, y_absl::string_view(&c, 1))) {
            break;
          }
        }

        PrintRaw(text);
      }

      if (same_name_record.has_value() &&
          options_.annotation_collector != nullptr) {
        options_.annotation_collector->AddAnnotation(
            range_start, range_end, same_name_record->file_path,
            same_name_record->path, same_name_record->semantic);
      }

      if (opts.use_substitution_map) {
        auto insertion =
            substitutions_.emplace(var, std::make_pair(range_start, range_end));

        if (!insertion.second) {
          // This variable was used multiple times.
          // Make its span have negative length so
          // we can detect it if it gets used in an
          // annotation.
          insertion.first->second = {1, 0};
        }
      }
    }
  }

  Validate(arg_index == args.size(), opts,
           [original] { return y_absl::StrCat("unused args: ", original); });
  Validate(annot_stack.empty(), opts, [this, original] {
    return y_absl::StrFormat(
        "annotation range was not closed; expected %c}%c: %s",
        options_.variable_delimiter, options_.variable_delimiter, original);
  });

  // For multiline raw strings, we always make sure to end on a newline.
  if (fmt.is_raw_string && !at_start_of_line_) {
    PrintRaw("\n");
    at_start_of_line_ = true;
  }
}
}  // namespace io
}  // namespace protobuf
}  // namespace google
