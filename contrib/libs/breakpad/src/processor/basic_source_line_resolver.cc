// Copyright (c) 2010 Google Inc.
// All rights reserved.
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
//
// basic_source_line_resolver.cc: BasicSourceLineResolver implementation.
//
// See basic_source_line_resolver.h and basic_source_line_resolver_types.h
// for documentation.

#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <limits>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "google_breakpad/processor/basic_source_line_resolver.h"
#include "processor/basic_source_line_resolver_types.h"
#include "processor/module_factory.h"

#include "processor/tokenize.h"

using std::deque;
using std::make_pair;
using std::map;
using std::unique_ptr;
using std::vector;

namespace google_breakpad {

#ifdef _WIN32
#ifdef _MSC_VER
#define strtok_r strtok_s
#endif
#define strtoull _strtoui64
#endif

namespace {

// Utility function to tokenize given the presence of an optional initial
// field. In this case, optional_field is the expected string for the optional
// field, and max_tokens is the maximum number of tokens including the optional
// field. Refer to the documentation for Tokenize for descriptions of the other
// arguments.
bool TokenizeWithOptionalField(char* line,
                               const char* optional_field,
                               const char* separators,
                               int max_tokens,
                               vector<char*>* tokens) {
  // First tokenize assuming the optional field is not present.  If we then see
  // the optional field, additionally tokenize the last token into two tokens.
  if (!Tokenize(line, separators, max_tokens - 1, tokens)) {
    return false;
  }

  if (strcmp(tokens->front(), optional_field) == 0) {
    // The optional field is present. Split the last token in two to recover the
    // field prior to the last.
    vector<char*> last_tokens;
    if (!Tokenize(tokens->back(), separators, 2, &last_tokens)) {
      return false;
    }
    // Replace the previous last token with the two new tokens.
    tokens->pop_back();
    tokens->push_back(last_tokens[0]);
    tokens->push_back(last_tokens[1]);
  }

  return true;
}

}  // namespace

static const char* kWhitespace = " \r\n";
static const int kMaxErrorsPrinted = 5;
static const int kMaxErrorsBeforeBailing = 100;

BasicSourceLineResolver::BasicSourceLineResolver() :
    SourceLineResolverBase(new BasicModuleFactory) { }

// static
void BasicSourceLineResolver::Module::LogParseError(
   const string& message,
   int line_number,
   int* num_errors) {
  if (++(*num_errors) <= kMaxErrorsPrinted) {
    if (line_number > 0) {
      BPLOG(ERROR) << "Line " << line_number << ": " << message;
    } else {
      BPLOG(ERROR) << message;
    }
  }
}

bool BasicSourceLineResolver::Module::LoadMapFromMemory(
    char* memory_buffer,
    size_t memory_buffer_size) {
  linked_ptr<Function> cur_func;
  int line_number = 0;
  int num_errors = 0;
  int inline_num_errors = 0;
  char* save_ptr;

  // If the length is 0, we can still pretend we have a symbol file. This is
  // for scenarios that want to test symbol lookup, but don't necessarily care
  // if certain modules do not have any information, like system libraries.
  if (memory_buffer_size == 0) {
    return true;
  }

  // Make sure the last character is null terminator.
  size_t last_null_terminator = memory_buffer_size - 1;
  if (memory_buffer[last_null_terminator] != '\0') {
    memory_buffer[last_null_terminator] = '\0';
  }

  // Skip any null terminators at the end of the memory buffer, and make sure
  // there are no other null terminators in the middle of the memory buffer.
  bool has_null_terminator_in_the_middle = false;
  while (last_null_terminator > 0 &&
         memory_buffer[last_null_terminator - 1] == '\0') {
    last_null_terminator--;
  }
  for (size_t i = 0; i < last_null_terminator; i++) {
    if (memory_buffer[i] == '\0') {
      memory_buffer[i] = '_';
      has_null_terminator_in_the_middle = true;
    }
  }
  if (has_null_terminator_in_the_middle) {
    LogParseError(
       "Null terminator is not expected in the middle of the symbol data",
       line_number,
       &num_errors);
  }

  char* buffer;
  buffer = strtok_r(memory_buffer, "\r\n", &save_ptr);

  while (buffer != NULL) {
    ++line_number;

    if (strncmp(buffer, "FILE ", 5) == 0) {
      if (!ParseFile(buffer)) {
        LogParseError("ParseFile on buffer failed", line_number, &num_errors);
      }
    } else if (strncmp(buffer, "STACK ", 6) == 0) {
      if (!ParseStackInfo(buffer)) {
        LogParseError("ParseStackInfo failed", line_number, &num_errors);
      }
    } else if (strncmp(buffer, "FUNC ", 5) == 0) {
      cur_func.reset(ParseFunction(buffer));
      if (!cur_func.get()) {
        LogParseError("ParseFunction failed", line_number, &num_errors);
      } else {
        // StoreRange will fail if the function has an invalid address or size.
        // We'll silently ignore this, the function and any corresponding lines
        // will be destroyed when cur_func is released.
        functions_.StoreRange(cur_func->address, cur_func->size, cur_func);
      }
    } else if (strncmp(buffer, "PUBLIC ", 7) == 0) {
      // Clear cur_func: public symbols don't contain line number information.
      cur_func.reset();

      if (!ParsePublicSymbol(buffer)) {
        LogParseError("ParsePublicSymbol failed", line_number, &num_errors);
      }
    } else if (strncmp(buffer, "MODULE ", 7) == 0) {
      // Ignore these.  They're not of any use to BasicSourceLineResolver,
      // which is fed modules by a SymbolSupplier.  These lines are present to
      // aid other tools in properly placing symbol files so that they can
      // be accessed by a SymbolSupplier.
      //
      // MODULE <guid> <age> <filename>
    } else if (strncmp(buffer, "INFO ", 5) == 0) {
      // Ignore these as well, they're similarly just for housekeeping.
      //
      // INFO CODE_ID <code id> <filename>
    } else if (strncmp(buffer, "INLINE ", 7) == 0) {
      linked_ptr<Inline> in = ParseInline(buffer);
      if (!in.get())
        LogParseError("ParseInline failed", line_number, &inline_num_errors);
      else
        cur_func->AppendInline(in);
    } else if (strncmp(buffer, "INLINE_ORIGIN ", 14) == 0) {
      if (!ParseInlineOrigin(buffer)) {
        LogParseError("ParseInlineOrigin failed", line_number,
                      &inline_num_errors);
      }
    } else {
      if (!cur_func.get()) {
        LogParseError("Found source line data without a function",
                       line_number, &num_errors);
      } else {
        Line* line = ParseLine(buffer);
        if (!line) {
          LogParseError("ParseLine failed", line_number, &num_errors);
        } else {
          cur_func->lines.StoreRange(line->address, line->size,
                                     linked_ptr<Line>(line));
        }
      }
    }
    if (num_errors > kMaxErrorsBeforeBailing) {
      break;
    }
    buffer = strtok_r(NULL, "\r\n", &save_ptr);
  }
  is_corrupt_ = num_errors > 0;
  return true;
}

void BasicSourceLineResolver::Module::ConstructInlineFrames(
    StackFrame* frame,
    MemAddr address,
    const ContainedRangeMap<uint64_t, linked_ptr<Inline>>& inline_map,
    deque<unique_ptr<StackFrame>>* inlined_frames) const {
  vector<const linked_ptr<Inline>*> inlines;
  if (!inline_map.RetrieveRanges(address, inlines)) {
    return;
  }

  for (const linked_ptr<Inline>* const in : inlines) {
    unique_ptr<StackFrame> new_frame =
        unique_ptr<StackFrame>(new StackFrame(*frame));
    auto origin = inline_origins_.find(in->get()->origin_id);
    if (origin != inline_origins_.end()) {
      new_frame->function_name = origin->second->name;
    } else {
      new_frame->function_name = "<name omitted>";
    }
    
    // Store call site file and line in current frame, which will be updated
    // later.
    new_frame->source_line = in->get()->call_site_line;
    if (in->get()->has_call_site_file_id) {
      auto file = files_.find(in->get()->call_site_file_id);
      if (file != files_.end()) {
        new_frame->source_file_name = file->second;
      }
    }

    // Use the starting address of the inlined range as inlined function base.
    new_frame->function_base = new_frame->module->base_address();
    for (const auto& range : in->get()->inline_ranges) {
      if (address >= range.first && address < range.first + range.second) {
        new_frame->function_base += range.first;
        break;
      }
    }
    new_frame->trust = StackFrame::FRAME_TRUST_INLINE;

    // The inlines vector has an order from innermost entry to outermost entry.
    // By push_back, we will have inlined_frames with the same order.
    inlined_frames->push_back(std::move(new_frame));
  }

  // Update the source file and source line for each inlined frame.
  if (!inlined_frames->empty()) {
    string parent_frame_source_file_name = frame->source_file_name;
    int parent_frame_source_line = frame->source_line;
    frame->source_file_name = inlined_frames->back()->source_file_name;
    frame->source_line = inlined_frames->back()->source_line;
    for (unique_ptr<StackFrame>& inlined_frame : *inlined_frames) {
      std::swap(inlined_frame->source_file_name, parent_frame_source_file_name);
      std::swap(inlined_frame->source_line, parent_frame_source_line);
    }
  }
}

void BasicSourceLineResolver::Module::LookupAddress(
    StackFrame* frame,
    deque<unique_ptr<StackFrame>>* inlined_frames) const {
  MemAddr address = frame->instruction - frame->module->base_address();

  // First, look for a FUNC record that covers address. Use
  // RetrieveNearestRange instead of RetrieveRange so that, if there
  // is no such function, we can use the next function to bound the
  // extent of the PUBLIC symbol we find, below. This does mean we
  // need to check that address indeed falls within the function we
  // find; do the range comparison in an overflow-friendly way.
  linked_ptr<Function> func;
  linked_ptr<PublicSymbol> public_symbol;
  MemAddr function_base;
  MemAddr function_size;
  MemAddr public_address;
  if (functions_.RetrieveNearestRange(address, &func, &function_base,
                                      NULL /* delta */, &function_size) &&
      address >= function_base && address - function_base < function_size) {
    frame->function_name = func->name;
    frame->function_base = frame->module->base_address() + function_base;
    frame->is_multiple = func->is_multiple;

    linked_ptr<Line> line;
    MemAddr line_base;
    if (func->lines.RetrieveRange(address, &line, &line_base, NULL /* delta */,
                                  NULL /* size */)) {
      FileMap::const_iterator it = files_.find(line->source_file_id);
      if (it != files_.end()) {
        frame->source_file_name = files_.find(line->source_file_id)->second;
      }
      frame->source_line = line->line;
      frame->source_line_base = frame->module->base_address() + line_base;
    }

    // Check if this is inlined function call.
    if (inlined_frames) {
      ConstructInlineFrames(frame, address, func->inlines, inlined_frames);
    }
  } else if (public_symbols_.Retrieve(address,
                                      &public_symbol, &public_address) &&
             (!func.get() || public_address > function_base)) {
    frame->function_name = public_symbol->name;
    frame->function_base = frame->module->base_address() + public_address;
    frame->is_multiple = public_symbol->is_multiple;
  }
}

WindowsFrameInfo* BasicSourceLineResolver::Module::FindWindowsFrameInfo(
    const StackFrame* frame) const {
  MemAddr address = frame->instruction - frame->module->base_address();
  scoped_ptr<WindowsFrameInfo> result(new WindowsFrameInfo());

  // We only know about WindowsFrameInfo::STACK_INFO_FRAME_DATA and
  // WindowsFrameInfo::STACK_INFO_FPO. Prefer them in this order.
  // WindowsFrameInfo::STACK_INFO_FRAME_DATA is the newer type that
  // includes its own program string.
  // WindowsFrameInfo::STACK_INFO_FPO is the older type
  // corresponding to the FPO_DATA struct. See stackwalker_x86.cc.
  linked_ptr<WindowsFrameInfo> frame_info;
  if ((windows_frame_info_[WindowsFrameInfo::STACK_INFO_FRAME_DATA]
       .RetrieveRange(address, &frame_info))
      || (windows_frame_info_[WindowsFrameInfo::STACK_INFO_FPO]
          .RetrieveRange(address, &frame_info))) {
    result->CopyFrom(*frame_info.get());
    return result.release();
  }

  // Even without a relevant STACK line, many functions contain
  // information about how much space their parameters consume on the
  // stack. Use RetrieveNearestRange instead of RetrieveRange, so that
  // we can use the function to bound the extent of the PUBLIC symbol,
  // below. However, this does mean we need to check that ADDRESS
  // falls within the retrieved function's range; do the range
  // comparison in an overflow-friendly way.
  linked_ptr<Function> function;
  MemAddr function_base, function_size;
  if (functions_.RetrieveNearestRange(address, &function, &function_base,
                                      NULL /* delta */, &function_size) &&
      address >= function_base && address - function_base < function_size) {
    result->parameter_size = function->parameter_size;
    result->valid |= WindowsFrameInfo::VALID_PARAMETER_SIZE;
    return result.release();
  }

  // PUBLIC symbols might have a parameter size. Use the function we
  // found above to limit the range the public symbol covers.
  linked_ptr<PublicSymbol> public_symbol;
  MemAddr public_address;
  if (public_symbols_.Retrieve(address, &public_symbol, &public_address) &&
      (!function.get() || public_address > function_base)) {
    result->parameter_size = public_symbol->parameter_size;
  }

  return NULL;
}

CFIFrameInfo* BasicSourceLineResolver::Module::FindCFIFrameInfo(
    const StackFrame* frame) const {
  MemAddr address = frame->instruction - frame->module->base_address();
  MemAddr initial_base, initial_size;
  string initial_rules;

  // Find the initial rule whose range covers this address. That
  // provides an initial set of register recovery rules. Then, walk
  // forward from the initial rule's starting address to frame's
  // instruction address, applying delta rules.
  if (!cfi_initial_rules_.RetrieveRange(address, &initial_rules, &initial_base,
                                        NULL /* delta */, &initial_size)) {
    return NULL;
  }

  // Create a frame info structure, and populate it with the rules from
  // the STACK CFI INIT record.
  scoped_ptr<CFIFrameInfo> rules(new CFIFrameInfo());
  if (!ParseCFIRuleSet(initial_rules, rules.get()))
    return NULL;

  // Find the first delta rule that falls within the initial rule's range.
  map<MemAddr, string>::const_iterator delta =
    cfi_delta_rules_.lower_bound(initial_base);

  // Apply delta rules up to and including the frame's address.
  while (delta != cfi_delta_rules_.end() && delta->first <= address) {
    ParseCFIRuleSet(delta->second, rules.get());
    delta++;
  }

  return rules.release();
}

bool BasicSourceLineResolver::Module::ParseFile(char* file_line) {
  long index;
  char* filename;
  if (SymbolParseHelper::ParseFile(file_line, &index, &filename)) {
    files_.insert(make_pair(index, string(filename)));
    return true;
  }
  return false;
}

bool BasicSourceLineResolver::Module::ParseInlineOrigin(
  char* inline_origin_line) {
  bool has_file_id;
  long origin_id;
  long source_file_id;
  char* origin_name;
  if (SymbolParseHelper::ParseInlineOrigin(inline_origin_line, &has_file_id,
                                           &origin_id, &source_file_id,
                                           &origin_name)) {
    inline_origins_.insert(make_pair(
        origin_id,
        new InlineOrigin(has_file_id, source_file_id, origin_name)));
    return true;
  }
  return false;
}

linked_ptr<BasicSourceLineResolver::Inline>
BasicSourceLineResolver::Module::ParseInline(char* inline_line) {
  bool has_call_site_file_id;
  long inline_nest_level;
  long call_site_line;
  long call_site_file_id;
  long origin_id;
  vector<std::pair<MemAddr, MemAddr>> ranges;
  if (SymbolParseHelper::ParseInline(inline_line, &has_call_site_file_id,
                                     &inline_nest_level, &call_site_line,
                                     &call_site_file_id, &origin_id, &ranges)) {
    return linked_ptr<Inline>(new Inline(has_call_site_file_id,
                                         inline_nest_level, call_site_line,
                                         call_site_file_id, origin_id, ranges));
  }
  return linked_ptr<Inline>();
}

BasicSourceLineResolver::Function*
BasicSourceLineResolver::Module::ParseFunction(char* function_line) {
  bool is_multiple;
  uint64_t address;
  uint64_t size;
  long stack_param_size;
  char* name;
  if (SymbolParseHelper::ParseFunction(function_line, &is_multiple, &address,
                                       &size, &stack_param_size, &name)) {
    return new Function(name, address, size, stack_param_size, is_multiple);
  }
  return NULL;
}

BasicSourceLineResolver::Line* BasicSourceLineResolver::Module::ParseLine(
    char* line_line) {
  uint64_t address;
  uint64_t size;
  long line_number;
  long source_file;

  if (SymbolParseHelper::ParseLine(line_line, &address, &size, &line_number,
                                   &source_file)) {
    return new Line(address, size, source_file, line_number);
  }
  return NULL;
}

bool BasicSourceLineResolver::Module::ParsePublicSymbol(char* public_line) {
  bool is_multiple;
  uint64_t address;
  long stack_param_size;
  char* name;

  if (SymbolParseHelper::ParsePublicSymbol(public_line, &is_multiple, &address,
                                           &stack_param_size, &name)) {
    // A few public symbols show up with an address of 0.  This has been seen
    // in the dumped output of ntdll.pdb for symbols such as _CIlog, _CIpow,
    // RtlDescribeChunkLZNT1, and RtlReserveChunkLZNT1.  They would conflict
    // with one another if they were allowed into the public_symbols_ map,
    // but since the address is obviously invalid, gracefully accept them
    // as input without putting them into the map.
    if (address == 0) {
      return true;
    }

    linked_ptr<PublicSymbol> symbol(new PublicSymbol(name, address,
                                                     stack_param_size,
                                                     is_multiple));
    return public_symbols_.Store(address, symbol);
  }
  return false;
}

bool BasicSourceLineResolver::Module::ParseStackInfo(char* stack_info_line) {
  // Skip "STACK " prefix.
  stack_info_line += 6;

  // Find the token indicating what sort of stack frame walking
  // information this is.
  while (*stack_info_line == ' ')
    stack_info_line++;
  const char* platform = stack_info_line;
  while (!strchr(kWhitespace, *stack_info_line))
    stack_info_line++;
  *stack_info_line++ = '\0';

  // MSVC stack frame info.
  if (strcmp(platform, "WIN") == 0) {
    int type = 0;
    uint64_t rva, code_size;
    linked_ptr<WindowsFrameInfo>
      stack_frame_info(WindowsFrameInfo::ParseFromString(stack_info_line,
                                                         type,
                                                         rva,
                                                         code_size));
    if (stack_frame_info == NULL)
      return false;

    // TODO(mmentovai): I wanted to use StoreRange's return value as this
    // method's return value, but MSVC infrequently outputs stack info that
    // violates the containment rules.  This happens with a section of code
    // in strncpy_s in test_app.cc (testdata/minidump2).  There, problem looks
    // like this:
    //   STACK WIN 4 4242 1a a 0 ...  (STACK WIN 4 base size prolog 0 ...)
    //   STACK WIN 4 4243 2e 9 0 ...
    // ContainedRangeMap treats these two blocks as conflicting.  In reality,
    // when the prolog lengths are taken into account, the actual code of
    // these blocks doesn't conflict.  However, we can't take the prolog lengths
    // into account directly here because we'd wind up with a different set
    // of range conflicts when MSVC outputs stack info like this:
    //   STACK WIN 4 1040 73 33 0 ...
    //   STACK WIN 4 105a 59 19 0 ...
    // because in both of these entries, the beginning of the code after the
    // prolog is at 0x1073, and the last byte of contained code is at 0x10b2.
    // Perhaps we could get away with storing ranges by rva + prolog_size
    // if ContainedRangeMap were modified to allow replacement of
    // already-stored values.

    windows_frame_info_[type].StoreRange(rva, code_size, stack_frame_info);
    return true;
  } else if (strcmp(platform, "CFI") == 0) {
    // DWARF CFI stack frame info
    return ParseCFIFrameInfo(stack_info_line);
  } else {
    // Something unrecognized.
    return false;
  }
}

bool BasicSourceLineResolver::Module::ParseCFIFrameInfo(
    char* stack_info_line) {
  char* cursor;

  // Is this an INIT record or a delta record?
  char* init_or_address = strtok_r(stack_info_line, " \r\n", &cursor);
  if (!init_or_address)
    return false;

  if (strcmp(init_or_address, "INIT") == 0) {
    // This record has the form "STACK INIT <address> <size> <rules...>".
    char* address_field = strtok_r(NULL, " \r\n", &cursor);
    if (!address_field) return false;

    char* size_field = strtok_r(NULL, " \r\n", &cursor);
    if (!size_field) return false;

    char* initial_rules = strtok_r(NULL, "\r\n", &cursor);
    if (!initial_rules) return false;

    MemAddr address = strtoul(address_field, NULL, 16);
    MemAddr size    = strtoul(size_field,    NULL, 16);
    cfi_initial_rules_.StoreRange(address, size, initial_rules);
    return true;
  }

  // This record has the form "STACK <address> <rules...>".
  char* address_field = init_or_address;
  char* delta_rules = strtok_r(NULL, "\r\n", &cursor);
  if (!delta_rules) return false;
  MemAddr address = strtoul(address_field, NULL, 16);
  cfi_delta_rules_[address] = delta_rules;
  return true;
}

bool BasicSourceLineResolver::Function::AppendInline(linked_ptr<Inline> in) {
  // This happends if in's parent wasn't added due to a malformed INLINE record.
  if (in->inline_nest_level > last_added_inline_nest_level + 1)
    return false;

  last_added_inline_nest_level = in->inline_nest_level;

  // Store all ranges into current level of inlines.
  for (auto range : in->inline_ranges)
    inlines.StoreRange(range.first, range.second, in);
  return true;
}

// static
bool SymbolParseHelper::ParseFile(char* file_line, long* index,
                                  char** filename) {
  // FILE <id> <filename>
  assert(strncmp(file_line, "FILE ", 5) == 0);
  file_line += 5;  // skip prefix

  vector<char*> tokens;
  if (!Tokenize(file_line, kWhitespace, 2, &tokens)) {
    return false;
  }

  char* after_number;
  *index = strtol(tokens[0], &after_number, 10);
  if (!IsValidAfterNumber(after_number) || *index < 0 ||
      *index == std::numeric_limits<long>::max()) {
    return false;
  }

  *filename = tokens[1];
  if (!*filename) {
    return false;
  }

  return true;
}

// static
bool SymbolParseHelper::ParseInlineOrigin(char* inline_origin_line,
                                          bool* has_file_id,
                                          long* origin_id,
                                          long* file_id,
                                          char** name) {
  // Old INLINE_ORIGIN format:
  // INLINE_ORIGIN <origin_id> <file_id> <name>
  // New INLINE_ORIGIN format:
  // INLINE_ORIGIN <origin_id> <name>
  assert(strncmp(inline_origin_line, "INLINE_ORIGIN ", 14) == 0);
  inline_origin_line += 14;  // skip prefix
  vector<char*> tokens;
  // Split the line into two parts so that the first token is "<origin_id>", and
  // second token is either "<file_id> <name>"" or "<name>"" depending on the
  // format version.
  if (!Tokenize(inline_origin_line, kWhitespace, 2, &tokens)) {
    return false;
  }

  char* after_number;
  *origin_id = strtol(tokens[0], &after_number, 10);
  if (!IsValidAfterNumber(after_number) || *origin_id < 0 ||
      *origin_id == std::numeric_limits<long>::max()) {
    return false;
  }

  // If the field after origin_id is a number, then it's old format.
  char* remaining_line = tokens[1];
  *has_file_id = true;
  for (size_t i = 0;
       i < strlen(remaining_line) && remaining_line[i] != ' ' && *has_file_id;
       ++i) {
    // If the file id is -1, it might be an artificial function that doesn't
    // have file id. So, we consider -1 as a valid special case.
    if (remaining_line[i] == '-' && i == 0) {
      continue;
    }
    *has_file_id = isdigit(remaining_line[i]);
  }

  if (*has_file_id) {
    // If it's old format, split "<file_id> <name>" to {"<field_id>", "<name>"}.
    if (!Tokenize(remaining_line, kWhitespace, 2, &tokens)) {
      return false;
    }
    *file_id = strtol(tokens[0], &after_number, 10);
    // If the file id is -1, it might be an artificial function that doesn't
    // have file id. So, we consider -1 as a valid special case.
    if (!IsValidAfterNumber(after_number) || *file_id < -1 ||
        *file_id == std::numeric_limits<long>::max()) {
      return false;
    }
  }

  *name = tokens[1];
  if (!*name) {
    return false;
  }

  return true;
}

// static
bool SymbolParseHelper::ParseInline(
    char* inline_line,
    bool* has_call_site_file_id,
    long* inline_nest_level,
    long* call_site_line,
    long* call_site_file_id,
    long* origin_id,
    vector<std::pair<MemAddr, MemAddr>>* ranges) {
  // Old INLINE format:
  // INLINE <inline_nest_level> <call_site_line> <origin_id> [<address> <size>]+
  // New INLINE format:
  // INLINE <inline_nest_level> <call_site_line> <call_site_file_id> <origin_id>
  // [<address> <size>]+
  assert(strncmp(inline_line, "INLINE ", 7) == 0);
  inline_line += 7; // skip prefix

  vector<char*> tokens;
  // Increase max_tokens if necessary.
  Tokenize(inline_line, kWhitespace, 512, &tokens);

  // Determine the version of INLINE record by parity of the vector length.
  *has_call_site_file_id = tokens.size() % 2 == 0;

  // The length of the vector should be at least 5.
  if (tokens.size() < 5) {
    return false;
  }

  char* after_number;
  size_t next_idx = 0;

  *inline_nest_level = strtol(tokens[next_idx++], &after_number, 10);
  if (!IsValidAfterNumber(after_number) || *inline_nest_level < 0 ||
      *inline_nest_level == std::numeric_limits<long>::max()) {
    return false;
  }

  *call_site_line = strtol(tokens[next_idx++], &after_number, 10);
  if (!IsValidAfterNumber(after_number) || *call_site_line < 0 ||
      *call_site_line == std::numeric_limits<long>::max()) {
    return false;
  }

  if (*has_call_site_file_id) {
    *call_site_file_id = strtol(tokens[next_idx++], &after_number, 10);
    // If the file id is -1, it might be an artificial function that doesn't
    // have file id. So, we consider -1 as a valid special case.
    if (!IsValidAfterNumber(after_number) || *call_site_file_id < -1 ||
        *call_site_file_id == std::numeric_limits<long>::max()) {
      return false;
    }
  }

  *origin_id = strtol(tokens[next_idx++], &after_number, 10);
  if (!IsValidAfterNumber(after_number) || *origin_id < 0 ||
      *origin_id == std::numeric_limits<long>::max()) {
    return false;
  }

  while (next_idx < tokens.size()) {
    MemAddr address = strtoull(tokens[next_idx++], &after_number, 16);
    if (!IsValidAfterNumber(after_number) ||
        address == std::numeric_limits<unsigned long long>::max()) {
      return false;
    }
    MemAddr size = strtoull(tokens[next_idx++], &after_number, 16);
    if (!IsValidAfterNumber(after_number) ||
        size == std::numeric_limits<unsigned long long>::max()) {
      return false;
    }
    ranges->push_back({address, size});
  }

  return true;
}

// static
bool SymbolParseHelper::ParseFunction(char* function_line, bool* is_multiple,
                                      uint64_t* address, uint64_t* size,
                                      long* stack_param_size, char** name) {
  // FUNC [<multiple>] <address> <size> <stack_param_size> <name>
  assert(strncmp(function_line, "FUNC ", 5) == 0);
  function_line += 5;  // skip prefix

  vector<char*> tokens;
  if (!TokenizeWithOptionalField(function_line, "m", kWhitespace, 5, &tokens)) {
    return false;
  }

  *is_multiple = strcmp(tokens[0], "m") == 0;
  int next_token = *is_multiple ? 1 : 0;

  char* after_number;
  *address = strtoull(tokens[next_token++], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *address == std::numeric_limits<unsigned long long>::max()) {
    return false;
  }
  *size = strtoull(tokens[next_token++], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *size == std::numeric_limits<unsigned long long>::max()) {
    return false;
  }
  *stack_param_size = strtol(tokens[next_token++], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *stack_param_size == std::numeric_limits<long>::max() ||
      *stack_param_size < 0) {
    return false;
  }
  *name = tokens[next_token++];

  return true;
}

// static
bool SymbolParseHelper::ParseLine(char* line_line, uint64_t* address,
                                  uint64_t* size, long* line_number,
                                  long* source_file) {
  // <address> <size> <line number> <source file id>
  vector<char*> tokens;
  if (!Tokenize(line_line, kWhitespace, 4, &tokens)) {
    return false;
  }

  char* after_number;
  *address  = strtoull(tokens[0], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *address == std::numeric_limits<unsigned long long>::max()) {
    return false;
  }
  *size = strtoull(tokens[1], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *size == std::numeric_limits<unsigned long long>::max()) {
    return false;
  }
  *line_number = strtol(tokens[2], &after_number, 10);
  if (!IsValidAfterNumber(after_number) ||
      *line_number == std::numeric_limits<long>::max()) {
    return false;
  }
  *source_file = strtol(tokens[3], &after_number, 10);
  if (!IsValidAfterNumber(after_number) || *source_file < 0 ||
      *source_file == std::numeric_limits<long>::max()) {
    return false;
  }

  // Valid line numbers normally start from 1, however there are functions that
  // are associated with a source file but not associated with any line number
  // (block helper function) and for such functions the symbol file contains 0
  // for the line numbers.  Hence, 0 should be treated as a valid line number.
  // For more information on block helper functions, please, take a look at:
  // http://clang.llvm.org/docs/Block-ABI-Apple.html
  if (*line_number < 0) {
    return false;
  }

  return true;
}

// static
bool SymbolParseHelper::ParsePublicSymbol(char* public_line, bool* is_multiple,
                                          uint64_t* address,
                                          long* stack_param_size,
                                          char** name) {
  // PUBLIC [<multiple>] <address> <stack_param_size> <name>
  assert(strncmp(public_line, "PUBLIC ", 7) == 0);
  public_line += 7;  // skip prefix

  vector<char*> tokens;
  if (!TokenizeWithOptionalField(public_line, "m", kWhitespace, 4, &tokens)) {
    return false;
  }

  *is_multiple = strcmp(tokens[0], "m") == 0;
  int next_token = *is_multiple ? 1 : 0;

  char* after_number;
  *address = strtoull(tokens[next_token++], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *address == std::numeric_limits<unsigned long long>::max()) {
    return false;
  }
  *stack_param_size = strtol(tokens[next_token++], &after_number, 16);
  if (!IsValidAfterNumber(after_number) ||
      *stack_param_size == std::numeric_limits<long>::max() ||
      *stack_param_size < 0) {
    return false;
  }
  *name = tokens[next_token++];

  return true;
}

// static
bool SymbolParseHelper::IsValidAfterNumber(char* after_number) {
  if (after_number != NULL && strchr(kWhitespace, *after_number) != NULL) {
    return true;
  }
  return false;
}

}  // namespace google_breakpad
