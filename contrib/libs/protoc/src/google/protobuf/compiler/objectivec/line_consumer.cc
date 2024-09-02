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

#ifndef _MSC_VER
#include <unistd.h>
#endif
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>

#include <climits>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "y_absl/strings/ascii.h"
#include "y_absl/strings/str_cat.h"
#include "google/protobuf/compiler/objectivec/line_consumer.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"

#ifdef _WIN32
#include "google/protobuf/io/io_win32.h"
#endif

// NOTE: src/google/protobuf/compiler/plugin.cc makes use of cerr for some
// error cases, so it seems to be ok to use as a back door for errors.

namespace google {
namespace protobuf {
namespace compiler {
namespace objectivec {

// <io.h> is transitively included in this file. Import the functions explicitly
// in this port namespace to avoid ambiguous definition.
namespace posix {
#ifdef _WIN32
using google::protobuf::io::win32::open;
#else   // !_WIN32
using ::open;
#endif  // _WIN32
}  // namespace posix

namespace {

bool ascii_isnewline(char c) { return c == '\n' || c == '\r'; }

bool ReadLine(y_absl::string_view* input, y_absl::string_view* line) {
  for (int len = 0; len < input->size(); ++len) {
    if (ascii_isnewline((*input)[len])) {
      *line = y_absl::string_view(input->data(), len);
      ++len;  // advance over the newline
      *input = y_absl::string_view(input->data() + len, input->size() - len);
      return true;
    }
  }
  return false;  // Ran out of input with no newline.
}

void RemoveComment(y_absl::string_view* input) {
  int offset = input->find('#');
  if (offset != y_absl::string_view::npos) {
    input->remove_suffix(input->length() - offset);
  }
}

class Parser {
 public:
  explicit Parser(LineConsumer* line_consumer)
      : line_consumer_(line_consumer), line_(0) {}

  // Feeds in some input, parse what it can, returning success/failure. Calling
  // again after an error is undefined.
  bool ParseChunk(y_absl::string_view chunk, TProtoStringType* out_error);

  // Should be called to finish parsing (after all input has been provided via
  // successful calls to ParseChunk(), calling after a ParseChunk() failure is
  // undefined). Returns success/failure.
  bool Finish(TProtoStringType* out_error);

  int last_line() const { return line_; }

 private:
  LineConsumer* line_consumer_;
  int line_;
  TProtoStringType leftover_;
};

bool Parser::ParseChunk(y_absl::string_view chunk, TProtoStringType* out_error) {
  y_absl::string_view full_chunk;
  if (!leftover_.empty()) {
    leftover_ += TProtoStringType(chunk);
    full_chunk = y_absl::string_view(leftover_);
  } else {
    full_chunk = chunk;
  }

  y_absl::string_view line;
  while (ReadLine(&full_chunk, &line)) {
    ++line_;
    RemoveComment(&line);
    line = y_absl::StripAsciiWhitespace(line);
    if (!line.empty() && !line_consumer_->ConsumeLine(line, out_error)) {
      if (out_error->empty()) {
        *out_error = "ConsumeLine failed without setting an error.";
      }
      leftover_.clear();
      return false;
    }
  }

  if (full_chunk.empty()) {
    leftover_.clear();
  } else {
    leftover_ = TProtoStringType(full_chunk);
  }
  return true;
}

bool Parser::Finish(TProtoStringType* out_error) {
  // If there is still something to go, flush it with a newline.
  if (!leftover_.empty() && !ParseChunk("\n", out_error)) {
    return false;
  }
  // This really should never fail if ParseChunk succeeded, but check to be
  // sure.
  if (!leftover_.empty()) {
    *out_error = "ParseSimple Internal error: finished with pending data.";
    return false;
  }
  return true;
}

}  // namespace

bool ParseSimpleFile(y_absl::string_view path, LineConsumer* line_consumer,
                     TProtoStringType* out_error) {
  int fd;
  do {
    fd = posix::open(TProtoStringType(path).c_str(), O_RDONLY);
  } while (fd < 0 && errno == EINTR);
  if (fd < 0) {
    *out_error =
        y_absl::StrCat("error: Unable to open \"", path, "\", ", strerror(errno));
    return false;
  }
  io::FileInputStream file_stream(fd);
  file_stream.SetCloseOnDelete(true);

  return ParseSimpleStream(file_stream, path, line_consumer, out_error);
}

bool ParseSimpleStream(io::ZeroCopyInputStream& input_stream,
                       y_absl::string_view stream_name,
                       LineConsumer* line_consumer, TProtoStringType* out_error) {
  TProtoStringType local_error;
  Parser parser(line_consumer);
  const void* buf;
  int buf_len;
  while (input_stream.Next(&buf, &buf_len)) {
    if (buf_len == 0) {
      continue;
    }

    if (!parser.ParseChunk(
            y_absl::string_view(static_cast<const char*>(buf), buf_len),
            &local_error)) {
      *out_error = y_absl::StrCat("error: ", stream_name, " Line ",
                                parser.last_line(), ", ", local_error);
      return false;
    }
  }
  if (!parser.Finish(&local_error)) {
    *out_error = y_absl::StrCat("error: ", stream_name, " Line ",
                              parser.last_line(), ", ", local_error);
    return false;
  }
  return true;
}

}  // namespace objectivec
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
