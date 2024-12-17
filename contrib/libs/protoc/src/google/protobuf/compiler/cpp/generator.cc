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

#include "google/protobuf/compiler/cpp/generator.h"

#include <cstdlib>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "y_absl/strings/match.h"
#include "y_absl/strings/str_cat.h"
#include "y_absl/strings/string_view.h"
#include "google/protobuf/compiler/cpp/file.h"
#include "google/protobuf/compiler/cpp/helpers.h"
#include "google/protobuf/descriptor.pb.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace cpp {
namespace {
TProtoStringType NumberedCcFileName(y_absl::string_view basename, int number) {
  return y_absl::StrCat(basename, ".out/", number, ".cc");
}

y_absl::flat_hash_map<y_absl::string_view, TProtoStringType> CommonVars(
    const Options& options) {
  bool is_oss = options.opensource_runtime;
  return {
      {"proto_ns", ProtobufNamespace(options)},
      {"pb", y_absl::StrCat("::", ProtobufNamespace(options))},
      {"pbi", y_absl::StrCat("::", ProtobufNamespace(options), "::internal")},

      {"string", "TProtoStringType"},
      {"int8", "::int8_t"},
      {"int32", "::arc_i32"},
      {"int64", "::arc_i64"},
      {"uint8", "::uint8_t"},
      {"uint32", "::arc_ui32"},
      {"uint64", "::arc_ui64"},

      {"hrule_thick", kThickSeparator},
      {"hrule_thin", kThinSeparator},

      // Warning: there is some clever naming/splitting here to avoid extract
      // script rewrites.  The names of these variables must not be things that
      // the extract script will rewrite.  That's why we use "CHK" (for example)
      // instead of "Y_ABSL_CHECK".
      //
      // These values are things the extract script would rewrite if we did not
      // split them.  It might not strictly matter since we don't generate
      // google3 code in open-source.  But it's good to prevent surprising
      // things from happening.
      {"GOOGLE_PROTOBUF", is_oss ? "GOOGLE_PROTOBUF"
                                 : "GOOGLE3_PROTOBU"
                                   "F"},
      {"CHK",
       "Y_ABSL_CHEC"
       "K"},
      {"DCHK",
       "Y_ABSL_DCHEC"
       "K"},
  };
}
}  // namespace

bool CppGenerator::Generate(const FileDescriptor* file,
                            const TProtoStringType& parameter,
                            GeneratorContext* generator_context,
                            TProtoStringType* error) const {
  std::vector<std::pair<TProtoStringType, TProtoStringType>> options;
  ParseGeneratorParameter(parameter, &options);

  // -----------------------------------------------------------------
  // parse generator options

  // If the dllexport_decl option is passed to the compiler, we need to write
  // it in front of every symbol that should be exported if this .proto is
  // compiled into a Windows DLL.  E.g., if the user invokes the protocol
  // compiler as:
  //   protoc --cpp_out=dllexport_decl=FOO_EXPORT:outdir foo.proto
  // then we'll define classes like this:
  //   class FOO_EXPORT Foo {
  //     ...
  //   }
  // FOO_EXPORT is a macro which should expand to __declspec(dllexport) or
  // __declspec(dllimport) depending on what is being compiled.
  //
  // If the proto_h option is passed to the compiler, we will generate all
  // classes and enums so that they can be forward-declared from files that
  // need them from imports.
  //
  // If the lite option is passed to the compiler, we will generate the
  // current files and all transitive dependencies using the LITE runtime.
  Options file_options;

  file_options.opensource_runtime = opensource_runtime_;
  file_options.runtime_include_base = runtime_include_base_;

  for (const auto& option : options) {
    const auto& key = option.first;
    const auto& value = option.second;

    if (key == "dllexport_decl") {
      file_options.dllexport_decl = value;
    } else if (key == "safe_boundary_check") {
      file_options.safe_boundary_check = true;
    } else if (key == "annotate_headers") {
      file_options.annotate_headers = true;
    } else if (key == "annotation_pragma_name") {
      file_options.annotation_pragma_name = value;
    } else if (key == "annotation_guard_name") {
      file_options.annotation_guard_name = value;
    } else if (key == "speed") {
      file_options.enforce_mode = EnforceOptimizeMode::kSpeed;
    } else if (key == "code_size") {
      file_options.enforce_mode = EnforceOptimizeMode::kCodeSize;
    } else if (key == "lite") {
      file_options.enforce_mode = EnforceOptimizeMode::kLiteRuntime;
    } else if (key == "lite_implicit_weak_fields") {
      file_options.enforce_mode = EnforceOptimizeMode::kLiteRuntime;
      file_options.lite_implicit_weak_fields = true;
      if (!value.empty()) {
        file_options.num_cc_files = std::strtol(value.c_str(), nullptr, 10);
      }
    } else if (key == "proto_h") {
      file_options.proto_h = true;
    } else if (key == "proto_static_reflection_h") {
    } else if (key == "annotate_accessor") {
      file_options.annotate_accessor = true;
    } else if (key == "inject_field_listener_events") {
      file_options.field_listener_options.inject_field_listener_events = true;
    } else if (key == "forbidden_field_listener_events") {
      std::size_t pos = 0;
      do {
        std::size_t next_pos = value.find_first_of("+", pos);
        if (next_pos == TProtoStringType::npos) {
          next_pos = value.size();
        }
        if (next_pos > pos)
          file_options.field_listener_options.forbidden_field_listener_events
              .emplace(value.substr(pos, next_pos - pos));
        pos = next_pos + 1;
      } while (pos < value.size());
    } else if (key == "unverified_lazy_message_sets") {
      file_options.unverified_lazy_message_sets = true;
    } else if (key == "force_eagerly_verified_lazy") {
      file_options.force_eagerly_verified_lazy = true;
    } else if (key == "experimental_tail_call_table_mode") {
      if (value == "never") {
        file_options.tctable_mode = Options::kTCTableNever;
      } else if (value == "guarded") {
        file_options.tctable_mode = Options::kTCTableGuarded;
      } else if (value == "always") {
        file_options.tctable_mode = Options::kTCTableAlways;
      } else {
        *error = y_absl::StrCat(
            "Unknown value for experimental_tail_call_table_mode: ", value);
        return false;
      }
    } else {
      *error = y_absl::StrCat("Unknown generator option: ", key);
      return false;
    }
  }

  // The safe_boundary_check option controls behavior for Google-internal
  // protobuf APIs.
  if (file_options.safe_boundary_check && file_options.opensource_runtime) {
    *error =
        "The safe_boundary_check option is not supported outside of Google.";
    return false;
  }

  // -----------------------------------------------------------------


  TProtoStringType basename = StripProto(file->name());

  auto generate_reserved_static_reflection_header = [&basename,
                                                     &generator_context]() {
    auto output = y_absl::WrapUnique(generator_context->Open(
        y_absl::StrCat(basename, ".proto.static_reflection.h")));
    io::Printer(output.get()).Emit(R"cc(
      // Reserved for future use.
    )cc");
  };
  // Suppress maybe unused warning.
  (void)generate_reserved_static_reflection_header;

  if (MaybeBootstrap(file_options, generator_context, file_options.bootstrap,
                     &basename)) {
    return true;
  }

  FileGenerator file_generator(file, file_options);

  // Generate header(s).
  if (file_options.proto_h) {
    auto output = y_absl::WrapUnique(
        generator_context->Open(y_absl::StrCat(basename, ".proto.h")));

    GeneratedCodeInfo annotations;
    io::AnnotationProtoCollector<GeneratedCodeInfo> annotation_collector(
        &annotations);
    io::Printer::Options options;
    if (file_options.annotate_headers) {
      options.annotation_collector = &annotation_collector;
    }

    io::Printer p(output.get(), options);
    auto v = p.WithVars(CommonVars(file_options));

    TProtoStringType info_path = y_absl::StrCat(basename, ".proto.h.meta");
    file_generator.GenerateProtoHeader(
        &p, file_options.annotate_headers ? info_path : "");

    if (file_options.annotate_headers) {
      auto info_output = y_absl::WrapUnique(generator_context->Open(info_path));
      annotations.SerializeToZeroCopyStream(info_output.get());
    }
  }

  {
    auto output = y_absl::WrapUnique(
        generator_context->Open(y_absl::StrCat(basename, ".pb.h")));

    GeneratedCodeInfo annotations;
    io::AnnotationProtoCollector<GeneratedCodeInfo> annotation_collector(
        &annotations);
    io::Printer::Options options;
    if (file_options.annotate_headers) {
      options.annotation_collector = &annotation_collector;
    }

    io::Printer p(output.get(), options);
    auto v = p.WithVars(CommonVars(file_options));

    TProtoStringType info_path = y_absl::StrCat(basename, ".pb.h.meta");
    file_generator.GeneratePBHeader(
        &p, file_options.annotate_headers ? info_path : "");

    if (file_options.annotate_headers) {
      auto info_output = y_absl::WrapUnique(generator_context->Open(info_path));
      annotations.SerializeToZeroCopyStream(info_output.get());
    }
  }

  // Generate cc file(s).
  if (UsingImplicitWeakFields(file, file_options)) {
    {
      // This is the global .cc file, containing
      // enum/services/tables/reflection
      auto output = y_absl::WrapUnique(
          generator_context->Open(y_absl::StrCat(basename, ".pb.cc")));
      io::Printer p(output.get());
      auto v = p.WithVars(CommonVars(file_options));

      file_generator.GenerateGlobalSource(&p);
    }

    int num_cc_files =
        file_generator.NumMessages() + file_generator.NumExtensions();

    // If we're using implicit weak fields then we allow the user to
    // optionally specify how many files to generate, not counting the global
    // pb.cc file. If we have more files than messages, then some files will
    // be generated as empty placeholders.
    if (file_options.num_cc_files > 0) {
      Y_ABSL_CHECK_LE(num_cc_files, file_options.num_cc_files)
          << "There must be at least as many numbered .cc files as messages "
             "and extensions.";
      num_cc_files = file_options.num_cc_files;
    }

    int cc_file_number = 0;
    for (int i = 0; i < file_generator.NumMessages(); ++i) {
      auto output = y_absl::WrapUnique(generator_context->Open(
          NumberedCcFileName(basename, cc_file_number++)));
      io::Printer p(output.get());
      auto v = p.WithVars(CommonVars(file_options));

      file_generator.GenerateSourceForMessage(i, &p);
    }

    for (int i = 0; i < file_generator.NumExtensions(); ++i) {
      auto output = y_absl::WrapUnique(generator_context->Open(
          NumberedCcFileName(basename, cc_file_number++)));
      io::Printer p(output.get());
      auto v = p.WithVars(CommonVars(file_options));

      file_generator.GenerateSourceForExtension(i, &p);
    }

    // Create empty placeholder files if necessary to match the expected number
    // of files.
    while (cc_file_number < num_cc_files) {
      (void)y_absl::WrapUnique(generator_context->Open(
          NumberedCcFileName(basename, cc_file_number++)));
    }
  } else {
    auto output = y_absl::WrapUnique(
        generator_context->Open(y_absl::StrCat(basename, ".pb.cc")));
    io::Printer p(output.get());
    auto v = p.WithVars(CommonVars(file_options));

    file_generator.GenerateSource(&p);
  }

  return true;
}
}  // namespace cpp
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
