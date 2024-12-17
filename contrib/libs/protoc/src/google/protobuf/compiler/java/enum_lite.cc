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

#include "google/protobuf/compiler/java/enum_lite.h"

#include <string>

#include "y_absl/container/flat_hash_map.h"
#include "y_absl/strings/str_cat.h"
#include "google/protobuf/compiler/java/context.h"
#include "google/protobuf/compiler/java/doc_comment.h"
#include "google/protobuf/compiler/java/helpers.h"
#include "google/protobuf/compiler/java/name_resolver.h"
#include "google/protobuf/descriptor.pb.h"
#include "google/protobuf/io/printer.h"

namespace google {
namespace protobuf {
namespace compiler {
namespace java {

EnumLiteGenerator::EnumLiteGenerator(const EnumDescriptor* descriptor,
                                     bool immutable_api, Context* context)
    : descriptor_(descriptor),
      immutable_api_(immutable_api),
      context_(context),
      name_resolver_(context->GetNameResolver()) {
  for (int i = 0; i < descriptor_->value_count(); i++) {
    const EnumValueDescriptor* value = descriptor_->value(i);
    const EnumValueDescriptor* canonical_value =
        descriptor_->FindValueByNumber(value->number());

    if (value == canonical_value) {
      canonical_values_.push_back(value);
    } else {
      Alias alias;
      alias.value = value;
      alias.canonical_value = canonical_value;
      aliases_.push_back(alias);
    }
  }
}

EnumLiteGenerator::~EnumLiteGenerator() {}

void EnumLiteGenerator::Generate(io::Printer* printer) {
  WriteEnumDocComment(printer, descriptor_);
  MaybePrintGeneratedAnnotation(context_, printer, descriptor_, immutable_api_);
  printer->Print(
      "$deprecation$public enum $classname$\n"
      "    implements com.google.protobuf.Internal.EnumLite {\n",
      "classname", descriptor_->name(), "deprecation",
      descriptor_->options().deprecated() ? "@java.lang.Deprecated " : "");
  printer->Annotate("classname", descriptor_);
  printer->Indent();

  for (int i = 0; i < canonical_values_.size(); i++) {
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType> vars;
    vars["name"] = canonical_values_[i]->name();
    vars["number"] = y_absl::StrCat(canonical_values_[i]->number());
    WriteEnumValueDocComment(printer, canonical_values_[i]);
    if (canonical_values_[i]->options().deprecated()) {
      printer->Print("@java.lang.Deprecated\n");
    }
    printer->Print(vars, "$name$($number$),\n");
    printer->Annotate("name", canonical_values_[i]);
  }

  if (SupportUnknownEnumValue(descriptor_->file())) {
    printer->Print("${$UNRECOGNIZED$}$(-1),\n", "{", "", "}", "");
    printer->Annotate("{", "}", descriptor_);
  }

  printer->Print(
      ";\n"
      "\n");

  // -----------------------------------------------------------------

  for (int i = 0; i < aliases_.size(); i++) {
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType> vars;
    vars["classname"] = descriptor_->name();
    vars["name"] = aliases_[i].value->name();
    vars["canonical_name"] = aliases_[i].canonical_value->name();
    WriteEnumValueDocComment(printer, aliases_[i].value);
    printer->Print(
        vars, "public static final $classname$ $name$ = $canonical_name$;\n");
    printer->Annotate("name", aliases_[i].value);
  }

  for (int i = 0; i < descriptor_->value_count(); i++) {
    y_absl::flat_hash_map<y_absl::string_view, TProtoStringType> vars;
    vars["name"] = descriptor_->value(i)->name();
    vars["number"] = y_absl::StrCat(descriptor_->value(i)->number());
    vars["{"] = "";
    vars["}"] = "";
    vars["deprecation"] = descriptor_->value(i)->options().deprecated()
                              ? "@java.lang.Deprecated "
                              : "";
    WriteEnumValueDocComment(printer, descriptor_->value(i));
    printer->Print(vars,
                   "$deprecation$public static final int ${$$name$_VALUE$}$ = "
                   "$number$;\n");
    printer->Annotate("{", "}", descriptor_->value(i));
  }
  printer->Print("\n");

  // -----------------------------------------------------------------

  printer->Print(
      "\n"
      "@java.lang.Override\n"
      "public final int getNumber() {\n");
  if (SupportUnknownEnumValue(descriptor_->file())) {
    printer->Print(
        "  if (this == UNRECOGNIZED) {\n"
        "    throw new java.lang.IllegalArgumentException(\n"
        "        \"Can't get the number of an unknown enum value.\");\n"
        "  }\n");
  }
  printer->Print(
      "  return value;\n"
      "}\n"
      "\n");
  if (context_->options().opensource_runtime) {
    printer->Print(
        "/**\n"
        " * @param value The number of the enum to look for.\n"
        " * @return The enum associated with the given number.\n"
        " * @deprecated Use {@link #forNumber(int)} instead.\n"
        " */\n"
        "@java.lang.Deprecated\n"
        "public static $classname$ valueOf(int value) {\n"
        "  return forNumber(value);\n"
        "}\n"
        "\n",
        "classname", descriptor_->name());
  }

  if (!context_->options().opensource_runtime) {
    printer->Print("@com.google.protobuf.Internal.ProtoMethodMayReturnNull\n");
  }
  printer->Print(
      "public static $classname$ forNumber(int value) {\n"
      "  switch (value) {\n",
      "classname", descriptor_->name());
  printer->Indent();
  printer->Indent();

  for (int i = 0; i < canonical_values_.size(); i++) {
    printer->Print("case $number$: return $name$;\n", "name",
                   canonical_values_[i]->name(), "number",
                   y_absl::StrCat(canonical_values_[i]->number()));
  }

  printer->Outdent();
  printer->Outdent();
  printer->Print(
      "    default: return null;\n"
      "  }\n"
      "}\n"
      "\n"
      "public static com.google.protobuf.Internal.EnumLiteMap<$classname$>\n"
      "    internalGetValueMap() {\n"
      "  return internalValueMap;\n"
      "}\n"
      "private static final com.google.protobuf.Internal.EnumLiteMap<\n"
      "    $classname$> internalValueMap =\n"
      "      new com.google.protobuf.Internal.EnumLiteMap<$classname$>() {\n"
      "        @java.lang.Override\n"
      "        public $classname$ findValueByNumber(int number) {\n"
      "          return $classname$.forNumber(number);\n"
      "        }\n"
      "      };\n"
      "\n"
      "public static com.google.protobuf.Internal.EnumVerifier \n"
      "    internalGetVerifier() {\n"
      "  return $classname$Verifier.INSTANCE;\n"
      "}\n"
      "\n"
      "private static final class $classname$Verifier implements \n"
      "     com.google.protobuf.Internal.EnumVerifier { \n"
      "        static final com.google.protobuf.Internal.EnumVerifier "
      "          INSTANCE = new $classname$Verifier();\n"
      "        @java.lang.Override\n"
      "        public boolean isInRange(int number) {\n"
      "          return $classname$.forNumber(number) != null;\n"
      "        }\n"
      "      };\n"
      "\n",
      "classname", descriptor_->name());
  if (!context_->options().opensource_runtime) {
    printer->Print(
        "/**\n"
        " * Override of toString that prints the number and name.\n"
        " * This is primarily intended as a developer aid.\n"
        " *\n"
        " * <p>NOTE: This implementation is liable to change in the future,\n"
        " * and should not be relied on in code.\n"
        " */\n"
        "@java.lang.Override\n"
        "public java.lang.String toString() {\n"
        "  StringBuilder result = new StringBuilder(\"<\");\n"
        "  result.append(getClass().getName()).append('@')\n"
        "      .append(java.lang.Integer.toHexString(\n"
        "        java.lang.System.identityHashCode(this)));\n");
    if (SupportUnknownEnumValue(descriptor_->file())) {
      printer->Print(
          "  if (this != UNRECOGNIZED) {\n"
          "    result.append(\" number=\").append(getNumber());\n"
          "  }\n");
    } else {
      printer->Print("  result.append(\" number=\").append(getNumber());\n");
    }
    printer->Print(
        "  return result.append(\" name=\")\n"
        "      .append(name()).append('>').toString();\n"
        "}\n"
        "\n");
  }

  printer->Print(
      "private final int value;\n\n"
      "private $classname$(int value) {\n",
      "classname", descriptor_->name());
  printer->Print(
      "  this.value = value;\n"
      "}\n");

  printer->Print(
      "\n"
      "// @@protoc_insertion_point(enum_scope:$full_name$)\n",
      "full_name", descriptor_->full_name());

  printer->Outdent();
  printer->Print("}\n\n");
}

}  // namespace java
}  // namespace compiler
}  // namespace protobuf
}  // namespace google
