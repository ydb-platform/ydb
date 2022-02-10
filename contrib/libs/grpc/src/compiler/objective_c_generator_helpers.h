/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#ifndef GRPC_INTERNAL_COMPILER_OBJECTIVE_C_GENERATOR_HELPERS_H
#define GRPC_INTERNAL_COMPILER_OBJECTIVE_C_GENERATOR_HELPERS_H

#include <map>
#include "src/compiler/config.h"
#include "src/compiler/generator_helpers.h"

#include <google/protobuf/compiler/objectivec/objectivec_helpers.h>

namespace grpc_objective_c_generator {

using ::grpc::protobuf::FileDescriptor;
using ::grpc::protobuf::ServiceDescriptor;
using ::TString; 

inline string MessageHeaderName(const FileDescriptor* file) {
  return google::protobuf::compiler::objectivec::FilePath(file) + ".pbobjc.h";
}

inline string ServiceClassName(const ServiceDescriptor* service) {
  const FileDescriptor* file = service->file();
  string prefix = file->options().objc_class_prefix();
  return prefix + service->name();
}

inline ::TString LocalImport(const ::TString& import) { 
  return ::TString("#import \"" + import + "\"\n"); 
}

inline ::TString FrameworkImport(const ::TString& import, 
                                     const ::TString& framework) { 
  // Flattens the directory structure: grab the file name only
  std::size_t pos = import.rfind("/");
  // If pos is npos, pos + 1 is 0, which gives us the entire string,
  // so there's no need to check that
  ::TString filename = import.substr(pos + 1, import.size() - (pos + 1)); 
  return ::TString("#import <" + framework + "/" + filename + ">\n"); 
}

inline ::TString SystemImport(const ::TString& import) { 
  return ::TString("#import <" + import + ">\n"); 
}

inline ::TString PreprocConditional(::TString symbol, bool invert) { 
  return invert ? "!defined(" + symbol + ") || !" + symbol
                : "defined(" + symbol + ") && " + symbol;
}

inline ::TString PreprocIf(const ::TString& symbol, 
                               const ::TString& if_true) { 
  return ::TString("#if " + PreprocConditional(symbol, false) + "\n" + 
                       if_true + "#endif\n"); 
}

inline ::TString PreprocIfNot(const ::TString& symbol, 
                                  const ::TString& if_true) { 
  return ::TString("#if " + PreprocConditional(symbol, true) + "\n" + 
                       if_true + "#endif\n"); 
}

inline ::TString PreprocIfElse(const ::TString& symbol, 
                                   const ::TString& if_true, 
                                   const ::TString& if_false) { 
  return ::TString("#if " + PreprocConditional(symbol, false) + "\n" + 
                       if_true + "#else\n" + if_false + "#endif\n"); 
}

inline ::TString PreprocIfNotElse(const ::TString& symbol, 
                                      const ::TString& if_true, 
                                      const ::TString& if_false) { 
  return ::TString("#if " + PreprocConditional(symbol, true) + "\n" + 
                       if_true + "#else\n" + if_false + "#endif\n"); 
}

}  // namespace grpc_objective_c_generator
#endif  // GRPC_INTERNAL_COMPILER_OBJECTIVE_C_GENERATOR_HELPERS_H
