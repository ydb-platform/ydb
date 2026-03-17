// Copyright 2005-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the 'License');
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an 'AS IS' BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// See www.openfst.org for extensive documentation on this weighted
// finite-state transducer library.

#include <fst/extensions/far/far-class.h>

#include <string>
#include <utility>

#include <fst/extensions/far/script-impl.h>
#include <fst/arc.h>
#include <fst/script/script-impl.h>

namespace fst {
namespace script {

// FarReaderClass.

std::unique_ptr<FarReaderClass> FarReaderClass::Open(
    const std::string &source) {
  const std::vector<std::string> sources{source};
  return FarReaderClass::Open(sources);
}

std::unique_ptr<FarReaderClass> FarReaderClass::Open(
    const std::vector<std::string> &sources) {
  if (sources.empty()) {
    LOG(ERROR) << "FarReaderClass::Open: No files specified";
    return nullptr;
  }
  auto arc_type = LoadArcTypeFromFar(sources.front());
  if (arc_type.empty()) {
    LOG(ERROR) << "FarReaderClass::Open: File could not be opened: "
               << sources.front();
    return nullptr;
  }
  // TODO(jrosenstock): What if we have an empty FAR for the first one,
  // then non-empty?  We need to check all input FARs.
  OpenFarReaderClassArgs args(sources);
  args.retval = nullptr;
  Apply<Operation<OpenFarReaderClassArgs>>("OpenFarReaderClass", arc_type,
                                           &args);
  return std::move(args.retval);
}

REGISTER_FST_OPERATION(OpenFarReaderClass, StdArc, OpenFarReaderClassArgs);
REGISTER_FST_OPERATION(OpenFarReaderClass, LogArc, OpenFarReaderClassArgs);
REGISTER_FST_OPERATION(OpenFarReaderClass, Log64Arc, OpenFarReaderClassArgs);
REGISTER_FST_OPERATION(OpenFarReaderClass, ErrorArc, OpenFarReaderClassArgs);

// FarWriterClass.

std::unique_ptr<FarWriterClass> FarWriterClass::Create(
    const std::string &source, const std::string &arc_type, FarType type) {
  CreateFarWriterClassInnerArgs iargs(source, type);
  CreateFarWriterClassArgs args(iargs);
  args.retval = nullptr;
  Apply<Operation<CreateFarWriterClassArgs>>("CreateFarWriterClass", arc_type,
                                             &args);
  return std::move(args.retval);
}

REGISTER_FST_OPERATION(CreateFarWriterClass, StdArc, CreateFarWriterClassArgs);
REGISTER_FST_OPERATION(CreateFarWriterClass, LogArc, CreateFarWriterClassArgs);
REGISTER_FST_OPERATION(CreateFarWriterClass, Log64Arc,
                       CreateFarWriterClassArgs);
REGISTER_FST_OPERATION(CreateFarWriterClass, ErrorArc,
                       CreateFarWriterClassArgs);

}  // namespace script
}  // namespace fst
