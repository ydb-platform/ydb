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
//
// These classes are only recommended for use in high-level scripting
// applications. Most users should use the lower-level templated versions
// corresponding to these classes.

#include <fst/script/fst-class.h>

#include <istream>
#include <memory>
#include <string>

#include <fst/log.h>
#include <fst/equal.h>
#include <fst/fst-decl.h>
#include <fst/reverse.h>
#include <fst/union.h>
#include <string_view>

namespace fst {
namespace script {
namespace {

// Helper functions.

template <class F>
std::unique_ptr<F> ReadFstClass(std::istream &istrm,
                                const std::string &source) {
  if (!istrm) {
    LOG(ERROR) << "ReadFstClass: Can't open file: " << source;
    return nullptr;
  }
  FstHeader hdr;
  if (!hdr.Read(istrm, source)) return nullptr;
  const FstReadOptions read_options(source, &hdr);
  const auto &arc_type = hdr.ArcType();
  static const auto *reg = FstClassIORegistration<F>::Register::GetRegister();
  const auto reader = reg->GetReader(arc_type);
  if (!reader) {
    LOG(ERROR) << "ReadFstClass: Unknown arc type: " << arc_type;
    return nullptr;
  }
  return reader(istrm, read_options);
}

template <class F>
std::unique_ptr<FstClassImplBase> CreateFstClass(std::string_view arc_type) {
  static const auto *reg = FstClassIORegistration<F>::Register::GetRegister();
  auto creator = reg->GetCreator(arc_type);
  if (!creator) {
    FSTERROR() << "CreateFstClass: Unknown arc type: " << arc_type;
    return nullptr;
  }
  return creator();
}

template <class F>
std::unique_ptr<FstClassImplBase> ConvertFstClass(const FstClass &other) {
  static const auto *reg = FstClassIORegistration<F>::Register::GetRegister();
  auto converter = reg->GetConverter(other.ArcType());
  if (!converter) {
    FSTERROR() << "ConvertFstClass: Unknown arc type: " << other.ArcType();
    return nullptr;
  }
  return converter(other);
}

}  // namespace

// FstClass methods.

std::unique_ptr<FstClass> FstClass::Read(const std::string &source) {
  if (!source.empty()) {
    std::ifstream istrm(source, std::ios_base::in | std::ios_base::binary);
    return ReadFstClass<FstClass>(istrm, source);
  } else {
    return ReadFstClass<FstClass>(std::cin, "standard input");
  }
}

std::unique_ptr<FstClass> FstClass::Read(std::istream &istrm,
                                         const std::string &source) {
  return ReadFstClass<FstClass>(istrm, source);
}

bool FstClass::WeightTypesMatch(const WeightClass &weight,
                                std::string_view op_name) const {
  if (WeightType() != weight.Type()) {
    FSTERROR() << op_name << ": FST and weight with non-matching weight types: "
               << WeightType() << " and " << weight.Type();
    return false;
  }
  return true;
}

// MutableFstClass methods.

std::unique_ptr<MutableFstClass> MutableFstClass::Read(
    const std::string &source, bool convert) {
  if (convert == false) {
    if (!source.empty()) {
      std::ifstream in(source, std::ios_base::in | std::ios_base::binary);
      return ReadFstClass<MutableFstClass>(in, source);
    } else {
      return ReadFstClass<MutableFstClass>(std::cin, "standard input");
    }
  } else {  // Converts to VectorFstClass if not mutable.
    std::unique_ptr<FstClass> ifst(FstClass::Read(source));
    if (!ifst) return nullptr;
    if (ifst->Properties(kMutable, false) == kMutable) {
      return fst::WrapUnique(down_cast<MutableFstClass *>(ifst.release()));
    } else {
      return std::make_unique<VectorFstClass>(*ifst.release());
    }
  }
}

// VectorFstClass methods.

std::unique_ptr<VectorFstClass> VectorFstClass::Read(
    const std::string &source) {
  if (!source.empty()) {
    std::ifstream in(source, std::ios_base::in | std::ios_base::binary);
    return ReadFstClass<VectorFstClass>(in, source);
  } else {
    return ReadFstClass<VectorFstClass>(std::cin, "standard input");
  }
}

VectorFstClass::VectorFstClass(std::string_view arc_type)
    : MutableFstClass(CreateFstClass<VectorFstClass>(arc_type)) {}

VectorFstClass::VectorFstClass(const FstClass &other)
    : MutableFstClass(ConvertFstClass<VectorFstClass>(other)) {}

// Registration.

REGISTER_FST_CLASSES(StdArc);
REGISTER_FST_CLASSES(LogArc);
REGISTER_FST_CLASSES(Log64Arc);

}  // namespace script
}  // namespace fst
