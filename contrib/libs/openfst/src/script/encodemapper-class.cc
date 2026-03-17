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

#include <fst/script/encodemapper-class.h>

#include <cstdint>
#include <memory>
#include <string>

#include <fst/script/script-impl.h>
#include <string_view>

namespace fst {
namespace script {
namespace {

// Helper methods.

std::unique_ptr<EncodeMapperClass> ReadEncodeMapper(std::istream &istrm,
                                                    const std::string &source) {
  if (!istrm) {
    LOG(ERROR) << "ReadEncodeMapperClass: Can't open file: " << source;
    return nullptr;
  }
  EncodeTableHeader hdr;
  if (!hdr.Read(istrm, source)) return nullptr;
  const auto &arc_type = hdr.ArcType();
  // TODO(b/141172858): deprecated, remove by 2020-01-01.
  if (arc_type.empty()) {
    LOG(ERROR) << "Old-style EncodeMapper cannot be used with script interface";
    return nullptr;
  }
  // The actual reader also consumes the header, so to be kind we rewind.
  istrm.seekg(0, istrm.beg);
  static const auto *reg =
      EncodeMapperClassIORegistration::Register::GetRegister();
  const auto reader = reg->GetReader(arc_type);
  if (!reader) {
    LOG(ERROR) << "EncodeMapperClass::Read: Unknown arc type: " << arc_type;
    return nullptr;
  }
  return reader(istrm, source);
}

std::unique_ptr<EncodeMapperImplBase> CreateEncodeMapper(
    std::string_view arc_type, uint8_t flags, EncodeType type) {
  static const auto *reg =
      EncodeMapperClassIORegistration::Register::GetRegister();
  auto creator = reg->GetCreator(arc_type);
  if (!creator) {
    FSTERROR() << "EncodeMapperClass: Unknown arc type: " << arc_type;
    return nullptr;
  }
  return creator(flags, type);
}

}  // namespace

EncodeMapperClass::EncodeMapperClass(std::string_view arc_type, uint8_t flags,
                                     EncodeType type)
    : impl_(CreateEncodeMapper(arc_type, flags, type)) {}

std::unique_ptr<EncodeMapperClass> EncodeMapperClass::Read(
    const std::string &source) {
  if (!source.empty()) {
    std::ifstream strm(source, std::ios_base::in | std::ios_base::binary);
    return ReadEncodeMapper(strm, source);
  } else {
    return ReadEncodeMapper(std::cin, "standard input");
  }
}

std::unique_ptr<EncodeMapperClass> EncodeMapperClass::Read(
    std::istream &strm, const std::string &source) {
  return ReadEncodeMapper(strm, source);
}

// Registration.

REGISTER_ENCODEMAPPER_CLASS(StdArc);
REGISTER_ENCODEMAPPER_CLASS(LogArc);
REGISTER_ENCODEMAPPER_CLASS(Log64Arc);

}  // namespace script
}  // namespace fst
