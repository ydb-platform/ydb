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

#include <fst/script/info-impl.h>

#include <cstdint>
#include <string>

namespace fst {

// Column width for property names.
constexpr int kWidth = 50;

void FstInfo::Info() const {
  std::ostream &ostrm = std::cout;
  const auto old = ostrm.setf(std::ios::left);
  ostrm.width(kWidth);
  ostrm << "fst type" << FstType() << std::endl;
  ostrm.width(kWidth);
  ostrm << "arc type" << ArcType() << std::endl;
  ostrm.width(kWidth);
  ostrm << "input symbol table" << InputSymbols() << std::endl;
  ostrm.width(kWidth);
  ostrm << "output symbol table" << OutputSymbols() << std::endl;
  if (!LongInfo()) {
    ostrm.setf(old);
    return;
  }
  ostrm.width(kWidth);
  ostrm << "# of states" << NumStates() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of arcs" << NumArcs() << std::endl;
  ostrm.width(kWidth);
  ostrm << "initial state" << Start() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of final states" << NumFinal() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of input/output epsilons" << NumEpsilons() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of input epsilons" << NumInputEpsilons() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of output epsilons" << NumOutputEpsilons() << std::endl;
  ostrm.width(kWidth);
  ostrm << "input label multiplicity" << InputLabelMultiplicity() << std::endl;
  ostrm.width(kWidth);
  ostrm << "output label multiplicity" << OutputLabelMultiplicity()
        << std::endl;
  ostrm.width(kWidth);
  std::string arc_type = "";
  switch (ArcFilterType()) {
    case script::ArcFilterType::ANY:
      break;
    case script::ArcFilterType::EPSILON: {
      arc_type = "epsilon ";
      break;
    }
    case script::ArcFilterType::INPUT_EPSILON: {
      arc_type = "input-epsilon ";
      break;
    }
    case script::ArcFilterType::OUTPUT_EPSILON: {
      arc_type = "output-epsilon ";
      break;
    }
  }
  const auto accessible_label = "# of " + arc_type + "accessible states";
  ostrm.width(kWidth);
  ostrm << accessible_label << NumAccessible() << std::endl;
  const auto coaccessible_label = "# of " + arc_type + "coaccessible states";
  ostrm.width(kWidth);
  ostrm << coaccessible_label << NumCoAccessible() << std::endl;
  const auto connected_label = "# of " + arc_type + "connected states";
  ostrm.width(kWidth);
  ostrm << connected_label << NumConnected() << std::endl;
  const auto numcc_label = "# of " + arc_type + "connected components";
  ostrm.width(kWidth);
  ostrm << numcc_label << NumCc() << std::endl;
  const auto numscc_label = "# of " + arc_type + "strongly conn components";
  ostrm.width(kWidth);
  ostrm << numscc_label << NumScc() << std::endl;
  ostrm.width(kWidth);
  ostrm << "input matcher"
        << (InputMatchType() == MATCH_INPUT
                ? 'y'
                : InputMatchType() == MATCH_NONE ? 'n' : '?')
        << std::endl;
  ostrm.width(kWidth);
  ostrm << "output matcher"
        << (OutputMatchType() == MATCH_OUTPUT
                ? 'y'
                : OutputMatchType() == MATCH_NONE ? 'n' : '?')
        << std::endl;
  ostrm.width(kWidth);
  ostrm << "input lookahead" << (InputLookAhead() ? 'y' : 'n') << std::endl;
  ostrm.width(kWidth);
  ostrm << "output lookahead" << (OutputLookAhead() ? 'y' : 'n') << std::endl;
  PrintProperties(ostrm, Properties());
  ostrm.setf(old);
}

void PrintProperties(std::ostream &ostrm, const uint64_t properties) {
  uint64_t prop = 1;
  for (auto i = 0; i < 64; ++i, prop <<= 1) {
    if (prop & kBinaryProperties) {
      const char value = properties & prop ? 'y' : 'n';
      ostrm.width(kWidth);
      ostrm << internal::PropertyNames[i] << value << std::endl;
    } else if (prop & kPosTrinaryProperties) {
      char value = '?';
      if (properties & prop) {
        value = 'y';
      } else if (properties & prop << 1) {
        value = 'n';
      }
      ostrm.width(kWidth);
      ostrm << internal::PropertyNames[i] << value << std::endl;
    }
  }
}

void PrintHeader(std::ostream &ostrm, const FstHeader &header) {
  const auto old = ostrm.setf(std::ios::left);
  ostrm.width(kWidth);
  ostrm << "fst type" << header.FstType() << std::endl;
  ostrm.width(kWidth);
  ostrm << "arc type" << header.ArcType() << std::endl;
  ostrm.width(kWidth);
  ostrm << "version" << header.Version() << std::endl;

  // Flags
  const auto flags = header.GetFlags();
  ostrm.width(kWidth);
  ostrm << "input symbol table" << (flags & FstHeader::HAS_ISYMBOLS ? 'y' : 'n')
        << std::endl;
  ostrm.width(kWidth);
  ostrm << "output symbol table"
        << (flags & FstHeader::HAS_OSYMBOLS ? 'y' : 'n') << std::endl;
  ostrm.width(kWidth);
  ostrm << "aligned" << (flags & FstHeader::IS_ALIGNED ? 'y' : 'n')
        << std::endl;

  ostrm.width(kWidth);
  ostrm << "initial state" << header.Start() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of states" << header.NumStates() << std::endl;
  ostrm.width(kWidth);
  ostrm << "# of arcs" << header.NumArcs() << std::endl;

  PrintProperties(ostrm, header.Properties());
  ostrm.setf(old);
}

}  // namespace fst
