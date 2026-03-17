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
// Class to compute various information about FSTs, a helper class for
// fstinfo.cc.

#ifndef FST_SCRIPT_INFO_IMPL_H_
#define FST_SCRIPT_INFO_IMPL_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include <fst/connect.h>
#include <fst/dfs-visit.h>
#include <fst/fst.h>
#include <fst/lookahead-matcher.h>
#include <fst/matcher.h>
#include <fst/queue.h>
#include <fst/test-properties.h>
#include <fst/verify.h>
#include <fst/visit.h>
#include <fst/script/arcfilter-impl.h>
#include <string_view>

namespace fst {

// Compute various information about FSTs, helper class for fstinfo.cc.
// WARNING: Stand-alone use of this class is not recommended, most code
// should call directly the relevant library functions: Fst<Arc>::NumStates,
// Fst<Arc>::NumArcs, TestProperties, etc.
class FstInfo {
 public:
  // When info_type is "short" (or "auto" and not an ExpandedFst) then only
  // minimal info is computed and can be requested.
  template <typename Arc>
  FstInfo(const Fst<Arc> &fst, bool test_properties,
          script::ArcFilterType arc_filter_type = script::ArcFilterType::ANY,
          std::string_view info_type = "auto", bool verify = true)
      : fst_type_(fst.Type()),
        input_symbols_(fst.InputSymbols() ? fst.InputSymbols()->Name()
                                          : "none"),
        output_symbols_(fst.OutputSymbols() ? fst.OutputSymbols()->Name()
                                            : "none"),
        nstates_(0),
        narcs_(0),
        start_(kNoStateId),
        nfinal_(0),
        nepsilons_(0),
        niepsilons_(0),
        noepsilons_(0),
        ilabel_mult_(0.0),
        olabel_mult_(0.0),
        naccess_(0),
        ncoaccess_(0),
        nconnect_(0),
        ncc_(0),
        nscc_(0),
        input_match_type_(MATCH_NONE),
        output_match_type_(MATCH_NONE),
        input_lookahead_(false),
        output_lookahead_(false),
        properties_(0),
        arc_filter_type_(arc_filter_type),
        long_info_(true),
        arc_type_(Arc::Type()) {
    using Label = typename Arc::Label;
    using StateId = typename Arc::StateId;
    using Weight = typename Arc::Weight;
    if (info_type == "long") {
      long_info_ = true;
    } else if (info_type == "short") {
      long_info_ = false;
    } else if (info_type == "auto") {
      long_info_ = fst.Properties(kExpanded, false);
    } else {
      FSTERROR() << "Bad info type: " << info_type;
      return;
    }
    if (!long_info_) return;
    // If the FST is not sane, we return.
    if (verify && !Verify(fst)) {
      FSTERROR() << "FstInfo: Verify: FST not well-formed";
      return;
    }
    start_ = fst.Start();
    properties_ = fst.Properties(kFstProperties, test_properties);
    for (StateIterator<Fst<Arc>> siter(fst); !siter.Done(); siter.Next()) {
      ++nstates_;
      const auto s = siter.Value();
      if (fst.Final(s) != Weight::Zero()) ++nfinal_;
      std::map<Label, size_t> ilabel_count;
      std::map<Label, size_t> olabel_count;
      for (ArcIterator<Fst<Arc>> aiter(fst, s); !aiter.Done(); aiter.Next()) {
        const auto &arc = aiter.Value();
        ++narcs_;
        if (arc.ilabel == 0 && arc.olabel == 0) ++nepsilons_;
        if (arc.ilabel == 0) ++niepsilons_;
        if (arc.olabel == 0) ++noepsilons_;
        ++ilabel_count[arc.ilabel];
        ++olabel_count[arc.olabel];
      }
      for (auto it = ilabel_count.begin(); it != ilabel_count.end(); ++it) {
        ilabel_mult_ += it->second * it->second;
      }
      for (auto it = olabel_count.begin(); it != olabel_count.end(); ++it) {
        olabel_mult_ += it->second * it->second;
      }
    }
    if (narcs_ > 0) {
      ilabel_mult_ /= narcs_;
      olabel_mult_ /= narcs_;
    }
    {
      std::vector<StateId> cc;
      CcVisitor<Arc> cc_visitor(&cc);
      FifoQueue<StateId> fifo_queue;
      switch (arc_filter_type) {
        case script::ArcFilterType::ANY: {
          Visit(fst, &cc_visitor, &fifo_queue);
          break;
        }
        case script::ArcFilterType::EPSILON: {
          Visit(fst, &cc_visitor, &fifo_queue, EpsilonArcFilter<Arc>());
          break;
        }
        case script::ArcFilterType::INPUT_EPSILON: {
          Visit(fst, &cc_visitor, &fifo_queue, InputEpsilonArcFilter<Arc>());
          break;
        }
        case script::ArcFilterType::OUTPUT_EPSILON: {
          Visit(fst, &cc_visitor, &fifo_queue, OutputEpsilonArcFilter<Arc>());
          break;
        }
      }
      for (StateId s = 0; s < cc.size(); ++s) {
        if (cc[s] >= ncc_) ncc_ = cc[s] + 1;
      }
    }
    {
      std::vector<StateId> scc;
      std::vector<bool> access, coaccess;
      uint64_t props = 0;
      SccVisitor<Arc> scc_visitor(&scc, &access, &coaccess, &props);
      switch (arc_filter_type) {
        case script::ArcFilterType::ANY: {
          DfsVisit(fst, &scc_visitor);
          break;
        }
        case script::ArcFilterType::EPSILON: {
          DfsVisit(fst, &scc_visitor, EpsilonArcFilter<Arc>());
          break;
        }
        case script::ArcFilterType::INPUT_EPSILON: {
          DfsVisit(fst, &scc_visitor, InputEpsilonArcFilter<Arc>());
          break;
        }
        case script::ArcFilterType::OUTPUT_EPSILON: {
          DfsVisit(fst, &scc_visitor, OutputEpsilonArcFilter<Arc>());
          break;
        }
      }
      for (StateId s = 0; s < scc.size(); ++s) {
        if (access[s]) ++naccess_;
        if (coaccess[s]) ++ncoaccess_;
        if (access[s] && coaccess[s]) ++nconnect_;
        if (scc[s] >= nscc_) nscc_ = scc[s] + 1;
      }
    }
    LookAheadMatcher<Fst<Arc>> imatcher(fst, MATCH_INPUT);
    input_match_type_ = imatcher.Type(test_properties);
    input_lookahead_ = imatcher.Flags() & kInputLookAheadMatcher;
    LookAheadMatcher<Fst<Arc>> omatcher(fst, MATCH_OUTPUT);
    output_match_type_ = omatcher.Type(test_properties);
    output_lookahead_ = omatcher.Flags() & kOutputLookAheadMatcher;
  }

  // Short info.

  const std::string &FstType() const { return fst_type_; }

  const std::string &ArcType() const { return arc_type_; }

  const std::string &InputSymbols() const { return input_symbols_; }

  const std::string &OutputSymbols() const { return output_symbols_; }

  bool LongInfo() const { return long_info_; }

  script::ArcFilterType ArcFilterType() const { return arc_filter_type_; }

  // Long info.

  MatchType InputMatchType() const {
    CheckLong();
    return input_match_type_;
  }

  MatchType OutputMatchType() const {
    CheckLong();
    return output_match_type_;
  }

  bool InputLookAhead() const {
    CheckLong();
    return input_lookahead_;
  }

  bool OutputLookAhead() const {
    CheckLong();
    return output_lookahead_;
  }

  int64_t NumStates() const {
    CheckLong();
    return nstates_;
  }

  size_t NumArcs() const {
    CheckLong();
    return narcs_;
  }

  int64_t Start() const {
    CheckLong();
    return start_;
  }

  size_t NumFinal() const {
    CheckLong();
    return nfinal_;
  }

  size_t NumEpsilons() const {
    CheckLong();
    return nepsilons_;
  }

  size_t NumInputEpsilons() const {
    CheckLong();
    return niepsilons_;
  }

  size_t NumOutputEpsilons() const {
    CheckLong();
    return noepsilons_;
  }

  double InputLabelMultiplicity() const {
    CheckLong();
    return ilabel_mult_;
  }

  double OutputLabelMultiplicity() const {
    CheckLong();
    return olabel_mult_;
  }

  size_t NumAccessible() const {
    CheckLong();
    return naccess_;
  }

  size_t NumCoAccessible() const {
    CheckLong();
    return ncoaccess_;
  }

  size_t NumConnected() const {
    CheckLong();
    return nconnect_;
  }

  size_t NumCc() const {
    CheckLong();
    return ncc_;
  }

  size_t NumScc() const {
    CheckLong();
    return nscc_;
  }

  uint64_t Properties() const {
    CheckLong();
    return properties_;
  }

  void Info() const;

 private:
  void CheckLong() const {
    if (!long_info_)
      FSTERROR() << "FstInfo: Method only available with long info signature";
  }

  std::string fst_type_;
  std::string input_symbols_;
  std::string output_symbols_;
  int64_t nstates_;
  size_t narcs_;
  int64_t start_;
  size_t nfinal_;
  size_t nepsilons_;
  size_t niepsilons_;
  size_t noepsilons_;
  double ilabel_mult_;
  double olabel_mult_;
  size_t naccess_;
  size_t ncoaccess_;
  size_t nconnect_;
  size_t ncc_;
  size_t nscc_;
  MatchType input_match_type_;
  MatchType output_match_type_;
  bool input_lookahead_;
  bool output_lookahead_;
  uint64_t properties_;
  script::ArcFilterType arc_filter_type_;
  bool long_info_;
  std::string arc_type_;
};

// Prints `properties` to `ostrm` in a user-friendly multi-line format.
void PrintProperties(std::ostream &ostrm, uint64_t properties);

// Prints `header` to `ostrm` in a user-friendly multi-line format.
void PrintHeader(std::ostream &ostrm, const FstHeader &header);

}  // namespace fst

#endif  // FST_SCRIPT_INFO_IMPL_H_
