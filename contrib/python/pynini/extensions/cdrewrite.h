// Copyright 2016-2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//


#ifndef PYNINI_CDREWRITE_H_
#define PYNINI_CDREWRITE_H_

// Compiles context-dependent rewrite rules into weighted transducers.
//
// For more information on the compilation procedure, see:
//
// Mohri, M., and Sproat, R. 1996. An efficient compiler for weighted rewrite
// rules. In Proc. ACL, pages 231-238.

#include <memory>
#include <utility>
#include <vector>

#include <fst/log.h>
#include <fst/compat.h>
#include <fst/arc-map.h>
#include <fst/arcsort.h>
#include <fst/closure.h>
#include <fst/compose.h>
#include <fst/concat.h>
#include <fst/determinize.h>
#include <fst/fst.h>
#include <fst/minimize.h>
#include <fst/mutable-fst.h>
#include <fst/project.h>
#include <fst/reverse.h>
#include <fst/rmepsilon.h>
#include <fst/vector-fst.h>
#include "checkprops.h"
#include "cross.h"
#include "optimize.h"

namespace fst {

enum CDRewriteDirection { LEFT_TO_RIGHT, RIGHT_TO_LEFT, SIMULTANEOUS };

enum CDRewriteMode { OBLIGATORY, OPTIONAL };

namespace internal {

// This class is used to represent context-dependent rewrite rules. A given rule
// can be compile into a weighted transducer using different parameters
// (direction, mode, alphabet) by calling Compile. See comments before
// constructor and Compile member function for detailed usage. Most users should
// use the CDRewriteCompile functions rather than the class itself.
template <class Arc>
class CDRewriteRule {
 public:
  using Label = typename Arc::Label;
  using Weight = typename Arc::Weight;

  // Creates object representing the context-dependent rewrite rule:
  //
  //   phi -> psi / lamba __ rho .
  //
  // If phiXpsi is true, psi is a transducer with input domain phi instead of
  // an acceptor.
  //
  // phi, lambda, and rho must be unweighted acceptors and psi must be a
  // weighted transducer when phiXpsi is true and a weighted acceptor
  // otherwise.
  CDRewriteRule(const Fst<Arc> &phi, const Fst<Arc> &psi,
                const Fst<Arc> &lambda, const Fst<Arc> &rho, bool phiXpsi,
                Label initial_boundary_marker = kNoLabel,
                Label final_boundary_marker = kNoLabel)
      : phi_(phi.Copy()),
        psi_(psi.Copy()),
        lambda_(lambda.Copy()),
        rho_(rho.Copy()),
        phiXpsi_(phiXpsi),
        initial_boundary_marker_(initial_boundary_marker),
        final_boundary_marker_(final_boundary_marker) {}

  // Builds the transducer representing the context-dependent rewrite rule.
  // sigma is an FST specifying (the closure of) the alphabet for the resulting
  // transducer. The dir argument can be LEFT_TO_RIGHT, RIGHT_TO_LEFT or
  // SIMULTANEOUS; mode can be OBLIGATORY or OPTIONAL; sigma must be an
  // unweighted acceptor representing a bifix code.
  //
  // The error bit on the output FST is set if any argument does not satisfy the
  // preconditions.
  void Compile(const Fst<Arc> &sigma, MutableFst<Arc> *fst,
               CDRewriteDirection dir = LEFT_TO_RIGHT,
               CDRewriteMode mode = OBLIGATORY);

 private:
  enum MarkerType { MARK = 1, CHECK = 2, CHECK_COMPLEMENT = 3};

  void MakeMarker(VectorFst<StdArc> *fst, const VectorFst<StdArc> &sigma,
                  MarkerType type,
                  const std::vector<std::pair<Label, Label>> &markers);

  void IgnoreMarkers(MutableFst<Arc> *fst,
                     const std::vector<std::pair<Label, Label>> &markers);

  void AddMarkersToSigma(MutableFst<Arc> *sigma,
                         const std::vector<std::pair<Label, Label>> &markers);

  void AppendMarkers(MutableFst<Arc> *fst,
                     const std::vector<std::pair<Label, Label>> &markers);

  void PrependMarkers(MutableFst<Arc> *fst,
                      const std::vector<std::pair<Label, Label>> &markers);

  void MakeFilter(const Fst<Arc> &beta, const Fst<Arc> &sigma,
                  MutableFst<Arc> *filter, MarkerType type,
                  const std::vector<std::pair<Label, Label>> &markers,
                  bool reverse);

  void MakeReplace(MutableFst<Arc> *fst, const Fst<Arc> &sigma);

  static Label MaxLabel(const Fst<Arc> &fst);

  // Constructs transducer that either inserts or deletes boundary markers.
  void HandleBoundaryMarkers(const Fst<Arc> &sigma, VectorFst<Arc> *final_fst,
                             bool del, bool add_initial_boundary_marker,
                             bool add_final_boundary_marker);

  // Constructs epsilon:initial sigma* epsilon:final_fst
  void BoundaryInserter(const Fst<Arc> &sigma, VectorFst<Arc> *final_fst,
                        bool add_initial_boundary_marker,
                        bool add_final_boundary_marker);

  // Constructs initial:epsilon sigma* final_fst:epsilon
  void BoundaryDeleter(const Fst<Arc> &sigma, VectorFst<Arc> *final_fst,
                       bool add_initial_boundary_marker,
                       bool add_final_boundary_marker);

  // Does the FST have this label on some arc?
  static bool HasArcWithLabel(const Fst<Arc> &fst, Label label);

  std::unique_ptr<Fst<Arc>> phi_;
  std::unique_ptr<Fst<Arc>> psi_;
  std::unique_ptr<Fst<Arc>> lambda_;
  std::unique_ptr<Fst<Arc>> rho_;
  const bool phiXpsi_;
  CDRewriteDirection dir_;
  CDRewriteMode mode_;

  // The following labels are used to represent the symbols: <_1, <_2 and > in
  // Mohri and Sproat. For instance, for left-to-right obligatory rules, <_1 is
  // used to mark the start of an occurrence of phi that need to be rewritten,
  // <_2 is used to mark the start of an occurrence of phi that should not be
  // rewritten and > is used to mark the end of occurrences of phi.
  Label lbrace1_;
  Label lbrace2_;
  Label rbrace_;

  // The following labels are used in rules where we need to explicitly mark the
  // beginning or end of a string. They should be set to kNoLabel whenever the
  // corresponding boundary is not needed.
  Label initial_boundary_marker_;
  Label final_boundary_marker_;

  CDRewriteRule(const CDRewriteRule &) = delete;
  CDRewriteRule &operator=(const CDRewriteRule &) = delete;
};

// Turns an FST into a marker transducer of specified type using the specified
// markers (markers) for the regular expression represented by the FST.
template <class Arc>
void CDRewriteRule<Arc>::MakeMarker(
    VectorFst<StdArc> *fst, const VectorFst<StdArc> &sigma, MarkerType type,
    const std::vector<std::pair<Label, Label>> &markers) {
  using StateId = StdArc::StateId;
  using Weight = StdArc::Weight;
  if (fst->Properties(kAcceptor, true) != kAcceptor) {
    FSTERROR() << "CDRewriteRule::MakeMarker: input FST must be an acceptor";
    fst->SetProperties(kError, kError);
    return;
  }
  auto num_states = fst->NumStates();
  // When num_states == 0, *fst is really Complement(sigma) and we build the
  // result upon sigma (== Complement(Complement(sigma))) directly in each case.
  switch (type) {
    case MARK:
      // Type 1: Insert (or delete) markers after each match.
      if (num_states == 0) {
        *fst = sigma;
      } else {
        for (StateId s = 0; s < num_states; ++s) {
          if (fst->Final(s) == Weight::Zero()) {
            fst->SetFinal(s);
          } else {
            const auto i = fst->AddState();
            fst->SetFinal(i, fst->Final(s));
            for (ArcIterator<StdFst> aiter(*fst, s); !aiter.Done();
                 aiter.Next()) {
              fst->AddArc(i, aiter.Value());
            }
            fst->SetFinal(s, Weight::Zero());
            fst->DeleteArcs(s);
            for (const auto &marker : markers) {
              fst->AddArc(s, StdArc(marker.first, marker.second, i));
            }
          }
        }
      }
      break;
    case CHECK:
      // Type 2: Check that each marker is preceded by a match.
      if (num_states == 0) {
        *fst = sigma;
      } else {
        for (StateId s = 0; s < num_states; ++s) {
          if (fst->Final(s) == Weight::Zero()) {
            fst->SetFinal(s);
          } else {
            for (const auto &marker : markers) {
              fst->AddArc(s, StdArc(marker.first, marker.second, s));
            }
          }
        }
      }
      break;
    case CHECK_COMPLEMENT:
      // Type 3: Check that each marker is not preceded by a match.
      if (num_states == 0) {
        *fst = sigma;
        num_states = fst->NumStates();
        for (StateId s = 0; s < num_states; ++s) {
          if (fst->Final(s) != Weight::Zero()) {
            for (const auto &marker : markers) {
              fst->AddArc(s, StdArc(marker.first, marker.second, s));
            }
          }
        }
      } else {
        for (StateId s = 0; s < num_states; ++s) {
          if (fst->Final(s) == Weight::Zero()) {
            fst->SetFinal(s);
            for (const auto &marker : markers) {
              fst->AddArc(s, StdArc(marker.first, marker.second, s));
            }
          }
        }
      }
      break;
  }
}

// Adds self loops allowing the markers at all states specified by markers in
// any position, corresponding to the subscripting conventions of Mohri &
// Sproat.
template <class Arc>
void CDRewriteRule<Arc>::IgnoreMarkers(
    MutableFst<Arc> *fst, const std::vector<std::pair<Label, Label>> &markers) {
  for (StateIterator<MutableFst<Arc>> siter(*fst); !siter.Done();
       siter.Next()) {
    const auto s = siter.Value();
    for (const auto &marker : markers) {
      fst->AddArc(s, Arc(marker.first, marker.second, s));
    }
  }
}

// Turns Sigma^* into (Sigma union markers)^*.
template <class Arc>
void CDRewriteRule<Arc>::AddMarkersToSigma(
    MutableFst<Arc> *sigma,
    const std::vector<std::pair<Label, Label>> &markers) {
  for (StateIterator<MutableFst<Arc>> siter(*sigma); !siter.Done();
       siter.Next()) {
    const auto s = siter.Value();
    if (sigma->Final(s) != Arc::Weight::Zero()) {
      for (const auto &marker : markers) {
        sigma->AddArc(s, Arc(marker.first, marker.second, sigma->Start()));
      }
    }
  }
}

// Adds loops at the initial state for all alphabet symbols in the current
// alphabet (sigma).
template <class Arc>
inline void PrependSigmaStar(MutableFst<Arc> *fst,
                             const Fst<Arc> &sigma) {
  Concat(sigma, fst);
  RmEpsilon(fst);
}

// Appends a transition for each of the (ilabel, olabel) pairs specified by the
// markers.
template <class Arc>
void CDRewriteRule<Arc>::AppendMarkers(
    MutableFst<Arc> *fst, const std::vector<std::pair<Label, Label>> &markers) {
  using Weight = typename Arc::Weight;
  VectorFst<Arc> temp_fst;
  const auto start_state = temp_fst.AddState();
  const auto final_state = temp_fst.AddState();
  temp_fst.SetStart(start_state);
  temp_fst.SetFinal(final_state);
  for (const auto &marker : markers) {
    temp_fst.EmplaceArc(start_state, marker.first, marker.second, final_state);
  }
  Concat(fst, temp_fst);
}

// Prepends a transition for each of the (ilabel, olabel) pairs specified by
// the markers.
template <class Arc>
void CDRewriteRule<Arc>::PrependMarkers(
    MutableFst<Arc> *fst, const std::vector<std::pair<Label, Label>> &markers) {
  if (fst->Start() == kNoStateId) fst->SetStart(fst->AddState());
  const auto new_start = fst->AddState();
  const auto old_start = fst->Start();
  fst->SetStart(new_start);
  for (const auto &marker : markers) {
    fst->AddArc(new_start, Arc(marker.first, marker.second, old_start));
  }
}

//
// Creates the marker transducer of the specified type for the markers defined
// in the markers argument for the regular expression sigma^* beta. When
// reverse is true, the reverse of the marker transducer corresponding to
// sigma^* reverse(beta).
//
// The operations in MakeFilter do not depend on the semiring, and indeed for
// some semirings the various optimizations needed in MakeFilter cause
// problems. We therefore map incoming acceptors in whatever semiring to
// unweighted acceptors. Ideally this would be in the boolean, but we simulate
// it with the tropical.
template <class Arc>
void CDRewriteRule<Arc>::MakeFilter(
    const Fst<Arc> &beta, const Fst<Arc> &sigma, MutableFst<Arc> *filter,
    MarkerType type, const std::vector<std::pair<Label, Label>> &markers,
    bool reverse) {
  VectorFst<StdArc> ufilter;
  static const RmWeightMapper<Arc, StdArc> stdarc_mapper;
  ArcMap(beta, &ufilter, stdarc_mapper);
  VectorFst<StdArc> usigma;
  ArcMap(sigma, &usigma, stdarc_mapper);
  if (ufilter.Start() == kNoStateId) {
    ufilter.SetStart(ufilter.AddState());
  }
  static const IdentityArcMapper<StdArc> imapper;
  if (reverse) {
    Reverse(ArcMapFst(ufilter, imapper), &ufilter);
    VectorFst<StdArc> reversed_sigma;
    Reverse(usigma, &reversed_sigma);
    RmEpsilon(&reversed_sigma);
    PrependSigmaStar(&ufilter, reversed_sigma);
  } else {
    PrependSigmaStar(&ufilter, usigma);
  }
  RmEpsilon(&ufilter);
  DeterminizeFst<StdArc> det(ufilter);
  ArcMap(det, &ufilter, imapper);
  Minimize(&ufilter);
  MakeMarker(&ufilter, usigma, type, markers);
  if (reverse) {
    Reverse(ArcMapFst(ufilter, imapper), &ufilter);
  }
  static const ILabelCompare<StdArc> icomp;
  ArcSort(&ufilter, icomp);
  static const RmWeightMapper<StdArc, Arc> rmweight;
  ArcMap(ufilter, filter, rmweight);
}

// Turns the FST representing phi X psi into a "replace" transducer.
template <class Arc>
void CDRewriteRule<Arc>::MakeReplace(MutableFst<Arc> *fst,
                                     const Fst<Arc> &sigma) {
  using Weight = typename Arc::Weight;
  Optimize(fst);
  if (fst->Start() == kNoStateId) fst->SetStart(fst->AddState());
  // Label pairs for to be added arcs to the initial state or from the final
  // states.
  std::pair<Label, Label> initial_pair;
  std::pair<Label, Label> final_pair;
  // Label for self-loops to be added at the new initial state (to be created)
  // and at every other state.
  std::vector<std::pair<Label, Label>> initial_loops;
  std::vector<std::pair<Label, Label>> all_loops;
  switch (mode_) {
    case OBLIGATORY:
      all_loops.emplace_back(lbrace1_, 0);
      all_loops.emplace_back(lbrace2_, 0);
      all_loops.emplace_back(rbrace_, 0);
      switch (dir_) {
        case LEFT_TO_RIGHT:
          initial_pair = {lbrace1_, lbrace1_};
          final_pair = {rbrace_, 0};
          initial_loops.emplace_back(lbrace2_, lbrace2_);
          initial_loops.emplace_back(rbrace_, 0);
          break;
        case RIGHT_TO_LEFT:
          initial_pair = {rbrace_, 0};
          final_pair = {lbrace1_, lbrace1_};
          initial_loops.emplace_back(lbrace2_, lbrace2_);
          initial_loops.emplace_back(rbrace_, 0);
          break;
        case SIMULTANEOUS:
          initial_pair = {lbrace1_, 0};
          final_pair = {rbrace_, 0};
          initial_loops.emplace_back(lbrace2_, 0);
          initial_loops.emplace_back(rbrace_, 0);
          break;
      }
      break;
    case OPTIONAL:
      all_loops.emplace_back(rbrace_, 0);
      initial_loops.emplace_back(rbrace_, 0);
      switch (dir_) {
        case LEFT_TO_RIGHT:
          initial_pair = {0, lbrace1_};
          final_pair = {rbrace_, 0};
          break;
        case RIGHT_TO_LEFT:
          initial_pair = {rbrace_, 0};
          final_pair = {0, lbrace1_};
          break;
        case SIMULTANEOUS:
          initial_pair = {lbrace1_, 0};
          final_pair = {rbrace_, 0};
          break;
      }
      break;
  }
  // Adds loops at all states.
  IgnoreMarkers(fst, all_loops);
  // Creates new initial and final states.
  const auto start_state = fst->AddState();
  const auto final_state = fst->AddState();
  fst->AddArc(start_state,
              Arc(initial_pair.first, initial_pair.second, fst->Start()));
  // Makes all final states non final with transition to new final state.
  for (StateIterator<MutableFst<Arc>> siter(*fst); !siter.Done();
       siter.Next()) {
    const auto s = siter.Value();
    if (fst->Final(s) == Weight::Zero()) continue;
    fst->AddArc(s, Arc(final_pair.first, final_pair.second, fst->Final(s),
                       final_state));
    fst->SetFinal(s, Weight::Zero());
  }
  fst->SetFinal(final_state);
  fst->SetFinal(start_state);
  fst->SetStart(start_state);
  // Adds required loops at new initial state.
  VectorFst<Arc> sigma_m(sigma);
  AddMarkersToSigma(&sigma_m, initial_loops);
  PrependSigmaStar(fst, sigma_m);
  Closure(fst, CLOSURE_STAR);
  Optimize(fst);
  ArcSort(fst, ILabelCompare<Arc>());
}

template <class Arc>
typename Arc::Label CDRewriteRule<Arc>::MaxLabel(const Fst<Arc> &fst) {
  Label max = kNoLabel;
  for (StateIterator<Fst<Arc>> siter(fst); !siter.Done(); siter.Next()) {
    for (ArcIterator<Fst<Arc>> aiter(fst, siter.Value()); !aiter.Done();
         aiter.Next()) {
      const auto &arc = aiter.Value();
      if (arc.ilabel > max) max = arc.ilabel;
      if (arc.olabel > max) max = arc.olabel;
    }
  }
  return max;
}

// Builds the transducer representing the context-dependent rewrite rule. sigma
// is an FST specifying (the closure of) the alphabet for the resulting
// transducer. dir can be LEFT_TO_RIGHT, RIGHT_TO_LEFT or SIMULTANEOUS. mode can
// be OBLIGATORY or OPTIONAL. sigma must be an unweighted acceptor representing
// a bifix code.
//
// The error bit on the output FST is set if any argument does not satisfy the
// preconditions.
template <class Arc>
void CDRewriteRule<Arc>::Compile(const Fst<Arc> &sigma, MutableFst<Arc> *fst,
                                 CDRewriteDirection dir, CDRewriteMode mode) {
  dir_ = dir;
  mode_ = mode;
  if (!CheckUnweightedAcceptor(*phi_, "CDRewriteRule::Compile", "phi")) {
    fst->SetProperties(kError, kError);
    return;
  }
  if (!CheckUnweightedAcceptor(*lambda_, "CDRewriteRule::Compile", "lambda")) {
    fst->SetProperties(kError, kError);
    return;
  }
  if (!CheckUnweightedAcceptor(*rho_, "CDRewriteRule::Compile", "rho")) {
    fst->SetProperties(kError, kError);
    return;
  }
  if (!phiXpsi_ && (psi_->Properties(kAcceptor, true) != kAcceptor)) {
    FSTERROR() << "CDRewriteRuleRule::Compile: psi must be an acceptor or "
               << "phiXpsi must be set to true";
    fst->SetProperties(kError, kError);
    return;
  }
  if (!CheckUnweightedAcceptor(sigma, "CDRewriteRule::Compile", "sigma")) {
    fst->SetProperties(kError, kError);
    return;
  }
  static const ILabelCompare<Arc> icomp;
  static const IdentityArcMapper<Arc> imapper;
  VectorFst<Arc> mutable_sigma(sigma);
  // Determines whether we have initial and final boundaries and whether we need
  // to add them to sigma. The markers can be referenced in phi or in,
  // respectively, lambda or rho.
  const bool add_initial_boundary_marker =
      HasArcWithLabel(*lambda_, initial_boundary_marker_) ||
      HasArcWithLabel(*phi_, initial_boundary_marker_);
  const bool add_final_boundary_marker =
      HasArcWithLabel(*rho_, final_boundary_marker_) ||
      HasArcWithLabel(*phi_, final_boundary_marker_);
  if (add_initial_boundary_marker) {
    AddMarkersToSigma(&mutable_sigma, {{initial_boundary_marker_,
                                        initial_boundary_marker_}});
  }
  if (add_final_boundary_marker) {
    AddMarkersToSigma(&mutable_sigma, {{final_boundary_marker_,
                                        final_boundary_marker_}});
  }
  rbrace_ = MaxLabel(mutable_sigma) + 1;
  lbrace1_ = rbrace_ + 1;
  lbrace2_ = rbrace_ + 2;
  VectorFst<Arc> sigma_rbrace(mutable_sigma);
  AddMarkersToSigma(&sigma_rbrace, {{rbrace_, rbrace_}});
  fst->DeleteStates();
  VectorFst<Arc> replace;
  if (phiXpsi_) {
    ArcMap(*psi_, &replace, imapper);
  } else {
    Cross(*phi_, *psi_, &replace);
  }
  MakeReplace(&replace, mutable_sigma);
  switch (dir_) {
    case LEFT_TO_RIGHT: {
      // Builds r filter.
      VectorFst<Arc> r;
      MakeFilter(*rho_, mutable_sigma, &r, MARK, {{0, rbrace_}}, true);
      switch (mode_) {
        case OBLIGATORY: {
          VectorFst<Arc> phi_rbrace;  // Appends > after phi_, matches all >.
          ArcMap(*phi_, &phi_rbrace, imapper);
          IgnoreMarkers(&phi_rbrace, {{rbrace_, rbrace_}});
          AppendMarkers(&phi_rbrace, {{rbrace_, rbrace_}});
          // Builds f filter.
          VectorFst<Arc> f;
          MakeFilter(phi_rbrace, sigma_rbrace, &f, MARK,
                     {{0, lbrace1_}, {0, lbrace2_}}, true);
          // Builds l1 filter.
          VectorFst<Arc> l1;
          MakeFilter(*lambda_, mutable_sigma, &l1, CHECK, {{lbrace1_, 0}},
                     false);
          IgnoreMarkers(&l1, {{lbrace2_, lbrace2_}});
          ArcSort(&l1, ILabelCompare<Arc>());
          // Builds l2 filter.
          VectorFst<Arc> l2;
          MakeFilter(*lambda_, mutable_sigma, &l2, CHECK_COMPLEMENT,
                     {{lbrace2_, 0}}, false);
          // Builds (((r o f) o replace) o l1) o l2.
          VectorFst<Arc> c;
          Compose(r, f, &c);
          Compose(c, replace, fst);
          Compose(*fst, l1, &c);
          Compose(c, l2, fst);
          break;
        }
        case OPTIONAL: {
          // Builds l filter.
          VectorFst<Arc> l;
          MakeFilter(*lambda_, mutable_sigma, &l, CHECK, {{lbrace1_, 0}},
                     false);
          // Builds (r o replace) o l.
          VectorFst<Arc> c;
          Compose(r, replace, &c);
          Compose(c, l, fst);
          break;
        }
      }
      break;
    }
    case RIGHT_TO_LEFT: {
      // Builds l filter.
      VectorFst<Arc> l;
      MakeFilter(*lambda_, mutable_sigma, &l, MARK, {{0, rbrace_}}, false);
      switch (mode_) {
        case OBLIGATORY: {
          VectorFst<Arc> rbrace_phi;  // Prepends > before phi, matches all >
          ArcMap(*phi_, &rbrace_phi, imapper);
          IgnoreMarkers(&rbrace_phi, {{rbrace_, rbrace_}});
          PrependMarkers(&rbrace_phi, {{rbrace_, rbrace_}});
          // Builds f filter.
          VectorFst<Arc> f;
          MakeFilter(rbrace_phi, sigma_rbrace, &f, MARK,
                     {{0, lbrace1_}, {0, lbrace2_}}, false);
          // Builds r1 filter.
          VectorFst<Arc> r1;
          MakeFilter(*rho_, mutable_sigma, &r1, CHECK, {{lbrace1_, 0}}, true);
          IgnoreMarkers(&r1, {{lbrace2_, lbrace2_}});
          ArcSort(&r1, icomp);
          // Builds r2 filter.
          VectorFst<Arc> r2;
          MakeFilter(*rho_, mutable_sigma, &r2, CHECK_COMPLEMENT,
                     {{lbrace2_, 0}},
                     true);
          // Builds (((l o f) o replace) o r1) o r2.
          VectorFst<Arc> c;
          Compose(l, f, &c);
          Compose(c, replace, fst);
          Compose(*fst, r1, &c);
          Compose(c, r2, fst);
          break;
        }
        case OPTIONAL: {
          // Builds r filter.
          VectorFst<Arc> r;
          MakeFilter(*rho_, mutable_sigma, &r, CHECK, {{lbrace1_, 0}}, true);
          // Builds (l o replace) o r.
          VectorFst<Arc> c;
          Compose(l, replace, &c);
          Compose(c, r, fst);
          break;
        }
      }
      break;
    }
    case SIMULTANEOUS: {
      // Builds r filter.
      VectorFst<Arc> r;
      MakeFilter(*rho_, mutable_sigma, &r, MARK, {{0, rbrace_}}, true);
      switch (mode_) {
        case OBLIGATORY: {
          VectorFst<Arc> phi_rbrace;  // Appends > after phi, matches all >.
          ArcMap(*phi_, &phi_rbrace, imapper);
          IgnoreMarkers(&phi_rbrace, {{rbrace_, rbrace_}});
          AppendMarkers(&phi_rbrace, {{rbrace_, rbrace_}});
          // Builds f filter.
          VectorFst<Arc> f;
          MakeFilter(phi_rbrace, sigma_rbrace, &f, MARK,
                     {{0, lbrace1_}, {0, lbrace2_}}, true);
          // Builds l1 filter.
          VectorFst<Arc> l1;
          MakeFilter(*lambda_, mutable_sigma, &l1, CHECK,
                     {{lbrace1_, lbrace1_}}, false);
          IgnoreMarkers(&l1, {{lbrace2_, lbrace2_}, {rbrace_, rbrace_}});
          ArcSort(&l1, icomp);
          // Builds l2 filter.
          VectorFst<Arc> l2;
          MakeFilter(*lambda_, mutable_sigma, &l2, CHECK_COMPLEMENT,
                     {{lbrace2_, lbrace2_}}, false);
          IgnoreMarkers(&l2, {{lbrace1_, lbrace1_}, {rbrace_, rbrace_}});
          ArcSort(&l2, icomp);
          // Builds (((r o f) o l1) o l2) o replace.
          VectorFst<Arc> c;
          Compose(r, f, &c);
          Compose(c, l1, fst);
          Compose(*fst, l2, &c);
          Compose(c, replace, fst);
          break;
        }
        case OPTIONAL: {
          // Builds l filter.
          VectorFst<Arc> l;
          MakeFilter(*lambda_, mutable_sigma, &l, CHECK, {{0, lbrace1_}},
                     false);
          IgnoreMarkers(&l, {{rbrace_, rbrace_}});
          static const ILabelCompare<Arc> icomp;
          ArcSort(&l, icomp);
          // Builds (r o l) o replace.
          VectorFst<Arc> c;
          Compose(r, l, &c);
          Compose(c, replace, fst);
          break;
        }
      }
      break;
    }
  }
  // If we need to handle boundary markers we do an extra composition of the
  // boundary inserter and boundary deleter.
  if (add_initial_boundary_marker || add_final_boundary_marker) {
    VectorFst<Arc> inserter;
    BoundaryInserter(sigma, &inserter,
                     add_initial_boundary_marker, add_final_boundary_marker);
    VectorFst<Arc> deleter;
    BoundaryDeleter(sigma, &deleter,
                    add_initial_boundary_marker, add_final_boundary_marker);
    VectorFst<Arc> tmp;
    ArcSort(fst, icomp);
    Compose(inserter, *fst, &tmp);
    static const OLabelCompare<Arc> ocomp;
    ArcSort(&tmp, ocomp);
    Compose(tmp, deleter, fst);
  }
  Optimize(fst);
  ArcSort(fst, icomp);
}

template <class Arc>
void CDRewriteRule<Arc>::HandleBoundaryMarkers(const Fst<Arc> &sigma,
                                               VectorFst<Arc> *final_fst,
                                               bool del,
                                               bool add_initial_boundary_marker,
                                               bool add_final_boundary_marker) {
  VectorFst<Arc> initial;
  auto start = initial.AddState();
  initial.SetStart(start);
  if (add_initial_boundary_marker) {
    const auto end = initial.AddState();
    initial.SetFinal(end);
    initial.AddArc(start, Arc(del ? initial_boundary_marker_ : 0,
                              del ? 0 : initial_boundary_marker_, end));
  } else {
    initial.SetFinal(start);
  }
  start = final_fst->AddState();
  final_fst->SetStart(start);
  if (add_final_boundary_marker) {
    const auto end = final_fst->AddState();
    final_fst->SetFinal(end);
    final_fst->AddArc(start, Arc(del ? final_boundary_marker_ : 0,
                                 del ? 0 : final_boundary_marker_, end));
  } else {
    final_fst->SetFinal(start);
  }
  Concat(&initial, sigma);
  Concat(initial, final_fst);
  // Fixes bug whereby
  //
  // CDRewrite["" : "a", "", "", sigma]
  //
  // produces no output ("rewrite failed") because the rule inserts an "a"
  // before the "[BOS]" and after the "[EOS]", in addition to anywhere in the
  // input string. The output filter "[BOS] sigma [EOS]" blocks these, so
  // that in an obligatory application, you get no output. The new version
  // deletes anything from sigma that occurs before the [BOS] or after the
  // [EOS], so that you only get insertion where you should, in the string
  // proper. Note that only in an insertion with no specified left or right
  // context will this situation arise.
  //
  // The slight drawback is that if someone writes an ill-formed
  // insertion rule such as
  //
  // CDRewrite["" : "a" , "[EOS]", "", sigma]
  //
  // (note the misplaced [EOS]), then this will give an output---though not
  // with the illicit inserted "a" as written, as opposed to simply failing as
  // it would have before. It's not clear this is a bad result.
  if (del && (add_initial_boundary_marker || add_final_boundary_marker)) {
    VectorFst<Arc> del_sigma(sigma);
    // Creates the sigma^* deletion FST.
    for (StateIterator<VectorFst<Arc>> siter(del_sigma);
         !siter.Done();
         siter.Next()) {
      for (MutableArcIterator<VectorFst<Arc>> aiter(&del_sigma, siter.Value());
           !aiter.Done();
           aiter.Next()) {
        auto arc = aiter.Value();
        arc.olabel = 0;
        aiter.SetValue(arc);
      }
    }
    VectorFst<Arc> initial_del_sigma;
    if (add_initial_boundary_marker) {
      initial_del_sigma = del_sigma;
    } else {
      start = initial_del_sigma.AddState();
      initial_del_sigma.SetStart(start);
      initial_del_sigma.SetFinal(start);
    }
    VectorFst<Arc> final_del_sigma;
    if (add_final_boundary_marker) {
      final_del_sigma = del_sigma;
    } else {
      start = final_del_sigma.AddState();
      final_del_sigma.SetStart(start);
      final_del_sigma.SetFinal(start);
    }
    Concat(&initial_del_sigma, *final_fst);
    Concat(initial_del_sigma, &final_del_sigma);
    *final_fst = final_del_sigma;
  }
}

template <class Arc>
void CDRewriteRule<Arc>::BoundaryInserter(const Fst<Arc> &sigma,
                                          VectorFst<Arc> *final_fst,
                                          bool add_initial_boundary_marker,
                                          bool add_final_boundary_marker) {
  HandleBoundaryMarkers(sigma, final_fst, false,
                        add_initial_boundary_marker,
                        add_final_boundary_marker);
  Optimize(final_fst);
  ArcSort(final_fst, OLabelCompare<Arc>());
}

template <class Arc>
void CDRewriteRule<Arc>::BoundaryDeleter(const Fst<Arc> &sigma,
                                         VectorFst<Arc> *final_fst,
                                         bool add_initial_boundary_marker,
                                         bool add_final_boundary_marker) {
  HandleBoundaryMarkers(sigma, final_fst, true,
                        add_initial_boundary_marker,
                        add_final_boundary_marker);
  Optimize(final_fst);
  ArcSort(final_fst, ILabelCompare<Arc>());
}

template <class Arc>
bool CDRewriteRule<Arc>::HasArcWithLabel(const Fst<Arc> &fst, Label label) {
  if (label == kNoLabel) return false;
  for (StateIterator<Fst<Arc>> siter(fst); !siter.Done(); siter.Next()) {
    for (ArcIterator<Fst<Arc>> aiter(fst, siter.Value());
         !aiter.Done();
         aiter.Next()) {
      Arc arc = aiter.Value();
      if (arc.ilabel == label || arc.olabel == label)
        return true;
    }
  }
  return false;
}

}  // namespace internal.

// Builds a transducer representing the context-dependent rewrite rule:
//
//   phi -> psi / lamba __ rho .
//
// If phiXpsi is true, psi is a transducer with input domain phi instead of
// an acceptor.
//
// phi, lambda, and rho must be unweighted acceptors and psi must be a
// weighted transducer when phiXpsi is true and a weighted acceptor
// otherwise. sigma is an FST specifying (the closure of) the alphabet
// for the resulting transducer. dir can be LEFT_TO_RIGHT, RIGHT_TO_LEFT or
// SIMULTANEOUS. mode can be OBLIGATORY or OPTIONAL. sigma must be an unweighted
// acceptor representing a bifix code.
//
// The error bit on the output FST is set if any argument does not satisfy the
// preconditions.
template <class Arc>
void CDRewriteCompile(const Fst<Arc> &phi, const Fst<Arc> &psi,
                      const Fst<Arc> &lambda, const Fst<Arc> &rho,
                      const Fst<Arc> &sigma, MutableFst<Arc> *fst,
                      CDRewriteDirection dir = LEFT_TO_RIGHT,
                      CDRewriteMode mode = OBLIGATORY,
                      bool phiXpsi = true,
                      typename Arc::Label initial_boundary_marker = kNoLabel,
                      typename Arc::Label final_boundary_marker = kNoLabel) {
  internal::CDRewriteRule<Arc> cdrule(phi, psi, lambda, rho, phiXpsi,
                                      initial_boundary_marker,
                                      final_boundary_marker);
  cdrule.Compile(sigma, fst, dir, mode);
}

// Builds a transducer object representing the context-dependent rewrite rule:
//
//   phi -> psi / lamba __ rho .
//
// phi, lambda, and rho must be unweighted acceptors and psi must be a
// weighted acceptor. sigma is an FST specifying (the closure of) the alphabet
// for the resulting transducer. dir can be LEFT_TO_RIGHT, RIGHT_TO_LEFT or
// SIMULTANEOUS. mode can be OBLIGATORY or OPTIONAL. sigma must be an unweighted
// acceptor representing a bifix code.
//
// The error bit on the output FST is set if any argument does not satisfy the
// preconditions.
template <class Arc>
void CDRewriteCompile(const Fst<Arc> &phi, const Fst<Arc> &psi,
                      const Fst<Arc> &lambda, const Fst<Arc> &rho,
                      const Fst<Arc> &sigma, MutableFst<Arc> *fst,
                      CDRewriteDirection dir = LEFT_TO_RIGHT,
                      CDRewriteMode mode = OBLIGATORY,
                      typename Arc::Label initial_boundary_marker = kNoLabel,
                      typename Arc::Label final_boundary_marker = kNoLabel) {
  CDRewriteCompile(phi, psi, lambda, rho, sigma, fst, dir, mode, false,
                   initial_boundary_marker, final_boundary_marker);
}

// Builds a transducer object representing the context-dependent rewrite rule:
//
//   phi -> psi / lamba __ rho .
//
// where tau represents the cross-product of phi X psi.
//
// Lambda, and rho must be unweighted acceptors. sigma is an FST specifying (the
// closure of) the alphabet for the resulting transducer. dir can be
// LEFT_TO_RIGHT, RIGHT_TO_LEFT or SIMULTANEOUS. mode can be OBLIGATORY or
// OPTIONAL. sigma must be an unweighted acceptor representing a bifix code.
//
// The error bit on the output FST is set if any argument does not satisfy the
// preconditions.
template <class Arc>
void CDRewriteCompile(const Fst<Arc> &tau, const Fst<Arc> &lambda,
                      const Fst<Arc> &rho, const Fst<Arc> &sigma,
                      MutableFst<Arc> *fst,
                      CDRewriteDirection dir = LEFT_TO_RIGHT,
                      CDRewriteMode mode = OBLIGATORY,
                      typename Arc::Label initial_boundary_marker = kNoLabel,
                      typename Arc::Label final_boundary_marker = kNoLabel) {
  VectorFst<Arc> phi(tau);
  Project(&phi, ProjectType::INPUT);
  ArcMap(&phi, RmWeightMapper<Arc>());
  Optimize(&phi);
  CDRewriteCompile(phi, tau, lambda, rho, sigma, fst, dir, mode, true,
                   initial_boundary_marker, final_boundary_marker);
}

}  // namespace fst

#endif  // PYNINI_CDREWRITE_H_

