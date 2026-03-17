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


#ifndef PYNINI_PATHS_H_
#define PYNINI_PATHS_H_

// An iterative definition of all paths of an acyclic automaton.
//
// For a given path, one can ask for the input label sequence, output label
// sequence, and total weight; the StringPathIterator also can print the label
// sequences as strings.
//
// The PathIterator class is agnostic about labels and symbol tables; the
// StringPathIterator wrapper knows about this and also checks the input FST's
// properties (e.g., to make sure that it is acyclic).

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include <fst/fst.h>
#include <fst/string.h>

namespace fst {

constexpr int32_t kInitialStateIsFinal = -2;
constexpr int32_t kNewState = -1;

// An iterator to provide a succession of paths from an automaton. Calling
// Next() gets the next path. Done() returns true if all the paths have been
// visited. Accessible path values are ILabels()---the sequence of input
// labels, OLabels()---for output labels, and Weight().
//
// Note that PathIterator is symbol table and string-agnostic; consider using
// StringPathIterator if you need either.
//
// When check_acyclic is set, checks acyclicity of the FST. An acyclic FST may
// lead to infinite loops and thus check_acyclic should only be false when the
// caller can ensure finite iteration (e.g., knowing the FST is acyclic;
// limiting the number of iterated paths).
template <class Arc>
class PathIterator {
 public:
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using ArcWeight = typename Arc::Weight;

  explicit PathIterator(const Fst<Arc> &fst, bool check_acyclic = true);

  bool Done() const { return path_states_.empty(); }

  // Checks if initialization was successful. Check this before accessing the
  // iterator if it was constructed with check_acyclic = true.
  bool Error() const { return error_; }

  const std::vector<Label> &ILabels() const { return path_ilabels_; }

  void Next();

  const std::vector<Label> &OLabels() const { return path_olabels_; }

  void Reset();

  ArcWeight Weight() const {
    auto weight = ArcWeight::One();
    for (const auto &path_weight : path_weights_) {
      weight = Times(weight, path_weight);
    }
    return weight;
  }

 protected:
  void SetError();

 private:
  // Inserts labels and weights from an arc.
  void VisitArc(const Arc &arc);

  void MaybePopLabels();

  // If initialization failed.
  bool error_;
  // Copy of FST being iterated over.
  std::unique_ptr<const Fst<Arc>> fst_;
  // Vector of states visited on this path.
  std::vector<StateId> path_states_;
  // Vector of input labels.
  std::vector<Label> path_ilabels_;
  // Vector of output labels.
  std::vector<Label> path_olabels_;
  // Vector of weights.
  std::vector<ArcWeight> path_weights_;
  // Vector of offsets for each arc iterator for each state, so that we can
  // remember where we left off. Note that -2 (kInitialStateIsFinal) and -1
  // (kNewState) here have special meanings, on which see below.
  std::vector<int32_t> arc_iterator_offsets_;
  bool pop_labels_;

  PathIterator(const PathIterator &) = delete;
  PathIterator &operator=(const PathIterator &) = delete;
};

template <class Arc>
PathIterator<Arc>::PathIterator(const Fst<Arc> &fst, bool check_acyclic)
    : error_(false), fst_(fst.Copy()), pop_labels_(false) {
  if (check_acyclic && !fst.Properties(kAcyclic, true)) {
    SetError();
    FSTERROR() << "PathIterator: Cyclic FSTs have an infinite number of paths";
    return;
  }
  Reset();
}

template <class Arc>
void PathIterator<Arc>::Reset() {
  pop_labels_ = false;
  const auto start = fst_->Start();
  if (start == kNoStateId) return;
  // Seeds the search with the start state.
  path_states_.push_back(start);
  const auto weight = fst_->Final(start);
  path_weights_.push_back(weight);
  // If the initial state is also a final state, then Next() has immediate work
  // to do, so we indicate that with kInitialStateIsFinal. Otherwise we set it
  // to kNewState, which means "I haven't started the arc iterator at this state
  // yet".
  arc_iterator_offsets_.push_back(weight == ArcWeight::Zero() ?
                                  kNewState : kInitialStateIsFinal);
  Next();
}

template <class Arc>
void PathIterator<Arc>::Next() {
  if (Done()) return;
  if (arc_iterator_offsets_.back() == kInitialStateIsFinal) {
    arc_iterator_offsets_.pop_back();
    arc_iterator_offsets_.push_back(kNewState);
    return;
  }
  // At the current state (the back of path_states_) we increment the
  // arc_iterator offset (meaning that if it's -1, aka kNewState, then we set it
  // to 0 and therefore start reading).
  StateId nextstate;
  while (!Done()) {
    const auto offset = arc_iterator_offsets_.back() + 1;
    arc_iterator_offsets_.pop_back();
    arc_iterator_offsets_.push_back(offset);
    ArcIterator<Fst<Arc>> aiter(*fst_, path_states_.back());
    aiter.Seek(offset);
    // If the arc iterator is done, then we are done at this state, and we move
    // back.
    if (aiter.Done()) {
      MaybePopLabels();
      path_states_.pop_back();
      path_weights_.pop_back();
      arc_iterator_offsets_.pop_back();
      // Otherwise we proceed moving to the current arc's next state, then break
      // out of this loop and attempt to move forward.
    } else {
      const auto &arc = aiter.Value();
      MaybePopLabels();
      path_weights_.pop_back();
      VisitArc(arc);
      nextstate = arc.nextstate;
      break;
    }
  }
  if (Done()) return;
  // Now we proceed forward until we hit a final state.
  while (nextstate != kNoStateId) {
    path_states_.push_back(nextstate);
    const auto weight = fst_->Final(nextstate);
    if (weight == ArcWeight::Zero()) {
      ArcIterator<Fst<Arc>> aiter(*fst_, nextstate);
      if (aiter.Done()) {
        // We reached a non-final state with no exiting arcs. Pop it. This
        // shouldn't happen unless someone passes an unconnected machine.
        path_states_.pop_back();
        return;
      } else {
        const auto &arc = aiter.Value();
        VisitArc(arc);
        nextstate = arc.nextstate;
        arc_iterator_offsets_.push_back(0);
      }
    } else {
      // If we are at a final state, we act as if we have taken a transition to
      // a hallucinated superfinal state which is the "real" final state and
      // which is the sole destination for any arc leaving a final state. This
      // bit of pretend is necessary so that we don't actually rewind in the
      // case that there are valid suffixes of the path terminating here, as in
      // something like /foo(bar)?/. Path weights and arc iterator offsets will
      // be popped on the next iteration, but we will not pop labels as no arcs
      // in the input FST are being traversed here.
      pop_labels_ = false;
      path_weights_.push_back(weight);
      arc_iterator_offsets_.push_back(-1);
      return;
    }
  }
}

template <class Arc>
void PathIterator<Arc>::SetError() {
  error_ = true;
}

template <class Arc>
void PathIterator<Arc>::VisitArc(const Arc &arc) {
  path_ilabels_.push_back(arc.ilabel);
  path_olabels_.push_back(arc.olabel);
  path_weights_.push_back(arc.weight);
}

template <class Arc>
void PathIterator<Arc>::MaybePopLabels() {
  if (pop_labels_) {
    path_ilabels_.pop_back();
    path_olabels_.pop_back();
  } else {
    pop_labels_ = true;
  }
}

// A useful alias when using StdArc.
using StdPathIterator = PathIterator<StdArc>;

// StringPathIterator is a wrapper for PathIterator that handles symbol tables
// and the conversion of the label sequences to strings.
//
// When check_acyclic is set, checks acyclicity of FST. An acyclic FST may
// lead to infinite loops and thus check_acyclic should only be false when the
// caller can ensure finite iteration (e.g., knowing the FST is acyclic or
// limiting the number of iterated paths).
template <class Arc>
class StringPathIterator : public PathIterator<Arc> {
 public:
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using ArcWeight = typename Arc::Weight;

  using PathIterator<Arc>::Done;
  using PathIterator<Arc>::Error;
  using PathIterator<Arc>::ILabels;
  using PathIterator<Arc>::Next;
  using PathIterator<Arc>::OLabels;
  using PathIterator<Arc>::Reset;
  using PathIterator<Arc>::SetError;

  explicit StringPathIterator(const Fst<Arc> &fst,
                              TokenType input_token_type = TokenType::BYTE,
                              TokenType output_token_type = TokenType::BYTE,
                              const SymbolTable *isymbols = nullptr,
                              const SymbolTable *osymbols = nullptr,
                              bool check_acyclic = true);

  // The same, but sets input and output token types/symbol tables to the same
  // argument.
  explicit StringPathIterator(const Fst<Arc> &fst, TokenType token_type,
                              const SymbolTable *symbols = nullptr,
                              bool check_acyclic = true)
      : StringPathIterator(fst, token_type, token_type, symbols, symbols,
                           check_acyclic) {}

  void IString(std::string *str);

  std::string IString();

  void OString(std::string *str);

  std::string OString();

 private:
  TokenType input_token_type_;
  TokenType output_token_type_;
  const SymbolTable *isymbols_;
  const SymbolTable *osymbols_;
};

// When check_acyclic is set, checks acyclicity of FST. An acyclic FST may
// lead to infinite loops and thus check_acyclic should only be false when the
// caller can ensure finite iteration (e.g., knowing the FST is acyclic or
// limiting the number of iterated paths).
template <class Arc>
StringPathIterator<Arc>::StringPathIterator(const Fst<Arc> &fst,
                                            TokenType input_token_type,
                                            TokenType output_token_type,
                                            const SymbolTable *isymbols,
                                            const SymbolTable *osymbols,
                                            bool check_acyclic)
    : PathIterator<Arc>(fst, check_acyclic),
      input_token_type_(input_token_type),
      output_token_type_(output_token_type),
      isymbols_(isymbols),
      osymbols_(osymbols) {}

template <class Arc>
void StringPathIterator<Arc>::IString(std::string *str) {
  if (!LabelsToString(ILabels(), str, input_token_type_, isymbols_)) SetError();
}

template <class Arc>
std::string StringPathIterator<Arc>::IString() {
  std::string result;
  IString(&result);
  return result;
}

template <class Arc>
void StringPathIterator<Arc>::OString(std::string *str) {
  if (!LabelsToString(OLabels(), str, output_token_type_, osymbols_)) {
    SetError();
  }
}

template <class Arc>
std::string StringPathIterator<Arc>::OString() {
  std::string result;
  OString(&result);
  return result;
}

// A useful alias when using StdArc.
using StdStringPathIterator = StringPathIterator<StdArc>;

}  // namespace fst

#endif  // PYNINI_PATHS_H_

