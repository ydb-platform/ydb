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
// Compresses and decompresses unweighted FSTs.

#ifndef FST_EXTENSIONS_COMPRESS_COMPRESS_H_
#define FST_EXTENSIONS_COMPRESS_COMPRESS_H_

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <ios>
#include <iostream>
#include <istream>
#include <map>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <vector>

#include <fst/compat.h>
#include <fst/log.h>
#include <fst/extensions/compress/elias.h>
#include <fst/encode.h>
#include <fstream>
#include <fst/fst.h>
#include <fst/mutable-fst.h>
#include <fst/queue.h>
#include <fst/statesort.h>
#include <fst/visit.h>

namespace fst {

// Identifies stream data as a vanilla compressed FST.
inline constexpr int32_t kCompressMagicNumber = 1858869554;

namespace internal {

// Expands a Lempel Ziv code and returns the set of code words where
// expanded_code[i] is the i^th Lempel Ziv codeword.
template <class Var, class Edge>
bool ExpandLZCode(const std::vector<std::pair<Var, Edge>> &code,
                  std::vector<std::vector<Edge>> *expanded_code) {
  expanded_code->resize(code.size());
  for (int i = 0; i < code.size(); ++i) {
    if (code[i].first > i) {
      LOG(ERROR) << "ExpandLZCode: Not a valid code";
      return false;
    }
    auto &codeword = (*expanded_code)[i];
    if (code[i].first == 0) {
      codeword.resize(1, code[i].second);
    } else {
      const auto &other_codeword = (*expanded_code)[code[i].first - 1];
      codeword.resize(other_codeword.size() + 1);
      std::copy(other_codeword.cbegin(), other_codeword.cend(),
                codeword.begin());
      codeword[other_codeword.size()] = code[i].second;
    }
  }
  return true;
}

}  // namespace internal

// Lempel Ziv on data structure Edge, with a less-than operator EdgeLessThan and
// an equals operator EdgeEquals.
template <class Var, class Edge, class EdgeLessThan, class EdgeEquals>
class LempelZiv {
 public:
  LempelZiv() : dict_number_(0), default_edge_() {
    root_.current_number = dict_number_++;
    root_.current_edge = default_edge_;
    decode_vector_.emplace_back(0, default_edge_);
  }

  // Encodes a vector input into output.
  void BatchEncode(const std::vector<Edge> &input,
                   std::vector<std::pair<Var, Edge>> *output);

  // Decodes codedvector to output, returning false if the index exceeds the
  // size.
  bool BatchDecode(const std::vector<std::pair<Var, Edge>> &input,
                   std::vector<Edge> *output);

  // Decodes a single dictionary element, returning false if the index exceeds
  // the size.
  bool SingleDecode(const Var &index, Edge *output) {
    if (index >= decode_vector_.size()) {
      LOG(ERROR) << "LempelZiv::SingleDecode: "
                 << "Index exceeded the dictionary size";
      return false;
    } else {
      *output = decode_vector_[index].second;
      return true;
    }
  }

 private:
  struct Node {
    Var current_number;
    Edge current_edge;
    std::map<Edge, std::unique_ptr<Node>, EdgeLessThan> next_number;
  };

  Node root_;
  Var dict_number_;
  std::vector<std::pair<Var, Edge>> decode_vector_;
  Edge default_edge_;
};

template <class Var, class Edge, class EdgeLessThan, class EdgeEquals>
void LempelZiv<Var, Edge, EdgeLessThan, EdgeEquals>::BatchEncode(
    const std::vector<Edge> &input, std::vector<std::pair<Var, Edge>> *output) {
  for (auto it = input.cbegin(); it != input.cend(); ++it) {
    auto *temp_node = &root_;
    while (it != input.cend()) {
      auto next = temp_node->next_number.find(*it);
      if (next != temp_node->next_number.cend()) {
        temp_node = next->second.get();
        ++it;
      } else {
        break;
      }
    }
    if (it == input.cend() && temp_node->current_number != 0) {
      output->emplace_back(temp_node->current_number, default_edge_);
    } else if (it != input.cend()) {
      output->emplace_back(temp_node->current_number, *it);
      auto new_node = std::make_unique<Node>();
      new_node->current_number = dict_number_++;
      new_node->current_edge = *it;
      temp_node->next_number[*it] = std::move(new_node);
    }
    if (it == input.cend()) break;
  }
}

template <class Var, class Edge, class EdgeLessThan, class EdgeEquals>
bool LempelZiv<Var, Edge, EdgeLessThan, EdgeEquals>::BatchDecode(
    const std::vector<std::pair<Var, Edge>> &input, std::vector<Edge> *output) {
  for (const auto &[var, edge] : input) {
    std::vector<Edge> temp_output;
    EdgeEquals InstEdgeEquals;
    if (InstEdgeEquals(edge, default_edge_) != 1) {
      decode_vector_.emplace_back(var, edge);
      temp_output.push_back(edge);
    }
    auto temp_integer = var;
    if (temp_integer >= decode_vector_.size()) {
      LOG(ERROR) << "LempelZiv::BatchDecode: "
                 << "Index exceeded the dictionary size";
      return false;
    } else {
      while (temp_integer != 0) {
        temp_output.push_back(decode_vector_[temp_integer].second);
        temp_integer = decode_vector_[temp_integer].first;
      }
      output->insert(output->cend(), temp_output.rbegin(), temp_output.rend());
    }
  }
  return true;
}

template <class Arc>
class Compressor {
 public:
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;

  Compressor() = default;

  // Compresses an FST into a boolean vector code, returning true on success.
  bool Compress(const Fst<Arc> &fst, std::ostream &strm);

  // Decompresses the boolean vector into an FST, returning true on success.
  bool Decompress(std::istream &strm, const std::string &source,
                  MutableFst<Arc> *fst);

  // Computes the BFS order of a FST.
  void BfsOrder(const ExpandedFst<Arc> &fst, std::vector<StateId> *order);

  // Preprocessing step to convert an FST to a isomorphic FST.
  void Preprocess(const Fst<Arc> &fst, MutableFst<Arc> *preprocessedfst,
                  EncodeMapper<Arc> *encoder);

  // Performs Lempel Ziv and outputs a stream of integers.
  void EncodeProcessedFst(const ExpandedFst<Arc> &fst, std::ostream &strm);

  // Decodes FST from the stream.
  void DecodeProcessedFst(const std::vector<StateId> &input,
                          MutableFst<Arc> *fst, bool unweighted);

  // Writes the boolean file to the stream.
  void WriteToStream(std::ostream &strm);

  // Writes the weights to the stream.
  void WriteWeight(const std::vector<Weight> &input, std::ostream &strm);

  void ReadWeight(std::istream &strm, std::vector<Weight> *output);

  // Same as fst::Decode, but doesn't remove the final epsilons.
  void DecodeForCompress(MutableFst<Arc> *fst, const EncodeMapper<Arc> &mapper);

  // Updates buffer_code_.
  template <class CVar>
  void WriteToBuffer(CVar input) {
    std::vector<bool> current_code;
    Elias<CVar>::DeltaEncode(input, &current_code);
    buffer_code_.insert(buffer_code_.cend(), current_code.begin(),
                        current_code.end());
  }

 private:
  struct LZLabel {
    LZLabel() : label(0) {}
    Label label;
  };

  struct LabelLessThan {
    bool operator()(const LZLabel &labelone, const LZLabel &labeltwo) const {
      return labelone.label < labeltwo.label;
    }
  };

  struct LabelEquals {
    bool operator()(const LZLabel &labelone, const LZLabel &labeltwo) const {
      return labelone.label == labeltwo.label;
    }
  };

  struct Transition {
    Transition() : nextstate(0), label(0), weight(Weight::Zero()) {}

    StateId nextstate;
    Label label;
    Weight weight;
  };

  struct TransitionLessThan {
    bool operator()(const Transition &transition_one,
                    const Transition &transition_two) const {
      if (transition_one.nextstate == transition_two.nextstate) {
        return transition_one.label < transition_two.label;
      } else {
        return transition_one.nextstate < transition_two.nextstate;
      }
    }
  };

  struct TransitionEquals {
    bool operator()(const Transition &transition_one,
                    const Transition &transition_two) const {
      return transition_one.nextstate == transition_two.nextstate &&
             transition_one.label == transition_two.label;
    }
  };

  struct OldDictCompare {
    bool operator()(const std::pair<StateId, Transition> &pair_one,
                    const std::pair<StateId, Transition> &pair_two) const {
      if (pair_one.second.nextstate == pair_two.second.nextstate) {
        return pair_one.second.label < pair_two.second.label;
      } else {
        return pair_one.second.nextstate < pair_two.second.nextstate;
      }
    }
  };

  std::vector<bool> buffer_code_;
  std::vector<Weight> arc_weight_;
  std::vector<Weight> final_weight_;
};

template <class Arc>
void Compressor<Arc>::DecodeForCompress(MutableFst<Arc> *fst,
                                        const EncodeMapper<Arc> &mapper) {
  ArcMap(fst, EncodeMapper<Arc>(mapper, DECODE));
  fst->SetInputSymbols(mapper.InputSymbols());
  fst->SetOutputSymbols(mapper.OutputSymbols());
}

template <class Arc>
void Compressor<Arc>::BfsOrder(const ExpandedFst<Arc> &fst,
                               std::vector<StateId> *order) {
  class BfsVisitor {
   public:
    // Requires order->size() >= fst.NumStates().
    explicit BfsVisitor(std::vector<StateId> *order) : order_(order) {}

    void InitVisit(const Fst<Arc> &fst) {}

    bool InitState(StateId s, StateId) {
      order_->at(s) = num_bfs_states_++;
      return true;
    }

    bool WhiteArc(StateId s, const Arc &arc) { return true; }
    bool GreyArc(StateId s, const Arc &arc) { return true; }
    bool BlackArc(StateId s, const Arc &arc) { return true; }
    void FinishState(StateId s) {}
    void FinishVisit() {}

   private:
    std::vector<StateId> *order_ = nullptr;
    StateId num_bfs_states_ = 0;
  };

  order->assign(fst.NumStates(), kNoStateId);
  BfsVisitor visitor(order);
  FifoQueue<StateId> queue;
  Visit(fst, &visitor, &queue, AnyArcFilter<Arc>());
}

template <class Arc>
void Compressor<Arc>::Preprocess(const Fst<Arc> &fst,
                                 MutableFst<Arc> *preprocessedfst,
                                 EncodeMapper<Arc> *encoder) {
  *preprocessedfst = fst;
  if (!preprocessedfst->NumStates()) return;
  // Relabels the edges and develops a dictionary.
  Encode(preprocessedfst, encoder);
  std::vector<StateId> order;
  // Finds the BFS sorting order of the FST.
  BfsOrder(*preprocessedfst, &order);
  // Reorders the states according to the BFS order.
  StateSort(preprocessedfst, order);
}

template <class Arc>
void Compressor<Arc>::EncodeProcessedFst(const ExpandedFst<Arc> &fst,
                                         std::ostream &strm) {
  std::vector<StateId> output;
  LempelZiv<StateId, LZLabel, LabelLessThan, LabelEquals> dict_new;
  LempelZiv<StateId, Transition, TransitionLessThan, TransitionEquals> dict_old;
  std::vector<LZLabel> current_new_input;
  std::vector<Transition> current_old_input;
  std::vector<std::pair<StateId, LZLabel>> current_new_output;
  std::vector<std::pair<StateId, Transition>> current_old_output;
  std::vector<StateId> final_states;
  const auto number_of_states = fst.NumStates();
  StateId seen_states = 0;
  // Adds the number of states.
  WriteToBuffer<StateId>(number_of_states);
  for (StateId state = 0; state < number_of_states; ++state) {
    current_new_input.clear();
    current_old_input.clear();
    current_new_output.clear();
    current_old_output.clear();
    if (state > seen_states) ++seen_states;
    // Collects the final states.
    if (fst.Final(state) != Weight::Zero()) {
      final_states.push_back(state);
      final_weight_.push_back(fst.Final(state));
    }
    // Reads the states.
    for (ArcIterator<Fst<Arc>> aiter(fst, state); !aiter.Done(); aiter.Next()) {
      const auto &arc = aiter.Value();
      if (arc.nextstate > seen_states) {  // RILEY: > or >= ?
        ++seen_states;
        LZLabel temp_label;
        temp_label.label = arc.ilabel;
        arc_weight_.push_back(arc.weight);
        current_new_input.push_back(temp_label);
      } else {
        Transition temp_transition;
        temp_transition.nextstate = arc.nextstate;
        temp_transition.label = arc.ilabel;
        temp_transition.weight = arc.weight;
        current_old_input.push_back(temp_transition);
      }
    }
    // Adds new states.
    dict_new.BatchEncode(current_new_input, &current_new_output);
    WriteToBuffer<StateId>(current_new_output.size());
    for (auto it = current_new_output.cbegin(); it != current_new_output.cend();
         ++it) {
      WriteToBuffer<StateId>(it->first);
      WriteToBuffer<Label>((it->second).label);
    }
    // Adds old states by sorting and using difference coding.
    static const TransitionLessThan transition_less_than;
    std::sort(current_old_input.begin(), current_old_input.end(),
              transition_less_than);
    for (auto it = current_old_input.begin(); it != current_old_input.end();
         ++it) {
      arc_weight_.push_back(it->weight);
    }
    dict_old.BatchEncode(current_old_input, &current_old_output);
    std::vector<StateId> dict_old_temp;
    std::vector<Transition> transition_old_temp;
    for (auto it = current_old_output.begin(); it != current_old_output.end();
         ++it) {
      dict_old_temp.push_back(it->first);
      transition_old_temp.push_back(it->second);
    }
    if (!transition_old_temp.empty()) {
      if ((transition_old_temp.back()).nextstate == 0 &&
          (transition_old_temp.back()).label == 0) {
        transition_old_temp.pop_back();
      }
    }
    std::sort(dict_old_temp.begin(), dict_old_temp.end());
    std::sort(transition_old_temp.begin(), transition_old_temp.end(),
              transition_less_than);
    WriteToBuffer<StateId>(dict_old_temp.size());
    if (dict_old_temp.size() == transition_old_temp.size()) {
      WriteToBuffer<int>(0);
    } else {
      WriteToBuffer<int>(1);
    }
    StateId previous;
    if (!dict_old_temp.empty()) {
      WriteToBuffer<StateId>(dict_old_temp.front());
      previous = dict_old_temp.front();
    }
    if (dict_old_temp.size() > 1) {
      for (auto it = dict_old_temp.begin() + 1; it != dict_old_temp.end();
           ++it) {
        WriteToBuffer<StateId>(*it - previous);
        previous = *it;
      }
    }
    if (!transition_old_temp.empty()) {
      WriteToBuffer<StateId>((transition_old_temp.front()).nextstate);
      previous = transition_old_temp.front().nextstate;
      WriteToBuffer<Label>(transition_old_temp.front().label);
    }
    if (transition_old_temp.size() > 1) {
      for (auto it = transition_old_temp.begin() + 1;
           it != transition_old_temp.end(); ++it) {
        WriteToBuffer<StateId>(it->nextstate - previous);
        previous = it->nextstate;
        WriteToBuffer<StateId>(it->label);
      }
    }
  }
  // Adds final states.
  WriteToBuffer<StateId>(final_states.size());
  if (!final_states.empty()) {
    for (auto it = final_states.begin(); it != final_states.end(); ++it) {
      WriteToBuffer<StateId>(*it);
    }
  }
  WriteToStream(strm);
  const uint8_t unweighted = fst.Properties(kUnweighted, true) == kUnweighted;
  WriteType(strm, unweighted);
  if (unweighted == 0) {
    WriteWeight(arc_weight_, strm);
    WriteWeight(final_weight_, strm);
  }
}

template <class Arc>
void Compressor<Arc>::DecodeProcessedFst(const std::vector<StateId> &input,
                                         MutableFst<Arc> *fst,
                                         bool unweighted) {
  LempelZiv<StateId, LZLabel, LabelLessThan, LabelEquals> dict_new;
  LempelZiv<StateId, Transition, TransitionLessThan, TransitionEquals> dict_old;
  std::vector<std::pair<StateId, LZLabel>> current_new_input;
  std::vector<std::pair<StateId, Transition>> current_old_input;
  std::vector<LZLabel> current_new_output;
  std::vector<Transition> current_old_output;
  std::vector<std::pair<StateId, Transition>> actual_old_dict_numbers;
  std::vector<Transition> actual_old_dict_transitions;
  auto arc_weight_it = arc_weight_.begin();
  Transition default_transition;
  StateId seen_states = 1;
  // Adds states..
  const StateId num_states = input.front();
  if (num_states > 0) {
    const StateId start_state = fst->AddState();
    fst->SetStart(start_state);
    for (StateId state = 1; state < num_states; ++state) {
      fst->AddState();
    }
  }
  auto main_it = input.cbegin();
  ++main_it;
  for (StateId current_state = 0; current_state < num_states; ++current_state) {
    if (current_state >= seen_states) ++seen_states;
    current_new_input.clear();
    current_new_output.clear();
    current_old_input.clear();
    current_old_output.clear();
    // New states.
    StateId current_number_new_elements = *main_it;
    ++main_it;
    for (StateId new_integer = 0; new_integer < current_number_new_elements;
         ++new_integer) {
      std::pair<StateId, LZLabel> temp_new_dict_element;
      temp_new_dict_element.first = *main_it;
      ++main_it;
      LZLabel temp_label;
      temp_label.label = *main_it;
      ++main_it;
      temp_new_dict_element.second = temp_label;
      current_new_input.push_back(temp_new_dict_element);
    }
    dict_new.BatchDecode(current_new_input, &current_new_output);
    for (const auto &label : current_new_output) {
      if (!unweighted) {
        fst->AddArc(current_state,
                    Arc(label.label, label.label, *arc_weight_it, seen_states));
        ++arc_weight_it;
      } else {
        fst->AddArc(current_state,
                    Arc(label.label, label.label, Weight::One(), seen_states));
      }
      ++seen_states;
    }
    StateId current_number_old_elements = *main_it;
    ++main_it;
    StateId is_zero_removed = *main_it;
    ++main_it;
    StateId previous = 0;
    actual_old_dict_numbers.clear();
    for (StateId new_integer = 0; new_integer < current_number_old_elements;
         ++new_integer) {
      std::pair<StateId, Transition> pair_temp_transition;
      if (new_integer == 0) {
        pair_temp_transition.first = *main_it;
        previous = *main_it;
      } else {
        pair_temp_transition.first = *main_it + previous;
        previous = pair_temp_transition.first;
      }
      ++main_it;
      Transition temp_test;
      if (!dict_old.SingleDecode(pair_temp_transition.first, &temp_test)) {
        FSTERROR() << "Compressor::Decode: failed";
        fst->SetProperties(kError, kError);
        return;
      }
      pair_temp_transition.second = temp_test;
      actual_old_dict_numbers.push_back(pair_temp_transition);
    }
    // Reorders the dictionary elements.
    static const OldDictCompare old_dict_compare;
    std::sort(actual_old_dict_numbers.begin(), actual_old_dict_numbers.end(),
              old_dict_compare);
    // Transitions.
    previous = 0;
    actual_old_dict_transitions.clear();
    for (StateId new_integer = 0;
         new_integer < current_number_old_elements - is_zero_removed;
         ++new_integer) {
      Transition temp_transition;
      if (new_integer == 0) {
        temp_transition.nextstate = *main_it;
        previous = *main_it;
      } else {
        temp_transition.nextstate = *main_it + previous;
        previous = temp_transition.nextstate;
      }
      ++main_it;
      temp_transition.label = *main_it;
      ++main_it;
      actual_old_dict_transitions.push_back(temp_transition);
    }
    if (is_zero_removed == 1) {
      actual_old_dict_transitions.push_back(default_transition);
    }
    auto trans_it = actual_old_dict_transitions.cbegin();
    auto dict_it = actual_old_dict_numbers.cbegin();
    while (trans_it != actual_old_dict_transitions.cend() &&
           dict_it != actual_old_dict_numbers.cend()) {
      if (dict_it->first == 0) {
        ++dict_it;
      } else {
        std::pair<StateId, Transition> temp_pair;
        static const TransitionEquals transition_equals;
        static const TransitionLessThan transition_less_than;
        if (transition_equals(*trans_it, default_transition)) {
          temp_pair.first = dict_it->first;
          temp_pair.second = default_transition;
          ++dict_it;
        } else if (transition_less_than(dict_it->second, *trans_it)) {
          temp_pair.first = dict_it->first;
          temp_pair.second = *trans_it;
          ++dict_it;
        } else {
          temp_pair.first = 0;
          temp_pair.second = *trans_it;
        }
        ++trans_it;
        current_old_input.push_back(temp_pair);
      }
    }
    while (trans_it != actual_old_dict_transitions.cend()) {
      std::pair<StateId, Transition> temp_pair;
      temp_pair.first = 0;
      temp_pair.second = *trans_it;
      ++trans_it;
      current_old_input.push_back(temp_pair);
    }
    // Adds old elements in the dictionary.
    if (!dict_old.BatchDecode(current_old_input, &current_old_output)) {
      FSTERROR() << "Compressor::Decode: Failed";
      fst->SetProperties(kError, kError);
      return;
    }
    for (auto it = current_old_output.cbegin(); it != current_old_output.cend();
         ++it) {
      if (!unweighted) {
        fst->AddArc(current_state,
                    Arc(it->label, it->label, *arc_weight_it, it->nextstate));
        ++arc_weight_it;
      } else {
        fst->AddArc(current_state,
                    Arc(it->label, it->label, Weight::One(), it->nextstate));
      }
    }
  }
  // Adds the final states.
  StateId number_of_final_states = *main_it;
  if (number_of_final_states > 0) {
    ++main_it;
    for (StateId temp_int = 0; temp_int < number_of_final_states; ++temp_int) {
      if (unweighted) {
        fst->SetFinal(*main_it, Weight(0));
      } else {
        fst->SetFinal(*main_it, final_weight_[temp_int]);
      }
      ++main_it;
    }
  }
}

template <class Arc>
void Compressor<Arc>::ReadWeight(std::istream &strm,
                                 std::vector<Weight> *output) {
  int64_t size;
  Weight weight;
  ReadType(strm, &size);
  for (int64_t i = 0; i < size; ++i) {
    weight.Read(strm);
    output->push_back(weight);
  }
}

template <class Arc>
bool Compressor<Arc>::Decompress(std::istream &strm, const std::string &source,
                                 MutableFst<Arc> *fst) {
  fst->DeleteStates();
  int32_t magic_number = 0;
  ReadType(strm, &magic_number);
  if (magic_number != kCompressMagicNumber) {
    LOG(ERROR) << "Decompress: Bad compressed Fst: " << source;
    return false;
  }
  std::unique_ptr<EncodeMapper<Arc>> encoder(
      EncodeMapper<Arc>::Read(strm, "Decoding", DECODE));
  std::vector<bool> bool_code;
  uint8_t block;
  uint8_t msb = 128;
  int64_t data_size;
  ReadType(strm, &data_size);
  for (int64_t i = 0; i < data_size; ++i) {
    ReadType(strm, &block);
    for (int j = 0; j < 8; ++j) {
      uint8_t temp = msb & block;
      bool_code.push_back(temp == 128);
      block = block << 1;
    }
  }
  std::vector<StateId> int_code;
  Elias<StateId>::BatchDecode(bool_code, &int_code);
  bool_code.clear();
  uint8_t unweighted;
  ReadType(strm, &unweighted);
  if (unweighted == 0) {
    ReadWeight(strm, &arc_weight_);
    ReadWeight(strm, &final_weight_);
  }
  DecodeProcessedFst(int_code, fst, unweighted);
  DecodeForCompress(fst, *encoder);
  return !fst->Properties(kError, false);
}

template <class Arc>
void Compressor<Arc>::WriteWeight(const std::vector<Weight> &input,
                                  std::ostream &strm) {
  int64_t size = input.size();
  WriteType(strm, size);
  for (auto it = input.begin(); it != input.end(); ++it) {
    it->Write(strm);
  }
}

template <class Arc>
void Compressor<Arc>::WriteToStream(std::ostream &strm) {
  while (buffer_code_.size() % 8 != 0) buffer_code_.push_back(true);
  int64_t data_size = buffer_code_.size() / 8;
  WriteType(strm, data_size);
  int64_t i = 0;
  uint8_t block;
  for (auto it = buffer_code_.begin(); it != buffer_code_.end(); ++it) {
    if (i % 8 == 0) {
      if (i > 0) WriteType(strm, block);
      block = 0;
    } else {
      block = block << 1;
    }
    block |= *it;
    ++i;
  }
  WriteType(strm, block);
}

template <class Arc>
bool Compressor<Arc>::Compress(const Fst<Arc> &fst, std::ostream &strm) {
  VectorFst<Arc> processedfst;
  EncodeMapper<Arc> encoder(kEncodeLabels, ENCODE);
  Preprocess(fst, &processedfst, &encoder);
  WriteType(strm, kCompressMagicNumber);
  encoder.Write(strm, "ostream");
  EncodeProcessedFst(processedfst, strm);
  return true;
}

// Convenience functions that call the compressor and decompressor.

template <class Arc>
void Compress(const Fst<Arc> &fst, std::ostream &strm) {
  Compressor<Arc> comp;
  comp.Compress(fst, strm);
}

template <class Arc>
bool Compress(const Fst<Arc> &fst, const std::string &source) {
  std::ofstream fstrm;
  if (!source.empty()) {
    fstrm.open(source, std::ios_base::out | std::ios_base::binary);
    if (!fstrm) {
      LOG(ERROR) << "Compress: Can't open file: " << source;
      return false;
    }
  }
  std::ostream &ostrm = fstrm.is_open() ? fstrm : std::cout;
  Compress(fst, ostrm);
  return !!ostrm;
}

template <class Arc>
bool Decompress(std::istream &strm, const std::string &source,
                MutableFst<Arc> *fst) {
  Compressor<Arc> comp;
  comp.Decompress(strm, source, fst);
  return true;
}

// Returns true on success.
template <class Arc>
bool Decompress(const std::string &source, MutableFst<Arc> *fst) {
  std::ifstream fstrm;
  if (!source.empty()) {
    fstrm.open(source, std::ios_base::in | std::ios_base::binary);
    if (!fstrm) {
      LOG(ERROR) << "Decompress: Can't open file: " << source;
      return false;
    }
  }
  std::istream &istrm = fstrm.is_open() ? fstrm : std::cin;
  Decompress(istrm, source.empty() ? "standard input" : source, fst);
  return !!istrm;
}

}  // namespace fst

#endif  // FST_EXTENSIONS_COMPRESS_COMPRESS_H_
