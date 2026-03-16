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


#ifndef PYNINI_PREFIX_TREE_H_
#define PYNINI_PREFIX_TREE_H_

#include <array>
#include <map>
#include <memory>
#include <stack>
#include <utility>

#include <fst/log.h>
#include <fst/arc.h>
#include <fst/vector-fst.h>
#include <map>
#include <fst/compat.h>
#include <fst/compat.h>

namespace fst {
namespace internal {

template <class Label, class StateId, class Node>
Node *LookupOrInsertChild(
    std::map<Label, std::unique_ptr<Node>> *children, Label label,
    StateId *num_states) {
  std::unique_ptr<Node> &value = (*children)[label];
  if (!value) value = std::make_unique<Node>((*num_states)++);
  return value.get();
}

template <class Arc>
class BaseONode {
 public:
  using StateId = typename Arc::StateId;
  using ArcWeight = typename Arc::Weight;

  explicit BaseONode(StateId state)
      : weight_(ArcWeight::Zero()), state_(state) {}

  const ArcWeight &Weight() const { return weight_; }

  StateId State() const { return state_; }

  void SetWeight(ArcWeight &&weight) { weight_ = weight; }

 protected:
  ArcWeight weight_;
  const StateId state_;
};

template <class StateId, class ONode>
class BaseINode {
 public:
  explicit BaseINode(StateId state) : onode_(nullptr), state_(state) {}

  const ONode *Output() const { return onode_.get(); }

  ONode *Output() { return onode_.get(); }

  StateId State() const { return state_; }

 protected:
  std::unique_ptr<ONode> onode_;
  const StateId state_;
};

// Prefix tree node for the output labels of the FST.
template <class A>
struct PrefixTreeTransducerPolicy {
  using Arc = A;
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using ArcWeight = typename Arc::Weight;

  class ONode : public BaseONode<Arc> {
   public:
    using ChildMap = std::map<Label, std::unique_ptr<ONode>>;

    explicit ONode(StateId state) : BaseONode<Arc>(state) {}

    const ChildMap &Children() const { return children_; }

    ONode *LookupOrInsertChild(Label label, StateId *num_states) {
      return internal::LookupOrInsertChild(&children_, label, num_states);
    }

   private:
    ChildMap children_;
  };

  class INode : public BaseINode<StateId, ONode> {
   public:
    using ChildMap = std::map<Label, std::unique_ptr<INode>>;

    explicit INode(StateId state): BaseINode<StateId, ONode>(state) {}

    const ChildMap &Children() const { return children_; }

    INode *LookupOrInsertChild(Label label, StateId *num_states) {
      return internal::LookupOrInsertChild(&children_, label, num_states);
    }

    void InsertONode(StateId *num_states) {
      using Base = BaseINode<StateId, ONode>;
      Base::onode_ = std::make_unique<ONode>((*num_states)++);
    }

   private:
    ChildMap children_;
  };

  static Arc MakeIArc(Label label, const INode *dest) {
    return Arc(label, 0, dest->State());
  }

  static Arc MakeOArc(Label label, const ONode *dest) {
    return Arc(0, label, dest->State());
  }

  static void InputOutputBridge(MutableFst<Arc> *fst, StateId start,
                                const ONode *onode) {
    fst->AddArc(start, Arc(0, 0, onode->State()));
  }

  static constexpr bool IsAcceptor() { return false; }
};

template <class A>
struct PrefixTreeAcceptorPolicy {
 public:
  using Arc = A;
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using ArcWeight = typename Arc::Weight;

  class ONode : public BaseONode<Arc> {
   public:
    explicit ONode(StateId state) : BaseONode<Arc>(state) {}

    // Returns a constexpr empty container.
    static constexpr std::array<std::pair<Label, std::unique_ptr<ONode>>, 0>
    Children() {
      return {};
    }

    static constexpr ONode *LookupOrInsertChild(Label, StateId *) {
      return nullptr;
    }
  };

  class INode : public BaseINode<StateId, ONode> {
   public:
    using ChildMap = std::map<Label, std::unique_ptr<INode>>;

    explicit INode(StateId state): BaseINode<StateId, ONode>(state) {}

    const ChildMap &Children() const { return children_; }

    INode *LookupOrInsertChild(Label label, StateId *num_states) {
      return internal::LookupOrInsertChild(&children_, label, num_states);
    }

    void InsertONode(StateId *unused_num_states) {
      using Base = BaseINode<StateId, ONode>;
      // Make the ONode copy the INode's StateId, and not increment external
      // `num_states`.
      Base::onode_ = std::make_unique<ONode>(Base::state_);
    }

   private:
    ChildMap children_;
  };

  static Arc MakeIArc(Label label, const INode *dest) {
    return Arc(label, label, dest->State());
  }

  static Arc MakeOArc(Label label, const ONode *dest) {
    return Arc(0, 0, dest->State());
  }

  static void InputOutputBridge(MutableFst<Arc> *, StateId, const ONode *) {}

  static constexpr bool IsAcceptor() { return true; }
};

// This class is neither thread-safe nor thread-hostile.
template <class Arc, class Policy>
class PrefixTree {
 public:
  using Label = typename Arc::Label;
  using StateId = typename Arc::StateId;
  using Weight = typename Arc::Weight;
  using ONode = typename Policy::ONode;
  using INode = typename Policy::INode;

  PrefixTree() : num_states_(0), root_(nullptr) {}

  StateId NumStates() const { return num_states_; }

  // Add an entry to the prefix tree, consisting of two label sequences and a
  // weight. Each label sequence must be provided as a pair of iterators.
  template <class Iterator1, class Iterator2, class T>
  void Add(Iterator1 it1, Iterator1 end1, Iterator2 it2, Iterator2 end2,
           T &&weight) {
    if (!root_) {
      CHECK_EQ(0, num_states_);
      root_ = std::make_unique<INode>(num_states_++);
    }
    INode *inode = root_.get();
    for (Label ilabel : fst::make_range(it1, end1)) {
      if (!ilabel) continue;  // Skips over epsilons.
      inode = inode->LookupOrInsertChild(ilabel, &num_states_);
    }
    if (!inode->Output()) inode->InsertONode(&num_states_);
    ONode *onode = inode->Output();
    if constexpr (!Policy::IsAcceptor()) {
      for (Label olabel : fst::make_range(it2, end2)) {
        if (!olabel) continue;  // Skips over epsilons.
        onode = onode->LookupOrInsertChild(olabel, &num_states_);
      }
    }
    onode->SetWeight(Plus(onode->Weight(), std::forward<T>(weight)));
  }

  // With semiring One as a default.
  template <class Iterator1, class Iterator2>
  void Add(Iterator1 it1, Iterator1 end1, Iterator2 it2, Iterator2 end2) {
    Add(it1, end1, it2, end2, Weight::One());
  }

  template <class Container1, class Container2, class T>
  void Add(const Container1 &cont1, const Container2 &cont2, T &&weight) {
    Add(cont1.begin(), cont1.end(), cont2.begin(), cont2.end(),
        std::forward<T>(weight));
  }

  // With semiring One as a default.
  template <class Container1, class Container2>
  void Add(const Container1 &cont1, const Container2 &cont2) {
    Add(cont1.begin(), cont1.end(), cont2.begin(), cont2.end(), Weight::One());
  }

  // Removes all elements from this prefix tree.
  void Clear() {
    num_states_ = 0;
    root_ = nullptr;
  }

  // Write the current prefix tree transducer to a mutable FST.
  void ToFst(MutableFst<Arc> *fst) const {
    fst->DeleteStates();
    if (num_states_ == 0) {
      CHECK(!root_);
      return;
    }
    // For the creation of the FST to be efficient, we reserve enough space
    // for the states and arcs to avoid reallocation and internal copying.
    fst->AddStates(num_states_);
    fst->SetStart(root_->State());
    std::stack<const INode *> iq;
    std::stack<const ONode *> oq;
    iq.push(root_.get());
    while (!iq.empty()) {
      const INode *inode = iq.top();
      iq.pop();
      const auto q = inode->State();
      CHECK_NE(kNoStateId, q);
      const ONode *onode = inode->Output();
      fst->ReserveArcs(q, (onode ? 1 : 0) + inode->Children().size());
      if (onode) {
        Policy::InputOutputBridge(fst, q, onode);
        oq.push(onode);
      }
      for (const auto &[label, child] : inode->Children()) {
        fst->AddArc(q, Policy::MakeIArc(label, child.get()));
        iq.push(child.get());
      }
    }
    while (!oq.empty()) {
      const ONode *onode = oq.top();
      oq.pop();
      const auto q = onode->State();
      CHECK_NE(kNoStateId, q);
      for (const auto &[label, child] : onode->Children()) {
        fst->AddArc(q, Policy::MakeOArc(label, child.get()));
        oq.push(child.get());
      }
      fst->SetFinal(q, onode->Weight());
    }
  }

 private:
  StateId num_states_;
  std::unique_ptr<INode> root_;

  PrefixTree(const PrefixTree &) = delete;
  PrefixTree &operator=(const PrefixTree &) = delete;
};

}  // namespace internal

template <class Arc>
using TransducerPrefixTree =
    internal::PrefixTree<Arc, internal::PrefixTreeTransducerPolicy<Arc>>;

// Note that during `Add`, an `AcceptorPrefixTree` only looks at the first of
// the two strings passed.
template <class Arc>
using AcceptorPrefixTree =
    internal::PrefixTree<Arc, internal::PrefixTreeAcceptorPolicy<Arc>>;

}  // namespace fst

#endif  // PYNINI_PREFIX_TREE_H_

