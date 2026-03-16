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
// Common classes for Multi Pushdown Transducer (MPDT) expansion/traversal.

#ifndef FST_EXTENSIONS_MPDT_MPDT_H_
#define FST_EXTENSIONS_MPDT_MPDT_H_

#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <vector>

#include <fst/compat.h>
#include <fst/extensions/pdt/pdt.h>
#include <unordered_map>
#include <optional>

namespace fst {

enum class MPdtType : uint8_t {
  READ_RESTRICT,   // Can only read from first empty stack
  WRITE_RESTRICT,  // Can only write to first empty stack
  NO_RESTRICT,     // No read-write restrictions
};

namespace internal {

template <typename StackId, typename Level, Level nlevels>
using StackConfig = std::array<StackId, nlevels>;

// Forward declaration so `KeyPair` can declare `KeyPairHasher` its friend.
template <typename Level>
struct KeyPairHasher;

// Defines the KeyPair type used as the key to MPdtStack.paren_id_map_. The hash
// function is provided as a separate struct to match templating syntax.
template <typename Level>
class KeyPair {
 public:
  KeyPair(Level level, size_t id) : level_(level), underlying_id_(id) {}

  inline bool operator==(const KeyPair &rhs) const {
    return level_ == rhs.level_ && underlying_id_ == rhs.underlying_id_;
  }

 private:
  friend KeyPairHasher<Level>;
  Level level_;
  size_t underlying_id_;
};

template <typename Level>
struct KeyPairHasher {
  inline size_t operator()(const KeyPair<Level> &keypair) const {
    return std::hash<Level>()(keypair.level_) ^
           (std::hash<size_t>()(keypair.underlying_id_) << 1);
  }
};

template <typename StackId, typename Level, Level nlevels = 2,
          MPdtType restrict = MPdtType::READ_RESTRICT>
class MPdtStack {
 public:
  using Label = Level;
  using Config = StackConfig<StackId, Level, nlevels>;
  using ConfigToStackId = std::map<Config, StackId>;

  MPdtStack(const std::vector<std::pair<Label, Label>> &parens,
            const std::vector<Level> &assignments);

  ~MPdtStack() = default;

  MPdtStack(const MPdtStack &other)
      : error_(other.error_),
        min_paren_(other.min_paren_),
        max_paren_(other.max_paren_),
        paren_levels_(other.paren_levels_),
        parens_(other.parens_),
        paren_map_(other.paren_map_),
        paren_id_map_(other.paren_id_map_),
        config_to_stack_id_map_(other.config_to_stack_id_map_),
        stack_id_to_config_map_(other.stack_id_to_config_map_),
        next_stack_id_(other.next_stack_id_),
        stacks_(other.stacks_) {}

  MPdtStack &operator=(const MPdtStack &other) {
    *this = MPdtStack(other);
    return *this;
  }

  MPdtStack(MPdtStack &&) = default;

  MPdtStack &operator=(MPdtStack &&) = default;

  StackId Find(StackId stack_id, Label label);

  // For now we do not implement Pop since this is needed only for
  // ShortestPath().

  // For Top we find the first non-empty config, and find the paren ID of that
  // (or -1) if there is none, then map that to the external stack_id to return.
  ssize_t Top(StackId stack_id) const {
    if (stack_id == -1) return -1;
    const auto config = InternalStackIds(stack_id);
    Level level = 0;
    StackId underlying_id = -1;
    for (; level < nlevels; ++level) {
      if (!Empty(config, level)) {
        underlying_id = stacks_[level]->Top(config[level]);
        break;
      }
    }
    if (underlying_id == -1) return -1;
    const auto it = paren_id_map_.find(KeyPair<Level>(level, underlying_id));
    if (it == paren_id_map_.end()) return -1;  // NB: shouldn't happen.
    return it->second;
  }

  ssize_t ParenId(Label label) const {
    const auto it = paren_map_.find(label);
    return it != paren_map_.end() ? it->second : -1;
  }

  bool Error() { return error_; }

  // Each component stack has an internal stack ID for a given configuration and
  // label.
  // This function maps a configuration of those to the stack ID the caller
  // sees.
  inline StackId ExternalStackId(const Config &config) {
    const auto it = config_to_stack_id_map_.find(config);
    StackId result;
    if (it == config_to_stack_id_map_.end()) {
      result = next_stack_id_++;
      config_to_stack_id_map_.insert(
          std::pair<Config, StackId>(config, result));
      stack_id_to_config_map_[result] = config;
    } else {
      result = it->second;
    }
    return result;
  }

  // Retrieves the internal stack ID for a corresponding external stack ID.
  inline const Config InternalStackIds(StackId stack_id) const {
    auto it = stack_id_to_config_map_.find(stack_id);
    if (it == stack_id_to_config_map_.end()) {
      it = stack_id_to_config_map_.find(-1);
    }
    return it->second;
  }

  inline bool Empty(const Config &config, Level level) const {
    return config[level] <= 0;
  }

  inline bool AllEmpty(const Config &config) {
    for (Level level = 0; level < nlevels; ++level) {
      if (!Empty(config, level)) return false;
    }
    return true;
  }

 private:
  bool error_;
  Label min_paren_;
  Label max_paren_;
  // Stores level of each paren.
  std::unordered_map<Label, Label> paren_levels_;
  std::vector<std::pair<Label, Label>> parens_;   // As in pdt.h.
  std::unordered_map<Label, size_t> paren_map_;  // As in pdt.h.
  // Maps between internal paren_id and external paren_id.
  std::unordered_map<KeyPair<Level>, size_t, KeyPairHasher<Level>>
      paren_id_map_;
  // Maps between internal stack ids and external stack id.
  ConfigToStackId config_to_stack_id_map_;
  std::unordered_map<StackId, Config> stack_id_to_config_map_;
  StackId next_stack_id_;
  // Array of stacks.
  std::array<std::optional<PdtStack<StackId, Label>>, nlevels> stacks_;
};

template <typename StackId, typename Level, Level nlevels, MPdtType restrict>
MPdtStack<StackId, Level, nlevels, restrict>::MPdtStack(
    const std::vector<std::pair<Level, Level>> &parens,  // NB: Label = Level.
    const std::vector<Level> &assignments)
    : error_(false),
      min_paren_(kNoLabel),
      max_paren_(kNoLabel),
      parens_(parens),
      next_stack_id_(1) {
  using Label = Level;
  if (parens.size() != assignments.size()) {
    FSTERROR() << "MPdtStack: Parens of different size from assignments";
    error_ = true;
    return;
  }
  std::array<std::vector<std::pair<Label, Label>>, nlevels> vectors;
  for (Level i = 0; i < assignments.size(); ++i) {
    // Assignments here start at 0, so assuming the human-readable version has
    // them starting at 1, we should subtract 1 here
    const auto level = assignments[i] - 1;
    if (level < 0 || level >= nlevels) {
      FSTERROR() << "MPdtStack: Specified level " << level << " out of bounds";
      error_ = true;
      return;
    }
    const auto &pair = parens[i];
    vectors[level].push_back(pair);
    paren_levels_[pair.first] = level;
    paren_levels_[pair.second] = level;
    paren_map_[pair.first] = i;
    paren_map_[pair.second] = i;
    const KeyPair<Level> key(level, vectors[level].size() - 1);
    paren_id_map_[key] = i;
    if (min_paren_ == kNoLabel || pair.first < min_paren_) {
      min_paren_ = pair.first;
    }
    if (pair.second < min_paren_) min_paren_ = pair.second;
    if (max_paren_ == kNoLabel || pair.first > max_paren_) {
      max_paren_ = pair.first;
    }
    if (pair.second > max_paren_) max_paren_ = pair.second;
  }
  using Config = StackConfig<StackId, Level, nlevels>;
  Config neg_one;
  Config zero;
  for (Level level = 0; level < nlevels; ++level) {
    stacks_[level].emplace(vectors[level]);
    neg_one[level] = -1;
    zero[level] = 0;
  }
  config_to_stack_id_map_[neg_one] = -1;
  config_to_stack_id_map_[zero] = 0;
  stack_id_to_config_map_[-1] = neg_one;
  stack_id_to_config_map_[0] = zero;
}

template <typename StackId, typename Level, Level nlevels, MPdtType restrict>
StackId MPdtStack<StackId, Level, nlevels, restrict>::Find(StackId stack_id,
                                                           Level label) {
  // Non-paren.
  if (min_paren_ == kNoLabel || label < min_paren_ || label > max_paren_) {
    return stack_id;
  }
  const auto it = paren_map_.find(label);
  // Non-paren.
  if (it == paren_map_.end()) return stack_id;
  ssize_t paren_id = it->second;
  // Gets the configuration associated with this stack_id.
  const auto config = InternalStackIds(stack_id);
  // Gets the level.
  const auto level = paren_levels_.find(label)->second;
  // If the label is an open paren we push:
  //
  // 1) if the restrict type is not MPdtType::WRITE_RESTRICT, or
  // 2) the restrict type is MPdtType::WRITE_RESTRICT, and all the stacks above
  // the level are empty.
  if (label == parens_[paren_id].first) {  // Open paren.
    if (restrict == MPdtType::WRITE_RESTRICT) {
      for (Level upper_level = 0; upper_level < level; ++upper_level) {
        if (!Empty(config, upper_level)) return -1;  // Non-empty stack blocks.
      }
    }
    // If the label is an close paren we pop:
    //
    // 1) if the restrict type is not MPdtType::READ_RESTRICT, or
    // 2) the restrict type is MPdtType::READ_RESTRICT, and all the stacks above
    // the level are empty.
  } else if (restrict == MPdtType::READ_RESTRICT) {
    for (Level lower_level = 0; lower_level < level; ++lower_level) {
      if (!Empty(config, lower_level)) return -1;  // Non-empty stack blocks.
    }
  }
  const auto nid = stacks_[level]->Find(config[level], label);
  // If the new ID is -1, that means that there is no valid transition at the
  // level we want.
  if (nid == -1) {
    return -1;
  } else {
    using Config = StackConfig<StackId, Level, nlevels>;
    Config nconfig(config);
    nconfig[level] = nid;
    return ExternalStackId(nconfig);
  }
}

}  // namespace internal
}  // namespace fst

#endif  // FST_EXTENSIONS_MPDT_MPDT_H_
