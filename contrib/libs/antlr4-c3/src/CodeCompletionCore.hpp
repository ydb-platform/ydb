//
//  CodeCompletionCore.hpp
//
//  C++ port of antlr4-c3 (TypeScript) by Mike Lischke
//  Licensed under the MIT License.
//

#pragma once

#include <Parser.h>
#include <ParserRuleContext.h>
#include <Token.h>
#include <Vocabulary.h>
#include <atn/ATNState.h>
#include <atn/PredicateTransition.h>
#include <atn/RuleStartState.h>
#include <atn/Transition.h>
#include <misc/IntervalSet.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <map>
#include <optional>
#include <string>
#include <typeindex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace c3 {

using TokenList = std::vector<size_t>;

using RuleList = std::vector<size_t>;

struct CandidateRule {
  size_t startTokenIndex;
  RuleList ruleList;

  friend bool operator==(const CandidateRule& lhs, const CandidateRule& rhs) = default;
};

/**
 * All the candidates which have been found. Tokens and rules are separated.
 * – Token entries include a list of tokens that directly follow them (see also
 * the "following" member in the FollowSetWithPath class). – Rule entries
 * include the index of the starting token within the evaluated rule, along with
 * a call stack of rules found during evaluation. – cancelled will be true if
 * the collectCandidates() was cancelled or timed out.
 */
struct CandidatesCollection {
  std::map<size_t, TokenList> tokens;
  std::map<size_t, CandidateRule> rules;
  bool isCancelled;

  friend bool operator==(const CandidatesCollection& lhs, const CandidatesCollection& rhs) =
      default;
};

/**
 * Optional parameters for `CodeCompletionCore`.
 */
struct Parameters {
  /** An option parser rule context to limit the search space. */
  const antlr4::ParserRuleContext* context = nullptr;

  /** If non-zero, the number of milliseconds until collecting times out. */
  std::optional<std::chrono::milliseconds> timeout = std::nullopt;

  /**
   * If set to a non-`NULL` atomic boolean, and that boolean value is set to
   * true while the function is executing, then collecting candidates will abort
   * as soon as possible.
   */
  std::atomic<bool>* isCancelled = nullptr;
};

struct DebugOptions {
  /**
   * Not dependent on showDebugOutput.
   * Prints the collected rules + tokens to terminal.
   */
  bool showResult = false;

  /**
   * Enables printing ATN state info to terminal.
   */
  bool showDebugOutput = false;

  /**
   * Only relevant when showDebugOutput is true.
   * Enables transition printing for a state.
   */
  bool showTransitions = false;

  /**
   * Only relevant when showDebugOutput is true.
   * Enables call stack printing for each rule recursion.
   */
  bool showRuleStack = false;
};

class CodeCompletionCore {
private:
  struct PipelineEntry {
    antlr4::atn::ATNState* state;
    size_t tokenListIndex;
  };

  struct RuleWithStartToken {
    size_t startTokenIndex;
    size_t ruleIndex;
  };

  using RuleWithStartTokenList = std::vector<RuleWithStartToken>;

  /**
   * A record for a follow set along with the path at which this set was found.
   * If there is only a single symbol in the interval set then we also collect and
   * store tokens which follow this symbol directly in its rule (i.e. there is no
   * intermediate rule transition). Only single label transitions are considered.
   * This is useful if you have a chain of tokens which can be suggested as a
   * whole, because there is a fixed sequence in the grammar.
   */
  struct FollowSetWithPath {
    antlr4::misc::IntervalSet intervals;
    RuleList path;
    TokenList following;
  };

  /**
   * A list of follow sets (for a given state number) + all of them combined for
   * quick hit tests + whether they are exhaustive (false if subsequent
   * yet-unprocessed rules could add further tokens to the follow set, true
   * otherwise). This data is static in nature (because the used ATN states are
   * part of a static struct: the ATN). Hence it can be shared between all C3
   * instances, however it depends on the actual parser class (type).
   */
  struct FollowSetsHolder {
    std::vector<FollowSetWithPath> sets;
    antlr4::misc::IntervalSet combined;
    bool isExhaustive;
  };

  using FollowSetsPerState = std::unordered_map<size_t, FollowSetsHolder>;

  /** Token stream position info after a rule was processed. */
  using RuleEndStatus = std::unordered_set<size_t>;

public:
  explicit CodeCompletionCore(antlr4::Parser* parser);

  /**
   * Tailoring of the result:
   * Tokens which should not appear in the candidates set.
   */
  std::unordered_set<size_t> ignoredTokens;  // NOLINT: public field

  /**
   * Rules which replace any candidate token they contain.
   * This allows to return descriptive rules (e.g. className, instead of
   * ID/identifier).
   */
  std::unordered_set<size_t> preferredRules;  // NOLINT: public field

  /**
   * Specify if preferred rules should translated top-down (higher index rule
   * returns first) or bottom-up (lower index rule returns first).
   */
  bool translateRulesTopDown = false;  // NOLINT: public field

  /**
   * Print human readable ATN state and other info.
   */
  DebugOptions debugOptions;  // NOLINT: public field

  /**
   * This is the main entry point. The caret token index specifies the token
   * stream index for the token which currently covers the caret (or any other
   * position you want to get code completion candidates for). Optionally you
   * can pass in a parser rule context which limits the ATN walk to only that or
   * called rules. This can significantly speed up the retrieval process but
   * might miss some candidates (if they are outside of the given context).
   *
   * @param caretTokenIndex The index of the token at the caret position.
   * @param parameters Optional parameters.
   * @returns The collection of completion candidates. If cancelled or timed
   * out, the returned collection will have its 'cancelled' value set to true
   * and the collected candidates may be incomplete.
   */
  CandidatesCollection collectCandidates(size_t caretTokenIndex, Parameters parameters = {});

private:
  static thread_local std::unordered_map<std::type_index, FollowSetsPerState> followSetsByATN;
  static std::vector<std::string> atnStateTypeMap;

  antlr4::Parser* parser;
  const antlr4::atn::ATN* atn;
  const antlr4::dfa::Vocabulary* vocabulary;
  const std::vector<std::string>* ruleNames;
  std::vector<const antlr4::Token*> tokens;
  std::vector<int> precedenceStack;

  size_t tokenStartIndex = 0;
  size_t statesProcessed = 0;

  /**
   * A mapping of rule index + token stream position to end token positions.
   * A rule which has been visited before with the same input position will
   * always produce the same output positions.
   */
  std::unordered_map<size_t, std::unordered_map<size_t, RuleEndStatus>> shortcutMap;

  /** The collected candidates (rules and tokens). */
  c3::CandidatesCollection candidates;

  std::optional<std::chrono::milliseconds> timeout;
  std::atomic<bool>* cancel;
  std::chrono::steady_clock::time_point timeoutStart;

  bool checkPredicate(const antlr4::atn::PredicateTransition* transition);

  bool translateStackToRuleIndex(RuleWithStartTokenList const& ruleWithStartTokenList);

  bool translateToRuleIndex(size_t index, RuleWithStartTokenList const& ruleWithStartTokenList);

  static std::vector<size_t> getFollowingTokens(const antlr4::atn::Transition* transition);

  FollowSetsHolder determineFollowSets(antlr4::atn::ATNState* start, antlr4::atn::ATNState* stop);

  bool collectFollowSets(
      antlr4::atn::ATNState* state,
      antlr4::atn::ATNState* stopState,
      std::vector<FollowSetWithPath>& followSets,
      std::vector<antlr4::atn::ATNState*>& stateStack,
      std::vector<size_t>& ruleStack
  );

  RuleEndStatus processRule(
      antlr4::atn::RuleStartState* startState,
      size_t tokenListIndex,
      RuleWithStartTokenList& callStack,
      int precedence,
      size_t indentation,
      bool& timedOut
  );

  antlr4::misc::IntervalSet allUserTokens() const;

  std::string generateBaseDescription(antlr4::atn::ATNState* state);

  void printDescription(
      size_t indentation,
      antlr4::atn::ATNState* state,
      std::string const& baseDescription,
      size_t tokenIndex
  );

  void printRuleState(RuleWithStartTokenList const& stack);

  void printOverallResults();
};

}  // namespace c3
