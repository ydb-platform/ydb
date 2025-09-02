//
//  CodeCompletionCore.cpp
//
//  C++ port of antlr4-c3 (TypeScript) by Mike Lischke
//  Licensed under the MIT License.
//

#include "CodeCompletionCore.hpp"

#include <Parser.h>
#include <ParserRuleContext.h>
#include <Token.h>
#include <Vocabulary.h>
#include <atn/ATN.h>
#include <atn/ATNState.h>
#include <atn/ATNStateType.h>
#include <atn/PrecedencePredicateTransition.h>
#include <atn/PredicateTransition.h>
#include <atn/RuleStartState.h>
#include <atn/RuleStopState.h>
#include <atn/RuleTransition.h>
#include <atn/Transition.h>
#include <atn/TransitionType.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <iostream>
#include <iterator>
#include <ranges>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

namespace c3 {

namespace {

std::vector<size_t> longestCommonPrefix(
    std::vector<size_t> const& lhs, std::vector<size_t> const& rhs
) {
  size_t index = 0;
  for (; index < std::min(lhs.size(), rhs.size()); index++) {
    if (lhs[index] != rhs[index]) {
      break;
    }
  }
  return {
      lhs.begin(),
      std::next(lhs.begin(), static_cast<std::ptrdiff_t>(index)),
  };
}

}  // namespace

thread_local std::unordered_map<std::type_index, CodeCompletionCore::FollowSetsPerState>  // NOLINT
    CodeCompletionCore::followSetsByATN = {};

// Matches ATNStateType enum
std::vector<std::string> CodeCompletionCore::atnStateTypeMap  // NOLINT
    {
        "invalid",
        "basic",
        "rule start",
        "block start",
        "plus block start",
        "star block start",
        "token start",
        "rule stop",
        "block end",
        "star loop back",
        "star loop entry",
        "plus loop back",
        "loop end",
    };

CodeCompletionCore::CodeCompletionCore(antlr4::Parser* parser)
    : parser(parser)
    , atn(&parser->getATN())
    , vocabulary(&parser->getVocabulary())
    , ruleNames(&parser->getRuleNames())
    , timeout(0)
    , cancel(nullptr) {
}

CandidatesCollection CodeCompletionCore::collectCandidates(
    size_t caretTokenIndex, Parameters parameters
) {
  const auto* context = parameters.context;

  timeout = parameters.timeout;
  cancel = parameters.isCancelled;
  timeoutStart = std::chrono::steady_clock::now();

  shortcutMap.clear();
  candidates.rules.clear();
  candidates.tokens.clear();
  candidates.isCancelled = false;
  statesProcessed = 0;
  precedenceStack = {};

  tokenStartIndex = (context != nullptr) ? context->start->getTokenIndex() : 0;
  auto* const tokenStream = parser->getTokenStream();

  tokens = {};
  size_t offset = tokenStartIndex;
  while (true) {
    const antlr4::Token* token = tokenStream->get(offset++);
    if (token->getChannel() == antlr4::Token::DEFAULT_CHANNEL) {
      tokens.push_back(token);

      if (token->getTokenIndex() >= caretTokenIndex) {
        break;
      }
    }

    // Do not check for the token index here, as we want to end with the first
    // unhidden token on or after the caret.
    if (token->getType() == antlr4::Token::EOF) {
      break;
    }
  }

  RuleWithStartTokenList callStack = {};
  const size_t startRule = (context != nullptr) ? context->getRuleIndex() : 0;

  processRule(atn->ruleToStartState[startRule], 0, callStack, 0, 0, candidates.isCancelled);

  for (auto& [_, following] : candidates.tokens) {
    auto removed = std::ranges::remove_if(following, [&](size_t token) {
      return ignoredTokens.contains(token);
    });
    following.erase(std::begin(removed), std::end(removed));
  }

  printOverallResults();

  return candidates;
}

/**
 * Checks if the predicate associated with the given transition evaluates to
 * true.
 *
 * @param transition The transition to check.
 * @returns the evaluation result of the predicate.
 */
bool CodeCompletionCore::checkPredicate(const antlr4::atn::PredicateTransition* transition) {
  return transition->getPredicate()->eval(parser, &antlr4::ParserRuleContext::EMPTY);
}

/**
 * Walks the rule chain upwards or downwards (depending on
 * translateRulesTopDown) to see if that matches any of the preferred rules. If
 * found, that rule is added to the collection candidates and true is returned.
 *
 * @param ruleWithStartTokenList The list to convert.
 * @returns true if any of the stack entries was converted.
 */
bool CodeCompletionCore::translateStackToRuleIndex(
    RuleWithStartTokenList const& ruleWithStartTokenList
) {
  if (preferredRules.empty()) {
    return false;
  }

  // Change the direction we iterate over the rule stack
  auto forward = std::views::iota(0U, ruleWithStartTokenList.size());
  auto backward = forward | std::views::reverse;

  if (translateRulesTopDown) {
    // Loop over the rule stack from lowest to highest rule level. This will
    // prioritize a lower preferred rule if it is a child of a higher one that
    // is also a preferred rule.
    return std::ranges::any_of(backward, [&](auto index) {
      return translateToRuleIndex(index, ruleWithStartTokenList);
    });
  }

  // Loop over the rule stack from highest to lowest rule level. This will
  // prioritize a higher preferred rule if it contains a lower one that is
  // also a preferred rule.
  return std::ranges::any_of(forward, [&](auto index) {
    return translateToRuleIndex(index, ruleWithStartTokenList);
  });
}

/**
 * Given the index of a rule from a rule chain, check if that matches any of the
 * preferred rules. If it matches, that rule is added to the collection
 * candidates and true is returned.
 *
 * @param i The rule index.
 * @param ruleWithStartTokenList The list to check.
 * @returns true if the specified rule is in the list of preferred rules.
 */
bool CodeCompletionCore::translateToRuleIndex(
    size_t index, RuleWithStartTokenList const& ruleWithStartTokenList
) {
  const auto& rwst = ruleWithStartTokenList[index];

  if (preferredRules.contains(rwst.ruleIndex)) {
    // Add the rule to our candidates list along with the current rule path,
    // but only if there isn't already an entry like that.
    std::vector<size_t> path;
    path.reserve(index);
    for (size_t i = 0; i < index; i++) {
      path.push_back(ruleWithStartTokenList[i].ruleIndex);
    }

    bool addNew = true;
    for (auto const& [cRuleEntryRuleIndex, cRuleEntryCandidateRule] : candidates.rules) {
      if (cRuleEntryRuleIndex != rwst.ruleIndex ||
          cRuleEntryCandidateRule.ruleList.size() != path.size()) {
        continue;
      }

      // Found an entry for this rule. Same path?
      bool samePath = true;
      for (size_t i = 0; i < path.size(); i++) {
        if (path[i] == cRuleEntryCandidateRule.ruleList[i]) {
          samePath = false;
          break;
        }
      }

      // If same path, then don't add a new (duplicate) entry.
      if (samePath) {
        addNew = false;
        break;
      }
    }

    if (addNew) {
      candidates.rules[rwst.ruleIndex] = {
          .startTokenIndex = rwst.startTokenIndex,
          .ruleList = path,
      };
      if (debugOptions.showDebugOutput) {
        std::cout << "=====> collected: " << ruleNames->at(rwst.ruleIndex) << "\n";
      }
    }

    return true;
  }

  return false;
}

/**
 * This method follows the given transition and collects all symbols within the
 * same rule that directly follow it without intermediate transitions to other
 * rules and only if there is a single symbol for a transition.
 *
 * @param transition The transition from which to start.
 * @returns A list of toke types.
 */
std::vector<size_t> CodeCompletionCore::getFollowingTokens(const antlr4::atn::Transition* transition
) {
  std::vector<size_t> result;
  std::vector<antlr4::atn::ATNState*> pipeline = {transition->target};

  while (!pipeline.empty()) {
    antlr4::atn::ATNState* state = pipeline.back();
    pipeline.pop_back();

    if (state == nullptr) {
      continue;
    }

    for (const antlr4::atn::ConstTransitionPtr& outgoing : state->transitions) {
      if (outgoing->getTransitionType() == antlr4::atn::TransitionType::ATOM) {
        if (!outgoing->isEpsilon()) {
          const auto list = outgoing->label();
          if (list.size() == 1) {
            result.push_back(list.get(0));
            pipeline.push_back(outgoing->target);
          }
        } else {
          pipeline.push_back(outgoing->target);
        }
      }
    }
  }

  return result;
}

/**
 * Entry point for the recursive follow set collection function.
 *
 * @param start Start state.
 * @param stop Stop state.
 * @returns Follow sets.
 */
CodeCompletionCore::FollowSetsHolder CodeCompletionCore::determineFollowSets(
    antlr4::atn::ATNState* start, antlr4::atn::ATNState* stop
) {
  std::vector<FollowSetWithPath> sets = {};
  std::vector<antlr4::atn::ATNState*> stateStack = {};
  std::vector<size_t> ruleStack = {};
  const bool isExhaustive = collectFollowSets(start, stop, sets, stateStack, ruleStack);

  // Sets are split by path to allow translating them to preferred rules. But
  // for quick hit tests it is also useful to have a set with all symbols
  // combined.
  antlr4::misc::IntervalSet combined;
  for (const auto& set : sets) {
    combined.addAll(set.intervals);
  }

  return {
      .sets = sets,
      .combined = combined,
      .isExhaustive = isExhaustive,
  };
}

/**
 * Collects possible tokens which could be matched following the given ATN
 * state. This is essentially the same algorithm as used in the LL1Analyzer
 * class, but here we consider predicates also and use no parser rule context.
 *
 * @param s The state to continue from.
 * @param stopState The state which ends the collection routine.
 * @param followSets A pass through parameter to add found sets to.
 * @param stateStack A stack to avoid endless recursions.
 * @param ruleStack The current rule stack.
 * @returns true if the follow sets is exhaustive, i.e. we terminated before the
 * rule end was reached, so no subsequent rules could add tokens
 */
bool CodeCompletionCore::collectFollowSets(  // NOLINT
    antlr4::atn::ATNState* state,
    antlr4::atn::ATNState* stopState,
    std::vector<FollowSetWithPath>& followSets,
    std::vector<antlr4::atn::ATNState*>& stateStack,
    std::vector<size_t>& ruleStack
) {
  if (std::ranges::find(stateStack, state) != stateStack.end()) {
    return true;
  }
  stateStack.push_back(state);

  if (state == stopState || state->getStateType() == antlr4::atn::ATNStateType::RULE_STOP) {
    stateStack.pop_back();
    return false;
  }

  bool isExhaustive = true;
  for (const antlr4::atn::ConstTransitionPtr& transitionPtr : state->transitions) {
    const antlr4::atn::Transition* transition = transitionPtr.get();

    if (transition->getTransitionType() == antlr4::atn::TransitionType::RULE) {
      const auto* ruleTransition = dynamic_cast<const antlr4::atn::RuleTransition*>(transition);

      if (std::ranges::find(ruleStack, ruleTransition->target->ruleIndex) != ruleStack.end()) {
        continue;
      }

      ruleStack.push_back(ruleTransition->target->ruleIndex);
      const bool ruleFollowSetsIsExhaustive =
          collectFollowSets(transition->target, stopState, followSets, stateStack, ruleStack);
      ruleStack.pop_back();

      // If the subrule had an epsilon transition to the rule end, the tokens
      // added to the follow set are non-exhaustive and we should continue
      // processing subsequent transitions post-rule
      if (!ruleFollowSetsIsExhaustive) {
        const bool nextStateFollowSetsIsExhaustive = collectFollowSets(
            ruleTransition->followState, stopState, followSets, stateStack, ruleStack
        );
        isExhaustive = isExhaustive && nextStateFollowSetsIsExhaustive;
      }

    } else if (transition->getTransitionType() == antlr4::atn::TransitionType::PREDICATE) {
      if (checkPredicate(dynamic_cast<const antlr4::atn::PredicateTransition*>(transition))) {
        const bool nextStateFollowSetsIsExhaustive =
            collectFollowSets(transition->target, stopState, followSets, stateStack, ruleStack);
        isExhaustive = isExhaustive && nextStateFollowSetsIsExhaustive;
      }
    } else if (transition->isEpsilon()) {
      const bool nextStateFollowSetsIsExhaustive =
          collectFollowSets(transition->target, stopState, followSets, stateStack, ruleStack);
      isExhaustive = isExhaustive && nextStateFollowSetsIsExhaustive;
    } else if (transition->getTransitionType() == antlr4::atn::TransitionType::WILDCARD) {
      followSets.push_back({
          .intervals = allUserTokens(),
          .path = ruleStack,
          .following = {},
      });
    } else {
      antlr4::misc::IntervalSet label = transition->label();
      if (!label.isEmpty()) {
        if (transition->getTransitionType() == antlr4::atn::TransitionType::NOT_SET) {
          label = label.complement(allUserTokens());
        }
        followSets.push_back({
            .intervals = label,
            .path = ruleStack,
            .following = getFollowingTokens(transition),
        });
      }
    }
  }
  stateStack.pop_back();

  return isExhaustive;
}

/**
 * Walks the ATN for a single rule only. It returns the token stream position
 * for each path that could be matched in this rule. The result can be empty in
 * case we hit only non-epsilon transitions that didn't match the current input
 * or if we hit the caret position.
 *
 * @param startState The start state.
 * @param tokenListIndex The token index we are currently at.
 * @param callStack The stack that indicates where in the ATN we are currently.
 * @param precedence The current precedence level.
 * @param indentation A value to determine the current indentation when doing
 * debug prints.
 * @returns the set of token stream indexes (which depend on the ways that had
 * to be taken).
 */
CodeCompletionCore::RuleEndStatus CodeCompletionCore::processRule(  // NOLINT
    antlr4::atn::RuleStartState* startState,
    size_t tokenListIndex,
    RuleWithStartTokenList& callStack,
    int precedence,      // NOLINT
    size_t indentation,  // NOLINT
    bool& timedOut
) {
  // Cancelled by external caller?
  if (cancel != nullptr && cancel->load()) {
    timedOut = true;
    return {};
  }

  // Check for timeout
  timedOut = false;
  if (timeout.has_value() && std::chrono::steady_clock::now() - timeoutStart > timeout) {
    timedOut = true;
    return {};
  }

  // Start with rule specific handling before going into the ATN walk.

  // Check first if we've taken this path with the same input before.
  std::unordered_map<size_t, RuleEndStatus>& positionMap = shortcutMap[startState->ruleIndex];
  if (positionMap.contains(tokenListIndex)) {
    if (debugOptions.showDebugOutput) {
      std::cout << "=====> shortcut" << "\n";
    }
    return positionMap[tokenListIndex];
  }

  RuleEndStatus result;

  // For rule start states we determine and cache the follow set, which gives us
  // 3 advantages: 1) We can quickly check if a symbol would be matched when we
  // follow that rule. We can so check in advance
  //    and can save us all the intermediate steps if there is no match.
  // 2) We'll have all symbols that are collectable already together when we are
  // at the caret on rule enter. 3) We get this lookup for free with any 2nd or
  // further visit of the same rule, which often happens
  //    in non trivial grammars, especially with (recursive) expressions and of
  //    course when invoking code completion multiple times.
  FollowSetsPerState& setsPerState = followSetsByATN[typeid(parser)];

  if (!setsPerState.contains(startState->stateNumber)) {
    antlr4::atn::RuleStopState* stop = atn->ruleToStopState[startState->ruleIndex];
    setsPerState[startState->stateNumber] = determineFollowSets(startState, stop);
  }

  const FollowSetsHolder& followSets = setsPerState[startState->stateNumber];

  // Get the token index where our rule starts from our (possibly filtered)
  // token list
  const size_t startTokenIndex = tokens[tokenListIndex]->getTokenIndex();

  callStack.push_back({
      .startTokenIndex = startTokenIndex,
      .ruleIndex = startState->ruleIndex,
  });

  if (tokenListIndex >= tokens.size() - 1) {  // At caret?
    if (preferredRules.contains(startState->ruleIndex)) {
      // No need to go deeper when collecting entries and we reach a rule that
      // we want to collect anyway.
      translateStackToRuleIndex(callStack);
    } else {
      // Convert all follow sets to either single symbols or their associated
      // preferred rule and add the result to our candidates list.
      for (const FollowSetWithPath& set : followSets.sets) {
        RuleWithStartTokenList fullPath = callStack;

        // Rules derived from our followSet will always start at the same token
        // as our current rule.
        for (const size_t rule : set.path) {
          fullPath.push_back({
              .startTokenIndex = startTokenIndex,
              .ruleIndex = rule,
          });
        }

        if (!translateStackToRuleIndex(fullPath)) {
          for (const size_t symbol : set.intervals.toList()) {
            if (!ignoredTokens.contains(symbol)) {
              if (debugOptions.showDebugOutput) {
                std::cout << "=====> collected: " << vocabulary->getDisplayName(symbol) << "\n";
              }
              if (!candidates.tokens.contains(symbol)) {
                // Following is empty if there is more than one entry in the
                // set.
                candidates.tokens[symbol] = set.following;
              } else if (candidates.tokens[symbol] != set.following) {
                // More than one following list for the same symbol.
                candidates.tokens[symbol] = {};
              }
            }
          }
        }
      }
    }

    if (!followSets.isExhaustive) {
      // If we're at the caret but the follow sets is non-exhaustive (empty or
      // all tokens are optional), we should continue to collect tokens
      // following this rule
      result.insert(tokenListIndex);
    }

    callStack.pop_back();

    return result;
  }

  // Process the rule if we either could pass it without consuming anything
  // (epsilon transition) or if the current input symbol will be matched
  // somewhere after this entry point. Otherwise stop here.
  const size_t currentSymbol = tokens[tokenListIndex]->getType();
  if (followSets.isExhaustive && !followSets.combined.contains(currentSymbol)) {
    callStack.pop_back();

    return result;
  }

  if (startState->isLeftRecursiveRule) {
    precedenceStack.push_back(precedence);
  }

  // The current state execution pipeline contains all yet-to-be-processed ATN
  // states in this rule. For each such state we store the token index + a list
  // of rules that lead to it.
  std::vector<PipelineEntry> statePipeline;

  // Bootstrap the pipeline.
  statePipeline.push_back({.state = startState, .tokenListIndex = tokenListIndex});

  while (!statePipeline.empty()) {
    if (cancel != nullptr && cancel->load()) {
      timedOut = true;
      return {};
    }

    const PipelineEntry currentEntry = statePipeline.back();
    statePipeline.pop_back();
    ++statesProcessed;

    const size_t currentSymbol = tokens[currentEntry.tokenListIndex]->getType();

    const bool atCaret = currentEntry.tokenListIndex >= tokens.size() - 1;
    if (debugOptions.showDebugOutput) {
      printDescription(
          indentation,
          currentEntry.state,
          generateBaseDescription(currentEntry.state),
          currentEntry.tokenListIndex
      );
      if (debugOptions.showRuleStack) {
        printRuleState(callStack);
      }
    }

    if (currentEntry.state->getStateType() == antlr4::atn::ATNStateType::RULE_STOP) {
      // Record the token index we are at, to report it to the caller.
      result.insert(currentEntry.tokenListIndex);
      continue;
    }

    // We simulate here the same precedence handling as the parser does, which
    // uses hard coded values. For rules that are not left recursive this value
    // is ignored (since there is no precedence transition).
    for (const antlr4::atn::ConstTransitionPtr& transition : currentEntry.state->transitions) {
      switch (transition->getTransitionType()) {
        case antlr4::atn::TransitionType::RULE: {
          const auto* ruleTransition =
              dynamic_cast<const antlr4::atn::RuleTransition*>(transition.get());
          auto* ruleStartState = dynamic_cast<antlr4::atn::RuleStartState*>(ruleTransition->target);
          bool innerCancelled = false;
          const RuleEndStatus endStatus = processRule(
              ruleStartState,
              currentEntry.tokenListIndex,
              callStack,
              ruleTransition->precedence,
              indentation + 1,
              innerCancelled
          );

          if (innerCancelled) {
            timedOut = true;
            return {};
          }

          for (const size_t position : endStatus) {
            statePipeline.push_back({
                .state = ruleTransition->followState,
                .tokenListIndex = position,
            });
          }

        } break;

        case antlr4::atn::TransitionType::PREDICATE: {
          const auto* predTransition =
              dynamic_cast<const antlr4::atn::PredicateTransition*>(transition.get());
          if (checkPredicate(predTransition)) {
            statePipeline.push_back({
                .state = transition->target,
                .tokenListIndex = currentEntry.tokenListIndex,
            });
          }

        } break;

        case antlr4::atn::TransitionType::PRECEDENCE: {
          const auto* predTransition =
              dynamic_cast<const antlr4::atn::PrecedencePredicateTransition*>(transition.get());
          if (predTransition->getPrecedence() >= precedenceStack[precedenceStack.size() - 1]) {
            statePipeline.push_back({
                .state = transition->target,
                .tokenListIndex = currentEntry.tokenListIndex,
            });
          }

        } break;

        case antlr4::atn::TransitionType::WILDCARD: {
          if (atCaret) {
            if (!translateStackToRuleIndex(callStack)) {
              for (const auto token :
                   std::views::iota(antlr4::Token::MIN_USER_TOKEN_TYPE, atn->maxTokenType + 1)) {
                if (!ignoredTokens.contains(token)) {
                  candidates.tokens[token] = {};
                }
              }
            }
          } else {
            statePipeline.push_back({
                .state = transition->target,
                .tokenListIndex = currentEntry.tokenListIndex + 1,
            });
          }

        } break;

        default: {
          if (transition->isEpsilon()) {
            // Jump over simple states with a single outgoing epsilon
            // transition.
            statePipeline.push_back({
                .state = transition->target,
                .tokenListIndex = currentEntry.tokenListIndex,
            });
            continue;
          }

          antlr4::misc::IntervalSet set = transition->label();
          if (!set.isEmpty()) {
            if (transition->getTransitionType() == antlr4::atn::TransitionType::NOT_SET) {
              set = set.complement(allUserTokens());
            }
            if (atCaret) {
              if (!translateStackToRuleIndex(callStack)) {
                const std::vector<ptrdiff_t> list = set.toList();
                const bool hasTokenSequence = list.size() == 1;
                for (const size_t symbol : list) {
                  if (!ignoredTokens.contains(symbol)) {
                    if (debugOptions.showDebugOutput) {
                      std::cout << "=====> collected: " << vocabulary->getDisplayName(symbol)
                                << "\n";
                    }

                    std::vector<size_t> followingTokens;
                    if (hasTokenSequence) {
                      followingTokens = getFollowingTokens(transition.get());
                    }

                    if (!candidates.tokens.contains(symbol)) {
                      candidates.tokens[symbol] = followingTokens;
                    } else {
                      candidates.tokens[symbol] =
                          longestCommonPrefix(followingTokens, candidates.tokens[symbol]);
                    }
                  }
                }
              }
            } else {
              if (set.contains(currentSymbol)) {
                if (debugOptions.showDebugOutput) {
                  std::cout << "=====> consumed: " << vocabulary->getDisplayName(currentSymbol)
                            << "\n";
                }
                statePipeline.push_back({
                    .state = transition->target,
                    .tokenListIndex = currentEntry.tokenListIndex + 1,
                });
              }
            }
          }
        }
      }
    }
  }

  callStack.pop_back();
  if (startState->isLeftRecursiveRule) {
    precedenceStack.pop_back();
  }

  // Cache the result, for later lookup to avoid duplicate walks.
  positionMap[tokenListIndex] = result;

  return result;
}

antlr4::misc::IntervalSet CodeCompletionCore::allUserTokens() const {
  const auto min = antlr4::Token::MIN_USER_TOKEN_TYPE;
  const auto max = static_cast<ptrdiff_t>(atn->maxTokenType);
  return antlr4::misc::IntervalSet::of(min, max);
}

std::string CodeCompletionCore::generateBaseDescription(antlr4::atn::ATNState* state) {
  const std::string stateValue = (state->stateNumber == antlr4::atn::ATNState::INVALID_STATE_NUMBER)
                                     ? "Invalid"
                                     : std::to_string(state->stateNumber);
  std::stringstream output;

  output << "[" << stateValue << " " << atnStateTypeMap[static_cast<size_t>(state->getStateType())]
         << "]";
  output << " in ";
  output << ruleNames->at(state->ruleIndex);
  return output.str();
}

void CodeCompletionCore::printDescription(
    size_t indentation,
    antlr4::atn::ATNState* state,
    std::string const& baseDescription,
    size_t tokenIndex
) {
  const auto indent = std::string(indentation * 2, ' ');

  std::string transitionDescription;
  if (debugOptions.showTransitions) {
    for (const antlr4::atn::ConstTransitionPtr& transition : state->transitions) {
      std::string labels;
      std::vector<ptrdiff_t> symbols = transition->label().toList();

      if (symbols.size() > 2) {
        // Only print start and end symbols to avoid large lists in debug output.
        labels = vocabulary->getDisplayName(static_cast<size_t>(symbols[0])) + " .. " +
                 vocabulary->getDisplayName(static_cast<size_t>(symbols[symbols.size() - 1]));
      } else {
        for (const size_t symbol : symbols) {
          if (!labels.empty()) {
            labels += ", ";
          }
          labels += vocabulary->getDisplayName(symbol);
        }
      }
      if (labels.empty()) {
        labels = "ε";
      }

      transitionDescription += "\n";
      transitionDescription += indent;
      transitionDescription += "\t(";
      transitionDescription += labels;
      transitionDescription += ") [";
      transitionDescription += std::to_string(transition->target->stateNumber);
      transitionDescription += " ";
      transitionDescription +=
          atnStateTypeMap[static_cast<size_t>(transition->target->getStateType())];
      transitionDescription += "] in ";
      transitionDescription += ruleNames->at(transition->target->ruleIndex);
    }
  }

  std::string output;
  if (tokenIndex >= tokens.size() - 1) {
    output = "<<" + std::to_string(tokenStartIndex + tokenIndex) + ">> ";
  } else {
    output = "<" + std::to_string(tokenStartIndex + tokenIndex) + "> ";
  }

  std::cout << indent + output + "Current state: " + baseDescription + transitionDescription
            << "\n";
}

void CodeCompletionCore::printRuleState(RuleWithStartTokenList const& stack) {
  if (stack.empty()) {
    std::cout << "<empty stack>\n";
    return;
  }

  for (const RuleWithStartToken& rule : stack) {
    std::cout << ruleNames->at(rule.ruleIndex);
  }
  std::cout << "\n";
}

void CodeCompletionCore::printOverallResults() {
  if (debugOptions.showResult) {
    if (candidates.isCancelled) {
      std::cout << "*** TIMED OUT ***\n";
    }

    std::cout << "States processed: " << statesProcessed << "\n\n";

    std::cout << "Collected rules:\n";
    for (const auto& [tokenIndex, rule] : candidates.rules) {
      std::cout << ruleNames->at(tokenIndex);
      std::cout << ", path: ";

      for (const size_t token : rule.ruleList) {
        std::cout << ruleNames->at(token) << " ";
      }
    }
    std::cout << "\n\n";

    std::set<std::string> sortedTokens;
    for (const auto& [token, tokenList] : candidates.tokens) {
      std::string value = vocabulary->getDisplayName(token);
      for (const size_t following : tokenList) {
        value += " " + vocabulary->getDisplayName(following);
      }
      sortedTokens.emplace(value);
    }

    std::cout << "Collected tokens:\n";
    for (const std::string& symbol : sortedTokens) {
      std::cout << symbol;
    }
    std::cout << "\n\n";
  }
}

}  // namespace c3
