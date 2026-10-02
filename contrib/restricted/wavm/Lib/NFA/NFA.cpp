#include "WAVM/NFA/NFA.h"
#include <inttypes.h>
#include <string.h>
#include <algorithm>
#include <memory>
#include <string>
#include <vector>
#include "WAVM/Inline/Assert.h"
#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/Inline/Errors.h"
#include "WAVM/Inline/Hash.h"
#include "WAVM/Inline/HashMap.h"
#include "WAVM/Inline/HashSet.h"
#include "WAVM/Inline/Timing.h"
#include "WAVM/Logging/Logging.h"

using namespace WAVM;
using namespace WAVM::NFA;

struct NFAState
{
	HashMap<StateIndex, CharSet> nextStateToPredicateMap;
	std::vector<StateIndex> epsilonNextStates;
};

namespace WAVM { namespace NFA {
	struct Builder
	{
		std::vector<NFAState> nfaStates;
	};
}}

typedef std::vector<StateIndex> StateSet;

template<typename Element> void addUnique(std::vector<Element>& vector, const Element& element)
{
	for(const auto& existingElement : vector)
	{
		if(existingElement == element) { return; }
	}
	vector.push_back(element);
}

Builder* NFA::createBuilder()
{
	auto builder = new Builder();
	addState(builder);
	return builder;
}

StateIndex NFA::addState(Builder* builder)
{
	WAVM_ASSERT(builder->nfaStates.size() < INT16_MAX);
	builder->nfaStates.emplace_back();
	return StateIndex(builder->nfaStates.size() - 1);
}

void NFA::addEdge(Builder* builder,
				  StateIndex initialState,
				  const CharSet& predicate,
				  StateIndex nextState)
{
	CharSet& transitionPredicate
		= builder->nfaStates[initialState].nextStateToPredicateMap.getOrAdd(nextState, CharSet{});
	transitionPredicate = transitionPredicate | predicate;
}

void NFA::addEpsilonEdge(Builder* builder, StateIndex initialState, StateIndex nextState)
{
	addUnique(builder->nfaStates[initialState].epsilonNextStates, nextState);
}

StateIndex NFA::getNonTerminalEdge(Builder* builder, StateIndex initialState, char c)
{
	for(const auto& nextStateToPredicatePair :
		builder->nfaStates[initialState].nextStateToPredicateMap)
	{
		if(nextStateToPredicatePair.key >= 0 && nextStateToPredicatePair.value.contains((U8)c))
		{ return nextStateToPredicatePair.key; }
	}

	return unmatchedCharacterTerminal;
}

struct DFAState
{
	StateIndex nextStateByChar[256];
	DFAState()
	{
		for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
		{ nextStateByChar[charIndex] = unmatchedCharacterTerminal | edgeDoesntConsumeInputFlag; }
	}
};

static std::vector<DFAState> convertToDFA(Builder* builder)
{
	Timing::Timer timer;

	std::vector<DFAState> dfaStates;
	HashMap<StateSet, StateIndex> nfaStateSetToDFAStateMap;
	std::vector<StateSet> dfaStateToNFAStateSetMap;
	std::vector<StateIndex> pendingDFAStates;

	nfaStateSetToDFAStateMap.set(StateSet{0}, (StateIndex)0);
	dfaStateToNFAStateSetMap.emplace_back(StateSet{0});
	dfaStates.emplace_back();
	pendingDFAStates.push_back((StateIndex)0);

	Uptr maxLocalStates = 0;
	Uptr maxDFANextStates = 0;

	while(pendingDFAStates.size())
	{
		const StateIndex currentDFAStateIndex = pendingDFAStates.back();
		pendingDFAStates.pop_back();

		const StateSet currentStateSet = dfaStateToNFAStateSetMap[currentDFAStateIndex];

		// Expand the set of current states to include all states reachable by epsilon transitions
		// from the current states.
		StateSet epsilonClosureCurrentStateSet = currentStateSet;
		for(Uptr scanIndex = 0; scanIndex < epsilonClosureCurrentStateSet.size(); ++scanIndex)
		{
			StateIndex scanState = epsilonClosureCurrentStateSet[scanIndex];
			if(scanState >= 0)
			{
				for(auto epsilonNextState : builder->nfaStates[scanState].epsilonNextStates)
				{ addUnique(epsilonClosureCurrentStateSet, epsilonNextState); }
			}
		}

		// Find the subset of the non-terminal states in the current set.
		StateSet nonTerminalCurrentStateSet;
		StateIndex currentTerminalState = unmatchedCharacterTerminal | edgeDoesntConsumeInputFlag;
		bool hasCurrentTerminalState = false;
		for(auto stateIndex : epsilonClosureCurrentStateSet)
		{
			if(stateIndex >= 0) { addUnique(nonTerminalCurrentStateSet, stateIndex); }
			else
			{
				if(hasCurrentTerminalState)
				{
					Errors::fatalf("NFA has multiple possible terminal states for the same input");
				}
				hasCurrentTerminalState = true;
				currentTerminalState = stateIndex | edgeDoesntConsumeInputFlag;
			}
		}

		// Build a compact index of the states following all states in the current set.
		HashMap<StateIndex, StateIndex> stateIndexToLocalStateIndexMap;
		std::vector<StateIndex> localStateIndexToStateIndexMap;
		for(auto stateIndex : nonTerminalCurrentStateSet)
		{
			const NFAState& nfaState = builder->nfaStates[stateIndex];
			for(auto transition : nfaState.nextStateToPredicateMap)
			{
				if(!stateIndexToLocalStateIndexMap.contains(transition.key))
				{
					stateIndexToLocalStateIndexMap.set(
						transition.key, (StateIndex)localStateIndexToStateIndexMap.size());
					localStateIndexToStateIndexMap.emplace_back(transition.key);
				}
			}
		}

		if(!stateIndexToLocalStateIndexMap.contains(currentTerminalState))
		{
			stateIndexToLocalStateIndexMap.set(currentTerminalState,
											   (StateIndex)localStateIndexToStateIndexMap.size());
			localStateIndexToStateIndexMap.emplace_back(currentTerminalState);
		}

		static constexpr Uptr numSupportedLocalStates = 64;
		typedef DenseStaticIntSet<StateIndex, numSupportedLocalStates> LocalStateSet;

		const Uptr numLocalStates = stateIndexToLocalStateIndexMap.size();
		WAVM_ASSERT(numLocalStates <= numSupportedLocalStates);
		maxLocalStates = std::max<Uptr>(maxLocalStates, numLocalStates);

		// Combine the [nextState][char] transition maps for current states and transpose to
		// [char][nextState] After building the compact index of referenced states, the nextState
		// set can be represented as a 64-bit mask.
		LocalStateSet charToLocalStateSet[256];
		for(auto stateIndex : nonTerminalCurrentStateSet)
		{
			const NFAState& nfaState = builder->nfaStates[stateIndex];
			for(auto transition : nfaState.nextStateToPredicateMap)
			{
				for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
				{
					if(transition.value.contains((char)charIndex))
					{
						charToLocalStateSet[charIndex].add(
							stateIndexToLocalStateIndexMap[transition.key]);
					}
				}
			}
		}

		const LocalStateSet currentTerminalStateLocalSet(
			stateIndexToLocalStateIndexMap[currentTerminalState]);
		for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
		{
			if(charToLocalStateSet[charIndex].isEmpty())
			{ charToLocalStateSet[charIndex] = currentTerminalStateLocalSet; }
		}

		// Find the set of unique local state sets that follow this state set.
		std::vector<LocalStateSet> uniqueLocalNextStateSets;
		for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
		{
			const LocalStateSet localStateSet = charToLocalStateSet[charIndex];
			if(!localStateSet.isEmpty()) { addUnique(uniqueLocalNextStateSets, localStateSet); }
		}

		// For each unique local state set that follows this state set, find or create a
		// corresponding DFA state.
		HashMap<LocalStateSet, StateIndex> localStateSetToDFAStateIndexMap;
		for(auto localNextStateSet : uniqueLocalNextStateSets)
		{
			// Convert the local state set bit mask to a global NFA state set.
			StateSet nextStateSet;
			{
				LocalStateSet residualLocalStateSet = localNextStateSet;
				while(true)
				{
					const StateIndex localStateIndex = residualLocalStateSet.getSmallestMember();
					if(localStateIndex == numSupportedLocalStates) { break; }

					nextStateSet.push_back(localStateIndexToStateIndexMap.at(localStateIndex));
					residualLocalStateSet.remove(localStateIndex);
				};
			}

			if(nextStateSet.size() == 1 && *nextStateSet.begin() < 0)
			{ localStateSetToDFAStateIndexMap.set(localNextStateSet, *nextStateSet.begin()); }
			else
			{
				// Find an existing DFA state corresponding to this NFA state set.
				const StateIndex* nextDFAState = nfaStateSetToDFAStateMap.get(nextStateSet);
				if(nextDFAState)
				{ localStateSetToDFAStateIndexMap.set(localNextStateSet, *nextDFAState); }
				else
				{
					// If no corresponding DFA state existing yet, create a new one and add it to
					// the queue of pending states to process.
					const StateIndex nextDFAStateIndex = (StateIndex)dfaStates.size();
					localStateSetToDFAStateIndexMap.set(localNextStateSet, nextDFAStateIndex);
					nfaStateSetToDFAStateMap.set(nextStateSet, nextDFAStateIndex);
					dfaStateToNFAStateSetMap.emplace_back(nextStateSet);
					dfaStates.emplace_back();
					pendingDFAStates.push_back(nextDFAStateIndex);
				}
			}
		}

		// Set up the DFA transition map.
		DFAState& dfaState = dfaStates[nfaStateSetToDFAStateMap[currentStateSet]];
		for(auto localStateSetToDFAStateIndex : localStateSetToDFAStateIndexMap)
		{
			const LocalStateSet& localStateSet = localStateSetToDFAStateIndex.key;
			const StateIndex nextDFAStateIndex = localStateSetToDFAStateIndex.value;
			for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
			{
				if(charToLocalStateSet[charIndex] == localStateSet)
				{ dfaState.nextStateByChar[charIndex] = nextDFAStateIndex; }
			}
		}
		maxDFANextStates = std::max(maxDFANextStates, (Uptr)uniqueLocalNextStateSets.size());
	};

	Timing::logTimer("translated NFA->DFA", timer);
	Log::printf(Log::metrics,
				"  translated NFA with %" WAVM_PRIuPTR " states to DFA with %" WAVM_PRIuPTR
				" states\n",
				Uptr(builder->nfaStates.size()),
				Uptr(dfaStates.size()));
	Log::printf(Log::metrics,
				"  maximum number of states following a NFA state set: %" WAVM_PRIuPTR "\n",
				maxLocalStates);
	Log::printf(Log::metrics,
				"  maximum number of states following a DFA state: %" WAVM_PRIuPTR "\n",
				maxDFANextStates);

	return dfaStates;
}

struct StateTransitionsByChar
{
	U8 c;
	StateIndex* nextStateByInitialState;
	Uptr numStates;
	StateTransitionsByChar(U8 inC, Uptr inNumStates)
	: c(inC), nextStateByInitialState(nullptr), numStates(inNumStates)
	{
	}
	StateTransitionsByChar(StateTransitionsByChar&& inMove) noexcept
	: c(inMove.c)
	, nextStateByInitialState(inMove.nextStateByInitialState)
	, numStates(inMove.numStates)
	{
		inMove.nextStateByInitialState = nullptr;
	}
	~StateTransitionsByChar()
	{
		if(nextStateByInitialState) { delete[] nextStateByInitialState; }
	}

	void operator=(StateTransitionsByChar&& inMove) noexcept
	{
		c = inMove.c;
		nextStateByInitialState = inMove.nextStateByInitialState;
		numStates = inMove.numStates;
		inMove.nextStateByInitialState = nullptr;
	}

	bool operator<(const StateTransitionsByChar& right) const
	{
		WAVM_ASSERT(numStates == right.numStates);
		return memcmp(nextStateByInitialState,
					  right.nextStateByInitialState,
					  sizeof(StateIndex) * numStates)
			   < 0;
	}
	bool operator!=(const StateTransitionsByChar& right) const
	{
		WAVM_ASSERT(numStates == right.numStates);
		return memcmp(nextStateByInitialState,
					  right.nextStateByInitialState,
					  sizeof(StateIndex) * numStates)
			   != 0;
	}
};

NFA::Machine::Machine(Builder* builder)
{
	// Convert the NFA constructed by the builder to a DFA.
	std::vector<DFAState> dfaStates = convertToDFA(builder);
	WAVM_ASSERT(dfaStates.size() <= internalMaxStates);
	delete builder;

	Timing::Timer timer;

	// Transpose the [state][character] transition map to [character][state].
	std::vector<StateTransitionsByChar> stateTransitionsByChar;
	for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
	{
		stateTransitionsByChar.push_back(StateTransitionsByChar((U8)charIndex, dfaStates.size()));
		stateTransitionsByChar[charIndex].nextStateByInitialState
			= new StateIndex[dfaStates.size()];
		for(Uptr stateIndex = 0; stateIndex < dfaStates.size(); ++stateIndex)
		{
			stateTransitionsByChar[charIndex].nextStateByInitialState[stateIndex]
				= dfaStates[stateIndex].nextStateByChar[charIndex];
		}
	}

	// Sort the [character][state] transition map by the [state] column.
	numStates = dfaStates.size();
	std::sort(stateTransitionsByChar.begin(), stateTransitionsByChar.end());

	// Build a minimal set of character equivalence classes that have the same transition across all
	// states.
	U8 characterToClassMap[256];
	U8 representativeCharsByClass[256];
	numClasses = 1;
	characterToClassMap[stateTransitionsByChar[0].c] = 0;
	representativeCharsByClass[0] = stateTransitionsByChar[0].c;
	for(Uptr charIndex = 1; charIndex < stateTransitionsByChar.size(); ++charIndex)
	{
		if(stateTransitionsByChar[charIndex] != stateTransitionsByChar[charIndex - 1])
		{
			representativeCharsByClass[numClasses] = stateTransitionsByChar[charIndex].c;
			++numClasses;
		}
		characterToClassMap[stateTransitionsByChar[charIndex].c] = U8(numClasses - 1);
	}

	// Build a [charClass][state] transition map.
	stateAndOffsetToNextStateMap = new InternalStateIndex[numClasses * numStates];
	for(Uptr classIndex = 0; classIndex < numClasses; ++classIndex)
	{
		for(Uptr stateIndex = 0; stateIndex < numStates; ++stateIndex)
		{
			stateAndOffsetToNextStateMap[stateIndex + classIndex * numStates] = InternalStateIndex(
				dfaStates[stateIndex].nextStateByChar[representativeCharsByClass[classIndex]]);
		}
	}

	// Build a map from character index to offset into [charClass][initialState] transition map.
	WAVM_ASSERT((numClasses - 1) * (numStates - 1) <= UINT32_MAX);
	for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
	{ charToOffsetMap[charIndex] = U32(numStates * characterToClassMap[charIndex]); }

	Timing::logTimer("reduced DFA character classes", timer);
	Log::printf(Log::metrics, "  reduced DFA character classes to %" WAVM_PRIuPTR "\n", numClasses);
}

NFA::Machine::~Machine()
{
	if(stateAndOffsetToNextStateMap)
	{
		delete[] stateAndOffsetToNextStateMap;
		stateAndOffsetToNextStateMap = nullptr;
	}
}

void NFA::Machine::moveFrom(Machine&& inMachine) noexcept
{
	memcpy(charToOffsetMap, inMachine.charToOffsetMap, sizeof(charToOffsetMap));
	stateAndOffsetToNextStateMap = inMachine.stateAndOffsetToNextStateMap;
	inMachine.stateAndOffsetToNextStateMap = nullptr;
	numClasses = inMachine.numClasses;
	numStates = inMachine.numStates;
}

static char nibbleToHexChar(U8 value) { return value < 10 ? ('0' + value) : 'a' + value - 10; }

static std::string escapeString(const std::string& string)
{
	std::string result;
	for(Uptr charIndex = 0; charIndex < string.size(); ++charIndex)
	{
		auto c = string[charIndex];
		if(c == '\\') { result += "\\\\"; }
		else if(c == '\"')
		{
			result += "\\\"";
		}
		else if(c == '\n')
		{
			result += "\\n";
		}
		else if(c < 0x20 || c > 0x7e)
		{
			result += "\\\\";
			result += nibbleToHexChar((c & 0xf0) >> 4);
			result += nibbleToHexChar((c & 0x0f) >> 0);
		}
		else
		{
			result += c;
		}
	}
	return result;
}

static std::string getGraphEdgeCharLabel(Uptr charIndex)
{
	switch(charIndex)
	{
	case '^': return "\\^";
	case '\f': return "\\f";
	case '\r': return "\\r";
	case '\n': return "\\n";
	case '\t': return "\\t";
	case '\'': return "\\\'";
	case '\"': return "\\\"";
	case '\\': return "\\\\";
	case '-': return "\\-";
	default: return std::string(1, (char)charIndex);
	};
}

static std::string getGraphEdgeLabel(const CharSet& charSet)
{
	std::string edgeLabel;
	U8 numSetChars = 0;
	for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
	{
		if(charSet.contains((char)charIndex)) { ++numSetChars; }
	}
	if(numSetChars > 1) { edgeLabel += "["; }
	const bool isNegative = numSetChars >= 100;
	if(isNegative) { edgeLabel += "^"; }
	for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
	{
		if(charSet.contains((char)charIndex) != isNegative)
		{
			edgeLabel += getGraphEdgeCharLabel(charIndex);
			if(charIndex + 2 < 256 && charSet.contains((char)charIndex + 1) != isNegative
			   && charSet.contains((char)charIndex + 2) != isNegative)
			{
				edgeLabel += "-";
				charIndex += 2;
				while(charIndex + 1 < 256 && charSet.contains((char)charIndex + 1) != isNegative)
				{ ++charIndex; }
				edgeLabel += getGraphEdgeCharLabel(charIndex);
			}
		}
	}
	if(numSetChars > 1) { edgeLabel += "]"; }
	return edgeLabel;
}

std::string NFA::dumpNFAGraphViz(const Builder* builder)
{
	std::string result;
	result += "digraph {\n";
	HashSet<StateIndex> terminalStates;
	for(Uptr stateIndex = 0; stateIndex < builder->nfaStates.size(); ++stateIndex)
	{
		const NFAState& nfaState = builder->nfaStates[stateIndex];

		result += "state" + std::to_string(stateIndex) + "[shape=square label=\""
				  + std::to_string(stateIndex) + "\"];\n";

		for(const auto& statePredicatePair : nfaState.nextStateToPredicateMap)
		{
			std::string edgeLabel = getGraphEdgeLabel(statePredicatePair.value);
			std::string nextStateName = statePredicatePair.key < 0
											? "terminal" + std::to_string(-statePredicatePair.key)
											: "state" + std::to_string(statePredicatePair.key);
			result += "state" + std::to_string(stateIndex) + " -> " + nextStateName + "[label=\""
					  + escapeString(edgeLabel) + "\"];\n";
			if(statePredicatePair.key < 0) { terminalStates.add(statePredicatePair.key); }
		}

		for(auto epsilonNextState : nfaState.epsilonNextStates)
		{
			std::string nextStateName = epsilonNextState < 0
											? "terminal" + std::to_string(-epsilonNextState)
											: "state" + std::to_string(epsilonNextState);
			result += "state" + std::to_string(stateIndex) + " -> " + nextStateName
					  + "[label=\"&epsilon;\"];\n";
		}
	}
	for(auto terminalState : terminalStates)
	{
		result += "terminal" + std::to_string(-terminalState) + "[shape=octagon label=\""
				  + std::to_string(maximumTerminalStateIndex - terminalState) + "\"];\n";
	}
	result += "}\n";
	return result;
}

std::string NFA::Machine::dumpDFAGraphViz() const
{
	std::string result;
	result += "digraph {\n";
	HashSet<StateIndex> terminalStates;

	CharSet* classCharSets = new(alloca(sizeof(CharSet) * numClasses)) CharSet[numClasses];
	for(Uptr charIndex = 0; charIndex < 256; ++charIndex)
	{
		const Uptr classIndex = charToOffsetMap[charIndex] / numStates;
		classCharSets[classIndex].add((U8)charIndex);
	}

	{
		HashMap<StateIndex, CharSet> transitions;
		for(Uptr classIndex = 0; classIndex < numClasses; ++classIndex)
		{
			const InternalStateIndex nextState
				= stateAndOffsetToNextStateMap[0 + classIndex * numStates];
			CharSet& transitionPredicate = transitions.getOrAdd(nextState, CharSet{});
			transitionPredicate = classCharSets[classIndex] | transitionPredicate;
		}

		Uptr startIndex = 0;
		for(auto transitionPair : transitions)
		{
			if((transitionPair.key & ~edgeDoesntConsumeInputFlag) != unmatchedCharacterTerminal)
			{
				result += "start" + std::to_string(startIndex) + "[shape=triangle label=\"\"];\n";

				std::string edgeLabel = getGraphEdgeLabel(transitionPair.value);
				std::string nextStateName
					= transitionPair.key < 0
						  ? "terminal"
								+ std::to_string(
									-(transitionPair.key & ~edgeDoesntConsumeInputFlag))
						  : "state" + std::to_string(transitionPair.key);
				result += "start" + std::to_string(startIndex) + " -> " + nextStateName
						  + "[label=\""
						  + (transitionPair.key < 0
									 && (transitionPair.key & edgeDoesntConsumeInputFlag) != 0
								 ? "&epsilon; "
								 : "")
						  + escapeString(edgeLabel) + "\"];\n";

				if(transitionPair.key < 0)
				{
					terminalStates.add(
						StateIndex(transitionPair.key & ~edgeDoesntConsumeInputFlag));
				}

				++startIndex;
			}
		}
	}

	for(Uptr stateIndex = 1; stateIndex < numStates; ++stateIndex)
	{
		result += "state" + std::to_string(stateIndex) + "[shape=square label=\""
				  + std::to_string(stateIndex) + "\"];\n";

		HashMap<StateIndex, CharSet> transitions;
		for(Uptr classIndex = 0; classIndex < numClasses; ++classIndex)
		{
			const InternalStateIndex nextState
				= stateAndOffsetToNextStateMap[stateIndex + classIndex * numStates];
			CharSet& transitionPredicate = transitions.getOrAdd(nextState, CharSet{});
			transitionPredicate = classCharSets[classIndex] | transitionPredicate;
		}

		for(auto transitionPair : transitions)
		{
			if((transitionPair.key & ~edgeDoesntConsumeInputFlag) != unmatchedCharacterTerminal)
			{
				std::string edgeLabel = getGraphEdgeLabel(transitionPair.value);
				std::string nextStateName
					= transitionPair.key < 0
						  ? "terminal"
								+ std::to_string(
									-(transitionPair.key & ~edgeDoesntConsumeInputFlag))
						  : "state" + std::to_string(transitionPair.key);
				result += "state" + std::to_string(stateIndex) + " -> " + nextStateName
						  + "[label=\""
						  + (transitionPair.key < 0
									 && (transitionPair.key & edgeDoesntConsumeInputFlag) != 0
								 ? "&epsilon; "
								 : "")
						  + escapeString(edgeLabel) + "\"];\n";

				if(transitionPair.key < 0) { terminalStates.add(transitionPair.key); }
			}
		}
	}
	for(auto terminalState : terminalStates)
	{
		result += "terminal" + std::to_string(-(terminalState & ~edgeDoesntConsumeInputFlag))
				  + "[shape=octagon label=\""
				  + std::to_string(maximumTerminalStateIndex
								   - (terminalState & ~edgeDoesntConsumeInputFlag))
				  + "\"];\n";
	}
	result += "}\n";
	return result;
}
