#pragma once

#include "WAVM/Inline/BasicTypes.h"
#include "WAVM/NFA/NFA.h"

namespace WAVM { namespace RegExp {
	// Parses a regular expression from a string, and adds a recognizer for it to the given NFA
	// The recognizer will start from initialState, and end in finalState when the regular
	// expression has been completely matched.
	WAVM_API void addToNFA(const char* regexpString,
						   NFA::Builder* nfaBuilder,
						   NFA::StateIndex initialState,
						   NFA::StateIndex finalState);
}}
