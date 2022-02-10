/** \file
 * Declarations for all the antlr3 C runtime interfaces/classes. This
 * allows the structures that define the interfaces to contain pointers to
 * each other without trying to sort out the cyclic interdependencies that
 * would otherwise result.
 */
#ifndef	_ANTLR3_INTERFACES_HPP
#define	_ANTLR3_INTERFACES_HPP

// [The "BSD licence"]
// Copyright (c) 2005-2009 Gokulakannan Somasundaram, ElectronDB
//
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions
// are met:
// 1. Redistributions of source code must retain the above copyright
//    notice, this list of conditions and the following disclaimer.
// 2. Redistributions in binary form must reproduce the above copyright
//    notice, this list of conditions and the following disclaimer in the
//    documentation and/or other materials provided with the distribution.
// 3. The name of the author may not be used to endorse or promote products
//    derived from this software without specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE AUTHOR ``AS IS'' AND ANY EXPRESS OR
// IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
// OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
// IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY DIRECT, INDIRECT,
// INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
// NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
// THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

namespace antlr3 {

// Definitions that indicate the encoding scheme character streams and strings etc
enum Encoding
{
	ENC_8BIT      = 4               /// General latin-1 or other 8 bit encoding scheme such as straight ASCII
	, ENC_UTF8    = 8               /// UTF-8 encoding scheme
	, ENC_UTF16   = 16              /// UTF-16 encoding scheme (which also covers UCS2 as that does not have surrogates)
	, ENC_UTF16BE
	, ENC_UTF16LE
	, ENC_UTF32   = 32              /// UTF-32 encoding scheme (basically straight 32 bit)
	, ENC_UTF32BE
	, ENC_UTF32LE
	, ENC_EBCDIC  = 64              /// Input is 8 bit EBCDIC (which we convert to 8 bit ASCII on the fly
};

enum ChannelType
{
	TOKEN_DEFAULT_CHANNEL = 0           /// Default channel for a token
	, HIDDEN	= 99                    /// Reserved channel number for a HIDDEN token - a token that is hidden from the parser
};

/// Pointer to an instantiation of 'class' #ANTLR3_EXCEPTION
/// \ingroup ANTLR3_EXCEPTION
///
enum ExceptionType
{
	/** Indicates that the recognizer received a token
	 *  in the input that was not predicted.
	 */
	RECOGNITION_EXCEPTION = 0
	/** Indicates that the recognizer was expecting one token and found a
	 *  a different one.
	 */
	, MISMATCHED_TOKEN_EXCEPTION

	/** Recognizer could not find a valid alternative from the input
	 */
	, NO_VIABLE_ALT_EXCEPTION

	/* Character in a set was not found
	 */
	, MISMATCHED_SET_EXCEPTION

	/* A rule predicting at least n elements found less than that,
	 * such as: WS: " "+;
	 */
	, EARLY_EXIT_EXCEPTION

	, FAILED_PREDICATE_EXCEPTION

	, MISMATCHED_TREE_NODE_EXCEPTION

	, REWRITE_EARLY_EXCEPTION

	, UNWANTED_TOKEN_EXCEPTION

	, MISSING_TOKEN_EXCEPTION
};

template<class ImplTraits, class SuperType>
class IntStream;

/// Pointer to an instantiation of 'class' #ANTLR3_RECOGNIZER_SHARED_STATE
/// \ingroup ANTLR3_RECOGNIZER_SHARED_STATE
///
template<class ImplTraits, class SuperType>
class RecognizerSharedState;

/// Pointer to an instantiation of 'class' #ANTLR3_BITSET_LIST
/// \ingroup ANTLR3_BITSET_LIST
///
template<class AllocatorType>
class BitsetList;

/// Pointer to an instantiation of 'class' #ANTLR3_BITSET
/// \ingroup ANTLR3_BITSET
///
template<class AllocatorType>
class Bitset;

/// Pointer to an instantiation of 'class' #ANTLR3_COMMON_TOKEN
/// \ingroup ANTLR3_COMMON_TOKEN
///
template<class ImplTraits>
class CommonToken;

template<class ImplTraits, ExceptionType Ex, class StreamType>
class ANTLR_Exception;

/// Pointer to an instantiation of 'class' #ANTLR3_TOPO
/// \ingroup ANTLR3_TOPO
///
template<class AllocPolicyType>
class Topo;

/// Pointer to an instantiation of 'class' #ANTLR3_INPUT_STREAM
/// \ingroup ANTLR3_INPUT_STREAM
///
template<class ImplTraits>
class InputStream;

/// Pointer to an instantiation of 'class' #ANTLR3_LEX_STATE
/// \ingroup ANTLR3_LEX_STATE
///
template<class ImplTraits>
class LexState;

/// Pointer to an instantiation of 'class' #ANTLR3_TOKEN_SOURCE
/// \ingroup ANTLR3_TOKEN_SOURCE
///
template<class ImplTraits>
class TokenSource;

/// Pointer to an instantiation of 'class' #ANTLR3_TOKEN_STREAM
/// \ingroup ANTLR3_TOKEN_STREAM
///
template<class ImplTraits>
class TokenStream;

/// Pointer to an instantiation of 'class' #ANTLR3_COMMON_TOKEN_STREAM
/// \ingroup ANTLR3_COMMON_TOKEN_STREAM
///
template<class ImplTraits>
class CommonTokenStream;

/// Pointer to an instantiation of 'class' #ANTLR3_CYCLIC_DFA
/// \ingroup ANTLR3_CYCLIC_DFA
///
template<class ImplTraits, class ComponentType>
class CyclicDFA;

/// Pointer to an instantiation of 'class' #ANTLR3_LEXER
/// \ingroup ANTLR3_LEXER
///
template<class ImplTraits>
class Lexer;

/// Pointer to an instantiation of 'class' #ANTLR3_PARSER
/// \ingroup ANTLR3_PARSER
///
template<class ImplTraits>
class Parser;

/// Pointer to an instantiation of 'class' #ANTLR3_BASE_TREE
/// \ingroup ANTLR3_BASE_TREE
///
template<class ImplTraits>
class BaseTree;

/// Pointer to an instantiation of 'class' #ANTLR3_COMMON_TREE
/// \ingroup ANTLR3_COMMON_TREE
///
template<class ImplTraits>
class CommonTree;

/// Pointer to an instantiation of 'class' #ANTLR3_PARSE_TREE
/// \ingroup ANTLR3_PARSE_TREE
///
template<class ImplTraits>
class ParseTree;

/// Pointer to an instantiation of 'class' #ANTLR3_TREE_NODE_STREAM
/// \ingroup ANTLR3_TREE_NODE_STREAM
///
template<class ImplTraits>
class TreeNodeStream;

/// Pointer to an instantiation of 'class' #ANTLR3_COMMON_TREE_NODE_STREAM
/// \ingroup ANTLR3_COMMON_TREE_NODE_STREAM
///
template<class ImplTraits>
class CommonTreeNodeStream;

/// Pointer to an instantiation of 'class' #ANTLR3_TREE_WALK_STATE
/// \ingroup ANTLR3_TREE_WALK_STATE
///
template<class ImplTraits>
class TreeWalkState;

/// Pointer to an instantiation of 'class' #ANTLR3_COMMON_TREE_ADAPTOR
/// \ingroup ANTLR3_COMMON_TREE_ADAPTOR
///
template<class ImplTraits>
class CommonTreeAdaptor;

/// Pointer to an instantiation of 'class' #ANTLR3_TREE_PARSER
/// \ingroup ANTLR3_TREE_PARSER
///
template<class ImplTraits>
class TreeParser;

/// Pointer to an instantiation of 'class' #ANTLR3_INT_TRIE
/// \ingroup ANTLR3_INT_TRIE
///
template< class DataType, class AllocPolicyType >
class IntTrie;

/// Pointer to an instantiation of 'class' #ANTLR3_REWRITE_RULE_ELEMENT_STREAM
/// \ingroup ANTLR3_REWRITE_RULE_ELEMENT_STREAM
///
template<class ImplTraits, class SuperType>
class RewriteRuleElementStream;

template<class ImplTraits>
class RewriteRuleTokenStream;

template<class ImplTraits>
class RewriteRuleSubtreeStream;

template<class ImplTraits>
class RewriteRuleNodeStream;

/// Pointer to an instantiation of 'class' #ANTLR3_DEBUG_EVENT_LISTENER
/// \ingroup ANTLR3_DEBUG_EVENT_LISTENER
///
template<class ImplTraits>
class  DebugEventListener;

//A Class just used for forwarding other classes for simplifying class forwarding
//Logic: constructor is made simple
template<class A>
class ClassForwarder {};

template<bool b>
class BoolForwarder {};
class Empty {};

template<class ImplTraits, class StreamType>
class ComponentTypeFinder
{
};

template<class ImplTraits>
class ComponentTypeFinder< ImplTraits, typename ImplTraits::InputStreamType>
{
public:
	typedef typename ImplTraits::LexerType ComponentType;
};

template<class ImplTraits>
class ComponentTypeFinder< ImplTraits, typename ImplTraits::TokenStreamType>
{
public:
	typedef typename ImplTraits::ParserType ComponentType;
};

template<class ImplTraits>
class ComponentTypeFinder< ImplTraits, typename ImplTraits::TreeNodeStreamType>
{
public:
	typedef typename ImplTraits::TreeParserType ComponentType;
};

}

#endif
