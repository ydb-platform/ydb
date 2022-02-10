/** \file
 * While the C runtime does not need to model the state of
 * multiple lexers and parsers in the same way as the Java runtime does
 * it is no overhead to reflect that model. In fact the
 * C runtime has always been able to share recognizer state.
 *
 * This 'class' therefore defines all the elements of a recognizer
 * (either lexer, parser or tree parser) that are need to
 * track the current recognition state. Multiple recognizers
 * may then share this state, for instance when one grammar
 * imports another.
 */

#ifndef	_ANTLR3_RECOGNIZER_SHARED_STATE_HPP
#define	_ANTLR3_RECOGNIZER_SHARED_STATE_HPP

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

/** All the data elements required to track the current state
 *  of any recognizer (lexer, parser, tree parser).
 * May be share between multiple recognizers such that
 * grammar inheritance is easily supported.
 */
template<class ImplTraits, class StreamType>
class RecognizerSharedState  : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename StreamType::UnitType TokenType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	
	typedef typename ComponentTypeFinder<ImplTraits, StreamType>::ComponentType  ComponentType;
	typedef typename ImplTraits::template RewriteStreamType< ComponentType > RewriteStreamType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::TokenSourceType TokenSourceType;
	typedef typename ImplTraits::template ExceptionBaseType<StreamType> ExceptionBaseType;
	typedef typename ImplTraits::BitsetType BitsetType;
	typedef typename ImplTraits::BitsetListType BitsetListType;

	typedef typename ImplTraits::TreeAdaptorType TreeAdaptorType;

	typedef typename AllocPolicyType::template StackType< BitsetListType > FollowingType;
	typedef typename AllocPolicyType::template StackType< typename ImplTraits::InputStreamType* > InputStreamsType;
	typedef InputStreamsType StreamsType;
	typedef typename AllocPolicyType::template VectorType<RewriteStreamType> RewriteStreamsType;

	typedef IntTrie<ImplTraits, ANTLR_MARKER> RuleListType;
	typedef IntTrie<ImplTraits, std::shared_ptr<RuleListType>> RuleMemoType;

private:
	/** Points to the first in a possible chain of exceptions that the
     *  recognizer has discovered.
     */
    ExceptionBaseType*			m_exception;


    /** Track the set of token types that can follow any rule invocation.
     *  Stack structure, to support: List<BitSet>.
     */
    FollowingType		m_following;

    /** Track around a hint from the creator of the recognizer as to how big this
     *  thing is going to get, as the actress said to the bishop. This allows us
     *  to tune hash tables accordingly. This might not be the best place for this
     *  in the end but we will see.
     */
    ANTLR_UINT32		m_sizeHint;


    /** If set to true then the recognizer has an exception
     * condition (this is tested by the generated code for the rules of
     * the grammar).
     */
    bool				m_error;


    /** This is true when we see an error and before having successfully
     *  matched a token.  Prevents generation of more than one error message
     *  per error.
     */
    bool				m_errorRecovery;

	/** In lieu of a return value, this indicates that a rule or token
     *  has failed to match.  Reset to false upon valid token match.
     */
    bool				m_failed;

	/*
	Instead of allocating CommonTokenType, we do it in the stack. hence we need a null indicator
	*/
	bool				m_token_present;

    /** The index into the input stream where the last error occurred.
     * 	This is used to prevent infinite loops where an error is found
     *  but no token is consumed during recovery...another error is found,
     *  ad nauseam.  This is a failsafe mechanism to guarantee that at least
     *  one token/tree node is consumed for two errors.
     */
    ANTLR_MARKER		m_lastErrorIndex;

    /** When the recognizer terminates, the error handling functions
     *  will have incremented this value if any error occurred (that was displayed). It can then be
     *  used by the grammar programmer without having to use static globals.
     */
    ANTLR_UINT32		m_errorCount;

    /** If 0, no backtracking is going on.  Safe to exec actions etc...
     *  If >0 then it's the level of backtracking.
     */
    ANTLR_INT32			m_backtracking;

    /** ANTLR3_VECTOR of ANTLR3_LIST for rule memoizing.
     *  Tracks  the stop token index for each rule.  ruleMemo[ruleIndex] is
     *  the memoization table for ruleIndex.  For key ruleStartIndex, you
     *  get back the stop token for associated rule or MEMO_RULE_FAILED.
     *
     *  This is only used if rule memoization is on.
     */
    RuleMemoType*		m_ruleMemo;

    /** Pointer to an array of token names
     *  that are generally useful in error reporting. The generated parsers install
     *  this pointer. The table it points to is statically allocated as 8 bit ascii
     *  at parser compile time - grammar token names are thus restricted in character
     *  sets, which does not seem to terrible.
     */
    ANTLR_UINT8**		m_tokenNames;

    /** The goal of all lexer rules/methods is to create a token object.
     *  This is an instance variable as multiple rules may collaborate to
     *  create a single token.  For example, NUM : INT | FLOAT ;
     *  In this case, you want the INT or FLOAT rule to set token and not
     *  have it reset to a NUM token in rule NUM.
     */
    CommonTokenType		m_token;

    /** A lexer is a source of tokens, produced by all the generated (or
     *  hand crafted if you like) matching rules. As such it needs to provide
     *  a token source interface implementation. For others, this will become a empty class
     */
    TokenSourceType*	m_tokSource;

    /** The channel number for the current token
     */
    ANTLR_UINT32			m_channel;

    /** The token type for the current token
     */
    ANTLR_UINT32			m_type;

    /** The input line (where it makes sense) on which the first character of the current
     *  token resides.
     */
    ANTLR_INT32			m_tokenStartLine;

    /** The character position of the first character of the current token
     *  within the line specified by tokenStartLine
     */
    ANTLR_INT32		m_tokenStartCharPositionInLine;

    /** What character index in the stream did the current token start at?
     *  Needed, for example, to get the text for current token.  Set at
     *  the start of nextToken.
     */
    ANTLR_MARKER		m_tokenStartCharIndex;

    /** Text for the current token. This can be overridden by setting this
     *  variable directly or by using the SETTEXT() macro (preferred) in your
     *  lexer rules.
     */
    StringType			m_text;

    /** Input stream stack, which allows the C programmer to switch input streams
     *  easily and allow the standard nextToken() implementation to deal with it
     *  as this is a common requirement.
     */
    InputStreamsType	m_streams;

    /** Tree adaptor drives an AST trie construction.
     *  Is shared between multiple imported grammars.
     */
    TreeAdaptorType*    m_treeAdaptor;

public:
	RecognizerSharedState();
	ExceptionBaseType* get_exception() const;
	FollowingType& get_following();
	ANTLR_UINT32 get_sizeHint() const;
	bool get_error() const;
	bool get_errorRecovery() const;
	bool get_failed() const;
	bool get_token_present() const;
	ANTLR_MARKER get_lastErrorIndex() const;
	ANTLR_UINT32 get_errorCount() const;
	ANTLR_INT32 get_backtracking() const;
	RuleMemoType* get_ruleMemo() const;
	ANTLR_UINT8** get_tokenNames() const;
	ANTLR_UINT8* get_tokenName( ANTLR_UINT32 i ) const;
	CommonTokenType* get_token();
	TokenSourceType* get_tokSource() const;
	ANTLR_UINT32& get_channel();
	ANTLR_UINT32 get_type() const;
	ANTLR_INT32 get_tokenStartLine() const;
	ANTLR_INT32 get_tokenStartCharPositionInLine() const;
	ANTLR_MARKER get_tokenStartCharIndex() const;
	StringType& get_text();
	InputStreamsType& get_streams();
	TreeAdaptorType* get_treeAdaptor() const;
	
	void  set_following( const FollowingType& following );
	void  set_sizeHint( ANTLR_UINT32 sizeHint );
	void  set_error( bool error );
	void  set_errorRecovery( bool errorRecovery );
	void  set_failed( bool failed );
	void  set_token_present(bool token_present);
	void  set_lastErrorIndex( ANTLR_MARKER lastErrorIndex );
	void  set_errorCount( ANTLR_UINT32 errorCount );
	void  set_backtracking( ANTLR_INT32 backtracking );
	void  set_ruleMemo( RuleMemoType* ruleMemo );
	void  set_tokenNames( ANTLR_UINT8** tokenNames );
	void  set_tokSource( TokenSourceType* tokSource );
	void  set_channel( ANTLR_UINT32 channel );
	void  set_exception( ExceptionBaseType* exception );
	void  set_type( ANTLR_UINT32 type );
	void  set_token( const CommonTokenType* tok);
	void  set_tokenStartLine( ANTLR_INT32 tokenStartLine );
	void  set_tokenStartCharPositionInLine( ANTLR_INT32 tokenStartCharPositionInLine );
	void  set_tokenStartCharIndex( ANTLR_MARKER tokenStartCharIndex );
	void  set_text( const StringType& text );
	void  set_streams( const InputStreamsType& streams );
	void  set_treeAdaptor( TreeAdaptorType* adaptor );
	
	void inc_errorCount();
	void inc_backtracking();
	void dec_backtracking();
};

}

#include "antlr3recognizersharedstate.inl"

#endif


