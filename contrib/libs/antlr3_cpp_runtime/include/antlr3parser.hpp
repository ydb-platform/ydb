/** \file
 * Base implementation of an ANTLR3 parser.
 *
 *
 */
#ifndef	_ANTLR3_PARSER_HPP
#define	_ANTLR3_PARSER_HPP

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

/** This is the main interface for an ANTLR3 parser.
 */
template< class ImplTraits >
class Parser  :  public ImplTraits::template RecognizerType< typename ImplTraits::TokenStreamType >
{
public:
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::TokenStreamType  TokenStreamType;
	typedef typename TokenStreamType::IntStreamType  IntStreamType;
	typedef TokenStreamType StreamType;

	typedef typename ImplTraits::template RecognizerType< typename ImplTraits::TokenStreamType > RecognizerType;
	typedef typename RecognizerType::RecognizerSharedStateType RecognizerSharedStateType;

	typedef DebugEventListener<ImplTraits> DebugEventListenerType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef CommonTokenType TokenType;
	typedef typename ImplTraits::BitsetListType BitsetListType;
	typedef ANTLR_ExceptionBase<ImplTraits, TokenStreamType> ExceptionBaseType;
	typedef Empty TokenSourceType;

	typedef typename RecognizerSharedStateType::FollowingType FollowingType;
	typedef typename RecognizerSharedStateType::RuleMemoType RuleMemoType;
	typedef typename ImplTraits::DebugEventListenerType DebuggerType;

private:
    /** A provider of a tokenstream interface, for the parser to consume
     *  tokens from.
     */
    TokenStreamType*			m_tstream;

public:
	Parser( ANTLR_UINT32 sizeHint, RecognizerSharedStateType* state );
	Parser( ANTLR_UINT32 sizeHint, TokenStreamType* tstream, RecognizerSharedStateType* state );
	Parser( ANTLR_UINT32 sizeHint, TokenStreamType* tstream, DebugEventListenerType* dbg,
											RecognizerSharedStateType* state );
	TokenStreamType* get_tstream() const;
	TokenStreamType* get_input() const;
	IntStreamType* get_istream() const;
	RecognizerType* get_rec();

	//same as above. Just that get_istream exists for lexer, parser, treeparser
	//get_parser_istream exists only for parser, treeparser. So use it accordingly
	IntStreamType* get_parser_istream() const;

	/** A pointer to a function that installs a debugger object (it also
	 *  installs the debugging versions of the parser methods. This means that
	 *  a non debug parser incurs no overhead because of the debugging stuff.
	 */
	void	setDebugListener(DebugEventListenerType* dbg);

    /** A pointer to a function that installs a token stream
     * for the parser.
     */
    void	setTokenStream(TokenStreamType*);

    /** A pointer to a function that returns the token stream for this
     *  parser.
     */
    TokenStreamType*	getTokenStream();

	void exConstruct();
	TokenType*	getMissingSymbol( IntStreamType* istream, ExceptionBaseType* e,
								  ANTLR_UINT32	expectedTokenType, BitsetListType*	follow);

	void mismatch(ANTLR_UINT32 ttype, BitsetListType* follow);

    /** Pointer to a function that knows how to free resources of an ANTLR3 parser.
     */
	~Parser();

	void fillExceptionData( ExceptionBaseType* ex );
	void displayRecognitionError( ANTLR_UINT8** tokenNames, ExceptionBaseType* ex );

	//convenience functions exposed in .stg
	const RecognizerType* get_recognizer() const;
	RecognizerSharedStateType* get_psrstate() const;
	void set_psrstate(RecognizerSharedStateType* state);
	bool haveParsedRule(ANTLR_MARKER	ruleIndex);
	void memoize(ANTLR_MARKER	ruleIndex, ANTLR_MARKER	ruleParseStart);
	ANTLR_MARKER  index() const;
	bool hasException() const;
	ExceptionBaseType* get_exception() const;
	const CommonTokenType* matchToken( ANTLR_UINT32 ttype, BitsetListType* follow );
	void matchAnyToken();
	const FollowingType& get_follow_stack() const;
	void followPush( const BitsetListType& follow );
	void followPop();
	void precover();
	void preporterror();
	ANTLR_UINT32 LA(ANTLR_INT32 i);
	const CommonTokenType*  LT(ANTLR_INT32 k);
	void constructEx();
	void consume();
	ANTLR_MARKER mark();
	void rewind(ANTLR_MARKER marker);
	void rewindLast();
	void seek(ANTLR_MARKER index);
	bool get_perror_recovery() const;
	void set_perror_recovery( bool val );
	bool hasFailed() const;
	bool get_failedflag() const;
	void set_failedflag( bool failed );
	ANTLR_INT32 get_backtracking() const;
	void inc_backtracking();
	void dec_backtracking();
	CommonTokenType* recoverFromMismatchedSet(BitsetListType*	follow);
	bool	recoverFromMismatchedElement(BitsetListType*	follow);
	RuleMemoType* getRuleMemo() const;
	DebuggerType* get_debugger() const;
	TokenStreamType* get_strstream() const;
	void setRuleMemo(RuleMemoType* rulememo);

};

//Generic rule return value. Unlike the general ANTLR, this gets generated for
//every rule in the target. Handle rule exit here
template<class ImplTraits>
class RuleReturnValue
{
public:
	typedef typename ImplTraits::BaseParserType BaseParserType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;

	const CommonTokenType*		start;
	const CommonTokenType*		stop;

	RuleReturnValue(BaseParserType* psr = NULL );
	RuleReturnValue( const RuleReturnValue& val );
	RuleReturnValue& operator=( const RuleReturnValue& val );
	void call_start_placeholder(BaseParserType*);
	void call_stop_placeholder(BaseParserType*);
	RuleReturnValue& get_struct();
	~RuleReturnValue();
};

//This kind makes sure that whenever tokens are condensed into a rule,
//all the tokens except the start and stop tokens are deleted
template<class ImplTraits>
class RuleReturnValue_1 : public RuleReturnValue<ImplTraits>
{
public:
	typedef RuleReturnValue<ImplTraits> BaseType;
	typedef typename BaseType::BaseParserType BaseParserType;

	BaseParserType* parser;

	RuleReturnValue_1();
	RuleReturnValue_1( BaseParserType* psr);
	RuleReturnValue_1( const RuleReturnValue_1& val );
	void call_start_placeholder(BaseParserType*);  //its dummy here
	~RuleReturnValue_1();
};

}

#include "antlr3parser.inl"

#endif
