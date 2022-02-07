/** \file
 * Defines the basic structure to support recognizing by either a lexer,
 * parser, or tree parser.
 * \addtogroup BaseRecognizer
 * @{
 */
#ifndef	_ANTLR3_BASERECOGNIZER_HPP
#define	_ANTLR3_BASERECOGNIZER_HPP

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

/** \brief Base tracking context structure for all types of
 * recognizers.
 */
template< class ImplTraits, class StreamType >
class BaseRecognizer : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::AllocPolicyType	AllocPolicyType;
	typedef typename StreamType::IntStreamType	IntStreamType;
	typedef typename ComponentTypeFinder<ImplTraits, StreamType>::ComponentType  SuperType;
	typedef typename StreamType::UnitType		UnitType;
	typedef typename ImplTraits::template ExceptionBaseType<StreamType> ExceptionBaseType;
	typedef typename ImplTraits::BitsetType BitsetType;
	typedef typename ImplTraits::BitsetListType		BitsetListType;
	typedef typename ImplTraits::StringType	StringType;
	typedef typename ImplTraits::template RecognizerSharedStateType<StreamType>  RecognizerSharedStateType;
	typedef typename ImplTraits::DebugEventListenerType DebugEventListenerType;
	typedef typename ImplTraits::LexerType LexerType;
	typedef typename ImplTraits::ParserType ParserType;
	typedef typename ImplTraits::TreeParserType TreeParserType;

	typedef typename AllocPolicyType::template StackType<StringType>  StringStackType;
	typedef typename AllocPolicyType::template ListType<StringType>  StringListType;

private:
	/// A pointer to the shared recognizer state, such that multiple
	/// recognizers can use the same inputs streams and so on (in
	/// the case of grammar inheritance for instance.
	///
	RecognizerSharedStateType*		m_state;

	/// If set to something other than NULL, then this structure is
	/// points to an instance of the debugger interface. In general, the
	/// debugger is only referenced internally in recovery/error operations
	/// so that it does not cause overhead by having to check this pointer
	/// in every function/method
	///
	DebugEventListenerType*		m_debugger;


public:
	BaseRecognizer(ANTLR_UINT32 sizeHint, RecognizerSharedStateType* state);

	SuperType* get_super();
	RecognizerSharedStateType* get_state() const;
	DebugEventListenerType* get_debugger() const;
	void  set_state( RecognizerSharedStateType* state );
	void  set_debugger( DebugEventListenerType* debugger );

    /// Match current input symbol against ttype.  Upon error, do one token
	/// insertion or deletion if possible.
	/// To turn off single token insertion or deletion error
	/// recovery, override mismatchRecover() and have it call
	/// plain mismatch(), which does not recover.  Then any error
	/// in a rule will cause an exception and immediate exit from
	/// rule.  Rule would recover by resynchronizing to the set of
	/// symbols that can follow rule ref.
	///
    const UnitType*	match(ANTLR_UINT32 ttype, BitsetListType* follow);

	/// Consumes the next token, whatever it is, and resets the recognizer state
	/// so that it is not in error.
	///
	/// \param recognizer
	/// Recognizer context pointer
	///
    void	matchAny();

	/// function that decides if the token ahead of the current one is the
	/// one we were loking for, in which case the curernt one is very likely extraneous
	/// and can be reported that way.
	///
	bool mismatchIsUnwantedToken(IntStreamType* input, ANTLR_UINT32 ttype);

	/// function that decides if the current token is one that can logically
	/// follow the one we were looking for, in which case the one we were looking for is
	/// probably missing from the input.
	///
	bool mismatchIsMissingToken(IntStreamType* input, BitsetListType* follow);

    /// Factor out what to do upon token mismatch so tree parsers can behave
	/// differently.  Override and call mismatchRecover(input, ttype, follow)
	/// to get single token insertion and deletion.  Use this to turn off
	/// single token insertion and deletion. Override mismatchRecover
	/// to call this instead.
	///
	/// \remark mismatch only works for parsers and must be overridden for anything else.
	///
    void mismatch(ANTLR_UINT32 ttype, BitsetListType* follow);

    /// Report a recognition problem.
	///
	/// This method sets errorRecovery to indicate the parser is recovering
	/// not parsing.  Once in recovery mode, no errors are generated.
	/// To get out of recovery mode, the parser must successfully match
	/// a token (after a resync).  So it will go:
	///
	///		1. error occurs
	///		2. enter recovery mode, report error
	///		3. consume until token found in resynch set
	///		4. try to resume parsing
	///		5. next match() will reset errorRecovery mode
	///
	/// If you override, make sure to update errorCount if you care about that.
	///
    void	reportError();
	void	reportError( ClassForwarder<LexerType> );
	template<typename CompType>
	void	reportError( ClassForwarder<CompType> );

    /** Function that is called to display a recognition error message. You may
     *  override this function independently of (*reportError)() above as that function calls
     *  this one to do the actual exception printing.
     */
    void	displayRecognitionError(ANTLR_UINT8** tokenNames);

	/// Get number of recognition errors (lexer, parser, tree parser).  Each
	/// recognizer tracks its own number.  So parser and lexer each have
	/// separate count.  Does not count the spurious errors found between
	/// an error and next valid token match
	///
	/// \see reportError()
	///
	ANTLR_UINT32 getNumberOfSyntaxErrors();

    /** Function that recovers from an error found in the input stream.
     *  Generally, this will be a #ANTLR3_EXCEPTION_NOVIABLE_ALT but it could also
     *  be from a mismatched token that the (*match)() could not recover from.
     */
    void	recover();

    /** function that is a hook to listen to token consumption during error recovery.
     *  This is mainly used by the debug parser to send events to the listener.
     */
    void	beginResync();

    /** function that is a hook to listen to token consumption during error recovery.
     *  This is mainly used by the debug parser to send events to the listener.
     */
    void	endResync();

	/** function that is a hook to listen to token consumption during error recovery.
     *  This is mainly used by the debug parser to send events to the listener.
     */
    void	beginBacktrack(ANTLR_UINT32 level);

    /** function that is a hook to listen to token consumption during error recovery.
     *  This is mainly used by the debug parser to send events to the listener.
     */
    void	endBacktrack(ANTLR_UINT32 level, bool successful);

    /// Compute the error recovery set for the current rule.
	/// Documentation below is from the Java implementation.
	///
	/// During rule invocation, the parser pushes the set of tokens that can
	/// follow that rule reference on the stack; this amounts to
	/// computing FIRST of what follows the rule reference in the
	/// enclosing rule. This local follow set only includes tokens
	/// from within the rule; i.e., the FIRST computation done by
	/// ANTLR stops at the end of a rule.
	//
	/// EXAMPLE
	//
	/// When you find a "no viable alt exception", the input is not
	/// consistent with any of the alternatives for rule r.  The best
	/// thing to do is to consume tokens until you see something that
	/// can legally follow a call to r *or* any rule that called r.
	/// You don't want the exact set of viable next tokens because the
	/// input might just be missing a token--you might consume the
	/// rest of the input looking for one of the missing tokens.
	///
	/// Consider grammar:
	///
	/// a : '[' b ']'
	///   | '(' b ')'
	///   ;
	/// b : c '^' INT ;
	/// c : ID
	///   | INT
	///   ;
	///
	/// At each rule invocation, the set of tokens that could follow
	/// that rule is pushed on a stack.  Here are the various "local"
	/// follow sets:
	///
	/// FOLLOW(b1_in_a) = FIRST(']') = ']'
	/// FOLLOW(b2_in_a) = FIRST(')') = ')'
	/// FOLLOW(c_in_b) = FIRST('^') = '^'
	///
	/// Upon erroneous input "[]", the call chain is
	///
	/// a -> b -> c
	///
	/// and, hence, the follow context stack is:
	///
	/// depth  local follow set     after call to rule
	///   0         <EOF>                    a (from main())
	///   1          ']'                     b
	///   3          '^'                     c
	///
	/// Notice that ')' is not included, because b would have to have
	/// been called from a different context in rule a for ')' to be
	/// included.
	///
	/// For error recovery, we cannot consider FOLLOW(c)
	/// (context-sensitive or otherwise).  We need the combined set of
	/// all context-sensitive FOLLOW sets--the set of all tokens that
	/// could follow any reference in the call chain.  We need to
	/// resync to one of those tokens.  Note that FOLLOW(c)='^' and if
	/// we resync'd to that token, we'd consume until EOF.  We need to
	/// sync to context-sensitive FOLLOWs for a, b, and c: {']','^'}.
	/// In this case, for input "[]", LA(1) is in this set so we would
	/// not consume anything and after printing an error rule c would
	/// return normally.  It would not find the required '^' though.
	/// At this point, it gets a mismatched token error and throws an
	/// exception (since LA(1) is not in the viable following token
	/// set).  The rule exception handler tries to recover, but finds
	/// the same recovery set and doesn't consume anything.  Rule b
	/// exits normally returning to rule a.  Now it finds the ']' (and
	/// with the successful match exits errorRecovery mode).
	///
	/// So, you can see that the parser walks up call chain looking
	/// for the token that was a member of the recovery set.
	///
	/// Errors are not generated in errorRecovery mode.
	///
	/// ANTLR's error recovery mechanism is based upon original ideas:
	///
	/// "Algorithms + Data Structures = Programs" by Niklaus Wirth
	///
	/// and
	///
	/// "A note on error recovery in recursive descent parsers":
	/// http://portal.acm.org/citation.cfm?id=947902.947905
	///
	/// Later, Josef Grosch had some good ideas:
	///
	/// "Efficient and Comfortable Error Recovery in Recursive Descent
	/// Parsers":
	/// ftp://www.cocolab.com/products/cocktail/doca4.ps/ell.ps.zip
	///
	/// Like Grosch I implemented local FOLLOW sets that are combined
	/// at run-time upon error to avoid overhead during parsing.
	///
    BitsetType*	computeErrorRecoverySet();

    /// Compute the context-sensitive FOLLOW set for current rule.
	/// Documentation below is from the Java runtime.
	///
	/// This is the set of token types that can follow a specific rule
	/// reference given a specific call chain.  You get the set of
	/// viable tokens that can possibly come next (look ahead depth 1)
	/// given the current call chain.  Contrast this with the
	/// definition of plain FOLLOW for rule r:
	///
	///  FOLLOW(r)={x | S=>*alpha r beta in G and x in FIRST(beta)}
	///
	/// where x in T* and alpha, beta in V*; T is set of terminals and
	/// V is the set of terminals and non terminals.  In other words,
	/// FOLLOW(r) is the set of all tokens that can possibly follow
	/// references to r in///any* sentential form (context).  At
	/// runtime, however, we know precisely which context applies as
	/// we have the call chain.  We may compute the exact (rather
	/// than covering superset) set of following tokens.
	///
	/// For example, consider grammar:
	///
	/// stat : ID '=' expr ';'      // FOLLOW(stat)=={EOF}
	///      | "return" expr '.'
	///      ;
	/// expr : atom ('+' atom)* ;   // FOLLOW(expr)=={';','.',')'}
	/// atom : INT                  // FOLLOW(atom)=={'+',')',';','.'}
	///      | '(' expr ')'
	///      ;
	///
	/// The FOLLOW sets are all inclusive whereas context-sensitive
	/// FOLLOW sets are precisely what could follow a rule reference.
	/// For input input "i=(3);", here is the derivation:
	///
	/// stat => ID '=' expr ';'
	///      => ID '=' atom ('+' atom)* ';'
	///      => ID '=' '(' expr ')' ('+' atom)* ';'
	///      => ID '=' '(' atom ')' ('+' atom)* ';'
	///      => ID '=' '(' INT ')' ('+' atom)* ';'
	///      => ID '=' '(' INT ')' ';'
	///
	/// At the "3" token, you'd have a call chain of
	///
	///   stat -> expr -> atom -> expr -> atom
	///
	/// What can follow that specific nested ref to atom?  Exactly ')'
	/// as you can see by looking at the derivation of this specific
	/// input.  Contrast this with the FOLLOW(atom)={'+',')',';','.'}.
	///
	/// You want the exact viable token set when recovering from a
	/// token mismatch.  Upon token mismatch, if LA(1) is member of
	/// the viable next token set, then you know there is most likely
	/// a missing token in the input stream.  "Insert" one by just not
	/// throwing an exception.
	///
    BitsetType*	computeCSRuleFollow();

    /// Compute the current followset for the input stream.
	///
    BitsetType*	combineFollows(bool exact);

    /// Attempt to recover from a single missing or extra token.
	///
	/// EXTRA TOKEN
	///
	/// LA(1) is not what we are looking for.  If LA(2) has the right token,
	/// however, then assume LA(1) is some extra spurious token.  Delete it
	/// and LA(2) as if we were doing a normal match(), which advances the
	/// input.
	///
	/// MISSING TOKEN
	///
	/// If current token is consistent with what could come after
	/// ttype then it is ok to "insert" the missing token, else throw
	/// exception For example, Input "i=(3;" is clearly missing the
	/// ')'.  When the parser returns from the nested call to expr, it
	/// will have call chain:
	///
	///    stat -> expr -> atom
	///
	/// and it will be trying to match the ')' at this point in the
	/// derivation:
	///
	///       => ID '=' '(' INT ')' ('+' atom)* ';'
	///                          ^
	/// match() will see that ';' doesn't match ')' and report a
	/// mismatched token error.  To recover, it sees that LA(1)==';'
	/// is in the set of tokens that can follow the ')' token
	/// reference in rule atom.  It can assume that you forgot the ')'.
	///
	/// The exception that was passed in, in the java implementation is
	/// sorted in the recognizer exception stack in the C version. To 'throw' it we set the
	/// error flag and rules cascade back when this is set.
	///
    const UnitType* recoverFromMismatchedToken( ANTLR_UINT32	ttype, BitsetListType*	follow);

    /** Function that recovers from a mismatched set in the token stream, in a similar manner
     *  to (*recoverFromMismatchedToken)
     */
    const UnitType* recoverFromMismatchedSet(BitsetListType*	follow);

    /** common routine to handle single token insertion for recovery functions.
     */
	/// This code is factored out from mismatched token and mismatched set
	///  recovery.  It handles "single token insertion" error recovery for
	/// both.  No tokens are consumed to recover from insertions.  Return
	/// true if recovery was possible else return false.
	///
    bool	recoverFromMismatchedElement(BitsetListType*	follow);

    /** function that consumes input until the next token matches
     *  the given token.
     */
    void	consumeUntil(ANTLR_UINT32   tokenType);

    /** function that consumes input until the next token matches
     *  one in the given set.
     */
    void	consumeUntilSet(BitsetType*	set);

    /** function that returns an ANTLR3_LIST of the strings that identify
     *  the rules in the parser that got you to this point. Can be overridden by installing your
     *	own function set.
     *
     * \todo Document how to override invocation stack functions.
     */
	StringStackType	getRuleInvocationStack();
	StringStackType	getRuleInvocationStackNamed(ANTLR_UINT8*    name);

    /** function that converts an ANLR3_LIST of tokens to an ANTLR3_LIST of
     *  string token names. As this is mostly used in string template processing it may not be useful
     *  in the C runtime.
     */
    StringListType	toStrings( const StringListType& );

    /** function to return whether the rule has parsed input starting at the supplied
     *  start index before. If the rule has not parsed input starting from the supplied start index,
     *  then it will return ANTLR3_MEMO_RULE_UNKNOWN. If it has parsed from the suppled start point
     *  then it will return the point where it last stopped parsing after that start point.
     */
    ANTLR_MARKER	getRuleMemoization( ANTLR_INTKEY	ruleIndex,
												ANTLR_MARKER	ruleParseStart);

    /** function that determines whether the rule has parsed input at the current index
     *  in the input stream
     */
    bool	alreadyParsedRule(ANTLR_MARKER	ruleIndex);

    /** Function that records whether the rule has parsed the input at a
     *  current position successfully or not.
     */
    void	memoize(ANTLR_MARKER	ruleIndex,
								ANTLR_MARKER	ruleParseStart);

	/// Function that returns the current input symbol.
    /// The is placed into any label for the associated token ref; e.g., x=ID.  Token
	/// and tree parsers need to return different objects. Rather than test
	/// for input stream type or change the IntStream interface, I use
	/// a simple method to ask the recognizer to tell me what the current
	/// input symbol is.
	///
	/// This is ignored for lexers and the lexer implementation of this
	/// function should return NULL.
	///
	const UnitType*	getCurrentInputSymbol(IntStreamType* istream);
	const UnitType*	getCurrentInputSymbol(IntStreamType* istream, ClassForwarder<LexerType>);
	const UnitType*	getCurrentInputSymbol(IntStreamType* istream, ClassForwarder<ParserType>);
	const UnitType*	getCurrentInputSymbol(IntStreamType* istream, ClassForwarder<TreeParserType>);

	/// Conjure up a missing token during error recovery.
	///
	/// The recognizer attempts to recover from single missing
	/// symbols. But, actions might refer to that missing symbol.
	/// For example, x=ID {f($x);}. The action clearly assumes
	/// that there has been an identifier matched previously and that
	/// $x points at that token. If that token is missing, but
	/// the next token in the stream is what we want we assume that
	/// this token is missing and we keep going. Because we
	/// have to return some token to replace the missing token,
	/// we have to conjure one up. This method gives the user control
	/// over the tokens returned for missing tokens. Mostly,
	/// you will want to create something special for identifier
	/// tokens. For literals such as '{' and ',', the default
	/// action in the parser or tree parser works. It simply creates
	/// a CommonToken of the appropriate type. The text will be the token.
	/// If you change what tokens must be created by the lexer,
	/// override this method to create the appropriate tokens.
	///
	UnitType*	getMissingSymbol( IntStreamType*		istream, ExceptionBaseType*		e,
												ANTLR_UINT32			expectedTokenType,
												BitsetListType*		follow);

    /** Function that returns whether the supplied grammar function
     *  will parse the current input stream or not. This is the way that syntactic
     *  predicates are evaluated. Unlike java, C is perfectly happy to invoke code
     *  via a pointer to a function (hence that's what all the ANTLR3 C interfaces
     *  do.
     */
	template<typename Predicate>
    bool  synpred( ClassForwarder<Predicate> );

	//In place of exConstruct, just directly instantiate the Exception Object

    /** Reset the recognizer
     */
    void  reset();
	void  reset( ClassForwarder<LexerType> );
	template<typename CompType>
	void  reset( ClassForwarder<CompType> );

	void exConstruct();

    ~BaseRecognizer();

};

}

#include "antlr3baserecognizer.inl"

/// @}
///

#endif	    /* _ANTLR3_BASERECOGNIZER_H	*/

