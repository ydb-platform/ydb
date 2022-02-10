/** \file
 * Base interface for any ANTLR3 lexer.
 *
 * An ANLTR3 lexer builds from two sets of components:
 *
 *  - The runtime components that provide common functionality such as
 *    traversing character streams, building tokens for output and so on.
 *  - The generated rules and struutre of the actual lexer, which call upon the
 *    runtime components.
 *
 * A lexer class contains  a character input stream, a base recognizer interface
 * (which it will normally implement) and a token source interface (which it also
 * implements. The Tokensource interface is called by a token consumer (such as
 * a parser, but in theory it can be anything that wants a set of abstract
 * tokens in place of a raw character stream.
 *
 * So then, we set up a lexer in a sequence akin to:
 *
 *  - Create a character stream (something which implements ANTLR3_INPUT_STREAM)
 *    and initialize it.
 *  - Create a lexer interface and tell it where it its input stream is.
 *    This will cause the creation of a base recognizer class, which it will
 *    override with its own implementations of some methods. The lexer creator
 *    can also then in turn override anything it likes.
 *  - The lexer token source interface is then passed to some interface that
 *    knows how to use it, byte calling for a next token.
 *  - When a next token is called, let ze lexing begin.
 *
 */
#ifndef	_ANTLR3_LEXER_HPP
#define	_ANTLR3_LEXER_HPP

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

/* Definitions
 */

namespace antlr3 {

static const ANTLR_UINT32	ANTLR_STRING_TERMINATOR	= 0xFFFFFFFF;

template<class ImplTraits>
class  Lexer : public ImplTraits::template RecognizerType< typename ImplTraits::InputStreamType >,
			   public ImplTraits::TokenSourceType
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::InputStreamType InputStreamType;
	typedef InputStreamType StreamType;
	typedef typename InputStreamType::IntStreamType IntStreamType;
	typedef typename ImplTraits::CommonTokenType CommonTokenType;
	typedef typename ImplTraits::StreamDataType TokenType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::StringStreamType StringStreamType;
	typedef typename ImplTraits::template RecognizerType< InputStreamType > RecognizerType;
	typedef typename RecognizerType::RecognizerSharedStateType RecognizerSharedStateType;
	typedef typename ImplTraits::template ExceptionBaseType<InputStreamType> ExceptionBaseType;
	typedef typename ImplTraits::BitsetListType BitsetListType;
	typedef typename ImplTraits::TokenSourceType TokenSourceType;

	typedef typename RecognizerSharedStateType::RuleMemoType RuleMemoType;
	typedef typename RecognizerType::DebugEventListenerType DebuggerType;

private:
    /** A pointer to the character stream whence this lexer is receiving
     *  characters.
     *  TODO: I may come back to this and implement charstream outside
     *  the input stream as per the java implementation.
     */
    InputStreamType*		m_input;

public:
	Lexer(ANTLR_UINT32 sizeHint, RecognizerSharedStateType* state);
	Lexer(ANTLR_UINT32 sizeHint, InputStreamType* input, RecognizerSharedStateType* state);

	InputStreamType* get_input() const;
	IntStreamType* get_istream() const;
	RecognizerType* get_rec();
	const RecognizerType* get_rec() const;
	TokenSourceType* get_tokSource();
	
	//functions used in .stg file
	const RecognizerType* get_recognizer() const;
	RecognizerSharedStateType* get_lexstate() const;
	void set_lexstate( RecognizerSharedStateType* lexstate );
	const TokenSourceType* get_tokSource() const;
	CommonTokenType* get_ltoken() const;
	void set_ltoken( const CommonTokenType* ltoken );
	bool hasFailed() const;
	ANTLR_INT32 get_backtracking() const;
	void inc_backtracking();
	void dec_backtracking();
	bool get_failedflag() const;
	void set_failedflag( bool failed );
	InputStreamType* get_strstream() const;
	ANTLR_MARKER  index() const;
	void	seek(ANTLR_MARKER index);
	const CommonTokenType* EOF_Token() const;
	bool hasException() const;
	ExceptionBaseType* get_exception() const;
	void constructEx();
	void lrecover();
	ANTLR_MARKER mark();
	void rewind(ANTLR_MARKER marker);
	void rewindLast();
	void setText( const StringType& text );
	void skip();
	RuleMemoType* getRuleMemo() const;
	DebuggerType* get_debugger() const;
	void setRuleMemo(RuleMemoType* rulememo);
	ANTLR_UINT32 LA(ANTLR_INT32 i);
	void consume();
	void memoize(ANTLR_MARKER	ruleIndex, ANTLR_MARKER	ruleParseStart);
	bool haveParsedRule(ANTLR_MARKER	ruleIndex);

    /** Pointer to a function that sets the charstream source for the lexer and
     *  causes it to  be reset.
     */
    void	setCharStream(InputStreamType* input);

    /*!
	 * \brief
	 * Change to a new input stream, remembering the old one.
	 *
	 * \param lexer
	 * Pointer to the lexer instance to switch input streams for.
	 *
	 * \param input
	 * New input stream to install as the current one.
	 *
	 * Switches the current character input stream to
	 * a new one, saving the old one, which we will revert to at the end of this
	 * new one.
	 */
    void	pushCharStream(InputStreamType* input);

	/*!
	 * \brief
	 * Stops using the current input stream and reverts to any prior
	 * input stream on the stack.
	 *
	 * \param lexer
	 * Description of parameter lexer.
	 *
	 * Pointer to a function that abandons the current input stream, whether it
	 * is empty or not and reverts to the previous stacked input stream.
	 *
	 * \remark
	 * The function fails silently if there are no prior input streams.
	 */
    void	popCharStream();

    /** Function that emits (a copy of ) the supplied token as the next token in
     *  the stream.
     */
    void	emit(const CommonTokenType* token);

    /** Pointer to a function that constructs a new token from the lexer stored information
     */
    CommonTokenType*	emit();

    /** Pointer to a function that attempts to match and consume the specified string from the input
     *  stream. Note that strings muse be passed as terminated arrays of ANTLR3_UCHAR. Strings are terminated
     *  with 0xFFFFFFFF, which is an invalid UTF32 character
     */
    bool	matchs(ANTLR_UCHAR* string);

    /** Pointer to a function that matches and consumes the specified character from the input stream.
     *  The input stream is required to provide characters via LA() as UTF32 characters. The default lexer
     *  implementation is source encoding agnostic and so input streams do not generally need to
     *  override the default implmentation.
     */
    bool	matchc(ANTLR_UCHAR c);

    /** Pointer to a function that matches any character in the supplied range (I suppose it could be a token range too
     *  but this would only be useful if the tokens were in tsome guaranteed order which is
     *  only going to happen with a hand crafted token set).
     */
    bool	matchRange(ANTLR_UCHAR low, ANTLR_UCHAR high);

    /** Pointer to a function that matches the next token/char in the input stream
     *  regardless of what it actaully is.
     */
    void		matchAny();

    /** Pointer to a function that recovers from an error found in the input stream.
     *  Generally, this will be a #ANTLR3_EXCEPTION_NOVIABLE_ALT but it could also
     *  be from a mismatched token that the (*match)() could not recover from.
     */
    void		recover();

    /** Function to return the current line number in the input stream
     */
    ANTLR_UINT32	getLine();
    ANTLR_MARKER	getCharIndex();
    ANTLR_UINT32	getCharPositionInLine();

    /** Function to return the text so far for the current token being generated
     */
    StringType 	getText();

	//Other utility functions
	void fillExceptionData( ExceptionBaseType* ex );

	/** Default lexer error handler (works for 8 bit streams only!!!)
	 */
	void displayRecognitionError( ANTLR_UINT8** tokenNames, ExceptionBaseType* ex);
	void exConstruct();
	TokenType*	getMissingSymbol( IntStreamType* istream, ExceptionBaseType* e,
								  ANTLR_UINT32	expectedTokenType, BitsetListType*	follow);

    /** Pointer to a function that knows how to free the resources of a lexer
     */
	~Lexer();
};

}

#include "antlr3lexer.inl"

#endif
