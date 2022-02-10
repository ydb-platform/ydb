/** \file
 * Defines the interface for an ANTLR3 common token stream. Custom token streams should create
 * one of these and then override any functions by installing their own pointers
 * to implement the various functions.
 */
#ifndef	_ANTLR3_TOKENSTREAM_HPP
#define	_ANTLR3_TOKENSTREAM_HPP

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

/** Definition of a token source, which has a pointer to a function that
 *  returns the next token (using a token factory if it is going to be
 *  efficient) and a pointer to an ANTLR3_INPUT_STREAM. This is slightly
 *  different to the Java interface because we have no way to implement
 *  multiple interfaces without defining them in the interface structure
 *  or casting (void *), which is too convoluted.
 */
namespace antlr3 {

//We are not making it subclass AllocPolicy, as this will always be a base class
template<class ImplTraits>
class TokenSource
{
public:
	typedef typename ImplTraits::CommonTokenType TokenType;
	typedef TokenType CommonTokenType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::LexerType LexerType;

private:
    /** A special pre-allocated token, which signifies End Of Tokens. Because this must
     *  be set up with the current input index and so on, we embed the structure and
     *  return the address of it. It is marked as factoryMade, so that it is never
     *  attempted to be freed.
     */
    TokenType				m_eofToken;

	/// A special pre-allocated token, which is returned by mTokens() if the
	/// lexer rule said to just skip the generated token altogether.
	/// Having this single token stops us wasting memory by have the token factory
	/// actually create something that we are going to SKIP(); anyway.
	///
	TokenType				m_skipToken;

    /** When the token source is constructed, it is populated with the file
     *  name from whence the tokens were produced by the lexer. This pointer is a
     *  copy of the one supplied by the CharStream (and may be NULL) so should
     *  not be manipulated other than to copy or print it.
     */
    StringType				m_fileName;

public:
	TokenType& get_eofToken();
	const TokenType& get_eofToken() const;
	TokenType& get_skipToken();
	StringType& get_fileName();
	LexerType* get_super();

	void set_fileName( const StringType& fileName );

	/**
	 * \brief
	 * Default implementation of the nextToken() call for a lexer.
	 *
	 * \param toksource
	 * Points to the implementation of a token source. The lexer is
	 * addressed by the super structure pointer.
	 *
	 * \returns
	 * The next token in the current input stream or the EOF token
	 * if there are no more tokens in any input stream in the stack.
	 *
	 * Write detailed description for nextToken here.
	 *
	 * \remarks
	 * Write remarks for nextToken here.
	 *
	 * \see nextTokenStr
	 */
    TokenType*  nextToken();
	CommonTokenType* nextToken( BoolForwarder<true> /*isFiltered*/ );
	CommonTokenType* nextToken( BoolForwarder<false> /*isFiltered*/ );

	///
	/// \brief
	/// Returns the next available token from the current input stream.
	///
	/// \param toksource
	/// Points to the implementation of a token source. The lexer is
	/// addressed by the super structure pointer.
	///
	/// \returns
	/// The next token in the current input stream or the EOF token
	/// if there are no more tokens.
	///
	/// \remarks
	/// Write remarks for nextToken here.
	///
	/// \see nextToken
	///
	TokenType*	nextTokenStr();

protected:
	TokenSource();
};

/** Definition of the ANTLR3 common token stream interface.
 * \remark
 * Much of the documentation for this interface is stolen from Ter's Java implementation.
 */
template<class ImplTraits>
class TokenStream  : public ImplTraits::TokenIntStreamType
{
public:
	typedef typename ImplTraits::TokenSourceType TokenSourceType;
	typedef typename ImplTraits::TokenIntStreamType IntStreamType;
	typedef typename ImplTraits::CommonTokenType TokenType;
	typedef TokenType UnitType;
	typedef typename ImplTraits::StringType StringType;
	typedef typename ImplTraits::DebugEventListenerType DebugEventListenerType;
	typedef typename ImplTraits::TokenStreamType TokenStreamType;
	typedef typename ImplTraits::ParserType ComponentType;

protected:
    /** Pointer to the token source for this stream
     */
    TokenSourceType*    m_tokenSource;

	/// Debugger interface, is this is a debugging token stream
	///
	DebugEventListenerType*	m_debugger;

	/// Indicates the initial stream state for dbgConsume()
	///
	bool				m_initialStreamState;

public:
	TokenStream(TokenSourceType* source, DebugEventListenerType* debugger);
	IntStreamType* get_istream();
	TokenSourceType* get_tokenSource() const;
	void set_tokenSource( TokenSourceType* tokenSource );

    /** Get Token at current input pointer + i ahead where i=1 is next Token.
     *  i<0 indicates tokens in the past.  So -1 is previous token and -2 is
     *  two tokens ago. LT(0) is undefined.  For i>=n, return Token.EOFToken.
     *  Return null for LT(0) and any index that results in an absolute address
     *  that is negative.
     */
    const TokenType*  LT(ANTLR_INT32 k);

    /** Where is this stream pulling tokens from?  This is not the name, but
     *  a pointer into an interface that contains a ANTLR3_TOKEN_SOURCE interface.
     *  The Token Source interface contains a pointer to the input stream and a pointer
     *  to a function that returns the next token.
     */
    TokenSourceType*   getTokenSource();

    /** Function that installs a token source for teh stream
     */
    void	setTokenSource(TokenSourceType*   tokenSource);

    /** Return the text of all the tokens in the stream, as the old tramp in
     *  Leeds market used to say; "Get the lot!"
     */
    StringType	toString();

    /** Return the text of all tokens from start to stop, inclusive.
     *  If the stream does not buffer all the tokens then it can just
     *  return an empty ANTLR3_STRING or NULL;  Grammars should not access $ruleLabel.text in
     *  an action in that case.
     */
    StringType	 toStringSS(ANTLR_MARKER start, ANTLR_MARKER stop);

    /** Because the user is not required to use a token with an index stored
     *  in it, we must provide a means for two token objects themselves to
     *  indicate the start/end location.  Most often this will just delegate
     *  to the other toString(int,int).  This is also parallel with
     *  the pTREENODE_STREAM->toString(Object,Object).
     */
    StringType	 toStringTT(const TokenType* start, const TokenType* stop);


    /** Function that sets the token stream into debugging mode
     */
    void	setDebugListener(DebugEventListenerType* debugger);

	TokenStream();

};

/** Common token stream is an implementation of ANTLR_TOKEN_STREAM for the default
 *  parsers and recognizers. You may of course build your own implementation if
 *  you are so inclined.
 */
template<bool TOKENS_ACCESSED_FROM_OWNING_RULE, class ListType, class MapType>
class TokenStoreSelector
{
public:
	typedef ListType TokensType;
};

template<class ListType, class MapType>
class TokenStoreSelector<true, ListType, MapType>
{
public:
	typedef MapType TokensType;
};

template<class ImplTraits>
class	CommonTokenStream : public TokenStream<ImplTraits>
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::BitsetType BitsetType;
	typedef typename ImplTraits::CommonTokenType TokenType;
	typedef typename ImplTraits::TokenSourceType TokenSourceType;
	typedef typename ImplTraits::DebugEventListenerType DebugEventListenerType;
	typedef typename AllocPolicyType::template ListType<TokenType> TokensListType;
	typedef typename AllocPolicyType::template OrderedMapType<ANTLR_MARKER, TokenType> TokensMapType;
	typedef typename TokenStoreSelector< ImplTraits::TOKENS_ACCESSED_FROM_OWNING_RULE,
	                                       TokensListType, TokensMapType >::TokensType TokensType;

	typedef typename AllocPolicyType::template UnOrderedMapType<ANTLR_UINT32, ANTLR_UINT32> ChannelOverridesType;
	typedef typename AllocPolicyType::template OrderedSetType<ANTLR_UINT32> DiscardSetType;
	typedef typename AllocPolicyType::template ListType<ANTLR_UINT32> IntListType;
	typedef TokenStream<ImplTraits> BaseType;

private:
    /** Records every single token pulled from the source indexed by the token index.
     *  There might be more efficient ways to do this, such as referencing directly in to
     *  the token factory pools, but for now this is convenient and the ANTLR3_LIST is not
     *  a huge overhead as it only stores pointers anyway, but allows for iterations and
     *  so on.
     */
    TokensType			m_tokens;

    /** Override map of tokens. If a token type has an entry in here, then
     *  the pointer in the table points to an int, being the override channel number
     *  that should always be used for this token type.
     */
    ChannelOverridesType	m_channelOverrides;

    /** Discared set. If a token has an entry in this table, then it is thrown
     *  away (data pointer is always NULL).
     */
    DiscardSetType			m_discardSet;

    /* The channel number that this token stream is tuned to. For instance, whitespace
     * is usually tuned to channel 99, which no token stream would normally tune to and
     * so it is thrown away.
     */
    ANTLR_UINT32			m_channel;

	/** The index into the tokens list of the current token (the next one that will be
     *  consumed. p = -1 indicates that the token list is empty.
     */
    ANTLR_INT32				m_p;

	/* The total number of tokens issued till now. For streams that delete tokens,
	   this helps in issuing the index
	 */
	ANTLR_UINT32			m_nissued;

    /** If this flag is set to true, then tokens that the stream sees that are not
     *  in the channel that this stream is tuned to, are not tracked in the
     *  tokens table. When set to false, ALL tokens are added to the tracking.
     */
    bool					m_discardOffChannel;

public:
	CommonTokenStream(ANTLR_UINT32 hint, TokenSourceType* source = NULL,
										DebugEventListenerType* debugger = NULL);
	~CommonTokenStream();
	TokensType& get_tokens();
	const TokensType& get_tokens() const;
	DiscardSetType& get_discardSet();
	const DiscardSetType& get_discardSet() const;
	ANTLR_INT32 get_p() const;
	void set_p( ANTLR_INT32 p );
	void inc_p();
	void dec_p();

    /** A simple filter mechanism whereby you can tell this token stream
     *  to force all tokens of type ttype to be on channel.  For example,
     *  when interpreting, we cannot exec actions so we need to tell
     *  the stream to force all WS and NEWLINE to be a different, ignored
     *  channel.
     */
    void setTokenTypeChannel(ANTLR_UINT32 ttype, ANTLR_UINT32 channel);

    /** Add a particular token type to the discard set. If a token is found to belong
     *  to this set, then it is skipped/thrown away
     */
    void discardTokenType(ANTLR_INT32 ttype);

	//This will discard tokens of a particular rule after the rule execution completion
	void discardTokens( ANTLR_MARKER start, ANTLR_MARKER stop );
	void discardTokens( ANTLR_MARKER start, ANTLR_MARKER stop, 
								BoolForwarder<true>  tokens_accessed_from_owning_rule  );
	void discardTokens( ANTLR_MARKER start, ANTLR_MARKER stop, 
								BoolForwarder<false>  tokens_accessed_from_owning_rule  );

	void insertToken( const TokenType& tok );
	void insertToken( const TokenType& tok, BoolForwarder<true>  tokens_accessed_from_owning_rule  );
	void insertToken( const TokenType& tok, BoolForwarder<false>  tokens_accessed_from_owning_rule  );

	/** Get a token at an absolute index i; 0..n-1.  This is really only
	 *  needed for profiling and debugging and token stream rewriting.
	 *  If you don't want to buffer up tokens, then this method makes no
	 *  sense for you.  Naturally you can't use the rewrite stream feature.
	 *  I believe DebugTokenStream can easily be altered to not use
	 *  this method, removing the dependency.
	 */
	const TokenType* get(ANTLR_MARKER i);
	const TokenType* getToken(ANTLR_MARKER i);
	const TokenType* getToken( ANTLR_MARKER tok_idx, BoolForwarder<true>  tokens_accessed_from_owning_rule );
	const TokenType* getToken( ANTLR_MARKER tok_idx, BoolForwarder<false>  tokens_accessed_from_owning_rule  );

    /** Signal to discard off channel tokens from here on in.
     */
    void discardOffChannelToks(bool discard);

    /** Function that returns a pointer to the ANTLR3_LIST of all tokens
     *  in the stream (this causes the buffer to fill if we have not get any yet)
     */
    TokensType*	getTokens();

    /** Function that returns all the tokens between a start and a stop index.
     */
    void getTokenRange(ANTLR_UINT32 start, ANTLR_UINT32 stop, TokensListType& tokenRange);

    /** Function that returns all the tokens indicated by the specified bitset, within a range of tokens
     */
    void getTokensSet(ANTLR_UINT32 start, ANTLR_UINT32 stop, BitsetType* types, TokensListType& tokenSet);

    /** Function that returns all the tokens indicated by being a member of the supplied List
     */
    void getTokensList(ANTLR_UINT32 start, ANTLR_UINT32 stop,
									const IntListType& list, TokensListType& tokenList);

    /** Function that returns all tokens of a certain type within a range.
     */
    void getTokensType(ANTLR_UINT32 start, ANTLR_UINT32 stop, ANTLR_UINT32 type, TokensListType& tokens);

    /** Function that resets the token stream so that it can be reused, but
     *  but that does not free up any resources, such as the token factory
     *  the factory pool and so on. This prevents the need to keep freeing
     *  and reallocating the token pools if the thing you are building is
     *  a multi-shot dameon or somethign like that. It is much faster to
     *  just reuse all the vectors.
     */
    void  reset();

	const TokenType* LB(ANTLR_INT32 k);


	void fillBufferExt();
	void fillBuffer();

	bool hasReachedFillbufferTarget( ANTLR_UINT32 cnt, BoolForwarder<true>  tokens_accessed_from_owning_rule  );
	bool hasReachedFillbufferTarget( ANTLR_UINT32 cnt, BoolForwarder<false>  tokens_accessed_from_owning_rule  );

	ANTLR_UINT32 skipOffTokenChannels(ANTLR_INT32 i);
	ANTLR_UINT32 skipOffTokenChannelsReverse(ANTLR_INT32 x);
	ANTLR_MARKER index_impl();
};

class TokenAccessException : public std::exception
{
	virtual const char* what() const noexcept
	{
		return " Attempted access on Deleted Token";
	}
};

}

#include "antlr3tokenstream.inl"

#endif
