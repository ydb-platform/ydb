namespace antlr3 {

template<class ImplTraits>
TokenSource<ImplTraits>::TokenSource()
	:m_eofToken( ImplTraits::CommonTokenType::TOKEN_EOF), 
	m_skipToken( ImplTraits::CommonTokenType::TOKEN_INVALID)
{
}

template<class ImplTraits>
ANTLR_INLINE typename TokenSource<ImplTraits>::CommonTokenType& TokenSource<ImplTraits>::get_eofToken()
{
	return m_eofToken;
}

template<class ImplTraits>
ANTLR_INLINE const typename TokenSource<ImplTraits>::TokenType& TokenSource<ImplTraits>::get_eofToken() const
{
	return m_eofToken;
}

template<class ImplTraits>
ANTLR_INLINE typename TokenSource<ImplTraits>::CommonTokenType& TokenSource<ImplTraits>::get_skipToken()
{
	return m_skipToken;
}

template<class ImplTraits>
ANTLR_INLINE typename TokenSource<ImplTraits>::StringType& TokenSource<ImplTraits>::get_fileName()
{
	return m_fileName;
}

template<class ImplTraits>
ANTLR_INLINE void TokenSource<ImplTraits>::set_fileName( const StringType& fileName )
{
	m_fileName = fileName;
}

template<class ImplTraits>
typename TokenSource<ImplTraits>::LexerType* TokenSource<ImplTraits>::get_super()
{
	return static_cast<LexerType*>(this);
}

template<class ImplTraits>
typename TokenSource<ImplTraits>::TokenType*	TokenSource<ImplTraits>::nextTokenStr()
{
	typedef typename LexerType::RecognizerSharedStateType RecognizerSharedStateType;
	typedef typename LexerType::InputStreamType InputStreamType;
	typedef typename LexerType::IntStreamType IntStreamType;
	LexerType*                  lexer;
    RecognizerSharedStateType*	state;
    InputStreamType*            input;
    IntStreamType*              istream;

    lexer   = this->get_super();
    state   = lexer->get_rec()->get_state();
    input   = lexer->get_input();
    istream = input->get_istream();

    /// Loop until we get a non skipped token or EOF
    ///
    for	(;;)
    {
        // Get rid of any previous token (token factory takes care of
        // any de-allocation when this token is finally used up.
        //
        state->set_token_present(false);
        state->set_error(false);	    // Start out without an exception
        state->set_failed(false);

        // Now call the matching rules and see if we can generate a new token
        //
        for	(;;)
        {
            // Record the start of the token in our input stream.
            //
            state->set_channel( TOKEN_DEFAULT_CHANNEL );
            state->set_tokenStartCharIndex( (ANTLR_MARKER)input->get_nextChar() );
            state->set_tokenStartCharPositionInLine( input->get_charPositionInLine() );
            state->set_tokenStartLine( input->get_line() );
            state->set_text("");

            if  (istream->LA(1) == ANTLR_CHARSTREAM_EOF)
            {
                // Reached the end of the current stream, nothing more to do if this is
                // the last in the stack.
                //
                TokenType&    teof = m_eofToken;

                teof.set_startIndex(lexer->getCharIndex());
                teof.set_stopIndex(lexer->getCharIndex());
                teof.set_line(lexer->getLine());
                return  &teof;
            }

            state->set_token_present( false );
            state->set_error(false);	    // Start out without an exception
            state->set_failed(false);

            // Call the generated lexer, see if it can get a new token together.
            //
            lexer->mTokens();

            if  (state->get_error()  == true)
            {
                // Recognition exception, report it and try to recover.
                //
                state->set_failed(true);
                lexer->get_rec()->reportError();
                lexer->recover(); 
                if (state->get_token_present())
                    // Good(or invalid) token factored by custom recover procedure
                    //
                    return  state->get_token();
            }
            else
            {
                if ( !state->get_token_present() )
                {
                    // Emit the real token, which adds it in to the token stream basically
                    //
                    lexer->emit();
                }
                else if	( *(state->get_token()) ==  m_skipToken )
                {
                    // A real token could have been generated, but "Computer say's naaaaah" and it
                    // it is just something we need to skip altogether.
                    //
                    continue;
                }

                // Good token, not skipped, not EOF token
                //
                return  state->get_token();
            }
        }
    }
}

template<class ImplTraits>
typename TokenSource<ImplTraits>::TokenType*  TokenSource<ImplTraits>::nextToken()
{
	return this->nextToken( BoolForwarder<LexerType::IsFiltered>() );
}

template<class ImplTraits>
typename TokenSource<ImplTraits>::CommonTokenType*	TokenSource<ImplTraits>::nextToken( BoolForwarder<true> /*isFiltered*/ )
{
	LexerType*   lexer;
	typename LexerType::RecognizerSharedStateType* state;

	lexer   = this->get_super();
	state	= lexer->get_lexstate();

	/* Get rid of any previous token (token factory takes care of
		* any deallocation when this token is finally used up.
		*/
	state->set_token_present( false );
	state->set_error( false );	    /* Start out without an exception	*/
	state->set_failed(false);

	/* Record the start of the token in our input stream.
		*/
	state->set_tokenStartCharIndex( lexer->index() );
	state->set_tokenStartCharPositionInLine( lexer->getCharPositionInLine() );
	state->set_tokenStartLine( lexer->getLine() );
	state->set_text("");

	/* Now call the matching rules and see if we can generate a new token
		*/
	for	(;;)
	{
		if (lexer->LA(1) == ANTLR_CHARSTREAM_EOF)
		{
			/* Reached the end of the stream, nothing more to do.
				*/
			CommonTokenType&    teof = m_eofToken;

			teof.set_startIndex(lexer->getCharIndex());
			teof.set_stopIndex(lexer->getCharIndex());
			teof.set_line(lexer->getLine());
			return  &teof;
		}

		state->set_token_present(false);
		state->set_error(false);	    /* Start out without an exception	*/

		{
			ANTLR_MARKER   m;

			m	= lexer->get_istream()->mark();
			state->set_backtracking(1);				/* No exceptions */
			state->set_failed(false);

			/* Call the generated lexer, see if it can get a new token together.
				*/
			lexer->mTokens();
    		state->set_backtracking(0);

    		/* mTokens backtracks with synpred at BACKTRACKING==2
				and we set the synpredgate to allow actions at level 1. */

			if(state->get_failed())
			{
				lexer->rewind(m);
				lexer->consume(); //<! advance one char and try again !>
			}
			else
			{
				lexer->emit();					/* Assemble the token and emit it to the stream */
				TokenType* tok = state->get_token();
				return tok;
			}
		}
	}
}

template<class ImplTraits>
typename TokenSource<ImplTraits>::CommonTokenType*	TokenSource<ImplTraits>::nextToken( BoolForwarder<false> /*isFiltered*/ )
{
	// Find the next token in the current stream
	//
	CommonTokenType* tok = this->nextTokenStr();

	// If we got to the EOF token then switch to the previous
	// input stream if there were any and just return the
	// EOF if there are none. We must check the next token
	// in any outstanding input stream we pop into the active
	// role to see if it was sitting at EOF after PUSHing the
	// stream we just consumed, otherwise we will return EOF
	// on the reinstalled input stream, when in actual fact
	// there might be more input streams to POP before the
	// real EOF of the whole logical inptu stream. Hence we
	// use a while loop here until we find somethign in the stream
	// that isn't EOF or we reach the actual end of the last input
	// stream on the stack.
	//
	while(tok->get_type() == CommonTokenType::TOKEN_EOF)
	{
		typename ImplTraits::LexerType*   lexer;
		lexer   = static_cast<typename ImplTraits::LexerType*>( this->get_super() );

		if  ( lexer->get_rec()->get_state()->get_streams().size() > 0)
		{
			// We have another input stream in the stack so we
			// need to revert to it, then resume the loop to check
			// it wasn't sitting at EOF itself.
			//
			lexer->popCharStream();
			tok = this->nextTokenStr();
		}
		else
		{
			// There were no more streams on the input stack
			// so this EOF is the 'real' logical EOF for
			// the input stream. So we just exit the loop and 
			// return the EOF we have found.
			//
			break;
		}
		
	}

	// return whatever token we have, which may be EOF
	//
	return  tok;
}

template<class ImplTraits>
TokenStream<ImplTraits>::TokenStream()
{
	m_tokenSource = NULL;
	m_debugger = NULL;
	m_initialStreamState = false;
}

template<class ImplTraits>
typename TokenStream<ImplTraits>::IntStreamType* TokenStream<ImplTraits>::get_istream()
{
	return this;
}

template<class ImplTraits>
TokenStream<ImplTraits>::TokenStream(TokenSourceType* source, DebugEventListenerType* debugger)
{
	m_initialStreamState = false;
	m_tokenSource = source;
	m_debugger = debugger;
}

template<class ImplTraits>
CommonTokenStream<ImplTraits>::CommonTokenStream(ANTLR_UINT32 , TokenSourceType* source, 
													DebugEventListenerType* debugger)
					: CommonTokenStream<ImplTraits>::BaseType( source, debugger )
{
	m_p = -1;
	m_channel = TOKEN_DEFAULT_CHANNEL;
	m_discardOffChannel = false;
	m_nissued = 0;
}

template<class ImplTraits>
typename CommonTokenStream<ImplTraits>::TokensType& CommonTokenStream<ImplTraits>::get_tokens()
{
	return m_tokens;
}

template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::TokensType& CommonTokenStream<ImplTraits>::get_tokens() const
{
	return m_tokens;
}

template<class ImplTraits>
typename CommonTokenStream<ImplTraits>::DiscardSetType& CommonTokenStream<ImplTraits>::get_discardSet()
{
	return m_discardSet;
}

template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::DiscardSetType& CommonTokenStream<ImplTraits>::get_discardSet() const
{
	return m_discardSet;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_INT32 CommonTokenStream<ImplTraits>::get_p() const
{
	return m_p;
}

template<class ImplTraits>
ANTLR_INLINE void CommonTokenStream<ImplTraits>::set_p( ANTLR_INT32 p )
{
	m_p = p;
}

template<class ImplTraits>
ANTLR_INLINE void CommonTokenStream<ImplTraits>::inc_p()
{
	++m_p;
}

template<class ImplTraits>
ANTLR_INLINE void CommonTokenStream<ImplTraits>::dec_p()
{
	--m_p;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_MARKER CommonTokenStream<ImplTraits>::index_impl()
{
	return m_p;
}

// Reset a token stream so it can be used again and can reuse it's
// resources.
//
template<class ImplTraits>
void  CommonTokenStream<ImplTraits>::reset()
{
	// Free any resources that ar most like specifc to the
    // run we just did.
    //
	m_discardSet.clear();
	m_channelOverrides.clear();

    // Now, if there were any existing tokens in the stream,
    // then we just reset the vector count so that it starts
    // again. We must traverse the entries unfortunately as
    // there may be free pointers for custom token types and
    // so on. However that is just a quick NULL check on the
    // vector entries.
    //
	m_tokens.clear();

    // Reset to defaults
    //
    m_discardOffChannel  = false;
    m_channel            = ImplTraits::CommonTokenType::TOKEN_DEFAULT_CHANNEL;
    m_p	            = -1;
}

template<class ImplTraits>
void	TokenStream<ImplTraits>::setDebugListener(DebugEventListenerType* debugger)
{
	m_debugger = debugger;
	m_initialStreamState = false;
}

template<class ImplTraits>
const typename TokenStream<ImplTraits>::TokenType*  TokenStream<ImplTraits>::LT(ANTLR_INT32 k)
{
	ANTLR_INT32    i;
	ANTLR_INT32    n;
	TokenStreamType* cts;

	cts	    = this->get_super();

    if(k < 0)
	{
		return cts->LB(-k);
	}

	ANTLR_INT32 req_idx = cts->get_p() + k - 1;
	ANTLR_INT32 cached_size = static_cast<ANTLR_INT32>(this->get_istream()->get_cachedSize());

	if(	(cts->get_p() == -1) ||
		( ( req_idx >= cached_size ) && ( (cached_size % ImplTraits::TOKEN_FILL_BUFFER_INCREMENT) == 0 ) )
	  )
	{
		cts->fillBuffer();
	}

    // Here we used to check for k == 0 and return 0, but this seems
    // a superfluous check to me. LT(k=0) is therefore just undefined
    // and we won't waste the clock cycles on the check
    //
	cached_size = static_cast<ANTLR_INT32>(this->get_istream()->get_cachedSize());
	if	( req_idx >= cached_size )
	{
		TokenType&    teof = cts->get_tokenSource()->get_eofToken();

		teof.set_startIndex( this->get_istream()->index());
		teof.set_stopIndex( this->get_istream()->index());
		return  &teof;
	}

	i	= cts->get_p();
	n	= 1;

	/* Need to find k good tokens, skipping ones that are off channel
	*/
	while( n < k)
	{
		/* Skip off-channel tokens */
		i = cts->skipOffTokenChannels(i+1); /* leave p on valid token    */
		n++;
	}
	
	if( ( i >= cached_size ) && ( (cached_size % ImplTraits::TOKEN_FILL_BUFFER_INCREMENT) == 0 ) )
	{
		cts->fillBuffer();
	}
	if	( (ANTLR_UINT32) i >= this->get_istream()->get_cachedSize() )
	{
		TokenType&    teof = cts->get_tokenSource()->get_eofToken();

		teof.set_startIndex(this->get_istream()->index());
		teof.set_stopIndex(this->get_istream()->index());
		return  &teof;
	}

	// Here the token must be in the input vector. Rather then incur
	// function call penalty, we just return the pointer directly
	// from the vector
	//
	return cts->getToken(i);
}

template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::TokenType* CommonTokenStream<ImplTraits>::LB(ANTLR_INT32 k)
{
    ANTLR_INT32 i;
    ANTLR_INT32 n;

    if (m_p == -1)
    {
        this->fillBuffer();
    }
    if (k == 0)
    {
        return NULL;
    }
    if ((m_p - k) < 0)
    {
        return NULL;
    }

    i = m_p;
    n = 1;

    /* Need to find k good tokens, going backwards, skipping ones that are off channel
     */
    while (n <= k)
    {
        /* Skip off-channel tokens
         */

        i = this->skipOffTokenChannelsReverse(i - 1); /* leave p on valid token    */
        n++;
    }
    if (i < 0)
    {
        return NULL;
    }
	
	// Here the token must be in the input vector. Rather then incut
	// function call penalty, we jsut return the pointer directly
	// from the vector
	//
	return this->getToken(i);
}

template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::TokenType*   CommonTokenStream<ImplTraits>::getToken(ANTLR_MARKER i)
{
	return this->get(i);
}


template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::TokenType* CommonTokenStream<ImplTraits>::get(ANTLR_MARKER i)
{
	return this->getToken( static_cast<ANTLR_MARKER>(i), 
							BoolForwarder<ImplTraits::TOKENS_ACCESSED_FROM_OWNING_RULE>() );
}

template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::TokenType* CommonTokenStream<ImplTraits>::getToken( ANTLR_MARKER tok_idx,
															BoolForwarder<true>  /*tokens_accessed_from_owning_rule*/  )
{
	typename TokensType::iterator iter = m_tokens.find(tok_idx);
	if( iter == m_tokens.end() )
	{
		TokenAccessException ex;
		throw ex;
	}
	const TokenType& tok = iter->second;
    return  &tok; 
}

template<class ImplTraits>
const typename CommonTokenStream<ImplTraits>::TokenType* CommonTokenStream<ImplTraits>::getToken( ANTLR_MARKER tok_idx, BoolForwarder<false>  /*tokens_accessed_from_owning_rule*/   )
{
	TokenType& tok = m_tokens.at( static_cast<ANTLR_UINT32>(tok_idx) );
    return  &tok; 
}

template<class ImplTraits>
typename TokenStream<ImplTraits>::TokenSourceType* TokenStream<ImplTraits>::get_tokenSource() const
{
	return m_tokenSource;
}

template<class ImplTraits>
void TokenStream<ImplTraits>::set_tokenSource( TokenSourceType* tokenSource )
{
	m_tokenSource = tokenSource;
}

template<class ImplTraits>
typename TokenStream<ImplTraits>::StringType	TokenStream<ImplTraits>::toString()
{
	TokenStreamType* cts = static_cast<TokenStreamType>(this);

	if	(cts->get_p() == -1)
    {
		cts->fillBuffer();
    }

    return  this->toStringSS(0, this->get_istream()->size());
}

template<class ImplTraits>
typename TokenStream<ImplTraits>::StringType
TokenStream<ImplTraits>::toStringSS(ANTLR_MARKER start, ANTLR_MARKER stop)
{
    StringType string;
    TokenSourceType* tsource;
    const TokenType* tok;
    TokenStreamType* cts;

    cts = this->get_super();

    if (cts->get_p() == -1)
    {
        cts->fillBuffer();
    }
    if (stop >= this->get_istream()->size())
    {
        stop = this->get_istream()->size() - 1;
    }

    /* Who is giving us these tokens?
     */
    tsource = cts->get_tokenSource();

    if (tsource != NULL && !cts->get_tokens().empty() )
    {
        /* Finally, let's get a string
         */
        for (ANTLR_MARKER i = start; i <= stop; i++)
        {
            tok = cts->get(i);
            if (tok != NULL)
            {
                string.append( tok->getText() );
            }
        }

        return string;
    }
    return "";
}

template<class ImplTraits>
typename TokenStream<ImplTraits>::StringType
TokenStream<ImplTraits>::toStringTT(const TokenType* start, const TokenType* stop)
{
	if	(start != NULL && stop != NULL)
	{
		return	this->toStringSS( start->get_tokenIndex(), 
								  stop->get_tokenIndex());
	}
	else
	{
		return	"";
	}
}

/** A simple filter mechanism whereby you can tell this token stream
 *  to force all tokens of type ttype to be on channel.  For example,
 *  when interpreting, we cannot execute actions so we need to tell
 *  the stream to force all WS and NEWLINE to be a different, ignored,
 *  channel.
 */
template<class ImplTraits>
void	CommonTokenStream<ImplTraits>::setTokenTypeChannel ( ANTLR_UINT32 ttype, ANTLR_UINT32 channel)
{
    /* We add one to the channel so we can distinguish NULL as being no entry in the
     * table for a particular token type.
     */
    m_channelOverrides[ttype] = (ANTLR_UINT32)channel + 1;

}

template<class ImplTraits>
void  CommonTokenStream<ImplTraits>::discardTokenType(ANTLR_INT32 ttype)
{
	 /* We add one to the channel so we can distinguish NULL as being no entry in the
     * table for a particular token type. We could use bitsets for this I suppose too.
     */
	m_discardSet.insert(ttype);
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::discardOffChannelToks(bool discard)
{
	m_discardOffChannel = discard;
}

template<class ImplTraits>
typename CommonTokenStream<ImplTraits>::TokensType*  CommonTokenStream<ImplTraits>::getTokens()
{
	if	(m_p == -1)
    {
		this->fillBuffer();
    }

    return  &m_tokens;
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::getTokenRange(ANTLR_UINT32 start, ANTLR_UINT32 stop, 
																	TokensListType& tokenRange)
{
	return this->getTokensSet(start, stop, NULL, tokenRange);
}

/** Given a start and stop index, return a List of all tokens in
 *  the token type BitSet.  Return null if no tokens were found.  This
 *  method looks at both on and off channel tokens.
 */
template<class ImplTraits>
void
CommonTokenStream<ImplTraits>::getTokensSet(ANTLR_UINT32 start, ANTLR_UINT32 stop, BitsetType* types,
                                                    TokensListType& filteredList )
{
    ANTLR_UINT32	    i;
    ANTLR_UINT32	    n;
    TokenType*	tok;

    if	( m_p == -1)
    {
		this->fillBuffer();
    }
    if	(stop > this->get_istream()->size())
    {
		stop = this->get_istream()->size();
    }
    if	(start > stop)
    {
		return;
    }

    /* We have the range set, now we need to iterate through the
     * installed tokens and create a new list with just the ones we want
     * in it. We are just moving pointers about really.
     */
    for(i = start, n = 0; i<= stop; i++)
    {
		tok = this->get(i);

		if  (	   types == NULL
			|| (types->isMember( tok->get_type() ) == true )
			)
		{
			filteredList.push_back(tok);
		}
	}
    
    return ;
}

template<class ImplTraits>
void
CommonTokenStream<ImplTraits>::getTokensList(ANTLR_UINT32 start, ANTLR_UINT32 stop, 
													const IntListType& list, TokensListType& newlist)
{
    BitsetType*		bitSet;

    bitSet  = Bitset<ImplTraits>::BitsetFromList(list);
    this->getTokensSet(start, stop, bitSet, newlist);
    delete bitSet;
}

template<class ImplTraits>
void 
CommonTokenStream<ImplTraits>::getTokensType(ANTLR_UINT32 start, ANTLR_UINT32 stop, ANTLR_UINT32 type,
                                                  TokensListType& newlist   )
{
    BitsetType*  bitSet;

    bitSet  = BitsetType::BitsetOf(type, -1);
    this->getTokensSet(start, stop, bitSet, newlist);

    delete bitSet;
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::fillBufferExt()
{
    this->fillBuffer();
}

template<class ImplTraits>
bool CommonTokenStream<ImplTraits>::hasReachedFillbufferTarget( ANTLR_UINT32 cnt, 
																BoolForwarder<true> )
{
	return ( cnt >= ImplTraits::TOKEN_FILL_BUFFER_INCREMENT );
}

template<class ImplTraits>
bool CommonTokenStream<ImplTraits>::hasReachedFillbufferTarget( ANTLR_UINT32, 
																BoolForwarder<false>  )
{
	return false;
}


template<class ImplTraits>
void CommonTokenStream<ImplTraits>::fillBuffer() 
{
    ANTLR_UINT32 index;
    TokenType* tok;
    bool discard;
    
    /* Start at index 0 of course
     */
	ANTLR_UINT32 cached_p = (m_p < 0) ? 0 : m_p;
    index = m_nissued;
	ANTLR_UINT32 cnt = 0;

    /* Pick out the next token from the token source
     * Remember we just get a pointer (reference if you like) here
     * and so if we store it anywhere, we don't set any pointers to auto free it.
     */
    tok = this->get_tokenSource()->nextToken();

    while ( tok->get_type() != TokenType::TOKEN_EOF )
    {
        discard = false; /* Assume we are not discarding	*/

        /* I employ a bit of a trick, or perhaps hack here. Rather than
         * store a pointer to a structure in the override map and discard set
         * we store the value + 1 cast to a void *. Hence on systems where NULL = (void *)0
         * we can distinguish "not being there" from "being channel or type 0"
         */

        if ( m_discardSet.find(tok->get_type()) != m_discardSet.end() )
        {
            discard = true;
        }
        else if (   m_discardOffChannel == true
                 && tok->get_channel() != m_channel
                 )
        {
            discard = true;
        }
        else if (!m_channelOverrides.empty())
        {
            /* See if this type is in the override map
             */
			typename ChannelOverridesType::iterator iter = m_channelOverrides.find( tok->get_type() + 1 );

            if (iter != m_channelOverrides.end())
            {
                /* Override found
                 */
                tok->set_channel( ANTLR_UINT32_CAST(iter->second) - 1);
            }
        }

        /* If not discarding it, add it to the list at the current index
         */
        if (discard == false)
        {
            /* Add it, indicating that we will delete it and the table should not
             */
            tok->set_tokenIndex(index);
            ++m_p;
            this->insertToken(*tok);
            index++;
			m_nissued++;
			cnt++;
        }

		if( !this->hasReachedFillbufferTarget( cnt, 
						BoolForwarder<ImplTraits::TOKENS_ACCESSED_FROM_OWNING_RULE>()  ) )
			tok = this->get_tokenSource()->nextToken();
		else
			break;
    }

    /* Cache the size so we don't keep doing indirect method calls. We do this as
     * early as possible so that anything after this may utilize the cached value.
     */
    this->get_istream()->set_cachedSize( m_nissued );

    /* Set the consume pointer to the first token that is on our channel, we just read
     */
    m_p = cached_p;
    m_p = this->skipOffTokenChannels( m_p );

}
/// Given a starting index, return the index of the first on-channel
///  token.
///
template<class ImplTraits>
ANTLR_UINT32 CommonTokenStream<ImplTraits>::skipOffTokenChannels(ANTLR_INT32 i)
{
    ANTLR_INT32 n;
    n = this->get_istream()->get_cachedSize();

    while (i < n)
    {
        const TokenType* tok =  this->getToken(i);

        if (tok->get_channel() != m_channel )
        {
            i++;
        }
        else
        {
            return i;
        }
    }
    return i;
}

template<class ImplTraits>
ANTLR_UINT32  CommonTokenStream<ImplTraits>::skipOffTokenChannelsReverse(ANTLR_INT32 x)
{
    while (x >= 0)
    {
        const TokenType* tok =  this->getToken(x);
        
        if( tok->get_channel() != m_channel )
        {
            x--;
        }
        else
        {
            return x;
        }
    }
    return x;
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::discardTokens( ANTLR_MARKER start, ANTLR_MARKER stop )
{
	this->discardTokens( start, stop, BoolForwarder< ImplTraits::TOKENS_ACCESSED_FROM_OWNING_RULE >() );
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::discardTokens( ANTLR_MARKER start, ANTLR_MARKER stop, 
											BoolForwarder<true>  /*tokens_accessed_from_owning_rule */ )
{
	typename TokensType::iterator iter1 = m_tokens.lower_bound(start);
	typename TokensType::iterator iter2 = m_tokens.upper_bound(stop);
	m_tokens.erase( iter1, iter2 );
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::discardTokens( ANTLR_MARKER start, ANTLR_MARKER stop, 
											BoolForwarder<false>  /*tokens_accessed_from_owning_rule*/ )
{
	m_tokens.erase( m_tokens.begin() + start, m_tokens.begin() + stop );
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::insertToken( const TokenType& tok )
{
	this->insertToken( tok, BoolForwarder< ImplTraits::TOKENS_ACCESSED_FROM_OWNING_RULE >() );
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::insertToken( const TokenType& tok, BoolForwarder<true>  /*tokens_accessed_from_owning_rule*/  )
{
	assert( m_tokens.find( tok.get_index() ) == m_tokens.end() );
	assert( tok.get_index() == m_nissued );
	m_tokens[ tok.get_index() ] = tok;
}

template<class ImplTraits>
void CommonTokenStream<ImplTraits>::insertToken( const TokenType& tok, BoolForwarder<false>  /*tokens_accessed_from_owning_rule*/  )
{
	m_tokens.push_back( tok );
}

template<class ImplTraits>
CommonTokenStream<ImplTraits>::~CommonTokenStream()
{
	m_tokens.clear();
}

}
