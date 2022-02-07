namespace antlr3 {

template<class ImplTraits, class StreamType>
ANTLR_ExceptionBase<ImplTraits, StreamType>::ANTLR_ExceptionBase(const StringType& message)
	:m_message(message)
	,m_input(NULL)
{
	m_index = 0;
	m_token	= NULL;
	m_expecting = 0;
	m_expectingSet = NULL;
	m_node = NULL;
	m_c = 0;
	m_line = 0;
	m_charPositionInLine = 0;
	m_decisionNum = 0;
	m_state = 0;
	m_nextException = NULL;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::StringType& ANTLR_ExceptionBase<ImplTraits, StreamType>::get_message()
{
	return m_message;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::StringType& ANTLR_ExceptionBase<ImplTraits, StreamType>::get_streamName()
{
	return m_streamName;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_MARKER ANTLR_ExceptionBase<ImplTraits, StreamType>::get_index() const
{
	return m_index;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE const typename ANTLR_ExceptionBase<ImplTraits, StreamType>::TokenType* ANTLR_ExceptionBase<ImplTraits, StreamType>::get_token() const
{
	return m_token;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::ExceptionBaseType* ANTLR_ExceptionBase<ImplTraits, StreamType>::get_nextException() const
{
	return m_nextException;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 ANTLR_ExceptionBase<ImplTraits, StreamType>::get_expecting() const
{
	return m_expecting;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::BitsetListType* ANTLR_ExceptionBase<ImplTraits, StreamType>::get_expectingSet() const
{
	return m_expectingSet;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::TokenType* ANTLR_ExceptionBase<ImplTraits, StreamType>::get_node() const
{
	return m_node;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UCHAR ANTLR_ExceptionBase<ImplTraits, StreamType>::get_c() const
{
	return m_c;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 ANTLR_ExceptionBase<ImplTraits, StreamType>::get_line() const
{
	return m_line;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_INT32 ANTLR_ExceptionBase<ImplTraits, StreamType>::get_charPositionInLine() const
{
	return m_charPositionInLine;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 ANTLR_ExceptionBase<ImplTraits, StreamType>::get_decisionNum() const
{
	return m_decisionNum;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 ANTLR_ExceptionBase<ImplTraits, StreamType>::get_state() const
{
	return m_state;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::StringType& ANTLR_ExceptionBase<ImplTraits, StreamType>::get_ruleName()
{
	return m_ruleName;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename ANTLR_ExceptionBase<ImplTraits, StreamType>::IntStreamType* ANTLR_ExceptionBase<ImplTraits, StreamType>::get_input() const
{
	return m_input;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_message( const StringType& message )
{
	m_message = message;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_streamName( const StringType& streamName )
{
	m_streamName = streamName;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_index( ANTLR_MARKER index )
{
	m_index = index;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_token( const TokenType* token )
{
	if (m_token)
		delete m_token;
	m_token = token;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_nextException( ExceptionBaseType* nextException )
{
	m_nextException = nextException;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_expecting( ANTLR_UINT32 expecting )
{
	m_expecting = expecting;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_expectingSet( BitsetListType* expectingSet )
{
	m_expectingSet = expectingSet;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_node( TokenType* node )
{
	m_node = node;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_c( ANTLR_UCHAR c )
{
	m_c = c;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_line( ANTLR_UINT32 line )
{
	m_line = line;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_charPositionInLine( ANTLR_INT32 charPositionInLine )
{
	m_charPositionInLine = charPositionInLine;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_decisionNum( ANTLR_UINT32 decisionNum )
{
	m_decisionNum = decisionNum;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_state( ANTLR_UINT32 state )
{
	m_state = state;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_ruleName( const StringType& ruleName )
{
	m_ruleName = ruleName;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void ANTLR_ExceptionBase<ImplTraits, StreamType>::set_input( IntStreamType* input )
{
	m_input = input;
}


template<class ImplTraits, ExceptionType Ex, class StreamType>
	template<typename BaseRecognizerType>
ANTLR_Exception<ImplTraits, Ex, StreamType>::ANTLR_Exception(BaseRecognizerType* recognizer, const StringType& message)
	:BaseType( message )
{
	recognizer->get_super()->fillExceptionData( this );
	BaseType::m_input	= recognizer->get_super()->get_istream();
	BaseType::m_nextException	= recognizer->get_state()->get_exception();	/* So we don't leak the memory */
	recognizer->get_state()->set_exception(this);
	recognizer->get_state()->set_error( true );	    /* Exception is outstanding	*/
}

template<class ImplTraits, ExceptionType Ex, class StreamType>
ANTLR_UINT32 ANTLR_Exception<ImplTraits, Ex, StreamType>::getType() const
{
	return static_cast<ANTLR_UINT32>(Ex);
}

template<class ImplTraits, ExceptionType Ex, class StreamType>
void ANTLR_Exception<ImplTraits, Ex, StreamType>::print() const
{
   /* Ensure valid pointer
     */
	/* Number if no message, else the message
	*/
	if  ( BaseType::m_message.empty() )
	{
		fprintf(stderr, "ANTLR3_EXCEPTION number %d (%08X).\n", Ex, Ex);
	}
	else
	{
		fprintf(stderr, "ANTLR3_EXCEPTION: %s\n", BaseType::m_message.c_str() );
	}
}

template<class ImplTraits, ExceptionType Ex, class StreamType>
typename ANTLR_Exception<ImplTraits, Ex, StreamType>::StringType 
	ANTLR_Exception<ImplTraits, Ex, StreamType>::getName() const
{
	const char* exArray[] = {
						"org.antlr.runtime.RecognitionException"
						, "org.antlr.runtime.MismatchedTokenException"
						, "org.antlr.runtime.NoViableAltException"
						, "org.antlr.runtime.MismatchedSetException"
						, "org.antlr.runtime.EarlyExitException"
						, "org.antlr.runtime.FailedPredicateException"
						, "org.antlr.runtime.MismatchedTreeNodeException"
						, "org.antlr.runtime.tree.RewriteEarlyExitException"
						, "org.antlr.runtime.UnwantedTokenException"
						, "org.antlr.runtime.MissingTokenException"
					  };
	return StringType(exArray[Ex]);
}

template<class ImplTraits, ExceptionType Ex, class StreamType>
void ANTLR_Exception<ImplTraits, Ex, StreamType>::displayRecognitionError( ANTLR_UINT8** tokenNames, 
																			StringStreamType& str_stream ) const
{
	switch( Ex )
	{
	case RECOGNITION_EXCEPTION:
		// Indicates that the recognizer received a token
		// in the input that was not predicted. This is the basic exception type 
		// from which all others are derived. So we assume it was a syntax error.
		// You may get this if there are not more tokens and more are needed
		// to complete a parse for instance.
		//
		str_stream << " : syntax error...\n"; 
		break;
	case UNWANTED_TOKEN_EXCEPTION:
		// Indicates that the recognizer was fed a token which seesm to be
		// spurious input. We can detect this when the token that follows
		// this unwanted token would normally be part of the syntactically
		// correct stream. Then we can see that the token we are looking at
		// is just something that should not be there and throw this exception.
		//
		if	(tokenNames == NULL)
		{
			str_stream << " : Extraneous input...";
		}
		else
		{
			if	( BaseType::m_expecting == ImplTraits::CommonTokenType::TOKEN_EOF)
			{
				str_stream << " : Extraneous input - expected <EOF>\n";
			}
			else
			{
				str_stream << " : Extraneous input - expected "
						   << tokenNames[ BaseType::m_expecting] << " ...\n";
			}
		}
		break;
	case MISSING_TOKEN_EXCEPTION:
		// Indicates that the recognizer detected that the token we just
		// hit would be valid syntactically if preceeded by a particular 
		// token. Perhaps a missing ';' at line end or a missing ',' in an
		// expression list, and such like.
		//
		if	(tokenNames == NULL)
		{
			str_stream << " : Missing token ("
					   << BaseType::m_expecting << ")...\n";
		}
		else
		{
			if	( BaseType::m_expecting == ImplTraits::CommonTokenType::TOKEN_EOF )
			{
				str_stream <<" : Missing <EOF>\n";
			}
			else
			{
				str_stream << " : Missing " << tokenNames[BaseType::m_expecting] <<" \n";
			}
		}
		break;
	case NO_VIABLE_ALT_EXCEPTION:
		// We could not pick any alt decision from the input given
		// so god knows what happened - however when you examine your grammar,
		// you should. It means that at the point where the current token occurred
		// that the DFA indicates nowhere to go from here.
		//
		str_stream << " : cannot match to any predicted input...\n";
		break;
	case MISMATCHED_SET_EXCEPTION:
		{
			ANTLR_UINT32	  count;
			ANTLR_UINT32	  bit;
			ANTLR_UINT32	  size;
			ANTLR_UINT32	  numbits;

			// This means we were able to deal with one of a set of
			// possible tokens at this point, but we did not see any
			// member of that set.
			//
			str_stream << " : unexpected input :";

			// What tokens could we have accepted at this point in the
			// parse?
			//
			count   = 0;
			size    = 0;
			if (BaseType::m_expectingSet != NULL) {
				std::unique_ptr<BitsetType> errBits(BaseType::m_expectingSet->bitsetLoad());
				numbits = errBits->numBits();
				size    = errBits->size();
			}

			if  (size > 0)
			{
				// However many tokens we could have dealt with here, it is usually
				// not useful to print ALL of the set here. I arbitrarily chose 8
				// here, but you should do whatever makes sense for you of course.
				// No token number 0, so look for bit 1 and on.
				//
                str_stream << " expected one of : ";
				for	(bit = 1; bit < numbits && count < 8 && count < size; bit++)
				{
					// TODO: This doesn;t look right - should be asking if the bit is set!!
					//
					if  (tokenNames[bit])
					{
						str_stream <<  ( count > 0 ? ", " : "" )
								   <<  tokenNames[bit]; 
						count++;
					}
				}
				str_stream << "\n";
			}
			else
			{
                str_stream << " nothing is expected here\n";
			}
		}
		break;
	case EARLY_EXIT_EXCEPTION:
		str_stream << " : missing elements...\n";
		break;
	default:
		str_stream << " : syntax not recognized...\n"; 
		break;
	}
}

template<class ImplTraits, class StreamType>
ANTLR_ExceptionBase<ImplTraits,StreamType>::~ANTLR_ExceptionBase()
{
	ANTLR_ExceptionBase<ImplTraits,StreamType>* next;
	ANTLR_ExceptionBase<ImplTraits,StreamType>* ex = m_nextException;

	/* Ensure valid pointer
	 */
	while   (ex != NULL)
	{
		/* Pick up anythign following now, before we free the
		 * current memory block.
		 */
		next	= ex->m_nextException;
		ex->m_nextException = NULL;

		/* Free the actual structure itself
		 */
		delete ex;

		ex = next;
	}
	if ( m_token)
		delete m_token;
}

}
