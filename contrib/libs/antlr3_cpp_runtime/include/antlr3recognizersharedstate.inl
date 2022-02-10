namespace antlr3 {

template<class ImplTraits, class StreamType>
RecognizerSharedState<ImplTraits, StreamType>::RecognizerSharedState()
{
	m_exception = NULL;
	m_sizeHint = 0;
	m_error = false;
	m_errorRecovery = false;
	m_failed = false;
	m_token_present = false;
	m_lastErrorIndex = 0;
	m_errorCount = 0;
	m_backtracking = false;
	m_ruleMemo = NULL;
	m_tokenNames = NULL;
	m_tokSource = NULL;
	m_channel = 0;
	m_type = 0;
	m_tokenStartLine = 0;
	m_tokenStartCharPositionInLine = 0;
	m_tokenStartCharIndex = 0;
	m_treeAdaptor = NULL;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::FollowingType& RecognizerSharedState<ImplTraits, StreamType>::get_following()
{
	return m_following;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 RecognizerSharedState<ImplTraits, StreamType>::get_sizeHint() const
{
	return m_sizeHint;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE bool RecognizerSharedState<ImplTraits, StreamType>::get_error() const
{
	return m_error;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::ExceptionBaseType* 
RecognizerSharedState<ImplTraits, StreamType>::get_exception() const
{
	return m_exception;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE bool RecognizerSharedState<ImplTraits, StreamType>::get_errorRecovery() const
{
	return m_errorRecovery;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE bool RecognizerSharedState<ImplTraits, StreamType>::get_failed() const
{
	return m_failed;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE bool RecognizerSharedState<ImplTraits, StreamType>::get_token_present() const
{
	return m_token_present;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_MARKER RecognizerSharedState<ImplTraits, StreamType>::get_lastErrorIndex() const
{
	return m_lastErrorIndex;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 RecognizerSharedState<ImplTraits, StreamType>::get_errorCount() const
{
	return m_errorCount;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_INT32 RecognizerSharedState<ImplTraits, StreamType>::get_backtracking() const
{
	return m_backtracking;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::RuleMemoType* RecognizerSharedState<ImplTraits, StreamType>::get_ruleMemo() const
{
	return m_ruleMemo;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT8** RecognizerSharedState<ImplTraits, StreamType>::get_tokenNames() const
{
	return m_tokenNames;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT8* RecognizerSharedState<ImplTraits, StreamType>::get_tokenName( ANTLR_UINT32 i ) const
{
	return m_tokenNames[i];
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::CommonTokenType* RecognizerSharedState<ImplTraits, StreamType>::get_token()
{
	return &m_token;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::TokenSourceType* RecognizerSharedState<ImplTraits, StreamType>::get_tokSource() const
{
	return m_tokSource;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32& RecognizerSharedState<ImplTraits, StreamType>::get_channel()
{
	return m_channel;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_UINT32 RecognizerSharedState<ImplTraits, StreamType>::get_type() const
{
	return m_type;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_INT32 RecognizerSharedState<ImplTraits, StreamType>::get_tokenStartLine() const
{
	return m_tokenStartLine;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_INT32 RecognizerSharedState<ImplTraits, StreamType>::get_tokenStartCharPositionInLine() const
{
	return m_tokenStartCharPositionInLine;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE ANTLR_MARKER RecognizerSharedState<ImplTraits, StreamType>::get_tokenStartCharIndex() const
{
	return m_tokenStartCharIndex;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::StringType& RecognizerSharedState<ImplTraits, StreamType>::get_text()
{
	return m_text;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::StreamsType& RecognizerSharedState<ImplTraits, StreamType>::get_streams()
{
	return m_streams;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE typename RecognizerSharedState<ImplTraits, StreamType>::TreeAdaptorType* RecognizerSharedState<ImplTraits, StreamType>::get_treeAdaptor() const
{
	return m_treeAdaptor;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_exception( ExceptionBaseType* exception )
{
	m_exception = exception;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_following( const FollowingType& following )
{
	m_following = following;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_sizeHint( ANTLR_UINT32 sizeHint )
{
	m_sizeHint = sizeHint;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_error( bool error )
{
	m_error = error;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_errorRecovery( bool errorRecovery )
{
	m_errorRecovery = errorRecovery;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_failed( bool failed )
{
	m_failed = failed;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void  RecognizerSharedState<ImplTraits, StreamType>::set_token_present(bool token_present)
{
	m_token_present = token_present;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_lastErrorIndex( ANTLR_MARKER lastErrorIndex )
{
	m_lastErrorIndex = lastErrorIndex;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_errorCount( ANTLR_UINT32 errorCount )
{
	m_errorCount = errorCount;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_backtracking( ANTLR_INT32 backtracking )
{
	m_backtracking = backtracking;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_ruleMemo( RuleMemoType* ruleMemo )
{
	m_ruleMemo = ruleMemo;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_tokenNames( ANTLR_UINT8** tokenNames )
{
	m_tokenNames = tokenNames;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_tokSource( TokenSourceType* tokSource )
{
	m_tokSource = tokSource;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_channel( ANTLR_UINT32 channel )
{
	m_channel = channel;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void  RecognizerSharedState<ImplTraits, StreamType>::set_token(const CommonTokenType* tok)
{
	this->set_token_present( tok != NULL );
	if( tok != NULL )
		m_token = *tok;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_type( ANTLR_UINT32 type )
{
	m_type = type;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_tokenStartLine( ANTLR_INT32 tokenStartLine )
{
	m_tokenStartLine = tokenStartLine;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_tokenStartCharPositionInLine( ANTLR_INT32 tokenStartCharPositionInLine )
{
	m_tokenStartCharPositionInLine = tokenStartCharPositionInLine;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_tokenStartCharIndex( ANTLR_MARKER tokenStartCharIndex )
{
	m_tokenStartCharIndex = tokenStartCharIndex;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_text( const StringType& text )
{
	m_text = text;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_streams( const InputStreamsType& streams )
{
	m_streams = streams;
}
template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::set_treeAdaptor( TreeAdaptorType* adaptor )
{
	m_treeAdaptor = adaptor;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::inc_errorCount()
{
	++m_errorCount;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::inc_backtracking()
{
	++m_backtracking;
}

template<class ImplTraits, class StreamType>
ANTLR_INLINE void RecognizerSharedState<ImplTraits, StreamType>::dec_backtracking()
{
	--m_backtracking;
}

}
