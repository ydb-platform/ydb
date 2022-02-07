namespace antlr3 {

template<class ImplTraits>
CommonToken<ImplTraits>::CommonToken()
{
	m_type = 0;
    m_channel = 0;
	m_lineStart = NULL;
	m_line = 0;
	m_charPositionInLine = 0;
	m_input = NULL;
	m_index = 0;
	m_startIndex = 0;
	m_stopIndex = 0;
}

template<class ImplTraits>
CommonToken<ImplTraits>::CommonToken(ANTLR_UINT32 type)
{
	m_type = type;
	m_channel = 0;
	m_lineStart = NULL;
	m_line = 0;
	m_charPositionInLine = 0;
	m_input = NULL;
	m_index = 0;
	m_startIndex = 0;
	m_stopIndex = 0;
}

template<class ImplTraits>
CommonToken<ImplTraits>::CommonToken(TOKEN_TYPE type)
{
	m_type = type;
	m_channel = 0;
	m_lineStart = NULL;
	m_line = 0;
	m_charPositionInLine = 0;
	m_input = NULL;
	m_index = 0;
	m_startIndex = 0;
	m_stopIndex = 0;
}

template<class ImplTraits>
CommonToken<ImplTraits>::CommonToken( const CommonToken& ctoken )
	:m_tokText( ctoken.m_tokText )
	,UserData(ctoken.UserData)	 
{
	m_type = ctoken.m_type;
	m_channel = ctoken.m_channel;
	m_lineStart = ctoken.m_lineStart;
	m_line = ctoken.m_line;
	m_charPositionInLine = ctoken.m_charPositionInLine;
	m_input = ctoken.m_input;
	m_index = ctoken.m_index;
	m_startIndex = ctoken.m_startIndex;
	m_stopIndex = ctoken.m_stopIndex;
}

template<class ImplTraits>
CommonToken<ImplTraits>& CommonToken<ImplTraits>::operator=( const CommonToken& ctoken )
{
	UserData = ctoken.UserData;
	m_type = ctoken.m_type;
	m_channel = ctoken.m_channel;
	m_lineStart = ctoken.m_lineStart;
	m_line = ctoken.m_line;
	m_charPositionInLine = ctoken.m_charPositionInLine;
	m_input = ctoken.m_input;
	m_index = ctoken.m_index;
	m_startIndex = ctoken.m_startIndex;
	m_stopIndex = ctoken.m_stopIndex;

	m_tokText = ctoken.m_tokText;
	return *this;
}

template<class ImplTraits>
ANTLR_INLINE bool CommonToken<ImplTraits>::operator<( const CommonToken& ctoken ) const
{
	return (m_index < ctoken.m_index);
}

template<class ImplTraits>
bool CommonToken<ImplTraits>::operator==( const CommonToken& ctoken ) const
{
	return ( (m_type == ctoken.m_type) &&
		     (m_channel == ctoken.m_channel) &&
			 (m_lineStart == ctoken.m_lineStart) &&
			 (m_line == ctoken.m_line) &&
			 (m_charPositionInLine == ctoken.m_charPositionInLine) &&
			 (m_input == ctoken.m_input) &&
			 (m_index == ctoken.m_index) &&
			 (m_startIndex == ctoken.m_startIndex) &&
			 (m_stopIndex == ctoken.m_stopIndex) );
}

template<class ImplTraits>
ANTLR_INLINE typename CommonToken<ImplTraits>::InputStreamType* CommonToken<ImplTraits>::get_input() const
{
	return m_input;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_MARKER CommonToken<ImplTraits>::get_index() const
{
	return m_index;
}

template<class ImplTraits>
ANTLR_INLINE void CommonToken<ImplTraits>::set_index( ANTLR_MARKER index )
{
	m_index = index;
}

template<class ImplTraits>
void CommonToken<ImplTraits>::set_input( InputStreamType* input )
{
	m_input = input;
}

template<class ImplTraits>
typename CommonToken<ImplTraits>::StringType const &
CommonToken<ImplTraits>::getText() const
{
	static const StringType EOF_STRING("<EOF>");
	static const StringType EMPTY_STRING("");

	if ( !m_tokText.empty() )
		return m_tokText;

	// EOF is a special case
	//
	if ( m_type == TOKEN_EOF)
	{
		return EOF_STRING;
	}

	// We had nothing installed in the token, create a new string
	// from the input stream
	//
	if ( m_input != NULL)
	{
		return m_tokText = m_input->substr( this->get_startIndex(), this->get_stopIndex() );
	}
	// Nothing to return, there is no input stream
	//
	return EMPTY_STRING;
}

template<class ImplTraits>
ANTLR_INLINE void CommonToken<ImplTraits>::set_tokText( const StringType& text )
{
	m_tokText = text;
}

template<class ImplTraits>
ANTLR_INLINE void CommonToken<ImplTraits>::setText(ANTLR_UINT8* text)
{
	if( text == NULL )
		m_tokText.clear();
	else
		m_tokText = (const char*) text;
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::setText(const char* text)
{
	if( text == NULL )
		m_tokText.clear();
	else
		m_tokText = text;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32  CommonToken<ImplTraits>::get_type() const
{
	return m_type;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32  CommonToken<ImplTraits>::getType() const
{
	return m_type;
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::set_type(ANTLR_UINT32 ttype)
{
	m_type = ttype;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32   CommonToken<ImplTraits>::get_line() const
{
	return m_line;
}

template<class ImplTraits>
ANTLR_INLINE void CommonToken<ImplTraits>::set_line(ANTLR_UINT32 line)
{
	m_line = line;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_INT32  CommonToken<ImplTraits>::get_charPositionInLine() const
{
	return m_charPositionInLine;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_INT32  CommonToken<ImplTraits>::getCharPositionInLine() const
{
	return this->get_charPositionInLine();
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::set_charPositionInLine(ANTLR_INT32 pos)
{
	m_charPositionInLine = pos;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32   CommonToken<ImplTraits>::get_channel() const
{
	return m_channel;
}

template<class ImplTraits>
ANTLR_INLINE void CommonToken<ImplTraits>::set_channel(ANTLR_UINT32 channel)
{
	m_channel = channel;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_MARKER  CommonToken<ImplTraits>::get_tokenIndex() const
{
	return m_index;
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::set_tokenIndex(ANTLR_MARKER tokenIndex)
{
	m_index = tokenIndex;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_MARKER   CommonToken<ImplTraits>::get_startIndex() const
{
	return (m_startIndex == -1) ? (ANTLR_MARKER)(m_input->get_data()) : m_startIndex;
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::set_startIndex(ANTLR_MARKER index)
{
	m_startIndex = index;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_MARKER  CommonToken<ImplTraits>::get_stopIndex() const
{
	return m_stopIndex;
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::set_stopIndex(ANTLR_MARKER index)
{
	m_stopIndex = index;
}

template<class ImplTraits>
ANTLR_INLINE const typename CommonToken<ImplTraits>::StreamDataType* CommonToken<ImplTraits>::get_lineStart() const
{
	return m_lineStart;
}

template<class ImplTraits>
ANTLR_INLINE void	CommonToken<ImplTraits>::set_lineStart( const StreamDataType* lineStart )
{
	m_lineStart = lineStart;
}

template<class ImplTraits>
typename CommonToken<ImplTraits>::StringType  CommonToken<ImplTraits>::toString() const
{
    StringType  text;
    typedef typename ImplTraits::StringStreamType StringStreamType;
    StringStreamType  outtext; 

    text    =	this->getText();

    if	(text.empty())
		return "";

    /* Now we use our handy dandy string utility to assemble the
     * the reporting string
     * return "[@"+getTokenIndex()+","+start+":"+stop+"='"+txt+"',<"+type+">"+channelStr+","+line+":"+getCharPositionInLine()+"]";
     */
    outtext << "[Index: ";
    outtext << (int)this->get_tokenIndex();
    outtext << " (Start: ";
    outtext << (int)this->get_startIndex();
    outtext << "-Stop: ";
    outtext << (int)this->get_stopIndex();
    outtext << ") ='";
    outtext << text;
    outtext << "', type<";
    outtext << (int)m_type;
    outtext << "> ";

    if	(this->get_channel() > TOKEN_DEFAULT_CHANNEL)
    {
		outtext << "(channel = ";
		outtext << (int)this->get_channel();
		outtext << ") ";
    }

    outtext << "Line: ";
    outtext << (int)this->get_line();
    outtext << " LinePos:";
    outtext << (int)this->get_charPositionInLine();
    outtext << "]";

    return  outtext.str();
}

}
