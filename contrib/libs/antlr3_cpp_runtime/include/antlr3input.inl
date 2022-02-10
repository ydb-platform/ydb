namespace antlr3 {

template<class ImplTraits>
InputStream<ImplTraits>::InputStream(const ANTLR_UINT8* fileName, ANTLR_UINT32 encoding)
{
    // First order of business is to read the file into some buffer space
    // as just straight 8 bit bytes. Then we will work out the encoding and
    // byte order and adjust the API functions that are installed for the
    // default 8Bit stream accordingly.
    //
    this->createFileStream(fileName);

    // We have the data in memory now so we can deal with it according to 
    // the encoding scheme we were given by the user.
    //
    m_encoding = encoding;

    // Now we need to work out the endian type and install any 
    // API functions that differ from 8Bit
    //
    this->setupInputStream();

    // Now we can set up the file name
    //	
    BaseType::m_streamName	= (const char* )fileName;
    m_fileName		= BaseType::m_streamName;
}

template<class ImplTraits>
InputStream<ImplTraits>::InputStream(const ANTLR_UINT8* data, ANTLR_UINT32 encoding, ANTLR_UINT32 size, ANTLR_UINT8* name)
{
	// First order of business is to set up the stream and install the data pointer.
    // Then we will work out the encoding and byte order and adjust the API functions that are installed for the
    // default 8Bit stream accordingly.
    //
    this->createStringStream(data);
    
    // Size (in bytes) of the given 'string'
    //
    m_sizeBuf		= size;

    // We have the data in memory now so we can deal with it according to 
    // the encoding scheme we were given by the user.
    //
    m_encoding = encoding;

    // Now we need to work out the endian type and install any 
    // API functions that differ from 8Bit
    //
    this->setupInputStream();

    // Now we can set up the file name
    //	
    BaseType::m_streamName	= (name == NULL ) ? "" : (const char*)name;
    m_fileName		= BaseType::m_streamName;

}

template<class ImplTraits>
void InputStream<ImplTraits>::createStringStream(const ANTLR_UINT8* data)
{
	if	(data == NULL)
	{
		ParseNullStringException ex;
		throw ex;
	}

	// Structure was allocated correctly, now we can install the pointer
	//
    m_data             = data;
    m_isAllocated	   = false;

	// Call the common 8 bit input stream handler
	// initialization.
	//
	this->genericSetupStream();
}

template<class ImplTraits>
void InputStream<ImplTraits>::createFileStream(const ANTLR_UINT8* fileName)
{
	if	(fileName == NULL)
	{
		ParseFileAbsentException ex;
		throw ex;
	}

	// Structure was allocated correctly, now we can read the file.
	//
	FileUtils<ImplTraits>::AntlrRead8Bit(this, fileName);

	// Call the common 8 bit input stream handler
	// initialization.
	//
	this->genericSetupStream();
}

template<class ImplTraits>
void InputStream<ImplTraits>::genericSetupStream()
{
	this->set_charByteSize(1);
	
    /* Set up the input stream brand new
     */
    this->reset();
    
    /* Install default line separator character (it can be replaced
     * by the grammar programmer later)
     */
    this->set_newLineChar((ANTLR_UCHAR)'\n');
}

template<class ImplTraits>
InputStream<ImplTraits>::~InputStream()
{
	// Free the input stream buffer if we allocated it
    //
    if	(m_isAllocated && (m_data != NULL))
		AllocPolicyType::free((void*)m_data); //const_cast is required
}

template<class ImplTraits>
ANTLR_INLINE const typename InputStream<ImplTraits>::DataType* InputStream<ImplTraits>::get_data() const
{
	return m_data;
}
template<class ImplTraits>
ANTLR_INLINE bool InputStream<ImplTraits>::get_isAllocated() const
{
	return m_isAllocated;
}
template<class ImplTraits>
ANTLR_INLINE const typename InputStream<ImplTraits>::DataType* InputStream<ImplTraits>::get_nextChar() const
{
	return m_nextChar;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 InputStream<ImplTraits>::get_sizeBuf() const
{
	return m_sizeBuf;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 InputStream<ImplTraits>::get_line() const
{
	return m_line;
}
template<class ImplTraits>
ANTLR_INLINE const typename InputStream<ImplTraits>::DataType* InputStream<ImplTraits>::get_currentLine() const
{
	return m_currentLine;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_INT32 InputStream<ImplTraits>::get_charPositionInLine() const
{
	return m_charPositionInLine;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 InputStream<ImplTraits>::get_markDepth() const
{
	return m_markDepth;
}
template<class ImplTraits>
ANTLR_INLINE typename InputStream<ImplTraits>::MarkersType& InputStream<ImplTraits>::get_markers()
{
	return m_markers;
}
template<class ImplTraits>
ANTLR_INLINE const typename InputStream<ImplTraits>::StringType& InputStream<ImplTraits>::get_fileName() const
{
	return m_fileName;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 InputStream<ImplTraits>::get_fileNo() const
{
	return m_fileNo;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UCHAR InputStream<ImplTraits>::get_newlineChar() const
{
	return m_newlineChar;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT8 InputStream<ImplTraits>::get_charByteSize() const
{
	return m_charByteSize;
}
template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 InputStream<ImplTraits>::get_encoding() const
{
	return m_encoding;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_data( DataType* data )
{
	m_data = data;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_isAllocated( bool isAllocated )
{
	m_isAllocated = isAllocated;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_nextChar( const DataType* nextChar )
{
	m_nextChar = nextChar;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_sizeBuf( ANTLR_UINT32 sizeBuf )
{
	m_sizeBuf = sizeBuf;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_line( ANTLR_UINT32 line )
{
	m_line = line;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_currentLine( const DataType* currentLine )
{
	m_currentLine = currentLine;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_charPositionInLine( ANTLR_INT32 charPositionInLine )
{
	m_charPositionInLine = charPositionInLine;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_markDepth( ANTLR_UINT32 markDepth )
{
	m_markDepth = markDepth;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_markers( const MarkersType& markers )
{
	m_markers = markers;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_fileName( const StringType& fileName )
{
	m_fileName = fileName;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_fileNo( ANTLR_UINT32 fileNo )
{
	m_fileNo = fileNo;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_newlineChar( ANTLR_UCHAR newlineChar )
{
	m_newlineChar = newlineChar;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_charByteSize( ANTLR_UINT8 charByteSize )
{
	m_charByteSize = charByteSize;
}
template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::set_encoding( ANTLR_UINT32 encoding )
{
	m_encoding = encoding;
}

template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::inc_charPositionInLine()
{
	++m_charPositionInLine;
}

template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::inc_line()
{
	++m_line;
}

template<class ImplTraits>
ANTLR_INLINE void InputStream<ImplTraits>::inc_markDepth()
{
	++m_markDepth;
}

template<class ImplTraits>
ANTLR_INLINE void	InputStream<ImplTraits>::reset()
{
	m_nextChar		= m_data;	/* Input at first character */
    m_line			= 1;		/* starts at line 1	    */
    m_charPositionInLine	= 0;
    m_currentLine		= m_data;
    m_markDepth		= 0;		/* Reset markers	    */
    
    /* Clear out up the markers table if it is there
     */
	m_markers.clear();
}

template<class ImplTraits>
void    InputStream<ImplTraits>::reuse(ANTLR_UINT8* inString, ANTLR_UINT32 size, ANTLR_UINT8* name)
{
	m_isAllocated	= false;
    m_data		= inString;
    m_sizeBuf	= size;
    
    // Now we can set up the file name. As we are reusing the stream, there may already
    // be a string that we can reuse for holding the filename.
    //
	if	( BaseType::m_streamName.empty() ) 
	{
		BaseType::m_streamName	= ((name == NULL) ? "-memory-" : (const char *)name);
		m_fileName		= BaseType::m_streamName;
	}
	else
	{
		BaseType::m_streamName = ((name == NULL) ? "-memory-" : (const char *)name);
	}

    this->reset();
}

/*
template<class ImplTraits>
typename InputStream<ImplTraits>::DataType*	InputStream<ImplTraits>::LT(ANTLR_INT32 lt)
{
	return this->LA(lt);
}
*/

template<class ImplTraits>
ANTLR_UINT32	InputStream<ImplTraits>::size()
{
	return m_sizeBuf;
}

template<class ImplTraits>
ANTLR_MARKER	InputStream<ImplTraits>::index_impl()
{
	return (ANTLR_MARKER)m_nextChar;
}


template<class ImplTraits>
typename InputStream<ImplTraits>::StringType	InputStream<ImplTraits>::substr(ANTLR_MARKER start, ANTLR_MARKER stop)
{
	std::size_t len = static_cast<std::size_t>( (stop-start)/sizeof(DataType) + 1 );
	StringType str( (const char*)start, len );
	return str;
}

template<class ImplTraits>
ANTLR_UINT32	InputStream<ImplTraits>::get_line()
{
	return m_line;
}

template<class ImplTraits>
const typename InputStream<ImplTraits>::DataType*	InputStream<ImplTraits>::getLineBuf()
{
	return m_currentLine;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32	InputStream<ImplTraits>::get_charPositionInLine()
{
	return m_charPositionInLine;
}

template<class ImplTraits>
ANTLR_INLINE void	InputStream<ImplTraits>::set_charPositionInLine(ANTLR_UINT32 position)
{
	m_charPositionInLine = position;
}

template<class ImplTraits>
void	InputStream<ImplTraits>::set_newLineChar(ANTLR_UINT32 newlineChar)
{
	m_newlineChar = newlineChar;
}

template<class ImplTraits>
ANTLR_INLINE LexState<ImplTraits>::LexState()
{
	m_nextChar = NULL;
	m_line = 0;
	m_currentLine = NULL;
	m_charPositionInLine = 0;
}

template<class ImplTraits>
ANTLR_INLINE const typename LexState<ImplTraits>::DataType* LexState<ImplTraits>::get_nextChar() const
{
	return m_nextChar;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 LexState<ImplTraits>::get_line() const
{
	return m_line;
}

template<class ImplTraits>
ANTLR_INLINE const typename LexState<ImplTraits>::DataType* LexState<ImplTraits>::get_currentLine() const
{
	return m_currentLine;
}

template<class ImplTraits>
ANTLR_INLINE ANTLR_INT32 LexState<ImplTraits>::get_charPositionInLine() const
{
	return m_charPositionInLine;
}

template<class ImplTraits>
ANTLR_INLINE void LexState<ImplTraits>::set_nextChar( const DataType* nextChar )
{
	m_nextChar = nextChar;
}

template<class ImplTraits>
ANTLR_INLINE void LexState<ImplTraits>::set_line( ANTLR_UINT32 line )
{
	m_line = line;
}

template<class ImplTraits>
ANTLR_INLINE void LexState<ImplTraits>::set_currentLine( const DataType* currentLine )
{
	m_currentLine = currentLine;
}

template<class ImplTraits>
ANTLR_INLINE void LexState<ImplTraits>::set_charPositionInLine( ANTLR_INT32 charPositionInLine )
{
	m_charPositionInLine = charPositionInLine;
}

template<class ImplTraits>
ANTLR_INLINE typename InputStream<ImplTraits>::IntStreamType*	InputStream<ImplTraits>::get_istream()
{
	return this;
}

template<class ImplTraits>
void InputStream<ImplTraits>::setupInputStream()
{
	bool  isBigEndian;

    // Used to determine the endianness of the machine we are currently
    // running on.
    //
    ANTLR_UINT16 bomTest = 0xFEFF;
    
    // What endianess is the machine we are running on? If the incoming
    // encoding endianess is the same as this machine's natural byte order
    // then we can use more efficient API calls.
    //
    if  (*((ANTLR_UINT8*)(&bomTest)) == 0xFE)
    {
        isBigEndian = true;
    }
    else
    {
        isBigEndian = false;
    }

    // What encoding did the user tell us {s}he thought it was? I am going
    // to get sick of the questions on antlr-interest, I know I am.
    //
    switch  (m_encoding)
    {
        case    ENC_UTF8:

            // See if there is a BOM at the start of this UTF-8 sequence
            // and just eat it if there is. Windows .TXT files have this for instance
            // as it identifies UTF-8 even though it is of no consequence for byte order
            // as UTF-8 does not have a byte order.
            //
            if  (       (*(m_nextChar))      == 0xEF
                    &&  (*(m_nextChar+1))    == 0xBB
                    &&  (*(m_nextChar+2))    == 0xBF
                )
            {
                // The UTF8 BOM is present so skip it
                //
                m_nextChar += 3;
            }

            // Install the UTF8 input routines
            //
			this->setupIntStream( isBigEndian, isBigEndian );
			this->set_charByteSize(0);
            break;

        case    ENC_UTF16:

            // See if there is a BOM at the start of the input. If not then
            // we assume that the byte order is the natural order of this
            // machine (or it is really UCS2). If there is a BOM we determine if the encoding
            // is the same as the natural order of this machine.
            //
            if  (       (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar))      == 0xFE
                    &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+1))    == 0xFF
                )
            {
                // BOM Present, indicates Big Endian
                //
                m_nextChar += 1;

				this->setupIntStream( isBigEndian, true );
            }
            else if  (      (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar))      == 0xFF
                        &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+1))    == 0xFE
                )
            {
                // BOM present, indicates Little Endian
                //
                m_nextChar += 1;

                this->setupIntStream( isBigEndian, false );
            }
            else
            {
                // No BOM present, assume local computer byte order
                //
                this->setupIntStream(isBigEndian, isBigEndian);
            }
			this->set_charByteSize(2);
            break;

        case    ENC_UTF32:

            // See if there is a BOM at the start of the input. If not then
            // we assume that the byte order is the natural order of this
            // machine. If there is we determine if the encoding
            // is the same as the natural order of this machine.
            //
            if  (       (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar))      == 0x00
                    &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+1))    == 0x00
                    &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+2))    == 0xFE
                    &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+3))    == 0xFF
                )
            {
                // BOM Present, indicates Big Endian
                //
                m_nextChar += 1;

                this->setupIntStream(isBigEndian, true);
            }
            else if  (      (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar))      == 0xFF
                        &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+1))    == 0xFE
                        &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+1))    == 0x00
                        &&  (ANTLR_UINT8)(*((ANTLR_UINT8*)m_nextChar+1))    == 0x00
                )
            {
                // BOM present, indicates Little Endian
                //
                m_nextChar += 1;

				this->setupIntStream( isBigEndian, false );
            }
            else
            {
                // No BOM present, assume local computer byte order
                //
				this->setupIntStream( isBigEndian, isBigEndian );
            }
			this->set_charByteSize(4);
            break;

        case    ENC_UTF16BE:

            // Encoding is definately Big Endian with no BOM
            //
			this->setupIntStream( isBigEndian, true );
			this->set_charByteSize(2);
            break;

        case    ENC_UTF16LE:

            // Encoding is definately Little Endian with no BOM
            //
            this->setupIntStream( isBigEndian, false );
			this->set_charByteSize(2);
            break;

        case    ENC_UTF32BE:

            // Encoding is definately Big Endian with no BOM
            //
			this->setupIntStream( isBigEndian, true );
			this->set_charByteSize(4);
            break;

        case    ENC_UTF32LE:

            // Encoding is definately Little Endian with no BOM
            //
			this->setupIntStream( isBigEndian, false );
			this->set_charByteSize(4);
            break;

        case    ENC_EBCDIC:

            // EBCDIC is basically the same as ASCII but with an on the
            // fly translation to ASCII
            //
            this->setupIntStream( isBigEndian, isBigEndian );
			this->set_charByteSize(1);
            break;

        case    ENC_8BIT:
        default:

            // Standard 8bit/ASCII
            //
            this->setupIntStream( isBigEndian, isBigEndian );
			this->set_charByteSize(1);
            break;
    }    
}

}
