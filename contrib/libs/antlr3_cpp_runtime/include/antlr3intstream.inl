namespace antlr3 {

template<class ImplTraits, class SuperType>
ANTLR_INLINE IntStream<ImplTraits, SuperType>::IntStream()
{
	m_lastMarker = 0;
	m_upper_case = false;
}

template<class ImplTraits, class SuperType>
ANTLR_INLINE typename IntStream<ImplTraits, SuperType>::StringType	IntStream<ImplTraits, SuperType>::getSourceName()
{
	return m_streamName;
}

template<class ImplTraits, class SuperType>
ANTLR_INLINE typename IntStream<ImplTraits, SuperType>::StringType& 	IntStream<ImplTraits, SuperType>::get_streamName()
{
	return m_streamName;
}

template<class ImplTraits, class SuperType>
ANTLR_INLINE const typename IntStream<ImplTraits, SuperType>::StringType& 	IntStream<ImplTraits, SuperType>::get_streamName() const
{
	return m_streamName;
}

template<class ImplTraits, class SuperType>
ANTLR_INLINE ANTLR_MARKER IntStream<ImplTraits, SuperType>::get_lastMarker() const
{
	return m_lastMarker;
}

template<class ImplTraits, class SuperType>
ANTLR_INLINE void	IntStream<ImplTraits, SuperType>::setUcaseLA(bool flag)
{
	m_upper_case = flag;
}

template<class ImplTraits, class SuperType>
ANTLR_INLINE SuperType* IntStream<ImplTraits, SuperType>::get_super()
{
	return static_cast<SuperType*>(this);
}

template<class ImplTraits, class SuperType>
void	IntStream<ImplTraits, SuperType>::consume()
{
	SuperType* input = this->get_super();

	const ANTLR_UINT8* nextChar = input->get_nextChar();
	const ANTLR_UINT8* data = input->get_data();
	ANTLR_UINT32 sizeBuf = input->get_sizeBuf();

    if	( nextChar < ( data + sizeBuf ) )
    {	
		/* Indicate one more character in this line
		 */
		input->inc_charPositionInLine();
	
		if  ((ANTLR_UCHAR)(*(nextChar)) == input->get_newlineChar() )
		{
			/* Reset for start of a new line of input
			 */
			input->inc_line();
			input->set_charPositionInLine(0);
			input->set_currentLine(nextChar + 1);
		}

		/* Increment to next character position
		 */
		input->set_nextChar( nextChar + 1 );
    }
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la )
{
	SuperType* input = this->get_super();
	const ANTLR_UINT8* nextChar = input->get_nextChar();
	const ANTLR_UINT8* data = input->get_data();
	ANTLR_UINT32 sizeBuf = input->get_sizeBuf();

    if	(( nextChar + la - 1) >= (data + sizeBuf))
    {
		return	ANTLR_CHARSTREAM_EOF;
    }
    else
    {
		if( !m_upper_case )
			return	(ANTLR_UCHAR)(*(nextChar + la - 1));
		else
			return	(ANTLR_UCHAR)toupper(*(nextChar + la - 1));
    }
}

template<class ImplTraits, class SuperType>
ANTLR_MARKER IntStream<ImplTraits, SuperType>::mark()
{
	LexState<ImplTraits>*	    state;
    SuperType* input = this->get_super();

    /* New mark point 
     */
    input->inc_markDepth();

    /* See if we are revisiting a mark as we can just reuse the vector
     * entry if we are, otherwise, we need a new one
     */
    if	(input->get_markDepth() > input->get_markers().size() )
    {	
		input->get_markers().push_back( LexState<ImplTraits>() );
		LexState<ImplTraits>& state_r = input->get_markers().back();
		state = &state_r;
    }
    else
    {
		LexState<ImplTraits>& state_r = input->get_markers().at( input->get_markDepth() - 1 );
		state	= &state_r;

		/* Assume no errors for speed, it will just blow up if the table failed
		 * for some reasons, hence lots of unit tests on the tables ;-)
		 */
    }

    /* We have created or retrieved the state, so update it with the current
     * elements of the lexer state.
     */
    state->set_charPositionInLine( input->get_charPositionInLine() );
    state->set_currentLine( input->get_currentLine() );
    state->set_line( input->get_line() );
    state->set_nextChar( input->get_nextChar() );

    m_lastMarker = input->get_markDepth();

    /* And that's it
     */
    return  input->get_markDepth();
}

template<class ImplTraits, class SuperType>
ANTLR_MARKER	IntStream<ImplTraits, SuperType>::index()
{
	SuperType* input = this->get_super();
	return input->index_impl();
}

template<class ImplTraits, class SuperType>
void	IntStream<ImplTraits, SuperType>::rewind(ANTLR_MARKER mark)
{
    SuperType* input = this->get_super();

    /* Perform any clean up of the marks
     */
    this->release(mark);

    /* Find the supplied mark state 
     */
	ANTLR_UINT32 idx = static_cast<ANTLR_UINT32>( mark-1 );
    typename ImplTraits::LexStateType&   state = input->get_markers().at( idx );

    /* Seek input pointer to the requested point (note we supply the void *pointer
     * to whatever is implementing the int stream to seek).
     */
	this->seek( (ANTLR_MARKER)state.get_nextChar() );
    
    /* Reset to the reset of the information in the mark
     */
    input->set_charPositionInLine( state.get_charPositionInLine() );
    input->set_currentLine( state.get_currentLine() );
    input->set_line( state.get_line() );
    input->set_nextChar( state.get_nextChar() );

    /* And we are done
     */
}

template<class ImplTraits, class SuperType>
void	IntStream<ImplTraits, SuperType>::rewindLast()
{
	this->rewind(m_lastMarker);
}

template<class ImplTraits, class SuperType>
void	IntStream<ImplTraits, SuperType>::release(ANTLR_MARKER mark)
{
	SuperType* input = this->get_super();

	/* We don't do much here in fact as we never free any higher marks in
     * the hashtable as we just resuse any memory allocated for them.
     */
    input->set_markDepth( (ANTLR_UINT32)(mark - 1) );

}

template<class ImplTraits, class SuperType>
void IntStream<ImplTraits, SuperType>::setupIntStream(bool, bool)
{
}

template<class ImplTraits, class SuperType>
void	IntStream<ImplTraits, SuperType>::seek(ANTLR_MARKER seekPoint)
{
	ANTLR_INT32   count;
	SuperType* input = this->get_super();

	ANTLR_MARKER nextChar = (ANTLR_MARKER) input->get_nextChar();
	/* If the requested seek point is less than the current
	* input point, then we assume that we are resetting from a mark
	* and do not need to scan, but can just set to there.
	*/
	if	(seekPoint <= nextChar)
	{
		input->set_nextChar((ANTLR_UINT8*) seekPoint);
	}
	else
	{
		count	= (ANTLR_UINT32)(seekPoint - nextChar);

		while (count--)
		{
			this->consume();
		}
	}
}

template<class ImplTraits, class SuperType>
IntStream<ImplTraits, SuperType>::~IntStream()
{
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	EBCDIC_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la)
{
	// EBCDIC to ASCII conversion table
	//
	// This for EBCDIC EDF04 translated to ISO-8859.1 which is the usually accepted POSIX
	// translation and the character tables are published all over the interweb.
	// 
	const ANTLR_UCHAR e2a[256] =
	{
		0x00, 0x01, 0x02, 0x03, 0x85, 0x09, 0x86, 0x7f,
		0x87, 0x8d, 0x8e, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
		0x10, 0x11, 0x12, 0x13, 0x8f, 0x0a, 0x08, 0x97,
		0x18, 0x19, 0x9c, 0x9d, 0x1c, 0x1d, 0x1e, 0x1f,
		0x80, 0x81, 0x82, 0x83, 0x84, 0x92, 0x17, 0x1b,
		0x88, 0x89, 0x8a, 0x8b, 0x8c, 0x05, 0x06, 0x07, 
		0x90, 0x91, 0x16, 0x93, 0x94, 0x95, 0x96, 0x04,
		0x98, 0x99, 0x9a, 0x9b, 0x14, 0x15, 0x9e, 0x1a,
		0x20, 0xa0, 0xe2, 0xe4, 0xe0, 0xe1, 0xe3, 0xe5,
		0xe7, 0xf1, 0x60, 0x2e, 0x3c, 0x28, 0x2b, 0x7c,
		0x26, 0xe9, 0xea, 0xeb, 0xe8, 0xed, 0xee, 0xef,
		0xec, 0xdf, 0x21, 0x24, 0x2a, 0x29, 0x3b, 0x9f,
		0x2d, 0x2f, 0xc2, 0xc4, 0xc0, 0xc1, 0xc3, 0xc5,
		0xc7, 0xd1, 0x5e, 0x2c, 0x25, 0x5f, 0x3e, 0x3f,
		0xf8, 0xc9, 0xca, 0xcb, 0xc8, 0xcd, 0xce, 0xcf,
		0xcc, 0xa8, 0x3a, 0x23, 0x40, 0x27, 0x3d, 0x22,
		0xd8, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x67,
		0x68, 0x69, 0xab, 0xbb, 0xf0, 0xfd, 0xfe, 0xb1,
		0xb0, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70,
		0x71, 0x72, 0xaa, 0xba, 0xe6, 0xb8, 0xc6, 0xa4,
		0xb5, 0xaf, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78,
		0x79, 0x7a, 0xa1, 0xbf, 0xd0, 0xdd, 0xde, 0xae,
		0xa2, 0xa3, 0xa5, 0xb7, 0xa9, 0xa7, 0xb6, 0xbc,
		0xbd, 0xbe, 0xac, 0x5b, 0x5c, 0x5d, 0xb4, 0xd7,
		0xf9, 0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47,
		0x48, 0x49, 0xad, 0xf4, 0xf6, 0xf2, 0xf3, 0xf5,
		0xa6, 0x4a, 0x4b, 0x4c, 0x4d, 0x4e, 0x4f, 0x50,
		0x51, 0x52, 0xb9, 0xfb, 0xfc, 0xdb, 0xfa, 0xff,
		0xd9, 0xf7, 0x53, 0x54, 0x55, 0x56, 0x57, 0x58,
		0x59, 0x5a, 0xb2, 0xd4, 0xd6, 0xd2, 0xd3, 0xd5,
		0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
		0x38, 0x39, 0xb3, 0x7b, 0xdc, 0x7d, 0xda, 0x7e
	};

	SuperType* input = this->get_super();

    if	(( input->get_nextChar() + la - 1) >= ( input->get_data() + input->get_sizeBuf() ))
    {
        return	ANTLR_CHARSTREAM_EOF;
    }
    else
    {
        // Translate the required character via the constant conversion table
        //
        return	e2a[(*(input->get_nextChar() + la - 1))];
    }
}

template<class ImplTraits, class SuperType>
void EBCDIC_IntStream<ImplTraits, SuperType>::setupIntStream()
{
	SuperType* super = this->get_super();
	super->set_charByteSize(1);
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF16_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 i)
{
	return this->LA(i, ClassForwarder< typename ImplTraits::Endianness >() );
}

template<class ImplTraits, class SuperType>
void UTF16_IntStream<ImplTraits, SuperType>::consume()
{
	this->consume( ClassForwarder< typename ImplTraits::Endianness >() );
}

template<class ImplTraits, class SuperType>
ANTLR_MARKER	UTF16_IntStream<ImplTraits, SuperType>::index()
{
	SuperType* input = this->get_super();
    return  (ANTLR_MARKER)(input->get_nextChar());
}

template<class ImplTraits, class SuperType>
void UTF16_IntStream<ImplTraits, SuperType>::seek(ANTLR_MARKER seekPoint)
{
	SuperType* input = this->get_super();

	// If the requested seek point is less than the current
	// input point, then we assume that we are resetting from a mark
	// and do not need to scan, but can just set to there as rewind will
    // reset line numbers and so on.
	//
	if	(seekPoint <= (ANTLR_MARKER)(input->get_nextChar()))
	{
		input->set_nextChar( seekPoint );
	}
	else
	{
        // Call consume until we reach the asked for seek point or EOF
        //
        while( (this->LA(1) != ANTLR_CHARSTREAM_EOF) && (seekPoint < (ANTLR_MARKER)input->get_nextChar() ) )
	    {
			this->consume();
	    }
	}
}

template<class ImplTraits, class SuperType>
void IntStream<ImplTraits, SuperType>::findout_endian_spec(bool machineBigEndian, bool inputBigEndian)
{
	// We must install different UTF16 routines according to whether the input
	// is the same endianess as the machine we are executing upon or not. If it is not
	// then we must install methods that can convert the endianess on the fly as they go
	//

	if(machineBigEndian == true)
	{
		// Machine is Big Endian, if the input is also then install the 
		// methods that do not access input by bytes and reverse them.
		// Otherwise install endian aware methods.
		//
		if  (inputBigEndian == true) 
		{
			// Input is machine compatible
			//
			m_endian_spec = 1;
		}
		else
		{
			// Need to use methods that know that the input is little endian
			//
			m_endian_spec = 2;
		}
	}
	else
	{
		// Machine is Little Endian, if the input is also then install the 
		// methods that do not access input by bytes and reverse them.
		// Otherwise install endian aware methods.
		//
		if  (inputBigEndian == false) 
		{
			// Input is machine compatible
			//
			m_endian_spec =  1;
		}
		else
		{
			// Need to use methods that know that the input is Big Endian
			//
			m_endian_spec	= 3;
		}
	}
}

template<class ImplTraits, class SuperType>
void UTF16_IntStream<ImplTraits, SuperType>::setupIntStream(bool machineBigEndian, bool inputBigEndian)
{
	SuperType* super = this->get_super();
	super->set_charByteSize(2);

	this->findout_endian_spec( machineBigEndian, inputBigEndian );
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32 IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 i, ClassForwarder<RESOLVE_ENDIAN_AT_RUNTIME> )
{
	assert( (m_endian_spec >= 1) && (m_endian_spec <= 3));
	switch(m_endian_spec)
	{
	case 1:
		return this->LA(i, ClassForwarder<BYTE_AGNOSTIC>() );
		break;
	case 2:
		return this->LA(i, ClassForwarder<ANTLR_LITTLE_ENDIAN>() );
		break;
	case 3:
		return this->LA(i, ClassForwarder<ANTLR_BIG_ENDIAN>() );
		break;
	default:
		break;
	}
	return 0;
}

template<class ImplTraits, class SuperType>
void	IntStream<ImplTraits, SuperType>::consume( ClassForwarder<RESOLVE_ENDIAN_AT_RUNTIME> )
{
	assert( (m_endian_spec >= 1) && (m_endian_spec <= 3));
	switch(m_endian_spec)
	{
	case 1:
		this->consume( ClassForwarder<BYTE_AGNOSTIC>() );
		break;
	case 2:
		this->consume( ClassForwarder<ANTLR_LITTLE_ENDIAN>() );
		break;
	case 3:
		this->consume( ClassForwarder<ANTLR_BIG_ENDIAN>() );
		break;
	default:
		break;
	}
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF16_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la, ClassForwarder<BYTE_AGNOSTIC> )
{
	SuperType* input;
    UTF32   ch;
    UTF32   ch2;
    UTF16*	nextChar;

    // Find the input interface and where we are currently pointing to
    // in the input stream
    //
	input   = this->get_super;
	nextChar    = input->get_nextChar();

    // If a positive offset then advance forward, else retreat
    //
    if  (la >= 0)
    {
        while   (--la > 0 && (ANTLR_UINT8*)nextChar < ((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf() )
        {
            // Advance our copy of the input pointer
            //
            // Next char in natural machine byte order
            //
            ch  = *nextChar++;

            // If we have a surrogate pair then we need to consume
            // a following valid LO surrogate.
            //
            if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
            {
                // If the 16 bits following the high surrogate are in the source buffer...
                //
                if	((ANTLR_UINT8*)(nextChar) < (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf() ))
                {
                    // Next character is in natural machine byte order
                    //
                    ch2 = *nextChar;

                    // If it's a valid low surrogate, consume it
                    //
                    if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                    {
                        // We consumed one 16 bit character
                        //
						nextChar++;
                    }
                    // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                    // it.
                    //
                } 
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it because the buffer ended
                //
            }
            // Note that we did not check for an invalid low surrogate here, or that fact that the
            // lo surrogate was missing. We just picked out one 16 bit character unless the character
            // was a valid hi surrogate, in whcih case we consumed two 16 bit characters.
            //
        }
    }
    else
    {
        // We need to go backwards from our input point
        //
        while   (la++ < 0 && (ANTLR_UINT8*)nextChar > (ANTLR_UINT8*)input->get_data() )
        {
            // Get the previous 16 bit character
            //
            ch = *--nextChar;

            // If we found a low surrogate then go back one more character if
            // the hi surrogate is there
            //
            if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END) 
            {
                ch2 = *(nextChar-1);
                if (ch2 >= UNI_SUR_HIGH_START && ch2 <= UNI_SUR_HIGH_END) 
                {
                    // Yes, there is a high surrogate to match it so decrement one more and point to that
                    //
                    nextChar--;
                }
            }
        }
    }

    // Our local copy of nextChar is now pointing to either the correct character or end of file
    //
    // Input buffer size is always in bytes
    //
	if	( (ANTLR_UINT8*)nextChar >= (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf() ))
	{
		return	ANTLR_CHARSTREAM_EOF;
	}
	else
	{
        // Pick up the next 16 character (native machine byte order)
        //
        ch = *nextChar++;

        // If we have a surrogate pair then we need to consume
        // a following valid LO surrogate.
        //
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
        {
            // If the 16 bits following the high surrogate are in the source buffer...
            //
            if	((ANTLR_UINT8*)(nextChar) < (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf()))
            {
                // Next character is in natural machine byte order
                //
                ch2 = *nextChar;

                // If it's a valid low surrogate, consume it
                //
                if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                {
                    // Construct the UTF32 code point
                    //
                    ch = ((ch - UNI_SUR_HIGH_START) << halfShift)
								+ (ch2 - UNI_SUR_LOW_START) + halfBase;
                }
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it.
                //
            } 
            // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
            // it because the buffer ended
            //
        }
    }
    return ch;
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF16_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la, ClassForwarder<ANTLR_LITTLE_ENDIAN> )
{
	SuperType* input;
    UTF32           ch;
    UTF32           ch2;
    ANTLR_UCHAR*   nextChar;

    // Find the input interface and where we are currently pointing to
    // in the input stream
    //
	input       = this->get_super();
    nextChar    = input->get_nextChar();

    // If a positive offset then advance forward, else retreat
    //
    if  (la >= 0)
    {
        while   (--la > 0 && (ANTLR_UINT8*)nextChar < ((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf() )
        {
            // Advance our copy of the input pointer
            //
            // Next char in Little Endian byte order
            //
            ch  = (*nextChar) + (*(nextChar+1) << 8);
            nextChar += 2;

            // If we have a surrogate pair then we need to consume
            // a following valid LO surrogate.
            //
            if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
            {
                // If the 16 bits following the high surrogate are in the source buffer...
                //
                if	((ANTLR_UINT8*)(nextChar) < (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf() ))
                {
                    // Next character is in little endian byte order
                    //
                    ch2 = (*nextChar) + (*(nextChar+1) << 8);

                    // If it's a valid low surrogate, consume it
                    //
                    if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                    {
                        // We consumed one 16 bit character
                        //
						nextChar += 2;
                    }
                    // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                    // it.
                    //
                } 
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it because the buffer ended
                //
            }
            // Note that we did not check for an invalid low surrogate here, or that fact that the
            // lo surrogate was missing. We just picked out one 16 bit character unless the character
            // was a valid hi surrogate, in whcih case we consumed two 16 bit characters.
            //
        }
    }
    else
    {
        // We need to go backwards from our input point
        //
        while   (la++ < 0 && (ANTLR_UINT8*)nextChar > (ANTLR_UINT8*)input->get_data() )
        {
            // Get the previous 16 bit character
            //
            ch = (*nextChar - 2) + ((*nextChar -1) << 8);
            nextChar -= 2;

            // If we found a low surrogate then go back one more character if
            // the hi surrogate is there
            //
            if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END) 
            {
                ch2 = (*nextChar - 2) + ((*nextChar -1) << 8);
                if (ch2 >= UNI_SUR_HIGH_START && ch2 <= UNI_SUR_HIGH_END) 
                {
                    // Yes, there is a high surrogate to match it so decrement one more and point to that
                    //
                    nextChar -=2;
                }
            }
        }
    }

    // Our local copy of nextChar is now pointing to either the correct character or end of file
    //
    // Input buffer size is always in bytes
    //
	if	( (ANTLR_UINT8*)nextChar >= (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf()))
	{
		return	ANTLR_CHARSTREAM_EOF;
	}
	else
	{
        // Pick up the next 16 character (little endian byte order)
        //
        ch = (*nextChar) + (*(nextChar+1) << 8);
        nextChar += 2;

        // If we have a surrogate pair then we need to consume
        // a following valid LO surrogate.
        //
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
        {
            // If the 16 bits following the high surrogate are in the source buffer...
            //
            if	((ANTLR_UINT8*)(nextChar) < (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf()))
            {
                // Next character is in little endian byte order
                //
                ch2 = (*nextChar) + (*(nextChar+1) << 8);

                // If it's a valid low surrogate, consume it
                //
                if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                {
                    // Construct the UTF32 code point
                    //
                    ch = ((ch - UNI_SUR_HIGH_START) << halfShift)
								+ (ch2 - UNI_SUR_LOW_START) + halfBase;
                }
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it.
                //
            } 
            // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
            // it because the buffer ended
            //
        }
    }
    return ch;
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF16_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la, ClassForwarder<ANTLR_BIG_ENDIAN> )
{
	SuperType* input;
    UTF32           ch;
    UTF32           ch2;
    ANTLR_UCHAR*   nextChar;

    // Find the input interface and where we are currently pointing to
    // in the input stream
    //
	input       = this->get_super();
    nextChar    = input->get_nextChar();

    // If a positive offset then advance forward, else retreat
    //
    if  (la >= 0)
    {
        while   (--la > 0 && (ANTLR_UINT8*)nextChar < ((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf() )
        {
            // Advance our copy of the input pointer
            //
            // Next char in Big Endian byte order
            //
            ch  = ((*nextChar) << 8) + *(nextChar+1);
            nextChar += 2;

            // If we have a surrogate pair then we need to consume
            // a following valid LO surrogate.
            //
            if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
            {
                // If the 16 bits following the high surrogate are in the source buffer...
                //
                if	((ANTLR_UINT8*)(nextChar) < (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf()))
                {
                    // Next character is in big endian byte order
                    //
                    ch2 = ((*nextChar) << 8) + *(nextChar+1);

                    // If it's a valid low surrogate, consume it
                    //
                    if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                    {
                        // We consumed one 16 bit character
                        //
						nextChar += 2;
                    }
                    // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                    // it.
                    //
                } 
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it because the buffer ended
                //
            }
            // Note that we did not check for an invalid low surrogate here, or that fact that the
            // lo surrogate was missing. We just picked out one 16 bit character unless the character
            // was a valid hi surrogate, in whcih case we consumed two 16 bit characters.
            //
        }
    }
    else
    {
        // We need to go backwards from our input point
        //
        while   (la++ < 0 && (ANTLR_UINT8*)nextChar > (ANTLR_UINT8*)input->get_data() )
        {
            // Get the previous 16 bit character
            //
            ch = ((*nextChar - 2) << 8) + (*nextChar -1);
            nextChar -= 2;

            // If we found a low surrogate then go back one more character if
            // the hi surrogate is there
            //
            if (ch >= UNI_SUR_LOW_START && ch <= UNI_SUR_LOW_END) 
            {
                ch2 = ((*nextChar - 2) << 8) + (*nextChar -1);
                if (ch2 >= UNI_SUR_HIGH_START && ch2 <= UNI_SUR_HIGH_END) 
                {
                    // Yes, there is a high surrogate to match it so decrement one more and point to that
                    //
                    nextChar -=2;
                }
            }
        }
    }

    // Our local copy of nextChar is now pointing to either the correct character or end of file
    //
    // Input buffer size is always in bytes
    //
	if	( (ANTLR_UINT8*)nextChar >= (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf()))
	{
		return	ANTLR_CHARSTREAM_EOF;
	}
	else
	{
        // Pick up the next 16 character (big endian byte order)
        //
        ch = ((*nextChar) << 8) + *(nextChar+1);
        nextChar += 2;

        // If we have a surrogate pair then we need to consume
        // a following valid LO surrogate.
        //
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
        {
            // If the 16 bits following the high surrogate are in the source buffer...
            //
            if	((ANTLR_UINT8*)(nextChar) < (((ANTLR_UINT8*)input->get_data()) + input->get_sizeBuf()))
            {
                // Next character is in big endian byte order
                //
                ch2 = ((*nextChar) << 8) + *(nextChar+1);

                // If it's a valid low surrogate, consume it
                //
                if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                {
                    // Construct the UTF32 code point
                    //
                    ch = ((ch - UNI_SUR_HIGH_START) << halfShift)
								+ (ch2 - UNI_SUR_LOW_START) + halfBase;
                }
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it.
                //
            } 
            // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
            // it because the buffer ended
            //
        }
    }
    return ch;
}

template<class ImplTraits, class SuperType>
void	UTF16_IntStream<ImplTraits, SuperType>::consume( ClassForwarder<BYTE_AGNOSTIC> )
{
	SuperType* input;
    UTF32   ch;
    UTF32   ch2;

	input   = this->get_super();

    // Buffer size is always in bytes
    //
	if(input->get_nextChar() < (input->get_data() + input->get_sizeBuf()/2) )
	{	
		// Indicate one more character in this line
		//
		input->inc_charPositionInLine();

		if  ((ANTLR_UCHAR)(*(input->get_nextChar())) == input->get_newlineChar())
		{
			// Reset for start of a new line of input
			//
			input->inc_line();
			input->set_charPositionInLine(0);
			input->set_currentLine( input->get_nextChar() + 1 );
		}

		// Increment to next character position, accounting for any surrogates
		//
        // Next char in natural machine byte order
        //
        ch  = *(input->get_nextChar());

        // We consumed one 16 bit character
        //
		input->set_nextChar( input->get_nextChar() + 1 );

        // If we have a surrogate pair then we need to consume
        // a following valid LO surrogate.
        //
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) {

            // If the 16 bits following the high surrogate are in the source buffer...
            //
            if(input->get_nextChar() < (input->get_data() + input->get_sizeBuf()/2) )
            {
                // Next character is in natural machine byte order
                //
                ch2 = *(input->get_nextChar());

                // If it's a valid low surrogate, consume it
                //
                if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                {
                    // We consumed one 16 bit character
                    //
					input->set_nextChar( input->get_nextChar() + 1 );
                }
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it.
                //
            } 
            // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
            // it because the buffer ended
            //
        } 
        // Note that we did not check for an invalid low surrogate here, or that fact that the
        // lo surrogate was missing. We just picked out one 16 bit character unless the character
        // was a valid hi surrogate, in whcih case we consumed two 16 bit characters.
        //
	}

}

template<class ImplTraits, class SuperType>
void	UTF16_IntStream<ImplTraits, SuperType>::consume( ClassForwarder<ANTLR_LITTLE_ENDIAN> )
{
	SuperType* input;
    UTF32   ch;
    UTF32   ch2;

	input   = this->get_super();

    // Buffer size is always in bytes
    //
	if(input->get_nextChar() < (input->get_data() + input->get_sizeBuf()/2) )
	{	
		// Indicate one more character in this line
		//
		input->inc_charPositionInLine();

		if  ((ANTLR_UCHAR)(*(input->get_nextChar())) == input->get_newlineChar())
		{
			// Reset for start of a new line of input
			//
			input->inc_line();
			input->set_charPositionInLine(0);
			input->set_currentLine(input->get_nextChar() + 1);
		}

		// Increment to next character position, accounting for any surrogates
		//
        // Next char in litle endian form
        //
        ch  = *((ANTLR_UINT8*)input->get_nextChar()) + (*((ANTLR_UINT8*)input->get_nextChar() + 1) <<8);

        // We consumed one 16 bit character
        //
		input->set_nextChar( input->get_nextChar() + 1);

        // If we have a surrogate pair then we need to consume
        // a following valid LO surrogate.
        //
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
		{
            // If the 16 bits following the high surrogate are in the source buffer...
            //
            if(input->get_nextChar() < (input->get_data() + input->get_sizeBuf()/2) )
            {
                ch2 = *((ANTLR_UINT8*)input->get_nextChar()) + (*((ANTLR_UINT8*)input->get_nextChar() + 1) <<8);

                // If it's a valid low surrogate, consume it
                //
                if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                {
                    // We consumed one 16 bit character
                    //
					input->set_nextChar( input->get_nextChar() + 1);
                }
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it.
                //
            } 
            // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
            // it because the buffer ended
            //
        } 
        // Note that we did not check for an invalid low surrogate here, or that fact that the
        // lo surrogate was missing. We just picked out one 16 bit character unless the character
        // was a valid hi surrogate, in whcih case we consumed two 16 bit characters.
        //
	}
}

template<class ImplTraits, class SuperType>
void	UTF16_IntStream<ImplTraits, SuperType>::consume( ClassForwarder<ANTLR_BIG_ENDIAN> )
{
	SuperType* input;
    UTF32   ch;
    UTF32   ch2;

	input   = this->get_super();

    // Buffer size is always in bytes
    //
	if(input->get_nextChar() < (input->get_data() + input->get_sizeBuf()/2) )
	{	
		// Indicate one more character in this line
		//
		input->inc_charPositionInLine();

		if  ((ANTLR_UCHAR)(*(input->get_nextChar())) == input->get_newlineChar())
		{
			// Reset for start of a new line of input
			//
			input->inc_line();
			input->set_charPositionInLine(0);
			input->set_currentLine(input->get_nextChar() + 1);
		}

		// Increment to next character position, accounting for any surrogates
		//
        // Next char in big endian form
        //
        ch  = *((ANTLR_UINT8*)input->get_nextChar() + 1) + (*((ANTLR_UINT8*)input->get_nextChar() ) <<8);

        // We consumed one 16 bit character
        //
		input->set_nextChar( input->get_nextChar() + 1);

        // If we have a surrogate pair then we need to consume
        // a following valid LO surrogate.
        //
        if (ch >= UNI_SUR_HIGH_START && ch <= UNI_SUR_HIGH_END) 
		{
            // If the 16 bits following the high surrogate are in the source buffer...
            //
            if(input->get_nextChar() < (input->get_data() + input->get_sizeBuf()/2) )
            {
                // Big endian
                //
                ch2 = *((ANTLR_UINT8*)input->get_nextChar() + 1) + (*((ANTLR_UINT8*)input->get_nextChar() ) <<8);

                // If it's a valid low surrogate, consume it
                //
                if (ch2 >= UNI_SUR_LOW_START && ch2 <= UNI_SUR_LOW_END) 
                {
                    // We consumed one 16 bit character
                    //
					input->set_nextChar( input->get_nextChar() + 1);
                }
                // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
                // it.
                //
            } 
            // Note that we ignore a valid hi surrogate that has no lo surrogate to go with
            // it because the buffer ended
            //
        } 
        // Note that we did not check for an invalid low surrogate here, or that fact that the
        // lo surrogate was missing. We just picked out one 16 bit character unless the character
        // was a valid hi surrogate, in whcih case we consumed two 16 bit characters.
        //
	}
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF32_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 i)
{
	return this->LA( i, ClassForwarder<typename ImplTraits::Endianness>() );
}

template<class ImplTraits, class SuperType>
ANTLR_MARKER	UTF32_IntStream<ImplTraits, SuperType>::index()
{
	SuperType* input = this->get_super();
    return  (ANTLR_MARKER)(input->get_nextChar());
}

template<class ImplTraits, class SuperType>
void UTF32_IntStream<ImplTraits, SuperType>::seek(ANTLR_MARKER seekPoint)
{
	SuperType* input;

	input   = this->get_super();

	// If the requested seek point is less than the current
	// input point, then we assume that we are resetting from a mark
	// and do not need to scan, but can just set to there as rewind will
        // reset line numbers and so on.
	//
	if	(seekPoint <= (ANTLR_MARKER)(input->get_nextChar()))
	{
		input->set_nextChar( static_cast<typename ImplTraits::DataType*>(seekPoint) );
	}
	else
	{
        // Call consume until we reach the asked for seek point or EOF
        //
        while( (this->LA(1) != ANTLR_CHARSTREAM_EOF) && (seekPoint < (ANTLR_MARKER)input->get_nextChar()) )
	    {
			this->consume();
	    }
	}

}

template<class ImplTraits, class SuperType>
void UTF32_IntStream<ImplTraits, SuperType>::setupIntStream(bool machineBigEndian, bool inputBigEndian)
{
	SuperType* super = this->get_super();
	super->set_charByteSize(4);

	this->findout_endian_spec(machineBigEndian, inputBigEndian);
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF32_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la, ClassForwarder<BYTE_AGNOSTIC> )
{
    SuperType* input = this->get_super();

    if	(( input->get_nextChar() + la - 1) >= (input->get_data() + input->get_sizeBuf()/4 ))
    {
		return	ANTLR_CHARSTREAM_EOF;
    }
    else
    {
		return	(ANTLR_UCHAR)(*(input->get_nextChar() + la - 1));
    }
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF32_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la, ClassForwarder<ANTLR_LITTLE_ENDIAN> )
{
	SuperType* input = this->get_super();

    if	(( input->get_nextChar() + la - 1) >= (input->get_data() + input->get_sizeBuf()/4 ))
    {
		return	ANTLR_CHARSTREAM_EOF;
    }
    else
    {
        ANTLR_UCHAR   c;

        c = (ANTLR_UCHAR)(*(input->get_nextChar() + la - 1));

        // Swap Endianess to Big Endian
        //
        return (c>>24) | ((c<<8) & 0x00FF0000) | ((c>>8) & 0x0000FF00) | (c<<24);
    }
}

template<class ImplTraits, class SuperType>
ANTLR_UINT32	UTF32_IntStream<ImplTraits, SuperType>::LA( ANTLR_INT32 la, ClassForwarder<ANTLR_BIG_ENDIAN> )
{
	SuperType* input = this->get_super();

    if	(( input->get_nextChar() + la - 1) >= (input->get_data() + input->get_sizeBuf()/4 ))
    {
		return	ANTLR_CHARSTREAM_EOF;
    }
    else
    {
        ANTLR_UCHAR   c;

        c = (ANTLR_UCHAR)(*(input->get_nextChar() + la - 1));

        // Swap Endianess to Little Endian
        //
        return (c>>24) | ((c<<8) & 0x00FF0000) | ((c>>8) & 0x0000FF00) | (c<<24);
    }
}

template<class ImplTraits, class SuperType>
void	UTF32_IntStream<ImplTraits, SuperType>::consume()
{
	SuperType* input = this->get_super();

    // SizeBuf is always in bytes
    //
	if	( input->get_nextChar()  < (input->get_data() + input->get_sizeBuf()/4 ))
    {	
		/* Indicate one more character in this line
		 */
		input->inc_charPositionInLine();
	
		if  ((ANTLR_UCHAR)(*(input->get_nextChar())) == input->get_newlineChar())
		{
			/* Reset for start of a new line of input
			 */
			input->inc_line();
			input->set_charPositionInLine(0);
			input->set_currentLine(	input->get_nextChar() + 1 );
		}

		/* Increment to next character position
		 */
		input->set_nextChar( input->get_nextChar() + 1 );
    }
}

template<class ImplTraits, class SuperType>
void UTF8_IntStream<ImplTraits, SuperType>::setupIntStream(bool, bool)
{
	SuperType* super = this->get_super();
	super->set_charByteSize(0);
}

// ------------------------------------------------------
// Following is from Unicode.org (see antlr3convertutf.c)
//

/// Index into the table below with the first byte of a UTF-8 sequence to
/// get the number of trailing bytes that are supposed to follow it.
/// Note that *legal* UTF-8 values can't have 4 or 5-bytes. The table is
/// left as-is for anyone who may want to do such conversion, which was
/// allowed in earlier algorithms.
///
template<class ImplTraits, class SuperType>
const ANTLR_UINT32* UTF8_IntStream<ImplTraits, SuperType>::TrailingBytesForUTF8()
{
	static const ANTLR_UINT32 trailingBytesForUTF8[256] = {
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
		1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1, 1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,
		2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2, 3,3,3,3,3,3,3,3,4,4,4,4,5,5,5,5
	};

	return trailingBytesForUTF8;
}

/// Magic values subtracted from a buffer value during UTF8 conversion.
/// This table contains as many values as there might be trailing bytes
/// in a UTF-8 sequence.
///
template<class ImplTraits, class SuperType>
const UTF32* UTF8_IntStream<ImplTraits, SuperType>::OffsetsFromUTF8()
{
	static const UTF32 offsetsFromUTF8[6] = 
		{   0x00000000UL, 0x00003080UL, 0x000E2080UL, 
			0x03C82080UL, 0xFA082080UL, 0x82082080UL 
		};
	return 	offsetsFromUTF8;
}

// End of Unicode.org tables
// -------------------------


/** \brief Consume the next character in a UTF8 input stream
 *
 * \param input Input stream context pointer
 */
template<class ImplTraits, class SuperType>
void UTF8_IntStream<ImplTraits, SuperType>::consume()
{
    SuperType* input = this->get_super();
	const ANTLR_UINT32* trailingBytesForUTF8 = UTF8_IntStream::TrailingBytesForUTF8();
	const UTF32* offsetsFromUTF8 = UTF8_IntStream::OffsetsFromUTF8();

    ANTLR_UINT32           extraBytesToRead;
    ANTLR_UCHAR            ch;
    ANTLR_UINT8*           nextChar;

    nextChar = input->get_nextChar();

    if	(nextChar < (input->get_data() + input->get_sizeBuf()))
    {	
		// Indicate one more character in this line
		//
		input->inc_charPositionInLine();
	
        // Are there more bytes needed to make up the whole thing?
        //
        extraBytesToRead = trailingBytesForUTF8[*nextChar];

        if	((nextChar + extraBytesToRead) >= (input->get_data() + input->get_sizeBuf()))
        {
            input->set_nextChar( input->get_data() + input->get_sizeBuf() );
            return;
        }

        // Cases deliberately fall through (see note A in antlrconvertutf.c)
        // Legal UTF8 is only 4 bytes but 6 bytes could be used in old UTF8 so
        // we allow it.
        //
        ch  = 0;
       	switch (extraBytesToRead) 
		{
			case 5: ch += *nextChar++; ch <<= 6;
			case 4: ch += *nextChar++; ch <<= 6;
			case 3: ch += *nextChar++; ch <<= 6;
			case 2: ch += *nextChar++; ch <<= 6;
			case 1: ch += *nextChar++; ch <<= 6;
			case 0: ch += *nextChar++;
		}

        // Magically correct the input value
        //
		ch -= offsetsFromUTF8[extraBytesToRead];
		if  (ch == input->get_newlineChar())
		{
			/* Reset for start of a new line of input
			 */
			input->inc_line();
			input->set_charPositionInLine(0);
			input->set_currentLine(nextChar);
		}

        // Update input pointer
        //
        input->set_nextChar(nextChar);
    }
}

/** \brief Return the input element assuming a UTF8 input
 *
 * \param[in] input Input stream context pointer
 * \param[in] la 1 based offset of next input stream element
 *
 * \return Next input character in internal ANTLR3 encoding (UTF32)
 */
template<class ImplTraits, class SuperType>
ANTLR_UCHAR UTF8_IntStream<ImplTraits, SuperType>::LA(ANTLR_INT32 la)
{
    SuperType* input = this->get_super();
	const ANTLR_UINT32* trailingBytesForUTF8 = UTF8_IntStream::TrailingBytesForUTF8();
	const UTF32* offsetsFromUTF8 = UTF8_IntStream::OffsetsFromUTF8();
    ANTLR_UINT32           extraBytesToRead;
    ANTLR_UCHAR            ch;
    ANTLR_UINT8*           nextChar;

    nextChar = input->get_nextChar();

    // Do we need to traverse forwards or backwards?
    // - LA(0) is treated as LA(1) and we assume that the nextChar is
    //   already positioned.
    // - LA(n+) ; n>1 means we must traverse forward n-1 characters catering for UTF8 encoding
    // - LA(-n) means we must traverse backwards n chracters
    //
    if (la > 1) {

        // Make sure that we have at least one character left before trying to
        // loop through the buffer.
        //
        if	(nextChar < (input->get_data() + input->get_sizeBuf()))
        {	
            // Now traverse n-1 characters forward
            //
            while (--la > 0)
            {
                // Does the next character require trailing bytes?
                // If so advance the pointer by that many bytes as well as advancing
                // one position for what will be at least a single byte character.
                //
                nextChar += trailingBytesForUTF8[*nextChar] + 1;

                // Does that calculation take us past the byte length of the buffer?
                //
                if	(nextChar >= (input->get_data() + input->get_sizeBuf()))
                {
                    return ANTLR_CHARSTREAM_EOF;
                }
            }
        }
        else
        {
            return ANTLR_CHARSTREAM_EOF;
        }
    }
    else
    {
        // LA is negative so we decrease the pointer by n character positions
        //
        while   (nextChar > input->get_data() && la++ < 0)
        {
            // Traversing backwards in UTF8 means decermenting by one
            // then continuing to decrement while ever a character pattern
            // is flagged as being a trailing byte of an encoded code point.
            // Trailing UTF8 bytes always start with 10 in binary. We assumne that
            // the UTF8 is well formed and do not check boundary conditions
            //
            nextChar--;
            while ((*nextChar & 0xC0) == 0x80)
            {
                nextChar--;
            }
        }
    }

    // nextChar is now pointing at the UTF8 encoded character that we need to
    // decode and return.
    //
    // Are there more bytes needed to make up the whole thing?
    //
    extraBytesToRead = trailingBytesForUTF8[*nextChar];
    if	(nextChar + extraBytesToRead >= (input->get_data() + input->get_sizeBuf()))
    {
        return ANTLR_CHARSTREAM_EOF;
    }

    // Cases deliberately fall through (see note A in antlrconvertutf.c)
    // 
    ch  = 0;
    switch (extraBytesToRead) 
	{
        case 5: ch += *nextChar++; ch <<= 6;
        case 4: ch += *nextChar++; ch <<= 6;
        case 3: ch += *nextChar++; ch <<= 6;
        case 2: ch += *nextChar++; ch <<= 6;
        case 1: ch += *nextChar++; ch <<= 6;
        case 0: ch += *nextChar++;
    }

    // Magically correct the input value
    //
    ch -= offsetsFromUTF8[extraBytesToRead];

    return ch;
}

template<class ImplTraits>
TokenIntStream<ImplTraits>::TokenIntStream()
{
	m_cachedSize = 0;
}

template<class ImplTraits>
ANTLR_UINT32 TokenIntStream<ImplTraits>::get_cachedSize() const
{
	return m_cachedSize;
}

template<class ImplTraits>
void TokenIntStream<ImplTraits>::set_cachedSize( ANTLR_UINT32 cachedSize )
{
	m_cachedSize = cachedSize;
}

/** Move the input pointer to the next incoming token.  The stream
 *  must become active with LT(1) available.  consume() simply
 *  moves the input pointer so that LT(1) points at the next
 *  input symbol. Consume at least one token.
 *
 *  Walk past any token not on the channel the parser is listening to.
 */
template<class ImplTraits>
void TokenIntStream<ImplTraits>::consume()
{
	TokenStreamType* cts = static_cast<TokenStreamType*>(this);

    if((ANTLR_UINT32)cts->get_p() < m_cachedSize )
	{
		cts->inc_p();
		cts->set_p( cts->skipOffTokenChannels(cts->get_p()) );
	}
}
template<class ImplTraits>
void  TokenIntStream<ImplTraits>::consumeInitialHiddenTokens()
{
	ANTLR_MARKER	first;
	ANTLR_INT32	i;
	TokenStreamType*	ts;

	ts	    = this->get_super();
	first	= this->index();

	for	(i=0; i<first; i++)
	{
		ts->get_debugger()->consumeHiddenToken(ts->get(i));
	}

	ts->set_initialStreamState(false);
}


template<class ImplTraits>
ANTLR_UINT32	TokenIntStream<ImplTraits>::LA( ANTLR_INT32 i )
{
	const CommonTokenType*    tok;
	TokenStreamType*    ts	    = static_cast<TokenStreamType*>(this);

	tok	    =  ts->LT(i);

	if	(tok != NULL)
	{
		return	tok->get_type();
	}
	else
	{
		return	CommonTokenType::TOKEN_INVALID;
	}

}

template<class ImplTraits>
ANTLR_MARKER	TokenIntStream<ImplTraits>::mark()
{
    BaseType::m_lastMarker = this->index();
    return  BaseType::m_lastMarker;
}

template<class ImplTraits>
ANTLR_UINT32 TokenIntStream<ImplTraits>::size()
{
    if (this->get_cachedSize() > 0)
    {
		return  this->get_cachedSize();
    }
    TokenStreamType* cts   = this->get_super();

    this->set_cachedSize( static_cast<ANTLR_UINT32>(cts->get_tokens().size()) );
    return  this->get_cachedSize();
}

template<class ImplTraits>
void	TokenIntStream<ImplTraits>::release()
{
    return;
}

template<class ImplTraits>
ANTLR_MARKER   TokenIntStream<ImplTraits>::tindex()
{
	return this->get_super()->get_p();
}

template<class ImplTraits>
void	TokenIntStream<ImplTraits>::rewindLast()
{
    this->rewind( this->get_lastMarker() );
}

template<class ImplTraits>
void	TokenIntStream<ImplTraits>::rewind(ANTLR_MARKER marker)
{
	return this->seek(marker);
}

template<class ImplTraits>
void	TokenIntStream<ImplTraits>::seek(ANTLR_MARKER index)
{
    TokenStreamType* cts = static_cast<TokenStreamType*>(this);

    cts->set_p( static_cast<ANTLR_INT32>(index) );
}


/// Return a string that represents the name assoicated with the input source
///
/// /param[in] is The ANTLR3_INT_STREAM interface that is representing this token stream.
///
/// /returns 
/// /implements ANTLR3_INT_STREAM_struct::getSourceName()
///
template<class ImplTraits>
typename TokenIntStream<ImplTraits>::StringType
TokenIntStream<ImplTraits>::getSourceName()
{
	// Slightly convoluted as we must trace back to the lexer's input source
	// via the token source. The streamName that is here is not initialized
	// because this is a token stream, not a file or string stream, which are the
	// only things that have a context for a source name.
	//
	return this->get_super()->get_tokenSource()->get_fileName();
}

template<class ImplTraits>
void  TreeNodeIntStream<ImplTraits>::consume()
{
	TreeNodeStreamType* ctns = this->get_super();
	if( ctns->get_p() == -1 )
		ctns->fillBufferRoot();
	ctns->inc_p();
}
template<class ImplTraits>
ANTLR_MARKER		TreeNodeIntStream<ImplTraits>::tindex()
{
	TreeNodeStreamType* ctns = this->get_super();
	return (ANTLR_MARKER)(ctns->get_p());
}

template<class ImplTraits>
ANTLR_UINT32 TreeNodeIntStream<ImplTraits>::LA(ANTLR_INT32 i)
{
	TreeNodeStreamType* tns	    = this->get_super();

	// Ask LT for the 'token' at that position
	//
	TreeTypePtr t = tns->LT(i);

	if	(t == NULL)
	{
		return	CommonTokenType::TOKEN_INVALID;
	}

	// Token node was there so return the type of it
	//
	return  t->get_type();
}

template<class ImplTraits>
ANTLR_MARKER	TreeNodeIntStream<ImplTraits>::mark()
{
	TreeNodeStreamType* ctns	    = this->get_super();
	
	if	(ctns->get_p() == -1)
	{
		ctns->fillBufferRoot();
	}

	// Return the current mark point
	//
	this->set_lastMarker( this->index() );

	return this->get_lastMarker();

}

template<class ImplTraits>
void  TreeNodeIntStream<ImplTraits>::release(ANTLR_MARKER /*marker*/)
{

}

template<class ImplTraits>
void TreeNodeIntStream<ImplTraits>::rewindMark(ANTLR_MARKER marker)
{
	this->seek(marker);
}

template<class ImplTraits>
void TreeNodeIntStream<ImplTraits>::rewindLast()
{
	this->seek( this->get_lastMarker() );
}

template<class ImplTraits>
void	TreeNodeIntStream<ImplTraits>::seek(ANTLR_MARKER index)
{
	TreeNodeStreamType* ctns	    = this->get_super();
	ctns->set_p( ANTLR_UINT32_CAST(index) );
}

template<class ImplTraits>
ANTLR_UINT32	TreeNodeIntStream<ImplTraits>::size()
{
	TreeNodeStreamType* ctns	    = this->get_super();
	
	if	(ctns->get_p() == -1)
	{
		ctns->fillBufferRoot();
	}

	return ctns->get_nodes().size();
}


}
