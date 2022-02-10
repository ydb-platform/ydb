namespace antlr3 {

template <class ImplTraits>
ANTLR_INLINE BitsetList<ImplTraits>::BitsetList()
{
	m_bits = NULL;
	m_length  = 0;
}

template <class ImplTraits>
ANTLR_INLINE BitsetList<ImplTraits>::BitsetList( ANTLR_BITWORD* bits, ANTLR_UINT32 length )
{
	m_bits = bits;
	m_length  = length;
}

template <class ImplTraits>
ANTLR_INLINE ANTLR_BITWORD* BitsetList<ImplTraits>::get_bits() const
{
	return m_bits;
}

template <class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 BitsetList<ImplTraits>::get_length() const
{
	return m_length;
}

template <class ImplTraits>
ANTLR_INLINE void BitsetList<ImplTraits>::set_bits( ANTLR_BITWORD* bits )
{
	m_bits = bits;
}

template <class ImplTraits>
ANTLR_INLINE void BitsetList<ImplTraits>::set_length( ANTLR_UINT32 length )
{
	m_length = length;
}

template <class ImplTraits>
typename BitsetList<ImplTraits>::BitsetType* BitsetList<ImplTraits>::bitsetLoad()
{
	// Allocate memory for the bitset structure itself
	// the input parameter is the bit number (0 based)
	// to include in the bitset, so we need at at least
	// bit + 1 bits. If any arguments indicate a 
	// a bit higher than the default number of bits (0 means default size)
	// then Add() will take care
	// of it.
	//
	BitsetType* bitset  = new BitsetType();

	if	(this != NULL)
	{
		// Now we can add the element bits into the set
		//
		ANTLR_UINT32 count=0;
		while (count < m_length)
		{
			if( bitset->get_blist().get_length() <= count)
				bitset->grow(count+1);

			typename ImplTraits::BitsetListType& blist = bitset->get_blist();
			blist.m_bits[count] = *(m_bits+count);
			count++;
		}
	}

	// return the new bitset
	//
	return  bitset;
}

template <class ImplTraits>
typename BitsetList<ImplTraits>::BitsetType* BitsetList<ImplTraits>::bitsetCopy()
{
	BitsetType*  bitset;
	ANTLR_UINT32 numElements = m_length;

    // Avoid memory thrashing at the expense of a few more bytes
    //
    if	(numElements < 8)
		numElements = 8;

    // Allocate memory for the bitset structure itself
    //
    bitset  = new Bitset<ImplTraits>(numElements);
	memcpy(bitset->get_blist().get_bits(), m_bits, numElements * sizeof(ANTLR_BITWORD));

    // All seems good
    //
    return  bitset;
}

template <class ImplTraits>
Bitset<ImplTraits>::Bitset( ANTLR_UINT32 numBits )
{
	// Avoid memory thrashing at the up front expense of a few bytes
	if	(numBits < (8 * ANTLR_BITSET_BITS))
		numBits = 8 * ANTLR_BITSET_BITS;

	// No we need to allocate the memory for the number of bits asked for
	// in multiples of ANTLR3_UINT64. 
	//
	ANTLR_UINT32 numelements	= ((numBits -1) >> ANTLR_BITSET_LOG_BITS) + 1;

	m_blist.set_bits( (ANTLR_BITWORD*) AllocPolicyType::alloc0(numelements * sizeof(ANTLR_BITWORD)));

	m_blist.set_length( numelements );
}

template <class ImplTraits>
Bitset<ImplTraits>::Bitset( const Bitset& bitset )
	:m_blist(bitset.m_blist)
{
}

template <class ImplTraits>
ANTLR_INLINE Bitset<ImplTraits>*  Bitset<ImplTraits>::clone() const
{
	Bitset*  bitset;

    // Allocate memory for the bitset structure itself
    //
    bitset  = new Bitset( ANTLR_BITSET_BITS * m_blist.get_length() );

    // Install the actual bits in the source set
    //
    memcpy(bitset->m_blist.get_bits(), m_blist.get_bits(), 
				m_blist.get_length() * sizeof(ANTLR_BITWORD) );

    // All seems good
    //
    return  bitset;
}

template <class ImplTraits>
Bitset<ImplTraits>*  Bitset<ImplTraits>::bor(Bitset* bitset2)
{
	Bitset*  bitset;

    if	(this == NULL)
		return bitset2->clone();

    if	(bitset2 == NULL)
		return	this->clone();

    // Allocate memory for the newly ordered bitset structure itself.
    //
    bitset  = this->clone();
    bitset->bitsetORInPlace(bitset2);
    return  bitset;
}

template <class ImplTraits>
void	 Bitset<ImplTraits>::borInPlace(Bitset* bitset2)
{
	ANTLR_UINT32   minimum;

    if	(bitset2 == NULL)
		return;

	// First make sure that the target bitset is big enough
    // for the new bits to be ored in.
    //
    if	( m_blist.get_length() < bitset2->m_blist.get_length() )
		this->growToInclude( bitset2->m_blist.get_length() * sizeof(ANTLR_BITWORD) );
    
    // Or the miniimum number of bits after any resizing went on
    //
    if	( m_blist.get_length() < bitset2->m_blist.get_length() )
		minimum = m_blist.get_length();
	else
		minimum = bitset2->m_blist.get_length();

	ANTLR_BITWORD* bits1 = m_blist.get_bits();
	ANTLR_BITWORD* bits2 = bitset2->m_blist.get_bits();
	for	(ANTLR_UINT32 i = minimum; i > 0; i--)
		bits1[i-1] |= bits2[i-1];
}

template <class ImplTraits>
ANTLR_UINT32 Bitset<ImplTraits>::size() const
{
    ANTLR_UINT32   degree;
    ANTLR_INT32   i;
    ANTLR_INT8    bit;
    
    // TODO: Come back to this, it may be faster to & with 0x01
    // then shift right a copy of the 4 bits, than shift left a constant of 1.
    // But then again, the optimizer might just work this out
    // anyway.
    //
    degree  = 0;
	ANTLR_BITWORD* bits = m_blist.get_bits();
    for	(i = m_blist.get_length() - 1; i>= 0; i--)
    {
		if  (bits[i] != 0)
		{
			for(bit = ANTLR_BITSET_BITS - 1; bit >= 0; bit--)
			{
				if((bits[i] & (((ANTLR_BITWORD)1) << bit)) != 0)
				{
					degree++;
				}
			}
		}
    }
    return degree;
}

template <class ImplTraits>
ANTLR_INLINE void	Bitset<ImplTraits>::add(ANTLR_INT32 bit)
{
	ANTLR_UINT32   word = Bitset::WordNumber(bit);

    if	(word	>= m_blist.get_length() )
		this->growToInclude(bit);
 
	ANTLR_BITWORD* bits = m_blist.get_bits();
	bits[word] |= Bitset::BitMask(bit);
}

template <class ImplTraits>
void	Bitset<ImplTraits>::grow(ANTLR_INT32 newSize)
{
	ANTLR_BITWORD*   newBits;

    // Space for newly sized bitset - TODO: come back to this and use realloc?, it may
    // be more efficient...
    //
    newBits =  (ANTLR_BITWORD*) AllocPolicyType::alloc0(newSize * sizeof(ANTLR_BITWORD) );
    if	( m_blist.get_bits() != NULL)
    {
		// Copy existing bits
		//
		memcpy( newBits, m_blist.get_bits(), m_blist.get_length() * sizeof(ANTLR_BITWORD) );

		// Out with the old bits... de de de derrr
		//
		AllocPolicyType::free( m_blist.get_bits() );
    }

    // In with the new bits... keerrrang.
    //
    m_blist.set_bits(newBits);
    m_blist.set_length(newSize);
}

template <class ImplTraits>
bool	Bitset<ImplTraits>::equals(Bitset* bitset2) const
{
    ANTLR_UINT32   minimum;
    ANTLR_UINT32   i;

    if	(this == NULL || bitset2 == NULL)
		return	false;

    // Work out the minimum comparison set
    //
    if	( m_blist.get_length() < bitset2->m_blist.get_length() )
		minimum = m_blist.get_length();
    else
		minimum = bitset2->m_blist.get_length();

    // Make sure explict in common bits are equal
    //
    for	(i = minimum - 1; i < minimum ; i--)
    {
		ANTLR_BITWORD* bits1 = m_blist.get_bits();
		ANTLR_BITWORD* bits2 = bitset2->m_blist.get_bits();
		if  ( bits1[i] != bits2[i])
			return false;
    }

    // Now make sure the bits of the larger set are all turned
    // off.
    //
    if	( m_blist.get_length() > minimum)
    {
		for (i = minimum ; i < m_blist.get_length(); i++)
		{
			ANTLR_BITWORD* bits = m_blist.get_bits();
			if(bits[i] != 0)
				return false;
		}
    }
    else if (bitset2->m_blist.get_length() > minimum)
    {
		ANTLR_BITWORD* bits = m_blist.get_bits();
		for (i = minimum; i < bitset2->m_blist.get_length(); i++)
		{
			if	( bits[i] != 0 )
				return	false;
		}
    }

    return  true;
}

template <class ImplTraits>
bool	Bitset<ImplTraits>::isMember(ANTLR_UINT32 bit) const
{
    ANTLR_UINT32    wordNo = Bitset::WordNumber(bit);

    if	(wordNo >= m_blist.get_length())
		return false;
    
	ANTLR_BITWORD* bits = m_blist.get_bits();
    if	( (bits[wordNo] & Bitset::BitMask(bit)) == 0)
		return false;
    else
		return true;
}

template <class ImplTraits>
ANTLR_INLINE ANTLR_UINT32 Bitset<ImplTraits>::numBits() const
{
	return  m_blist.get_length() << ANTLR_BITSET_LOG_BITS;
}

template <class ImplTraits>
ANTLR_INLINE typename ImplTraits::BitsetListType& Bitset<ImplTraits>::get_blist()
{
	return m_blist;
}

template <class ImplTraits>
ANTLR_INLINE void Bitset<ImplTraits>::remove(ANTLR_UINT32 bit)
{
    ANTLR_UINT32    wordNo = Bitset::WordNumber(bit);

    if	(wordNo < m_blist.get_length())
	{
		ANTLR_BITWORD* bits = m_blist.get_bits();
		bits[wordNo] &= ~(Bitset::BitMask(bit));
	}
}

template <class ImplTraits>
ANTLR_INLINE bool Bitset<ImplTraits>::isNilNode() const
{
	ANTLR_UINT32    i;
	ANTLR_BITWORD* bits = m_blist.get_bits();
	for	(i = m_blist.get_length() -1 ; i < m_blist.get_length(); i--)
	{
		if(bits[i] != 0)
			return false;
	}
	return  true;
}

template <class ImplTraits>
ANTLR_INT32* Bitset<ImplTraits>::toIntList() const
{
	ANTLR_UINT32   numInts;	    // How many integers we will need
    ANTLR_UINT32   numBits;	    // How many bits are in the set
    ANTLR_UINT32   i;
    ANTLR_UINT32   index;

    ANTLR_INT32*  intList;

    numInts = this->size() + 1;
    numBits = this->numBits();
 
    intList = (ANTLR_INT32*) AllocPolicyType::alloc(numInts * sizeof(ANTLR_INT32));
    
    intList[0] = numInts;

    // Enumerate the bits that are turned on
    //
    for	(i = 0, index = 1; i<numBits; i++)
    {
		if  (this->isMember(i) == true)
			intList[index++]    = i;
    }

    // Result set
    //
    return  intList;
}

template <class ImplTraits>
ANTLR_INLINE Bitset<ImplTraits>::~Bitset()
{
	if	(m_blist.get_bits() != NULL)
		AllocPolicyType::free(m_blist.get_bits());
    return;
}

template <class ImplTraits>
void	Bitset<ImplTraits>::growToInclude(ANTLR_INT32 bit)
{
	ANTLR_UINT32	bl;
	ANTLR_UINT32	nw;

	bl = (m_blist.get_length() << 1);
	nw = Bitset::NumWordsToHold(bit);

	if	(bl > nw)
		this->grow(bl);
	else
		this->grow(nw);
}

template <class ImplTraits>
ANTLR_INLINE ANTLR_UINT64	Bitset<ImplTraits>::BitMask(ANTLR_UINT32 bitNumber)
{
	return  ((ANTLR_UINT64)1) << (bitNumber & (ANTLR_BITSET_MOD_MASK));
}

template <class ImplTraits>
ANTLR_INLINE ANTLR_UINT32	Bitset<ImplTraits>::NumWordsToHold(ANTLR_UINT32 bit)
{
	return  (bit >> ANTLR_BITSET_LOG_BITS) + 1;
}

template <class ImplTraits>
ANTLR_INLINE ANTLR_UINT32	Bitset<ImplTraits>::WordNumber(ANTLR_UINT32 bit)
{
	return  bit >> ANTLR_BITSET_LOG_BITS;
}

template <class ImplTraits>
void Bitset<ImplTraits>::bitsetORInPlace(Bitset* bitset2)
{
	ANTLR_UINT32   minimum;
    ANTLR_UINT32   i;

    if	(bitset2 == NULL)
		return;

    // First make sure that the target bitset is big enough
    // for the new bits to be ored in.
    //
    if	( m_blist.get_length() < bitset2->m_blist.get_length() )
		this->growToInclude( bitset2->m_blist.get_length() * sizeof(ANTLR_BITWORD) );
    
    // Or the miniimum number of bits after any resizing went on
    //
    if	( m_blist.get_length() < bitset2->m_blist.get_length() )
		minimum = m_blist.get_length();
	else
		minimum = bitset2->m_blist.get_length();

	ANTLR_BITWORD* bits1 = m_blist.get_bits();
	ANTLR_BITWORD* bits2 = bitset2->m_blist.get_bits();
    for	(i = minimum; i > 0; i--)
		bits1[i-1] |= bits2[i-1];
}

template <class ImplTraits>
Bitset<ImplTraits>* Bitset<ImplTraits>::BitsetOf(ANTLR_INT32 bit)
{
	// Allocate memory for the bitset structure itself
    // the input parameter is the bit number (0 based)
    // to include in the bitset, so we need at at least
    // bit + 1 bits. If any arguments indicate a 
    // a bit higher than the default number of bits (0 menas default size)
    // then Add() will take care
    // of it.
    //
	Bitset<ImplTraits>* bitset = new Bitset<ImplTraits>(0);
	bitset->add(bit);
	return bitset;
}

template <class ImplTraits>
Bitset<ImplTraits>* Bitset<ImplTraits>::BitsetOf(ANTLR_INT32 bit1, ANTLR_INT32 bit2)
{
	Bitset<ImplTraits>* bitset = Bitset<ImplTraits>::BitsetOf(bit1);
	bitset->add(bit2);
	return bitset;
}

//static 
template <class ImplTraits>
Bitset<ImplTraits>* Bitset<ImplTraits>::BitsetFromList(const IntListType& list)
{
	// We have no idea what exactly is in the list
    // so create a default bitset and then just add stuff
    // as we enumerate.
    //
    Bitset<ImplTraits>* bitset  = new Bitset<ImplTraits>(0);
	for( int i = 0; i < list.size(); ++i )
		bitset->add( list[i] );

	return bitset;
}

}
