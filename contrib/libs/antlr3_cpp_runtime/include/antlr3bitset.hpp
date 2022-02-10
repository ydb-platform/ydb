/**
 * \file
 * Defines the basic structures of an ANTLR3 bitset. this is a C version of the 
 * cut down Bitset class provided with the java version of antlr 3.
 * 
 * 
 */
#ifndef	_ANTLR3_BITSET_HPP
#define	_ANTLR3_BITSET_HPP

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

/** How many bits in the elements
 */
static const ANTLR_UINT32	ANTLR_BITSET_BITS =	64;

/** How many bits in a nible of bits
 */
static const ANTLR_UINT32	ANTLR_BITSET_NIBBLE	= 4;

/** log2 of ANTLR3_BITSET_BITS 2^ANTLR3_BITSET_LOG_BITS = ANTLR3_BITSET_BITS
 */
static const ANTLR_UINT32	ANTLR_BITSET_LOG_BITS =	6;

/** We will often need to do a mod operator (i mod nbits).
 *  For powers of two, this mod operation is the
 *  same as:
 *   - (i & (nbits-1)).  
 *
 * Since mod is relatively slow, we use an easily
 * precomputed mod mask to do the mod instead.
 */
static const ANTLR_UINT32	ANTLR_BITSET_MOD_MASK = ANTLR_BITSET_BITS - 1;

template <class ImplTraits>
class BitsetList : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename ImplTraits::BitsetType BitsetType;

private:
	/// Pointer to the allocated array of bits for this bit set, which
    /// is an array of 64 bit elements (of the architecture). If we find a 
    /// machine/C compiler that does not know anything about 64 bit values
    ///	then it should be easy enough to produce a 32 bit (or less) version
    /// of the bitset code. Note that the pointer here may be static if laid down
	/// by the code generation, and it must be copied if it is to be manipulated
	/// to perform followset calculations.
    ///
    ANTLR_BITWORD*  m_bits;

    /// Length of the current bit set in ANTLR3_UINT64 units.
    ///
    ANTLR_UINT32    m_length;

public:
	BitsetList();
	BitsetList( ANTLR_BITWORD* bits, ANTLR_UINT32 length );

	ANTLR_BITWORD* get_bits() const;
	ANTLR_UINT32 get_length() const;
	void set_bits( ANTLR_BITWORD* bits );
	void set_length( ANTLR_UINT32 length );

	///
	/// \brief
	/// Creates a new bitset with at least one 64 bit bset of bits, but as
	/// many 64 bit sets as are required.
	///
	/// \param[in] bset
	/// A variable number of bits to add to the set, ending in -1 (impossible bit).
	/// 
	/// \returns
	/// A new bit set with all of the specified bitmaps in it and the API
	/// initialized.
	/// 
	/// Call as:
	///  - pANTLR3_BITSET = antlrBitsetLoad(bset, bset11, ..., -1);
	///  - pANTLR3_BITSET = antlrBitsetOf(-1);  Create empty bitset 
	///
	/// \remarks
	/// Stdargs function - must supply -1 as last paremeter, which is NOT
	/// added to the set.
	/// 
	///
	BitsetType* bitsetLoad();

	BitsetType* bitsetCopy();

};

template <class ImplTraits>
class Bitset : public ImplTraits::AllocPolicyType
{
public:
	typedef typename ImplTraits::AllocPolicyType AllocPolicyType;
	typedef typename AllocPolicyType::template ListType<ANTLR_UINT32> IntListType;
	typedef typename ImplTraits::BitsetListType BitsetListType;

private:
	/// The actual bits themselves
	///
	BitsetListType		m_blist;

public:
	Bitset( ANTLR_UINT32 nbits=0 );
	Bitset( const Bitset& bitset );
    Bitset*  clone() const;
	Bitset*  bor(Bitset* bitset2);

	BitsetListType& get_blist();
	void	 borInPlace(Bitset* bitset2);
	ANTLR_UINT32 size() const;
	void	add(ANTLR_INT32 bit);
	void	grow(ANTLR_INT32 newSize);
	bool	equals(Bitset* bitset2) const;
	bool	isMember(ANTLR_UINT32 bit) const;
	ANTLR_UINT32 numBits() const;
	void remove(ANTLR_UINT32 bit);
	bool isNilNode() const;

	/** Produce an integer list of all the bits that are turned on
	 *  in this bitset. Used for error processing in the main as the bitset
	 *  reresents a number of integer tokens which we use for follow sets
	 *  and so on.
	 *
	 *  The first entry is the number of elements following in the list.
	 */
	ANTLR_INT32* toIntList() const;

	///
	/// \brief
	/// Creates a new bitset with at least one element, but as
	/// many elements are required.
	/// 
	/// \param[in] bit
	/// A variable number of bits to add to the set, ending in -1 (impossible bit).
	/// 
	/// \returns
	/// A new bit set with all of the specified elements added into it.
	/// 
	/// Call as:
	///  - pANTLR3_BITSET = antlrBitsetOf(n, n1, n2, -1);
	///  - pANTLR3_BITSET = antlrBitsetOf(-1);  Create empty bitset 
	///
	/// \remarks
	/// Stdargs function - must supply -1 as last paremeter, which is NOT
	/// added to the set.
	/// 
	///
	//C++ doesn't like variable length arguments. so use function overloading
	static Bitset* BitsetOf(ANTLR_INT32 bit);
	static Bitset* BitsetOf(ANTLR_INT32 bit1, ANTLR_INT32 bit2);
	
	///
	/// \brief
	/// Creates a new bitset with at least one 64 bit bset of bits, but as
	/// many 64 bit sets as are required.
	///
	/// \param[in] bset
	/// A variable number of bits to add to the set, ending in -1 (impossible bit).
	/// 
	/// \returns
	/// A new bit set with all of the specified bitmaps in it and the API
	/// initialized.
	/// 
	/// Call as:
	///  - pANTLR3_BITSET = antlrBitsetLoad(bset, bset11, ..., -1);
	///  - pANTLR3_BITSET = antlrBitsetOf(-1);  Create empty bitset 
	///
	/// \remarks
	/// Stdargs function - must supply -1 as last paremeter, which is NOT
	/// added to the set.
	/// 
	///antlr3BitsetList
	static Bitset*  BitsetFromList(const IntListType& list);
	~Bitset();

private:
	void	growToInclude(ANTLR_INT32 bit);
	static ANTLR_UINT64	BitMask(ANTLR_UINT32 bitNumber);
	static ANTLR_UINT32	NumWordsToHold(ANTLR_UINT32 bit);
	static ANTLR_UINT32	WordNumber(ANTLR_UINT32 bit);
	void bitsetORInPlace(Bitset* bitset2);
	
};

}

#include "antlr3bitset.inl"

#endif

