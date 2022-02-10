#ifndef	_ANTLR3MEMORY_HPP
#define	_ANTLR3MEMORY_HPP

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

class DefaultAllocPolicy
{
public:
	//limitation of c++. unable to write a typedef 
	template <class TYPE>
	class AllocatorType : public std::allocator<TYPE>
	{
	public:
		typedef TYPE value_type;
		typedef value_type* pointer;
		typedef const value_type* const_pointer;
		typedef value_type& reference;
		typedef const value_type& const_reference;
		typedef size_t size_type;
		typedef ptrdiff_t difference_type;
		template<class U> struct rebind {
			typedef AllocatorType<U> other;
		};

		AllocatorType() noexcept {}
		AllocatorType( const AllocatorType& ) noexcept {}
		template<typename U> AllocatorType(const AllocatorType<U>& ) noexcept{}
	};

	template<class TYPE>
	class VectorType : public std::vector< TYPE, AllocatorType<TYPE> >
	{
	};
	
	template<class TYPE>
	class ListType : public std::deque< TYPE, AllocatorType<TYPE> >
	{
	};	

	template<class TYPE>
	class StackType : public std::deque< TYPE, AllocatorType<TYPE> >
	{
	public:
		void push( const TYPE& elem ) {  this->push_back(elem); 	}
		void pop()  { this->pop_back(); }
		TYPE& peek() { return this->back(); }
		TYPE& top() { return this->back(); }
		const TYPE& peek() const { return this->back(); }
		const TYPE& top() const { return this->back(); }
	};	


	template<class TYPE>
	class OrderedSetType : public std::set< TYPE, std::less<TYPE>, AllocatorType<TYPE> >
	{
	};

	template<class TYPE>
	class UnOrderedSetType : public std::set< TYPE, std::less<TYPE>, AllocatorType<TYPE> >
	{
	};

	template<class KeyType, class ValueType>
	class UnOrderedMapType : public std::map< KeyType, ValueType, std::less<KeyType>, 
										AllocatorType<std::pair<const KeyType, ValueType> > >
	{
	};

	template<class KeyType, class ValueType>
	class OrderedMapType : public std::map< KeyType, ValueType, std::less<KeyType>, 
										AllocatorType<std::pair<KeyType, ValueType> > >
	{
	};

	template<class TYPE>
	class SmartPtrType : public std::unique_ptr<TYPE, std::default_delete<TYPE> >
	{
		typedef typename std::unique_ptr<TYPE, std::default_delete<TYPE> > BaseType;
	public:
		SmartPtrType() {};
		SmartPtrType( SmartPtrType&& other )
            : BaseType(other)
		{};
		SmartPtrType & operator=(SmartPtrType&& other) //= default;
		{
			BaseType::swap(other);
			//return std::move((BaseType&)other);
			return *this;
		}
	private:
		SmartPtrType & operator=(const SmartPtrType&) /*= delete*/;
		SmartPtrType(const SmartPtrType&) /*= delete*/;
	};

	ANTLR_INLINE static void* operator new (std::size_t bytes)
	{ 
		void* p = alloc(bytes);
		return p;
	}
	ANTLR_INLINE static void* operator new (std::size_t , void* p) { return p; }
	ANTLR_INLINE static void* operator new[]( std::size_t bytes)
	{
		void* p = alloc(bytes); 
		return p;
	}
	ANTLR_INLINE static void operator delete(void* p)
	{
		DefaultAllocPolicy::free(p);
	}
	ANTLR_INLINE static void operator delete(void* , void* ) {} //placement delete

	ANTLR_INLINE static void operator delete[](void* p)
	{
		DefaultAllocPolicy::free(p);
	}

	ANTLR_INLINE static void* alloc( std::size_t bytes )
	{
		void* p = malloc(bytes); 
		if( p== NULL )
			throw std::bad_alloc();
		return p;
	}

	ANTLR_INLINE static void* alloc0( std::size_t bytes )
	{
		void* p = calloc(1, bytes);
		if( p== NULL )
			throw std::bad_alloc();
		return p;
	}

	ANTLR_INLINE static void  free( void* p )
	{
		return ::free(p);
	}
	
	ANTLR_INLINE static void* realloc(void *ptr, size_t size)
	{
		return ::realloc( ptr, size );
	}
};

}

#endif	/* _ANTLR3MEMORY_H */
