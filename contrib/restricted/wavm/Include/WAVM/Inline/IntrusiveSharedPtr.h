#pragma once

namespace WAVM {
	// A smart pointer that uses an intrusive reference counter.
	// Needs Pointee to define memory functions to manipulate the reference count:
	//   void addRef();
	//   void removeRef();
	template<typename Pointee> struct IntrusiveSharedPtr
	{
		// Constructors/destructor
		constexpr IntrusiveSharedPtr() : value(nullptr) {}
		IntrusiveSharedPtr(Pointee* inValue)
		{
			value = inValue;
			if(value) { value->addRef(); }
		}
		IntrusiveSharedPtr(const IntrusiveSharedPtr<Pointee>& inCopy)
		{
			value = inCopy.value;
			if(value) { value->addRef(); }
		}
		IntrusiveSharedPtr(IntrusiveSharedPtr<Pointee>&& inMove) noexcept
		{
			value = inMove.value;
			inMove.value = nullptr;
		}

		~IntrusiveSharedPtr()
		{
			if(value) { value->removeRef(); }
		}

		// Adopt a raw pointer: coerces the pointer to an IntrusiveSharedPtr without calling addRef
		// or removeRef.
		static IntrusiveSharedPtr<Pointee> adopt(Pointee*&& inValue)
		{
			IntrusiveSharedPtr<Pointee> result(adoptConstructorSelector, inValue);
			inValue = nullptr;
			return result;
		}

		// Assignment operators
		void operator=(Pointee* inValue)
		{
			auto oldValue = value;
			value = inValue;
			if(value) { value->addRef(); }
			if(oldValue) { oldValue->removeRef(); }
		}
		void operator=(const IntrusiveSharedPtr<Pointee>& inCopy)
		{
			auto oldValue = value;
			value = inCopy.value;
			if(value) { value->addRef(); }
			if(oldValue) { oldValue->removeRef(); }
		}
		void operator=(IntrusiveSharedPtr<Pointee>&& inMove) noexcept
		{
			auto oldValue = value;
			value = inMove.value;
			inMove.value = nullptr;
			if(oldValue) { oldValue->removeRef(); }
		}

		// Accessors
		constexpr operator Pointee*() const { return value; }
		constexpr Pointee& operator*() const { return *value; }
		constexpr Pointee* operator->() const { return value; }

	private:
		Pointee* value;

		enum AdoptConstructorSelector
		{
			adoptConstructorSelector
		};

		constexpr IntrusiveSharedPtr(AdoptConstructorSelector, Pointee* inValue) : value(inValue) {}
	};
}
