//
// TypeHandler.h
//
// Library: Data
// Package: DataCore
// Module:  TypeHandler
//
// Definition of the TypeHandler class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_TypeHandler_INCLUDED
#define CHDB_Data_TypeHandler_INCLUDED


#include <cstddef>
#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Data/AbstractBinder.h"
#include "CHDBPoco/Data/AbstractExtractor.h"
#include "CHDBPoco/Data/AbstractPreparator.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Nullable.h"
#include "CHDBPoco/SharedPtr.h"
#include "CHDBPoco/Tuple.h"


namespace CHDBPoco
{
namespace Data
{


    class AbstractTypeHandler
    /// Parent class for type handlers.
    /// The reason for this class is to prevent instantiations of type handlers.
    /// For documentation on type handlers, see TypeHandler class.
    {
    protected:
        AbstractTypeHandler();
        ~AbstractTypeHandler();
        AbstractTypeHandler(const AbstractTypeHandler &);
        AbstractTypeHandler & operator=(const AbstractTypeHandler &);
    };


    template <class T>
    class TypeHandler : public AbstractTypeHandler
    /// Converts Rows to a Type and the other way around. Provide template specializations to support your own complex types.
    ///
    /// Take as example the following (simplified) class:
    ///	class Person
    ///	{
    ///	private:
    ///		std::string _lastName;
    ///		std::string _firstName;
    ///		int		 _age;
    ///	public:
    ///		const std::string& getLastName();
    ///		[...] // other set/get methods (returning const reference), a default constructor,
    ///		[...] // optional < operator (for set, multiset) or function operator (for map, multimap)
    ///	};
    ///
    /// The TypeHandler must provide a custom bind, size, prepare and extract method:
    ///
    ///	template <>
    ///	class TypeHandler<struct Person>
    ///	{
    ///	public:
    ///		static std::size_t size()
    ///		{
    ///			return 3; // lastName + firstname + age occupy three columns
    ///		}
    ///
    ///		static void bind(std::size_t pos, const Person& obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
    ///		{
    ///			// the table is defined as Person (LastName VARCHAR(30), FirstName VARCHAR, Age INTEGER(3))
    ///			// Note that we advance pos by the number of columns the datatype uses! For string/int this is one.
    ///			CHDB_poco_assert_dbg (!pBinder.isNull());
    ///			TypeHandler<std::string>::bind(pos++, obj.getLastName(), pBinder, dir);
    ///			TypeHandler<std::string>::bind(pos++, obj.getFirstName(), pBinder, dir);
    ///			TypeHandler<int>::bind(pos++, obj.getAge(), pBinder, dir);
    ///		}
    ///
    ///		static void prepare(std::size_t pos, const Person& obj, AbstractPreparator::Ptr pPreparator)
    ///		{
    ///			// the table is defined as Person (LastName VARCHAR(30), FirstName VARCHAR, Age INTEGER(3))
    ///			CHDB_poco_assert_dbg (!pPreparator.isNull());
    ///			TypeHandler<std::string>::prepare(pos++, obj.getLastName(), pPreparator);
    ///			TypeHandler<std::string>::prepare(pos++, obj.getFirstName(), pPreparator);
    ///			TypeHandler<int>::prepare(pos++, obj.getAge(), pPreparator);
    ///		}
    ///
    ///		static void extract(std::size_t pos, Person& obj, const Person& defVal, AbstractExtractor::Ptr pExt)
    ///		{
    ///			// defVal is the default person we should use if we encounter NULL entries, so we take the individual fields
    ///			// as defaults. You can do more complex checking, ie return defVal if only one single entry of the fields is null etc...
    ///			CHDB_poco_assert_dbg (!pExt.isNull());
    ///			std::string lastName;
    ///			std::string firstName;
    ///			int age = 0;
    ///			// the table is defined as Person (LastName VARCHAR(30), FirstName VARCHAR, Age INTEGER(3))
    ///			TypeHandler<std::string>::extract(pos++, lastName, defVal.getLastName(), pExt);
    ///			TypeHandler<std::string>::extract(pos++, firstName, defVal.getFirstName(), pExt);
    ///			TypeHandler<int>::extract(pos++, age, defVal.getAge(), pExt);
    ///			obj.setLastName(lastName);
    ///			obj.setFirstName(firstName);
    ///			obj.setAge(age);
    ///		}
    ///	};
    ///
    /// Note that the TypeHandler template specialization must always be declared in the namespace CHDBPoco::Data.
    /// Apart from that no further work is needed. One can now use Person with into and use clauses.
    {
    public:
        static void bind(std::size_t pos, const T & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            pBinder->bind(pos, obj, dir);
        }

        static std::size_t size() { return 1u; }

        static void extract(std::size_t pos, T & obj, const T & defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            if (!pExt->extract(pos, obj))
                obj = defVal;
        }

        static void prepare(std::size_t pos, const T & obj, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            pPreparator->prepare(pos, obj);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T>
    class TypeHandler<std::deque<T>> : public AbstractTypeHandler
    /// Specialization of type handler for std::deque.
    {
    public:
        static void bind(std::size_t pos, const std::deque<T> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            pBinder->bind(pos, obj, dir);
        }

        static std::size_t size() { return 1u; }

        static void extract(std::size_t pos, std::deque<T> & obj, const T & defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            if (!pExt->extract(pos, obj))
                obj.assign(obj.size(), defVal);
        }

        static void prepare(std::size_t pos, const std::deque<T> & obj, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            pPreparator->prepare(pos, obj);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T>
    class TypeHandler<std::vector<T>> : public AbstractTypeHandler
    /// Specialization of type handler for std::vector.
    {
    public:
        static void bind(std::size_t pos, const std::vector<T> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            pBinder->bind(pos, obj, dir);
        }

        static std::size_t size() { return 1u; }

        static void extract(std::size_t pos, std::vector<T> & obj, const T & defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            if (!pExt->extract(pos, obj))
                obj.assign(obj.size(), defVal);
        }

        static void prepare(std::size_t pos, const std::vector<T> & obj, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            pPreparator->prepare(pos, obj);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T>
    class TypeHandler<std::list<T>> : public AbstractTypeHandler
    /// Specialization of type handler for std::list.
    {
    public:
        static void bind(std::size_t pos, const std::list<T> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            pBinder->bind(pos, obj, dir);
        }

        static std::size_t size() { return 1u; }

        static void extract(std::size_t pos, std::list<T> & obj, const T & defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            if (!pExt->extract(pos, obj))
                obj.assign(obj.size(), defVal);
        }

        static void prepare(std::size_t pos, const std::list<T> & obj, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            pPreparator->prepare(pos, obj);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };

    template <typename T>
    class TypeHandler<Nullable<T>>
    /// Specialization of type handler for Nullable.
    {
    public:
        static void bind(std::size_t pos, const Nullable<T> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            if (obj.isNull())
            {
                pBinder->bind(pos++, CHDBPoco::Data::Keywords::null, dir);
            }
            else
            {
                pBinder->bind(pos++, obj.value(), dir);
            }
        }

        static void prepare(std::size_t pos, const Nullable<T> & obj, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            if (obj.isNull())
            {
                pPreparator->prepare(pos++, CHDBPoco::Data::Keywords::null);
            }
            else
            {
                pPreparator->prepare(pos++, obj.value());
            }
        }

        static std::size_t size() { return 1u; }

        static void extract(std::size_t pos, Nullable<T> & obj, const Nullable<T> &, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            T val;

            if (pExt->extract(pos++, val))
            {
                obj = val;
            }
            else
            {
                obj.clear();
            }
        }

    private:
        TypeHandler();
        ~TypeHandler();
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


/// CHDBPoco::Tuple TypeHandler specializations

// define this macro to nothing for smaller code size
#define CHDB_POCO_TUPLE_TYPE_HANDLER_INLINE inline


    template <typename TupleType, typename Type, int N>
    CHDB_POCO_TUPLE_TYPE_HANDLER_INLINE void
    tupleBind(std::size_t & pos, TupleType tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
    {
        TypeHandler<Type>::bind(pos, tuple.template get<N>(), pBinder, dir);
        pos += TypeHandler<Type>::size();
    }


    template <typename TupleType, typename Type, int N>
    CHDB_POCO_TUPLE_TYPE_HANDLER_INLINE void tuplePrepare(std::size_t & pos, TupleType tuple, AbstractPreparator::Ptr pPreparator)
    {
        TypeHandler<Type>::prepare(pos, tuple.template get<N>(), pPreparator);
        pos += TypeHandler<Type>::size();
    }


    template <typename TupleType, typename DefValType, typename Type, int N>
    CHDB_POCO_TUPLE_TYPE_HANDLER_INLINE void tupleExtract(std::size_t & pos, TupleType tuple, DefValType defVal, AbstractExtractor::Ptr pExt)
    {
        CHDBPoco::Data::TypeHandler<Type>::extract(pos, tuple.template get<N>(), defVal.template get<N>(), pExt);
        pos += TypeHandler<Type>::size();
    }


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13,
        class T14,
        class T15,
        class T16,
        class T17,
        class T18,
        class T19>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<
            CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>::CONSTREFTYPE
            TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<
            CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18, T19>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T14, 14>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T15, 15>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T16, 16>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T17, 17>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T18, 18>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T19, 19>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T14, 14>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T15, 15>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T16, 16>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T17, 17>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T18, 18>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T19, 19>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size() + TypeHandler<T14>::size() + TypeHandler<T15>::size()
                + TypeHandler<T16>::size() + TypeHandler<T17>::size() + TypeHandler<T18>::size() + TypeHandler<T19>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T14, 14>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T15, 15>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T16, 16>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T17, 17>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T18, 18>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T19, 19>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13,
        class T14,
        class T15,
        class T16,
        class T17,
        class T18>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<
            CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<
            CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17, T18>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T14, 14>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T15, 15>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T16, 16>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T17, 17>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T18, 18>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T14, 14>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T15, 15>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T16, 16>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T17, 17>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T18, 18>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size() + TypeHandler<T14>::size() + TypeHandler<T15>::size()
                + TypeHandler<T16>::size() + TypeHandler<T17>::size() + TypeHandler<T18>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T14, 14>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T15, 15>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T16, 16>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T17, 17>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T18, 18>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13,
        class T14,
        class T15,
        class T16,
        class T17>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<
            CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>::CONSTREFTYPE TupleConstRef;
        typedef
            typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16, T17>>::REFTYPE
                TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T14, 14>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T15, 15>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T16, 16>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T17, 17>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T14, 14>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T15, 15>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T16, 16>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T17, 17>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size() + TypeHandler<T14>::size() + TypeHandler<T15>::size()
                + TypeHandler<T16>::size() + TypeHandler<T17>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T14, 14>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T15, 15>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T16, 16>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T17, 17>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13,
        class T14,
        class T15,
        class T16>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>
    {
    public:
        typedef
            typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>::CONSTREFTYPE
                TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15, T16>>::REFTYPE
            TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T14, 14>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T15, 15>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T16, 16>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T14, 14>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T15, 15>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T16, 16>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size() + TypeHandler<T14>::size() + TypeHandler<T15>::size()
                + TypeHandler<T16>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T14, 14>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T15, 15>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T16, 16>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13,
        class T14,
        class T15>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>::CONSTREFTYPE
            TupleConstRef;
        typedef
            typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T14, 14>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T15, 15>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T14, 14>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T15, 15>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size() + TypeHandler<T14>::size() + TypeHandler<T15>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T14, 14>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T15, 15>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13,
        class T14>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>::CONSTREFTYPE
            TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T14, 14>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T14, 14>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size() + TypeHandler<T14>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T14, 14>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12,
        class T13>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>
    {
    public:
        typedef
            typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T13, 13>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T13, 13>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size() + TypeHandler<T13>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T13, 13>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <
        class T0,
        class T1,
        class T2,
        class T3,
        class T4,
        class T5,
        class T6,
        class T7,
        class T8,
        class T9,
        class T10,
        class T11,
        class T12>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T12, 12>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T12, 12>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size()
                + TypeHandler<T12>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T12, 12>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5, class T6, class T7, class T8, class T9, class T10, class T11>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T11, 11>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T11, 11>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size() + TypeHandler<T11>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T11, 11>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5, class T6, class T7, class T8, class T9, class T10>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T10, 10>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T10, 10>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size() + TypeHandler<T10>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T10, 10>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5, class T6, class T7, class T8, class T9>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, T9>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T9, 9>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T9, 9>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size() + TypeHandler<T9>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T9, 9>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5, class T6, class T7, class T8>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, T8, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T8, 8>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T8, 8>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size()
                + TypeHandler<T8>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T8, 8>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5, class T6, class T7>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, T7, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T7, 7>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T7, 7>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size() + TypeHandler<T7>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T7, 7>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5, class T6>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, T6, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T6, 6>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T6, 6>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size() + TypeHandler<T6>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T6, 6>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4, class T5>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, T5, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T5, 5>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T5, 5>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size() + TypeHandler<T5>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T5, 5>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3, class T4>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, T4, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, T4, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T4, 4>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T4, 4>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size()
                + TypeHandler<T4>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T4, 4>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2, class T3>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, T3, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, T3, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T3, 3>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T3, 3>(pos, tuple, pPreparator);
        }

        static std::size_t size()
        {
            return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size() + TypeHandler<T3>::size();
        }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T3, 3>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1, class T2>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, T2, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, T2, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T2, 2>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T2, 2>(pos, tuple, pPreparator);
        }

        static std::size_t size() { return TypeHandler<T0>::size() + TypeHandler<T1>::size() + TypeHandler<T2>::size(); }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T2, 2>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0, class T1>
    class TypeHandler<CHDBPoco::Tuple<T0, T1, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, T1, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
            tupleBind<TupleConstRef, T1, 1>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
            tuplePrepare<TupleConstRef, T1, 1>(pos, tuple, pPreparator);
        }

        static std::size_t size() { return TypeHandler<T0>::size() + TypeHandler<T1>::size(); }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
            tupleExtract<TupleRef, TupleConstRef, T1, 1>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T0>
    class TypeHandler<CHDBPoco::Tuple<T0, NullTypeList>>
    {
    public:
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, NullTypeList>>::CONSTREFTYPE TupleConstRef;
        typedef typename CHDBPoco::TypeWrapper<CHDBPoco::Tuple<T0, NullTypeList>>::REFTYPE TupleRef;

        static void bind(std::size_t pos, TupleConstRef tuple, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            CHDB_poco_assert_dbg(!pBinder.isNull());
            tupleBind<TupleConstRef, T0, 0>(pos, tuple, pBinder, dir);
        }

        static void prepare(std::size_t pos, TupleConstRef tuple, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            tuplePrepare<TupleConstRef, T0, 0>(pos, tuple, pPreparator);
        }

        static std::size_t size() { return TypeHandler<T0>::size(); }

        static void extract(std::size_t pos, TupleRef tuple, TupleConstRef defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());
            tupleExtract<TupleRef, TupleConstRef, T0, 0>(pos, tuple, defVal, pExt);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class K, class V>
    class TypeHandler<std::pair<K, V>> : public AbstractTypeHandler
    {
    public:
        static void bind(std::size_t pos, const std::pair<K, V> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            TypeHandler<K>::bind(pos, obj.first, pBinder, dir);
            pos += TypeHandler<K>::size();
            TypeHandler<V>::bind(pos, obj.second, pBinder, dir);
        }

        static std::size_t size() { return static_cast<std::size_t>(TypeHandler<K>::size() + TypeHandler<V>::size()); }

        static void extract(std::size_t pos, std::pair<K, V> & obj, const std::pair<K, V> & defVal, AbstractExtractor::Ptr pExt)
        {
            TypeHandler<K>::extract(pos, obj.first, defVal.first, pExt);
            pos += TypeHandler<K>::size();
            TypeHandler<V>::extract(pos, obj.second, defVal.second, pExt);
        }

        static void prepare(std::size_t pos, const std::pair<K, V> & obj, AbstractPreparator::Ptr pPreparator)
        {
            TypeHandler<K>::prepare(pos, obj.first, pPreparator);
            pos += TypeHandler<K>::size();
            TypeHandler<V>::prepare(pos, obj.second, pPreparator);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T>
    class TypeHandler<CHDBPoco::AutoPtr<T>> : public AbstractTypeHandler
    /// Specialization of type handler for CHDBPoco::AutoPtr
    {
    public:
        static void bind(std::size_t pos, const CHDBPoco::AutoPtr<T> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            // *obj will trigger a nullpointer exception if empty: this is on purpose
            TypeHandler<T>::bind(pos, *obj, pBinder, dir);
        }

        static std::size_t size() { return static_cast<std::size_t>(TypeHandler<T>::size()); }

        static void extract(std::size_t pos, CHDBPoco::AutoPtr<T> & obj, const CHDBPoco::AutoPtr<T> & defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());

            obj = CHDBPoco::AutoPtr<T>(new T());
            if (defVal)
                TypeHandler<T>::extract(pos, *obj, *defVal, pExt);
            else
                TypeHandler<T>::extract(pos, *obj, *obj, pExt);
        }

        static void prepare(std::size_t pos, const CHDBPoco::AutoPtr<T> &, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            TypeHandler<T>::prepare(pos, T(), pPreparator);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


    template <class T>
    class TypeHandler<CHDBPoco::SharedPtr<T>> : public AbstractTypeHandler
    /// Specialization of type handler for CHDBPoco::SharedPtr
    {
    public:
        static void bind(std::size_t pos, const CHDBPoco::SharedPtr<T> & obj, AbstractBinder::Ptr pBinder, AbstractBinder::Direction dir)
        {
            // *obj will trigger a nullpointer exception if empty
            TypeHandler<T>::bind(pos, *obj, pBinder, dir);
        }

        static std::size_t size() { return static_cast<std::size_t>(TypeHandler<T>::size()); }

        static void extract(std::size_t pos, CHDBPoco::SharedPtr<T> & obj, const CHDBPoco::SharedPtr<T> & defVal, AbstractExtractor::Ptr pExt)
        {
            CHDB_poco_assert_dbg(!pExt.isNull());

            obj = CHDBPoco::SharedPtr<T>(new T());
            if (defVal)
                TypeHandler<T>::extract(pos, *obj, *defVal, pExt);
            else
                TypeHandler<T>::extract(pos, *obj, *obj, pExt);
        }

        static void prepare(std::size_t pos, const CHDBPoco::SharedPtr<T> &, AbstractPreparator::Ptr pPreparator)
        {
            CHDB_poco_assert_dbg(!pPreparator.isNull());
            TypeHandler<T>::prepare(pos, T(), pPreparator);
        }

    private:
        TypeHandler(const TypeHandler &);
        TypeHandler & operator=(const TypeHandler &);
    };


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_TypeHandler_INCLUDED
