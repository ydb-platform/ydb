//
// RowFilter.h
//
// Library: Data
// Package: DataCore
// Module:  RowFilter
//
// Definition of the RowFilter class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CHDB_Data_RowFilter_INCLUDED
#define CHDB_Data_RowFilter_INCLUDED


#include <list>
#include <map>
#include <utility>
#include "CHDBPoco/AutoPtr.h"
#include "CHDBPoco/Data/Data.h"
#include "CHDBPoco/Dynamic/Var.h"
#include "CHDBPoco/RefCountedObject.h"
#include "CHDBPoco/String.h"
#include "CHDBPoco/Tuple.h"


namespace CHDBPoco
{
namespace Data
{


    class RecordSet;


    class Data_API RowFilter : public RefCountedObject
    /// RowFilter class provides row filtering functionality.
    /// A filter contains a set of criteria (field name, value and
    /// logical operation) for row filtering.
    /// Additionally, a row filter contains a map of pointers to other
    /// filters with related logical operations between filters.
    /// RowFilter is typically added to recordset in order to filter
    /// its content. Since the recordset own iteration is dependent upon
    /// filtering, whenever the filtering criteria is changed,
    /// the filter automatically notifies all associated recordsets
    /// by rewinding them to the first position.
    {
    public:
        enum Comparison
        {
            VALUE_LESS_THAN,
            VALUE_LESS_THAN_OR_EQUAL,
            VALUE_EQUAL,
            VALUE_GREATER_THAN,
            VALUE_GREATER_THAN_OR_EQUAL,
            VALUE_NOT_EQUAL,
            VALUE_IS_NULL
        };

        enum LogicOperator
        {
            OP_AND,
            OP_OR,
            OP_NOT
        };

        typedef bool (*CompT)(const CHDBPoco::Dynamic::Var &, const CHDBPoco::Dynamic::Var &);
        typedef AutoPtr<RowFilter> Ptr;
        typedef std::map<std::string, Comparison> Comparisons;
        typedef Tuple<CHDBPoco::Dynamic::Var, Comparison, LogicOperator> ComparisonEntry;
        typedef std::multimap<std::string, ComparisonEntry> ComparisonMap;
        typedef std::map<AutoPtr<RowFilter>, LogicOperator> FilterMap;

        RowFilter(RecordSet * pRecordSet);
        /// Creates the top-level RowFilter and associates it with the recordset.

        RowFilter(Ptr pParent, LogicOperator op = OP_OR);
        /// Creates child RowFilter and associates it with the parent filter.

        ~RowFilter();
        /// Destroys the RowFilter.

        void addFilter(Ptr pFilter, LogicOperator comparison);
        /// Appends another filter to this one.

        void removeFilter(Ptr pFilter);
        /// Removes filter from this filter.

        bool has(Ptr pFilter) const;
        /// Returns true if this filter is parent of pFilter;

        template <typename T>
        void add(const std::string & name, Comparison comparison, const T & value, LogicOperator op = OP_OR)
        /// Adds value to the filter.
        {
            rewindRecordSet();
            _comparisonMap.insert(ComparisonMap::value_type(toUpper(name), ComparisonEntry(value, comparison, op)));
        }

        template <typename T>
        void add(const std::string & name, const std::string & comp, const T & value, LogicOperator op = OP_OR)
        /// Adds value to the filter.
        {
            add(name, getComparison(comp), value, op);
        }

        template <typename T>
        void addAnd(const std::string & name, const std::string & comp, const T & value)
        /// Adds logically AND-ed value to the filter.
        {
            add(name, getComparison(comp), value, OP_AND);
        }

        template <typename T>
        void addOr(const std::string & name, const std::string & comp, const T & value)
        /// Adds logically OR-ed value to the filter.
        {
            add(name, getComparison(comp), value, OP_OR);
        }

        int remove(const std::string & name);
        /// Removes named comparisons from the filter.
        /// All comparisons with specified name are removed.
        /// Returns the number of comparisons removed.

        void toggleNot();
        /// Togless the NOT operator for this filter;

        bool isNot() const;
        /// Returns true if filter is NOT-ed, false otherwise.

        bool isEmpty() const;
        /// Returns true if there is not filtering criteria specified.

        bool isAllowed(std::size_t row) const;
        /// Returns true if name and value are allowed.

        bool exists(const std::string & name) const;
        /// Returns true if name is known to this row filter.

    private:
        RowFilter();
        RowFilter(const RowFilter &);
        RowFilter & operator=(const RowFilter &);

        void init();

        static bool equal(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool notEqual(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool less(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool greater(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool lessOrEqual(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool greaterOrEqual(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool logicalAnd(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool logicalOr(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2);
        static bool isNull(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var &);

        static void doCompare(CHDBPoco::Dynamic::Var & ret, CHDBPoco::Dynamic::Var & val, CompT comp, const ComparisonEntry & ce);

        RecordSet & recordSet() const;

        Comparison getComparison(const std::string & comp) const;

        void rewindRecordSet();

        Comparisons _comparisons;
        ComparisonMap _comparisonMap;
        mutable RecordSet * _pRecordSet;
        Ptr _pParent;
        FilterMap _filterMap;
        bool _not;

        friend class RecordSet;
    };


    ///
    /// inlines
    ///


    inline bool RowFilter::has(Ptr pFilter) const
    {
        return _filterMap.find(pFilter) != _filterMap.end();
    }


    inline bool RowFilter::isEmpty() const
    {
        return _comparisonMap.size() == 0;
    }


    inline bool RowFilter::exists(const std::string & name) const
    {
        return _comparisonMap.find(name) != _comparisonMap.end();
    }


    inline void RowFilter::toggleNot()
    {
        _not = !_not;
    }


    inline bool RowFilter::isNot() const
    {
        return _not;
    }


    inline bool RowFilter::equal(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 == p2;
    }


    inline bool RowFilter::notEqual(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 != p2;
    }


    inline bool RowFilter::less(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 < p2;
    }


    inline bool RowFilter::greater(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 > p2;
    }


    inline bool RowFilter::lessOrEqual(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 <= p2;
    }


    inline bool RowFilter::greaterOrEqual(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 >= p2;
    }


    inline bool RowFilter::logicalAnd(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 && p2;
    }


    inline bool RowFilter::logicalOr(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var & p2)
    {
        return p1 || p2;
    }


    inline bool RowFilter::isNull(const CHDBPoco::Dynamic::Var & p1, const CHDBPoco::Dynamic::Var &)
    {
        return p1.isEmpty();
    }


}
} // namespace CHDBPoco::Data


#endif // CHDB_Data_RowFilter_INCLUDED
