#ifndef JINJA2CPP_GENERIC_LIST_H
#define JINJA2CPP_GENERIC_LIST_H

#include <jinja2cpp/utils/i_comparable.h>
#include <jinja2cpp/value_ptr.h>

#include <optional>

#include <functional>
#include <iterator>
#include <memory>

namespace jinja2
{
class Value;

/*!
 * \brief Interface for accessing items in list by the indexes
 *
 * This interface should provided by the particular list implementation in case of support index-based access to the items.
 */
struct IIndexBasedAccessor : virtual IComparable
{
    virtual ~IIndexBasedAccessor() = default;
    /*!
     * \brief This method is called to get the item by the specified index
     *
     * @param idx Index of item to get
     *
     * @return requested item
     */
    virtual Value GetItemByIndex(int64_t idx) const = 0;
};

struct IListEnumerator;
using ListEnumeratorPtr = types::ValuePtr<IListEnumerator>;

inline auto MakeEmptyListEnumeratorPtr()
{
    return ListEnumeratorPtr();
}

/*!
 * \brief Generic list enumerator interface
 *
 * This interface should be implemented by the lists of any type. Interface is used to enumerate the list contents item by item.
 *
 * Implementation notes: Initial state of the enumerator should be "before the first item". So, the first call of \ref MoveNext method either moves the
 * enumerator to the first element end returns `true` or moves enumerator to the end and returns `false` in case of empty list. Each call of \ref GetCurrent
 * method should return the current enumerable item.
 */
struct IListEnumerator : virtual IComparable
{
    //! Destructor
    virtual ~IListEnumerator() = default;

    /*!
     * \brief Method is called to reset enumerator to the initial state ('before the first element') if applicable.
     *
     * For the sequences which allow multi-pass iteration this method should reset enumerator to the initial state. For the single-pass sequences method
     * can do nothing.
     */
    virtual void Reset() = 0;

    /*!
     * \brief Method is called to move the enumerator to the next item (if any)
     *
     * @return `true` if enumerator successfully moved and `false` if enumerator reaches the end of the sequence.
     */
    virtual bool MoveNext() = 0;

    /*!
     * \brief Method is called to get the current item value. Can be called multiply times
     *
     * @return Value of the item if the current item is valid item and empty value otherwise
     */
    virtual Value GetCurrent() const = 0;

    /*!
     * \brief Method is called to make a deep **copy** of the current enumerator state if possible
     *
     * @return New enumerator object with copy of the current enumerator state or empty pointer if copying is not applicable to the enumerator
     */
    virtual ListEnumeratorPtr Clone() const = 0;

    /*!
     * \brief Method is called to transfer current enumerator state to the new object
     *
     * State of the enumerator after successful creation of the new object is unspecified by there is a guarantee that there will no calls to the 'moved'
     * enumerator. Destruction of the moved enumerator shouldn't affect the newly created object.
     *
     * @return New enumerator object which holds and owns the current enumerator state
     */
    virtual ListEnumeratorPtr Move() = 0;
};

/*!
 * \brief Generic list implementation interface
 *
 * Every list implementation should implement this interface for providing access to the items of the list. There are several types of lists and implementation
 * notes for every of them:
 * - **Single-pass sequences** . For instance, input-stream iterators, generator-based sequences. This type of generic lists should provide no size and no indexer. Enumerator of such sequence supports should be moveable but non-copyable and non-resetable.
 * - **Forward sequences** . For instance, single-linked lists. This type of lists should provide no size and indexer. Enumerator should be moveable, copyable and resetable.
 * - **Bidirectional sequences**. Have got the same implementation requirements as forward sequences.
 * - **Random-access sequences**. Such as arrays or vectors. Should provide valid size (number of stored items), valid indexer implementation and moveable,
 *   copyable and resetable enumerator.
 *
 *   It's assumed that indexer interface is a part of list implementation.
 */
struct IListItemAccessor : virtual IComparable
{
    virtual ~IListItemAccessor() = default;

    /*!
     * \brief Called to get pointer to indexer interface implementation (if applicable)
     *
     * See implementation notes for the interface. This method should return pointer to the valid indexer interface if (and only if) list implementation
     * supports random access to the items.
     *
     * Method can be called several times from the different threads.
     *
     * @return Pointer to the indexer interface implementation or null if indexing isn't supported for the list
     */
    virtual const IIndexBasedAccessor* GetIndexer() const = 0;

    /*!
     * \brief Called to get enumerator of the particular list
     *
     * See implementation notes for the interface. This method should return unique pointer (with custom deleter) to the ListEnumerator interface implementation. Enumerator implementation should follow the requirements for the particular list type implementation.
     *
     * Method can be called several times from the different threads.
     *
     * @return Pointer to the enumerator of the list
     */
    virtual ListEnumeratorPtr CreateEnumerator() const = 0;

    /*!
     * \brief Called to get size of the list if applicable.
     *
     * See implementation notes for the interface. This method should return valid (non-empty) size only for random-access sequences. In other cases this
     * method should return empty optional.
     *
     * Method can be called several times from the different threads.
     *
     * @return Non-empty optional with the valid size of the list or empty optional in case of non-random sequence implementation
     */
    virtual std::optional<size_t> GetSize() const = 0;

    /*!
     * \brief Helper factory method of particular enumerator implementation
     *
     * @tparam T Type of enumerator to create
     * @tparam Args Type of enumerator construct args
     * @param args Actual enumerator constructor args
     * @return Unique pointer to the enumerator
     */
    template<typename T, typename... Args>
    static ListEnumeratorPtr MakeEnumerator(Args&&... args);
};


namespace detail
{
class GenericListIterator;
} // namespace detail

/*!
 * \brief Facade class for generic lists
 *
 * This class holds the implementation of particular generic list interface and provides friendly access to it's method. Also this class is used to hold
 * the particular list. Pointer to the generic list interface implementation is held inside std::function object which provides access to the pointer to the interface.
 *
 * You can use \ref MakeGenericList method to create instances of the GenericList:
 * ```
 * std::array<int, 9> sampleList{10, 20, 30, 40, 50, 60, 70, 80, 90};
 *
 * ValuesMap params = {
 *     {"input", jinja2::MakeGenericList(begin(sampleList), end(sampleList)) }
 * };
 * ```
 */
class JINJA2CPP_EXPORT GenericList
{
public:
    //! Default constructor
    GenericList() = default;

    /*!
     * \brief Initializing constructor
     *
     * This constructor is only one way to create the valid GenericList object. `accessor` is a functional object which provides access to the \ref IListItemAccessor
     * interface. The most common way of GenericList creation is to initialize it with lambda which simultaniously holds and and provide access to the
     * generic list implementation:
     *
     * ```
     * auto MakeGeneratedList(ListGenerator&& fn)
     * {
     *     return GenericList([accessor = GeneratedListAccessor(std::move(fn))]() {return &accessor;});
     * }
     * ```
     *
     * @param accessor Functional object which provides access to the particular generic list implementation
     */
    explicit GenericList(std::function<const IListItemAccessor*()> accessor)
        : m_accessor(std::move(accessor))
    {
    }

    /*!
     * \brief Get size of the list
     *
     * @return Actual size of the generic list or empty optional object if not applicable
     */
    std::optional<size_t> GetSize() const
    {
        return m_accessor ? m_accessor()->GetSize() : std::optional<size_t>();
    }

    /*!
     * \brief Get pointer to the list accessor interface implementation
     *
     * @return Pointer to the list accessor interface or nullptr in case of non-initialized GenericList object
     */
    auto GetAccessor() const
    {
        return m_accessor ? m_accessor() : nullptr;
    }

    /*!
     * \brief Check the GenericList object state
     *
     * @return true if GenericList object is valid (initialize) or false otherwize
     */
    bool IsValid() const
    {
        return !(!m_accessor);
    }

    /*!
     * \brief Get interator to the first element of the list
     *
     * @return Iterator to the first element of the generic list or iterator equal to the `end()` if list is empty or not initialized
     */
    detail::GenericListIterator begin() const;
    /*!
     * \brief Get the end iterator
     *
     * @return 'end' iterator of the generic list
     */
    detail::GenericListIterator end() const;

    /*!
     * \brief Get interator to the first element of the list
     *
     * @return Iterator to the first element of the generic list or iterator equal to the `end()` if list is empty or not initialized
     */
    auto cbegin() const;
    /*!
     * \brief Get the end iterator
     *
     * @return 'end' iterator of the generic list
     */
    auto cend() const;

    /*!
     * \brief Compares with the objects of same type
     *
     * @return true if equal
     */
    bool IsEqual(const GenericList& rhs) const;

private:
    std::function<const IListItemAccessor*()> m_accessor;
};

bool operator==(const GenericList& lhs, const GenericList& rhs);
bool operator!=(const GenericList& lhs, const GenericList& rhs);

template<typename T, typename ...Args>
inline ListEnumeratorPtr IListItemAccessor::MakeEnumerator(Args&& ...args)
{
    return ListEnumeratorPtr(types::MakeValuePtr<T>(std::forward<Args>(args)...));
}
} // namespace jinja2


#endif // JINJA2CPP_GENERIC_LIST_H
