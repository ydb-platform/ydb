#pragma once

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/list.h>
#include <util/generic/map.h>
#include <util/generic/ptr.h>
#include <util/generic/singleton.h>
#include <util/generic/vector.h>
#include <util/str_stl.h>
#include <util/stream/output.h>
#include <util/string/util.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/defaults.h>
#include <util/system/guard.h>
#include <util/system/sem.h>
#include <util/system/spinlock.h>

#include <array>

namespace NMonitoring {
#define BEGIN_OUTPUT_COUNTERS             \
    void OutputImpl(IOutputStream& out) { \
        char prettyBuf[32];
#define END_OUTPUT_COUNTERS \
    out.Flush();            \
    }

#define OUTPUT_NAMED_COUNTER(var, name) out << name << ": \t" << var << NMonitoring::PrettyNum(var, prettyBuf, 32) << '\n'
#define OUTPUT_COUNTER(var) OUTPUT_NAMED_COUNTER(var, #var);

    char* PrettyNumShort(i64 val, char* buf, size_t size);
    char* PrettyNum(i64 val, char* buf, size_t size);

    // This class is deprecated. Please consider to use
    // library/cpp/monlib/metrics instead. See more info at
    // https://wiki.yandex-team.ru/solomon/libs/monlib_cpp/
    class TDeprecatedCounter {
    public:
        using TValue = TAtomic;
        using TValueBase = TAtomicBase;

        TDeprecatedCounter()
            : Value()
            , Derivative(false)
        {
        }

        TDeprecatedCounter(TValueBase value, bool derivative = false)
            : Value(value)
            , Derivative(derivative)
        {
        }

        bool ForDerivative() const {
            return Derivative;
        }

        operator TValueBase() const {
            return AtomicGet(Value);
        }
        TValueBase Val() const {
            return AtomicGet(Value);
        }

        void Set(TValueBase val) {
            AtomicSet(Value, val);
        }

        TValueBase Inc() {
            return AtomicIncrement(Value);
        }
        TValueBase Dec() {
            return AtomicDecrement(Value);
        }

        TValueBase Add(const TValueBase val) {
            return AtomicAdd(Value, val);
        }
        TValueBase Sub(const TValueBase val) {
            return AtomicAdd(Value, -val);
        }

        // operator overloads convinient
        void operator++() {
            Inc();
        }
        void operator++(int) {
            Inc();
        }

        void operator--() {
            Dec();
        }
        void operator--(int) {
            Dec();
        }

        void operator+=(TValueBase rhs) {
            Add(rhs);
        }
        void operator-=(TValueBase rhs) {
            Sub(rhs);
        }

        TValueBase operator=(TValueBase rhs) {
            AtomicSwap(&Value, rhs);
            return rhs;
        }

        bool operator!() const {
            return AtomicGet(Value) == 0;
        }

        TAtomic& GetAtomic() {
            return Value;
        }

    private:
        TAtomic Value;
        bool Derivative;
    };

    template <typename T>
    struct TDeprecatedCountersBase {
        virtual ~TDeprecatedCountersBase() {
        }

        virtual void OutputImpl(IOutputStream&) = 0;

        static T& Instance() {
            return *Singleton<T>();
        }

        static void Output(IOutputStream& out) {
            Instance().OutputImpl(out);
        }
    };

    // This class is deprecated. Please consider to use
    // library/cpp/monlib/metrics instead. See more info at
    // https://wiki.yandex-team.ru/solomon/libs/monlib_cpp/
    //
    // Groups of G counters, defined by T type.
    // Less(a,b) returns true, if a < b.
    // It's threadsafe.
    template <typename T, typename G, typename TL = TLess<T>>
    class TDeprecatedCounterGroups {
    public:
        typedef TMap<T, G*> TGroups;
        typedef TVector<T> TGroupsNames;
        typedef THolder<TGroupsNames> TGroupsNamesPtr;

    private:
        class TCollection {
            struct TElement {
                T* Name;
                G* Counters;

            public:
                static bool Compare(const TElement& a, const TElement& b) {
                    return Less(*(a.Name), *(b.Name));
                }
            }; // TElement
        private:
            TArrayHolder<TElement> Elements;
            size_t Size;

        public:
            TCollection()
                : Size(0)
            {
            }

            TCollection(const TCollection& collection)
                : Elements(new TElement[collection.Size])
                , Size(collection.Size)
            {
                for (int i = 0; i < Size; ++i) {
                    Elements[i] = collection.Elements[i];
                }
            }

            TCollection(const TCollection& collection, T* name, G* counters)
                : Elements(new TElement[collection.Size + 1])
                , Size(collection.Size + 1)
            {
                for (size_t i = 0; i < Size - 1; ++i) {
                    Elements[i] = collection.Elements[i];
                }
                Elements[Size - 1].Name = name;
                Elements[Size - 1].Counters = counters;
                for (size_t i = 1; i < Size; ++i) {
                    size_t j = i;
                    while (j > 0 &&
                           TElement::Compare(Elements[j], Elements[j - 1])) {
                        std::swap(Elements[j], Elements[j - 1]);
                        --j;
                    }
                }
            }

            G* Find(const T& name) const {
                G* result = nullptr;
                if (Size == 0) {
                    return nullptr;
                }
                size_t l = 0;
                size_t r = Size - 1;
                while (l < r) {
                    size_t m = (l + r) / 2;
                    if (Less(*(Elements[m].Name), name)) {
                        l = m + 1;
                    } else {
                        r = m;
                    }
                }
                if (!Less(*(Elements[l].Name), name) && !Less(name, *(Elements[l].Name))) {
                    result = Elements[l].Counters;
                }
                return result;
            }

            void Free() {
                for (size_t i = 0; i < Size; ++i) {
                    T* name = Elements[i].Name;
                    G* counters = Elements[i].Counters;
                    Elements[i].Name = nullptr;
                    Elements[i].Counters = nullptr;
                    delete name;
                    delete counters;
                }
                Size = 0;
            }

            TGroupsNamesPtr GetNames() const {
                TGroupsNamesPtr result(new TGroupsNames());
                for (size_t i = 0; i < Size; ++i) {
                    result->push_back(*(Elements[i].Name));
                }
                return result;
            }
        }; // TCollection
        struct TOldGroup {
            TCollection* Collection;
            ui64 Time;
        };

    private:
        TCollection* Groups;
        TList<TOldGroup> OldGroups;
        TSpinLock AddMutex;

        ui64 Timeout;

        static TL Less;

    private:
        G* Add(const T& name) {
            TGuard<TSpinLock> guard(AddMutex);
            G* result = Groups->Find(name);
            if (result == nullptr) {
                T* newName = new T(name);
                G* newCounters = new G();
                TCollection* newGroups =
                    new TCollection(*Groups, newName, newCounters);
                ui64 now = ::Now().MicroSeconds();
                TOldGroup group;
                group.Collection = Groups;
                group.Time = now;
                OldGroups.push_back(group);
                for (ui32 i = 0; i < 5; ++i) {
                    if (OldGroups.front().Time + Timeout < now) {
                        delete OldGroups.front().Collection;
                        OldGroups.front().Collection = nullptr;
                        OldGroups.pop_front();
                    } else {
                        break;
                    }
                }
                Groups = newGroups;
                result = Groups->Find(name);
            }
            return result;
        }

    public:
        TDeprecatedCounterGroups(ui64 timeout = 5 * 1000000L) {
            Groups = new TCollection();
            Timeout = timeout;
        }

        virtual ~TDeprecatedCounterGroups() {
            TGuard<TSpinLock> guard(AddMutex);
            Groups->Free();
            delete Groups;
            Groups = nullptr;
            typename TList<TOldGroup>::iterator i;
            for (i = OldGroups.begin(); i != OldGroups.end(); ++i) {
                delete i->Collection;
                i->Collection = nullptr;
            }
            OldGroups.clear();
        }

        bool Has(const T& name) const {
            TCollection* groups = Groups;
            return groups->Find(name) != nullptr;
        }

        G* Find(const T& name) const {
            TCollection* groups = Groups;
            return groups->Find(name);
        }

        // Get group with the name, if it exists.
        // If there is no group with the name, add new group.
        G& Get(const T& name) {
            G* result = Find(name);
            if (result == nullptr) {
                result = Add(name);
                Y_ASSERT(result != nullptr);
            }
            return *result;
        }

        // Get copy of groups names array.
        TGroupsNamesPtr GetGroupsNames() const {
            TCollection* groups = Groups;
            TGroupsNamesPtr result = groups->GetNames();
            return result;
        }
    }; // TDeprecatedCounterGroups

    template <typename T, typename G, typename TL>
    TL TDeprecatedCounterGroups<T, G, TL>::Less;
}

static inline IOutputStream& operator<<(IOutputStream& o, const NMonitoring::TDeprecatedCounter& rhs) {
    return o << rhs.Val();
}

template <size_t N>
static inline IOutputStream& operator<<(IOutputStream& o, const std::array<NMonitoring::TDeprecatedCounter, N>& rhs) {
    for (typename std::array<NMonitoring::TDeprecatedCounter, N>::const_iterator it = rhs.begin(); it != rhs.end(); ++it) {
        if (!!*it)
            o << *it << Endl;
    }
    return o;
}
