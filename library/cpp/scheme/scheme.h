#pragma once

#include "scimpl_defs.h"

#include "fwd.h"

#include <util/generic/maybe.h>

#include <iterator>
#include <utility>

namespace NSc {
#ifdef _MSC_VER
#pragma warning(disable : 4521 4522)
#endif

    struct TMergeOptions {
        enum class EArrayMergeMode {
            Replace,
            Merge
        };
        EArrayMergeMode ArrayMergeMode = EArrayMergeMode::Replace;
    };

    // todo: try to remove some rarely used methods
    class TValue {
    public:
        enum class EType {
            Null = 0 /* "Null" */,
            Bool /* "Bool" */,
            IntNumber /* "Int" */,
            FloatNumber /* "Float" */,
            String /* "String" */,
            Array /* "Array" */,
            Dict /* "Dict" */
        };

        struct TScCore;
        using TCorePtr = TIntrusivePtr<TScCore>;
        using TPoolPtr = TIntrusivePtr<NDefinitions::TPool>;

        using TArray = ::NSc::TArray;
        using TDict = ::NSc::TDict;

    private:                      // A TValue instance has only these 3 fields
        mutable TCorePtr TheCore; // a pointer to a refcounted (kind of) variant
        bool CopyOnWrite = false; // a flag that thevalue is a COW shallow copy and should produce a deep copy once modified

        // Thus all copies of a TValue are by default shallow. Use TValue::Clone to force a deep copy.
        // A COW copy will see changes in its parent, but no change in the COW copy will propagate to its parent.

    public:
        // XXX: A newly constructed standalone TValue instance (even null!) consumes ~4 KB of memory because it allocates a memory pool inside.
        // Consider building a tree of TValues in top-down order (from root to leaves) to share a single pool
        // instead of creating child TValues first and appending them to a parent TValue.
        // All somehow cached values should be constant, otherwise the shared pool can grow infinitely.

        inline TValue();
        inline TValue(TValue& v);
        inline TValue(const TValue& v);
        inline TValue(TValue&& v) noexcept;

    public: // Operators
        inline TValue(double t);
        inline TValue(unsigned long long t);
        inline TValue(unsigned long t);
        inline TValue(unsigned t);
        inline TValue(long long t);
        inline TValue(long t);
        inline TValue(int t);
        //    inline TValue(bool b);

        inline TValue(TStringBuf t);
        inline TValue(const char*);

        inline operator double() const;
        inline operator float() const;
        inline operator long long() const;
        inline operator long() const;
        inline operator int() const;
        inline operator short() const;
        inline operator char() const;
        inline operator unsigned long long() const;
        inline operator unsigned long() const;
        inline operator unsigned() const;
        inline operator unsigned short() const;
        inline operator unsigned char() const;
        inline operator signed char() const;

        inline operator TStringBuf() const;

        inline operator const ::NSc::TArray&() const;
        inline operator const ::NSc::TDict&() const;

        inline TValue& operator=(double t);

        inline TValue& operator=(unsigned long long t);
        inline TValue& operator=(unsigned long t);
        inline TValue& operator=(unsigned t);

        inline TValue& operator=(long long t);
        inline TValue& operator=(long t);
        inline TValue& operator=(int t);
        //    inline TValue& operator=(bool t);

        inline TValue& operator=(TStringBuf t);
        inline TValue& operator=(const char* t);

        inline TValue& operator=(TValue& v) &;
        inline TValue& operator=(const TValue& v) &;
        inline TValue& operator=(TValue&& v) & noexcept;

        inline TValue& operator=(TValue& v) && = delete;
        inline TValue& operator=(const TValue& v) && = delete;
        inline TValue& operator=(TValue&& v) && = delete;

    public:
        template <class T> // ui16 or TStringBuf
        inline TValue& operator[](const T& idx) {
            return GetOrAdd(idx);
        }

        template <class T> // ui16 or TStringBuf
        inline const TValue& operator[](const T& idx) const {
            return Get(idx);
        }

    public: // Data methods ///////////////////////////////////////////////////////////
        inline EType GetType() const;

        inline bool IsNull() const;

        inline TValue& SetNull(); // returns self, will set type to Null

        TValue& Clear() {
            return ClearArray().ClearDict().SetNull();
        }

    public: // Number methods /////////////////////////////////////////////////////////
            // Bool, IntNumber and FloatNumber are all compatible.
            // If a TValue node has one of the types it may as well be used as another.
        // FloatNumber methods. Forces FloatNumber representation. Compatible with IntNumber and Bool

        inline bool IsNumber() const; // true if any of FloatNumber, IntNumber, Bool

        inline double GetNumber(double defaultval = 0) const; // Compatible with Bool, IntNumber and FloatNumber types

        inline double& GetNumberMutable(double defaultval = 0); // Will switch the type to FloatNumber

        inline TValue& SetNumber(double val = 0); // returns self, will switch the type to FloatNumber

        double ForceNumber(double deflt = 0) const; // Best-effort cast to double (will do TryFromString if applicable)

        // IntNumber methods. Forces integer representation. Compatible with FloatNumber and Bool types.
        // Note: if you don't care about distinguishing bools, ints and doubles, use *Number methods above

        inline bool IsIntNumber() const; // true only if IntNumber or Bool

        inline i64 GetIntNumber(i64 defaultval = 0) const; // Compatible with Bool, IntNumber and FloatNumber types

        inline i64& GetIntNumberMutable(i64 defaultval = 0); // Will switch the type to IntNumber

        inline TValue& SetIntNumber(i64 val = 0); // returns self, will switch the type to IntNumber

        i64 ForceIntNumber(i64 deflt = 0) const; // Best-effort cast to i64 (will do TryFromString for String)

        // Bool methods. Forces bool representation. Compatible with Float Number and Int Number methods above.
        // Note: if you don't care about distinguishing Bool, IntNumber and FloatNumber, use *Number methods above

        inline bool IsBool() const; // true only if Bool

        inline bool GetBool(bool defaultval = false) const; // Compatible with Bool, IntNumber and FloatNumber types

        inline TValue& SetBool(bool val = false); // returns self, will switch the type to Bool

    public: // Arcadia-specific boolean representation support
        // Tests for explicit True, also checks for arcadia-specific boolean representation
        bool IsTrue() const {
            return IsNumber() ? GetNumber() : ::IsTrue(GetString());
        }

        // Tests for explicit False, also checks for arcadia-specific boolean representation
        bool IsExplicitFalse() const {
            return IsNumber() ? !GetNumber() : IsFalse(GetString());
        }

    public: // String methods /////////////////////////////////////////////////////////
        inline bool IsString() const;

        inline TStringBuf GetString(TStringBuf defaultval = TStringBuf()) const;

        inline TValue& SetString(TStringBuf val = TStringBuf()); // returns self

        TString ForceString(const TString& deflt = TString()) const; // Best-effort cast to TString (will do ToString for numeric types)

        // todo: remove
        inline bool StringEmpty() const;
        inline size_t StringSize() const;

    public: // Array methods //////////////////////////////////////////////////////////
        inline bool IsArray() const;

        inline const TArray& GetArray() const;
        inline TArray& GetArrayMutable();
        inline TValue& SetArray(); // turns into array if needed, returns self
        inline TValue& ClearArray();

        inline bool Has(size_t idx) const;

        inline const TValue& Get(size_t idx) const; // returns child or default
        inline TValue* GetNoAdd(size_t idx); // returns link to existing child or nullptr

        inline TValue& Push(); // returns new child

        template <class T>
        TValue& Push(T&& t) {
            return Push() = std::forward<T>(t);
        } // returns new child

        TValue& Insert(ui16 idx) {
            return InsertUnsafe(idx);
        } // creates missing values, returns new child

        template <class T>
        TValue& Insert(ui16 idx, T&& v) {
            return InsertUnsafe(idx, std::forward<T>(v));
        } // creates missing values, returns new child

        template <class TIt>
        inline TValue& AppendAll(TIt begin, TIt end); // Append(vec.begin(), vec.end())

        template <class TColl>
        inline TValue& AppendAll(TColl&& coll); // Append(vec)

        inline TValue& AppendAll(std::initializer_list<TValue> coll);

        TValue& GetOrAdd(ui16 idx) {
            return GetOrAddUnsafe(idx);
        } // creates missing values, returns new child

        inline TValue& InsertUnsafe(size_t idx); // creates missing values, returns new child

        template <class T>
        TValue& InsertUnsafe(size_t idx, T&& t) {
            return InsertUnsafe(idx) = std::forward<T>(t);
        } // creates missing values, returns new child

        inline TValue& GetOrAddUnsafe(size_t idx); // creates missing values, returns new child

        inline TValue Pop();              // returns popped value
        inline TValue Delete(size_t idx); // returns deleted value if it existed, NSc::Null() otherwise

        inline TValue& Front() {
            return GetOrAdd(0);
        } // creates missing value, returns child
        inline const TValue& Front() const {
            return Get(0);
        } // returns child or default

        inline TValue& Back();             // creates missing value, returns child
        inline const TValue& Back() const; // returns child or default

        // todo: remove
        inline bool ArrayEmpty() const;
        inline size_t ArraySize() const;

    public: // Dict methods
        inline bool IsDict() const;

        inline const TDict& GetDict() const;
        inline TDict& GetDictMutable();
        inline TValue& SetDict(); // turns into dict if not one, returns self
        inline TValue& ClearDict();

        inline bool Has(TStringBuf idx) const;

        inline const TValue& Get(TStringBuf idx) const;
        inline TValue* GetNoAdd(TStringBuf idx); // returns link to existing child or nullptr

        TValue& Add(TStringBuf idx) {
            return GetOrAdd(idx);
        }

        template <class T>
        TValue& Add(TStringBuf idx, T&& t) {
            return Add(idx) = std::forward<T>(t);
        }

        inline TValue& GetOrAdd(TStringBuf idx); // creates missing value, returns child

        inline TValue Delete(TStringBuf idx); // returns deleted value

        inline TValue& AddAll(std::initializer_list<std::pair<TStringBuf, TValue>> t);

        TStringBufs DictKeys(bool sorted = true) const;
        TStringBufs& DictKeys(TStringBufs&, bool sorted = true) const;

        // todo: remove
        inline bool DictEmpty() const;
        inline size_t DictSize() const;

    public: // Json methods ////////////////////////////////////////////////
        using TJsonOpts = NSc::TJsonOpts;
        using EJsonOpts = TJsonOpts::EJsonOpts;
        static const EJsonOpts JO_DEFAULT = TJsonOpts::JO_DEFAULT;
        static const EJsonOpts JO_SORT_KEYS = TJsonOpts::JO_SORT_KEYS;
        static const EJsonOpts JO_SKIP_UNSAFE = TJsonOpts::JO_SKIP_UNSAFE; // skip non-utf8 strings
        static const EJsonOpts JO_PRETTY = TJsonOpts::JO_PRETTY;
        static const EJsonOpts JO_SAFE = TJsonOpts::JO_SAFE;                                               // JO_SORT_KEYS | JO_SKIP_UNSAFE
        static const EJsonOpts JO_PARSER_STRICT_WITH_COMMENTS = TJsonOpts::JO_PARSER_STRICT_WITH_COMMENTS; // strict json + strict utf8
        static const EJsonOpts JO_PARSER_STRICT = TJsonOpts::JO_PARSER_STRICT;                             // strict json + strict utf8 + comments are disallowed
        static const EJsonOpts JO_PARSER_DISALLOW_DUPLICATE_KEYS = TJsonOpts::JO_PARSER_DISALLOW_DUPLICATE_KEYS;

        [[nodiscard]] static TValue FromJson(TStringBuf, const TJsonOpts& = TJsonOpts());
        [[nodiscard]] static TValue FromJsonThrow(TStringBuf, const TJsonOpts& = TJsonOpts());
        static bool FromJson(TValue&, TStringBuf, const TJsonOpts& = TJsonOpts());

        // TODO: Переименовать ToJson в ToJsonUnsafe, а ToJsonSafe в ToJson
        TString ToJson(const TJsonOpts& = TJsonOpts()) const;
        const TValue& ToJson(IOutputStream&, const TJsonOpts& = TJsonOpts()) const; // returns self

        // ToJson(JO_SORT_KEYS | JO_SKIP_UNSAFE)
        TString ToJsonSafe(const TJsonOpts& = TJsonOpts()) const;
        const TValue& ToJsonSafe(IOutputStream&, const TJsonOpts& = TJsonOpts()) const;

        // ToJson(JO_SORT_KEYS | JO_PRETTY | JO_SKIP_UNSAFE)
        TString ToJsonPretty(const TJsonOpts& = TJsonOpts()) const;
        const TValue& ToJsonPretty(IOutputStream&, const TJsonOpts& = TJsonOpts()) const;

        NJson::TJsonValue ToJsonValue() const;

        static TValue FromJsonValue(const NJson::TJsonValue&);
        static TValue& FromJsonValue(TValue&, const NJson::TJsonValue&); // returns self

        static TJsonOpts MakeOptsSafeForSerializer(TJsonOpts = TJsonOpts());
        static TJsonOpts MakeOptsPrettyForSerializer(TJsonOpts = TJsonOpts());

    public: // Merge methods ////////////////////////////////////////////////
        /*
     * LHS.MergeUpdate(RHS):
     *     1. Dict <- Dict:
     *            - Copy all nonconflicting key-value pairs from RHS to LHS.
     *            - For every pair of conflicting values apply LHS[key].MergeUpdate(RHS[key]).
     *     2. Anything <- Null:
     *            - Do nothing.
     *     3. Other conflicts:
     *            - Copy RHS over LHS.
     *
     * LHS.ReverseMerge(RHS):
     *     1. Dict <- Dict:
     *            - Copy all nonconflicting key-value pairs from RHS to LHS.
     *            - For every pair of conflicting values apply LHS[key].ReverseMerge(RHS[key]).
     *     2. Null <- Anything:
     *            - Copy RHS over LHS.
     *     3. Other conflicts:
     *            - Do nothing.
     */

        TValue& MergeUpdateJson(TStringBuf json, TMaybe<TMergeOptions> mergeOptions = {});  // returns self
        TValue& ReverseMergeJson(TStringBuf json, TMaybe<TMergeOptions> mergeOptions = {}); // returns self

        static bool MergeUpdateJson(TValue&, TStringBuf json, TMaybe<TMergeOptions> mergeOptions = {});  // returns true unless failed to parse the json
        static bool ReverseMergeJson(TValue&, TStringBuf json, TMaybe<TMergeOptions> mergeOptions = {}); // returns true unless failed to parse the json

        TValue& MergeUpdate(const TValue& delta, TMaybe<TMergeOptions> mergeOptions = {});  // return self
        TValue& ReverseMerge(const TValue& delta, TMaybe<TMergeOptions> mergeOptions = {}); // return self

    public: // Path methods /////////////////////////////////////////////////////////
        // TODO: add throwing variants
        // make sure to properly escape the tokens

        static TString EscapeForPath(TStringBuf rawKey); // converts a raw dict key into a valid token for a selector path

        static bool PathValid(TStringBuf path); // returns true if the path is syntactically valid

        bool PathExists(TStringBuf path) const; // returns true if the path is syntactically valid and the target value exists

        const TValue& TrySelect(TStringBuf path) const; // returns the target value
                                                        // if the path is syntactically valid and the target value exists
                                                        // otherwise returns NSc::Null()

        TValue* TrySelectOrAdd(TStringBuf path); // returns the target value if it exists or creates if not
                                                // if the path is syntactically valid
                                                // otherwise returns NSc::Null()

        TValue TrySelectAndDelete(TStringBuf path); // deletes and returns the target value
                                                    // if the path is syntactically valid and the target value existed
                                                    // otherwise returns NSc::Null()

    public:                                    // Copy methods /////////////////////////////////////////////////////////
        TValue Clone() const;                  // returns deep copy of self (on the separate pool)
        TValue& CopyFrom(const TValue& other); // deep copy other value into self, returns self

        TValue CreateNew() const;              // returns a new null value which shares a memory pool with self

        TValue& Swap(TValue& v);

        static bool Same(const TValue&, const TValue&);     // point to the same core
        static bool Equal(const TValue&, const TValue&);    // recursively equal
        static bool SamePool(const TValue&, const TValue&); // share arena

    public:
        // very specific methods useful in very specific corner cases

        static TValue From(const ::google::protobuf::Message&, bool mapAsDict = false);

        void To(::google::protobuf::Message&, const TProtoOpts& opts = {}) const;

    public:
        inline explicit TValue(TPoolPtr&);

        static const TScCore& DefaultCore();
        static const TArray& DefaultArray();
        static const TDict& DefaultDict();
        static const TValue& DefaultValue();
        static const TValue& Null() {
            return DefaultValue();
        }

        void DoWriteJsonImpl(IOutputStream&, const TJsonOpts&, NImpl::TKeySortContext&, NImpl::TSelfLoopContext&) const;

        bool IsSameOrAncestorOf(const TValue& other) const;

    private:
        TValue& DoMerge(const TValue& delta, bool olddelta, TMaybe<TMergeOptions> mergeOptions);
        TValue& DoMergeImpl(const TValue& delta, bool olddelta, TMaybe<TMergeOptions> mergeOptions, NImpl::TSelfLoopContext&, NImpl::TSelfOverrideContext&);
        TValue& DoCopyFromImpl(const TValue& other, NImpl::TSelfLoopContext&, NImpl::TSelfOverrideContext&);
        NJson::TJsonValue ToJsonValueImpl(NImpl::TSelfLoopContext&) const;

        bool IsSameOrAncestorOfImpl(const TScCore& other, NImpl::TSelfLoopContext& loopCtx) const;

        inline TScCore& CoreMutable();
        inline TScCore& CoreMutableForSet();
        inline const TScCore& Core() const;

        static inline TScCore* NewCore(TPoolPtr&);

        static TValue FromField(const ::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*);
        static TValue FromRepeatedField(const ::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*, int index);

        void ValueToField(const TValue& value, ::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*, const TProtoOpts& opts) const;
        void ToField(::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*, const TProtoOpts& opts) const;
        void ToEnumField(::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*, const TProtoOpts& opts) const;
        void ToRepeatedField(::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*, const TProtoOpts& opts) const;
        void ToMapField(::google::protobuf::Message&, const ::google::protobuf::FieldDescriptor*, const TProtoOpts& opts) const;
    };


    inline const TValue& Null() {
        return TValue::DefaultValue();
    }


    class TArray: public TDeque<TValue, TPoolAllocator>, TNonCopyable {
        using TParent = TDeque<TValue, TPoolAllocator>;

    public:
        TArray(TMemoryPool* p)
            : TParent(p)
        {
        }

        template <class TIt>
        void AppendAll(TIt begin, TIt end) {
            TParent::insert(TParent::end(), begin, end);
        }

        template <class TColl>
        void AppendAll(TColl&& coll) {
            AppendAll(std::begin(coll), std::end(coll));
        }

        void AppendAll(std::initializer_list<TValue> coll) {
            AppendAll(coll.begin(), coll.end());
        }

        const TValue& operator[](size_t i) const {
            return EnsureIndex(i);
        }

        TValue& operator[](size_t i) {
            return EnsureIndex(i);
        }

        const TValue& front() const {
            return EnsureIndex(0);
        }

        TValue& front() {
            return EnsureIndex(0);
        }

        const TValue& back() const {
            return EnsureIndex(LastIndex());
        }

        TValue& back() {
            return EnsureIndex(LastIndex());
        }

        void pop_back() {
            if (empty())
                return;
            TParent::pop_back();
        }

        void pop_front() {
            if (empty())
                return;
            TParent::pop_front();
        }

    private:
        size_t LastIndex() const {
            return ::Max<size_t>(size(), 1) - 1;
        }

        TValue& EnsureIndex(size_t i) {
            if (i >= size())
                resize(::Min<size_t>(i + 1, ::Max<ui16>()), TValue::DefaultValue());
            return TParent::operator[](i);
        }

        const TValue& EnsureIndex(size_t i) const {
            return i < size() ? TParent::operator[](i) : TValue::DefaultValue();
        }
    };


    // todo: densehashtable
    // todo: allow insertions
    // todo: make TDict methods safe
    class TDict: public THashMap<TStringBuf, TValue, THash<TStringBuf>, TEqualTo<TStringBuf>, TPoolAllocator>, TNonCopyable {
        using TParent = THashMap<TStringBuf, TValue, THash<TStringBuf>, TEqualTo<TStringBuf>, TPoolAllocator>;

    public:
        TDict(TMemoryPool* p)
            : TParent(p)
        {
        }

        template <class TStr>
        const TValue& Get(const TStr& key) const {
            const_iterator it = find(key);
            return it != end() ? it->second : TValue::DefaultValue();
        }
    };
}

#include "scimpl.h"
#include "scheme_cast.h"

#ifdef _MSC_VER
#pragma warning(default : 4521 4522)
#endif
