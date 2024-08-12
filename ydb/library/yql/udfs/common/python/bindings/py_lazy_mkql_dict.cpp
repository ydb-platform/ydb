#include "py_cast.h"
#include "py_errors.h"
#include "py_gil.h"
#include "py_utils.h"

#include <ydb/library/yql/public/udf/udf_value.h>
#include <ydb/library/yql/public/udf/udf_value_builder.h>
#include <ydb/library/yql/public/udf/udf_type_inspection.h>
#include <ydb/library/yql/public/udf/udf_terminator.h>

#include <util/generic/maybe.h>
#include <util/string/builder.h>

using namespace NKikimr;

namespace NPython {
namespace {
//////////////////////////////////////////////////////////////////////////////
// TLazyDictBase
//////////////////////////////////////////////////////////////////////////////
class TLazyDictBase: public NUdf::TBoxedValue
{
protected:
    class TIterator: public NUdf::TBoxedValue {
    public:
        TIterator(const TPyCastContext::TPtr& ctx, const NUdf::TType* type, TPyObjectPtr&& pyIter)
            : CastCtx_(ctx), ItemType_(type), PyIter_(std::move(pyIter))
        {}

        ~TIterator() {
            const TPyGilLocker lock;
            PyIter_.Reset();
        }

    private:
        bool Skip() override try {
            const TPyGilLocker lock;
            const TPyObjectPtr next(PyIter_Next(PyIter_.Get()));
            if (next) {
                return true;
            }

            if (PyErr_Occurred()) {
                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
            }

            return false;
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }

        bool Next(NUdf::TUnboxedValue& value) override try {
            const TPyGilLocker lock;
            const TPyObjectPtr next(PyIter_Next(PyIter_.Get()));
            if (next) {
                value = FromPyObject(CastCtx_, ItemType_, next.Get());
                return true;
            }

            if (PyErr_Occurred()) {
                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
            }

            return false;
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& payload) override {
            payload = NUdf::TUnboxedValuePod::Void();
            return Next(key);
        }

    private:
        const TPyCastContext::TPtr CastCtx_;
        const NUdf::TType* ItemType_;
        TPyObjectPtr PyIter_;
    };

    class TPairIterator: public NUdf::TBoxedValue {
    public:
        TPairIterator(const TPyCastContext::TPtr& ctx, const NUdf::TType* keyType, const NUdf::TType* payType, TPyObjectPtr&& pyIter)
            : CastCtx_(ctx),  KeyType_(keyType), PayType_(payType), PyIter_(std::move(pyIter))
        {}

        ~TPairIterator() {
            const TPyGilLocker lock;
            PyIter_.Reset();
        }

    private:
        bool Skip() override try {
            const TPyGilLocker lock;
            const TPyObjectPtr next(PyIter_Next(PyIter_.Get()));
            if (next) {
                return true;
            }

            if (PyErr_Occurred()) {
                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
            }

            return false;
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& pay) override try {
            const TPyGilLocker lock;
            const TPyObjectPtr next(PyIter_Next(PyIter_.Get()));
            if (next) {
                key = FromPyObject(CastCtx_, KeyType_, PyTuple_GET_ITEM(next.Get(), 0));
                pay = FromPyObject(CastCtx_, PayType_, PyTuple_GET_ITEM(next.Get(), 1));
                return true;
            }

            if (PyErr_Occurred()) {
                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
            }

            return false;
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }

    private:
        const TPyCastContext::TPtr CastCtx_;
        const NUdf::TType* KeyType_;
        const NUdf::TType* PayType_;
        TPyObjectPtr PyIter_;
    };

    TLazyDictBase(const TPyCastContext::TPtr& castCtx, const NUdf::TType* itemType, PyObject* pyObject)
        : CastCtx_(castCtx), ItemType_(itemType), PyObject_(pyObject, TPyObjectPtr::AddRef())
    {}

    ~TLazyDictBase() {
        const TPyGilLocker lock;
        PyObject_.Reset();
    }

    bool HasDictItems() const override try {
        const TPyGilLocker lock;
        const auto has = PyObject_IsTrue(PyObject_.Get());
        if (has < 0) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return bool(has);
    }
    catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    const TPyCastContext::TPtr CastCtx_;
    const NUdf::TType* ItemType_;
    TPyObjectPtr PyObject_;
};

//////////////////////////////////////////////////////////////////////////////
// TLazyMapping
//////////////////////////////////////////////////////////////////////////////
class TLazyMapping: public TLazyDictBase
{
public:
    TLazyMapping(const TPyCastContext::TPtr& ctx, const NUdf::TType* keyType, const NUdf::TType* payType, PyObject* dict)
        : TLazyDictBase(ctx, keyType, dict), PayType_(payType)
    {}

private:
    bool IsSortedDict() const override { return false; }

    ui64 GetDictLength() const override try {
        const TPyGilLocker lock;
        const auto len = PyMapping_Size(PyObject_.Get());
        if (len < 0) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return ui64(len);
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetKeysIterator() const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyList = PyMapping_Keys(PyObject_.Get())) {
            if (TPyObjectPtr pyIter = PyObject_GetIter(pyList.Get())) {
                return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, ItemType_, std::move(pyIter)));
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyList = PyMapping_Values(PyObject_.Get())) {
            if (TPyObjectPtr pyIter = PyObject_GetIter(pyList.Get())) {
                return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, PayType_, std::move(pyIter)));
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetDictIterator() const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyList = PyMapping_Items(PyObject_.Get())) {
            if (TPyObjectPtr pyIter = PyObject_GetIter(pyList.Get())) {
                return NUdf::TUnboxedValuePod(new TPairIterator(CastCtx_, ItemType_, PayType_, std::move(pyIter)));
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyKey = ToPyObject(CastCtx_, ItemType_, key)) {
            if (const auto item = PyObject_GetItem(PyObject_.Get(), pyKey.Get())) {
                return FromPyObject(CastCtx_, PayType_, item).Release().MakeOptional();
            }

            if (PyErr_Occurred()) {
                PyErr_Clear();
            }

            return NUdf::TUnboxedValue();
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyKey = ToPyObject(CastCtx_, ItemType_, key)) {
            const auto map = PyObject_.Get();
            const auto has = map->ob_type->tp_as_sequence && map->ob_type->tp_as_sequence->sq_contains ?
                (map->ob_type->tp_as_sequence->sq_contains)(map, pyKey.Get()) :
                PyMapping_HasKey(map, pyKey.Get());

            if (has >= 0) {
                return bool(has);
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

private:
    const NUdf::TType* PayType_;
};

//////////////////////////////////////////////////////////////////////////////
// TLazyDict
//////////////////////////////////////////////////////////////////////////////
class TLazyDict: public TLazyDictBase
{
public:
    TLazyDict(const TPyCastContext::TPtr& ctx, const NUdf::TType* keyType, const NUdf::TType* payType, PyObject* dict)
        : TLazyDictBase(ctx, keyType, dict), PayType_(payType)
    {}

private:
    bool IsSortedDict() const override { return false; }

    ui64 GetDictLength() const override try {
        const TPyGilLocker lock;
        const auto len = PyDict_Size(PyObject_.Get());
        if (len < 0) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return ui64(len);
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetKeysIterator() const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyList = PyDict_Keys(PyObject_.Get())) {
            if (TPyObjectPtr pyIter = PyObject_GetIter(pyList.Get())) {
                return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, ItemType_, std::move(pyIter)));
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyList = PyDict_Values(PyObject_.Get())) {
            if (TPyObjectPtr pyIter = PyObject_GetIter(pyList.Get())) {
                return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, PayType_, std::move(pyIter)));
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetDictIterator() const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyList = PyDict_Items(PyObject_.Get())) {
            if (TPyObjectPtr pyIter = PyObject_GetIter(pyList.Get())) {
                return NUdf::TUnboxedValuePod(new TPairIterator(CastCtx_, ItemType_, PayType_, std::move(pyIter)));
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyKey = ToPyObject(CastCtx_, ItemType_, key)) {
            if (const auto item = PyDict_GetItem(PyObject_.Get(), pyKey.Get())) {
                return FromPyObject(CastCtx_, PayType_, item).Release().MakeOptional();
            } else if (!PyErr_Occurred()) {
                return NUdf::TUnboxedValue();
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyKey = ToPyObject(CastCtx_, ItemType_, key)) {
            const auto has = PyDict_Contains(PyObject_.Get(), pyKey.Get());
            if (has >= 0) {
                return bool(has);
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

private:
    const NUdf::TType* PayType_;
};

//////////////////////////////////////////////////////////////////////////////
// TLazySet
//////////////////////////////////////////////////////////////////////////////
class TLazySet: public TLazyDictBase
{
public:
    TLazySet(const TPyCastContext::TPtr& ctx, const NUdf::TType* itemType, PyObject* set)
        : TLazyDictBase(ctx, itemType, set)
    {}

private:
    bool IsSortedDict() const override { return false; }

    ui64 GetDictLength() const override try {
        const TPyGilLocker lock;
        const auto len = PySet_Size(PyObject_.Get());
        if (len < 0) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return ui64(len);
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        return Contains(key) ? NUdf::TUnboxedValuePod::Void() : NUdf::TUnboxedValuePod();
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyKey = ToPyObject(CastCtx_, ItemType_, key)) {
            const auto has = PySet_Contains(PyObject_.Get(), pyKey.Get());
            if (has >= 0) {
                return bool(has);
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetKeysIterator() const override try {
        const TPyGilLocker lock;
        if (TPyObjectPtr pyIter = PyObject_GetIter(PyObject_.Get())) {
            return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, ItemType_, std::move(pyIter)));
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return GetKeysIterator();
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return GetKeysIterator();
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return GetKeysIterator();
    }

    ui64 GetListLength() const override {
        return GetDictLength();
    }

    bool HasListItems() const override {
        return HasDictItems();
    }

    bool HasFastListLength() const override {
        return true;
    }
};

//////////////////////////////////////////////////////////////////////////////
// TLazySequenceAsSet
//////////////////////////////////////////////////////////////////////////////
class TLazySequenceAsSet: public TLazyDictBase
{
public:
    TLazySequenceAsSet(const TPyCastContext::TPtr& ctx, const NUdf::TType* keyType, PyObject* sequence)
        : TLazyDictBase(ctx, keyType, sequence)
    {}

private:
    bool IsSortedDict() const override { return false; }

    ui64 GetDictLength() const override try {
        const TPyGilLocker lock;
        const auto len = PySequence_Size(PyObject_.Get());
        if (len < 0) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
        }
        return ui64(len);
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        return Contains(key) ? NUdf::TUnboxedValuePod::Void() : NUdf::TUnboxedValuePod();
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override try {
        const TPyGilLocker lock;
        if (const TPyObjectPtr pyKey = ToPyObject(CastCtx_, ItemType_, key)) {
            const auto has = PySequence_Contains(PyObject_.Get(), pyKey.Get());
            if (has >= 0) {
                return bool(has);
            }
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetKeysIterator() const override try {
        const TPyGilLocker lock;
        if (TPyObjectPtr pyIter = PyObject_GetIter(PyObject_.Get())) {
            return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, ItemType_, std::move(pyIter)));
        }
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return GetKeysIterator();
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return GetKeysIterator();
    }

    NUdf::TUnboxedValue GetListIterator() const override {
        return GetKeysIterator();
    }

    ui64 GetListLength() const override {
        return GetDictLength();
    }

    bool HasListItems() const override {
        return HasDictItems();
    }

    bool HasFastListLength() const override {
        return true;
    }
};

//////////////////////////////////////////////////////////////////////////////
// TLazySequenceAsDict
//////////////////////////////////////////////////////////////////////////////
template<typename KeyType>
class TLazySequenceAsDict: public NUdf::TBoxedValue
{
private:
    class TKeyIterator: public NUdf::TBoxedValue {
    public:
        TKeyIterator(Py_ssize_t size)
            : Size(size), Index(0)
        {}

    private:
        bool Skip() override {
            if (Index >= Size)
                return false;

            ++Index;
            return true;
        }

        bool Next(NUdf::TUnboxedValue& value) override {
            if (Index >= Size)
                return false;

            value = NUdf::TUnboxedValuePod(KeyType(Index++));
            return true;
        }

    private:
        const Py_ssize_t Size;
        Py_ssize_t Index;
    };

    class TIterator: public NUdf::TBoxedValue {
    public:
        TIterator(const TPyCastContext::TPtr& ctx, const NUdf::TType* itemType, Py_ssize_t size, const TPyObjectPtr& pySeq)
            : CastCtx_(ctx),  ItemType_(itemType), PySeq_(pySeq), Size(size), Index(0)
        {}

        ~TIterator() {
            const TPyGilLocker lock;
            PySeq_.Reset();
        }

    private:
        bool Skip() override {
            if (Index >= Size)
                return false;

            ++Index;
            return true;
        }

        bool Next(NUdf::TUnboxedValue& value) override try {
            if (Index >= Size)
                return false;

            const TPyGilLocker lock;
            value = FromPyObject(CastCtx_, ItemType_, PySequence_Fast_GET_ITEM(PySeq_.Get(), Index++));
            return true;
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }

        bool NextPair(NUdf::TUnboxedValue& key, NUdf::TUnboxedValue& pay) override try {
            if (Index >= Size)
                return false;

            const TPyGilLocker lock;
            key = NUdf::TUnboxedValuePod(KeyType(Index));
            pay = FromPyObject(CastCtx_, ItemType_, PySequence_Fast_GET_ITEM(PySeq_.Get(), Index++));
            return true;
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }

    private:
        const TPyCastContext::TPtr CastCtx_;
        const NUdf::TType* ItemType_;
        TPyObjectPtr PySeq_;
        const Py_ssize_t Size;
        Py_ssize_t Index;
    };

public:
    TLazySequenceAsDict(const TPyCastContext::TPtr& ctx, const NUdf::TType* itemType, TPyObjectPtr&& sequence, Py_ssize_t size)
        : CastCtx_(ctx), ItemType_(itemType), Size(size), PySeq_(std::move(sequence))
    {}

    ~TLazySequenceAsDict()
    {
        const TPyGilLocker lock;
        PySeq_.Reset();
    }

private:
    bool IsSortedDict() const override { return true; }

    bool HasDictItems() const override {
        return Size > 0;
    }

    ui64 GetDictLength() const override {
        return Size;
    }

    NUdf::TUnboxedValue Lookup(const NUdf::TUnboxedValuePod& key) const override {
        const Py_ssize_t index = key.Get<KeyType>();
        if (index >= -Size && index < Size) try {
            const TPyGilLocker lock;
            if (const auto item = PySequence_Fast_GET_ITEM(PySeq_.Get(), index >= 0 ? index : Size + index)) {
                return FromPyObject(CastCtx_, ItemType_, item).Release().MakeOptional();
            } else if (PyErr_Occurred()) {
                UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << GetLastErrorAsString()).data());
            }
        } catch (const yexception& e) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
        }
        return NUdf::TUnboxedValue();
    }

    bool Contains(const NUdf::TUnboxedValuePod& key) const override {
        const Py_ssize_t index = key.Get<KeyType>();
        return index >= -Size && index < Size;
    }

    NUdf::TUnboxedValue GetKeysIterator() const override {
        return NUdf::TUnboxedValuePod(new TKeyIterator(Size));
    }

    NUdf::TUnboxedValue GetPayloadsIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, ItemType_, Size, PySeq_));
    }

    NUdf::TUnboxedValue GetDictIterator() const override {
        return NUdf::TUnboxedValuePod(new TIterator(CastCtx_, ItemType_, Size, PySeq_));
    }

    const TPyCastContext::TPtr CastCtx_;
    const NUdf::TType* ItemType_;
    const Py_ssize_t Size;
    TPyObjectPtr PySeq_;
};

} // namspace

NUdf::TUnboxedValue FromPyDict(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* keyType,
        const NUdf::TType* payType,
        PyObject* dict)
{
    return NUdf::TUnboxedValuePod(new TLazyDict(castCtx, keyType, payType, dict));
}

NUdf::TUnboxedValue FromPyMapping(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* keyType,
        const NUdf::TType* payType,
        PyObject* map)
{
    return NUdf::TUnboxedValuePod(new TLazyMapping(castCtx, keyType, payType, map));
}

NUdf::TUnboxedValue FromPySet(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        PyObject* set)
{
    return NUdf::TUnboxedValuePod(new TLazySet(castCtx, itemType, set));
}

NUdf::TUnboxedValue FromPySequence(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* keyType,
        PyObject* set)
{
    return NUdf::TUnboxedValuePod(new TLazySequenceAsSet(castCtx, keyType, set));
}

NUdf::TUnboxedValue FromPySequence(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* itemType,
        const NUdf::TDataTypeId keyType,
        PyObject* sequence)
{
    if (TPyObjectPtr fast = PySequence_Fast(sequence, "Can't get fast sequence.")) {
    const auto size = PySequence_Fast_GET_SIZE(fast.Get());
    if (size >= 0) {
        switch (keyType) {
#define MAKE_PRIMITIVE_TYPE_SIZE(type) \
            case NUdf::TDataType<type>::Id: \
                return NUdf::TUnboxedValuePod(new TLazySequenceAsDict<type>(castCtx, itemType, std::move(fast), size));
            INTEGRAL_VALUE_TYPES(MAKE_PRIMITIVE_TYPE_SIZE)
#undef MAKE_PRIMITIVE_TYPE_SIZE
        }
        Y_ABORT("Invalid key type.");
    }
    }
    UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << GetLastErrorAsString()).data());
}

} // namespace NPython
