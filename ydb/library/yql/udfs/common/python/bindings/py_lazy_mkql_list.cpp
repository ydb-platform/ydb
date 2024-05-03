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

static ui64 CalculateIteratorLength(PyObject* iter, const TPyCastContext::TPtr& castCtx)
{
    PyObject* item;

    ui64 length = 0;
    while ((item = PyIter_Next(iter))) {
        length++;
        Py_DECREF(item);
    }

    if (PyErr_Occurred()) {
        UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << GetLastErrorAsString()).data());
    }

    return length;
}

static bool IsIteratorHasItems(PyObject* iter, const TPyCastContext::TPtr& castCtx)
{
    if (const TPyObjectPtr item = PyIter_Next(iter)) {
        return true;
    }

    if (PyErr_Occurred()) {
        UdfTerminate((TStringBuilder() << castCtx->PyCtx->Pos << GetLastErrorAsString()).data());
    }

    return false;
}

//////////////////////////////////////////////////////////////////////////////
// TBaseLazyList
//////////////////////////////////////////////////////////////////////////////
template<typename TDerived>
class TBaseLazyList: public NUdf::TBoxedValue
{
    using TListSelf = TBaseLazyList<TDerived>;

    class TIterator: public NUdf::TBoxedValue {
    public:
        TIterator(const TPyCastContext::TPtr& ctx, const NUdf::TType* type, TPyObjectPtr&& pyIter)
            : CastCtx_(ctx)
            , PyIter_(std::move(pyIter))
            , ItemType_(type)
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

    private:
        const TPyCastContext::TPtr CastCtx_;
        TPyObjectPtr PyIter_;
        const NUdf::TType* ItemType_;
    };

public:
    TBaseLazyList(
            const TPyCastContext::TPtr& castCtx,
            TPyObjectPtr&& pyObject,
            const NUdf::TType* type)
        : CastCtx_(castCtx)
        , PyObject_(std::move(pyObject))
        , ItemType_(NUdf::TListTypeInspector(*CastCtx_->PyCtx->TypeInfoHelper, type).GetItemType())
    {
    }

    ~TBaseLazyList() {
        TPyGilLocker lock;
        PyObject_.Reset();
    }

private:
    TPyObjectPtr GetIterator() const try {
        return static_cast<const TDerived*>(this)->GetIteratorImpl();
    }
    catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    bool HasFastListLength() const override {
        return Length_.Defined();
    }

    ui64 GetEstimatedListLength() const override {
        return GetListLength();
    }

    ui64 GetListLength() const override try {
        if (!Length_.Defined()) {
            const TPyGilLocker lock;
            TPyObjectPtr iter = GetIterator();
            Length_ = CalculateIteratorLength(iter.Get(), CastCtx_);
        }

        return *Length_;
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    bool HasListItems() const override try {
        if (Length_.Defined())
            return *Length_ > 0;

        const TPyGilLocker lock;
        TPyObjectPtr iter = GetIterator();
        const bool hasItems = IsIteratorHasItems(iter.Get(), CastCtx_);
        if (!hasItems) {
            Length_ = 0;
        }
        return hasItems;
    }
    catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    NUdf::TUnboxedValue GetListIterator() const override try {
        const TPyGilLocker lock;
        TPyObjectPtr pyIter = GetIterator();
        auto* self = const_cast<TListSelf*>(this);
        return NUdf::TUnboxedValuePod(new TIterator(self->CastCtx_, self->ItemType_, std::move(pyIter)));
    } catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    const NUdf::TOpaqueListRepresentation* GetListRepresentation() const override {
        return nullptr;
    }

    NUdf::IBoxedValuePtr ReverseListImpl(
            const NUdf::IValueBuilder& builder) const override
    {
        Y_UNUSED(builder);
        return nullptr;
    }

    NUdf::IBoxedValuePtr SkipListImpl(
            const NUdf::IValueBuilder& builder, ui64 count) const override
    {
        Y_UNUSED(builder);
        Y_UNUSED(count);
        return nullptr;
    }

    NUdf::IBoxedValuePtr TakeListImpl(
            const NUdf::IValueBuilder& builder, ui64 count) const override
    {
        Y_UNUSED(builder);
        Y_UNUSED(count);
        return nullptr;
    }

    NUdf::IBoxedValuePtr ToIndexDictImpl(
            const NUdf::IValueBuilder& builder) const override
    {
        Y_UNUSED(builder);
        return nullptr;
    }

protected:
    const TPyCastContext::TPtr CastCtx_;
    TPyObjectPtr PyObject_;
    const NUdf::TType* ItemType_;
    mutable TMaybe<ui64> Length_;
};

//////////////////////////////////////////////////////////////////////////////
// TLazyIterable
//////////////////////////////////////////////////////////////////////////////
class TLazyIterable: public TBaseLazyList<TLazyIterable>
{
    using TBase = TBaseLazyList<TLazyIterable>;
public:
    TLazyIterable(
            const TPyCastContext::TPtr& castCtx,
            TPyObjectPtr&& pyObject,
            const NUdf::TType* type)
        : TBase(castCtx, std::move(pyObject), type)
    {}

    TPyObjectPtr GetIteratorImpl() const {
        if (const  TPyObjectPtr ret = PyObject_GetIter(PyObject_.Get())) {
            return ret;
        }

        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos
            << "Cannot get iterator from object: "
            << PyObjectRepr(PyObject_.Get()) << ", error: "
            << GetLastErrorAsString()).data());
    }

private:
    bool HasFastListLength() const override {
        return Length_.Defined();
    }

    ui64 GetListLength() const override try {
        if (!Length_.Defined()) {
            const TPyGilLocker lock;
            const auto len = PyObject_Size(PyObject_.Get());
            if (len >= 0) {
                Length_ = len;
            } else {
                Length_ = CalculateIteratorLength(GetIteratorImpl().Get(), CastCtx_);
            }
        }
        return *Length_;
    }
    catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }

    bool HasListItems() const override try {
        const TPyGilLocker lock;
        bool hasItems = false;
        const auto isTrue = PyObject_IsTrue(PyObject_.Get());
        if (isTrue != -1) {
            hasItems = static_cast<bool>(isTrue);
        } else {
            TPyObjectPtr iter = GetIteratorImpl();
            hasItems = IsIteratorHasItems(iter.Get(), CastCtx_);
        }
        if (!hasItems) {
            Length_ = 0;
        }
        return hasItems;
    }
    catch (const yexception& e) {
        UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << e.what()).data());
    }
};

//////////////////////////////////////////////////////////////////////////////
// TLazyIterator
//////////////////////////////////////////////////////////////////////////////
class TLazyIterator: public TBaseLazyList<TLazyIterator>
{
    using TBase = TBaseLazyList<TLazyIterator>;
public:
    TLazyIterator(
            const TPyCastContext::TPtr& castCtx,
            TPyObjectPtr&& pyObject,
            const NUdf::TType* type)
        : TBase(castCtx, std::move(pyObject), type)
        , IteratorDrained_(false)
    {}

    TPyObjectPtr GetIteratorImpl() const {
        if (IteratorDrained_) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos <<
                "Lazy list was build under python iterator. "
                "Iterator was already used.").data());
        }
        IteratorDrained_ = true;
        return PyObject_;
    }

private:
    mutable bool IteratorDrained_;
};

//////////////////////////////////////////////////////////////////////////////
// TLazyGenerator
//////////////////////////////////////////////////////////////////////////////
class TLazyGenerator: public TBaseLazyList<TLazyGenerator>
{
    using TBase = TBaseLazyList<TLazyGenerator>;
public:
    TLazyGenerator(
            const TPyCastContext::TPtr& castCtx,
            TPyObjectPtr&& pyObject,
            const NUdf::TType* type)
        : TBase(castCtx, std::move(pyObject), type)
    {
        // keep ownership of function closure if any
        if (PyFunction_Check(PyObject_.Get())) {
            PyObject* closure = PyFunction_GetClosure(PyObject_.Get());
            if (closure) {
                Closure_ = TPyObjectPtr(closure, TPyObjectPtr::ADD_REF);
            }
        }
    }

    ~TLazyGenerator() {
        const TPyGilLocker lock;
        Closure_.Reset();
    }

    TPyObjectPtr GetIteratorImpl() const {
        TPyObjectPtr generator = PyObject_CallObject(PyObject_.Get(), nullptr);
        if (!generator || !PyGen_Check(generator.Get())) {
            UdfTerminate((TStringBuilder() << CastCtx_->PyCtx->Pos << "Expected generator as a result of function call").data());
        }
        return PyObject_GetIter(generator.Get());
    }

private:
    TPyObjectPtr Closure_;
};

} // namspace


NUdf::TUnboxedValue FromPyLazyGenerator(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        TPyObjectPtr callableObj)
{
    return NUdf::TUnboxedValuePod(new TLazyGenerator(castCtx, std::move(callableObj), type));
}

NUdf::TUnboxedValue FromPyLazyIterable(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        TPyObjectPtr iterableObj)
{
    return NUdf::TUnboxedValuePod(new TLazyIterable(castCtx, std::move(iterableObj), type));
}

NUdf::TUnboxedValue FromPyLazyIterator(
        const TPyCastContext::TPtr& castCtx,
        const NUdf::TType* type,
        TPyObjectPtr iteratorObj)
{
    return NUdf::TUnboxedValuePod(new TLazyIterator(castCtx, std::move(iteratorObj), type));
}

} // namespace NPython
