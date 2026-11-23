typedef interface IIterator IIterator;
typedef interface IIterable IIterable;

/* IIterator */
typedef struct IIteratorVtbl {
  BEGIN_INTERFACE

  /*** IUnknown methods ***/
  HRESULT (STDMETHODCALLTYPE *QueryInterface)(
    IIterator *This,
    REFIID riid,
    void **ppvObject);

  ULONG (STDMETHODCALLTYPE *AddRef)(
    IIterator *This);

  ULONG (STDMETHODCALLTYPE *Release)(
    IIterator *This);

  /*** IInspectable methods ***/
  HRESULT (STDMETHODCALLTYPE *GetIids)(
    IIterator *This,
    UINT32 *count,
    IID **ids);

  HRESULT (STDMETHODCALLTYPE *GetRuntimeClassName)(
    IIterator *This,
    HSTRING *className);

  HRESULT (STDMETHODCALLTYPE *GetTrustLevel)(
    IIterator *This,
    TrustLevel *trustLevel);

  /*** IIterator methods ***/
  HRESULT (STDMETHODCALLTYPE *get_Current)(
    IIterator *This,
    IUnknown **current);

  HRESULT (STDMETHODCALLTYPE *get_HasCurrent)(
    IIterator *This,
    CHAR *hasCurrent);

  HRESULT (STDMETHODCALLTYPE *MoveNext)(
    IIterator *This,
    CHAR *hasCurrent);

  HRESULT (STDMETHODCALLTYPE *GetMany)(
    IIterator *This,
    UINT capacity,
    void *value,
    UINT *actual);

  END_INTERFACE
} IIteratorVtbl;

interface IIterator {
  CONST_VTBL IIteratorVtbl* lpVtbl;
};

/*** IUnknown methods ***/
#define IIterator_QueryInterface(This,riid,ppvObject) (This)->lpVtbl->QueryInterface(This,riid,ppvObject)
#define IIterator_AddRef(This) (This)->lpVtbl->AddRef(This)
#define IIterator_Release(This) (This)->lpVtbl->Release(This)
/*** IInspectable methods ***/
#define IIterator_GetIids(This,count,ids) (This)->lpVtbl->GetIids(This,count,ids)
#define IIterator_GetRuntimeClassName(This,name) (This)->lpVtbl->GetRuntimeClassName(This,name)
#define IIterator_GetTrustLevel(This,level) (This)->lpVtbl->GetTrustLevel(This,level)
/*** IIterator methods ***/
#define IIterator_get_Current(This,current) (This)->lpVtbl->get_Current(This,current)
#define IIterator_get_HasCurrent(This,hasCurrent) (This)->lpVtbl->get_HasCurrent(This,hasCurrent)
#define IIterator_MoveNext(This,hasCurrent) (This)->lpVtbl->MoveNext(This,hasCurrent)
#define IIterator_GetMany(This,capacity,value,actual) (This)->lpVtbl->GetMany(This,capacity,value,actual)

/* IIterable */
typedef struct IIterableVtbl {
  BEGIN_INTERFACE

  /*** IUnknown methods ***/
  HRESULT (STDMETHODCALLTYPE *QueryInterface)(
    IIterable *This,
    REFIID riid,
    void **ppvObject);

  ULONG (STDMETHODCALLTYPE *AddRef)(
    IIterable *This);

  ULONG (STDMETHODCALLTYPE *Release)(
    IIterable *This);

  /*** IInspectable methods ***/
  HRESULT (STDMETHODCALLTYPE *GetIids)(
    IIterable *This,
    UINT32 *count,
    IID **ids);

  HRESULT (STDMETHODCALLTYPE *GetRuntimeClassName)(
    IIterable *This,
    HSTRING *className);

  HRESULT (STDMETHODCALLTYPE *GetTrustLevel)(
    IIterable *This,
    TrustLevel *trustLevel);

  /*** IIterable methods ***/
  HRESULT (STDMETHODCALLTYPE *First)(
    IIterable *This,
    IIterator **first);

  END_INTERFACE
} IIterableVtbl;

interface IIterable {
  CONST_VTBL IIterableVtbl* lpVtbl;
};

/*** IUnknown methods ***/
#define IIterable_QueryInterface(This,riid,ppvObject) (This)->lpVtbl->QueryInterface(This,riid,ppvObject)
#define IIterable_AddRef(This) (This)->lpVtbl->AddRef(This)
#define IIterable_Release(This) (This)->lpVtbl->Release(This)
/*** IInspectable methods ***/
#define IIterable_GetIids(This,count,ids) (This)->lpVtbl->GetIids(This,count,ids)
#define IIterable_GetRuntimeClassName(This,name) (This)->lpVtbl->GetRuntimeClassName(This,name)
#define IIterable_GetTrustLevel(This,level) (This)->lpVtbl->GetTrustLevel(This,level)
/*** IIterable methods ***/
#define IIterable_First(This,retval) (This)->lpVtbl->First(This,retval)
