#if NTDDI_VERSION < NTDDI_WIN8
/* The following code is copied verbatim from MinGW-w64 shobjidl.h */
/*
 * IApplicationActivationManager interface
 */
typedef enum ACTIVATEOPTIONS {
    AO_NONE = 0x0,
    AO_DESIGNMODE = 0x1,
    AO_NOERRORUI = 0x2,
    AO_NOSPLASHSCREEN = 0x4
} ACTIVATEOPTIONS;

DEFINE_ENUM_FLAG_OPERATORS(ACTIVATEOPTIONS)

#ifndef __IApplicationActivationManager_INTERFACE_DEFINED__
#define __IApplicationActivationManager_INTERFACE_DEFINED__

DEFINE_GUID(IID_IApplicationActivationManager, 0x2e941141, 0x7f97, 0x4756, 0xba,0x1d, 0x9d,0xec,0xde,0x89,0x4a,0x3d);
#if defined(__cplusplus) && !defined(CINTERFACE)
MIDL_INTERFACE("2e941141-7f97-4756-ba1d-9decde894a3d")
IApplicationActivationManager : public IUnknown
{
    virtual HRESULT STDMETHODCALLTYPE ActivateApplication(
        LPCWSTR appUserModelId,
        LPCWSTR arguments,
        ACTIVATEOPTIONS options,
        DWORD *processId) = 0;

    virtual HRESULT STDMETHODCALLTYPE ActivateForFile(
        LPCWSTR appUserModelId,
        IShellItemArray *itemArray,
        LPCWSTR verb,
        DWORD *processId) = 0;

    virtual HRESULT STDMETHODCALLTYPE ActivateForProtocol(
        LPCWSTR appUserModelId,
        IShellItemArray *itemArray,
        DWORD *processId) = 0;

};
#ifdef __CRT_UUID_DECL
__CRT_UUID_DECL(IApplicationActivationManager, 0x2e941141, 0x7f97, 0x4756, 0xba,0x1d, 0x9d,0xec,0xde,0x89,0x4a,0x3d)
#endif
#else
typedef struct IApplicationActivationManagerVtbl {
    BEGIN_INTERFACE

    /*** IUnknown methods ***/
    HRESULT (STDMETHODCALLTYPE *QueryInterface)(
        IApplicationActivationManager *This,
        REFIID riid,
        void **ppvObject);

    ULONG (STDMETHODCALLTYPE *AddRef)(
        IApplicationActivationManager *This);

    ULONG (STDMETHODCALLTYPE *Release)(
        IApplicationActivationManager *This);

    /*** IApplicationActivationManager methods ***/
    HRESULT (STDMETHODCALLTYPE *ActivateApplication)(
        IApplicationActivationManager *This,
        LPCWSTR appUserModelId,
        LPCWSTR arguments,
        ACTIVATEOPTIONS options,
        DWORD *processId);

    HRESULT (STDMETHODCALLTYPE *ActivateForFile)(
        IApplicationActivationManager *This,
        LPCWSTR appUserModelId,
        IShellItemArray *itemArray,
        LPCWSTR verb,
        DWORD *processId);

    HRESULT (STDMETHODCALLTYPE *ActivateForProtocol)(
        IApplicationActivationManager *This,
        LPCWSTR appUserModelId,
        IShellItemArray *itemArray,
        DWORD *processId);

    END_INTERFACE
} IApplicationActivationManagerVtbl;

interface IApplicationActivationManager {
    CONST_VTBL IApplicationActivationManagerVtbl* lpVtbl;
};

#ifdef COBJMACROS
#ifndef WIDL_C_INLINE_WRAPPERS
/*** IUnknown methods ***/
#define IApplicationActivationManager_QueryInterface(This,riid,ppvObject) (This)->lpVtbl->QueryInterface(This,riid,ppvObject)
#define IApplicationActivationManager_AddRef(This) (This)->lpVtbl->AddRef(This)
#define IApplicationActivationManager_Release(This) (This)->lpVtbl->Release(This)
/*** IApplicationActivationManager methods ***/
#define IApplicationActivationManager_ActivateApplication(This,appUserModelId,arguments,options,processId) (This)->lpVtbl->ActivateApplication(This,appUserModelId,arguments,options,processId)
#define IApplicationActivationManager_ActivateForFile(This,appUserModelId,itemArray,verb,processId) (This)->lpVtbl->ActivateForFile(This,appUserModelId,itemArray,verb,processId)
#define IApplicationActivationManager_ActivateForProtocol(This,appUserModelId,itemArray,processId) (This)->lpVtbl->ActivateForProtocol(This,appUserModelId,itemArray,processId)
#else
/*** IUnknown methods ***/
static FORCEINLINE HRESULT IApplicationActivationManager_QueryInterface(IApplicationActivationManager* This,REFIID riid,void **ppvObject) {
    return This->lpVtbl->QueryInterface(This,riid,ppvObject);
}
static FORCEINLINE ULONG IApplicationActivationManager_AddRef(IApplicationActivationManager* This) {
    return This->lpVtbl->AddRef(This);
}
static FORCEINLINE ULONG IApplicationActivationManager_Release(IApplicationActivationManager* This) {
    return This->lpVtbl->Release(This);
}
/*** IApplicationActivationManager methods ***/
static FORCEINLINE HRESULT IApplicationActivationManager_ActivateApplication(IApplicationActivationManager* This,LPCWSTR appUserModelId,LPCWSTR arguments,ACTIVATEOPTIONS options,DWORD *processId) {
    return This->lpVtbl->ActivateApplication(This,appUserModelId,arguments,options,processId);
}
static FORCEINLINE HRESULT IApplicationActivationManager_ActivateForFile(IApplicationActivationManager* This,LPCWSTR appUserModelId,IShellItemArray *itemArray,LPCWSTR verb,DWORD *processId) {
    return This->lpVtbl->ActivateForFile(This,appUserModelId,itemArray,verb,processId);
}
static FORCEINLINE HRESULT IApplicationActivationManager_ActivateForProtocol(IApplicationActivationManager* This,LPCWSTR appUserModelId,IShellItemArray *itemArray,DWORD *processId) {
    return This->lpVtbl->ActivateForProtocol(This,appUserModelId,itemArray,processId);
}
#endif
#endif

#endif


#endif  /* __IApplicationActivationManager_INTERFACE_DEFINED__ */
#endif /* NTDDI_VERSION < NTDDI_WIN8 */