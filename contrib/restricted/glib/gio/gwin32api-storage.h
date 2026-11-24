struct DateTime;

typedef struct DateTime {
    UINT64 UniversalTime;
} DateTime;

/* The following is copied verbatim from MinGW-w64 windows.storage.h */
enum StorageItemTypes;
enum FileAttributes;
enum NameCollisionOption;
enum StorageDeleteOption;

typedef enum NameCollisionoption {
    NameCollisionoption_GenerateUniqueName = 0,
    NameCollisionoption_ReplaceExisting = 1,
    NameCollisionoption_FailIfExists = 2
} NameCollisionOption;

typedef enum FileAttributes {
    FileAttributes_Normal = 0,
    FileAttributes_ReadOnly = 1,
    FileAttributes_Directory = 2,
    FileAttributes_Archive = 3,
    FileAttributes_Temporary = 4
} FileAttributes;

typedef enum StorageItemTypes {
    StorageItemTypes_None = 0,
    StorageItemTypes_File = 1,
    StorageItemTypes_Folder = 2
} StorageItemTypes;

typedef enum StorageDeleteOption {
    StorageDeleteOption_Default = 0,
    StorageDeleteOption_PermanentDelete = 1
} StorageDeleteOption;

#ifndef __IStorageItem_FWD_DEFINED__
#define __IStorageItem_FWD_DEFINED__
typedef interface IStorageItem IStorageItem;
#endif

/*
 * IStorageItem interface
 */
#ifndef __IStorageItem_INTERFACE_DEFINED__
#define __IStorageItem_INTERFACE_DEFINED__

DEFINE_GUID(IID_IStorageItem, 0x4207a996, 0xca2f, 0x42f7, 0xbd,0xe8, 0x8b,0x10,0x45,0x7a,0x7f,0x30);
#if defined(__cplusplus) && !defined(CINTERFACE)
MIDL_INTERFACE("4207a996-ca2f-42f7-bde8-8b10457a7f30")
IStorageItem : public IInspectable
{
    virtual HRESULT STDMETHODCALLTYPE RenameAsyncOverloadDefaultOptions(
        HSTRING desiredName,
        IInspectable **action) = 0;

    virtual HRESULT STDMETHODCALLTYPE RenameAsync(
        HSTRING desiredName,
        NameCollisionOption option,
        IInspectable **action) = 0;

    virtual HRESULT STDMETHODCALLTYPE DeleteAsyncOverloadDefaultOptions(
        IInspectable **action) = 0;

    virtual HRESULT STDMETHODCALLTYPE DeleteAsync(
        StorageDeleteOption option,
        IInspectable **action) = 0;

    virtual HRESULT STDMETHODCALLTYPE GetBasicPropertiesAsync(
        IInspectable **action) = 0;

    virtual HRESULT STDMETHODCALLTYPE get_Name(
        HSTRING *value) = 0;

    virtual HRESULT STDMETHODCALLTYPE get_Path(
        HSTRING *value) = 0;

    virtual HRESULT STDMETHODCALLTYPE get_Attributes(
        FileAttributes *value) = 0;

    virtual HRESULT STDMETHODCALLTYPE get_DateCreated(
        DateTime *value) = 0;

    virtual HRESULT STDMETHODCALLTYPE IsOfType(
        StorageItemTypes itemType,
        boolean *value) = 0;

};
#ifdef __CRT_UUID_DECL
__CRT_UUID_DECL(IStorageItem, 0x4207a996, 0xca2f, 0x42f7, 0xbd,0xe8, 0x8b,0x10,0x45,0x7a,0x7f,0x30)
#endif
#else
typedef struct IStorageItemVtbl {
    BEGIN_INTERFACE

    /*** IUnknown methods ***/
    HRESULT (STDMETHODCALLTYPE *QueryInterface)(
        IStorageItem* This,
        REFIID riid,
        void **ppvObject);

    ULONG (STDMETHODCALLTYPE *AddRef)(
        IStorageItem* This);

    ULONG (STDMETHODCALLTYPE *Release)(
        IStorageItem* This);

    /*** IInspectable methods ***/
    HRESULT (STDMETHODCALLTYPE *GetIids)(
        IStorageItem* This,
        ULONG *iidCount,
        IID **iids);

    HRESULT (STDMETHODCALLTYPE *GetRuntimeClassName)(
        IStorageItem* This,
        HSTRING *className);

    HRESULT (STDMETHODCALLTYPE *GetTrustLevel)(
        IStorageItem* This,
        TrustLevel *trustLevel);

    /*** IStorageItem methods ***/
    HRESULT (STDMETHODCALLTYPE *RenameAsyncOverloadDefaultOptions)(
        IStorageItem* This,
        HSTRING desiredName,
        IInspectable **action);

    HRESULT (STDMETHODCALLTYPE *RenameAsync)(
        IStorageItem* This,
        HSTRING desiredName,
        NameCollisionOption option,
        IInspectable **action);

    HRESULT (STDMETHODCALLTYPE *DeleteAsyncOverloadDefaultOptions)(
        IStorageItem* This,
        IInspectable **action);

    HRESULT (STDMETHODCALLTYPE *DeleteAsync)(
        IStorageItem* This,
        StorageDeleteOption option,
        IInspectable **action);

    HRESULT (STDMETHODCALLTYPE *GetBasicPropertiesAsync)(
        IStorageItem* This,
        IInspectable **action);

    HRESULT (STDMETHODCALLTYPE *get_Name)(
        IStorageItem* This,
        HSTRING *value);

    HRESULT (STDMETHODCALLTYPE *get_Path)(
        IStorageItem* This,
        HSTRING *value);

    HRESULT (STDMETHODCALLTYPE *get_Attributes)(
        IStorageItem* This,
        FileAttributes *value);

    HRESULT (STDMETHODCALLTYPE *get_DateCreated)(
        IStorageItem* This,
        DateTime *value);

    HRESULT (STDMETHODCALLTYPE *IsOfType)(
        IStorageItem* This,
        StorageItemTypes itemType,
        boolean *value);

    END_INTERFACE
} IStorageItemVtbl;
interface IStorageItem {
    CONST_VTBL IStorageItemVtbl* lpVtbl;
};

#ifdef COBJMACROS
#ifndef WIDL_C_INLINE_WRAPPERS
/*** IUnknown methods ***/
#define IStorageItem_QueryInterface(This,riid,ppvObject) (This)->lpVtbl->QueryInterface(This,riid,ppvObject)
#define IStorageItem_AddRef(This) (This)->lpVtbl->AddRef(This)
#define IStorageItem_Release(This) (This)->lpVtbl->Release(This)
/*** IInspectable methods ***/
#define IStorageItem_GetIids(This,iidCount,iids) (This)->lpVtbl->GetIids(This,iidCount,iids)
#define IStorageItem_GetRuntimeClassName(This,className) (This)->lpVtbl->GetRuntimeClassName(This,className)
#define IStorageItem_GetTrustLevel(This,trustLevel) (This)->lpVtbl->GetTrustLevel(This,trustLevel)
/*** IStorageItem methods ***/
#define IStorageItem_RenameAsyncOverloadDefaultOptions(This,desiredName,action) (This)->lpVtbl->RenameAsyncOverloadDefaultOptions(This,desiredName,action)
#define IStorageItem_RenameAsync(This,desiredName,option,action) (This)->lpVtbl->RenameAsync(This,desiredName,option,action)
#define IStorageItem_DeleteAsyncOverloadDefaultOptions(This,action) (This)->lpVtbl->DeleteAsyncOverloadDefaultOptions(This,action)
#define IStorageItem_DeleteAsync(This,option,action) (This)->lpVtbl->DeleteAsync(This,option,action)
#define IStorageItem_GetBasicPropertiesAsync(This,action) (This)->lpVtbl->GetBasicPropertiesAsync(This,action)
#define IStorageItem_get_Name(This,value) (This)->lpVtbl->get_Name(This,value)
#define IStorageItem_get_Path(This,value) (This)->lpVtbl->get_Path(This,value)
#define IStorageItem_get_Attributes(This,value) (This)->lpVtbl->get_Attributes(This,value)
#define IStorageItem_get_DateCreated(This,value) (This)->lpVtbl->get_DateCreated(This,value)
#define IStorageItem_IsOfType(This,itemType,value) (This)->lpVtbl->IsOfType(This,itemType,value)
#else
/*** IUnknown methods ***/
static FORCEINLINE HRESULT IStorageItem_QueryInterface(IStorageItem* This,REFIID riid,void **ppvObject) {
    return This->lpVtbl->QueryInterface(This,riid,ppvObject);
}
static FORCEINLINE ULONG IStorageItem_AddRef(IStorageItem* This) {
    return This->lpVtbl->AddRef(This);
}
static FORCEINLINE ULONG IStorageItem_Release(IStorageItem* This) {
    return This->lpVtbl->Release(This);
}
/*** IInspectable methods ***/
static FORCEINLINE HRESULT IStorageItem_GetIids(IStorageItem* This,ULONG *iidCount,IID **iids) {
    return This->lpVtbl->GetIids(This,iidCount,iids);
}
static FORCEINLINE HRESULT IStorageItem_GetRuntimeClassName(IStorageItem* This,HSTRING *className) {
    return This->lpVtbl->GetRuntimeClassName(This,className);
}
static FORCEINLINE HRESULT IStorageItem_GetTrustLevel(IStorageItem* This,TrustLevel *trustLevel) {
    return This->lpVtbl->GetTrustLevel(This,trustLevel);
}
/*** IStorageItem methods ***/
static FORCEINLINE HRESULT IStorageItem_RenameAsyncOverloadDefaultOptions(IStorageItem* This,HSTRING desiredName,IInspectable **action) {
    return This->lpVtbl->RenameAsyncOverloadDefaultOptions(This,desiredName,action);
}
static FORCEINLINE HRESULT IStorageItem_RenameAsync(IStorageItem* This,HSTRING desiredName,NameCollisionOption option,IInspectable **action) {
    return This->lpVtbl->RenameAsync(This,desiredName,option,action);
}
static FORCEINLINE HRESULT IStorageItem_DeleteAsyncOverloadDefaultOptions(IStorageItem* This,IInspectable **action) {
    return This->lpVtbl->DeleteAsyncOverloadDefaultOptions(This,action);
}
static FORCEINLINE HRESULT IStorageItem_DeleteAsync(IStorageItem* This,StorageDeleteOption option,IInspectable **action) {
    return This->lpVtbl->DeleteAsync(This,option,action);
}
static FORCEINLINE HRESULT IStorageItem_GetBasicPropertiesAsync(IStorageItem* This,IInspectable **action) {
    return This->lpVtbl->GetBasicPropertiesAsync(This,action);
}
static FORCEINLINE HRESULT IStorageItem_get_Name(IStorageItem* This,HSTRING *value) {
    return This->lpVtbl->get_Name(This,value);
}
static FORCEINLINE HRESULT IStorageItem_get_Path(IStorageItem* This,HSTRING *value) {
    return This->lpVtbl->get_Path(This,value);
}
static FORCEINLINE HRESULT IStorageItem_get_Attributes(IStorageItem* This,FileAttributes *value) {
    return This->lpVtbl->get_Attributes(This,value);
}
static FORCEINLINE HRESULT IStorageItem_get_DateCreated(IStorageItem* This,DateTime *value) {
    return This->lpVtbl->get_DateCreated(This,value);
}
static FORCEINLINE HRESULT IStorageItem_IsOfType(IStorageItem* This,StorageItemTypes itemType,boolean *value) {
    return This->lpVtbl->IsOfType(This,itemType,value);
}
#endif
#endif

#endif

HRESULT STDMETHODCALLTYPE IStorageItem_RenameAsyncOverloadDefaultOptions_Proxy(
    IStorageItem* This,
    HSTRING desiredName,
    IInspectable **action);
void __RPC_STUB IStorageItem_RenameAsyncOverloadDefaultOptions_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_RenameAsync_Proxy(
    IStorageItem* This,
    HSTRING desiredName,
    NameCollisionOption option,
    IInspectable **action);
void __RPC_STUB IStorageItem_RenameAsync_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_DeleteAsyncOverloadDefaultOptions_Proxy(
    IStorageItem* This,
    IInspectable **action);
void __RPC_STUB IStorageItem_DeleteAsyncOverloadDefaultOptions_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_DeleteAsync_Proxy(
    IStorageItem* This,
    StorageDeleteOption option,
    IInspectable **action);
void __RPC_STUB IStorageItem_DeleteAsync_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_GetBasicPropertiesAsync_Proxy(
    IStorageItem* This,
    IInspectable **action);
void __RPC_STUB IStorageItem_GetBasicPropertiesAsync_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_get_Name_Proxy(
    IStorageItem* This,
    HSTRING *value);
void __RPC_STUB IStorageItem_get_Name_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_get_Path_Proxy(
    IStorageItem* This,
    HSTRING *value);
void __RPC_STUB IStorageItem_get_Path_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_get_Attributes_Proxy(
    IStorageItem* This,
    FileAttributes *value);
void __RPC_STUB IStorageItem_get_Attributes_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_get_DateCreated_Proxy(
    IStorageItem* This,
    DateTime *value);
void __RPC_STUB IStorageItem_get_DateCreated_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);
HRESULT STDMETHODCALLTYPE IStorageItem_IsOfType_Proxy(
    IStorageItem* This,
    StorageItemTypes itemType,
    boolean *value);
void __RPC_STUB IStorageItem_IsOfType_Stub(
    IRpcStubBuffer* This,
    IRpcChannelBuffer* pRpcChannelBuffer,
    PRPC_MESSAGE pRpcMessage,
    DWORD* pdwStubPhase);

#endif  /* __IStorageItem_INTERFACE_DEFINED__ */
