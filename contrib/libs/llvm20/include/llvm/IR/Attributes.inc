#ifdef GET_ATTR_NAMES
#undef GET_ATTR_NAMES
#ifndef ATTRIBUTE_ALL
#define ATTRIBUTE_ALL(FIRST, SECOND)
#endif

#ifndef ATTRIBUTE_ENUM
#define ATTRIBUTE_ENUM(FIRST, SECOND) ATTRIBUTE_ALL(FIRST, SECOND)
#endif

ATTRIBUTE_ENUM(AllocAlign,allocalign)
ATTRIBUTE_ENUM(AllocatedPointer,allocptr)
ATTRIBUTE_ENUM(AlwaysInline,alwaysinline)
ATTRIBUTE_ENUM(Builtin,builtin)
ATTRIBUTE_ENUM(Cold,cold)
ATTRIBUTE_ENUM(Convergent,convergent)
ATTRIBUTE_ENUM(CoroDestroyOnlyWhenComplete,coro_only_destroy_when_complete)
ATTRIBUTE_ENUM(CoroElideSafe,coro_elide_safe)
ATTRIBUTE_ENUM(DeadOnUnwind,dead_on_unwind)
ATTRIBUTE_ENUM(DisableSanitizerInstrumentation,disable_sanitizer_instrumentation)
ATTRIBUTE_ENUM(FnRetThunkExtern,fn_ret_thunk_extern)
ATTRIBUTE_ENUM(Hot,hot)
ATTRIBUTE_ENUM(HybridPatchable,hybrid_patchable)
ATTRIBUTE_ENUM(ImmArg,immarg)
ATTRIBUTE_ENUM(InReg,inreg)
ATTRIBUTE_ENUM(InlineHint,inlinehint)
ATTRIBUTE_ENUM(JumpTable,jumptable)
ATTRIBUTE_ENUM(MinSize,minsize)
ATTRIBUTE_ENUM(MustProgress,mustprogress)
ATTRIBUTE_ENUM(Naked,naked)
ATTRIBUTE_ENUM(Nest,nest)
ATTRIBUTE_ENUM(NoAlias,noalias)
ATTRIBUTE_ENUM(NoBuiltin,nobuiltin)
ATTRIBUTE_ENUM(NoCallback,nocallback)
ATTRIBUTE_ENUM(NoCapture,nocapture)
ATTRIBUTE_ENUM(NoCfCheck,nocf_check)
ATTRIBUTE_ENUM(NoDivergenceSource,nodivergencesource)
ATTRIBUTE_ENUM(NoDuplicate,noduplicate)
ATTRIBUTE_ENUM(NoExt,noext)
ATTRIBUTE_ENUM(NoFree,nofree)
ATTRIBUTE_ENUM(NoImplicitFloat,noimplicitfloat)
ATTRIBUTE_ENUM(NoInline,noinline)
ATTRIBUTE_ENUM(NoMerge,nomerge)
ATTRIBUTE_ENUM(NoProfile,noprofile)
ATTRIBUTE_ENUM(NoRecurse,norecurse)
ATTRIBUTE_ENUM(NoRedZone,noredzone)
ATTRIBUTE_ENUM(NoReturn,noreturn)
ATTRIBUTE_ENUM(NoSanitizeBounds,nosanitize_bounds)
ATTRIBUTE_ENUM(NoSanitizeCoverage,nosanitize_coverage)
ATTRIBUTE_ENUM(NoSync,nosync)
ATTRIBUTE_ENUM(NoUndef,noundef)
ATTRIBUTE_ENUM(NoUnwind,nounwind)
ATTRIBUTE_ENUM(NonLazyBind,nonlazybind)
ATTRIBUTE_ENUM(NonNull,nonnull)
ATTRIBUTE_ENUM(NullPointerIsValid,null_pointer_is_valid)
ATTRIBUTE_ENUM(OptForFuzzing,optforfuzzing)
ATTRIBUTE_ENUM(OptimizeForDebugging,optdebug)
ATTRIBUTE_ENUM(OptimizeForSize,optsize)
ATTRIBUTE_ENUM(OptimizeNone,optnone)
ATTRIBUTE_ENUM(PresplitCoroutine,presplitcoroutine)
ATTRIBUTE_ENUM(ReadNone,readnone)
ATTRIBUTE_ENUM(ReadOnly,readonly)
ATTRIBUTE_ENUM(Returned,returned)
ATTRIBUTE_ENUM(ReturnsTwice,returns_twice)
ATTRIBUTE_ENUM(SExt,signext)
ATTRIBUTE_ENUM(SafeStack,safestack)
ATTRIBUTE_ENUM(SanitizeAddress,sanitize_address)
ATTRIBUTE_ENUM(SanitizeHWAddress,sanitize_hwaddress)
ATTRIBUTE_ENUM(SanitizeMemTag,sanitize_memtag)
ATTRIBUTE_ENUM(SanitizeMemory,sanitize_memory)
ATTRIBUTE_ENUM(SanitizeNumericalStability,sanitize_numerical_stability)
ATTRIBUTE_ENUM(SanitizeRealtime,sanitize_realtime)
ATTRIBUTE_ENUM(SanitizeRealtimeBlocking,sanitize_realtime_blocking)
ATTRIBUTE_ENUM(SanitizeThread,sanitize_thread)
ATTRIBUTE_ENUM(SanitizeType,sanitize_type)
ATTRIBUTE_ENUM(ShadowCallStack,shadowcallstack)
ATTRIBUTE_ENUM(SkipProfile,skipprofile)
ATTRIBUTE_ENUM(Speculatable,speculatable)
ATTRIBUTE_ENUM(SpeculativeLoadHardening,speculative_load_hardening)
ATTRIBUTE_ENUM(StackProtect,ssp)
ATTRIBUTE_ENUM(StackProtectReq,sspreq)
ATTRIBUTE_ENUM(StackProtectStrong,sspstrong)
ATTRIBUTE_ENUM(StrictFP,strictfp)
ATTRIBUTE_ENUM(SwiftAsync,swiftasync)
ATTRIBUTE_ENUM(SwiftError,swifterror)
ATTRIBUTE_ENUM(SwiftSelf,swiftself)
ATTRIBUTE_ENUM(WillReturn,willreturn)
ATTRIBUTE_ENUM(Writable,writable)
ATTRIBUTE_ENUM(WriteOnly,writeonly)
ATTRIBUTE_ENUM(ZExt,zeroext)
ATTRIBUTE_ENUM(ByRef,byref)
ATTRIBUTE_ENUM(ByVal,byval)
ATTRIBUTE_ENUM(ElementType,elementtype)
ATTRIBUTE_ENUM(InAlloca,inalloca)
ATTRIBUTE_ENUM(Preallocated,preallocated)
ATTRIBUTE_ENUM(StructRet,sret)
ATTRIBUTE_ENUM(Alignment,align)
ATTRIBUTE_ENUM(AllocKind,allockind)
ATTRIBUTE_ENUM(AllocSize,allocsize)
ATTRIBUTE_ENUM(Captures,captures)
ATTRIBUTE_ENUM(Dereferenceable,dereferenceable)
ATTRIBUTE_ENUM(DereferenceableOrNull,dereferenceable_or_null)
ATTRIBUTE_ENUM(Memory,memory)
ATTRIBUTE_ENUM(NoFPClass,nofpclass)
ATTRIBUTE_ENUM(StackAlignment,alignstack)
ATTRIBUTE_ENUM(UWTable,uwtable)
ATTRIBUTE_ENUM(VScaleRange,vscale_range)
ATTRIBUTE_ENUM(Range,range)
ATTRIBUTE_ENUM(Initializes,initializes)
#undef ATTRIBUTE_ENUM

#ifndef ATTRIBUTE_STRBOOL
#define ATTRIBUTE_STRBOOL(FIRST, SECOND) ATTRIBUTE_ALL(FIRST, SECOND)
#endif

ATTRIBUTE_STRBOOL(ApproxFuncFPMath,approx-func-fp-math)
ATTRIBUTE_STRBOOL(LessPreciseFPMAD,less-precise-fpmad)
ATTRIBUTE_STRBOOL(NoInfsFPMath,no-infs-fp-math)
ATTRIBUTE_STRBOOL(NoInlineLineTables,no-inline-line-tables)
ATTRIBUTE_STRBOOL(NoJumpTables,no-jump-tables)
ATTRIBUTE_STRBOOL(NoNansFPMath,no-nans-fp-math)
ATTRIBUTE_STRBOOL(NoSignedZerosFPMath,no-signed-zeros-fp-math)
ATTRIBUTE_STRBOOL(ProfileSampleAccurate,profile-sample-accurate)
ATTRIBUTE_STRBOOL(UnsafeFPMath,unsafe-fp-math)
ATTRIBUTE_STRBOOL(UseSampleProfile,use-sample-profile)
#undef ATTRIBUTE_STRBOOL

#ifndef ATTRIBUTE_COMPLEXSTR
#define ATTRIBUTE_COMPLEXSTR(FIRST, SECOND) ATTRIBUTE_ALL(FIRST, SECOND)
#endif

ATTRIBUTE_COMPLEXSTR(DenormalFPMath,denormal-fp-math)
ATTRIBUTE_COMPLEXSTR(DenormalFPMathF32,denormal-fp-math-f32)
#undef ATTRIBUTE_COMPLEXSTR

#undef ATTRIBUTE_ALL
#endif

#ifdef GET_ATTR_ENUM
#undef GET_ATTR_ENUM
FirstEnumAttr = 1,
AllocAlign = 1,
AllocatedPointer = 2,
AlwaysInline = 3,
Builtin = 4,
Cold = 5,
Convergent = 6,
CoroDestroyOnlyWhenComplete = 7,
CoroElideSafe = 8,
DeadOnUnwind = 9,
DisableSanitizerInstrumentation = 10,
FnRetThunkExtern = 11,
Hot = 12,
HybridPatchable = 13,
ImmArg = 14,
InReg = 15,
InlineHint = 16,
JumpTable = 17,
MinSize = 18,
MustProgress = 19,
Naked = 20,
Nest = 21,
NoAlias = 22,
NoBuiltin = 23,
NoCallback = 24,
NoCapture = 25,
NoCfCheck = 26,
NoDivergenceSource = 27,
NoDuplicate = 28,
NoExt = 29,
NoFree = 30,
NoImplicitFloat = 31,
NoInline = 32,
NoMerge = 33,
NoProfile = 34,
NoRecurse = 35,
NoRedZone = 36,
NoReturn = 37,
NoSanitizeBounds = 38,
NoSanitizeCoverage = 39,
NoSync = 40,
NoUndef = 41,
NoUnwind = 42,
NonLazyBind = 43,
NonNull = 44,
NullPointerIsValid = 45,
OptForFuzzing = 46,
OptimizeForDebugging = 47,
OptimizeForSize = 48,
OptimizeNone = 49,
PresplitCoroutine = 50,
ReadNone = 51,
ReadOnly = 52,
Returned = 53,
ReturnsTwice = 54,
SExt = 55,
SafeStack = 56,
SanitizeAddress = 57,
SanitizeHWAddress = 58,
SanitizeMemTag = 59,
SanitizeMemory = 60,
SanitizeNumericalStability = 61,
SanitizeRealtime = 62,
SanitizeRealtimeBlocking = 63,
SanitizeThread = 64,
SanitizeType = 65,
ShadowCallStack = 66,
SkipProfile = 67,
Speculatable = 68,
SpeculativeLoadHardening = 69,
StackProtect = 70,
StackProtectReq = 71,
StackProtectStrong = 72,
StrictFP = 73,
SwiftAsync = 74,
SwiftError = 75,
SwiftSelf = 76,
WillReturn = 77,
Writable = 78,
WriteOnly = 79,
ZExt = 80,
LastEnumAttr = 80,
FirstTypeAttr = 81,
ByRef = 81,
ByVal = 82,
ElementType = 83,
InAlloca = 84,
Preallocated = 85,
StructRet = 86,
LastTypeAttr = 86,
FirstIntAttr = 87,
Alignment = 87,
AllocKind = 88,
AllocSize = 89,
Captures = 90,
Dereferenceable = 91,
DereferenceableOrNull = 92,
Memory = 93,
NoFPClass = 94,
StackAlignment = 95,
UWTable = 96,
VScaleRange = 97,
LastIntAttr = 97,
FirstConstantRangeAttr = 98,
Range = 98,
LastConstantRangeAttr = 98,
FirstConstantRangeListAttr = 99,
Initializes = 99,
LastConstantRangeListAttr = 99,
#endif

#ifdef GET_ATTR_COMPAT_FUNC
#undef GET_ATTR_COMPAT_FUNC
static inline bool hasCompatibleFnAttrs(const Function &Caller,
                                        const Function &Callee) {
  bool Ret = true;

  Ret &= isEqual<SanitizeAddressAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeThreadAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeTypeAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeMemoryAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeHWAddressAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeMemTagAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeNumericalStabilityAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeRealtimeAttr>(Caller, Callee);
  Ret &= isEqual<SanitizeRealtimeBlockingAttr>(Caller, Callee);
  Ret &= isEqual<SafeStackAttr>(Caller, Callee);
  Ret &= isEqual<ShadowCallStackAttr>(Caller, Callee);
  Ret &= isEqual<UseSampleProfileAttr>(Caller, Callee);
  Ret &= isEqual<NoProfileAttr>(Caller, Callee);
  Ret &= checkDenormMode(Caller, Callee);
  Ret &= checkStrictFP(Caller, Callee);
  Ret &= isEqual(Caller, Callee, "sign-return-address");
  Ret &= isEqual(Caller, Callee, "sign-return-address-key");
  Ret &= isEqual(Caller, Callee, "branch-protection-pauth-lr");

  return Ret;
}

static inline void mergeFnAttrs(Function &Caller,
                                const Function &Callee) {
  setAND<LessPreciseFPMADAttr>(Caller, Callee);
  setAND<NoInfsFPMathAttr>(Caller, Callee);
  setAND<NoNansFPMathAttr>(Caller, Callee);
  setAND<ApproxFuncFPMathAttr>(Caller, Callee);
  setAND<NoSignedZerosFPMathAttr>(Caller, Callee);
  setAND<UnsafeFPMathAttr>(Caller, Callee);
  setOR<NoImplicitFloatAttr>(Caller, Callee);
  setOR<NoJumpTablesAttr>(Caller, Callee);
  setOR<ProfileSampleAccurateAttr>(Caller, Callee);
  setOR<SpeculativeLoadHardeningAttr>(Caller, Callee);
  adjustCallerSSPLevel(Caller, Callee);
  adjustCallerStackProbes(Caller, Callee);
  adjustCallerStackProbeSize(Caller, Callee);
  adjustMinLegalVectorWidth(Caller, Callee);
  adjustNullPointerValidAttr(Caller, Callee);
  setAND<MustProgressAttr>(Caller, Callee);
}

#endif
#ifdef GET_ATTR_PROP_TABLE
#undef GET_ATTR_PROP_TABLE
static const uint8_t AttrPropTable[] = {
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectAnd,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectCustom,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectCustom,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectMin,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectMin,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectCustom,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectCustom,
0 | AttributeProperty::FnAttr | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::FnAttr | AttributeProperty::IntersectPreserve,
0 | AttributeProperty::ParamAttr | AttributeProperty::RetAttr | AttributeProperty::IntersectCustom,
0 | AttributeProperty::ParamAttr | AttributeProperty::IntersectPreserve,
};
#endif
