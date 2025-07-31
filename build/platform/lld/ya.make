RESOURCES_LIBRARY()

DEFAULT(LLD_VERSION ${COMPILER_VERSION})

TOOLCHAIN(lld)
VERSION(${LLD_VERSION})

# There is no backward compatibility between LLVM IR versions 16 and 18.
# So, we need to select lld18 when using clang18 to compile src in LTO mode.
IF (LLD_VERSION == 20)
    DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(LLD_ROOT lld20.json)
ELSEIF (LLD_VERSION == 18)
    DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(LLD_ROOT lld18.json)
ELSEIF (LLD_VERSION == 16)
    DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(LLD_ROOT lld16.json)
ELSEIF (LLD_VERSION == 14)
    # Allow empty lld 14 for android
ELSE()
    MESSAGE(FATAL_ERROR "Unsupported LLD version ${LLD_VERSION} was required")
ENDIF()

IF (OS_ANDROID)
    # Use LLD shipped with Android NDK.
    LDFLAGS(
        -fuse-ld=lld
    )
    IF (ANDROID_API < 29)
        # Dynamic linker on Android does not support lld's default rosegment
        # prior to API Level 29 (Android Q)
        # See:
        # https://android.googlesource.com/platform/ndk/+/master/docs/BuildSystemMaintainers.md#additional-required-arguments
        # https://github.com/android/ndk/issues/1196
        LDFLAGS(
            -Wl,--no-rosegment
        )
    ENDIF()
    # Enable optimized relocations format (e.g. .rela.dyn section) to reduce binary size
    # See:
    # https://android.googlesource.com/platform/ndk/+/master/docs/BuildSystemMaintainers.md#relr-and-relocation-packing
    IF (ANDROID_API >= 30)
        LDFLAGS(-Wl,--pack-dyn-relocs=android+relr)
    ELSEIF (ANDROID_API >= 28)
        LDFLAGS(-Wl,--pack-dyn-relocs=android+relr,--use-android-relr-tags)
    ELSEIF (ANDROID_API >= 23)
        LDFLAGS(-Wl,--pack-dyn-relocs=android)
    ENDIF()
ELSEIF (OS_LINUX)
    LDFLAGS(
        -fuse-ld=lld
        --ld-path=${LLD_ROOT_RESOURCE_GLOBAL}/bin/ld.lld

        # dynlinker on auld ubuntu versions can not handle .rodata stored in standalone segment [citation needed]
        -Wl,--no-rosegment
        # add build-id to binaries to allow external tools check equality of binaries
        -Wl,--build-id=sha1
    )
ELSEIF (OS_FREEBSD)
    LDFLAGS(
        -fuse-ld=lld
        --ld-path=${LLD_ROOT_RESOURCE_GLOBAL}/bin/ld.lld
    )
ELSEIF (OS_DARWIN OR OS_IOS)
    LDFLAGS(
        -fuse-ld=lld
        --ld-path=${LLD_ROOT_RESOURCE_GLOBAL}/bin/ld64.lld
    )
ELSEIF (OS_EMSCRIPTEN)
    LDFLAGS(
        -fuse-ld=${LLD_ROOT_RESOURCE_GLOBAL}/bin/wasm-ld
        # FIXME: Linker does not capture "ld-path" and therefore it can not find "wasm-ld"
    )
ENDIF()

END()
