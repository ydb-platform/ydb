RESOURCES_LIBRARY()

LICENSE(Service-Prebuilt-Tool)

DEFAULT(LLD_VERSION ${CLANG_VER})

IF (LLD_VERSION == 14)
    DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(LLD_ROOT lld14.json)
ELSE()
    # fallback on latest version
    DECLARE_EXTERNAL_HOST_RESOURCES_BUNDLE_BY_JSON(LLD_ROOT lld16.json)
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
        --ld-path=${LLD_ROOT_RESOURCE_GLOBAL}/ld.lld

        # dynlinker on auld ubuntu versions can not handle .rodata stored in standalone segment [citation needed]
        -Wl,--no-rosegment
        # add build-id to binaries to allow external tools check equality of binaries
        -Wl,--build-id=sha1
    )
ELSEIF (OS_DARWIN OR OS_IOS)
    IF (MAPSMOBI_BUILD_TARGET AND XCODE)
        LDFLAGS(
            -fuse-ld=${LLD_ROOT_RESOURCE_GLOBAL}/ld64.lld
        )
    ELSEIF (XCODE)
        LDFLAGS(-DYA_XCODE)
    ELSE()
        LDFLAGS(
            -fuse-ld=lld
            --ld-path=${LLD_ROOT_RESOURCE_GLOBAL}/ld64.lld
            # FIXME: Remove fake linker version flag when clang 16 version arrives
            -mlinker-version=705
        )
    ENDIF()
ELSEIF (OS_EMSCRIPTEN)
    LDFLAGS(
        -fuse-ld=${LLD_ROOT_RESOURCE_GLOBAL}/wasm-ld
        # FIXME: Linker does not capture "ld-path" and therefore it can not find "wasm-ld"
    )
ENDIF()

END()
