#pragma once

#include <contrib/libs/pcre/pcre.h>

#include <util/generic/ptr.h>   // THolder
#include <util/system/types.h>  // wchar16, wchar32

namespace NPcre {
    template <class TCharType>
    struct TPcreTraits;

    template <>
    struct TPcreTraits<char> {
        using TCharType = char;
        using TStringType = const char*;
        using TCodeType = pcre;
        using TExtraType = pcre_extra;
        static constexpr TCodeType* (*Compile)(TStringType pattern, int options, int* errcodeptr, const char** errptr, int* erroffset, const unsigned char* tableptr) = pcre_compile2;
        static constexpr TExtraType* (*Study)(const TCodeType* pattern, int options, const char** errptr) = pcre_study;
        static constexpr int (*Exec)(const TCodeType* code, const TExtraType* extra, TStringType str, int length, int startoffset, int options, int* ovector, int ovecsize) = pcre_exec;
    };

    template <>
    struct TPcreTraits<wchar16> {
        using TCharType = wchar16;
        using TStringType = PCRE_SPTR16;
        using TCodeType = pcre16;
        using TExtraType = pcre16_extra;
        static constexpr TCodeType* (*Compile)(TStringType pattern, int options, int* errcodeptr, const char** errptr, int* erroffset, const unsigned char* tableptr) = pcre16_compile2;
        static constexpr TExtraType* (*Study)(const TCodeType* pattern, int options, const char** errptr) = pcre16_study;
        static constexpr int (*Exec)(const TCodeType* code, const TExtraType* extra, TStringType str, int length, int startoffset, int options, int* ovector, int ovecsize) = pcre16_exec;
    };

    template <>
    struct TPcreTraits<wchar32> {
        using TCharType = wchar32;
        using TStringType = PCRE_SPTR32;
        using TCodeType = pcre32;
        using TExtraType = pcre32_extra;
        static constexpr TCodeType* (*Compile)(TStringType pattern, int options, int* errcodeptr, const char** errptr, int* erroffset, const unsigned char* tableptr) = pcre32_compile2;
        static constexpr TExtraType* (*Study)(const TCodeType* pattern, int options, const char** errptr) = pcre32_study;
        static constexpr int (*Exec)(const TCodeType* code, const TExtraType* extra, TStringType str, int length, int startoffset, int options, int* ovector, int ovecsize) = pcre32_exec;
    };

    template <class TCharType>
    struct TFreePcre;

    template <>
    struct TFreePcre<char> {
        static inline void Destroy(void* ptr) noexcept {
            pcre_free(ptr);
        }
    };

    template <>
    struct TFreePcre<wchar16> {
        static inline void Destroy(void* ptr) noexcept {
            pcre16_free(ptr);
        }
    };

    template <>
    struct TFreePcre<wchar32> {
        static inline void Destroy(void* ptr) noexcept {
            pcre32_free(ptr);
        }
    };

    template <class TCharType>
    struct TFreePcreExtra;

    template <>
    struct TFreePcreExtra<char> {
        static inline void Destroy(pcre_extra* ptr) noexcept {
            pcre_free_study(ptr);
        }
    };

    template <>
    struct TFreePcreExtra<wchar16> {
        static inline void Destroy(pcre16_extra* ptr) noexcept {
            pcre16_free_study(ptr);
        }
    };

    template <>
    struct TFreePcreExtra<wchar32> {
        static inline void Destroy(pcre32_extra* ptr) noexcept {
            pcre32_free_study(ptr);
        }
    };

    template <typename TCharType>
    using TPcreCode = THolder<typename TPcreTraits<TCharType>::TCodeType, TFreePcre<TCharType>>;

    template <typename TCharType>
    using TPcreExtra = THolder<typename TPcreTraits<TCharType>::TExtraType, TFreePcreExtra<TCharType>>;
}

