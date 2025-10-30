//===----------------------------------------------------------------------===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
// SPDX-FileCopyrightText: Copyright (c) 2023 NVIDIA CORPORATION & AFFILIATES.
//
//===----------------------------------------------------------------------===//

#ifndef _LIBCUDACXX___TYPE_TRAITS_IS_TRIVIALLY_CONSTRUCTIBLE_H
#define _LIBCUDACXX___TYPE_TRAITS_IS_TRIVIALLY_CONSTRUCTIBLE_H

#include <cuda/std/detail/__config>

#if defined(_CCCL_IMPLICIT_SYSTEM_HEADER_GCC)
#  pragma GCC system_header
#elif defined(_CCCL_IMPLICIT_SYSTEM_HEADER_CLANG)
#  pragma clang system_header
#elif defined(_CCCL_IMPLICIT_SYSTEM_HEADER_MSVC)
#  pragma system_header
#endif // no system header

#include <cuda/std/__type_traits/integral_constant.h>
#include <cuda/std/__type_traits/is_scalar.h>

#include <cuda/std/__cccl/prologue.h>

_LIBCUDACXX_BEGIN_NAMESPACE_STD

#if defined(_CCCL_BUILTIN_IS_TRIVIALLY_CONSTRUCTIBLE) && !defined(_LIBCUDACXX_USE_IS_TRIVIALLY_CONSTRUCTIBLE_FALLBACK)

template <class _Tp, class... _Args>
struct _CCCL_TYPE_VISIBILITY_DEFAULT
is_trivially_constructible : public integral_constant<bool, _CCCL_BUILTIN_IS_TRIVIALLY_CONSTRUCTIBLE(_Tp, _Args...)>
{};

template <class _Tp, class... _Args>
inline constexpr bool is_trivially_constructible_v = _CCCL_BUILTIN_IS_TRIVIALLY_CONSTRUCTIBLE(_Tp, _Args...);

#else

template <class _Tp, class... _Args>
struct _CCCL_TYPE_VISIBILITY_DEFAULT is_trivially_constructible : false_type
{};

_CCCL_SUPPRESS_DEPRECATED_PUSH
template <class _Tp>
struct _CCCL_TYPE_VISIBILITY_DEFAULT is_trivially_constructible<_Tp>
#  if defined(_CCCL_BUILTIN_HAS_TRIVIAL_CONSTRUCTOR) && !defined(_LIBCUDACXX_USE_HAS_TRIVIAL_CONSTRUCTOR_FALLBACK)
    : integral_constant<bool, _CCCL_BUILTIN_HAS_TRIVIAL_CONSTRUCTOR(_Tp)>
#  else
    : integral_constant<bool, is_scalar<_Tp>::value>
#  endif // defined(_CCCL_BUILTIN_HAS_TRIVIAL_CONSTRUCTOR) && !defined(_LIBCUDACXX_USE_HAS_TRIVIAL_CONSTRUCTOR_FALLBACK)
{};
_CCCL_SUPPRESS_DEPRECATED_POP

template <class _Tp>
struct _CCCL_TYPE_VISIBILITY_DEFAULT
is_trivially_constructible<_Tp, _Tp&&> : integral_constant<bool, is_scalar<_Tp>::value>
{};

template <class _Tp>
struct _CCCL_TYPE_VISIBILITY_DEFAULT
is_trivially_constructible<_Tp, const _Tp&> : integral_constant<bool, is_scalar<_Tp>::value>
{};

template <class _Tp>
struct _CCCL_TYPE_VISIBILITY_DEFAULT
is_trivially_constructible<_Tp, _Tp&> : integral_constant<bool, is_scalar<_Tp>::value>
{};

template <class _Tp, class... _Args>
inline constexpr bool is_trivially_constructible_v = is_trivially_constructible<_Tp, _Args...>::value;

#endif // defined(_CCCL_BUILTIN_IS_TRIVIALLY_CONSTRUCTIBLE) &&
       // !defined(_LIBCUDACXX_USE_IS_TRIVIALLY_CONSTRUCTIBLE_FALLBACK)

_LIBCUDACXX_END_NAMESPACE_STD

#include <cuda/std/__cccl/epilogue.h>

#endif // _LIBCUDACXX___TYPE_TRAITS_IS_TRIVIALLY_CONSTRUCTIBLE_H
