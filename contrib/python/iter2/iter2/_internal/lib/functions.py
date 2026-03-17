from __future__ import annotations

import typing as tp


# --- Functions ---

type Fn0[R] = tp.Callable[[], R]
type Fn1[V, R] = tp.Callable[[V], R]
type Fn2[V1, V2, R] = tp.Callable[[V1, V2], R]
type Fn3[V1, V2, V3, R] = tp.Callable[[V1, V2, V3], R]
type Fn4[V1, V2, V3, V4, R] = tp.Callable[[V1, V2, V3, V4], R]
type Fn5[V1, V2, V3, V4, V5, R] = tp.Callable[[V1, V2, V3, V4, V5], R]
type Fn6[V1, V2, V3, V4, V5, V6, R] = tp.Callable[[V1, V2, V3, V4, V5, V6], R]
type Fn7[V1, V2, V3, V4, V5, V6, V7, R] = tp.Callable[[V1, V2, V3, V4, V5, V6, V7], R]

type Thunk[V] = tp.Callable[[], V]


# --- Predicates ---

type Predicate[V] = Fn1[V, bool]

type TypePredicate[V, T] = Fn1[V, tp.TypeGuard[T]]
