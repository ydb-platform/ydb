from typing import Final

# Constant fullnames for typechecking
# ===================================

#: Used for typed ``partial`` function.
TYPED_PARTIAL_FUNCTION: Final = 'returns.curry.partial'

#: Used for typed ``curry`` decorator.
TYPED_CURRY_FUNCTION: Final = 'returns.curry.curry'

#: Used for typed ``flow`` call.
TYPED_FLOW_FUNCTION: Final = 'returns._internal.pipeline.flow.flow'

#: Used for typed ``pipe`` call.
TYPED_PIPE_FUNCTION: Final = 'returns._internal.pipeline.pipe.pipe'
TYPED_PIPE_METHOD: Final = 'returns._internal.pipeline.pipe._Pipe.__call__'

#: Used for HKT emulation.
TYPED_KINDN: Final = 'returns.primitives.hkt.KindN'
TYPED_KINDN_ACCESS: Final = f'{TYPED_KINDN}.'
TYPED_KIND_DEKIND: Final = 'returns.primitives.hkt.dekind'
TYPED_KIND_KINDED_CALL: Final = 'returns.primitives.hkt.Kinded.__call__'
TYPED_KIND_KINDED_GET: Final = 'returns.primitives.hkt.Kinded.__get__'

#: Used for :ref:`do-notation`.
DO_NOTATION_METHODS: Final = (
    # Just validation:
    'returns.io.IO.do',
    'returns.maybe.Maybe.do',
    'returns.future.Future.do',
    # Also infer error types:
    'returns.result.Result.do',
    'returns.io.IOResult.do',
    'returns.future.FutureResult.do',
)
