import abc
from typing import (
    Any, Callable, Optional, Sequence, TypeVar, Union, cast, overload,
)

import click

from cloup._util import (
    FrozenSpace, check_arg, class_name,
    first_bool, make_one_line_repr, make_repr, pluralize, reindent,
)
from .common import (
    format_param_list,
    get_param_label,
    get_param_name,
    get_params_whose_value_is_set,
    get_required_params,
    param_value_is_set,
)
from .exceptions import ConstraintViolated, UnsatisfiableConstraint
from ..typing import Decorator, F

Op = TypeVar('Op', bound='Operator')
HelpRephraser = Callable[[click.Context, 'Constraint'], str]
ErrorRephraser = Callable[[ConstraintViolated], str]


class Constraint(abc.ABC):
    """
    A constraint that can be checked against an arbitrary collection of CLI
    parameters with respect to a specific :class:`click.Context` (which
    contains the values assigned to the parameters in ``ctx.params``).

    .. versionchanged:: 0.9.0
        calling a constraint, previously equivalent to :meth:`~Constraint.check`,
        is now equivalent to calling :func:`cloup.constrained_params` with this
        constraint as first argument.
    """

    @staticmethod
    def must_check_consistency(ctx: click.Context) -> bool:
        """Return ``True`` if consistency checks are enabled.

        .. versionchanged:: 0.9.0
            this method now a static method and takes a ``click.Context`` in input.
        """
        return first_bool(
            getattr(ctx, 'check_constraints_consistency', True),
            True,
        )

    def __getattr__(self, attr: str) -> Any:
        removed_attrs = ('toggle_consistency_checks', 'consistency_checks_toggled')
        if attr in removed_attrs:
            raise AttributeError(
                f'attribute `{attr}` was removed in v0.9. You can now enable/disable '
                f'consistency checks using the `click.Context` parameter '
                f'`check_constraints_consistency`. '
                f'Pass it as part of your `context_settings`.'
            )
        else:
            raise AttributeError(attr)

    @abc.abstractmethod
    def help(self, ctx: click.Context) -> str:
        """A description of the constraint. """

    def check_consistency(self, params: Sequence[click.Parameter]) -> None:
        """
        Perform some sanity checks that detect inconsistencies between these
        constraints and the properties of the input parameters (e.g. required).

        For example, a constraint that requires the parameters to be mutually
        exclusive is not consistent with a group of parameters with multiple
        required options.

        These sanity checks are meant to catch developer's mistakes and don't
        depend on the values assigned to the parameters; therefore:

        - they can be performed before any parameter parsing
        - they can be disabled in production (setting
          ``check_constraints_consistency=False`` in ``context_settings``)

        :param params: list of :class:`click.Parameter` instances
        :raises: :exc:`~cloup.constraints.errors.UnsatisfiableConstraint`
                 if the constraint cannot be satisfied independently from the values
                 provided by the user
        """

    @abc.abstractmethod
    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        """
        Check that the constraint is satisfied by the input parameters in the
        given context, which (among other things) contains the values assigned
        to the parameters in ``ctx.params``.

        You probably don't want to call this method directly.
        Use :meth:`check` instead.

        :param params: list of :class:`click.Parameter` instances
        :param ctx: :class:`click.Context`
        :raises:
            :exc:`~cloup.constraints.ConstraintViolated`
        """

    @overload
    def check(
        self, params: Sequence[click.Parameter], ctx: Optional[click.Context] = None
    ) -> None:
        ...

    @overload
    def check(self, params: Sequence[str], ctx: Optional[click.Context] = None) -> None:
        ...

    def check(
        self, params: Union[Sequence[click.Parameter], Sequence[str]],
        ctx: Optional[click.Context] = None
    ) -> None:
        """
        Raise an exception if the constraint is not satisfied by the input
        parameters in the given (or current) context.

        This method calls both :meth:`check_consistency` (if enabled) and
        :meth:`check_values`.

        .. tip::
            By default :meth:`check_consistency` is called since it shouldn't
            have any performance impact. Nonetheless, you can disable it in
            production passing ``check_constraints_consistency=False`` as part
            of your ``context_settings``.

        :param params: an iterable of parameter names or a sequence of
                       :class:`click.Parameter`
        :param ctx: a `click.Context`; if not provided, :func:`click.get_current_context`
                    is used
        :raises:
            :exc:`~cloup.constraints.ConstraintViolated`
            :exc:`~cloup.constraints.UnsatisfiableConstraint`
        """
        from ._support import ConstraintMixin

        if not params:
            raise ValueError("argument `params` can't be empty")

        ctx = click.get_current_context() if ctx is None else ctx
        if not isinstance(ctx.command, ConstraintMixin):  # this is needed for mypy
            raise TypeError('constraints work only if the command inherits from '
                            '`ConstraintMixin`')

        if isinstance(params[0], str):
            param_names = cast(Sequence[str], params)
            params_objects = ctx.command.get_params_by_name(param_names)
        else:
            params_objects = cast(Sequence[click.Parameter], params)

        if Constraint.must_check_consistency(ctx):
            self.check_consistency(params_objects)
        return self.check_values(params_objects, ctx)

    def rephrased(
        self,
        help: Union[None, str, HelpRephraser] = None,
        error: Union[None, str, ErrorRephraser] = None,
    ) -> 'Rephraser':
        """
        Override the help string and/or the error message of this constraint
        wrapping it with a :class:`Rephraser`.

        :param help:
            if provided, overrides the help string of this constraint. It can be
            a string or a function ``(ctx: click.Context, constr: Constraint) -> str``.
            If you want to hide this constraint from the help, pass ``help=""``.
        :param error:
            if provided, overrides the error message of this constraint.
            It can be:

            - a string, eventually a ``format`` string supporting the replacement
              fields described in :class:`ErrorFmt`.

            - or a function ``(err: ConstraintViolated) -> str``; note that
              a :class:`ConstraintViolated` error has fields for ``ctx``,
              ``constraint`` and ``params``, so it's a complete description
              of what happened.
        """
        return Rephraser(self, help=help, error=error)

    def hidden(self) -> 'Rephraser':
        """Hide this constraint from the command help."""
        return Rephraser(self, help='')

    def __call__(self, *param_adders: Decorator) -> Callable[[F], F]:
        """Equivalent to calling :func:`cloup.constrained_params` with this
        constraint as first argument.

        .. versionchanged:: 0.9.0
            this method, previously equivalent to :meth:`~Constraint.check`, is
            now equivalent to calling :func:`cloup.constrained_params` with this
            constraint as first argument.
        """
        from ._support import constrained_params
        # TODO: remove this check in the future
        if not callable(param_adders[0]):
            from cloup import __version__
            raise TypeError(reindent(f"""\n
                since Cloup v0.9, calling a constraint has a completely different
                semantics and takes parameter decorators as arguments, see:

                https://cloup.readthedocs.io/en/v{__version__}/pages/constraints.html#constraints-as-decorators

                To check a constraint imperatively, you can use the check() method.
            """, 4))
        return constrained_params(self, *param_adders)

    def __or__(self, other: 'Constraint') -> 'Or':
        return Or(self, other)

    def __and__(self, other: 'Constraint') -> 'And':
        return And(self, other)

    def __repr__(self) -> str:
        return f'{class_name(self)}()'


class Operator(Constraint, abc.ABC):
    """Base class for all n-ary operators defined on constraints. """

    HELP_SEP: str
    """Used as separator of all constraints' help strings."""

    def __init__(self, *constraints: Constraint):
        """N-ary operator for constraints.
        :param constraints: operands
        """
        self.constraints = constraints

    def help(self, ctx: click.Context) -> str:
        return self.HELP_SEP.join(
            '(%s)' % c.help(ctx) if isinstance(c, Operator) else c.help(ctx)
            for c in self.constraints
        )

    def check_consistency(self, params: Sequence[click.Parameter]) -> None:
        for c in self.constraints:
            c.check_consistency(params)

    def __repr__(self) -> str:
        return make_repr(self, *self.constraints)


class And(Operator):
    """It's satisfied if all operands are satisfied."""
    HELP_SEP = ' and '

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        for c in self.constraints:
            c.check_values(params, ctx)

    def __and__(self, other: Constraint) -> 'And':
        if isinstance(other, And):
            return And(*self.constraints, *other.constraints)
        return And(*self.constraints, other)


class Or(Operator):
    """It's satisfied if at least one of the operands is satisfied."""
    HELP_SEP = ' or '

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        for c in self.constraints:
            try:
                c.check_values(params, ctx)
                return
            except ConstraintViolated:
                pass
        raise ConstraintViolated.default(
            self.help(ctx), ctx=ctx, constraint=self, params=params
        )

    def __or__(self, other: Constraint) -> 'Or':
        if isinstance(other, Or):
            return Or(*self.constraints, *other.constraints)
        return Or(*self.constraints, other)


class ErrorFmt(FrozenSpace):
    """:class:`Rephraser` allows you to pass a ``format`` string as ``error``
    argument; this class contains the "replacement fields" supported by such
    format string. You can use them as following::

        mutually_exclusive.rephrased(
            error=f"{ErrorFmt.error}\\n"
                  f"Some extra information here."
        )
    """

    error = '{error}'
    """Replaced by the original error message. Useful if all you want is to
    append or prepend some extra info to the original error message."""

    param_list = '{param_list}'
    """Replaced by a 2-space indented list of the constrained parameters."""


class Rephraser(Constraint):
    """A constraint decorator that can override the help and/or the error
    message of the wrapped constraint.

    You'll rarely (if ever) use this class directly. In most cases, you'll use
    the method :meth:`Constraint.rephrased`. Refer to it for more info.

    .. seealso::

        - :meth:`Constraint.rephrased` -- wraps a constraint with a ``Rephraser``.
        - :class:`WrapperConstraint` -- alternative to ``Rephraser``.
        - :class:`ErrorFmt` -- describes the keyword you can use in an error
          format string.
    """

    def __init__(
        self, constraint: Constraint,
        help: Union[None, str, HelpRephraser] = None,
        error: Union[None, str, ErrorRephraser] = None,
    ):
        if help is None and error is None:
            raise ValueError('`help` and `error` cannot both be `None`')
        self.constraint = constraint
        self._help = help
        self._error = error

    def help(self, ctx: click.Context) -> str:
        if self._help is None:
            return self.constraint.help(ctx)
        elif isinstance(self._help, str):
            return self._help
        else:
            return self._help(ctx, self.constraint)

    def _get_rephrased_error(self, err: ConstraintViolated) -> Optional[str]:
        if self._error is None:
            return None
        elif isinstance(self._error, str):
            return self._error.format(
                error=str(err),
                param_list=format_param_list(err.params),
            )
        else:
            return self._error(err)

    def check_consistency(self, params: Sequence[click.Parameter]) -> None:
        try:
            self.constraint.check_consistency(params)
        except UnsatisfiableConstraint as exc:
            raise UnsatisfiableConstraint(
                self, params=params, reason=exc.reason)

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        try:
            self.constraint.check_values(params, ctx)
        except ConstraintViolated as err:
            rephrased_error = self._get_rephrased_error(err)
            if rephrased_error:
                raise ConstraintViolated(
                    rephrased_error, ctx=ctx, constraint=self, params=params)
            raise

    def __repr__(self) -> str:
        return make_one_line_repr(self, help=self._help)


class WrapperConstraint(Constraint, metaclass=abc.ABCMeta):
    """Abstract class that wraps another constraint and delegates all methods
    to it. Useful when you want to define a parametric constraint combining
    other existing constraints minimizing the boilerplate.

    This is an alternative to defining a function and using :class:`Rephraser`.
    Feel free to do that in your code, but cloup will stick to the convention
    that parametric constraints are defined as classes and written in
    camel-case."""

    def __init__(self, constraint: Constraint, **attrs: Any):
        """
        :param constraint: the constraint to wrap
        :param attrs: these are just used to generate a ``__repr__`` method
        """
        self._constraint = constraint
        self._attrs = attrs

    def help(self, ctx: click.Context) -> str:
        return self._constraint.help(ctx)

    def check_consistency(self, params: Sequence[click.Parameter]) -> None:
        try:
            self._constraint.check_consistency(params)
        except UnsatisfiableConstraint as exc:
            raise UnsatisfiableConstraint(self, params=params, reason=exc.reason)

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        self._constraint.check_values(params, ctx)

    def __repr__(self) -> str:
        return make_repr(self, **self._attrs)


class _RequireAll(Constraint):
    """Satisfied if all parameters are set."""

    def help(self, ctx: click.Context) -> str:
        return 'all required'

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        values = ctx.params
        unset_params = [param for param in params
                        if not param_value_is_set(param, values[get_param_name(param)])]
        if any(unset_params):
            raise ConstraintViolated(
                pluralize(
                    len(unset_params),
                    one=f"{get_param_label(unset_params[0])} is required",
                    many=f"the following parameters are required:\n"
                         f"{format_param_list(unset_params)}"),
                ctx=ctx,
                constraint=self,
                params=params,
            )


class RequireAtLeast(Constraint):
    """Satisfied if the number of set parameters is >= n."""

    def __init__(self, n: int):
        check_arg(n >= 0)
        self.min_num_params = n

    def help(self, ctx: click.Context) -> str:
        return f'at least {self.min_num_params} required'

    def check_consistency(self, params: Sequence[click.Parameter]) -> None:
        n = self.min_num_params
        if len(params) < n:
            reason = (
                f'the constraint requires a minimum of {n} parameters but '
                f'it is applied on a group of only {len(params)} parameters!'
            )
            raise UnsatisfiableConstraint(self, params, reason)

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        n = self.min_num_params
        given_params = get_params_whose_value_is_set(params, ctx.params)
        if len(given_params) < n:
            raise ConstraintViolated(
                f"at least {n} of the following parameters must be set:\n"
                f"{format_param_list(params)}",
                ctx=ctx, constraint=self, params=params,
            )

    def __repr__(self) -> str:
        return make_repr(self, self.min_num_params)


class AcceptAtMost(Constraint):
    """Satisfied if the number of set parameters is <= n."""

    def __init__(self, n: int):
        check_arg(n >= 0)
        self.max_num_params = n

    def help(self, ctx: click.Context) -> str:
        return f'at most {self.max_num_params} accepted'

    def check_consistency(self, params: Sequence[click.Parameter]) -> None:
        num_required_params = len(get_required_params(params))
        if num_required_params > self.max_num_params:
            reason = f'{num_required_params} of the parameters are required'
            raise UnsatisfiableConstraint(self, params, reason)

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        n = self.max_num_params
        given_params = get_params_whose_value_is_set(params, ctx.params)
        if len(given_params) > n:
            raise ConstraintViolated(
                f"no more than {n} of the following parameters can be set:\n"
                f"{format_param_list(params)}",
                ctx=ctx, constraint=self, params=params,
            )

    def __repr__(self) -> str:
        return make_repr(self, self.max_num_params)


class RequireExactly(WrapperConstraint):
    """Requires an exact number of parameters to be set."""

    def __init__(self, n: int):
        check_arg(n > 0)
        # Defined as a wrapper to reuse check_consistency() of the wrapped constraint.
        super().__init__(RequireAtLeast(n) & AcceptAtMost(n))
        self.num_params = n

    def help(self, ctx: click.Context) -> str:
        return f'exactly {self.num_params} required'

    def check_values(self, params: Sequence[click.Parameter], ctx: click.Context) -> None:
        n = self.num_params
        given_params = get_params_whose_value_is_set(params, ctx.params)
        if len(given_params) != n:
            reason = pluralize(
                count=n,
                zero='none of the following parameters must be set:\n',
                many=f'exactly {n} of the following parameters must be set:\n'
            ) + format_param_list(params)
            raise ConstraintViolated(
                reason, ctx=ctx, constraint=self, params=params)

    def __repr__(self) -> str:
        return make_repr(self, self.num_params)


class AcceptBetween(WrapperConstraint):
    def __init__(self, min: int, max: int):  # noqa
        """Satisfied if the number of set parameters is between
        ``min`` and ``max`` (included).

        :param min: must be an integer >= 0
        :param max: must be an integer > min
        """
        check_arg(min >= 0, 'min must be non-negative')
        if max is not None:
            check_arg(min < max, 'must be: min < max.')
        super().__init__(RequireAtLeast(min) & AcceptAtMost(max), min=min, max=max)
        self.min_num_params = min
        self.max_num_params = max

    def help(self, ctx: click.Context) -> str:
        return f'at least {self.min_num_params} required, ' \
               f'at most {self.max_num_params} accepted'


require_all = _RequireAll()
"""Satisfied if all parameters are set."""

accept_none = AcceptAtMost(0).rephrased(
    help='all forbidden',
    error=f'the following parameters should not be provided:\n'
          f'{ErrorFmt.param_list}'
)
"""Satisfied if none of the parameters is set. Useful only in conditional constraints."""

all_or_none = (require_all | accept_none).rephrased(
    help='provide all or none',
    error=f'the following parameters should be provided together (or none of '
          f'them should be provided):\n'
          f'{ErrorFmt.param_list}',
)
"""Satisfied if either all or none of the parameters are set."""

mutually_exclusive = AcceptAtMost(1).rephrased(
    help='mutually exclusive',
    error=f'the following parameters are mutually exclusive:\n'
          f'{ErrorFmt.param_list}'
)
"""Satisfied if at most one of the parameters is set."""

require_any = RequireAtLeast(1)
"""Alias for ``RequireAtLeast(1)``."""

require_one = RequireExactly(1)
"""Alias for ``RequireExactly(1)``."""
