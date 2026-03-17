[![Returns logo](https://raw.githubusercontent.com/dry-python/brand/master/logo/returns_white-outline.png)](https://github.com/dry-python/returns)

-----

[![test](https://github.com/dry-python/returns/actions/workflows/test.yml/badge.svg?branch=master&event=push)](https://github.com/dry-python/returns/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/dry-python/returns/branch/master/graph/badge.svg)](https://codecov.io/gh/dry-python/returns)
[![Documentation Status](https://readthedocs.org/projects/returns/badge/?version=latest)](https://returns.readthedocs.io/en/latest/?badge=latest)
[![Python Version](https://img.shields.io/pypi/pyversions/returns.svg)](https://pypi.org/project/returns/)
[![conda](https://img.shields.io/conda/v/conda-forge/returns?label=conda)](https://anaconda.org/conda-forge/returns)
[![wemake-python-styleguide](https://img.shields.io/badge/style-wemake-000000.svg)](https://github.com/wemake-services/wemake-python-styleguide)
[![Telegram chat](https://img.shields.io/badge/chat-join-blue?logo=telegram)](https://t.me/drypython)

-----

Make your functions return something meaningful, typed, and safe!


## Features

- Brings functional programming to Python land
- Provides a bunch of primitives to write declarative business logic
- Enforces better architecture
- Fully typed with annotations and checked with `mypy`, [PEP561 compatible](https://www.python.org/dev/peps/pep-0561/)
- Adds emulated [Higher Kinded Types](https://returns.readthedocs.io/en/latest/pages/hkt.html) support
- Provides type-safe interfaces to create your own data-types with enforced laws
- Has a bunch of helpers for better composition
- Pythonic and pleasant to write and to read 🐍
- Support functions and coroutines, framework agnostic
- Easy to start: has lots of docs, tests, and tutorials

[Quickstart](https://returns.readthedocs.io/en/latest/pages/quickstart.html) right now!


## Installation

```bash
pip install returns
```

You can also install `returns` with the latest supported `mypy` version:

```bash
pip install returns[compatible-mypy]
```

You would also need to configure our [`mypy` plugin](https://returns.readthedocs.io/en/latest/pages/contrib/mypy_plugins.html):

```ini
# In setup.cfg or mypy.ini:
[mypy]
plugins =
  returns.contrib.mypy.returns_plugin
```

or:

```toml
[tool.mypy]
plugins = ["returns.contrib.mypy.returns_plugin"]
```

We also recommend to use the same `mypy` settings we use, which you'll find in the `[tool.mypy]` sections in our [pyproject.toml](https://github.com/wemake-services/wemake-python-styleguide/blob/master/pyproject.toml) file.

Make sure you know how to get started, [check out our docs](https://returns.readthedocs.io/en/latest/)!
[Try our demo](https://repl.it/@sobolevn/returns#ex.py).


## Contents

- [Maybe container](#maybe-container) that allows you to write `None`-free code
- [RequiresContext container](#requirescontext-container) that allows you to use typed functional dependency injection
- [Result container](#result-container) that lets you to get rid of exceptions
- [IO container](#io-container) and [IOResult](#troublesome-io) that marks all impure operations and structures them
- [Future container](#future-container) and [FutureResult](#async-code-without-exceptions) to work with `async` code
- [Write your own container!](https://returns.readthedocs.io/en/latest/pages/create-your-own-container.html) You would still have all the features for your own types (including full existing code reuse and type-safety)
- Use [`do-notation`](https://returns.readthedocs.io/en/latest/pages/do-notation.html) to make your code easier!


## Maybe container

`None` is called the [worst mistake in the history of Computer Science](https://www.infoq.com/presentations/Null-References-The-Billion-Dollar-Mistake-Tony-Hoare/).

So, what can we do to check for `None` in our programs?
You can use builtin [Optional](https://mypy.readthedocs.io/en/stable/kinds_of_types.html#optional-types-and-the-none-type) type
and write a lot of `if some is not None:` conditions.
But, **having `null` checks here and there makes your code unreadable**.

```python
user: Optional[User]
discount_program: Optional['DiscountProgram'] = None

if user is not None:
     balance = user.get_balance()
     if balance is not None:
         credit = balance.credit_amount()
         if credit is not None and credit > 0:
             discount_program = choose_discount(credit)
```

Or you can use
[Maybe](https://returns.readthedocs.io/en/latest/pages/maybe.html) container!
It consists of `Some` and `Nothing` types,
representing existing state and empty (instead of `None`) state respectively.

```python
from typing import Optional
from returns.maybe import Maybe, maybe

@maybe  # decorator to convert existing Optional[int] to Maybe[int]
def bad_function() -> Optional[int]:
    ...

maybe_number: Maybe[float] = bad_function().bind_optional(
    lambda number: number / 2,
)
# => Maybe will return Some[float] only if there's a non-None value
#    Otherwise, will return Nothing
```

You can be sure that `.bind_optional()` method won't be called for `Nothing`.
Forget about `None`-related errors forever!

We can also bind a `Optional`-returning function over a container.
To achieve this, we are going to use `.bind_optional` method.

And here's how your initial refactored code will look:

```python
user: Optional[User]

# Type hint here is optional, it only helps the reader here:
discount_program: Maybe['DiscountProgram'] = Maybe.from_optional(
    user,
).bind_optional(  # This won't be called if `user is None`
    lambda real_user: real_user.get_balance(),
).bind_optional(  # This won't be called if `real_user.get_balance()` is None
    lambda balance: balance.credit_amount(),
).bind_optional(  # And so on!
    lambda credit: choose_discount(credit) if credit > 0 else None,
)
```

Much better, isn't it?


## RequiresContext container

Many developers do use some kind of dependency injection in Python.
And usually it is based on the idea
that there's some kind of a container and assembly process.

Functional approach is much simpler!

Imagine that you have a `django` based game, where you award users with points for each guessed letter in a word (unguessed letters are marked as `'.'`):

```python
from django.http import HttpRequest, HttpResponse
from words_app.logic import calculate_points

def view(request: HttpRequest) -> HttpResponse:
    user_word: str = request.POST['word']  # just an example
    points = calculate_points(user_word)
    ...  # later you show the result to user somehow

# Somewhere in your `words_app/logic.py`:

def calculate_points(word: str) -> int:
    guessed_letters_count = len([letter for letter in word if letter != '.'])
    return _award_points_for_letters(guessed_letters_count)

def _award_points_for_letters(guessed: int) -> int:
    return 0 if guessed < 5 else guessed  # minimum 6 points possible!
```

Awesome! It works, users are happy, your logic is pure and awesome.
But, later you decide to make the game more fun:
let's make the minimal accountable letters threshold
configurable for an extra challenge.

You can just do it directly:

```python
def _award_points_for_letters(guessed: int, threshold: int) -> int:
    return 0 if guessed < threshold else guessed
```

The problem is that `_award_points_for_letters` is deeply nested.
And then you have to pass `threshold` through the whole callstack,
including `calculate_points` and all other functions that might be on the way.
All of them will have to accept `threshold` as a parameter!
This is not useful at all!
Large code bases will struggle a lot from this change.

Ok, you can directly use `django.settings` (or similar)
in your `_award_points_for_letters` function.
And **ruin your pure logic with framework specific details**. That's ugly!

Or you can use `RequiresContext` container. Let's see how our code changes:

```python
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from words_app.logic import calculate_points

def view(request: HttpRequest) -> HttpResponse:
    user_word: str = request.POST['word']  # just an example
    points = calculate_points(user_word)(settings)  # passing the dependencies
    ...  # later you show the result to user somehow

# Somewhere in your `words_app/logic.py`:

from typing import Protocol
from returns.context import RequiresContext

class _Deps(Protocol):  # we rely on abstractions, not direct values or types
    WORD_THRESHOLD: int

def calculate_points(word: str) -> RequiresContext[int, _Deps]:
    guessed_letters_count = len([letter for letter in word if letter != '.'])
    return _award_points_for_letters(guessed_letters_count)

def _award_points_for_letters(guessed: int) -> RequiresContext[int, _Deps]:
    return RequiresContext(
        lambda deps: 0 if guessed < deps.WORD_THRESHOLD else guessed,
    )
```

And now you can pass your dependencies in a really direct and explicit way.
And have the type-safety to check what you pass to cover your back.
Check out [RequiresContext](https://returns.readthedocs.io/en/latest/pages/context.html) docs for more. There you will learn how to make `'.'` also configurable.

We also have [RequiresContextResult](https://returns.readthedocs.io/en/latest/pages/context.html#requirescontextresult-container)
for context-related operations that might fail. And also [RequiresContextIOResult](https://returns.readthedocs.io/en/latest/pages/context.html#requirescontextioresult-container) and [RequiresContextFutureResult](https://returns.readthedocs.io/en/latest/pages/context.html#requirescontextfutureresult-container).


## Result container

Please, make sure that you are also aware of
[Railway Oriented Programming](https://fsharpforfunandprofit.com/rop/).

### Straight-forward approach

Consider this code that you can find in **any** `python` project.

```python
import requests

def fetch_user_profile(user_id: int) -> 'UserProfile':
    """Fetches UserProfile dict from foreign API."""
    response = requests.get('/api/users/{0}'.format(user_id))
    response.raise_for_status()
    return response.json()
```

Seems legit, does it not?
It also seems like a pretty straightforward code to test.
All you need is to mock `requests.get` to return the structure you need.

But, there are hidden problems in this tiny code sample
that are almost impossible to spot at the first glance.

### Hidden problems

Let's have a look at the exact same code,
but with the all hidden problems explained.

```python
import requests

def fetch_user_profile(user_id: int) -> 'UserProfile':
    """Fetches UserProfile dict from foreign API."""
    response = requests.get('/api/users/{0}'.format(user_id))

    # What if we try to find user that does not exist?
    # Or network will go down? Or the server will return 500?
    # In this case the next line will fail with an exception.
    # We need to handle all possible errors in this function
    # and do not return corrupt data to consumers.
    response.raise_for_status()

    # What if we have received invalid JSON?
    # Next line will raise an exception!
    return response.json()
```

Now, all (probably all?) problems are clear.
How can we be sure that this function will be safe
to use inside our complex business logic?

We really cannot be sure!
We will have to create **lots** of `try` and `except` cases
just to catch the expected exceptions. Our code will become complex and unreadable with all this mess!

Or we can go with the top level `except Exception:` case
to catch literally everything.
And this way we would end up with catching unwanted ones.
This approach can hide serious problems from us for a long time.

### Pipe example

```python
import requests
from returns.result import Result, safe
from returns.pipeline import flow
from returns.pointfree import bind

def fetch_user_profile(user_id: int) -> Result['UserProfile', Exception]:
    """Fetches `UserProfile` TypedDict from foreign API."""
    return flow(
        user_id,
        _make_request,
        bind(_parse_json),
    )

@safe
def _make_request(user_id: int) -> requests.Response:
    # TODO: we are not yet done with this example, read more about `IO`:
    response = requests.get('/api/users/{0}'.format(user_id))
    response.raise_for_status()
    return response

@safe
def _parse_json(response: requests.Response) -> 'UserProfile':
    return response.json()
```

Now we have a clean and a safe and declarative way
to express our business needs:

- We start from making a request, that might fail at any moment,
- Then parsing the response if the request was successful,
- And then return the result.

Now, instead of returning regular values
we return values wrapped inside a special container
thanks to the
[@safe](https://returns.readthedocs.io/en/latest/pages/result.html#safe)
decorator. It will return [Success[YourType] or Failure[Exception]](https://returns.readthedocs.io/en/latest/pages/result.html).
And will never throw exception at us!

We also use [flow](https://returns.readthedocs.io/en/latest/pages/pipeline.html#flow)
and [bind](https://returns.readthedocs.io/en/latest/pages/pointfree.html#bind)
functions for handy and declarative composition.

This way we can be sure that our code won't break in
random places due to some implicit exception.
Now we control all parts and are prepared for the explicit errors.

We are not yet done with this example,
let's continue to improve it in the next chapter.


## IO container

Let's look at our example from another angle.
All its functions look like regular ones:
it is impossible to tell whether they are [pure](https://en.wikipedia.org/wiki/Pure_function)
or impure from the first sight.

It leads to a very important consequence:
*we start to mix pure and impure code together*.
We should not do that!

When these two concepts are mixed
we suffer really bad when testing or reusing it.
Almost everything should be pure by default.
And we should explicitly mark impure parts of the program.

That's why we have created `IO` container
to mark impure functions that never fail.

These impure functions use `random`, current datetime, environment, or console:

```python
import random
import datetime as dt

from returns.io import IO

def get_random_number() -> IO[int]:  # or use `@impure` decorator
    return IO(random.randint(1, 10))  # isn't pure, because random

now: Callable[[], IO[dt.datetime]] = impure(dt.datetime.now)

@impure
def return_and_show_next_number(previous: int) -> int:
    next_number = previous + 1
    print(next_number)  # isn't pure, because does IO
    return next_number
```

Now we can clearly see which functions are pure and which ones are impure.
This helps us a lot in building large applications, unit testing you code,
and composing business logic together.

### Troublesome IO

As it was already said, we use `IO` when we handle functions that do not fail.

What if our function can fail and is impure?
Like `requests.get()` we had earlier in our example.

Then we have to use a special `IOResult` type instead of a regular `Result`.
Let's find the difference:

- Our `_parse_json` function always returns
  the same result (hopefully) for the same input:
  you can either parse valid `json` or fail on invalid one.
  That's why we return pure `Result`, there's no `IO` inside
- Our `_make_request` function is impure and can fail.
  Try to send two similar requests with and without internet connection.
  The result will be different for the same input.
  That's why we must use `IOResult` here: it can fail and has `IO`

So, in order to fulfill our requirement and separate pure code from impure one,
we have to refactor our example.

### Explicit IO

Let's make our [IO](https://returns.readthedocs.io/en/latest/pages/io.html)
explicit!

```python
import requests
from returns.io import IOResult, impure_safe
from returns.result import safe
from returns.pipeline import flow
from returns.pointfree import bind_result

def fetch_user_profile(user_id: int) -> IOResult['UserProfile', Exception]:
    """Fetches `UserProfile` TypedDict from foreign API."""
    return flow(
        user_id,
        _make_request,
        # before: def (Response) -> UserProfile
        # after safe: def (Response) -> ResultE[UserProfile]
        # after bind_result: def (IOResultE[Response]) -> IOResultE[UserProfile]
        bind_result(_parse_json),
    )

@impure_safe
def _make_request(user_id: int) -> requests.Response:
    response = requests.get('/api/users/{0}'.format(user_id))
    response.raise_for_status()
    return response

@safe
def _parse_json(response: requests.Response) -> 'UserProfile':
    return response.json()
```

And later we can use [unsafe_perform_io](https://returns.readthedocs.io/en/latest/pages/io.html#unsafe-perform-io)
somewhere at the top level of our program to get the pure (or "real") value.

As a result of this refactoring session, we know everything about our code:

- Which parts can fail,
- Which parts are impure,
- How to compose them in a smart, readable, and typesafe manner.


## Future container

There are several issues with `async` code in Python:

1. You cannot call `async` function from a sync one
2. Any unexpectedly thrown exception can ruin your whole event loop
3. Ugly composition with lots of `await` statements

`Future` and `FutureResult` containers solve these issues!

### Mixing sync and async code

The main feature of [Future](https://returns.readthedocs.io/en/latest/pages/future.html)
is that it allows to run async code
while maintaining sync context. Let's see an example.

Let's say we have two functions,
the `first` one returns a number and the `second` one increments it:

```python
async def first() -> int:
    return 1

def second():  # How can we call `first()` from here?
    return first() + 1  # Boom! Don't do this. We illustrate a problem here.
```

If we try to just run `first()`, we will just create an unawaited coroutine.
It won't return the value we want.

But, if we would try to run `await first()`,
then we would need to change `second` to be `async`.
And sometimes it is not possible for various reasons.

However, with `Future` we can "pretend" to call async code from sync code:

```python
from returns.future import Future

def second() -> Future[int]:
    return Future(first()).map(lambda num: num + 1)
```

Without touching our `first` async function
or making `second` async we have achieved our goal.
Now, our async value is incremented inside a sync function.

However, `Future` still requires to be executed inside a proper eventloop:

```python
import anyio  # or asyncio, or any other lib

# We can then pass our `Future` to any library: asyncio, trio, curio.
# And use any event loop: regular, uvloop, even a custom one, etc
assert anyio.run(second().awaitable) == 2
```

As you can see `Future` allows you
to work with async functions from a sync context.
And to mix these two realms together.
Use raw `Future` for operations that cannot fail or raise exceptions.
Pretty much the same logic we had with our `IO` container.

### Async code without exceptions

We have already covered how [`Result`](#result-container) works
for both pure and impure code.
The main idea is: we don't raise exceptions, we return them.
It is **especially** critical in async code,
because a single exception can ruin
all our coroutines running in a single eventloop.

We have a handy combination of `Future` and `Result` containers: `FutureResult`.
Again, this is exactly like `IOResult`, but for impure async code.
Use it when your `Future` might have problems:
like HTTP requests or filesystem operations.

You can easily turn any wild throwing coroutine into a calm `FutureResult`:

```python
import anyio
from returns.future import future_safe
from returns.io import IOFailure

@future_safe
async def raising():
    raise ValueError('Not so fast!')

ioresult = anyio.run(raising.awaitable)  # all `Future`s return IO containers
assert ioresult == IOFailure(ValueError('Not so fast!'))  # True
```

Using `FutureResult` will keep your code safe from exceptions.
You can always `await` or execute inside an eventloop any `FutureResult`
to get sync `IOResult` instance to work with it in a sync manner.

### Better async composition

Previously, you had to do quite a lot of `await`ing while writing `async` code:

```python
async def fetch_user(user_id: int) -> 'User':
    ...

async def get_user_permissions(user: 'User') -> 'Permissions':
    ...

async def ensure_allowed(permissions: 'Permissions') -> bool:
    ...

async def main(user_id: int) -> bool:
    # Also, don't forget to handle all possible errors with `try / except`!
    user = await fetch_user(user_id)  # We will await each time we use a coro!
    permissions = await get_user_permissions(user)
    return await ensure_allowed(permissions)
```

Some people are ok with it, but some people don't like this imperative style.
The problem is that there was no choice.

But now, you can do the same thing in functional style!
With the help of `Future` and `FutureResult` containers:

```python
import anyio
from returns.future import FutureResultE, future_safe
from returns.io import IOSuccess, IOFailure

@future_safe
async def fetch_user(user_id: int) -> 'User':
    ...

@future_safe
async def get_user_permissions(user: 'User') -> 'Permissions':
    ...

@future_safe
async def ensure_allowed(permissions: 'Permissions') -> bool:
    ...

def main(user_id: int) -> FutureResultE[bool]:
    # We can now turn `main` into a sync function, it does not `await` at all.
    # We also don't care about exceptions anymore, they are already handled.
    return fetch_user(user_id).bind(get_user_permissions).bind(ensure_allowed)

correct_user_id: int  # has required permissions
banned_user_id: int  # does not have required permissions
wrong_user_id: int  # does not exist

# We can have correct business results:
assert anyio.run(main(correct_user_id).awaitable) == IOSuccess(True)
assert anyio.run(main(banned_user_id).awaitable) == IOSuccess(False)

# Or we can have errors along the way:
assert anyio.run(main(wrong_user_id).awaitable) == IOFailure(
    UserDoesNotExistError(...),
)
```

Or even something really fancy:

```python
from returns.pointfree import bind
from returns.pipeline import flow

def main(user_id: int) -> FutureResultE[bool]:
    return flow(
        fetch_user(user_id),
        bind(get_user_permissions),
        bind(ensure_allowed),
    )
```

Later we can also refactor our logical functions to be sync
and to return `FutureResult`.

Lovely, isn't it?


## More!

Want more?
[Go to the docs!](https://returns.readthedocs.io)
Or read these articles:

- [Python exceptions considered an anti-pattern](https://sobolevn.me/2019/02/python-exceptions-considered-an-antipattern)
- [Enforcing Single Responsibility Principle in Python](https://sobolevn.me/2019/03/enforcing-srp)
- [Typed functional Dependency Injection in Python](https://sobolevn.me/2020/02/typed-functional-dependency-injection)
- [How Async Should Have Been](https://sobolevn.me/2020/06/how-async-should-have-been)
- [Higher Kinded Types in Python](https://sobolevn.me/2020/10/higher-kinded-types-in-python)
- [Make Tests a Part of Your App](https://sobolevn.me/2021/02/make-tests-a-part-of-your-app)

Do you have an article to submit? Feel free to open a pull request!
