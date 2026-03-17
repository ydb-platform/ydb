# Asyncio DataLoader

DataLoader is a generic utility to be used as part of your application's data
fetching layer to provide a simplified and consistent API over various remote
data sources such as databases or web services via batching and caching.

[![PyPI - Version](https://img.shields.io/pypi/v/aiodataloader)](https://pypi.org/project/aiodataloader/)
![Test Status](https://github.com/syrusakbary/aiodataloader/actions/workflows/test.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/syrusakbary/aiodataloader/badge.svg?branch=master&service=github)](https://coveralls.io/github/syrusakbary/aiodataloader?branch=master)

A port of the "Loader" API originally developed by [@schrockn][] at Facebook in
2010 as a simplifying force to coalesce the sundry key-value store back-end
APIs which existed at the time. At Facebook, "Loader" became one of the
implementation details of the "Ent" framework, a privacy-aware data entity
loading and caching layer within web server product code. This ultimately became
the underpinning for Facebook's GraphQL server implementation and type
definitions.

Asyncio DataLoader is a Python port of the original JavaScript [DataLoader][]
implementation. DataLoader is often used when implementing a [GraphQL][] service,
though it is also broadly useful in other situations.


## Getting Started

First, install DataLoader using pip.

```sh
pip install aiodataloader
```

To get started, create a `DataLoader`. Each `DataLoader` instance represents a
unique cache. Typically instances are created per request when used within a
web-server like [Sanic][] if different users can see different things.

> Note: DataLoader assumes a AsyncIO environment with `async/await`
  available only in Python 3.5+.


## Batching

Batching is not an advanced feature, it's DataLoader's primary feature.
Create loaders by providing a batch loading function.

```python
from aiodataloader import DataLoader

class UserLoader(DataLoader):
    async def batch_load_fn(self, keys):
        return await my_batch_get_users(keys)

user_loader = UserLoader()
```

A batch loading function accepts an Iterable of keys, and returns a Promise which
resolves to a List of values[<sup>*</sup>](#batch-function).

Then load individual values from the loader. DataLoader will coalesce all
individual loads which occur within a single frame of execution (a single tick
of the event loop) and then call your batch function with all requested keys.

```python
user1_future = user_loader.load(1)
user2_future = user_loader.load(2)

user1 = await user1_future
user2 = await user2_future

user1_invitedby = user_loader.load(user1.invited_by_id)
user2_invitedby = user_loader.load(user2.invited_by_id)

print("User 1 was invited by", await user1_invitedby)
print("User 2 was invited by", await user2_invitedby)
```

A naive application may have issued four round-trips to a backend for the
required information, but with DataLoader this application will make at most
two.

DataLoader allows you to decouple unrelated parts of your application without
sacrificing the performance of batch data-loading. While the loader presents an
API that loads individual values, all concurrent requests will be coalesced and
presented to your batch loading function. This allows your application to safely
distribute data fetching requirements throughout your application and maintain
minimal outgoing data requests.

#### Batch Function

A batch loading function accepts an List of keys, and returns a Future which
resolves to a List of values. There are a few constraints that must be upheld:

 * The List of values must be the same length as the List of keys.
 * Each index in the List of values must correspond to the same index in the List of keys.

For example, if your batch function was provided the List of keys: `[ 2, 9, 6, 1 ]`,
and loading from a back-end service returned the values:

```python
{ 'id': 9, 'name': 'Chicago' }
{ 'id': 1, 'name': 'New York' }
{ 'id': 2, 'name': 'San Francisco' }
```

Our back-end service returned results in a different order than we requested, likely
because it was more efficient for it to do so. Also, it omitted a result for key `6`,
which we can interpret as no value existing for that key.

To uphold the constraints of the batch function, it must return an List of values
the same length as the List of keys, and re-order them to ensure each index aligns
with the original keys `[ 2, 9, 6, 1 ]`:

```python
[
  { 'id': 2, 'name': 'San Francisco' },
  { 'id': 9, 'name': 'Chicago' },
  None,
  { 'id': 1, 'name': 'New York' }
]
```


## Caching

DataLoader provides a memoization cache for all loads which occur in a single
request to your application. After `.load()` is called once with a given key,
the resulting value is cached to eliminate redundant loads.

In addition to relieving pressure on your data storage, caching results per-request
also creates fewer objects which may relieve memory pressure on your application:

```python
user_future1 = user_loader.load(1)
user_future2 = user_loader.load(1)

assert user_future1 == user_future2
```

#### Caching per-Request

DataLoader caching *does not* replace Redis, Memcache, or any other shared
application-level cache. DataLoader is first and foremost a data loading mechanism,
and its cache only serves the purpose of not repeatedly loading the same data in
the context of a single request to your Application. To do this, it maintains a
simple in-memory memoization cache (more accurately: `.load()` is a memoized function).

Avoid multiple requests from different users using the DataLoader instance, which
could result in cached data incorrectly appearing in each request. Typically,
DataLoader instances are created when a Request begins, and are not used once the
Request ends.

For example, when using with [Sanic][]:

```python
def create_loaders(auth_token) {
    return {
      'users': user_loader,
    }
}


app = Sanic(__name__)

@app.route("/")
async def test(request):
    auth_token = authenticate_user(request)
    loaders = create_loaders(auth_token)
    return render_page(request, loaders)
```

#### Clearing Cache

In certain uncommon cases, clearing the request cache may be necessary.

The most common example when clearing the loader's cache is necessary is after
a mutation or update within the same request, when a cached value could be out of
date and future loads should not use any possibly cached value.

Here's a simple example using SQL UPDATE to illustrate.

```python
# Request begins...
user_loader = ...

# And a value happens to be loaded (and cached).
user4 = await user_loader.load(4)

# A mutation occurs, invalidating what might be in cache.
await sql_run('UPDATE users WHERE id=4 SET username="zuck"')
user_loader.clear(4)

# Later the value load is loaded again so the mutated data appears.
user4 = await user_loader.load(4)

# Request completes.
```

#### Caching Exceptions

If a batch load fails (that is, a batch function throws or returns a rejected
Promise), then the requested values will not be cached. However if a batch
function returns an `Exception` instance for an individual value, that `Exception` will
be cached to avoid frequently loading the same `Exception`.

In some circumstances you may wish to clear the cache for these individual Errors:

```python
try:
    user_loader.load(1)
except Exception as e:
    user_loader.clear(1)
    raise
```

#### Disabling Cache

In certain uncommon cases, a DataLoader which *does not* cache may be desirable.
Calling `DataLoader(batch_fn, cache=false)` will ensure that every
call to `.load()` will produce a *new* Future, and requested keys will not be
saved in memory.

However, when the memoization cache is disabled, your batch function will
receive an array of keys which may contain duplicates! Each key will be
associated with each call to `.load()`. Your batch loader should provide a value
for each instance of the requested key.

For example:

```python
class MyLoader(DataLoader):
    cache = False
    async def batch_load_fn(self, keys):
        print(keys)
        return keys

my_loader = MyLoader()

my_loader.load('A')
my_loader.load('B')
my_loader.load('A')

# > [ 'A', 'B', 'A' ]
```

More complex cache behavior can be achieved by calling `.clear()` or `.clear_all()`
rather than disabling the cache completely. For example, this DataLoader will
provide unique keys to a batch function due to the memoization cache being
enabled, but will immediately clear its cache when the batch function is called
so later requests will load new values.

```python
class MyLoader(DataLoader):
    cache = False
    async def batch_load_fn(self, keys):
        self.clear_all()
        return keys
```


## API

#### class DataLoader

DataLoader creates a public API for loading data from a particular
data back-end with unique keys such as the `id` column of a SQL table or
document name in a MongoDB database, given a batch loading function.

Each `DataLoader` instance contains a unique memoized cache. Use caution when
used in long-lived applications or those which serve many users with different
access permissions and consider creating a new instance per web request.

##### `DataLoader(batch_load_fn, **options)`

Create a new `DataLoader` given a batch loading function and options.

- *batch_load_fn*: An async function (coroutine) which accepts an List of keys
  and returns a Future which resolves to an List of values.

- *options*:

  - *batch*: Default `True`. Set to `False` to disable batching, instead
    immediately invoking `batch_load_fn` with a single load key.

  - *max_batch_size*: Default `Infinity`. Limits the number of items that get
    passed in to the `batch_load_fn`.

  - *cache*: Default `True`. Set to `False` to disable memoization caching,
    instead creating a new Promise and new key in the `batch_load_fn` for every
    load of the same key.

  - *cache_key_fn*: A function to produce a cache key for a given load key.
    Defaults to `key => key`. Useful to provide when Python objects are keys
    and two similarly shaped objects should be considered equivalent.

  - *cache_map*: An instance of [dict][] (or an object with a similar API) to be
    used as the underlying cache for this loader. Default `{}`.

##### `load(key)`

Loads a key, returning a `Future` for the value represented by that key.

- *key*: An key value to load.

##### `load_many(keys)`

Loads multiple keys, promising an array of values:

```python
a, b = await my_loader.load_many([ 'a', 'b' ]);
```

This is equivalent to the more verbose:

```python
from asyncio import gather
a, b = await gather(
    my_loader.load('a'),
    my_loader.load('b')
)
```

- *keys*: A list of key values to load.

##### `clear(key)`

Clears the value at `key` from the cache, if it exists. Returns itself for
method chaining.

- *key*: An key value to clear.

##### `clear_all()`

Clears the entire cache. To be used when some event results in unknown
invalidations across this particular `DataLoader`. Returns itself for
method chaining.

##### `prime(key, value)`

Primes the cache with the provided key and value. If the key already exists, no
change is made. (To forcefully prime the cache, clear the key first with
`loader.clear(key).prime(key, value)`.) Returns itself for method chaining.


## Using with GraphQL

DataLoader pairs nicely well with [GraphQL][]. GraphQL fields are
designed to be stand-alone functions. Without a caching or batching mechanism,
it's easy for a naive GraphQL server to issue new database requests each time a
field is resolved.

Consider the following GraphQL request:

```
{
  me {
    name
    bestFriend {
      name
    }
    friends(first: 5) {
      name
      bestFriend {
        name
      }
    }
  }
}
```

Naively, if `me`, `bestFriend` and `friends` each need to request the backend,
there could be at most 13 database requests!

When using DataLoader with [graphene][], we could define the `User` type with clearer code and
at most 4 database requests, and possibly fewer if there are cache hits.

```python
class User(graphene.ObjectType):
    name = graphene.String()
    best_friend = graphene.Field(lambda: User)
    friends = graphene.List(lambda: User)

    def resolve_best_friend(self, args, context, info):
        return user_loader.load(self.best_friend_id)

    def resolve_friends(self, args, context, info):
        return user_loader.load_many(self.friend_ids)
```


## Common Patterns

### Creating a new DataLoader per request.

In many applications, a web server using DataLoader serves requests to many
different users with different access permissions. It may be dangerous to use
one cache across many users, and is encouraged to create a new DataLoader
per request:

```python
def create_loaders(auth_token):
  return {
    'users': DataLoader(lambda ids: gen_users(auth_token, ids)),
    'cdn_urls': DataLoader(lambda raw_urls: gen_cdn_urls(auth_token, raw_urls)),
    'stories': DataLoader(lambda keys: gen_stories(auth_token, keys)),
  }
}

# When handling an incoming web request:
loaders = create_loaders(request.query.auth_token)

# Then, within application logic:
user = await loaders.users.load(4)
pic = await loaders.cdn_urls.load(user.raw_pic_url)
```

Creating an object where each key is a `DataLoader` is one common pattern which
provides a single value to pass around to code which needs to perform
data loading, such as part of the `root_value` in a [GraphQL][] request.

### Loading by alternative keys.

Occasionally, some kind of value can be accessed in multiple ways. For example,
perhaps a "User" type can be loaded not only by an "id" but also by a "username"
value. If the same user is loaded by both keys, then it may be useful to fill
both caches when a user is loaded from either source:

```python
async def user_by_id_batch_fn(ids):
    users = await gen_users_by_id(ids)
    for user in users:
        username_loader.prime(user.username, user)
    return users

user_by_id_loader = DataLoader(user_by_id_batch_fn)

async def username_batch_fn(names):
    users = await gen_usernames(names)
    for user in users:
        user_by_id_loader.prime(user.id, user)
    return users

username_loader = DataLoader(username_batch_fn)
```


## Custom Caches

DataLoader can optionaly be provided a custom dict instance to use as its
memoization cache. More specifically, any object that implements the methods `get()`,
`set()`, `delete()` and `clear()` can be provided. This allows for custom dicts
which implement various [cache algorithms][] to be provided. By default,
DataLoader uses the standard [dict][] which simply grows until the DataLoader
is released. The default is appropriate when requests to your application are
short-lived.



## Video Source Code Walkthrough

**DataLoader Source Code Walkthrough (YouTube):**

<a href="https://youtu.be/OQTnXNCDywA" target="_blank" alt="DataLoader Source Code Walkthrough"><img src="https://img.youtube.com/vi/OQTnXNCDywA/0.jpg" /></a>


[@schrockn]: https://github.com/schrockn
[DataLoader]: https://github.com/graphql/dataloader
[GraphQL]: https://graphql.org
[dict]: https://docs.python.org/3/tutorial/datastructures.html#dictionaries
[graphene]: https://github.com/graphql-python/graphene
[graphql-core]: https://github.com/graphql-python/graphql-core
[cache algorithms]: https://en.wikipedia.org/wiki/Cache_algorithms
[Sanic]: https://sanic.readthedocs.io/en/latest/
