import hashlib
import time

from flask import make_response
from flask import request
from flask.views import View

from flask_caching import CachedResponse


def test_cached_view(app, cache):
    @app.route("/")
    @cache.cached(2)
    def cached_view():
        return str(time.time())

    tc = app.test_client()

    rv = tc.get("/")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/")

    assert the_time == rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/")
    assert the_time != rv.data.decode("utf-8")


def test_cached_view_class(app, cache):
    class CachedView(View):
        @cache.cached(2)
        def dispatch_request(self):
            return str(time.time())

    app.add_url_rule("/", view_func=CachedView.as_view("name"))

    tc = app.test_client()

    rv = tc.get("/")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/")

    assert the_time == rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/")
    assert the_time != rv.data.decode("utf-8")


def test_async_cached_view(app, cache):
    import asyncio
    import sys

    if sys.version_info < (3, 7):
        return

    @app.route("/test-async")
    @cache.cached(2)
    async def cached_async_view():
        await asyncio.sleep(0.1)
        return str(time.time())

    tc = app.test_client()
    rv = tc.get("/test-async")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/test-async")
    assert the_time == rv.data.decode("utf-8")


def test_cached_view_unless(app, cache):
    @app.route("/a")
    @cache.cached(5, unless=lambda: True)
    def non_cached_view():
        return str(time.time())

    @app.route("/b")
    @cache.cached(5, unless=lambda: False)
    def cached_view():
        return str(time.time())

    tc = app.test_client()

    rv = tc.get("/a")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/a")
    assert the_time != rv.data.decode("utf-8")

    rv = tc.get("/b")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)
    rv = tc.get("/b")

    assert the_time == rv.data.decode("utf-8")


def test_cached_view_response_filter(app, cache):
    @app.route("/a")
    @cache.cached(5, response_filter=lambda x: x[1] < 400)
    def cached_view():
        return (str(time.time()), app.return_code)

    tc = app.test_client()

    # 500 response does not cache
    app.return_code = 500
    rv = tc.get("/a")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/a")
    assert the_time != rv.data.decode("utf-8")

    # 200 response caches
    app.return_code = 200
    rv = tc.get("/a")
    the_time = rv.data.decode("utf-8")

    time.sleep(1)

    rv = tc.get("/a")
    assert the_time == rv.data.decode("utf-8")


def test_cached_view_forced_update(app, cache):
    forced_update = False

    @app.route("/a")
    @cache.cached(5, forced_update=lambda: forced_update)
    def view():
        return str(time.time())

    tc = app.test_client()

    rv = tc.get("/a")
    the_time = rv.data.decode("utf-8")
    time.sleep(1)
    rv = tc.get("/a")
    assert the_time == rv.data.decode("utf-8")

    forced_update = True
    rv = tc.get("/a")
    new_time = rv.data.decode("utf-8")
    assert new_time != the_time

    forced_update = False
    time.sleep(1)
    rv = tc.get("/a")
    assert new_time == rv.data.decode("utf-8")


def test_generate_cache_key_from_different_view(app, cache):
    @app.route("/cake/<flavor>")
    @cache.cached()
    def view_cake(flavor):
        # What's the cache key for apple cake? thanks for making me hungry
        view_cake.cake_cache_key = view_cake.make_cache_key("apple")
        return str(time.time())

    view_cake.cake_cache_key = ""

    @app.route("/pie/<flavor>")
    @cache.cached()
    def view_pie(flavor):
        # What's the cache key for apple cake?
        view_pie.cake_cache_key = view_cake.make_cache_key("apple")
        return str(time.time())

    view_pie.cake_cache_key = ""

    tc = app.test_client()
    tc.get("/cake/chocolate")
    tc.get("/pie/chocolate")

    assert view_cake.cake_cache_key == view_pie.cake_cache_key


# rename/move to seperate module?
def test_cache_key_property(app, cache):
    @app.route("/")
    @cache.cached(5)
    def cached_view():
        return str(time.time())

    assert hasattr(cached_view, "make_cache_key")
    assert callable(cached_view.make_cache_key)

    tc = app.test_client()

    rv = tc.get("/")
    the_time = rv.data.decode("utf-8")

    with app.test_request_context():
        cache_data = cache.get(cached_view.make_cache_key())
        assert the_time == cache_data


def test_set_make_cache_key_property(app, cache):
    @app.route("/")
    @cache.cached(5)
    def cached_view():
        return str(time.time())

    cached_view.make_cache_key = lambda *args, **kwargs: request.args["foo"]

    tc = app.test_client()

    rv = tc.get("/?foo=a")
    a = rv.data.decode("utf-8")

    rv = tc.get("/?foo=b")
    b = rv.data.decode("utf-8")
    assert a != b

    tc = app.test_client()
    rv = tc.get("/?foo=a")
    a_2 = rv.data.decode("utf-8")
    assert a == a_2

    rv = tc.get("/?foo=b")
    b_2 = rv.data.decode("utf-8")
    assert b == b_2


def test_make_cache_key_function_property(app, cache):
    @app.route("/<foo>/<bar>")
    @cache.memoize(5)
    def cached_view(foo, bar):
        return str(time.time())

    assert hasattr(cached_view, "make_cache_key")
    assert callable(cached_view.make_cache_key)

    tc = app.test_client()

    rv = tc.get("/a/b")
    the_time = rv.data.decode("utf-8")

    cache_key = cached_view.make_cache_key(cached_view.uncached, foo="a", bar="b")
    cache_data = cache.get(cache_key)
    assert the_time == cache_data

    different_key = cached_view.make_cache_key(cached_view.uncached, foo="b", bar="a")
    different_data = cache.get(different_key)
    assert the_time != different_data


def test_cache_timeout_property(app, cache):
    @app.route("/")
    @cache.memoize(2)
    def cached_view1():
        return str(time.time())

    @app.route("/<foo>/<bar>")
    @cache.memoize(4)
    def cached_view2(foo, bar):
        return str(time.time())

    assert hasattr(cached_view1, "cache_timeout")
    assert hasattr(cached_view2, "cache_timeout")
    assert cached_view1.cache_timeout == 2
    assert cached_view2.cache_timeout == 4

    # test that this is a read-write property
    cached_view1.cache_timeout = 5
    cached_view2.cache_timeout = 7

    assert cached_view1.cache_timeout == 5
    assert cached_view2.cache_timeout == 7
    tc = app.test_client()

    rv1 = tc.get("/")
    time1 = rv1.data.decode("utf-8")
    time.sleep(1)
    rv2 = tc.get("/a/b")
    time2 = rv2.data.decode("utf-8")

    # VIEW1
    # it's been 1 second, cache is still active
    assert time1 == tc.get("/").data.decode("utf-8")
    time.sleep(5)
    # it's been >5 seconds, cache is not still active
    assert time1 != tc.get("/").data.decode("utf-8")

    # VIEW2
    # it's been >17 seconds, cache is still active
    # self.assertEqual(time2, tc.get('/a/b').data.decode('utf-8'))
    assert time2 == tc.get("/a/b").data.decode("utf-8")
    time.sleep(3)
    # it's been >7 seconds, cache is not still active
    assert time2 != tc.get("/a/b").data.decode("utf-8")


def test_cache_timeout_dynamic(app, cache):
    @app.route("/")
    @cache.cached(timeout=1)
    def cached_view():
        # This should override the timeout to be 2 seconds
        return CachedResponse(response=make_response(str(time.time())), timeout=2)

    tc = app.test_client()

    rv1 = tc.get("/")
    time1 = rv1.data.decode("utf-8")
    time.sleep(1)

    # it's been 1 second, cache is still active
    assert time1 == tc.get("/").data.decode("utf-8")
    time.sleep(1)
    # it's been >2 seconds, cache is not still active
    assert time1 != tc.get("/").data.decode("utf-8")


def test_generate_cache_key_from_query_string(app, cache):
    """Test the _make_cache_key_query_string() cache key maker.

    Create three requests to verify that the same query string
    parameters (key/value) always reference the same cache,
    regardless of the order of parameters.

    Also test to make sure that the same cache isn't being used for
    any/all query string parameters.

    For example, these two requests should yield the same
    cache/cache key:

      * GET /v1/works?mock=true&offset=20&limit=15
      * GET /v1/works?limit=15&mock=true&offset=20

    Caching functionality is verified by a `@cached` route `/works` which
    produces a time in its response. The time in the response can verify that
    two requests with the same query string parameters/values, though
    differently ordered, produce responses with the same time.
    """

    @app.route("/works")
    @cache.cached(query_string=True)
    def view_works():
        return str(time.time())

    tc = app.test_client()

    # Make our first query...
    first_response = tc.get("/works?mock=true&offset=20&limit=15")
    first_time = first_response.get_data(as_text=True)

    # Make the second query...
    second_response = tc.get("/works?limit=15&mock=true&offset=20")
    second_time = second_response.get_data(as_text=True)

    # Now make sure the time for the first and second
    # query are the same!
    assert second_time == first_time

    # Last/third query with different parameters/values should
    # produce a different time.
    third_response = tc.get("/v1/works?limit=20&mock=true&offset=60")
    third_time = third_response.get_data(as_text=True)

    # ... making sure that different query parameter values
    # don't yield the same cache!
    assert not third_time == second_time


def test_generate_cache_key_from_query_string_repeated_paramaters(app, cache):
    """Test the _make_cache_key_query_string() cache key maker's support for
    repeated query paramaters

    URL params can be repeated with different values. Flask's MultiDict
    supports them
    """

    @app.route("/works")
    @cache.cached(query_string=True)
    def view_works():
        flatted_values = sum(request.args.listvalues(), [])
        return str(sorted(flatted_values)) + str(time.time())

    tc = app.test_client()

    # Make our first query...
    first_response = tc.get("/works?mock=true&offset=20&limit=15&user[]=123&user[]=124")
    first_time = first_response.get_data(as_text=True)

    # Make the second query...
    second_response = tc.get(
        "/works?mock=true&offset=20&limit=15&user[]=124&user[]=123"
    )
    second_time = second_response.get_data(as_text=True)

    # Now make sure the time for the first and second
    # query are the same!
    assert second_time == first_time

    # Last/third query with different parameters/values should
    # produce a different time.
    third_response = tc.get("/works?mock=true&offset=20&limit=15&user[]=125&user[]=124")
    third_time = third_response.get_data(as_text=True)

    # ... making sure that different query parameter values
    # don't yield the same cache!
    assert not third_time == second_time


def test_generate_cache_key_from_request_body(app, cache):
    """Test a user supplied cache key maker.
    Create three requests to verify that the same request body
    always reference the same cache
    Also test to make sure that the same cache isn't being used for
    any/all query string parameters.
    Caching functionality is verified by a `@cached` route `/works` which
    produces a time in its response. The time in the response can verify that
    two requests with the same request body produce responses with the same
    time.
    """

    def _make_cache_key_request_body(argument):
        """Create keys based on request body."""
        # now hash the request body so it can be
        # used as a key for cache.
        request_body = request.get_data(as_text=False)
        hashed_body = str(hashlib.md5(request_body).hexdigest())
        cache_key = request.path + hashed_body
        return cache_key

    @app.route("/works/<argument>", methods=["POST"])
    @cache.cached(make_cache_key=_make_cache_key_request_body)
    def view_works(argument):
        return str(time.time()) + request.get_data().decode()

    tc = app.test_client()

    # Make our request...
    first_response = tc.post("/works/arg", data=dict(mock=True, value=1, test=2))
    first_time = first_response.get_data(as_text=True)

    # Make the request...
    second_response = tc.post("/works/arg", data=dict(mock=True, value=1, test=2))
    second_time = second_response.get_data(as_text=True)

    # Now make sure the time for the first and second
    # requests are the same!
    assert second_time == first_time

    # Last/third request with different body should
    # produce a different time.
    third_response = tc.post("/works/arg", data=dict(mock=True, value=2, test=3))
    third_time = third_response.get_data(as_text=True)

    # ... making sure that different request bodies
    # don't yield the same cache!
    assert not third_time == second_time


def test_cache_with_query_string_and_source_check_enabled(app, cache):
    """Test the _make_cache_key_query_string() cache key maker with
    source_check set to True to include the view's function's source code as
    part of the cache hash key.
    """

    @cache.cached(query_string=True, source_check=True)
    def view_works():
        return str(time.time())

    app.add_url_rule("/works", "works", view_works)

    tc = app.test_client()

    # Make our first query...
    first_response = tc.get("/works?mock=true&offset=20&limit=15")
    first_time = first_response.get_data(as_text=True)

    # Make our second query...
    second_response = tc.get("/works?mock=true&offset=20&limit=15")
    second_time = second_response.get_data(as_text=True)

    # The cache should yield the same data first and second time
    assert first_time == second_time

    # Change the source of the function attached to the view
    @cache.cached(query_string=True, source_check=True)
    def view_works():
        return str(time.time())

    # ... and we override the function attached to the view
    app.view_functions["works"] = view_works

    tc = app.test_client()

    # Make the second query...
    third_response = tc.get("/works?mock=true&offset=20&limit=15")
    third_time = third_response.get_data(as_text=True)

    # Now make sure the time for the first and third
    # responses are not the same i.e. cached is not used!
    assert third_time[0] != first_time

    # Change the source of the function to what it was originally
    @cache.cached(query_string=True, source_check=True)
    def view_works():
        return str(time.time())

    app.view_functions["works"] = view_works

    tc = app.test_client()

    # Last/third query with different parameters/values should
    # produce a different time.
    forth_response = tc.get("/works?mock=true&offset=20&limit=15")
    forth_time = forth_response.get_data(as_text=True)

    # ... making sure that the first value and the forth value are the same
    # since the source is the same
    assert forth_time == first_time


def test_cache_with_query_string_and_source_check_disabled(app, cache):
    """Test the _make_cache_key_query_string() cache key maker with
    source_check set to False to exclude the view's function's source code as
    part of the cache hash key and to see if changing the source changes the
    data.
    """

    @cache.cached(query_string=True, source_check=False)
    def view_works():
        return str(time.time())

    app.add_url_rule("/works", "works", view_works)

    tc = app.test_client()

    # Make our first query...
    first_response = tc.get("/works?mock=true&offset=20&limit=15")
    first_time = first_response.get_data(as_text=True)

    # Make our second query...
    second_response = tc.get("/works?mock=true&offset=20&limit=15")
    second_time = second_response.get_data(as_text=True)

    # The cache should yield the same data first and second time
    assert first_time == second_time

    # Change the source of the function attached to the view
    @cache.cached(query_string=True, source_check=False)
    def view_works():
        return str(time.time())

    # ... and we override the function attached to the view
    app.view_functions["works"] = view_works

    tc = app.test_client()

    # Make the second query...
    third_response = tc.get("/works?mock=true&offset=20&limit=15")
    third_time = third_response.get_data(as_text=True)

    # Now make sure the time for the first and third responses are the same
    # i.e. cached is used since cache will not check for source changes!
    assert third_time == first_time


def test_hit_cache(app, cache):
    @app.route("/")
    @cache.cached(10, response_hit_indication=True)
    def cached_view():
        # This should override the timeout to be 2 seconds
        return {"data": "data"}

    tc = app.test_client()

    assert tc.get("/").headers.get("hit_cache") is None
    assert tc.get("/").headers.get("hit_cache") == "True"
    assert tc.get("/").headers.get("hit_cache") == "True"
