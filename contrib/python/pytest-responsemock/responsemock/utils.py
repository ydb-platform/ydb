from contextlib import contextmanager
from textwrap import dedent
from typing import Union, List, Generator, Optional

if False:  # pragma: nocover
    from responses import RequestsMock  # noqa


@contextmanager
def response_mock(
        rules: Union[List[str], str, List[bytes], bytes],
        *,
        bypass: bool = False,
        **kwargs
) -> Generator[Optional['RequestsMock'], None, None]:
    """Simple context manager to mock responses for `requests` package.

    Any request under that manager will be intercepted and mocked according
    to one or more ``rules`` passed to the manager. If actual request won't fall
    under any of given rules then an exception is raised (by default).

    Rules are simple strings, of pattern: ``HTTP_METHOD URL -> STATUS_CODE :BODY``.

    Example::

        def test_me(response_mock):


            json_response = json.dumps({'key': 'value', 'another': 'yes'})

            with response_mock([

                'GET http://a.b -> 200 :Nice',

                b'GET http://a.b/binary/ -> 200 :\xd1\x82\xd0\xb5\xd1\x81\xd1\x82',

                f'POST http://some.domain -> 400 :{json_response}'

                '''
                GET https://some.domain

                Allow: GET, HEAD
                Content-Language: ru

                -> 200 :OK
                '''

            ], bypass=False) as mock:

                mock.add_passthru('http://c.d')

                this_makes_requests()

    :param rules: One or several rules for response.
    :param bypass: Whether to bypass (disable) mocking.
    :param kwargs: Additional keyword arguments to pass to `RequestsMock`.

    """
    from responses import RequestsMock

    if bypass:

        yield

    else:

        with RequestsMock(**kwargs) as mock:

            def enc(val):
                return val.encode() if is_binary else val

            def dec(val):
                return val.decode() if is_binary else val

            if isinstance(rules, (str, bytes)):
                rules = [rules]

            for rule in rules:

                if not rule:
                    continue

                is_binary = isinstance(rule, bytes)

                if not is_binary:
                    rule = dedent(rule)

                rule = rule.strip()
                directives, _, response = rule.partition(enc('->'))

                headers = {}

                if (enc('\n')) in directives:
                    directives, *headers_block = directives.splitlines()

                    for header_line in headers_block:
                        header_line = header_line.strip()

                        if not header_line:
                            continue

                        key, _, val = header_line.partition(enc(':'))
                        val = val.strip()

                        if val:
                            headers[dec(key.strip())] = dec(val)

                add_kwargs = {}

                content_type = headers.pop('Content-Type', None)

                if content_type:
                    add_kwargs['content_type'] = content_type

                directives = list(
                    filter(
                        None,
                        map(
                            (bytes if is_binary else str).strip,
                            directives.split(enc(' '))
                        )
                    ))

                assert len(directives) == 2, (
                    f'Unsupported directives: {directives}. Expected: HTTP_METHOD URL')

                status, _, response = response.partition(enc(':'))

                status = int(status.strip())

                mock.add(
                    method=dec(directives[0]),
                    url=dec(directives[1]),
                    body=response,
                    status=status,
                    adding_headers=headers or None,
                    **add_kwargs
                )

            yield mock
