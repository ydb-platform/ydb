from shlex import quote


def to_curl(request, compressed=False, verify=True, pretty=False):
    """
    Returns a string with a curl command that makes the same HTTP request as
    the provided request object.

    Parameters
    ----------
    compressed : bool
        If `True` then a `--compressed` argument will be added to the result
    verify : bool
        If `False` then a `--insecure` argument will be added to the result,
        disabling TLS certificate verification
    """
    command = []

    inferred_method = 'GET'
    if request.body is not None:
        inferred_method = 'POST'
    if request.method != inferred_method:
        command.append('-X ' + quote(request.method))

    for k, v in request.headers.items():
        if v:
            command.append('-H ' + quote('{0}: {1}'.format(k, v)))
        else:
            # -H 'Accept:' disables sending the Accept header, use semicolon to send
            # empty header
            command.append('-H ' + quote('{0};'.format(k)))

    if request.body:
        body = request.body
        if isinstance(body, bytes):
            body = body.decode('utf-8')
        data_type = '-d'
        if body.startswith('@'):  # -d @filename causes curl to read from file
            data_type = '--data-raw'
        command.append(data_type + ' ' + quote(body))

    if compressed:
        command.append('--compressed')

    if not verify:
        command.append('--insecure')

    command.append(quote(request.url))

    joiner = ' '
    if pretty and len(command) > 3:
        joiner = ' \\\n  '
    return 'curl ' + joiner.join(command)
