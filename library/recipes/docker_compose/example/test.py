import urllib.request


def test_compose_works():
    request = urllib.request.urlopen("http://localhost:5000")
    response = request.read().decode(request.headers.get_content_charset())
    assert 'Hello World!' in response
