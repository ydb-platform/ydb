import pytest
from flask import json
from markupsafe import Markup

from flask_wtf import FlaskForm
from flask_wtf._compat import to_bytes
from flask_wtf.recaptcha import RecaptchaField
from flask_wtf.recaptcha.validators import Recaptcha, http


class RecaptchaForm(FlaskForm):
    class Meta:
        csrf = False

    recaptcha = RecaptchaField()


@pytest.fixture
def app(app):
    app.testing = False
    app.config['PROPAGATE_EXCEPTIONS'] = True
    app.config['RECAPTCHA_PUBLIC_KEY'] = 'public'
    app.config['RECAPTCHA_PRIVATE_KEY'] = 'private'
    return app


@pytest.yield_fixture(autouse=True)
def req_ctx(app):
    with app.test_request_context(
        data={'g-recaptcha-response': 'pass'}
    ) as ctx:
        yield ctx


def test_config(app, monkeypatch):
    f = RecaptchaForm()
    monkeypatch.setattr(app, 'testing', True)
    f.validate()
    assert not f.recaptcha.errors
    monkeypatch.undo()

    monkeypatch.delitem(app.config, 'RECAPTCHA_PUBLIC_KEY')
    pytest.raises(RuntimeError, f.recaptcha)
    monkeypatch.undo()

    monkeypatch.delitem(app.config, 'RECAPTCHA_PRIVATE_KEY')
    pytest.raises(RuntimeError, f.validate)


def test_render_has_js():
    f = RecaptchaForm()
    render = f.recaptcha()
    assert 'https://www.google.com/recaptcha/api.js' in render


def test_render_custom_html(app):
    app.config['RECAPTCHA_HTML'] = 'custom'
    f = RecaptchaForm()
    render = f.recaptcha()
    assert render == 'custom'
    assert isinstance(render, Markup)


def test_render_custom_args(app):
    app.config['RECAPTCHA_PARAMETERS'] = {'key': '(value)'}
    app.config['RECAPTCHA_DATA_ATTRS'] = {'red': 'blue'}
    f = RecaptchaForm()
    render = f.recaptcha()
    assert '?key=%28value%29' in render
    assert 'data-red="blue"' in render


def test_missing_response(app):
    with app.test_request_context():
        f = RecaptchaForm()
        f.validate()
        assert f.recaptcha.errors[0] == 'The response parameter is missing.'


class MockResponse(object):
    def __init__(self, code, error='invalid-input-response', read_bytes=False):
        self.code = code
        self.data = json.dumps({
            'success': not error,
            'error-codes': [error] if error else []
        })
        self.read_bytes = read_bytes

    def read(self):
        if self.read_bytes:
            return to_bytes(self.data)

        return self.data


def test_send_invalid_request(monkeypatch):
    def mock_urlopen(url, data):
        return MockResponse(200)

    monkeypatch.setattr(http, 'urlopen', mock_urlopen)
    f = RecaptchaForm()
    f.validate()
    assert f.recaptcha.errors[0] == (
        'The response parameter is invalid or malformed.'
    )


def test_response_from_json(app, monkeypatch):
    def mock_urlopen(url, data):
        return MockResponse(200)

    monkeypatch.setattr(http, 'urlopen', mock_urlopen)

    with app.test_request_context(
        data=json.dumps({'g-recaptcha-response': 'pass'}),
        content_type='application/json'
    ):
        f = RecaptchaForm()
        f.validate()
        assert f.recaptcha.errors[0] != 'The response parameter is missing.'


def test_request_fail(monkeypatch):
    def mock_urlopen(url, data):
        return MockResponse(400)

    monkeypatch.setattr(http, 'urlopen', mock_urlopen)
    f = RecaptchaForm()
    f.validate()
    assert f.recaptcha.errors


def test_request_success(monkeypatch):
    def mock_urlopen(url, data):
        return MockResponse(200, '')

    monkeypatch.setattr(http, 'urlopen', mock_urlopen)
    f = RecaptchaForm()
    f.validate()
    assert not f.recaptcha.errors


def test_request_unmatched_error(monkeypatch):
    def mock_urlopen(url, data):
        return MockResponse(200, 'not-an-error', True)

    monkeypatch.setattr(http, 'urlopen', mock_urlopen)
    f = RecaptchaForm()
    f.recaptcha.validators = [Recaptcha('custom')]
    f.validate()
    assert f.recaptcha.errors[0] == 'custom'
