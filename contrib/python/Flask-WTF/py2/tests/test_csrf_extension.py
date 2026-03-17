import pytest
from flask import Blueprint, abort, g, render_template_string, request

from flask_wtf import FlaskForm
from flask_wtf._compat import FlaskWTFDeprecationWarning
from flask_wtf.csrf import CSRFError, CSRFProtect, CsrfProtect, generate_csrf


@pytest.fixture
def app(app):
    CSRFProtect(app)

    @app.route('/', methods=['GET', 'POST'])
    def index():
        pass

    @app.after_request
    def add_csrf_header(response):
        response.headers.set('X-CSRF-Token', generate_csrf())
        return response

    return app


@pytest.fixture
def csrf(app):
    return app.extensions['csrf']


def test_render_token(req_ctx):
    token = generate_csrf()
    assert render_template_string('{{ csrf_token() }}') == token


def test_protect(app, client, app_ctx):
    response = client.post('/')
    assert response.status_code == 400
    assert 'The CSRF token is missing.' in response.get_data(as_text=True)

    app.config['WTF_CSRF_ENABLED'] = False
    assert client.post('/').get_data() == b''
    app.config['WTF_CSRF_ENABLED'] = True

    app.config['WTF_CSRF_CHECK_DEFAULT'] = False
    assert client.post('/').get_data() == b''
    app.config['WTF_CSRF_CHECK_DEFAULT'] = True

    assert client.options('/').status_code == 200
    assert client.post('/not-found').status_code == 404

    response = client.get('/')
    assert response.status_code == 200
    token = response.headers['X-CSRF-Token']
    assert client.post('/', data={'csrf_token': token}).status_code == 200
    assert client.post(
        '/', data={'prefix-csrf_token': token}
    ).status_code == 200
    assert client.post('/', data={'prefix-csrf_token': ''}).status_code == 400
    assert client.post('/', headers={'X-CSRF-Token': token}).status_code == 200


def test_same_origin(client):
    token = client.get('/').headers['X-CSRF-Token']
    response = client.post('/', base_url='https://localhost', headers={
        'X-CSRF-Token': token
    })
    data = response.get_data(as_text=True)
    assert 'The referrer header is missing.' in data

    response = client.post('/', base_url='https://localhost', headers={
        'X-CSRF-Token': token, 'Referer': 'http://localhost/'
    })
    data = response.get_data(as_text=True)
    assert 'The referrer does not match the host.' in data

    response = client.post('/', base_url='https://localhost', headers={
        'X-CSRF-Token': token, 'Referer': 'https://other/'
    })
    data = response.get_data(as_text=True)
    assert 'The referrer does not match the host.' in data

    response = client.post('/', base_url='https://localhost', headers={
        'X-CSRF-Token': token, 'Referer': 'https://localhost:8080/'
    })
    data = response.get_data(as_text=True)
    assert 'The referrer does not match the host.' in data

    response = client.post('/', base_url='https://localhost', headers={
        'X-CSRF-Token': token, 'Referer': 'https://localhost/'
    })
    assert response.status_code == 200


def test_form_csrf_short_circuit(app, client):
    @app.route('/skip', methods=['POST'])
    def skip():
        assert g.get('csrf_valid')
        # don't pass the token, then validate the form
        # this would fail if CSRFProtect didn't run
        form = FlaskForm(None)
        assert form.validate()

    token = client.get('/').headers['X-CSRF-Token']
    response = client.post('/skip', headers={'X-CSRF-Token': token})
    assert response.status_code == 200


def test_exempt_view(app, csrf, client):
    @app.route('/exempt', methods=['POST'])
    @csrf.exempt
    def exempt():
        pass

    response = client.post('/exempt')
    assert response.status_code == 200

    csrf.exempt('__tests__.test_csrf_extension.index')
    response = client.post('/')
    assert response.status_code == 200


def test_manual_protect(app, csrf, client):
    @app.route('/manual', methods=['GET', 'POST'])
    @csrf.exempt
    def manual():
        csrf.protect()

    response = client.get('/manual')
    assert response.status_code == 200

    response = client.post('/manual')
    assert response.status_code == 400


def test_exempt_blueprint(app, csrf, client):
    bp = Blueprint('exempt', __name__, url_prefix='/exempt')
    csrf.exempt(bp)

    @bp.route('/', methods=['POST'])
    def index():
        pass

    app.register_blueprint(bp)
    response = client.post('/exempt/')
    assert response.status_code == 200


def test_error_handler(app, client):
    @app.errorhandler(CSRFError)
    def handle_csrf_error(e):
        return e.description.lower()

    response = client.post('/')
    assert response.get_data(as_text=True) == 'the csrf token is missing.'


def test_validate_error_logged(client, monkeypatch):
    from flask_wtf.csrf import logger

    messages = []

    def assert_info(message):
        messages.append(message)

    monkeypatch.setattr(logger, 'info', assert_info)

    client.post('/')
    assert len(messages) == 1
    assert messages[0] == 'The CSRF token is missing.'


def test_deprecated_csrfprotect(recwarn):
    CsrfProtect()
    w = recwarn.pop(FlaskWTFDeprecationWarning)
    assert 'CSRFProtect' in str(w.message)


def test_deprecated_error_handler(csrf, client, recwarn):
    @csrf.error_handler
    def handle_csrf_error(reason):
        if 'abort' in request.form:
            abort(418)

        return 'return'

    w = recwarn.pop(FlaskWTFDeprecationWarning)
    assert '@app.errorhandler' in str(w.message)

    response = client.post('/', data={'abort': '1'})
    assert response.status_code == 418

    response = client.post('/')
    assert response.status_code == 200
    assert 'return' in response.get_data(as_text=True)
