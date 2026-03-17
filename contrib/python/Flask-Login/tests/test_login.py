from __future__ import annotations

import sys
import unittest
from collections.abc import Hashable
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from unittest.mock import ANY
from unittest.mock import Mock
from unittest.mock import patch

from flask import Blueprint
from flask import Flask
from flask import get_flashed_messages
from flask import Response
from flask import session
from flask.views import MethodView
from flask_login import AnonymousUserMixin
from flask_login import confirm_login
from flask_login import current_user
from flask_login import decode_cookie
from flask_login import encode_cookie
from flask_login import FlaskLoginClient
from flask_login import fresh_login_required
from flask_login import login_fresh
from flask_login import login_remembered
from flask_login import login_required
from flask_login import login_url
from flask_login import login_user
from flask_login import LoginManager
from flask_login import logout_user
from flask_login import make_next_param
from flask_login import session_protected
from flask_login import set_login_view
from flask_login import user_accessed
from flask_login import user_loaded_from_cookie
from flask_login import user_loaded_from_request
from flask_login import user_logged_in
from flask_login import user_logged_out
from flask_login import user_login_confirmed
from flask_login import user_needs_refresh
from flask_login import user_unauthorized
from flask_login import UserMixin
from flask_login.__about__ import __author__
from flask_login.__about__ import __author_email__
from flask_login.__about__ import __copyright__
from flask_login.__about__ import __description__
from flask_login.__about__ import __license__
from flask_login.__about__ import __maintainer__
from flask_login.__about__ import __title__
from flask_login.__about__ import __url__
from flask_login.__about__ import __version__
from flask_login.__about__ import __version_info__
from flask_login.utils import _secret_key
from flask_login.utils import _user_context_processor
from semantic_version import Version
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.test import Client

sys_version = Version(
    major=sys.version_info.major,
    minor=sys.version_info.minor,
    patch=sys.version_info.micro,
)

# Support Werkzeug < 2.3
new_cookie_methods = hasattr(Client, "get_cookie")


@dataclass
class BasicCookie:
    key: str
    value: str
    expires: datetime | None


def client_get_cookie(
    client: Client, key: str, domain: str = "localhost", path: str = "/"
) -> BasicCookie | None:
    if new_cookie_methods:
        if domain.startswith("."):
            domain = domain[1:]

        cookie = client.get_cookie(key, domain, path)

        if cookie is None:
            return None

        return BasicCookie(cookie.key, cookie.value, cookie.expires)
    else:
        try:
            cookie = client.cookie_jar._cookies[domain][path][key]

            if cookie.expires is None:
                expires = None
            else:
                expires = datetime.fromtimestamp(cookie.expires, timezone.utc)

            return BasicCookie(cookie.name, cookie.value, expires)
        except KeyError:
            return None


def client_set_cookie(
    client: Client, key: str, value: str, domain: str | None = None
) -> None:
    if new_cookie_methods:
        if domain.startswith("."):
            domain = domain[1:]

        client.set_cookie(key, value, domain=domain)
    else:
        client.set_cookie(domain, key, value)


@contextmanager
def listen_to(signal):
    """Context Manager that listens to signals and records emissions

    Example:

    with listen_to(user_logged_in) as listener:
        login_user(user)

        # Assert that a single emittance of the specific args was seen.
        listener.assert_heard_one(app, user=user))

        # Of course, you can always just look at the list yourself
        self.assertEqual(1, len(listener.heard))

    """

    class _SignalsCaught:
        def __init__(self):
            self.heard = []

        def add(self, *args, **kwargs):
            """The actual handler of the signal."""
            self.heard.append((args, kwargs))

        def assert_heard_one(self, *args, **kwargs):
            """The signal fired once, and with the arguments given"""
            if len(self.heard) == 0:
                raise AssertionError("No signals were fired")
            elif len(self.heard) > 1:
                msg = f"{len(self.heard)} signals were fired"
                raise AssertionError(msg)
            elif self.heard[0] != (args, kwargs):
                raise AssertionError(
                    "One signal was heard, but with incorrect"
                    f" arguments: Got ({self.heard[0]}) expected"
                    f" ({args}, {kwargs})"
                )

        def assert_heard_none(self, *args, **kwargs):
            """The signal fired no times"""
            if len(self.heard) >= 1:
                msg = f"{len(self.heard)} signals were fired"
                raise AssertionError(msg)

    results = _SignalsCaught()
    signal.connect(results.add)

    try:
        yield results
    finally:
        signal.disconnect(results.add)


class User(UserMixin):
    def __init__(self, name, id, active=True):
        self.id = id
        self.name = name
        self.active = active

    def get_id(self):
        return self.id

    @property
    def is_active(self):
        return self.active


notch = User("Notch", 1)
steve = User("Steve", 2)
creeper = User("Creeper", 3, False)
germanjapanese = User("Müller", "佐藤")  # str user_id

USERS = {1: notch, 2: steve, 3: creeper, "佐藤": germanjapanese}


class AboutTestCase(unittest.TestCase):
    """Make sure we can get version and other info."""

    def test_have_about_data(self):
        self.assertTrue(__title__ is not None)
        self.assertTrue(__description__ is not None)
        self.assertTrue(__url__ is not None)
        self.assertTrue(__version_info__ is not None)
        self.assertTrue(__version__ is not None)
        self.assertTrue(__author__ is not None)
        self.assertTrue(__author_email__ is not None)
        self.assertTrue(__maintainer__ is not None)
        self.assertTrue(__license__ is not None)
        self.assertTrue(__copyright__ is not None)


class StaticTestCase(unittest.TestCase):
    def test_static_loads_anonymous(self):
        app = Flask(__name__)
        app.static_url_path = "/static"
        app.secret_key = "this is a temp key"
        lm = LoginManager()
        lm.init_app(app)

        @lm.user_loader
        def load_user(user_id):
            return USERS[int(user_id)]

        with app.test_client() as c:
            c.get("/static/favicon.ico")
            self.assertTrue(current_user.is_anonymous)

    def test_static_loads_without_accessing_session(self):
        app = Flask(__name__)
        app.static_url_path = "/static"
        app.secret_key = "this is a temp key"
        lm = LoginManager()
        lm.init_app(app)

        @lm.user_loader
        def load_user(user_id):
            return USERS[int(user_id)]

        with app.test_client() as c:
            with listen_to(user_accessed) as listener:
                c.get("/static/favicon.ico")
                listener.assert_heard_none(app)


class InitializationTestCase(unittest.TestCase):
    """Tests the two initialization methods"""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "1234"

    def test_init_app(self):
        login_manager = LoginManager()
        login_manager.init_app(self.app, add_context_processor=True)

        self.assertIsInstance(login_manager, LoginManager)

    def test_class_init(self):
        login_manager = LoginManager(self.app, add_context_processor=True)

        self.assertIsInstance(login_manager, LoginManager)

    def test_no_user_loader_raises(self):
        login_manager = LoginManager(self.app, add_context_processor=True)
        with self.app.test_request_context():
            session["_user_id"] = "2"
            with self.assertRaises(Exception) as cm:
                login_manager._load_user()
            expected_message = "Missing user_loader or request_loader"
            self.assertTrue(str(cm.exception).startswith(expected_message))


class MethodViewLoginTestCase(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)
        self.app.config["LOGIN_DISABLED"] = False

        class SecretEndpoint(MethodView):
            decorators = [
                login_required,
                fresh_login_required,
            ]

            def options(self):
                return ""

            def get(self):
                return ""

        self.app.add_url_rule("/secret", view_func=SecretEndpoint.as_view("secret"))

    def test_options_call_exempt(self):
        with self.app.test_client() as c:
            result = c.open("/secret", method="OPTIONS")
            self.assertEqual(result.status_code, 200)


class LoginTestCase(unittest.TestCase):
    """Tests for results of the login_user function"""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "deterministic"
        self.app.config["SESSION_PROTECTION"] = None
        self.remember_cookie_name = "remember"
        self.app.config["REMEMBER_COOKIE_NAME"] = self.remember_cookie_name
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)
        self.app.config["LOGIN_DISABLED"] = False

        # Disable absolute location, like Werkzeug 2.1
        self.app.response_class.autocorrect_location_header = False

        @self.app.route("/")
        def index():
            return "Welcome!"

        @self.app.route("/secret")
        def secret():
            return self.login_manager.unauthorized()

        @self.app.route("/login-notch")
        def login_notch():
            return str(login_user(notch))

        @self.app.route("/login-notch-remember")
        def login_notch_remember():
            return str(login_user(notch, remember=True))

        @self.app.route("/login-notch-remember-custom")
        def login_notch_remember_custom():
            duration = timedelta(hours=7)
            return str(login_user(notch, remember=True, duration=duration))

        @self.app.route("/login-notch-permanent")
        def login_notch_permanent():
            session.permanent = True
            return str(login_user(notch))

        @self.app.route("/needs-refresh")
        def needs_refresh():
            return self.login_manager.needs_refresh()

        @self.app.route("/confirm-login")
        def _confirm_login():
            confirm_login()
            return ""

        @self.app.route("/username")
        def username():
            if current_user.is_authenticated:
                return current_user.name
            return "Anonymous"

        @self.app.route("/is-fresh")
        def is_fresh():
            return str(login_fresh())

        @self.app.route("/is-remembered")
        def is_remembered():
            return str(login_remembered())

        @self.app.route("/logout")
        def logout():
            return str(logout_user())

        @self.login_manager.user_loader
        def load_user(user_id):
            return USERS[int(user_id)]

        @self.login_manager.request_loader
        def load_user_from_request(request):
            user_id = request.args.get("user_id")
            try:
                user_id = int(float(user_id))
            except TypeError:
                pass
            return USERS.get(user_id)

        @self.app.route("/empty_session")
        def empty_session():
            return f"modified={session.modified}"

        # This will help us with the possibility of typoes in the tests. Now
        # we shouldn't have to check each response to help us set up state
        # (such as login pages) to make sure it worked: we will always
        # get an exception raised (rather than return a 404 response)
        @self.app.errorhandler(404)
        def handle_404(e):
            raise e

        unittest.TestCase.setUp(self)

    def _delete_session(self, c):
        # Helper method to cause the session to be deleted
        # as if the browser was closed. This will remove
        # the session regardless of the permanent flag
        # on the session!
        with c.session_transaction() as sess:
            sess.clear()

    #
    # Login
    #
    def test_test_request_context_users_are_anonymous(self):
        with self.app.test_request_context():
            self.assertTrue(current_user.is_anonymous)

    def test_defaults_anonymous(self):
        with self.app.test_client() as c:
            result = c.get("/username")
            self.assertEqual("Anonymous", result.data.decode("utf-8"))

    def test_login_user(self):
        with self.app.test_request_context():
            result = login_user(notch)
            self.assertTrue(result)
            self.assertEqual(current_user.name, "Notch")
            self.assertIs(login_fresh(), True)

    def test_login_user_not_fresh(self):
        with self.app.test_request_context():
            result = login_user(notch, fresh=False)
            self.assertTrue(result)
            self.assertEqual(current_user.name, "Notch")
            self.assertIs(login_fresh(), False)

    def test_login_user_emits_signal(self):
        with self.app.test_request_context():
            with listen_to(user_logged_in) as listener:
                login_user(notch)
                listener.assert_heard_one(self.app, user=notch)

    def test_login_inactive_user(self):
        with self.app.test_request_context():
            result = login_user(creeper)
            self.assertTrue(current_user.is_anonymous)
            self.assertFalse(result)

    def test_login_inactive_user_forced(self):
        with self.app.test_request_context():
            login_user(creeper, force=True)
            self.assertEqual(current_user.name, "Creeper")

    def test_login_user_with_request(self):
        user_id = 2
        user_name = USERS[user_id].name
        with self.app.test_client() as c:
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual(user_name, result.data.decode("utf-8"))

    def test_login_invalid_user_with_request(self):
        user_id = 9000
        user_name = "Anonymous"
        with self.app.test_client() as c:
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual(user_name, result.data.decode("utf-8"))

    #
    # Logout
    #
    def test_logout_logs_out_current_user(self):
        with self.app.test_request_context():
            login_user(notch)
            logout_user()
            self.assertTrue(current_user.is_anonymous)

    def test_logout_emits_signal(self):
        with self.app.test_request_context():
            login_user(notch)
            with listen_to(user_logged_out) as listener:
                logout_user()
                listener.assert_heard_one(self.app, user=notch)

    def test_logout_without_current_user(self):
        with self.app.test_request_context():
            login_user(notch)
            del session["_user_id"]
            with listen_to(user_logged_out) as listener:
                logout_user()
                listener.assert_heard_one(self.app, user=ANY)

    #
    # Unauthorized
    #
    def test_unauthorized_fires_unauthorized_signal(self):
        with self.app.test_client() as c:
            with listen_to(user_unauthorized) as listener:
                c.get("/secret")
                listener.assert_heard_one(self.app)

    def test_unauthorized_flashes_message_with_login_view(self):
        self.login_manager.login_view = "/login"

        expected_message = self.login_manager.login_message = "Log in!"
        expected_category = self.login_manager.login_message_category = "login"

        with self.app.test_client() as c:
            c.get("/secret")
            msgs = get_flashed_messages(category_filter=[expected_category])
            self.assertEqual([expected_message], msgs)

    def test_unauthorized_flash_message_localized(self):
        def _gettext(msg):
            if msg == "Log in!":
                return "Einloggen"

        self.login_manager.login_view = "/login"
        self.login_manager.localize_callback = _gettext
        self.login_manager.login_message = "Log in!"

        expected_message = "Einloggen"
        expected_category = self.login_manager.login_message_category = "login"

        with self.app.test_client() as c:
            c.get("/secret")
            msgs = get_flashed_messages(category_filter=[expected_category])
            self.assertEqual([expected_message], msgs)
        self.login_manager.localize_callback = None

    def test_unauthorized_uses_authorized_handler(self):
        @self.login_manager.unauthorized_handler
        def _callback():
            return Response("This is secret!", 401)

        with self.app.test_client() as c:
            result = c.get("/secret")
            self.assertEqual(result.status_code, 401)
            self.assertEqual("This is secret!", result.data.decode("utf-8"))

    def test_unauthorized_aborts_with_401(self):
        with self.app.test_client() as c:
            result = c.get("/secret")
            self.assertEqual(result.status_code, 401)

    def test_unauthorized_redirects_to_login_view(self):
        self.login_manager.login_view = "login"

        @self.app.route("/login")
        def login():
            return "Login Form Goes Here!"

        with self.app.test_client() as c:
            result = c.get("/secret")
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "/login?next=%2Fsecret")

    def test_unauthorized_with_next_in_session(self):
        self.login_manager.login_view = "login"
        self.app.config["USE_SESSION_FOR_NEXT"] = True

        @self.app.route("/login")
        def login():
            return session.pop("next", "")

        with self.app.test_client() as c:
            result = c.get("/secret")
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "/login")
            self.assertEqual(c.get("/login").data.decode("utf-8"), "/secret")

    def test_unauthorized_with_next_in_strong_session(self):
        self.login_manager.login_view = "login"
        self.app.config["SESSION_PROTECTION"] = "strong"
        self.app.config["USE_SESSION_FOR_NEXT"] = True

        @self.app.route("/login")
        def login():
            if current_user.is_authenticated:
                # Or anything that touches current_user
                pass
            return session.pop("next", "")

        with self.app.test_client() as c:
            result = c.get("/secret")
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "/login")
            self.assertEqual(c.get("/login").data.decode("utf-8"), "/secret")

    def test_unauthorized_uses_blueprint_login_view(self):
        with self.app.app_context():
            first = Blueprint("first", "first")
            second = Blueprint("second", "second")

            @self.app.route("/app_login")
            def app_login():
                return "Login Form Goes Here!"

            @self.app.route("/first_login")
            def first_login():
                return "Login Form Goes Here!"

            @self.app.route("/second_login")
            def second_login():
                return "Login Form Goes Here!"

            @self.app.route("/protected")
            @login_required
            def protected():
                return "Access Granted"

            @first.route("/protected")
            @login_required
            def first_protected():
                return "Access Granted"

            @second.route("/protected")
            @login_required
            def second_protected():
                return "Access Granted"

            self.app.register_blueprint(first, url_prefix="/first")
            self.app.register_blueprint(second, url_prefix="/second")

            set_login_view("app_login")
            set_login_view("first_login", blueprint=first)
            set_login_view("second_login", blueprint=second)

            with self.app.test_client() as c:
                result = c.get("/protected")
                self.assertEqual(result.status_code, 302)
                expected = "/app_login?next=%2Fprotected"
                self.assertEqual(result.location, expected)

                result = c.get("/first/protected")
                self.assertEqual(result.status_code, 302)
                expected = "/first_login?next=%2Ffirst%2Fprotected"
                self.assertEqual(result.location, expected)

                result = c.get("/second/protected")
                self.assertEqual(result.status_code, 302)
                expected = "/second_login?next=%2Fsecond%2Fprotected"
                self.assertEqual(result.location, expected)

    def test_set_login_view_without_blueprints(self):
        with self.app.app_context():

            @self.app.route("/app_login")
            def app_login():
                return "Login Form Goes Here!"

            @self.app.route("/protected")
            @login_required
            def protected():
                return "Access Granted"

            set_login_view("app_login")

            with self.app.test_client() as c:
                result = c.get("/protected")
                self.assertEqual(result.status_code, 302)
                expected = "/app_login?next=%2Fprotected"
                self.assertEqual(result.location, expected)

    #
    # Session Persistence/Freshness
    #
    def test_login_persists(self):
        with self.app.test_client() as c:
            c.get("/login-notch")
            result = c.get("/username")

            self.assertEqual("Notch", result.data.decode("utf-8"))

    def test_logout_persists(self):
        with self.app.test_client() as c:
            c.get("/login-notch")
            c.get("/logout")
            result = c.get("/username")
            self.assertEqual(result.data.decode("utf-8"), "Anonymous")

    def test_incorrect_id_logs_out(self):
        # Ensure that any attempt to reload the user by the ID
        # will seem as if the user is no longer valid
        @self.login_manager.user_loader
        def new_user_loader(user_id):
            return

        with self.app.test_client() as c:
            # Successfully logs in
            c.get("/login-notch")
            result = c.get("/username")

            self.assertEqual("Anonymous", result.data.decode("utf-8"))

    def test_authentication_is_fresh(self):
        with self.app.test_client() as c:
            c.get("/login-notch")
            fresh_result = c.get("/is-fresh")
            self.assertEqual("True", fresh_result.data.decode("utf-8"))
            remembered_result = c.get("/is-remembered")
            self.assertEqual("False", remembered_result.data.decode("utf-8"))

    def test_remember_me(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            self._delete_session(c)
            username_result = c.get("/username")
            self.assertEqual("Notch", username_result.data.decode("utf-8"))
            fresh_result = c.get("/is-fresh")
            self.assertEqual("False", fresh_result.data.decode("utf-8"))
            remembered_result = c.get("/is-remembered")
            self.assertEqual("True", remembered_result.data.decode("utf-8"))

    def test_remember_me_custom_duration(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember-custom")
            self._delete_session(c)
            username_result = c.get("/username")
            self.assertEqual("Notch", username_result.data.decode("utf-8"))
            fresh_result = c.get("/is-fresh")
            self.assertEqual("False", fresh_result.data.decode("utf-8"))
            remembered_result = c.get("/is-remembered")
            self.assertEqual("True", remembered_result.data.decode("utf-8"))

    def test_remember_me_uses_custom_cookie_parameters(self):
        name = self.app.config["REMEMBER_COOKIE_NAME"] = "myname"
        duration = self.app.config["REMEMBER_COOKIE_DURATION"] = timedelta(days=2)
        path = self.app.config["REMEMBER_COOKIE_PATH"] = "/mypath"
        domain = self.app.config["REMEMBER_COOKIE_DOMAIN"] = ".localhost.local"
        c = self.app.test_client()
        c.get("/login-notch-remember")
        cookie = client_get_cookie(c, name, domain, path)
        self.assertIsNotNone(cookie)
        self.assertIsNotNone(cookie.expires)
        expected_date = datetime.now(timezone.utc) + duration
        difference = expected_date - cookie.expires
        self.assertLess(difference, timedelta(seconds=10))
        self.assertGreater(difference, timedelta(seconds=-10))

    def test_remember_me_custom_duration_uses_custom_cookie(self):
        name = self.app.config["REMEMBER_COOKIE_NAME"] = "myname"
        self.app.config["REMEMBER_COOKIE_DURATION"] = 172800
        duration = timedelta(hours=7)
        path = self.app.config["REMEMBER_COOKIE_PATH"] = "/mypath"
        domain = self.app.config["REMEMBER_COOKIE_DOMAIN"] = ".localhost.local"
        c = self.app.test_client()
        c.get("/login-notch-remember-custom")
        cookie = client_get_cookie(c, name, domain, path)
        self.assertIsNotNone(cookie)
        self.assertIsNotNone(cookie.expires)
        expected_date = datetime.now(timezone.utc) + duration
        difference = expected_date - cookie.expires
        self.assertLess(difference, timedelta(seconds=10))
        self.assertGreater(difference, timedelta(seconds=-10))

    def test_remember_me_accepts_duration_as_int(self):
        self.app.config["REMEMBER_COOKIE_DURATION"] = 172800
        duration = timedelta(seconds=172800)
        name = self.app.config["REMEMBER_COOKIE_NAME"] = "myname"
        domain = self.app.config["REMEMBER_COOKIE_DOMAIN"] = ".localhost.local"
        c = self.app.test_client()
        result = c.get("/login-notch-remember")
        self.assertEqual(result.status_code, 200)
        cookie = client_get_cookie(c, name, domain)
        self.assertIsNotNone(cookie)
        self.assertIsNotNone(cookie.expires)
        expected_date = datetime.now(timezone.utc) + duration
        difference = expected_date - cookie.expires
        self.assertLess(difference, timedelta(seconds=10))
        self.assertGreater(difference, timedelta(seconds=-10))

    def test_remember_me_with_invalid_duration_returns_500_response(self):
        self.app.config["REMEMBER_COOKIE_DURATION"] = "123"

        with self.app.test_client() as c:
            result = c.get("/login-notch-remember")
            self.assertEqual(result.status_code, 500)

    def test_remember_me_with_invalid_custom_duration_returns_500_resp(self):
        @self.app.route("/login-notch-remember-custom-invalid")
        def login_notch_remember_custom_invalid():
            duration = "123"
            return str(login_user(notch, remember=True, duration=duration))

        with self.app.test_client() as c:
            result = c.get("/login-notch-remember-custom-invalid")
            self.assertEqual(result.status_code, 500)

    def test_set_cookie_with_invalid_duration_raises_exception(self):
        self.app.config["REMEMBER_COOKIE_DURATION"] = "123"

        with self.assertRaises(Exception) as cm:
            with self.app.test_request_context():
                session["_user_id"] = 2
                self.login_manager._set_cookie(None)

        expected_exception_message = (
            "REMEMBER_COOKIE_DURATION must be a datetime.timedelta, instead got: 123"
        )
        self.assertIn(expected_exception_message, str(cm.exception))

    def test_set_cookie_with_invalid_custom_duration_raises_exception(self):
        with self.assertRaises(Exception) as cm:
            with self.app.test_request_context():
                login_user(notch, remember=True, duration="123")

        expected_exception_message = (
            "duration must be a datetime.timedelta, instead got: 123"
        )
        self.assertIn(expected_exception_message, str(cm.exception))

    def test_remember_me_no_refresh_every_request(self):
        domain = self.app.config["REMEMBER_COOKIE_DOMAIN"] = ".localhost.local"
        path = self.app.config["REMEMBER_COOKIE_PATH"] = "/"
        self.app.config["REMEMBER_COOKIE_REFRESH_EACH_REQUEST"] = False
        c = self.app.test_client()
        c.get("/login-notch-remember")
        cookie1 = client_get_cookie(c, "remember", domain, path)
        self.assertIsNotNone(cookie1.expires)
        self._delete_session(c)
        c.get("/username")
        cookie2 = client_get_cookie(c, "remember", domain, path)
        self.assertEqual(cookie1.expires, cookie2.expires)

    def test_remember_me_refresh_each_request(self):
        with patch("flask_login.login_manager.datetime") as mock_dt:
            now = datetime.utcnow()
            mock_dt.utcnow = Mock(return_value=now)

            domain = self.app.config["REMEMBER_COOKIE_DOMAIN"] = ".localhost.local"
            path = self.app.config["REMEMBER_COOKIE_PATH"] = "/"
            self.app.config["REMEMBER_COOKIE_REFRESH_EACH_REQUEST"] = True
            c = self.app.test_client()
            c.get("/login-notch-remember")
            cookie1 = client_get_cookie(c, "remember", domain, path)
            self.assertIsNotNone(cookie1.expires)
            mock_dt.utcnow.return_value = now + timedelta(seconds=1)
            c.get("/username")
            cookie2 = client_get_cookie(c, "remember", domain, path)
            self.assertNotEqual(cookie1.expires, cookie2.expires)

    def test_remember_me_is_unfresh(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            self._delete_session(c)
            self.assertEqual("False", c.get("/is-fresh").data.decode("utf-8"))
            self.assertEqual("True", c.get("/is-remembered").data.decode("utf-8"))

    def test_login_persists_with_signle_x_forwarded_for(self):
        self.app.config["SESSION_PROTECTION"] = "strong"
        with self.app.test_client() as c:
            c.get("/login-notch", headers=[("X-Forwarded-For", "10.1.1.1")])
            result = c.get("/username", headers=[("X-Forwarded-For", "10.1.1.1")])
            self.assertEqual("Notch", result.data.decode("utf-8"))
            result = c.get("/username", headers=[("X-Forwarded-For", "10.1.1.1")])
            self.assertEqual("Notch", result.data.decode("utf-8"))

    def test_login_persists_with_many_x_forwarded_for(self):
        self.app.config["SESSION_PROTECTION"] = "strong"
        with self.app.test_client() as c:
            c.get("/login-notch", headers=[("X-Forwarded-For", "10.1.1.1")])
            result = c.get("/username", headers=[("X-Forwarded-For", "10.1.1.1")])
            self.assertEqual("Notch", result.data.decode("utf-8"))
            result = c.get(
                "/username", headers=[("X-Forwarded-For", "10.1.1.1, 10.1.1.2")]
            )
            self.assertEqual("Notch", result.data.decode("utf-8"))

    def test_user_loaded_from_cookie_fired(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            self._delete_session(c)
            with listen_to(user_loaded_from_cookie) as listener:
                c.get("/username")
                listener.assert_heard_one(self.app, user=notch)

    def test_user_loaded_from_request_fired(self):
        user_id = 1
        user_name = USERS[user_id].name
        with self.app.test_client() as c:
            with listen_to(user_loaded_from_request) as listener:
                url = f"/username?user_id={user_id}"
                result = c.get(url)
                self.assertEqual(user_name, result.data.decode("utf-8"))
                listener.assert_heard_one(self.app, user=USERS[user_id])

    def test_logout_stays_logged_out_with_remember_me(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            c.get("/logout")
            result = c.get("/username")
            self.assertEqual(result.data.decode("utf-8"), "Anonymous")

    def test_logout_stays_logged_out_with_remember_me_custom_duration(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember-custom")
            c.get("/logout")
            result = c.get("/username")
            self.assertEqual(result.data.decode("utf-8"), "Anonymous")

    def test_needs_refresh_uses_handler(self):
        @self.login_manager.needs_refresh_handler
        def _on_refresh():
            return "Needs Refresh!"

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            result = c.get("/needs-refresh")
            self.assertEqual("Needs Refresh!", result.data.decode("utf-8"))

    def test_needs_refresh_fires_needs_refresh_signal(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            with listen_to(user_needs_refresh) as listener:
                c.get("/needs-refresh")
                listener.assert_heard_one(self.app)

    def test_needs_refresh_fires_flash_when_redirect_to_refresh_view(self):
        self.login_manager.refresh_view = "/refresh_view"

        self.login_manager.needs_refresh_message = "Refresh"
        self.login_manager.needs_refresh_message_category = "refresh"
        category_filter = [self.login_manager.needs_refresh_message_category]

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            c.get("/needs-refresh")
            msgs = get_flashed_messages(category_filter=category_filter)
            self.assertIn(self.login_manager.needs_refresh_message, msgs)

    def test_needs_refresh_flash_message_localized(self):
        def _gettext(msg):
            if msg == "Refresh":
                return "Aktualisieren"

        self.login_manager.refresh_view = "/refresh_view"
        self.login_manager.localize_callback = _gettext

        self.login_manager.needs_refresh_message = "Refresh"
        self.login_manager.needs_refresh_message_category = "refresh"
        category_filter = [self.login_manager.needs_refresh_message_category]

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            c.get("/needs-refresh")
            msgs = get_flashed_messages(category_filter=category_filter)
            self.assertIn("Aktualisieren", msgs)
        self.login_manager.localize_callback = None

    def test_needs_refresh_aborts_401(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            result = c.get("/needs-refresh")
            self.assertEqual(result.status_code, 401)

    def test_redirects_to_refresh_view(self):
        @self.app.route("/refresh-view")
        def refresh_view():
            return ""

        self.login_manager.refresh_view = "refresh_view"
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            result = c.get("/needs-refresh")
            self.assertEqual(result.status_code, 302)
            expected = "/refresh-view?next=%2Fneeds-refresh"
            self.assertEqual(result.location, expected)

    def test_refresh_with_next_in_session(self):
        @self.app.route("/refresh-view")
        def refresh_view():
            return session.pop("next", "")

        self.login_manager.refresh_view = "refresh_view"
        self.app.config["USE_SESSION_FOR_NEXT"] = True

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            result = c.get("/needs-refresh")
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "/refresh-view")
            result = c.get("/refresh-view")
            self.assertEqual(result.data.decode("utf-8"), "/needs-refresh")

    def test_confirm_login(self):
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            self._delete_session(c)
            self.assertEqual("False", c.get("/is-fresh").data.decode("utf-8"))
            self.assertEqual("True", c.get("/is-remembered").data.decode("utf-8"))
            c.get("/confirm-login")
            self.assertEqual("True", c.get("/is-fresh").data.decode("utf-8"))
            self.assertEqual("True", c.get("/is-remembered").data.decode("utf-8"))

    def test_user_login_confirmed_signal_fired(self):
        with self.app.test_client() as c:
            with listen_to(user_login_confirmed) as listener:
                c.get("/confirm-login")
                listener.assert_heard_one(self.app)

    def test_session_not_modified(self):
        with self.app.test_client() as c:
            # Within the request we think we didn't modify the session.
            self.assertEqual(
                "modified=False", c.get("/empty_session").data.decode("utf-8")
            )
            # But after the request, the session could be modified by the
            # "after_request" handlers that call _update_remember_cookie.
            # Ensure that if nothing changed the session is not modified.
            self.assertFalse(session.modified)

    def test_invalid_remember_cookie(self):
        domain = self.app.config["REMEMBER_COOKIE_DOMAIN"] = ".localhost.local"
        c = self.app.test_client()
        c.get("/login-notch-remember")

        with c.session_transaction() as sess:
            sess["_user_id"] = None

        client_set_cookie(c, self.remember_cookie_name, "foo", domain=domain)
        result = c.get("/username")
        self.assertEqual("Anonymous", result.data.decode("utf-8"))

    #
    # Session Protection
    #
    def test_session_protection_basic_passes_successive_requests(self):
        self.app.config["SESSION_PROTECTION"] = "basic"
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            username_result = c.get("/username")
            self.assertEqual("Notch", username_result.data.decode("utf-8"))
            fresh_result = c.get("/is-fresh")
            self.assertEqual("True", fresh_result.data.decode("utf-8"))

    def test_session_protection_strong_passes_successive_requests(self):
        self.app.config["SESSION_PROTECTION"] = "strong"
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            username_result = c.get("/username")
            self.assertEqual("Notch", username_result.data.decode("utf-8"))
            fresh_result = c.get("/is-fresh")
            self.assertEqual("True", fresh_result.data.decode("utf-8"))

    def test_session_protection_basic_marks_session_unfresh(self):
        self.app.config["SESSION_PROTECTION"] = "basic"
        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            username_result = c.get("/username", headers=[("User-Agent", "different")])
            self.assertEqual("Notch", username_result.data.decode("utf-8"))
            fresh_result = c.get("/is-fresh")
            self.assertEqual("False", fresh_result.data.decode("utf-8"))

    def test_session_protection_basic_fires_signal(self):
        self.app.config["SESSION_PROTECTION"] = "basic"

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            with listen_to(session_protected) as listener:
                c.get("/username", headers=[("User-Agent", "different")])
                listener.assert_heard_one(self.app)

    def test_session_protection_basic_skips_when_remember_me(self):
        self.app.config["SESSION_PROTECTION"] = "basic"

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            # clear session to force remember me (and remove old session id)
            self._delete_session(c)
            # should not trigger protection because "sess" is empty
            with listen_to(session_protected) as listener:
                c.get("/username")
                listener.assert_heard_none(self.app)

    def test_session_protection_strong_skips_when_remember_me(self):
        self.app.config["SESSION_PROTECTION"] = "strong"

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            # clear session to force remember me (and remove old session id)
            self._delete_session(c)
            # should not trigger protection because "sess" is empty
            with listen_to(session_protected) as listener:
                c.get("/username")
                listener.assert_heard_none(self.app)

    def test_permanent_strong_session_protection_marks_session_unfresh(self):
        self.app.config["SESSION_PROTECTION"] = "strong"
        with self.app.test_client() as c:
            c.get("/login-notch-permanent")
            username_result = c.get("/username", headers=[("User-Agent", "different")])
            self.assertEqual("Notch", username_result.data.decode("utf-8"))
            fresh_result = c.get("/is-fresh")
            self.assertEqual("False", fresh_result.data.decode("utf-8"))

    def test_permanent_strong_session_protection_fires_signal(self):
        self.app.config["SESSION_PROTECTION"] = "strong"

        with self.app.test_client() as c:
            c.get("/login-notch-permanent")
            with listen_to(session_protected) as listener:
                c.get("/username", headers=[("User-Agent", "different")])
                listener.assert_heard_one(self.app)

    def test_session_protection_strong_deletes_session(self):
        self.app.config["SESSION_PROTECTION"] = "strong"
        with self.app.test_client() as c:
            # write some unrelated data in the session, to ensure it does not
            # get destroyed
            with c.session_transaction() as sess:
                sess["foo"] = "bar"
            c.get("/login-notch-remember")
            username_result = c.get("/username", headers=[("User-Agent", "different")])
            self.assertEqual("Anonymous", username_result.data.decode("utf-8"))
            with c.session_transaction() as sess:
                self.assertIn("foo", sess)
                self.assertEqual("bar", sess["foo"])

    def test_session_protection_strong_fires_signal_user_agent(self):
        self.app.config["SESSION_PROTECTION"] = "strong"

        with self.app.test_client() as c:
            c.get("/login-notch-remember")
            with listen_to(session_protected) as listener:
                c.get("/username", headers=[("User-Agent", "different")])
                listener.assert_heard_one(self.app)

    def test_session_protection_strong_fires_signal_x_forwarded_for(self):
        self.app.config["SESSION_PROTECTION"] = "strong"

        with self.app.test_client() as c:
            c.get("/login-notch-remember", headers=[("X-Forwarded-For", "10.1.1.1")])
            with listen_to(session_protected) as listener:
                c.get("/username", headers=[("X-Forwarded-For", "10.1.1.2")])
                listener.assert_heard_one(self.app)

    def test_session_protection_skip_when_off_and_anonymous(self):
        with self.app.test_client() as c:
            # no user access
            with listen_to(user_accessed) as user_listener:
                results = c.get("/")
                user_listener.assert_heard_none(self.app)

            # access user with no session data
            with listen_to(session_protected) as session_listener:
                results = c.get("/username")
                self.assertEqual(results.data.decode("utf-8"), "Anonymous")
                session_listener.assert_heard_none(self.app)

            # verify no session data has been set
            self.assertFalse(session)

    def test_session_protection_skip_when_basic_and_anonymous(self):
        self.app.config["SESSION_PROTECTION"] = "basic"

        with self.app.test_client() as c:
            # no user access
            with listen_to(user_accessed) as user_listener:
                results = c.get("/")
                user_listener.assert_heard_none(self.app)

            # access user with no session data
            with listen_to(session_protected) as session_listener:
                results = c.get("/username")
                self.assertEqual(results.data.decode("utf-8"), "Anonymous")
                session_listener.assert_heard_none(self.app)

            # verify no session data has been set
            self.assertFalse(session)

    #
    # Lazy Access User
    #
    def test_requests_without_accessing_session(self):
        with self.app.test_client() as c:
            c.get("/login-notch")

            # no session access
            with listen_to(user_accessed) as listener:
                c.get("/")
                listener.assert_heard_none(self.app)

            # should have a session access
            with listen_to(user_accessed) as listener:
                result = c.get("/username")
                listener.assert_heard_one(self.app)
                self.assertEqual(result.data.decode("utf-8"), "Notch")

    #
    # View Decorators
    #
    def test_login_required_decorator(self):
        @self.app.route("/protected")
        @login_required
        def protected():
            return "Access Granted"

        with self.app.test_client() as c:
            result = c.get("/protected")
            self.assertEqual(result.status_code, 401)

            c.get("/login-notch")
            result2 = c.get("/protected")
            self.assertIn("Access Granted", result2.data.decode("utf-8"))

    @unittest.skipIf(not hasattr(Flask, "ensure_sync"), "Flask version before async")
    def test_login_required_decorator_with_async(self):
        import asyncio

        @self.app.route("/protected")
        @login_required
        async def protected():
            await asyncio.sleep(0)
            return "Access Granted"

        with self.app.test_client() as c:
            self.app.config["LOGIN_DISABLED"] = True

            result = c.get("/protected")
            self.assertEqual(result.status_code, 200)

            self.app.config["LOGIN_DISABLED"] = False

            result = c.get("/protected")
            self.assertEqual(result.status_code, 401)

            c.get("/login-notch")
            result = c.get("/protected")
            self.assertEqual(result.status_code, 200)

            c.get("/login-notch")
            result2 = c.get("/protected")
            self.assertIn("Access Granted", result2.data.decode("utf-8"))

    def test_decorators_are_disabled(self):
        @self.app.route("/protected")
        @login_required
        @fresh_login_required
        def protected():
            return "Access Granted"

        self.app.config["LOGIN_DISABLED"] = True

        with self.app.test_client() as c:
            result = c.get("/protected")
            self.assertIn("Access Granted", result.data.decode("utf-8"))

    def test_fresh_login_required_decorator(self):
        @self.app.route("/very-protected")
        @fresh_login_required
        def very_protected():
            return "Access Granted"

        with self.app.test_client() as c:
            result = c.get("/very-protected")
            self.assertEqual(result.status_code, 401)

            c.get("/login-notch-remember")
            logged_in_result = c.get("/very-protected")
            self.assertEqual("Access Granted", logged_in_result.data.decode("utf-8"))

            self._delete_session(c)
            stale_result = c.get("/very-protected")
            self.assertEqual(stale_result.status_code, 401)

            c.get("/confirm-login")
            refreshed_result = c.get("/very-protected")
            self.assertEqual("Access Granted", refreshed_result.data.decode("utf-8"))

    #
    # Misc
    #
    def test_user_context_processor(self):
        with self.app.test_request_context():
            _ucp = self.app.context_processor(_user_context_processor)
            self.assertIsInstance(_ucp()["current_user"], AnonymousUserMixin)


class LoginViaRequestTestCase(unittest.TestCase):
    """Tests for LoginManager.request_loader."""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "deterministic"
        self.app.config["SESSION_PROTECTION"] = None
        self.remember_cookie_name = "remember"
        self.app.config["REMEMBER_COOKIE_NAME"] = self.remember_cookie_name
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)
        self.app.config["LOGIN_DISABLED"] = False

        @self.app.route("/")
        def index():
            return "Welcome!"

        @self.app.route("/login-notch")
        def login_notch():
            return str(login_user(notch))

        @self.app.route("/username")
        def username():
            if current_user.is_authenticated:
                return current_user.name
            return "Anonymous", 401

        @self.app.route("/logout")
        def logout():
            return str(logout_user())

        @self.login_manager.request_loader
        def load_user_from_request(request):
            user_id = request.args.get("user_id") or session.get("_user_id")
            try:
                user_id = int(float(user_id))
            except TypeError:
                pass
            return USERS.get(user_id)

        # This will help us with the possibility of typoes in the tests. Now
        # we shouldn't have to check each response to help us set up state
        # (such as login pages) to make sure it worked: we will always
        # get an exception raised (rather than return a 404 response)
        @self.app.errorhandler(404)
        def handle_404(e):
            raise e

        unittest.TestCase.setUp(self)

    def test_has_no_user_loader_callback(self):
        self.assertIsNone(self.login_manager._user_callback)

    def test_request_context_users_are_anonymous(self):
        with self.app.test_request_context():
            self.assertTrue(current_user.is_anonymous)

    def test_defaults_anonymous(self):
        with self.app.test_client() as c:
            result = c.get("/username")
            self.assertEqual(result.status_code, 401)

    def test_login_via_request(self):
        user_id = 2
        user_name = USERS[user_id].name
        with self.app.test_client() as c:
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual(user_name, result.data.decode("utf-8"))

    def test_login_via_request_uses_cookie_when_already_logged_in(self):
        user_id = 2
        user_name = notch.name
        with self.app.test_client() as c:
            c.get("/login-notch")
            url = "/username"
            result = c.get(url)
            self.assertEqual(user_name, result.data.decode("utf-8"))
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual("Steve", result.data.decode("utf-8"))

    def test_login_invalid_user_with_request(self):
        user_id = 9000
        with self.app.test_client() as c:
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual(result.status_code, 401)

    def test_login_invalid_user_with_request_when_already_logged_in(self):
        user_id = 9000
        with self.app.test_client() as c:
            url = "/login-notch"
            result = c.get(url)
            self.assertEqual("True", result.data.decode("utf-8"))
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual(result.status_code, 401)

    def test_login_user_with_request_does_not_modify_session(self):
        user_id = 2
        user_name = USERS[user_id].name
        with self.app.test_client() as c:
            url = f"/username?user_id={user_id}"
            result = c.get(url)
            self.assertEqual(user_name, result.data.decode("utf-8"))
            url = "/username"
            result = c.get(url)
            self.assertEqual("Anonymous", result.data.decode("utf-8"))


class TestLoginUrlGeneration(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)

        @self.app.route("/login")
        def login():
            return ""

    def test_make_next_param(self):
        with self.app.test_request_context():
            url = make_next_param("/login", "http://localhost/profile")
            self.assertEqual("/profile", url)

            url = make_next_param("https://localhost/login", "http://localhost/profile")
            self.assertEqual("http://localhost/profile", url)

            url = make_next_param(
                "http://accounts.localhost/login", "http://localhost/profile"
            )
            self.assertEqual("http://localhost/profile", url)

    def test_login_url_generation(self):
        with self.app.test_request_context():
            PROTECTED = "http://localhost/protected"

            self.assertEqual(
                "/login?n=%2Fprotected", login_url("/login", PROTECTED, "n")
            )

            url = login_url("/login", PROTECTED)
            self.assertEqual("/login?next=%2Fprotected", url)

            expected = (
                "https://auth.localhost/login?next=http%3A%2F%2Flocalhost%2Fprotected"
            )
            result = login_url("https://auth.localhost/login", PROTECTED)
            self.assertEqual(expected, result)

            self.assertEqual(
                "/login?affil=cgnu&next=%2Fprotected",
                login_url("/login?affil=cgnu", PROTECTED),
            )

    def test_login_url_generation_with_view(self):
        with self.app.test_request_context():
            self.assertEqual(
                "/login?next=%2Fprotected", login_url("login", "/protected")
            )

    def test_login_url_no_next_url(self):
        self.assertEqual(login_url("/foo"), "/foo")


class CookieEncodingTestCase(unittest.TestCase):
    def test_cookie_encoding(self):
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "deterministic"

        # COOKIE = u'1|7d276051c1eec578ed86f6b8478f7f7d803a7970'

        # Due to the restriction of 80 chars I have to break up the hash in two
        h1 = "0e9e6e9855fbe6df7906ec4737578a1d491b38d3fd5246c1561016e189d6516"
        h2 = "043286501ca43257c938e60aad77acec5ce916b94ca9d00c0bb6f9883ae4b82"
        h3 = "ae"
        COOKIE = "1|" + h1 + h2 + h3

        with app.test_request_context():
            self.assertEqual(COOKIE, encode_cookie("1"))
            self.assertEqual("1", decode_cookie(COOKIE))
            self.assertIsNone(decode_cookie("Foo|BAD_BASH"))
            self.assertIsNone(decode_cookie("no bar"))

    def test_cookie_encoding_with_key(self):
        app = Flask(__name__)
        app.config["SECRET_KEY"] = "not-used"
        key = "deterministic"

        # COOKIE = u'1|7d276051c1eec578ed86f6b8478f7f7d803a7970'

        # Due to the restriction of 80 chars I have to break up the hash in two
        h1 = "0e9e6e9855fbe6df7906ec4737578a1d491b38d3fd5246c1561016e189d6516"
        h2 = "043286501ca43257c938e60aad77acec5ce916b94ca9d00c0bb6f9883ae4b82"
        h3 = "ae"
        COOKIE = "1|" + h1 + h2 + h3

        with app.test_request_context():
            self.assertEqual(COOKIE, encode_cookie("1", key=key))
            self.assertEqual("1", decode_cookie(COOKIE, key=key))
            self.assertIsNone(decode_cookie("Foo|BAD_BASH", key=key))
            self.assertIsNone(decode_cookie("no bar", key=key))


class SecretKeyTestCase(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)

    def test_bytes(self):
        self.app.config["SECRET_KEY"] = b"\x9e\x8f\x14"
        with self.app.test_request_context():
            self.assertEqual(_secret_key(), b"\x9e\x8f\x14")

    def test_native(self):
        self.app.config["SECRET_KEY"] = "\x9e\x8f\x14"
        with self.app.test_request_context():
            self.assertEqual(_secret_key(), b"\x9e\x8f\x14")

    def test_default(self):
        self.assertEqual(_secret_key("\x9e\x8f\x14"), b"\x9e\x8f\x14")


class ImplicitIdUser(UserMixin):
    __slots__ = ()

    def __init__(self, id):
        self.id = id


class ExplicitIdUser(UserMixin):
    __slots__ = ()

    def __init__(self, name):
        self.name = name


class UserMixinTestCase(unittest.TestCase):
    def test_default_values(self):
        user = ImplicitIdUser(1)
        self.assertTrue(user.is_active)
        self.assertTrue(user.is_authenticated)
        self.assertFalse(user.is_anonymous)

    def test_get_id_from_id_attribute(self):
        user = ImplicitIdUser(1)
        self.assertEqual("1", user.get_id())

    def test_get_id_not_implemented(self):
        user = ExplicitIdUser("Notch")
        self.assertRaises(NotImplementedError, lambda: user.get_id())

    def test_equality(self):
        first = ImplicitIdUser(1)
        same = ImplicitIdUser(1)
        different = ImplicitIdUser(2)

        # Explicitly test the equality operator
        self.assertTrue(first == same)
        self.assertFalse(first == different)
        self.assertFalse(first != same)
        self.assertTrue(first != different)

        self.assertFalse(first == "1")
        self.assertTrue(first != "1")

    def test_hashable(self):
        self.assertTrue(isinstance(UserMixin(), Hashable))


class AnonymousUserTestCase(unittest.TestCase):
    def test_values(self):
        user = AnonymousUserMixin()

        self.assertFalse(user.is_active)
        self.assertFalse(user.is_authenticated)
        self.assertTrue(user.is_anonymous)
        self.assertIsNone(user.get_id())


class UnicodeCookieUserIDTestCase(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "deterministic"
        self.app.config["SESSION_PROTECTION"] = None
        self.remember_cookie_name = "remember"
        self.app.config["REMEMBER_COOKIE_NAME"] = self.remember_cookie_name
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)
        self.app.config["LOGIN_DISABLED"] = False

        @self.app.route("/")
        def index():
            return "Welcome!"

        @self.app.route("/login-germanjapanese-remember")
        def login_germanjapanese_remember():
            return str(login_user(germanjapanese, remember=True))

        @self.app.route("/username")
        def username():
            if current_user.is_authenticated:
                return current_user.name
            return "Anonymous"

        @self.app.route("/userid")
        def user_id():
            if current_user.is_authenticated:
                return current_user.id
            return "wrong_id"

        @self.login_manager.user_loader
        def load_user(user_id):
            return USERS[str(user_id)]

        # This will help us with the possibility of typoes in the tests. Now
        # we shouldn't have to check each response to help us set up state
        # (such as login pages) to make sure it worked: we will always
        # get an exception raised (rather than return a 404 response)
        @self.app.errorhandler(404)
        def handle_404(e):
            raise e

        unittest.TestCase.setUp(self)

    def _delete_session(self, c):
        # Helper method to cause the session to be deleted
        # as if the browser was closed. This will remove
        # the session regardless of the permanent flag
        # on the session!
        with c.session_transaction() as sess:
            sess.clear()

    def test_remember_me_username(self):
        with self.app.test_client() as c:
            c.get("/login-germanjapanese-remember")
            self._delete_session(c)
            result = c.get("/username")
            self.assertEqual("Müller", result.data.decode("utf-8"))

    def test_remember_me_user_id(self):
        with self.app.test_client() as c:
            c.get("/login-germanjapanese-remember")
            self._delete_session(c)
            result = c.get("/userid")
            self.assertEqual("佐藤", result.data.decode("utf-8"))


class StrictHostForRedirectsTestCase(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "deterministic"
        self.app.config["SESSION_PROTECTION"] = None
        self.remember_cookie_name = "remember"
        self.app.config["REMEMBER_COOKIE_NAME"] = self.remember_cookie_name
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)
        self.app.config["LOGIN_DISABLED"] = False

        @self.app.route("/secret")
        def secret():
            return self.login_manager.unauthorized()

        @self.app.route("/")
        def index():
            return "Welcome!"

        @self.login_manager.user_loader
        def load_user(user_id):
            return USERS[str(user_id)]

        # This will help us with the possibility of typoes in the tests. Now
        # we shouldn't have to check each response to help us set up state
        # (such as login pages) to make sure it worked: we will always
        # get an exception raised (rather than return a 404 response)
        @self.app.errorhandler(404)
        def handle_404(e):
            raise e

        unittest.TestCase.setUp(self)

    def test_unauthorized_uses_host_from_next_url(self):
        self.login_manager.login_view = "login"
        self.app.config["FORCE_HOST_FOR_REDIRECTS"] = None

        @self.app.route("/login")
        def login():
            return session.pop("next", "")

        with self.app.test_client() as c:
            result = c.get("/secret", base_url="http://foo.com")
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "/login?next=%2Fsecret")

    def test_unauthorized_uses_host_from_config_when_available(self):
        self.login_manager.login_view = "login"
        self.app.config["FORCE_HOST_FOR_REDIRECTS"] = "good.com"

        @self.app.route("/login")
        def login():
            return session.pop("next", "")

        with self.app.test_client() as c:
            result = c.get("/secret", base_url="http://bad.com")
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "//good.com/login?next=%2Fsecret")

    def test_unauthorized_uses_host_from_x_forwarded_for_header(self):
        self.login_manager.login_view = "login"
        self.app.config["FORCE_HOST_FOR_REDIRECTS"] = None
        self.app.wsgi_app = ProxyFix(self.app.wsgi_app, x_host=1)

        @self.app.route("/login")
        def login():
            return session.pop("next", "")

        with self.app.test_client() as c:
            headers = {
                "X-Forwarded-Host": "proxy.com",
            }
            result = c.get("/secret", base_url="http://foo.com", headers=headers)
            self.assertEqual(result.status_code, 302)
            self.assertEqual(result.location, "/login?next=%2Fsecret")

    def test_unauthorized_ignores_host_from_x_forwarded_for_header(self):
        self.login_manager.login_view = "login"
        self.app.config["FORCE_HOST_FOR_REDIRECTS"] = "good.com"

        @self.app.route("/login")
        def login():
            return session.pop("next", "")

        with self.app.test_client() as c:
            headers = {
                "X-Forwarded-Host": "proxy.com",
            }
            result = c.get("/secret", base_url="http://foo.com", headers=headers)
            self.assertEqual(result.status_code, 302)
            assert result.location == "//good.com/login?next=%2Fsecret"


class CustomTestClientTestCase(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["SECRET_KEY"] = "deterministic"
        self.app.config["SESSION_PROTECTION"] = None
        self.remember_cookie_name = "remember"
        self.app.config["REMEMBER_COOKIE_NAME"] = self.remember_cookie_name
        self.login_manager = LoginManager()
        self.login_manager.init_app(self.app)
        self.app.config["LOGIN_DISABLED"] = False
        self.app.test_client_class = FlaskLoginClient

        @self.app.route("/username")
        def username():
            if current_user.is_authenticated:
                return current_user.name
            return "Anonymous"

        @self.app.route("/is-fresh")
        def is_fresh():
            return str(login_fresh())

        @self.login_manager.user_loader
        def load_user(user_id):
            return USERS[int(user_id)]

        # This will help us with the possibility of typoes in the tests. Now
        # we shouldn't have to check each response to help us set up state
        # (such as login pages) to make sure it worked: we will always
        # get an exception raised (rather than return a 404 response)
        @self.app.errorhandler(404)
        def handle_404(e):
            raise e

        unittest.TestCase.setUp(self)

    def test_no_args_to_test_client(self):
        with self.app.test_client() as c:
            result = c.get("/username")
            self.assertEqual("Anonymous", result.data.decode("utf-8"))

    def test_user_arg_to_test_client(self):
        with self.app.test_client(user=notch) as c:
            username = c.get("/username")
            self.assertEqual("Notch", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("True", is_fresh.data.decode("utf-8"))

    def test_fresh_login_arg_to_test_client(self):
        with self.app.test_client(user=notch, fresh_login=False) as c:
            username = c.get("/username")
            self.assertEqual("Notch", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("False", is_fresh.data.decode("utf-8"))

    def test_session_protection_modes(self):
        # Disabled
        self.app.config["SESSION_PROTECTION"] = None
        with self.app.test_client(user=notch, fresh_login=False) as c:
            username = c.get("/username")
            self.assertEqual("Notch", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("False", is_fresh.data.decode("utf-8"))

        with self.app.test_client(user=notch, fresh_login=True) as c:
            username = c.get("/username")
            self.assertEqual("Notch", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("True", is_fresh.data.decode("utf-8"))

        # Enabled with mode: basic
        self.app.config["SESSION_PROTECTION"] = "basic"
        with self.app.test_client(user=notch, fresh_login=False) as c:
            username = c.get("/username")
            self.assertEqual("Notch", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("False", is_fresh.data.decode("utf-8"))

        with self.app.test_client(user=notch, fresh_login=True) as c:
            username = c.get("/username")
            self.assertEqual("Notch", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("False", is_fresh.data.decode("utf-8"))

        # Enabled with mode: strong
        self.app.config["SESSION_PROTECTION"] = "strong"
        with self.app.test_client(user=notch, fresh_login=False) as c:
            username = c.get("/username")
            self.assertEqual("Anonymous", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("False", is_fresh.data.decode("utf-8"))

        with self.app.test_client(user=notch, fresh_login=True) as c:
            username = c.get("/username")
            self.assertEqual("Anonymous", username.data.decode("utf-8"))
            is_fresh = c.get("/is-fresh")
            self.assertEqual("False", is_fresh.data.decode("utf-8"))
