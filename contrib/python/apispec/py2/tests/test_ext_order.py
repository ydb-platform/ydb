# -*- coding: utf-8 -*-
import pytest
# from bottle import route
from flask import Flask
from flask.views import MethodView
from tornado.web import RequestHandler
from apispec import APISpec
# from apispec.ext.bottle import BottlePlugin
from apispec.ext.flask import FlaskPlugin
from apispec.ext.tornado import TornadoPlugin
from apispec.ext.marshmallow import MarshmallowPlugin

def create_spec(plugins):
    return APISpec(
        title='Swagger Petstore',
        version='1.0.0',
        description='This is a sample Petstore server.  You can find out more '
        'about Swagger at <a href=\"http://swagger.wordnik.com\">http://swagger.wordnik.com</a> '
        'or on irc.freenode.net, #swagger.  For this sample, you can use the api '
        'key \"special-key\" to test the authorization filters',
        plugins=plugins,
    )

def confirm_ext_order_independency(web_framework_plugin, **kwargs_for_add_path):
    extensions = [web_framework_plugin(), MarshmallowPlugin()]
    specs = []
    for reverse in (False, True):
        if reverse:
            spec = create_spec(reversed(extensions))
        else:
            spec = create_spec(extensions)
        spec.add_path(**kwargs_for_add_path)
        get_op = spec._paths['/hello']['get']
        assert get_op['description'] == 'get a greeting'
        assert isinstance(get_op['responses'][200]['schema'], dict)
        specs.append(spec)
    assert specs[0].to_dict() == specs[1].to_dict()


class TestExtOrder(object):

    @pytest.mark.skipif(True, reason='Bottle is not used by Yandex')
    def test_bottle(self):
        # @route('/hello')
        # def hello():
        #     """A greeting endpoint.
        #
        #     ---
        #     get:
        #         description: get a greeting
        #         responses:
        #             200:
        #                 description: a pet to be returned
        #                 schema: tests.schemas.PetSchema
        #     """
        #     return 'hi'
        #
        # confirm_ext_order_independency(BottlePlugin, view=hello)
        pass

    def test_flask(self):
        app = Flask(__name__)

        @app.route('/hello')
        def hello():
            """A greeting endpoint.

            ---
            get:
                description: get a greeting
                responses:
                    200:
                        description: a pet to be returned
                        schema: tests.schemas.PetSchema
            """
            return 'hi'

        with app.test_request_context():
            confirm_ext_order_independency(FlaskPlugin, view=hello)

    def test_flask_method_view(self):
        app = Flask(__name__)

        class HelloApi(MethodView):
            def get(self_):
                """A greeting endpoint.

                ---
                description: get a greeting
                responses:
                    200:
                        description: a pet to be returned
                        schema: tests.schemas.PetSchema
                """
                return 'hi'

        method_view = HelloApi.as_view('hi')
        app.add_url_rule('/hello', view_func=method_view)
        with app.test_request_context():
            confirm_ext_order_independency(FlaskPlugin, view=method_view)

    def test_tornado(self):
        class TornadoHelloHandler(RequestHandler):
            def get(self_):
                """A greeting endpoint.

                ---
                description: get a greeting
                responses:
                    200:
                        description: a pet to be returned
                        schema: tests.schemas.PetSchema
                """
                self_.write('hi')

        urlspec = (r'/hello', TornadoHelloHandler)
        confirm_ext_order_independency(TornadoPlugin, urlspec=urlspec)
