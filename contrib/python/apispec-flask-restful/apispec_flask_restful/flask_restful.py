# -*- coding: utf-8 -*-
"""Flask-RESTful plugin"""

import logging
import re

import apispec
import yaml
from apispec import yaml_utils
from apispec.exceptions import APISpecError


def deduce_path(resource, **kwargs):
    """Find resource path using provided API or path itself"""
    api = kwargs.get("api", None)
    if not api:
        # flask-restful resource url passed
        return kwargs.get("path")

    # flask-restful API passed
    # Require MethodView
    if not getattr(resource, "endpoint", None):
        raise APISpecError("Flask-RESTful resource needed")

    if api.blueprint:
        # it is required to have Flask app to be able enumerate routes
        app = kwargs.get("app")
        if app:
            for rule in app.url_map.iter_rules():
                if rule.endpoint.endswith("." + resource.endpoint):
                    break
            else:
                raise APISpecError(
                    "Cannot find blueprint resource {}".format(resource.endpoint)
                )
        else:
            # Application not initialized yet, fallback to path
            return kwargs.get("path")

    else:
        for rule in api.app.url_map.iter_rules():
            if rule.endpoint == resource.endpoint:
                rule.endpoint.endswith("." + resource.endpoint)
                break
        else:
            raise APISpecError("Cannot find resource {}".format(resource.endpoint))

    return rule.rule


def parse_operations(resource, operations):
    """Parse operations for each method in a flask-restful resource"""
    for method in resource.methods:
        docstring = getattr(resource, method.lower()).__doc__
        if docstring:
            try:
                operation = yaml_utils.load_yaml_from_docstring(docstring)
            except yaml.YAMLError:
                operation = None
            if not operation:
                logging.getLogger(__name__).warning(
                    "Cannot load docstring for {}/{}".format(resource, method)
                )
            operations[method.lower()] = operation or {}


class RestfulPlugin(apispec.BasePlugin):
    """Extracts swagger spec from `flask-restful` methods."""

    def path_helper(self, path=None, operations=None, parameters=None, **kwargs):
        kwargs["path"] = path
        try:
            resource = kwargs.pop("resource")
            path = deduce_path(resource, **kwargs)
            path = re.sub(r"<(?:[^:<>]+:)?([^<>]+)>", r"{\1}", path)
            return path
        except Exception as exc:
            logging.getLogger(__name__).exception(
                "Exception parsing APISpec", exc_info=exc
            )
            raise

    def operation_helper(self, path=None, operations=None, **kwargs):
        if operations is None:
            return
        try:
            resource = kwargs.pop("resource")
            parse_operations(resource, operations)
        except Exception as exc:
            logging.getLogger(__name__).exception(
                "Exception parsing APISpec", exc_info=exc
            )
            raise
