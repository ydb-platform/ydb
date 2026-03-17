from __future__ import annotations

import typing

import flask
import marshmallow as ma

if typing.TYPE_CHECKING:
    from flask.wrappers import Response


class Schema(ma.Schema):
    """Base serializer with which to define custom serializers.

    See `marshmallow.Schema` for more details about the `Schema` API.
    """

    def jsonify(
        self, obj: typing.Any, many: bool | None = None, *args, **kwargs
    ) -> Response:
        """Return a JSON response containing the serialized data.


        :param obj: Object to serialize.
        :param bool many: Whether `obj` should be serialized as an instance
            or as a collection. If None, defaults to the value of the
            `many` attribute on this Schema.
        :param kwargs: Additional keyword arguments passed to `flask.jsonify`.

        .. versionchanged:: 0.6.0
            Takes the same arguments as `marshmallow.Schema.dump`. Additional
            keyword arguments are passed to `flask.jsonify`.

        .. versionchanged:: 0.6.3
            The `many` argument for this method defaults to the value of
            the `many` attribute on the Schema. Previously, the `many`
            argument of this method defaulted to False, regardless of the
            value of `Schema.many`.
        """
        if many is None:
            many = self.many
        data = self.dump(obj, many=many)
        return flask.jsonify(data, *args, **kwargs)
