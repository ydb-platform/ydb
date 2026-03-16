"""
Transform filter query into QuerySet
"""
from scim2_filter_parser.queries.sql import SQLQuery

from .utils import get_group_model, get_user_model


class FilterQuery:
    model_getter = None
    joins = ()
    attr_map = None
    query_class = SQLQuery

    @classmethod
    def table_name(cls):
        return cls.model_getter()._meta.db_table

    @classmethod
    def search(cls, filter_query, request=None):
        q = cls.query_class(filter_query, cls.table_name(), cls.attr_map, cls.joins)
        if q.where_sql is None:
            return cls.model_getter().objects.none()

        sql, params = cls.get_raw_args(q, request)

        return cls.model_getter().objects.raw(sql, params)

    @classmethod
    def get_raw_args(cls, q, request=None):
        """
        Return a Query object's SQL augmented with params from cls.get_extras.
        """
        sql, params = q.sql, q.params

        extra_sql, extra_params = cls.get_extras(q, request)
        if extra_sql:
            if "'%s'" in extra_sql:
                raise ValueError(
                    'Dangerous use of quotes around place holder. Please see '
                    'https://docs.djangoproject.com/en/2.2/ref/models/querysets/#extra '
                    'for more details.'
                )

            sql = sql.rstrip(';') + extra_sql + ';'
            params += extra_params

        return sql, params

    @classmethod
    def get_extras(cls, q, request=None) -> (str, list):
        """
        Return extra SQL and params to be attached to end of current Query's
        SQL and params.

        For example:
            return 'AND tenant_id = %s', [request.user.tenant_id]
        """
        return '', []


class UserFilterQuery(FilterQuery):
    model_getter = get_user_model
    attr_map = {
        # attr, sub attr, uri
        ('userName', None, None): 'username',
        ('name', 'familyName', None): 'last_name',
        ('familyName', None, None): 'last_name',
        ('name', 'givenName', None): 'first_name',
        ('givenName', None, None): 'first_name',
        ('active', None, None): 'is_active',
    }


class GroupFilterQuery(FilterQuery):
    model_getter = get_group_model
    attr_map = {}
