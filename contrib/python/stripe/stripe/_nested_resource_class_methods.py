from typing import List, Optional
from urllib.parse import quote_plus

from stripe._api_resource import APIResource


# TODO(major): 1704. Remove this. It is no longer used except for "nested_resource_url" and "nested_resource_request",
# which are unnecessary and deprecated and should also be removed.
def nested_resource_class_methods(
    resource: str,
    path: Optional[str] = None,
    operations: Optional[List[str]] = None,
    resource_plural: Optional[str] = None,
):
    if resource_plural is None:
        resource_plural = "%ss" % resource
    if path is None:
        path = resource_plural

    def wrapper(cls):
        def nested_resource_url(cls, id, nested_id=None):
            url = "%s/%s/%s" % (
                cls.class_url(),
                quote_plus(id),
                quote_plus(path),
            )
            if nested_id is not None:
                url += "/%s" % quote_plus(nested_id)
            return url

        resource_url_method = "%ss_url" % resource
        setattr(cls, resource_url_method, classmethod(nested_resource_url))

        def nested_resource_request(cls, method, url, **params):
            return APIResource._static_request(
                method,
                url,
                params=params,
            )

        resource_request_method = "%ss_request" % resource
        setattr(
            cls, resource_request_method, classmethod(nested_resource_request)
        )

        if operations is None:
            return cls

        for operation in operations:
            if operation == "create":

                def create_nested_resource(cls, id, **params):
                    url = getattr(cls, resource_url_method)(id)
                    return getattr(cls, resource_request_method)(
                        "post", url, **params
                    )

                create_method = "create_%s" % resource
                setattr(
                    cls, create_method, classmethod(create_nested_resource)
                )

            elif operation == "retrieve":

                def retrieve_nested_resource(cls, id, nested_id, **params):
                    url = getattr(cls, resource_url_method)(id, nested_id)
                    return getattr(cls, resource_request_method)(
                        "get", url, **params
                    )

                retrieve_method = "retrieve_%s" % resource
                setattr(
                    cls, retrieve_method, classmethod(retrieve_nested_resource)
                )

            elif operation == "update":

                def modify_nested_resource(cls, id, nested_id, **params):
                    url = getattr(cls, resource_url_method)(id, nested_id)
                    return getattr(cls, resource_request_method)(
                        "post", url, **params
                    )

                modify_method = "modify_%s" % resource
                setattr(
                    cls, modify_method, classmethod(modify_nested_resource)
                )

            elif operation == "delete":

                def delete_nested_resource(cls, id, nested_id, **params):
                    url = getattr(cls, resource_url_method)(id, nested_id)
                    return getattr(cls, resource_request_method)(
                        "delete", url, **params
                    )

                delete_method = "delete_%s" % resource
                setattr(
                    cls, delete_method, classmethod(delete_nested_resource)
                )

            elif operation == "list":

                def list_nested_resources(cls, id, **params):
                    url = getattr(cls, resource_url_method)(id)
                    return getattr(cls, resource_request_method)(
                        "get", url, **params
                    )

                list_method = "list_%s" % resource_plural
                setattr(cls, list_method, classmethod(list_nested_resources))

            else:
                raise ValueError("Unknown operation: %s" % operation)

        return cls

    return wrapper
