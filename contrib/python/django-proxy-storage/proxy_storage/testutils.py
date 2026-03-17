from mock import patch

from django.test import TestCase

from proxy_storage.meta_backends.orm import ORMMetaBackend
from proxy_storage.meta_backends.mongo import MongoMetaBackend


def override_proxy_storage_settings(**kwargs):
    return patch.multiple(
        'proxy_storage.settings.proxy_storage_settings',
        **kwargs
    )


def create_test_cases_for_proxy_storage(proxy_storage_class, test_case_bases, meta_backend_instances):
    test_cases = {}
    for test_case_base_tuple in test_case_bases:
        for meta_backend_instance in meta_backend_instances:
            if isinstance(meta_backend_instance, ORMMetaBackend):
                meta_backend_instance_marker = 'ORMMetaBackend_{}'.format(meta_backend_instance.model.__name__)
            elif isinstance(meta_backend_instance, MongoMetaBackend):
                meta_backend_instance_marker = 'MongoMetaBackend_{}__{}'.format(
                    meta_backend_instance.database.name,
                    meta_backend_instance.collection
                )
            else:
                raise Exception('You must create meta backend mark for {}'.format(type(meta_backend_instance)))
            new_test_case_class_name = '{0}_{1}_{2}'.format(
                test_case_base_tuple[0].__name__,
                proxy_storage_class.__name__,
                meta_backend_instance_marker
            )
            proxy_storage_instance = proxy_storage_class()
            proxy_storage_instance.meta_backend = meta_backend_instance

            # resulting test case
            new_test_case_class = type(
                new_test_case_class_name,
                test_case_base_tuple,
                {
                    'proxy_storage': proxy_storage_instance
                }
            )
            overrider = override_proxy_storage_settings(
                PROXY_STORAGE_CLASSES={
                    'some_proxy_storage_name': proxy_storage_class
                },
                PROXY_STORAGE_CLASSES_INVERTED={
                    proxy_storage_class: 'some_proxy_storage_name'
                }
            )
            new_test_case_class = overrider(new_test_case_class)
            if hasattr(new_test_case_class, 'setUp'):
                new_test_case_class.setUp = overrider(new_test_case_class.setUp)

            # add test case to locals
            test_cases[new_test_case_class_name] = new_test_case_class
    return test_cases