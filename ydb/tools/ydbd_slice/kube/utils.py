# Description: Utility functions for working with Kubernetes manifests

from typing import List, Tuple, Dict, Generator


CLUSTER_RESOURCES = [
    'namespace'
]


def filter_manifests(manifests: List[Tuple[str, str, str, str, str, Dict]], api_version: str, kind: List[str]) -> Generator[Tuple[str, str, str, str, str, Dict], None, None]:
    """
    Filter manifests by API version and kind.

    Args:
        manifests (List[Tuple[str, str, str, str, str, Dict]]): A list of manifests where each manifest is represented as a tuple containing:
            - path (str): The file path of the manifest.
            - api_version (str): The API version of the manifest.
            - kind (str): The kind of the Kubernetes resource.
            - namespace (str): The namespace of the resource.
            - name (str): The name of the resource.
            - data (Dict): The manifest data as a dictionary.
        api_version (str): The API version to filter by.
        kind (List[str]): A list of kinds to filter by.

    Yields:
        Generator[Tuple[str, str, str, str, str, Dict], None, None]: A generator yielding tuples of manifests that match the specified API version and kind.
    """
    for (path, manifest_api_version, manifest_kind, namespace, name, data) in manifests:
        if manifest_api_version == api_version and manifest_kind in kind:
            yield (path, manifest_api_version, manifest_kind, namespace, name, data)


def is_kubernetes_manifest(data: Dict) -> bool:
    """
    Check if the given data is a Kubernetes manifest.

    Args:
        data (Dict): The data to check.

    Returns:
        bool: True if the data is a Kubernetes manifest, False otherwise.
    """
    required_keys = ['apiVersion', 'kind', 'metadata']
    valid_api_versions = ["ydb.tech/v1alpha1", "batch/v1"]

    # Check if all required keys are present in the data
    if all(key in data for key in required_keys):
        metadata = data['metadata']

        # Check if 'name' is present in metadata
        if 'name' in metadata:
            # Validate that 'apiVersion' is a non-empty string and an acceptable value
            api_version_valid = isinstance(data['apiVersion'], str) and data['apiVersion'] in valid_api_versions

            # Check that 'kind' is a non-empty string
            kind_valid = isinstance(data['kind'], str) and data['kind']

            # Validate that 'name' is a non-empty string
            name_valid = isinstance(metadata['name'], str) and metadata['name']

            # Check if 'namespace' is required and validate it
            if data['kind'].lower() not in CLUSTER_RESOURCES:
                namespace_valid = 'namespace' in metadata and isinstance(metadata['namespace'], str) and metadata['namespace']
            else:
                namespace_valid = True

            return api_version_valid and kind_valid and name_valid and namespace_valid
    return False
