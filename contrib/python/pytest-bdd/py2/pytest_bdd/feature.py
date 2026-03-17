"""Feature.

The way of describing the behavior is based on Gherkin language, but a very
limited version. It doesn't support any parameter tables.
If the parametrization is needed to generate more test cases it can be done
on the fixture level of the pytest.
The <variable> syntax can be used here to make a connection between steps and
it will also validate the parameters mentioned in the steps with ones
provided in the pytest parametrization table.

Syntax example:

    Feature: Articles
        Scenario: Publishing the article
            Given I'm an author user
            And I have an article
            When I go to the article page
            And I press the publish button
            Then I should not see the error message
            And the article should be published  # Note: will query the database

:note: The "#" symbol is used for comments.
:note: There're no multiline steps, the description of the step must fit in
one line.
"""
import os.path
import sys

import glob2

from .parser import parse_feature


# Global features dictionary
features = {}


def force_unicode(obj, encoding="utf-8"):
    """Get the unicode string out of given object (python 2 and python 3).

    :param obj: An `object`, usually a string.

    :return: unicode string.
    """
    if sys.version_info < (3, 0):
        if isinstance(obj, str):
            return obj.decode(encoding)
        else:
            return unicode(obj)
    else:  # pragma: no cover
        return str(obj)


def force_encode(string, encoding="utf-8"):
    """Force string encoding (Python compatibility function).

    :param str string: A string value.
    :param str encoding: Encoding.

    :return: Encoded string.
    """
    if sys.version_info < (3, 0):
        if isinstance(string, unicode):
            string = string.encode(encoding)
    return string


def get_feature(base_path, filename, encoding="utf-8"):
    """Get a feature by the filename.

    :param str base_path: Base feature directory.
    :param str filename: Filename of the feature file.
    :param str encoding: Feature file encoding.

    :return: `Feature` instance from the parsed feature cache.

    :note: The features are parsed on the execution of the test and
           stored in the global variable cache to improve the performance
           when multiple scenarios are referencing the same file.
    """

    full_name = os.path.abspath(os.path.join(base_path, filename))
    feature = features.get(full_name)
    if not feature:
        feature = parse_feature(base_path, filename, encoding=encoding)
        features[full_name] = feature
    return feature


def get_features(paths, **kwargs):
    """Get features for given paths.

    :param list paths: `list` of paths (file or dirs)

    :return: `list` of `Feature` objects.
    """
    seen_names = set()
    features = []
    for path in paths:
        if path not in seen_names:
            seen_names.add(path)
            if os.path.isdir(path):
                features.extend(get_features(glob2.iglob(os.path.join(path, "**", "*.feature")), **kwargs))
            else:
                base, name = os.path.split(path)
                feature = get_feature(base, name, **kwargs)
                features.append(feature)
    features.sort(key=lambda feature: feature.name or feature.filename)
    return features
