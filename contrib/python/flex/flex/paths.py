import itertools
import collections
import functools
import re

from flex._compat import Mapping
from flex.exceptions import MultiplePathsFound
from flex.error_messages import MESSAGES
from flex.constants import (
    PATH,
    REQUEST_METHODS,
)
from flex.parameters import (
    find_parameter,
    merge_parameter_lists,
    dereference_parameter_list,
)


REGEX_REPLACEMENTS = (
    ('\.', '\.'),
    ('\{', '\{'),
    ('\}', '\}'),
)


def escape_regex_special_chars(api_path):
    """
    Turns the non prametrized path components into strings subtable for using
    as a regex pattern.  This primarily involves escaping special characters so
    that the actual character is matched in the regex.
    """
    def substitute(string, replacements):
        pattern, repl = replacements
        return re.sub(pattern, repl, string)

    return functools.reduce(substitute, REGEX_REPLACEMENTS, api_path)


# matches the parametrized parts of a path.
# eg. /{id}/ matches the `{id}` part of it.
PARAMETER_REGEX = re.compile('(\{[^\}]+})')


def construct_parameter_pattern(parameter):
    """
    Given a parameter definition returns a regex pattern that will match that
    part of the path.
    """
    name = parameter['name']
    type = parameter['type']

    repeated = '[^/]'

    if type == 'integer':
        repeated = '\d'

    return "(?P<{name}>{repeated}+)".format(name=name, repeated=repeated)


def process_path_part(part, parameters):
    """
    Given a part of a path either:
        - If it is a parameter:
            parse it to a regex group
        - Otherwise:
            escape any special regex characters
    """
    if PARAMETER_REGEX.match(part):
        parameter_name = part.strip('{}')
        try:
            parameter = find_parameter(
                parameters,
                name=parameter_name,
                in_=PATH
            )
        except ValueError:
            pass
        else:
            return construct_parameter_pattern(parameter)
    return escape_regex_special_chars(part)


def get_parameter_names_from_path(api_path):
    return tuple(p.strip('{}') for p in PARAMETER_REGEX.findall(api_path))


def path_to_pattern(api_path, parameters):
    """
    Given an api path, possibly with parameter notation, return a pattern
    suitable for turing into a regular expression which will match request
    paths that conform to the parameter definitions and the api path.
    """
    parts = re.split(PARAMETER_REGEX, api_path)
    pattern = ''.join((process_path_part(part, parameters) for part in parts))

    if not pattern.startswith('^'):
        pattern = "^{0}".format(pattern)
    if not pattern.endswith('$'):
        pattern = "{0}$".format(pattern)

    return pattern


def path_to_regex(api_path, path_parameters, operation_parameters=None,
                  context=None):
    if context is None:
        context = {}
    if operation_parameters is None:
        operation_parameters = []
    pattern = path_to_pattern(
        api_path=api_path,
        parameters=merge_parameter_lists(
            context.get('parameters', {}).values(),
            dereference_parameter_list(path_parameters, context),
            dereference_parameter_list(operation_parameters, context),
        ),
    )
    return re.compile(pattern)


def extract_path_parameters(path_definition):
    if not path_definition:
        return []
    return path_definition.get('parameters', [])


def extract_operation_parameters(path_definition):
    return list(itertools.chain.from_iterable(
        (v or {}).get('parameters', [])
        for k, v in path_definition.items()
        if k in REQUEST_METHODS
    ))


# Change multiple / to one /
# e.g., foo///bar//fiz/bar -> foo/bar/fiz/bar
NORMALIZE_SLASH_REGEX = re.compile(r"/+")


def match_path_to_api_path(path_definitions, target_path, base_path='',
                           context=None):
    """
    Match a request or response path to one of the api paths.

    Anything other than exactly one match is an error condition.
    """
    if context is None:
        context = {}
    assert isinstance(context, Mapping)
    if target_path.startswith(base_path):
        # Convert all of the api paths into Path instances for easier regex
        # matching.
        normalized_target_path = re.sub(NORMALIZE_SLASH_REGEX, '/',
                                        target_path)
        matching_api_paths = list()
        matching_api_paths_regex = list()
        for p, v in path_definitions.items():
            # Doing this to help with case where we might have base_path
            # being just /, and then the path starts with / as well.
            full_path = re.sub(NORMALIZE_SLASH_REGEX, '/', base_path + p)
            r = path_to_regex(
                api_path=full_path,
                path_parameters=extract_path_parameters(v),
                operation_parameters=extract_operation_parameters(v),
                context=context,
            )
            if full_path == normalized_target_path:
                matching_api_paths.append(p)
            elif r.match(normalized_target_path):
                matching_api_paths_regex.\
                    append((p, r.match(normalized_target_path)))

        # Keep it consistent with the previous behavior
        target_path = target_path[len(base_path):]
    else:
        matching_api_paths = []
        matching_api_paths_regex = []

    if not matching_api_paths and not matching_api_paths_regex:
        fstr = MESSAGES['path']['no_matching_paths_found'].format(target_path)
        raise LookupError(fstr)
    elif len(matching_api_paths) == 1:
        return matching_api_paths[0]
    elif len(matching_api_paths) > 1:
        raise MultiplePathsFound(
            MESSAGES['path']['multiple_paths_found'].format(
                target_path, [v[0] for v in matching_api_paths],
            )
        )
    elif len(matching_api_paths_regex) == 1:
        return matching_api_paths_regex[0][0]
    elif len(matching_api_paths_regex) > 1:
        # TODO: This area needs improved logic.
        # We check to see if any of the matched paths is longers than
        # the others.  If so, we *assume* it is the correct match.  This is
        # going to be prone to false positives. in certain cases.
        matches_by_path_size = collections.defaultdict(list)
        for path, match in matching_api_paths_regex:
            matches_by_path_size[len(path)].append(path)
        longest_match = max(matches_by_path_size.keys())
        if len(matches_by_path_size[longest_match]) == 1:
            return matches_by_path_size[longest_match][0]
        raise MultiplePathsFound(
            MESSAGES['path']['multiple_paths_found'].format(
                target_path, [v[0] for v in matching_api_paths_regex],
            )
        )
    else:
        return matching_api_paths_regex[0][0]
