import json
import yaml

from json import JSONDecodeError
from yaml import YAMLError

from .symbols import *
from .symbols import Symbol
from ._version import __version__

# rules
# - keys and strings which start with $ (or specified escape_str) are escaped to $$ (or escape_str * 2)
# - when source is dict and diff is a dict -> patch
# - when source is list and diff is a list patch dict -> patch
# - else -> replacement

class JsonDumper:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, obj, dest=None):
        if dest is None:
            return json.dumps(obj, **self.kwargs)
        else:
            return json.dump(obj, dest, **self.kwargs)


default_dumper = JsonDumper()


class YamlDumper:
    """Write object as YAML string"""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self,  obj, dest=None):
        """Format obj as a YAML string and optionally write to dest
        :param obj: dict to dump
        :param dest: file-like object
        :return: str
        """
        return yaml.dump(obj, dest, **self.kwargs)

class JsonLoader:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, src):
        """Parse and return JSON data
        :param src: str|file-like source
        :return: dict parsed data
        """
        if isinstance(src, str):
            return json.loads(src, **self.kwargs)
        else:
            return json.load(src, **self.kwargs)


default_loader = JsonLoader()


class YamlLoader:
    """Load YAML data from file-like object or string"""

    def __call__(self, src):
        """Parse and return YAML data
        :param src: str|file-like source
        :return: dict parsed data
        """
        return yaml.safe_load(src)

class Serializer:
    """Serializer helper loads and stores object data
    :param file_format: str json or yaml
    :param indent: int Output indentation in spaces
    :raise ValueError: file_path does not contains valid file_format data
    """

    def __init__(self, file_format, indent):
        # pyyaml _can_ load json but is ~20 times slower and has known issues so use
        # the json from stdlib when json is specified.
        self.serializers = {
            "json": (JsonLoader(), JsonDumper(indent=indent)),
            "yaml": (YamlLoader(), YamlDumper(indent=indent)),
        }
        self.file_format = file_format
        if file_format not in self.serializers:
            raise ValueError(f"Unsupported serialization format {file_format}, expected one of {self.serializers.keys()}")

    def deserialize_file(self, src):
        """Deserialize file from the specified format
        :param file_path: str path to file
        :param src: str|file-like source
        :return dict
        :raise ValueError: file_path does not contain valid file_format data
        """
        loader, _ = self.serializers[self.file_format]
        try:
            parsed = loader(src)
        except (JSONDecodeError, YAMLError) as ex:
            raise ValueError(f"Invalid {self.file_format} file") from ex
        return parsed

    def serialize_data(self, obj, stream):
        """Serialize obj and write to stream
        :param obj: dict to serialize
        :param stream: Writeable stream
        """
        _, dumper = self.serializers[self.file_format]
        dumper(obj, stream)


class JsonDiffSyntax:
    def emit_set_diff(self, a, b, s, added, removed):
        """
        Emits the difference between two sets.

        :param a: The original set.
        :param b: The modified set.
        :param s: The path to the current location in the JSON structure.
        :param added: Elements that were added to 'b'.
        :param removed: Elements that were removed from 'a'.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError()

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        """
        Emits the difference between two lists.

        :param a: The original list.
        :param b: The modified list.
        :param s: The path to the current location in the JSON structure.
        :param inserted: Index and value of elements inserted into 'b'.
        :param changed: Index, original value, and new value of elements that have changed.
        :param deleted: Index and value of elements that were deleted from 'a'.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError()

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        """
        Emits the difference between two dictionaries.

        :param a: The original dictionary.
        :param b: The modified dictionary.
        :param s: The path to the current location in the JSON structure.
        :param added: Key-value pairs that were added to 'b'.
        :param changed: Keys and their corresponding old and new values for items that have changed.
        :param removed: Keys of items that were removed from 'a'.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError()

    def emit_value_diff(self, a, b, s):
        """
        Emits the difference between two values.

        :param a: The original value.
        :param b: The modified value.
        :param s: The path to the current location in the JSON structure.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError()

    def patch(self, a, d):
        """
        Applies a patch to a JSON structure.

        :param a: The original JSON structure.
        :param d: The patch to apply.
        :return: The patched JSON structure.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError()

    def unpatch(self, a, d):
        """
        Reverses a patch on a JSON structure.

        :param a: The patched JSON structure.
        :param d: The patch that was applied.
        :return: The original JSON structure before the patch was applied.
        :raises NotImplementedError: This is an abstract method.
        """
        raise NotImplementedError()


class CompactJsonDiffSyntax:
    """
    Provides a compact syntax for JSON differences, focusing on minimizing the output size.
    This class is designed to emit and apply differences between two JSON structures in a compact form,
    making it suitable for scenarios where bandwidth or storage efficiency is critical.

    Example:
        Given two JSON structures, `a` and `b`:

        a = {"name": "Alice", "age": 30, "skills": ["Python", "Django"]}
        b = {"name": "Alice", "age": 31, "skills": ["Python", "Django", "Flask"]}

        The `emit_dict_diff` method would produce a compact diff like:

        {
            "age": 31,
            "skills": {"insert": [(2, "Flask")]}
        }

        This diff can then be applied to `a` using the `patch` method to obtain `b`.
    """

    def emit_set_diff(self, a, b, s, added, removed):
        """
        Emits a compact representation of the difference between two sets.

        :param a: The original set.
        :param b: The modified set.
        :param s: Similarity score between the two sets.
        :param added: Elements added to the original set.
        :param removed: Elements removed from the original set.
        :return: A dictionary representing the changes in a compact form.
        """
        if s == 0.0 or len(removed) == len(a):
            return {replace: b} if isinstance(b, dict) else b
        else:
            d = {}
            if removed:
                d[discard] = removed
            if added:
                d[add] = added
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        """
        Emits a compact representation of the difference between two lists.

        :param a: The original list.
        :param b: The modified list.
        :param s: Similarity score between the two lists.
        :param inserted: Elements inserted into the original list.
        :param changed: Elements changed in the original list.
        :param deleted: Elements deleted from the original list.
        :return: A dictionary representing the changes in a compact form.
        """
        if s == 0.0:
            return {replace: b} if isinstance(b, dict) else b
        elif s == 1.0 and not (inserted or changed or deleted):
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = [pos for pos, value in deleted]
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        """
        Emits a compact representation of the difference between two dictionaries.

        :param a: The original dictionary.
        :param b: The modified dictionary.
        :param s: Similarity score between the two dictionaries.
        :param added: Key-value pairs added to the original dictionary.
        :param changed: Key-value pairs changed in the original dictionary.
        :param removed: Keys removed from the original dictionary.
        :return: A dictionary representing the changes in a compact form.
        """
        if s == 0.0:
            return {replace: b} if isinstance(b, dict) else b
        elif s == 1.0 and not (added or changed or removed):
            return {}
        else:
            changed.update(added)
            if removed:
                changed[delete] = list(removed.keys())
            return changed

    def emit_value_diff(self, a, b, s):
        """
        Emits a compact representation of the difference between two values.

        :param a: The original value.
        :param b: The modified value.
        :param s: Similarity score between the two values.
        :return: A dictionary or value representing the change in a compact form.
        """
        if s == 1.0:
            return {}
        else:
            return {replace: b} if isinstance(b, dict) else b

    def patch(self, a, d):
        """
        Applies a compact diff to a JSON structure to produce the modified structure.

        :param a: The original JSON structure.
        :param d: The compact diff to apply.
        :return: The modified JSON structure after applying the diff.
        """
        if isinstance(d, dict):
            if not d:
                return a
            if replace in d:
                return d[replace]
            if isinstance(a, dict):
                a = dict(a)
                for k, v in d.items():
                    if k is delete:
                        for kdel in v:
                            del a[kdel]
                    else:
                        av = a.get(k, missing)
                        if av is missing:
                            a[k] = v
                        else:
                            a[k] = self.patch(av, v)
                return a
            elif isinstance(a, (list, tuple)):
                original_type = type(a)
                a = list(a)
                if delete in d:
                    for pos in d[delete]:
                        a.pop(pos)
                if insert in d:
                    for pos, value in d[insert]:
                        a.insert(pos, value)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        a[k] = self.patch(a[k], v)
                if original_type is not list:
                    a = original_type(a)
                return a
            elif isinstance(a, set):
                a = set(a)
                if discard in d:
                    for x in d[discard]:
                        a.discard(x)
                if add in d:
                    for x in d[add]:
                        a.add(x)
                return a
        return d


class ExplicitJsonDiffSyntax:
    """
    Provides an explicit syntax for JSON differences, focusing on clarity and readability.
    This class is designed to emit and apply differences between two JSON structures in a form that is easy to understand,
    making it suitable for scenarios where human readability of diffs is important.

    Example:
        Given two JSON structures, `a` and `b`:

        a = {"name": "Alice", "age": 30, "skills": ["Python", "Django"]}
        b = {"name": "Alice", "age": 31, "skills": ["Python", "Django", "Flask"]}

        The `emit_dict_diff` method would produce an explicit diff like:

        {
            "age": 31,
            "skills": {"insert": [(2, "Flask")]}
        }

        Unlike the compact syntax, this explicit form prioritizes readability and ease of understanding over minimizing size.
        This diff can then be applied to `a` using the `patch` method to obtain `b`.
    """

    def emit_set_diff(self, a, b, s, added, removed):
        """
        Emits an explicit representation of the difference between two sets.

        :param a: The original set.
        :param b: The modified set.
        :param s: Similarity score between the two sets.
        :param added: Elements added to the original set.
        :param removed: Elements removed from the original set.
        :return: A dictionary representing the changes in an explicit form.
        """
        if s == 0.0 or len(removed) == len(a):
            return b
        else:
            d = {}
            if removed:
                d[discard] = removed
            if added:
                d[add] = added
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        """
        Emits an explicit representation of the difference between two lists.

        :param a: The original list.
        :param b: The modified list.
        :param s: Similarity score between the two lists.
        :param inserted: Elements inserted into the original list.
        :param changed: Elements changed in the original list.
        :param deleted: Elements deleted from the original list.
        :return: A dictionary representing the changes in an explicit form.
        """
        if s == 0.0 and not (inserted or changed or deleted):
            return b
        elif s == 1.0 and not (inserted or changed or deleted):
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = [pos for pos, value in deleted]
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        """
        Emits an explicit representation of the difference between two dictionaries.

        :param a: The original dictionary.
        :param b: The modified dictionary.
        :param s: Similarity score between the two dictionaries.
        :param added: Key-value pairs added to the original dictionary.
        :param changed: Key-value pairs changed in the original dictionary.
        :param removed: Keys removed from the original dictionary.
        :return: A dictionary representing the changes in an explicit form.
        """
        if s == 0.0 and not (added or changed or removed):
            return b
        elif s == 1.0 and not (added or changed or removed):
            return {}
        else:
            d = {}
            if added:
                d[insert] = added
            if changed:
                d[update] = changed
            if removed:
                d[delete] = list(removed.keys())
            return d

    def emit_value_diff(self, a, b, s):
        """
        Emits an explicit representation of the difference between two values.

        :param a: The original value.
        :param b: The modified value.
        :param s: Similarity score between the two values.
        :return: A dictionary or value representing the change in an explicit form.
        """
        if s == 1.0:
            return {}
        else:
            return b


class SymmetricJsonDiffSyntax:
    """
    Provides a symmetric syntax for JSON differences, focusing on maintaining both original and modified values.
    This class is designed to emit differences between two JSON structures in a way that both the original and modified
    values are kept, making it suitable for scenarios where tracking both versions of the data is important.

    Example:
        Given two JSON structures, `a` and `b`:

        a = {"name": "Alice", "age": 30, "skills": ["Python", "Django"]}
        b = {"name": "Alice", "age": 31, "skills": ["Python", "Django", "Flask"]}

        The `emit_dict_diff` method would produce a symmetric diff like:

        {
            "age": [30, 31],
            "skills": {"insert": [(2, "Flask")]}
        }

        This diff maintains both the original and modified values for the age field, and clearly shows the insertion
        in the skills list. This format is particularly useful for applications that need to display or process both
        versions of the data.

        The `patch` and `unpatch` methods can apply and reverse these diffs, respectively, allowing for flexible
        data manipulation.
    """

    def emit_set_diff(self, a, b, s, added, removed):
        """
        Emits a symmetric representation of the difference between two sets.

        :param a: The original set.
        :param b: The modified set.
        :param s: Similarity score between the two sets.
        :param added: Elements added to the original set.
        :param removed: Elements removed from the original set.
        :return: A dictionary representing the changes in a symmetric form.
        """
        if s == 0.0 or len(removed) == len(a):
            return [a, b]
        else:
            d = {}
            if added:
                d[add] = added
            if removed:
                d[discard] = removed
            return d

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        """
        Emits a symmetric representation of the difference between two lists.

        :param a: The original list.
        :param b: The modified list.
        :param s: Similarity score between the two lists.
        :param inserted: Elements inserted into the original list.
        :param changed: Elements changed in the original list.
        :param deleted: Elements deleted from the original list.
        :return: A dictionary representing the changes in a symmetric form.
        """
        if s == 0.0 and not (inserted or changed or deleted):
            return [a, b]
        elif s == 1.0 and not (inserted or changed or deleted):
            return {}
        else:
            d = changed
            if inserted:
                d[insert] = inserted
            if deleted:
                d[delete] = deleted
            return d

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        """
        Emits a symmetric representation of the difference between two dictionaries.

        :param a: The original dictionary.
        :param b: The modified dictionary.
        :param s: Similarity score between the two dictionaries.
        :param added: Key-value pairs added to the original dictionary.
        :param changed: Key-value pairs changed in the original dictionary.
        :param removed: Keys removed from the original dictionary.
        :return: A dictionary representing the changes in a symmetric form.
        """
        if s == 0.0 and not (added or changed or removed):
            return [a, b]
        elif s == 1.0 and not (added or changed or removed):
            return {}
        else:
            d = changed
            if added:
                d[insert] = added
            if removed:
                d[delete] = removed
            return d

    def emit_value_diff(self, a, b, s):
        """
        Emits a symmetric representation of the difference between two values.

        :param a: The original value.
        :param b: The modified value.
        :param s: Similarity score between the two values.
        :return: A list containing the original and modified values.
        """
        if s == 1.0:
            return {}
        else:
            return [a, b]

    def patch(self, a, d):
        """
        Applies a symmetric diff to a JSON structure to produce the modified structure.

        :param a: The original JSON structure.
        :param d: The symmetric diff to apply.
        :return: The modified JSON structure after applying the diff.
        """
        if isinstance(d, list):
            _, b = d
            return b
        elif isinstance(d, dict):
            if not d:
                return a
            if isinstance(a, dict):
                a = dict(a)
                for k, v in d.items():
                    if k is delete:
                        for kdel, _ in v.items():
                            del a[kdel]
                    elif k is insert:
                        for kk, vv in v.items():
                            a[kk] = vv
                    else:
                        a[k] = self.patch(a[k], v)
                return a
            elif isinstance(a, (list, tuple)):
                original_type = type(a)
                a = list(a)
                if delete in d:
                    for pos, value in d[delete]:
                        a.pop(pos)
                if insert in d:
                    for pos, value in d[insert]:
                        a.insert(pos, value)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        a[k] = self.patch(a[k], v)
                if original_type is not list:
                    a = original_type(a)
                return a
            elif isinstance(a, set):
                a = set(a)
                if discard in d:
                    for x in d[discard]:
                        a.discard(x)
                if add in d:
                    for x in d[add]:
                        a.add(x)
                return a
        raise Exception("Invalid symmetric diff")

    def unpatch(self, b, d):
        """
        Reverses a symmetric diff on a JSON structure to produce the original structure.

        :param b: The modified JSON structure.
        :param d: The symmetric diff that was applied.
        :return: The original JSON structure before the diff was applied.
        """
        if isinstance(d, list):
            a, _ = d
            return a
        elif isinstance(d, dict):
            if not d:
                return b
            if isinstance(b, dict):
                b = dict(b)
                for k, v in d.items():
                    if k is delete:
                        for kk, vv in v.items():
                            b[kk] = vv
                    elif k is insert:
                        for kk, vv in v.items():
                            del b[kk]
                    else:
                        b[k] = self.unpatch(b[k], v)
                return b
            elif isinstance(b, (list, tuple)):
                original_type = type(b)
                b = list(b)
                for k, v in d.items():
                    if k is not delete and k is not insert:
                        k = int(k)
                        b[k] = self.unpatch(b[k], v)
                if insert in d:
                    for pos, value in reversed(d[insert]):
                        b.pop(pos)
                if delete in d:
                    for pos, value in reversed(d[delete]):
                        b.insert(pos, value)
                if original_type is not list:
                    b = original_type(b)
                return b
            elif isinstance(b, set):
                b = set(b)
                if discard in d:
                    for x in d[discard]:
                        b.add(x)
                if add in d:
                    for x in d[add]:
                        b.discard(x)
                return b
        raise Exception("Invalid symmetric diff")


class RightOnlyJsonDiffSyntax(CompactJsonDiffSyntax):
    """
    Extends CompactJsonDiffSyntax to focus exclusively on the right (modified) values for lists,
    suitable for scenarios where only the latest state matters, ignoring the specific changes that led there.
    Compare to the CompactJsonDiffSyntax, I will not compare the difference in list,
    because in some senario we only care about the right value (in most cases means latest value).
    Instead, I will pop the later list value.

    Example:
        Given two JSON structures, `a` and `b`:

        a = {"name": "Alice", "age": 30, "skills": ["Python", "Django"]}
        b = {"name": "Alice", "age": 31, "skills": ["Python", "Django", "Flask"]}

        The `emit_dict_diff` method would produce a diff focusing on the updated and added fields:

        {
            "age": 31,
            "skills": ["Python", "Django", "Flask"]
        }

        And the `emit_list_diff` method directly returns the modified list without detailing the individual changes:

        ["Python", "Django", "Flask"]

        This approach simplifies the diff when the path from `a` to `b` is not as relevant as the final state represented by `b`.
    """

    def emit_dict_diff(self, a, b, s, added, changed, removed):
        """
        Emits a diff for dictionaries focusing on the final state, combining added and changed fields, and listing removed keys.

        :param a: The original dictionary.
        :param b: The modified dictionary.
        :param s: Similarity score between the two dictionaries.
        :param added: Key-value pairs added to the original dictionary.
        :param changed: Key-value pairs changed in the original dictionary.
        :param removed: Keys removed from the original dictionary.
        :return: A dictionary representing the final state or changes in a compact form.
        """
        if s == 1.0:
            return {}
        else:
            changed.update(added)
            if removed:
                changed[delete] = list(removed.keys())
            return changed

    def emit_list_diff(self, a, b, s, inserted, changed, deleted):
        """
        Directly returns the modified list, disregarding the specifics of how it was altered from the original list.

        :param a: The original list.
        :param b: The modified list.
        :param s: Similarity score between the two lists.
        :param inserted: Elements inserted into the original list.
        :param changed: Elements changed in the original list.
        :param deleted: Elements deleted from the original list.
        :return: The modified list as the final state.
        """
        if s == 0.0:
            return b
        elif s == 1.0:
            return {}
        else:
            return b


builtin_syntaxes = {
    'compact': CompactJsonDiffSyntax(),
    'symmetric': SymmetricJsonDiffSyntax(),
    'explicit': ExplicitJsonDiffSyntax(),
    'rightonly': RightOnlyJsonDiffSyntax(),
}


class JsonDiffer:
    """
    A class for computing differences between two JSON structures and applying patches based on these differences.

    Attributes:
        options (Options): Configuration options for the differ.
        _symbol_map (dict): A mapping of escaped symbols to their Symbol instances.

    Methods:
        diff(a, b, fp=None): Computes the difference between two JSON structures.
        similarity(a, b): Calculates the similarity score between two JSON structures.
        patch(a, d, fp=None): Applies a diff to a JSON structure to produce the modified structure.
        unpatch(b, d, fp=None): Reverses a diff on a JSON structure to produce the original structure.
        _unescape(x): Unescapes a string that has been escaped.
        unmarshal(d): Converts a marshaled (potentially escaped) structure back to its original form.
        _escape(o): Escapes a string or symbol that needs escaping.
        marshal(d): Converts a structure to a marshaled (potentially escaped) form.
    """
    class Options:
        """
        A placeholder class for options used by JsonDiffer. Options include syntax, load, dump, marshal,
        loader, dumper, and escape_str.
        """
        pass

    def __init__(self, syntax='compact', load=False, dump=False, marshal=False,
                 loader=default_loader, dumper=default_dumper, escape_str='$'):
        """
        Initializes the JsonDiffer with specified options.

        :param syntax: The syntax to use for diffs. Defaults to 'compact'.
        :param load: Whether to automatically load JSON from strings or files.
        :param dump: Whether to automatically dump output to JSON strings or files.
        :param marshal: Whether to marshal diffs to handle special characters.
        :param loader: Custom function for loading JSON data.
        :param dumper: Custom function for dumping JSON data.
        :param escape_str: String used to escape special characters in keys.
        """
        self.options = JsonDiffer.Options()
        self.options.syntax = builtin_syntaxes.get(syntax, syntax)
        self.options.load = load
        self.options.dump = dump
        self.options.marshal = marshal
        self.options.loader = loader
        self.options.dumper = dumper
        self.options.escape_str = escape_str
        self._symbol_map = {
            escape_str + symbol.label: symbol
            for symbol in _all_symbols_
        }

    def _list_diff_0(self, C, X, Y):
        """
        Helper method for computing list differences using dynamic programming.
        """
        i, j = len(X), len(Y)
        r = []
        while True:
            if i > 0 and j > 0:
                d, s = self._obj_diff(X[i-1], Y[j-1])
                if s > 0 and C[i][j] == C[i-1][j-1] + s:
                    r.append((0, d, j-1, s))
                    i, j = i - 1, j - 1
                    continue
            if j > 0 and (i == 0 or C[i][j-1] >= C[i-1][j]):
                r.append((1, Y[j-1], j-1, 0.0))
                j = j - 1
                continue
            if i > 0 and (j == 0 or C[i][j-1] < C[i-1][j]):
                r.append((-1, X[i-1], i-1, 0.0))
                i = i - 1
                continue
            return reversed(r)

    def _list_diff(self, X, Y):
        """
        Computes the difference between two lists.
        """
        # LCS
        m = len(X)
        n = len(Y)
        # An (m+1) times (n+1) matrix
        C = [[0 for j in range(n+1)] for i in range(m+1)]
        for i in range(1, m+1):
            for j in range(1, n+1):
                _, s = self._obj_diff(X[i-1], Y[j-1])
                # Following lines are part of the original LCS algorithm
                # left in the code in case modification turns out to be problematic
                #if X[i-1] == Y[j-1]:
                #    C[i][j] = C[i-1][j-1] + 1
                #else:
                C[i][j] = max(C[i][j-1], C[i-1][j], C[i-1][j-1] + s)
        inserted = []
        deleted = []
        changed = {}
        tot_s = 0.0

        for sign, value, pos, s in self._list_diff_0(C, X, Y):
            if sign == 1:
                inserted.append((pos, value))
            elif sign == -1:
                deleted.insert(0, (pos, value))
            elif sign == 0 and s < 1:
                changed[pos] = value
            tot_s += s
        tot_n = len(X) + len(inserted)
        if tot_n == 0:
            s = 1.0
        else:
            s = tot_s / tot_n
        return self.options.syntax.emit_list_diff(X, Y, s, inserted, changed, deleted), s

    def _set_diff(self, a, b):
        """
        Computes the difference between two sets.
        """
        removed = a.difference(b)
        added = b.difference(a)
        if not removed and not added:
            return {}, 1.0
        ranking = sorted(
            (
                (self._obj_diff(x, y)[1], x, y)
                for x in removed
                for y in added
            ),
            reverse=True,
            key=lambda x: x[0]
        )
        r2 = set(removed)
        a2 = set(added)
        n_common = len(a) - len(removed)
        s_common = float(n_common)
        for s, x, y in ranking:
            if x in r2 and y in a2:
                r2.discard(x)
                a2.discard(y)
                s_common += s
                n_common += 1
            if not r2 or not a2:
                break
        n_tot = len(a) + len(added)
        s = s_common / n_tot if n_tot != 0 else 1.0
        return self.options.syntax.emit_set_diff(a, b, s, added, removed), s

    def _dict_diff(self, a, b, exclude_paths, path):
        """
        Computes the difference between two dictionaries.
        """
        removed = {}
        nremoved = 0
        nadded = 0
        nmatched = 0
        smatched = 0.0
        added = {}
        changed = {}
        for k, v in a.items():
            new_path = f'{path}.{k}' if path else k
            if new_path in exclude_paths:
                continue
            w = b.get(k, missing)
            if w is missing:
                nremoved += 1
                removed[k] = v
            else:
                nmatched += 1
                d, s = self._obj_diff(v, w, exclude_paths, new_path)
                if s < 1.0:
                    changed[k] = d
                smatched += 0.5 + 0.5 * s
        for k, v in b.items():
            if k not in a:
                new_path = f'{path}.{k}' if path else k
                if new_path in exclude_paths:
                    continue
                nadded += 1
                added[k] = v
        n_tot = nremoved + nmatched + nadded
        s = smatched / n_tot if n_tot != 0 else 1.0
        return self.options.syntax.emit_dict_diff(a, b, s, added, changed, removed), s

    def _obj_diff(self, a, b, exclude_paths=None, path=''):
        """
        Computes the difference between any two JSON-compatible objects.
        """
        if not exclude_paths:
            exclude_paths = []
        if path in exclude_paths:
            return {}, 1.0
        if a is b:
            return self.options.syntax.emit_value_diff(a, b, 1.0), 1.0
        if isinstance(a, dict) and isinstance(b, dict):
            return self._dict_diff(a, b, exclude_paths, path)
        elif isinstance(a, tuple) and isinstance(b, tuple):
            return self._list_diff(a, b)
        elif isinstance(a, list) and isinstance(b, list):
            return self._list_diff(a, b)
        elif isinstance(a, set) and isinstance(b, set):
            return self._set_diff(a, b)
        elif a != b:
            return self.options.syntax.emit_value_diff(a, b, 0.0), 0.0
        else:
            return self.options.syntax.emit_value_diff(a, b, 1.0), 1.0

    def diff(self, a, b, fp=None, exclude_paths: list = None) -> dict:
        """
        Computes the difference between two JSON structures.
        :param a: The original JSON structure.
        :param b: The modified JSON structure.
        :param fp: Optional file pointer to dump the diff to.
        :param exclude_paths: Optional list of string paths to exclude from the diff.
        """
        if not exclude_paths:
            exclude_paths = []
        if self.options.load:
            a = self.options.loader(a)
            b = self.options.loader(b)

        d, s = self._obj_diff(a, b, exclude_paths)

        if self.options.marshal or self.options.dump:
            d = self.marshal(d)

        if self.options.dump:
            return self.options.dumper(d, fp)
        else:
            return d

    def similarity(self, a, b):
        """
        Calculates the similarity score between two JSON structures.
        """
        if self.options.load:
            a = self.options.loader(a)
            b = self.options.loader(b)

        d, s = self._obj_diff(a, b)

        return s

    def patch(self, a, d, fp=None):
        """
        Applies a diff to a JSON structure to produce the modified structure.
        """
        if self.options.load:
            a = self.options.loader(a)
            d = self.options.loader(d)

        if self.options.marshal or self.options.load:
            d = self.unmarshal(d)

        b = self.options.syntax.patch(a, d)

        if self.options.dump:
            return self.options.dumper(b, fp)
        else:
            return b

    def unpatch(self, b, d, fp=None):
        """
        Reverses a diff on a JSON structure to produce the original structure.
        """
        if self.options.load:
            b = self.options.loader(b)
            d = self.options.loader(d)

        if self.options.marshal or self.options.load:
            d = self.unmarshal(d)

        a = self.options.syntax.unpatch(b, d)

        if self.options.dump:
            return self.options.dumper(a, fp)
        else:
            return a

    def _unescape(self, x):
        """
        Unescapes a string that has been escaped.
        """
        if isinstance(x, str):
            sym = self._symbol_map.get(x, None)
            if sym is not None:
                return sym
            if x.startswith(self.options.escape_str):
                return x[1:]
        return x

    def unmarshal(self, d):
        """
        Converts a marshaled (potentially escaped) structure back to its original form.
        """
        if isinstance(d, dict):
            return {
                self._unescape(k): self.unmarshal(v)
                for k, v in d.items()
            }
        elif isinstance(d, (list, tuple)):
            return type(d)(
                self.unmarshal(x)
                for x in d
            )
        else:
            return self._unescape(d)

    def _escape(self, o):
        """
        Escapes a string or symbol that needs escaping.
        """
        if type(o) is Symbol:
            return self.options.escape_str + o.label
        if isinstance(o, str) and o.startswith(self.options.escape_str):
            return self.options.escape_str + o
        return o

    def marshal(self, d):
        """
        Converts a structure to a marshaled (potentially escaped) form.
        """
        if isinstance(d, dict):
            return {
                self._escape(k): self.marshal(v)
                for k, v in d.items()
            }
        elif isinstance(d, (list, tuple)):
            return type(d)(
                self.marshal(x)
                for x in d
            )
        else:
            return self._escape(d)


def diff(a, b, fp=None, cls=JsonDiffer, **kwargs):
    """
    Computes the difference between two JSON structures using a specified JsonDiffer class.

    :param a: The original JSON structure.
    :param b: The modified JSON structure.
    :param fp: Optional file pointer to dump the diff to.
    :param cls: The JsonDiffer class or subclass to use for computing the diff.
    :param kwargs: Additional keyword arguments to pass to the JsonDiffer constructor.
    :return: The computed diff.
    """
    return cls(**kwargs).diff(a, b, fp)


def patch(a, d, fp=None, cls=JsonDiffer, **kwargs):
    """
    Applies a diff to a JSON structure to produce the modified structure using a specified JsonDiffer class.

    :param a: The original JSON structure.
    :param d: The diff to apply.
    :param fp: Optional file pointer to dump the patched structure to.
    :param cls: The JsonDiffer class or subclass to use for applying the diff.
    :param kwargs: Additional keyword arguments to pass to the JsonDiffer constructor.
    :return: The patched JSON structure.
    """
    return cls(**kwargs).patch(a, d, fp)


def similarity(a, b, cls=JsonDiffer, **kwargs):
    """
    Calculates the similarity score between two JSON structures using a specified JsonDiffer class.

    :param a: The first JSON structure.
    :param b: The second JSON structure.
    :param cls: The JsonDiffer class or subclass to use for calculating similarity.
    :param kwargs: Additional keyword arguments to pass to the JsonDiffer constructor.
    :return: A similarity score as a float between 0.0 and 1.0.
    """
    return cls(**kwargs).similarity(a, b)


__all__ = [
    "similarity",
    "diff",
    "JsonDiffer",
    "JsonDumper",
    "JsonLoader",
    "YamlDumper",
    "YamlLoader",
    "Serializer",
]
