# -*- coding: utf-8 -*-

from .. import __version__


class BaseScore:
    """A base score class to derive from."""
    def __init__(self, score):
        self.score = score

    def format(self, width=2, score_only=False, signature=''):
        raise NotImplementedError()


class Signature:
    """A convenience class to represent sacreBLEU reproducibility signatures.

    :param args: The resulting `Namespace` returned from `parse_args()`.
    Argument-value pairs from command-line would then be directly added
    to the signature.
    """
    def __init__(self, args):
        # Copy the dictionary
        self.args = dict(args.__dict__)
        self.short = self.args.get('short', False)

        self._abbr = {
            'version': 'v',
            'test': 't',
            'lang': 'l',
            'subset': 'S',
            'origlang': 'o',
        }

        self.info = {
            # None's will be ignored
            'version': __version__,
            'test': self.args.get('test_set', None),
            'lang': self.args.get('langpair', None),
            'origlang': self.args.get('origlang', None),
            'subset': self.args.get('subset', None),
        }

    def __str__(self):
        """Returns a formatted signature string."""
        pairs = []
        for name in sorted(self.info.keys()):
            value = self.info[name]
            if value is not None:
                final_name = self._abbr[name] if self.short else name
                pairs.append('{}.{}'.format(final_name, value))

        return '+'.join(pairs)

    def __repr__(self):
        return self.__str__()
