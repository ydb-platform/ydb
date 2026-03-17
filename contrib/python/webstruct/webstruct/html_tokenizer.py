# -*- coding: utf-8 -*-
"""
:mod:`webstruct.html_tokenizer` contains :class:`HtmlTokenizer` class
which allows to extract text from a web page and tokenize it, preserving
information about token position in HTML tree
(token + its tree position = :class:`HtmlToken`). :class:`HtmlTokenizer`
also allows to extract annotations from the tree (if present) and split
them from regular text/tokens.
"""

from __future__ import absolute_import, print_function
import re
import copy
from itertools import groupby
from collections import namedtuple
from six.moves import zip

from lxml.etree import iterwalk

from webstruct.sequence_encoding import IobEncoder
from webstruct.text_tokenizers import tokenize, TextToken
from webstruct.utils import (
    replace_html_tags,
    kill_html_tags,
)


_HtmlToken = namedtuple('HtmlToken', ['index',
                                      'tokens',
                                      'elem',
                                      'is_tail',
                                      'position',
                                      'length'])


class HtmlToken(_HtmlToken):
    """
    HTML token info.

    Attributes:

    * :attr:`index` is a token index (in the :attr:`tokens` list)
    * :attr:`tokens` is a list of all tokens in current html block
    * :attr:`elem` is the current html block (as lxml's Element) - most
      likely you want :attr:`parent` instead of it
    * :attr:`is_tail` flag indicates that token belongs to element tail
    * :attr:`position` is logical position(in letters or codepoints) of token start in parent text
    * :attr:`length` is logical length(in letters or codepoints) of token in parent text

    Computed properties:

    * :attr:`token` is the current token (as text);
    * :attr:`parent` is token's parent HTML element (as lxml's Element);
    * :attr:`root` is an ElementTree this token belongs to.

    """
    @property
    def token(self):
        return self.tokens[self.index]

    @property
    def parent(self):
        if not self.is_tail:
            return self.elem
        return self.elem.getparent()

    @property
    def root(self):
        return self.elem.getroottree()

    def __repr__(self):
        return ("HtmlToken("
                "token=%r, parent=%r, index=%s, position=%d, length=%d"
                ")") % (
            self.token, self.parent, self.index, self.position, self.length
        )


class HtmlTokenizer(object):
    """
    Class for converting HTML trees (returned by one of the
    :mod:`webstruct.loaders`) into lists of :class:`HtmlToken` instances
    and associated tags. Also, it can do the reverse conversion.

    Use :meth:`tokenize_single` to convert a single tree and :meth:`tokenize`
    to convert multiple trees.

    Use :meth:`detokenize_single` to get an annotated tree out of a list
    of :class:`HtmlToken` instances and a list of tags.

    Parameters
    ----------

    tagset : set, optional
        A set of entity types to keep.
        If not passed, all entity types are kept.
        Use this argument to discard some entity types from training data.
    sequence_encoder : object, optional
        Sequence encoder object. If not passed,
        :class:`~webstruct.sequence_encoding.IobEncoder` instance is created.
    text_toknize_func : callable, optional
        Function used for tokenizing text inside HTML elements.
        By default, :class:`HtmlTokenizer` uses
        :func:`webstruct.text_tokenizers.tokenize`.
    kill_html_tags: set, optional
        A set of HTML tags which should be removed. Contents inside
        removed tags is not removed. See :func:`webstruct.utils.kill_html_tags`
    replace_html_tags: dict, optional
        A mapping ``{'old_tagname': 'new_tagname'}``. It defines how tags
        should be renamed. See :func:`webstruct.utils.replace_html_tags`
    ignore_html_tags: set, optional
        A set of HTML tags which won't produce :class:`HtmlToken` instances,
        but will be kept in a tree. Default is ``{'script', 'style'}``.
    """
    def __init__(self, tagset=None, sequence_encoder=None,
                 text_tokenize_func=None, kill_html_tags=None,
                 replace_html_tags=None, ignore_html_tags=None):
        self.tagset = set(tagset) if tagset is not None else None
        self.text_tokenize_func = text_tokenize_func or tokenize
        self.kill_html_tags = kill_html_tags
        self.replace_html_tags = replace_html_tags

        if ignore_html_tags is not None:
            self.ignore_html_tags = set(ignore_html_tags)
        else:
            self.ignore_html_tags = {'script', 'style'}

        # FIXME: don't use shared instance of sequence encoder
        # because sequence encoder is stateful
        self.sequence_encoder = sequence_encoder or IobEncoder()

        tag_pattern = self.sequence_encoder.token_processor.tag_re.pattern
        self._tag_re = re.compile(r"(^|\s)%s(\s|$)" % tag_pattern.strip())

    def tokenize_single(self, tree):
        """
        Return two lists:

        * a list a list of HtmlToken tokens;
        * a list of associated tags.

        For unannotated HTML all tags will be "O" - they may be ignored.

        Example:

            >>> from webstruct import GateLoader, HtmlTokenizer
            >>> loader = GateLoader(known_entities={'PER'})
            >>> html_tokenizer = HtmlTokenizer(replace_html_tags={'b': 'strong'})
            >>> tree = loader.loadbytes(b"<p>hello, <PER>John <b>Doe</b></PER> <br> <PER>Mary</PER> said</p>")
            >>> html_tokens, tags = html_tokenizer.tokenize_single(tree)
            >>> html_tokens
            [HtmlToken(token='hello', parent=<Element p at ...>, index=0, ...), HtmlToken...]
            >>> tags
            ['O', 'B-PER', 'I-PER', 'B-PER', 'O']
            >>> for tok, iob_tag in zip(html_tokens, tags):
            ...     print("%5s" % iob_tag, tok.token, tok.elem.tag, tok.parent.tag)
                O hello p p
            B-PER John p p
            I-PER Doe strong strong
            B-PER Mary br p
                O said br p

        For HTML without text it returns empty lists::

            >>> html_tokenizer.tokenize_single(loader.loadbytes(b'<p></p>'))
            ([], [])

        """
        tree = copy.deepcopy(tree)
        self.sequence_encoder.reset()
        self._prepare_tree(tree)
        res = list(zip(*self._process_tree(tree)))
        if not res:
            return [], []
        return list(res[0]), list(res[1])

    def tokenize(self, trees):
        X, y = [], []
        for tree in trees:
            html_tokens, tags = self.tokenize_single(tree)
            X.append(html_tokens)
            y.append(tags)
        return X, y

    def detokenize_single(self, html_tokens, tags):
        """
        Build annotated ``lxml.etree.ElementTree`` from
        ``html_tokens`` (a list of :class:`.HtmlToken` instances)
        and ``tags`` (a list of their tags).
        **ATTENTION**: ``html_tokens`` should be tokenized from tree
        without tags

        Annotations are encoded as ``__START_TAG__`` and ``__END_TAG__``
        text tokens (this is the format :mod:`webstruct.loaders` use).
        """
        if len(html_tokens) != len(tags):
            raise ValueError("len(html_tokens) must be equal to len(tags)")

        if not html_tokens:
            return None

        tree = html_tokens[0].root

        # find starts/ends of token groups
        token_groups = self.sequence_encoder.group(zip(html_tokens, tags))
        starts, ends = set(), set()
        pos = 0
        for gr_tokens, gr_tag in token_groups:
            n_tokens = len(gr_tokens)
            if gr_tag != 'O':
                starts.add(pos)
                ends.add(pos + n_tokens - 1)
            pos += n_tokens

        # mark starts/ends with special tokens
        data = [(s, True) for s in starts]
        data.extend((s, False) for s in ends)
        keyfunc = lambda rec: (id(html_tokens[rec[0]].elem), html_tokens[rec[0]].is_tail)
        data.sort(key=keyfunc)

        for (_, is_tail), g in groupby(data, keyfunc):
            g = list(g)
            g.sort(key=lambda t: (html_tokens[t[0]].position, not t[1]))

            if not g:
                continue

            elem = html_tokens[g[0][0]].elem

            pos_in_source = 0
            source = elem.text
            if is_tail:
                source = elem.tail

            mods = list()

            for idx, is_starts in g:
                token = html_tokens[idx]
                tag = tags[idx]
                mods.append(source[pos_in_source:token.position])
                pos_in_source = token.position
                if is_starts:
                    patch = ' __START_%s__ ' % (tag[2:],)
                    mods.append(patch)
                else:
                    end_in_source = pos_in_source + token.length
                    mods.append(source[pos_in_source:end_in_source])
                    pos_in_source = pos_in_source + token.length
                    patch = ' __END_%s__ ' % (tag[2:],)
                    mods.append(patch)

            mods.append(source[pos_in_source:])
            modded = ''.join(mods)

            if is_tail:
                elem.tail = modded
            else:
                elem.text = modded

        return tree

    def _prepare_tree(self, tree):
        if self.kill_html_tags:
            kill_html_tags(tree, self.kill_html_tags, keep_child=True)

        if self.replace_html_tags:
            replace_html_tags(tree, self.replace_html_tags)

    def _process_tree(self, tree):
        if not isinstance(tree.tag, str) or tree.tag in self.ignore_html_tags:
            return

        head_tokens, head_tags = self._tokenize_and_split(tree.text)
        char_tokens = [t.chars for t in head_tokens]
        for index, (token, tag) in enumerate(zip(head_tokens, head_tags)):
            yield HtmlToken(index,
                            char_tokens,
                            tree,
                            False,
                            token.position,
                            token.length), tag

        for child in tree:  # where is my precious "yield from"?
            for html_token, tag in self._process_tree(child):
                yield html_token, tag

        tail_tokens, tail_tags = self._tokenize_and_split(tree.tail)
        char_tokens = [t.chars for t in tail_tokens]
        for index, (token, tag) in enumerate(zip(tail_tokens, tail_tags)):
            yield HtmlToken(index,
                            char_tokens,
                            tree,
                            True,
                            token.position,
                            token.length), tag

    def cleanup_tree(self, tree):
        cleaned = copy.deepcopy(tree)
        for _, elem in iterwalk(cleaned):
            self._cleanup_elem(elem)

        return cleaned

    def _cleanup_elem(self, elem):
        """ Remove special tokens from elem """
        if elem.text:
            elem.text = self._tag_re.sub("", elem.text)
        if elem.tail:
            elem.tail = self._tag_re.sub("", elem.tail)

    def _tokenize_and_split(self, text):
        text = text or ''
        input_tokens = [t for t in self.text_tokenize_func(text)]
        input_tokens = self._limit_tags(input_tokens)
        input_tokens = [TextToken(chars=t.chars,
                                  position=t.position,
                                  length=t.length) for t in input_tokens]
        chains = self.sequence_encoder.encode(t.chars for t in input_tokens)
        chains = self.sequence_encoder.from_indices(chains, input_tokens)
        chains = [l for l in chains]
        return self.sequence_encoder.split(chains)

    def _limit_tags(self, input_tokens):
        if self.tagset is None:
            return input_tokens

        proc = self.sequence_encoder.token_processor
        token_classes = [proc.classify(tok.chars) for tok in input_tokens]
        return [
            tok for (tok, (typ, value)) in zip(input_tokens, token_classes)
            if not (typ in {'start', 'end'} and value not in self.tagset)
        ]

    def __getstate__(self):
        dct = self.__dict__.copy()
        if self.text_tokenize_func is tokenize:
            dct['text_tokenize_func'] = 'DEFAULT'
        return dct

    def __setstate__(self, state):
        if state['text_tokenize_func'] == 'DEFAULT':
            state['text_tokenize_func'] = tokenize
        self.__dict__.update(state)
