# -*- coding: utf-8 -*-
#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import re

from collections import defaultdict
from collections import deque
from functools import partial
from itertools import chain

from intbitset import intbitset

import typecode

from commoncode.text import toascii
from licensedcode.spans import Span
from licensedcode.tokenize import query_lines
from licensedcode.tokenize import query_tokenizer

"""
Build license queries from scanned files to feed the detection pipeline.

A query is a sequence of tokens.
Queries are further broken down in query runs that are "slices" of a query.

Several heuristics are used to break down a query in query runs and this process
is important to the overall speed and accuracy of license detection: since the
most costly parts of detection is done query run by query run, and sequence
alignment is performed on the best ranking candidates from a probalistic
ranking, the defintion of what chunk should be matched matters a lot.

If too small, chunking would favour alignment against smaller rules and increase
the processing time as more alignments would need to be computed. If too big,
chunking would eschew alignment against smaller rules.

So based on the chunk size, the alignment may be stuck on working with a
suboptimal set of candidate rules yielding possibly matches that are too small
and scattered to make sense when matches are merged.

If chunks are bigger, this decreases the sensitivity to more specific smaller
rules and would mistakenly report licenses that may contain the text of other
smaller licenses instead of larger longer licenses. But this does speed up
detection as fewer alignments need to be computed.

So rather than breaking the queries using a single way for all queries, we
compute crude statistics on the query text "structure" using the counts of
lines, the fact a text file has very long lines, empty lines, lines with unknown
tokens, lines with junk tokens and decide how to break a query based on these.

For instance, some HTML or code file may be very sparse and their source have a
lot of empty lines. However, this is is most often due to the original text
having been transformed when encoded as HTML or an artifact of some generated
HTML or HTML editor. When we can detect these, we can eventually ignore
heuristics to break queries based on sequences of empty lines. (Note this is not
yet implemented for HTML).

Conversely, a file text could be very dense and may contain a single line or
only a few lines of text as can happen with minified JavaScript. In these cases
counting lines is useless and other heuristic are needed.
"""

# Tracing flags
TRACE = False
TRACE_QR = False
TRACE_QR_BREAK = False
TRACE_REPR = False


def logger_debug(*args):
    pass


if TRACE or TRACE_QR or TRACE_QR_BREAK:
    import sys

    use_print = True

    if use_print:
        printer = print
    else:
        import logging

        logger = logging.getLogger(__name__)
        # logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
        logging.basicConfig(stream=sys.stdout)
        logger.setLevel(logging.DEBUG)
        printer = logger.debug

    def logger_debug(*args):
        return printer(' '.join(isinstance(a, str) and a or repr(a) for a in args))

# for the cases of very long lines, we break texts in abritrary pseudo lines of
# up to 25 tokens (aka. words) each to avoid getting huge query runs for texts
# on a single line (e.g. minified JS or CSS).
MAX_TOKEN_PER_LINE = 25


def build_query(
    location=None,
    query_string=None,
    idx=None,
    text_line_threshold=15,
    bin_line_threshold=50,
):
    """
    Return a Query built from location or query string given an index.
    """
    if location:
        T = typecode.get_type(location)
        # TODO: implement additional type-driven heuristics for query chunking.
        if not T.contains_text:
            return
        if T.is_binary:
            # for binaries we want to avoid a large number of query runs as the
            # license context is often very sparse or absent
            qry = Query(location=location, idx=idx, line_threshold=bin_line_threshold)
        else:
            # for text
            qry = Query(location=location, idx=idx, line_threshold=text_line_threshold)
    else:
        # a string is always considered text
        qry = Query(query_string=query_string, idx=idx)

    return qry


class Query(object):
    """
    A query represent a whole file or string being scanned for licenses. It
    holds known tokens, known token line positions, unknown tokens positions,
    and query runs. It also tracks which parts have been matched as matching
    progresses.

    For positions, we track primarily the absolute position of known tokens.
    Unknown tokens are tracked only as the number of unknown in reference to a
    known token position. (using -1 for unknown tokens that precede the first
    known token.)

    A query is broken down in one or more "runs" that are slices of tokens used
    as a matching unit.
    """

    __slots__ = (
        'location',
        'query_string',
        'idx',
        'line_threshold',
        'tokens',
        'line_by_pos',
        'unknowns_by_pos',
        'unknowns_span',
        'stopwords_by_pos',
        'stopwords_span',
        'shorts_and_digits_pos',
        'query_runs',
        '_whole_query_run',
        'high_matchables',
        'low_matchables',
        'spdx_lid_token_ids',
        'spdx_lines',
        'has_long_lines',
        'is_binary',
    )

    def __init__(
        self,
        location=None,
        query_string=None,
        idx=None,
        line_threshold=4,
        _test_mode=False,
    ):
        """
        Initialize the query from a file `location` or `query_string` string for
        an `idx` LicenseIndex.

        Break query in runs when there are at least `line_threshold` empty lines
        or junk-only lines.
        """
        assert (location or query_string) and idx

        self.location = location
        self.query_string = query_string
        self.idx = idx

        self.line_threshold = line_threshold

        # True if the text is made of very long lines
        self.has_long_lines = False

        # True if the query is binary
        self.is_binary = False

        # kown token ids array
        self.tokens = []

        # index of known position -> line number where the pos is the list index
        self.line_by_pos = []

        # index of "known positions" (yes really!) to a number of unknown tokens
        # after that known position. For unknowns at the start, the position is
        # using the magic -1 key
        self.unknowns_by_pos = defaultdict(int)

        # Span of "known positions" (yes really!) followed by unknown token(s)
        self.unknowns_span = None

        # index of "known positions" (yes really!) to a number of stopword tokens
        # after that known position. For stopwords at the start, the position is
        # using the magic -1 key
        self.stopwords_by_pos = defaultdict(int)

        # Span of "known positions" (yes really!) followed by stopwords
        self.stopwords_span = None

        # set of known positions were there is a short, single letter token or
        # digits-only token
        # TODO: consider using an intbitset
        self.shorts_and_digits_pos = set()

        # list of the three SPDX-License-Identifier tokens to identify to detect
        # a line for SPDX id matching.
        # note: this will not match anything if the index is not properly set
        dic_get = idx.dictionary.get
        _spdx = dic_get(u'spdx')
        _spdx_id = dic_get(u'identifier')
        spdxid1 = [_spdx, dic_get(u'license'), _spdx_id]
        # Even though it is invalid, this Enlish seplling happens in the wild
        spdxid2 = [_spdx, dic_get(u'licence'), _spdx_id]
        # None, None None: this is mostly a possible issue in test mode
        self.spdx_lid_token_ids = [x for x in [spdxid1, spdxid2, ] if x != [None, None, None]]

        # list of tuple (original line text, start known pos, end known pos) for
        # lines starting with SPDX-License-Identifier. This is to support the
        # SPDX id matching
        self.spdx_lines = []

        self._whole_query_run = None

        # list of QueryRun objects. Does not include SPDX-related query runs
        self.query_runs = []
        if _test_mode:
            return

        tokens_by_line = self.tokens_by_line(
            location=location,
            query_string=query_string,
        )
        # this method has side effects to populate various data structures
        self.tokenize_and_build_runs(tokens_by_line, line_threshold=line_threshold)

        len_legalese = idx.len_legalese
        tokens = self.tokens
        # sets of known token positions initialized after query tokenization:
        self.high_matchables = intbitset([p for p, t in enumerate(tokens) if t < len_legalese])
        self.low_matchables = intbitset([p for p, t in enumerate(tokens) if t >= len_legalese])

    def tokens_length(self, with_unknown=False):
        """
        Return the length in tokens of this query.
        Include unknown tokens if with_unknown is True.
        """
        length = len(self.tokens)
        if with_unknown:
            length += sum(self.unknowns_by_pos.values())
        return length

    def whole_query_run(self):
        """
        Return a query run built from the whole range of query tokens.
        """
        if not self._whole_query_run:
            self._whole_query_run = QueryRun(query=self, start=0, end=len(self.tokens) - 1)
        return self._whole_query_run

    def spdx_lid_query_runs_and_text(self):
        """
        Yield a tuple of query run, line text for each SPDX-License-Identifier line.
        SPDX-License-Identifier is not part of what is returned.
        """
        for spdx_text, start, end in self.spdx_lines:
            qr = QueryRun(query=self, start=start, end=end)
            yield qr, spdx_text

    def subtract(self, qspan):
        """
        Subtract the qspan matched positions from the query matchable positions.
        """
        if qspan:
            self.high_matchables.difference_update(qspan)
            self.low_matchables.difference_update(qspan)

    @property
    def matchables(self):
        """
        Return a set of every matchable token positions for this query.
        """
        return self.low_matchables | self.high_matchables

    @property
    def matched(self):
        """
        Return a set of every matched token positions for this query.
        """
        all_pos = intbitset(range(len(self.tokens)))
        all_pos.difference_update(self.matchables)
        return all_pos

    # FIXME: this is not used anywhere except for tests
    def tokens_with_unknowns(self):
        """
        Yield the original tokens stream with unknown tokens represented by None.
        """
        unknowns = self.unknowns_by_pos
        # yield anything at the start
        for _ in range(unknowns[-1]):
            yield None

        for pos, token in enumerate(self.tokens):
            yield token
            for _ in range(unknowns[pos]):
                yield None

    def tokens_by_line(self, location=None, query_string=None):
        """
        Yield multiple sequences of tokens, one for each line in this query.

        SIDE EFFECT: This populates the query `line_by_pos`, `unknowns_by_pos`,
        `unknowns_span`, `stopwords_by_pos`, `stopwords_span`,
        `shorts_and_digits_pos` and `spdx_lines` .
        """
        from licensedcode.match_spdx_lid import split_spdx_lid
        from licensedcode.stopwords import STOPWORDS

        location = location or self.location
        query_string = query_string or self.query_string

        # bind frequently called functions to local scope
        line_by_pos_append = self.line_by_pos.append

        self_unknowns_by_pos = self.unknowns_by_pos
        unknowns_pos = set()
        unknowns_pos_add = unknowns_pos.add

        self_stopwords_by_pos = self.stopwords_by_pos
        stopwords_pos = set()
        stopwords_pos_add = stopwords_pos.add

        self_shorts_and_digits_pos_add = self.shorts_and_digits_pos.add
        dic_get = self.idx.dictionary.get

        # note: positions start at zero
        # absolute position in a query, including only known tokens
        known_pos = -1

        # flag ifset to True when we have found the first known token globally
        # across all query lines
        started = False

        spdx_lid_token_ids = self.spdx_lid_token_ids

        qlines = query_lines(location=location, query_string=query_string)
        if TRACE:
            logger_debug('tokens_by_line: query lines:')
            qlines = list(qlines)
            for line_num, line in qlines:
                logger_debug(' ', line_num, ':', line)

        for line_num, line in qlines:
            # keep track of tokens in a line
            line_tokens = []
            line_tokens_append = line_tokens.append
            line_first_known_pos = None

            for token in query_tokenizer(line):
                tid = dic_get(token)
                is_stopword = token in STOPWORDS
                if tid is not None and not is_stopword:
                    # this is a known token
                    known_pos += 1
                    started = True
                    line_by_pos_append(line_num)
                    if len(token) == 1 or token.isdigit():
                        self_shorts_and_digits_pos_add(known_pos)
                    if line_first_known_pos is None:
                        line_first_known_pos = known_pos
                else:
                    # process STOPWORDS and unknown words
                    if is_stopword:
                        if not started:
                            # If we have not yet started globally, then all tokens
                            # seen so far are stopwords and we keep a count of them
                            # in the magic "-1" position.
                            self_stopwords_by_pos[-1] += 1
                        else:
                            # here we have a new unknwon token positioned right after
                            # the current known_pos
                            self_stopwords_by_pos[known_pos] += 1
                            stopwords_pos_add(known_pos)
                        # we do not track stopwords, only their position
                        continue
                    else:
                        if not started:
                            # If we have not yet started globally, then all tokens
                            # seen so far are unknowns and we keep a count of them
                            # in the magic "-1" position.
                            self_unknowns_by_pos[-1] += 1
                        else:
                            # here we have a new unknwon token positioned right after
                            # the current known_pos
                            self_unknowns_by_pos[known_pos] += 1
                            unknowns_pos_add(known_pos)

                line_tokens_append(tid)

            # last known token position in the current line
            line_last_known_pos = known_pos

            # ONLY collect as SPDX a line that starts with SPDX License
            # Identifier. There are cases where this prefix does not start as
            # the firt tokens such as when we have one or two words (such as a
            # comment indicator DNL, REM etc.) that start the line and then and
            # an SPDX license identifier.
            spdx_start_offset = None
            if line_tokens[:3] in spdx_lid_token_ids:
                spdx_start_offset = 0
            elif line_tokens[1:4] in spdx_lid_token_ids:
                spdx_start_offset = 1
            elif line_tokens[2:5] in spdx_lid_token_ids:
                spdx_start_offset = 2

            if spdx_start_offset is not None:
                # keep the line, start/end known pos for SPDX matching
                spdx_prefix, spdx_expression = split_spdx_lid(line)
                spdx_text = ' '.join([spdx_prefix or '', spdx_expression])
                spdx_start_known_pos = line_first_known_pos + spdx_start_offset

                if spdx_start_known_pos <= line_last_known_pos:
                    self.spdx_lines.append((spdx_text, spdx_start_known_pos, line_last_known_pos))

            yield line_tokens

        # finally create a Span of positions followed by unkwnons and another
        # for positions followed by stopwords used for intersection with the
        # query span to do the scoring matches correctly
        self.unknowns_span = Span(unknowns_pos)
        self.stopwords_span = Span(stopwords_pos)

    def tokenize_and_build_runs(self, tokens_by_line, line_threshold=4):
        """
        Tokenize this query and populate tokens and query_runs at each break
        point. Only keep known token ids but consider unknown token ids to break
        a query in runs.

        `tokens_by_line` is the output of the self.tokens_by_line() method and is an
        iterator of lines (eg. list) of token ids.
        `line_threshold` is the number of empty or junk lines to break a new run.
        """
        self._tokenize_and_build_runs(tokens_by_line, line_threshold)

        if TRACE_QR:
            print()
            logger_debug('Initial Query runs for query:', self.location)
            for qr in self.query_runs:
                print(' ' , repr(qr))
            print()

    def refine_runs(self):
        # TODO: move me to the approximate matching loop so that this is done only if neeed
        # rebreak query runs based on potential rule boundaries
        query_runs = list(chain.from_iterable(
            break_on_boundaries(qr) for qr in self.query_runs))

        if TRACE_QR_BREAK:
            logger_debug('Initial # query runs:', len(self.query_runs), 'after breaking:', len(query_runs))

        self.query_runs = query_runs

        if TRACE_QR:
            print()
            logger_debug('FINAL Query runs for query:', self.location)
            for qr in self.query_runs:
                print(' ' , repr(qr))
            print()

    def _tokenize_and_build_runs(self, tokens_by_line, line_threshold=4):
        len_legalese = self.idx.len_legalese
        digit_only_tids = self.idx.digit_only_tids

        # initial query run
        query_run = QueryRun(query=self, start=0)

        # break in runs based on threshold of lines that are either empty, all
        # unknown, all low id/junk jokens or made only of digits.
        empty_lines = 0

        # token positions start at zero
        pos = 0

        # bind frequently called functions to local scope
        tokens_append = self.tokens.append
        query_runs_append = self.query_runs.append

        if self.location:
            ft = typecode.get_type(self.location)
            if ft.is_text_with_long_lines:
                self.has_long_lines = True
                tokens_by_line = break_long_lines(tokens_by_line)
            if ft.is_binary:
                self.is_binary = True

        for tokens in tokens_by_line:
            # have we reached a run break point?
            if len(query_run) > 0 and empty_lines >= line_threshold:
                query_runs_append(query_run)
                # start new query run
                query_run = QueryRun(query=self, start=pos)
                empty_lines = 0

            if len(query_run) == 0:
                query_run.start = pos

            if not tokens:
                empty_lines += 1
                continue

            line_has_known_tokens = False
            line_has_good_tokens = False
            line_is_all_digit = all([tid is None or tid in digit_only_tids for tid in tokens])

            for token_id in tokens:
                if token_id is not None:
                    tokens_append(token_id)
                    line_has_known_tokens = True
                    if token_id < len_legalese:
                        line_has_good_tokens = True
                    query_run.end = pos
                    pos += 1

            if line_is_all_digit:
                # close current run and start new query run
                empty_lines += 1
                continue

            if not line_has_known_tokens:
                empty_lines += 1
                continue

            if line_has_good_tokens:
                empty_lines = 0
            else:
                empty_lines += 1

        # append final run if any
        if len(query_run) > 0:
            if not all(tid in digit_only_tids for tid in query_run.tokens):
                query_runs_append(query_run)

        if TRACE_QR:
            print()
            logger_debug('Query runs for query:', self.location)
            for qr in self.query_runs:
                high_matchables = len([p for p, t in enumerate(qr.tokens) if t < len_legalese])

                print(' ' , repr(qr), 'high_matchables:', high_matchables)
            print()


def break_on_boundaries(query_run):
    """
    Given a QueryRun, yield more query runs broken down on boundaries discovered
    from matched rules and matched rule starts and ends.
    """
    if len(query_run) < 150:
        yield query_run
    else:
        from licensedcode.match_aho import get_matched_starts

        qr_tokens = query_run.tokens
        qr_start = query_run.start
        qr_end = query_run.end
        query = query_run.query
        idx = query.idx

        matched_starts = get_matched_starts(
            qr_tokens, qr_start, automaton=idx.starts_automaton)

        starts = dict(matched_starts)

        if TRACE_QR_BREAK:
            logger_debug('break_on_boundaries: len(starts):', len(starts),)

        if not starts:
            if TRACE_QR_BREAK: logger_debug('break_on_boundaries: Qr returned unchanged')
            yield query_run

        else:
            positions = deque()
            pos = qr_start
            while pos < qr_end:
                matches = starts.get(pos, None)
                if matches:
                    min_length, _ridentifier = matches[0]
                    if len(positions) >= min_length:
                        qr = QueryRun(query, positions[0], positions[-1])
                        if TRACE_QR_BREAK:
                            logger_debug('\nbreak_on_boundaries: new QueryRun', qr, '\n', matches, '\n')
                        yield qr
                        positions.clear()
                positions.append(pos)
                pos += 1

            if positions:
                qr = QueryRun(query, positions[0], positions[-1])
                yield qr
                if TRACE_QR_BREAK:
                    print()
                    logger_debug('\nbreak_on_boundaries: final QueryRun', qr, '\n', matches, '\n')


is_only_digit_and_punct = re.compile('^[^A-Za-z]+$').match


def break_long_lines(lines, threshold=MAX_TOKEN_PER_LINE):
    """
    Given an iterable of lines (each being a list of token ids), break lines
    that contain more than threshold in chunks. Return an iterable of lines.
    """
    for line in lines:
        for i in range(0, len(line), threshold):
            yield line[i:i + threshold]


class QueryRun(object):
    """
    A query run is a slice of query tokens identified by a start and end
    positions inclusive.
    """
    __slots__ = (
        'query', 'start', 'end', 'len_legalese', 'digit_only_tids',
        '_low_matchables', '_high_matchables',
    )

    def __init__(self, query, start, end=None):
        """
        Initialize a query run from starting at `start` and ending at `end` from
        a parent `query`.
        """
        self.query = query

        # absolute start and end positions of this run in the query
        self.start = start
        self.end = end

        self.len_legalese = self.query.idx.len_legalese
        self.digit_only_tids = self.query.idx.digit_only_tids
        self._low_matchables = None
        self._high_matchables = None

    def __len__(self):
        if self.end is None:
            return 0
        return self.end - self.start + 1

    def __repr__(self, trace_repr=TRACE_REPR):
        base = (
            'QueryRun('
                'start={start}, len={length}, '
                'start_line={start_line}, end_line={end_line}'
        )
        if trace_repr:
            base += ', tokens="{tokens}"'
        base += ')'
        qdata = self.to_dict(brief=False, comprehensive=True, include_high=False)
        return base.format(**qdata)

    @property
    def start_line(self):
        return self.query.line_by_pos[self.start]

    @property
    def end_line(self):
        return self.query.line_by_pos[self.end]

    @property
    def tokens(self):
        """
        Return the sequence of known token ids for this run.
        """
        if self.end is None:
            return []
        return self.query.tokens[self.start: self.end + 1]

    # FIXME: this is not used anywhere except for tests
    def tokens_with_unknowns(self):
        """
        Yield the original token ids stream including unknown tokens
        (represented by None).
        """
        unknowns = self.query.unknowns_by_pos
        # yield anything at the start only if this is the first query run
        if self.start == 0:
            for _ in range(unknowns[-1]):
                yield None

        for pos, token in self.tokens_with_pos():
            yield token
            if pos == self.end:
                break
            for _ in range(unknowns[pos]):
                yield None

    def tokens_with_pos(self):
        return enumerate(self.tokens, self.start)

    def is_digits_only(self):
        """
        Return True if this query run contains only digit tokens.
        """
        # FIXME: this should be cached
        return intbitset(self.tokens).issubset(self.digit_only_tids)

    def is_matchable(self, include_low=False, qspans=None):
        """
        Return True if this query run has some matchable high token positions.
        Optinally if `include_low`m include low tokens.
        If a list of `qspans` is provided, their positions are also subtracted.
        """
        if include_low:
            matchables = self.matchables
        else:
            matchables = self.high_matchables

        if self.is_digits_only():
            return False

        if not qspans:
            return matchables

        matched = intbitset.union(*[q._set for q in qspans])
        matchables = intbitset(matchables)
        matchables.difference_update(matched)
        return matchables

    @property
    def matchables(self):
        """
        Return a set of every matchable token ids positions for this query.
        """
        return self.low_matchables | self.high_matchables

    def matchable_tokens(self):
        """
        Return an iterable of matchable tokens tids for this query run.
        Return an empty list if there are no high matchable tokens.
        Return -1 for positions with non-matchable tokens.
        """
        high_matchables = self.high_matchables
        if not high_matchables:
            return []
        return (tid if pos in self.matchables else -1
                for pos, tid in self.tokens_with_pos())

    @property
    def low_matchables(self):
        """
        Set of known positions for low token ids that are still matchable for
        this run.
        """
        if not self._low_matchables:
            self._low_matchables = intbitset(
                [pos for pos in self.query.low_matchables
                 if self.start <= pos <= self.end])
        return self._low_matchables

    @property
    def high_matchables(self):
        """
        Set of known positions for high token ids that are still matchable for
        this run.
        """
        if not self._high_matchables:
            self._high_matchables = intbitset(
                [pos for pos in self.query.high_matchables
                 if self.start <= pos <= self.end])
        return self._high_matchables

    def subtract(self, qspan):
        """
        Subtract the qspan matched positions from the parent query matchable
        positions.
        """
        if qspan:
            self.query.subtract(qspan)
            self._high_matchables = self.high_matchables.difference_update(qspan)
            self._low_matchables = self.low_matchables.difference_update(qspan)

    def to_dict(self, brief=False, comprehensive=False, include_high=False):
        """
        Return a human readable dictionary representing the query run replacing
        token ids with their string values. If brief is True, the tokens
        sequence will be truncated to show only the first 5 and last five tokens
        of the run. Used for debugging and testing.
        """
        tokens_by_tid = self.query.idx.tokens_by_tid

        def tokens_string(tks, sort=False):
            "Return a string from a token id seq"
            tks = ('None' if tid is None else tokens_by_tid[tid] for tid in tks)
            ascii_text = partial(toascii, translit=True)
            tks = map(ascii_text, tks)
            if sort:
                tks = sorted(tks)
            return ' '.join(tks)

        if brief and len(self.tokens) > 10:
            tokens = tokens_string(self.tokens[:5]) + ' ... ' + tokens_string(self.tokens[-5:])
            high_tokens = ''
        else:
            tokens = tokens_string(self.tokens)
            high_tokens = set(t for t in self.tokens if t < self.len_legalese)
            high_tokens = tokens_string(high_tokens, sort=True)

        to_dict = dict(
            start=self.start,
            end=self.end,
            tokens=tokens,
        )
        if include_high:
            to_dict['high_tokens'] = high_tokens

        if comprehensive:
            to_dict.update(dict(
                start_line=self.start_line,
                end_line=self.end_line,
                length=len(self),
            ))
        return to_dict


# TODO: remove me this unused and obsolete code
def tokens_ngram_processor(tokens, ngram_len):
    """
    Given a `tokens` sequence or iterable of Tokens, return an iterator of
    tuples of Tokens where the tuples length is length `ngram_len`. Buffers at
    most `ngram_len` iterable items. The returned tuples contains
    either `ngram_len` items or less for these cases where the number of tokens
    is smaller than `ngram_len`.
    """
    ngram = deque()
    for token in tokens:
        if len(ngram) == ngram_len:
            yield tuple(ngram)
            ngram.popleft()
        if token.gap:
            ngram.append(token)
            yield tuple(ngram)
            # reset
            ngram.clear()
        else:
            ngram.append(token)
    if ngram:
        # yield last ngram
        yield tuple(ngram)
