#
# Copyright (c) nexB Inc. and others. All rights reserved.
# ScanCode is a trademark of nexB Inc.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/scancode-toolkit for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

from functools import total_ordering
from itertools import groupby

import attr

from licensedcode import MAX_DIST
from licensedcode import query
from licensedcode.spans import Span
from licensedcode.stopwords import STOPWORDS
from licensedcode.tokenize import matched_query_text_tokenizer
from licensedcode.tokenize import index_tokenizer

"""
LicenseMatch data structure and matches merging and filtering routines.
"""

TRACE = False
TRACE_MERGE = False
TRACE_REFINE = False
TRACE_FILTER_CONTAINED = False
TRACE_FILTER_OVERLAPPING = False
TRACE_FILTER_SHORT = False
TRACE_FILTER_SPURIOUS_SINGLE_TOKEN = False
TRACE_FILTER_SPURIOUS = False
TRACE_FILTER_RULE_MIN_COVERAGE = False
TRACE_FILTER_LOW_SCORE = False
TRACE_SET_LINES = False

TRACE_MATCHED_TEXT = False
TRACE_MATCHED_TEXT_DETAILS = False

# these control the details in a LicenseMatch representation
TRACE_REPR_MATCHED_RULE = False
TRACE_REPR_SPAN_DETAILS = False
TRACE_REPR_THRESHOLDS = False


def logger_debug(*args): pass


if (TRACE
    or TRACE_MERGE
    or TRACE_REFINE
    or TRACE_FILTER_CONTAINED
    or TRACE_FILTER_OVERLAPPING
    or TRACE_FILTER_RULE_MIN_COVERAGE
    or TRACE_FILTER_SPURIOUS_SINGLE_TOKEN
    or TRACE_FILTER_SPURIOUS
    or TRACE_FILTER_SHORT
    or TRACE_FILTER_RULE_MIN_COVERAGE
    or TRACE_FILTER_LOW_SCORE
    or TRACE_SET_LINES
    or TRACE_MATCHED_TEXT
    or TRACE_MATCHED_TEXT_DETAILS):

    use_print = True
    if use_print:
        printer = print
    else:
        import logging
        import sys
        logger = logging.getLogger(__name__)
        # logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)
        logging.basicConfig(stream=sys.stdout)
        logger.setLevel(logging.DEBUG)
        printer = logger.debug

    def logger_debug(*args):
        return printer(' '.join(isinstance(a, str) and a or repr(a)
                                     for a in args))

    def _debug_print_matched_query_text(match, query, extras=5):
        """
        Print a matched query text including `extras` tokens before and after
        the match. Used for debugging license matches.
        """
        # create a fake new match with extra tokens before and after
        new_match = match.combine(match)
        new_qstart = max([0, match.qstart - extras])
        new_qend = min([match.qend + extras, len(query.tokens)])
        new_qspan = Span(new_qstart, new_qend)
        new_match.qspan = new_qspan

        logger_debug(new_match)
        logger_debug(' MATCHED QUERY TEXT with extras')
        qt = new_match.matched_text(whole_lines=False)
        logger_debug(qt)


# TODO: use attrs
# FIXME: Implement each ordering functions. From the Python docs: Note: While
# this decorator makes it easy to create well behaved totally ordered types, it
# does come at the cost of slower execution and more complex stack traces for
# the derived comparison methods. If performance benchmarking indicates this is
# a bottleneck for a given application, implementing all six rich comparison
# methods instead is likely to provide an easy speed boost.
@total_ordering
class LicenseMatch(object):
    """
    License detection match to a rule with matched query positions and lines and
    matched index positions. Also computes a score for a match. At a high level,
    a match behaves a bit like a Span and has several similar methods taking
    into account both the query and index Span.
    """

    __slots__ = (
        'rule', 'qspan', 'ispan', 'hispan', 'query_run_start',
        'matcher', 'start_line', 'end_line', 'query',
    )

    def __init__(self, rule, qspan, ispan, hispan=None, query_run_start=0,
                 matcher='', start_line=0, end_line=0, query=None):
        """
        Create a new match from:
         - rule: matched Rule object
         - qspan: query text matched Span, start at zero which is the absolute
           query start (not the query_run start).
         - ispan: rule text matched Span, start at zero which is the rule start.
         - hispan: rule text matched Span for high tokens, start at zero which
           is the rule start. Always a subset of ispan.
         - matcher: a string indicating which matching procedure this match was
           created with. Used for diagnostics, debugging and testing.

         Note that the relationship between the qspan and ispan is such that:
         - they always have the exact same number of items but when sorted each
           value at an index may be different
         - the nth position when sorted by position is such that their token
           value is equal for this position.
        """
        self.rule = rule
        self.qspan = qspan
        self.ispan = ispan
        self.hispan = Span() if hispan is None else hispan
        self.query_run_start = query_run_start
        self.matcher = matcher
        self.start_line = start_line
        self.end_line = end_line
        self.query = query

    def __repr__(
        self,
        trace_spans=TRACE_REPR_SPAN_DETAILS,
        trace_thresholds=TRACE_REPR_THRESHOLDS,
        trace_rule=TRACE_REPR_MATCHED_RULE,
    ):
        spans = ''
        if trace_spans:
            hispan = self.hispan
            qspan = self.qspan
            ispan = self.ispan
            spans = ('\n    qspan=%(qspan)r, '
                     '\n    ispan=%(ispan)r, '
                     '\n    hispan=%(hispan)r' % locals())

        thresh = ''
        if trace_thresholds:
            qdens = round(self.qdensity() * 100, 2)
            idens = round(self.idensity() * 100, 2)
            thresh = ('\n    qdens=%(qdens)r, idens=%(idens)r' % locals())

        rule_id = self.rule.identifier
        if trace_rule:
            rule_id = '\n    ' + repr(self.rule)

        rep = dict(
            spans=spans,
            thresh=thresh,
            matcher=self.matcher,
            rule_id=rule_id,
            license_expression=self.rule.license_expression,
            score=self.score(),
            coverage=self.coverage(),
            len=self.len(),
            hilen=self.hilen(),
            qreg=(self.qstart, self.qend),
            rlen=self.rule.length,
            ireg=(self.istart, self.iend),
            lines=self.lines(),
        )
        return (
            'LicenseMatch: %(matcher)r, lines=%(lines)r, %(rule_id)r, '
            '%(license_expression)r, '
            'sc=%(score)r, cov=%(coverage)r, '
            'len=%(len)r, hilen=%(hilen)r, rlen=%(rlen)r, '
            'qreg=%(qreg)r, ireg=%(ireg)r%(thresh)s%(spans)s'
        ) % rep

    def __eq__(self, other):
        """
        Strict equality is based on licensing, matched positions and not based
        on matched rule.
        """
        return (isinstance(other, LicenseMatch)
            and self.same_licensing(other)
            and self.qspan == other.qspan
            and self.ispan == other.ispan
        )

    def __ne__(self, other):
        """
        Strict inequality is based on licensing, matched positions and not based
        on matched rule.
        """
        if not isinstance(other, LicenseMatch):
            return True

        return not all([
            self.same_licensing(other),
            self.qspan == other.qspan,
            self.ispan == other.ispan,
        ])

    def same_licensing(self, other):
        """
        Return True if other has the same licensing.
        """
        return self.rule.same_licensing(other.rule)

    def licensing_contains(self, other):
        """
        Return True if this match licensing contains the other match licensing.
        """
        return self.rule.licensing_contains(other.rule)

    def lines(self):
        return self.start_line, self.end_line

    @property
    def qstart(self):
        return self.qspan.start

    def __lt__(self, other):
        return self.qstart < other.qstart

    @property
    def qend(self):
        return self.qspan.end

    def len(self):
        """
        Return the length of the match as the number of matched tokens.
        """
        return len(self.qspan)

    @property
    def istart(self):
        return self.ispan.start

    @property
    def iend(self):
        return self.ispan.end

    def hilen(self):
        """
        Return the length of the match as the number of matched high tokens.
        """
        return len(self.hispan)

    def __contains__(self, other):
        """
        Return True if qspan contains other.qspan and ispan contains other.ispan.
        """
        return other.qspan in self.qspan and other.ispan in self.ispan

    def qcontains(self, other):
        """
        Return True if qspan contains other.qspan.
        """
        return other.qspan in self.qspan

    def qdistance_to(self, other):
        """
        Return the absolute qspan distance to other match.
        Touching and overlapping matches have a zero distance.
        """
        return self.qspan.distance_to(other.qspan)

    def idistance_to(self, other):
        """
        Return the absolute ispan distance from self to other match.
        Touching and overlapping matches have a zero distance.
        """
        return self.ispan.distance_to(other.ispan)

    def overlap(self, other):
        """
        Return the number of overlaping positions with other.
        """
        return self.qspan.overlap(other.qspan)

    def _icoverage(self):
        """
        Return the coverage of this match to the matched rule as a float between
        0 and 1.
        """
        if not self.rule.length:
            return 0
        return self.len() / self.rule.length

    def coverage(self):
        """
        Return the coverage of this match to the matched rule as a rounded float
        between 0 and 100.
        """
        return round(self._icoverage() * 100, 2)

    def qmagnitude(self):
        """
        Return the maximal query length represented by this match start and end
        in the query. This number represents the full extent of the matched
        query region including matched, unmatched and INCLUDING unknown tokens.

        The magnitude is the same as the length of a match for a contiguous
        match without any unknown token in its range. It will be greater than
        the matched length for a match with non-contiguous words. It can also be
        greater than the query length when there are unknown tokens in the
        matched range.
        """
        # The query side of the match may not be contiguous and may contain
        # unmatched known tokens or unknown tokens. Therefore we need to compute
        # the real portion query length including unknown tokens that is
        # included in this match, for both matches and unmatched tokens

        query = self.query
        qspan = self.qspan
        qmagnitude = self.qrange()

        # note: to avoid breaking many tests we check query presence
        if query:
            # Compute a count of unknown tokens that are inside the matched
            # range, ignoring end position of the query span: unknowns here do
            # not matter as they are never in the match but they influence the
            # score.
            unknowns_pos = qspan & query.unknowns_span
            qspe = qspan.end
            unknowns_pos = (pos for pos in unknowns_pos if pos != qspe)
            qry_unkxpos = query.unknowns_by_pos
            unknowns_in_match = sum(qry_unkxpos[pos] for pos in unknowns_pos)

            # update the magnitude by adding the count of unknowns in the match.
            # This number represents the full extent of the matched query region
            # including matched, unmatched and unknown tokens.
            qmagnitude += unknowns_in_match

        return qmagnitude

    def qcontains_stopwords(self):
        """
        Return True if this match query contains stopwords between its start and end
        in the query. Stopwords are never match by construction.
        """
        # The query side of the match may not be contiguous and may contain
        # unmatched stopword tokens.
        query = self.query
        qspan = self.qspan

        # note: to avoid breaking many tests we check query presence
        if query:
            qspe = qspan.end
            # Count stopword tokens that are inside the matched range, ignoring
            # end position of the query span. This is used to check if there are
            # stopwords inside an "only_known_words" match.

            stopwords_pos = qspan & query.stopwords_span
            stopwords_pos = (pos for pos in stopwords_pos if pos != qspe)
            qry_stopxpos = query.stopwords_by_pos
            return any(qry_stopxpos[pos] for pos in stopwords_pos)

    def qrange(self):
        """
        Return the maximal query length represented by this match start and end
        in the query. This number represents the full extent of the matched
        query region including matched, unmatched and IGNORING unknown tokens.
        """
        return self.qspan.magnitude()

    def qdensity(self):
        """
        Return the query density of this match as a ratio of its length to its
        qmagnitude, a float between 0 and 1. A dense match has all its matched
        query tokens contiguous and a maximum qdensity of one. A sparse low
        qdensity match has some non-contiguous matched query tokens interspersed
        between matched query tokens. An empty match has a zero qdensity.
        """
        mlen = self.len()
        if not mlen:
            return 0
        qmagnitude = self.qmagnitude()
        if not qmagnitude:
            return 0
        return mlen / qmagnitude

    def idensity(self):
        """
        Return the ispan density of this match as a ratio of its rule-side
        matched length to its rule side magnitude. This is a float between 0 and
        1. A dense match has all its matched rule tokens contiguous and a
        maximum idensity of one. A sparse low idensity match has some non-
        contiguous matched rule tokens interspersed between matched rule tokens.
        An empty match has a zero qdensity.
        """
        return self.ispan.density()

    def score(self):
        """
        Return the score for this match as a rounded float between 0 and 100.

        The score is an indication of the confidence that a match is good. It is
        computed from the number of matched tokens, the number of query tokens
        in the matched range (including unknowns and unmatched) and the matched
        rule relevance.
        """
        # relevance is a number between 0 and 100. Divide by 100
        relevance = self.rule.relevance / 100
        if not relevance:
            return 0

        qmagnitude = self.qmagnitude()

        # Compute the score as the ration of the matched query length to the
        # qmagnitude, e.g. the length of the matched region
        if not qmagnitude:
            return 0

        # FIXME: this should exposed as an q/icoverage() method instead
        query_coverage = self.len() / qmagnitude
        rule_coverage = self._icoverage()
        if query_coverage < 1 and rule_coverage < 1:
            # use rule coverage in this case
            return  round(rule_coverage * relevance * 100, 2)
        return  round(query_coverage * rule_coverage * relevance * 100, 2)

    def surround(self, other):
        """
        Return True if this match query span surrounds other other match query
        span.

        This is different from containment. A matched query region can surround
        another matched query region and have no positions in common with the
        surrounded match.
        """
        return self.qstart <= other.qstart and self.qend >= other.qend

    def is_after(self, other):
        """
        Return True if this match spans are strictly after other match spans.
        """
        return self.qspan.is_after(other.qspan) and self.ispan.is_after(other.ispan)

    def combine(self, other):
        """
        Return a new match object combining self and an other match.
        """
        if self.rule != other.rule:
            raise TypeError(
                'Cannot combine matches with different rules: '
                'from: %(self)r, to: %(other)r' % locals())

        if other.matcher not in self.matcher:
            newmatcher = ' '.join([self.matcher, other.matcher])
        else:
            newmatcher = self.matcher

        combined = LicenseMatch(
            rule=self.rule,
            qspan=Span(self.qspan | other.qspan),
            ispan=Span(self.ispan | other.ispan),
            hispan=Span(self.hispan | other.hispan),
            query_run_start=min(self.query_run_start, other.query_run_start),
            matcher=newmatcher,
            query=self.query)
        return combined

    def update(self, other):
        """
        Update self with other match and return the updated self in place.
        """
        combined = self.combine(other)
        self.qspan = combined.qspan
        self.ispan = combined.ispan
        self.hispan = combined.hispan
        self.matcher = combined.matcher
        self.query_run_start = min(self.query_run_start, other.query_run_start)
        return self

    def is_small(self):
        """
        Return True if this match is "small" based on its rule lengths and
        thresholds. Small matches are spurious matches that are discarded.
        """
        matched_len = self.len()
        min_matched_len = self.rule.min_matched_length

        high_matched_len = self.hilen()
        min_high_matched_len = self.rule.min_high_matched_length

        if TRACE_FILTER_SHORT:
            coverage = self.coverage()
            logger_debug(
                'LicenseMatch.is_small(): %(self)r' % locals(),)

        if matched_len < min_matched_len or high_matched_len < min_high_matched_len:
            if TRACE_FILTER_SHORT:
                logger_debug('  LicenseMatch.is_small(): CASE 1')
            return True

        if self.rule.is_small and self.coverage() < 80:
            if TRACE_FILTER_SHORT:
                logger_debug('  LicenseMatch.is_small(): CASE 2')
            return True

        if TRACE_FILTER_SHORT:
            logger_debug('  LicenseMatch.is_small(): not small')

        return False

    def itokens(self, idx):
        """
        Return the sequence of matched itoken ids.
        """
        ispan = self.ispan
        rid = self.rule.rid
        if rid is not None:
            for pos, token in enumerate(idx.tids_by_rid[rid]):
                if pos in ispan:
                    yield token

    def itokens_hash(self, idx):
        """
        Return a hash from the matched itoken ids.
        """
        from licensedcode.match_hash import index_hash
        itokens = list(self.itokens(idx))
        if itokens:
            return index_hash(itokens)

    # FIXME: this should be done for all the matches found in a given scanned
    # location at once to avoid reprocessing many times the original text
    def matched_text(
        self,
        whole_lines=False,
        highlight=True,
        highlight_matched=u'%s',
        highlight_not_matched=u'[%s]',
        _usecache=True
    ):
        """
        Return the matched text for this match or an empty string if no query
        exists for this match.

        `_usecache` can be set to False in testing to avoid any unwanted caching
        side effects as the caching depends on which index instance is being
        used and this index can change during testing.
        """
        query = self.query
        if not query:
            # TODO: should we raise an exception instead???
            # this case should never exist except for tests!
            return u''

        if whole_lines and query.has_long_lines:
            whole_lines = False

        return u''.join(get_full_matched_text(
            self,
            location=query.location,
            query_string=query.query_string,
            idx=query.idx,
            whole_lines=whole_lines,
            highlight=highlight,
            highlight_matched=highlight_matched,
            highlight_not_matched=highlight_not_matched, _usecache=_usecache)
        ).rstrip()


def set_lines(matches, line_by_pos):
    """
    Update a `matches` sequence with start and end line given a `line_by_pos`
    {pos: line} mapping.
    """
    # if there is no line_by_pos, do not bother: the lines will stay to zero.
    if line_by_pos:
        for match in matches:
            match.start_line = line_by_pos[match.qstart]
            match.end_line = line_by_pos[match.qend]
            if TRACE_SET_LINES:
                logger_debug('set_lines: match.start_line :', match.start_line)
                logger_debug('set_lines: match.end_line :', match.end_line)


def merge_matches(matches, max_dist=None):
    """
    Merge matches to the same rule in a sequence of LicenseMatch matches. Return
    a new list of merged matches if they can be merged. Match sequences that
    cannot be merged are returned as-is. For being merged two matches must also
    be in increasing query and index positions.
    """

    # shortcut for single matches
    if len(matches) < 2:
        return matches

    # only merge matches with the same rule: sort then group by rule for the
    # same rule, sort on start, longer high, longer match, matcher type
    sorter = lambda m: (m.rule.identifier, m.qspan.start, -m.hilen(), -m.len(), m.matcher)
    matches.sort(key=sorter)
    matches_by_rule = [(rid, list(rule_matches))
        for rid, rule_matches in groupby(matches, key=lambda m: m.rule.identifier)]

    if TRACE_MERGE: print('merge_matches: number of matches to process:', len(matches))

    if max_dist is None:
        max_dist = MAX_DIST

    merged = []
    for rid, rule_matches in matches_by_rule:
        if TRACE_MERGE: logger_debug('merge_matches: processing rule:', rid)

        rule_length = rule_matches[0].rule.length
        # FIXME this is likely too much as we are getting gaps that are often too big
        max_rule_side_dist = min((rule_length // 2) or 1, max_dist)

        # compare two matches in the sorted sequence: current and next
        i = 0
        while i < len(rule_matches) - 1:
            j = i + 1
            while j < len(rule_matches):
                current_match = rule_matches[i]
                next_match = rule_matches[j]
                if TRACE_MERGE: logger_debug('---> merge_matches: current:', current_match)
                if TRACE_MERGE: logger_debug('---> merge_matches: next:   ', next_match)

                # two exact matches can never be merged as they will not be overlapping
                # only sequence matches for the same rule can be merged
                # if current_match.matcher != MATCH_SEQ and next_match.matcher != MATCH_SEQ:
                #    if TRACE_MERGE: logger_debug('    ---> ###merge_matches: both matches are EXACT_MATCHES, skipping')
                #    break

                # FIXME: also considers the match length!
                # stop if we exceed max dist
                # or distance over 1/2 of rule length
                if (current_match.qdistance_to(next_match) > max_rule_side_dist
                or current_match.idistance_to(next_match) > max_rule_side_dist):
                    if TRACE_MERGE: logger_debug('    ---> ###merge_matches: MAX_DIST/max_rule_side_dist: %(max_rule_side_dist)d reached, breaking' % locals())
                    break

                # keep one of equal matches
                # with same qspan: FIXME: is this ever possible?
                if current_match.qspan == next_match.qspan and current_match.ispan == next_match.ispan:
                    if TRACE_MERGE: logger_debug('    ---> ###merge_matches: next EQUALS current, del next')
                    del rule_matches[j]
                    continue

                # if we have two equal ispans and some overlap
                # keep the shortest/densest match in qspan e.g. the smallest magnitude of the two
                if current_match.ispan == next_match.ispan and current_match.overlap(next_match):
                    cqmag = current_match.qspan.magnitude()
                    nqmag = next_match.qspan.magnitude()
                    if cqmag <= nqmag:
                        if TRACE_MERGE: logger_debug('    ---> ###merge_matches: current ispan EQUALS next ispan, current qmagnitude smaller, del next')
                        del rule_matches[j]
                        continue
                    else:
                        if TRACE_MERGE: logger_debug('    ---> ###merge_matches: current ispan EQUALS next ispan, next qmagnitude smaller, del current')
                        del rule_matches[i]
                        i -= 1
                        break

                # remove contained matches
                if current_match.qcontains(next_match):
                    if TRACE_MERGE: logger_debug('    ---> ###merge_matches: next CONTAINED in current, del next')
                    del rule_matches[j]
                    continue

                # remove contained matches the other way
                if next_match.qcontains(current_match):
                    if TRACE_MERGE: logger_debug('    ---> ###merge_matches: current CONTAINED in next, del current')
                    del rule_matches[i]
                    i -= 1
                    break

                # FIXME: qsurround is too weak. We want to check also isurround
                # merge surrounded
                if current_match.surround(next_match):
                    new_match = current_match.combine(next_match)
                    if len(new_match.qspan) == len(new_match.ispan):
                        # the merged matched is likely aligned
                        current_match.update(next_match)
                        if TRACE_MERGE: logger_debug('    ---> ###merge_matches: current SURROUNDS next, merged as new:', current_match)
                        del rule_matches[j]
                        continue

                # FIXME: qsurround is too weak. We want to check also isurround
                # merge surrounded the other way too: merge in current
                if next_match.surround(current_match):
                    new_match = current_match.combine(next_match)
                    if len(new_match.qspan) == len(new_match.ispan):
                        # the merged matched is likely aligned
                        next_match.update(current_match)
                        if TRACE_MERGE: logger_debug('    ---> ###merge_matches: next SURROUNDS current, merged as new:', current_match)
                        del rule_matches[i]
                        i -= 1
                        break

                # FIXME: what about the distance??

                # next_match is strictly in increasing sequence: merge in current
                if next_match.is_after(current_match):
                    current_match.update(next_match)
                    if TRACE_MERGE: logger_debug('    ---> ###merge_matches: next follows current, merged as new:', current_match)
                    del rule_matches[j]
                    continue

                # next_match overlaps
                # Check increasing sequence and overlap importance to decide merge
                if (current_match.qstart <= next_match.qstart
                and current_match.qend <= next_match.qend
                and current_match.istart <= next_match.istart
                and current_match.iend <= next_match.iend):
                    qoverlap = current_match.qspan.overlap(next_match.qspan)
                    if qoverlap:
                        ioverlap = current_match.ispan.overlap(next_match.ispan)
                        # only merge if overlaps are equals (otherwise they are not aligned)
                        if qoverlap == ioverlap:
                            current_match.update(next_match)
                            if TRACE_MERGE: logger_debug('    ---> ###merge_matches: next overlaps in sequence current, merged as new:', current_match)
                            del rule_matches[j]
                            continue

                j += 1
            i += 1
        merged.extend(rule_matches)
    return merged

# FIXME we should consider the length and distance between matches to break
# early from the loops: trying to check containment on wildly separated matches
# does not make sense


def filter_contained_matches(matches):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matche that are contained  in larger matches.

    For instance a match entirely contained in another bigger match is removed.
    When more than one matched position matches the same license(s), only one
    match of this set is kept.
    """

    # do not bother if there is only one match
    if len(matches) < 2:
        return matches, []

    discarded = []
    discarded_append = discarded.append

    # sort on start, longer high, longer match, matcher type
    sorter = lambda m: (m.qspan.start, -m.hilen(), -m.len(), m.matcher)
    matches = sorted(matches, key=sorter)

    if TRACE_FILTER_CONTAINED: print('filter_contained_matches: number of matches to process:', len(matches))
    if TRACE_FILTER_CONTAINED:
        print('filter_contained_matches: initial matches')
        for m in matches:
            print(m)

    # compare two matches in the sorted sequence: current and next match we
    # progressively compare a pair and remove next or current
    i = 0
    while i < len(matches) - 1:
        j = i + 1
        while j < len(matches):
            current_match = matches[i]
            next_match = matches[j]
            if TRACE_FILTER_CONTAINED:
                logger_debug('---> filter_contained_matches: current: i=', i, current_match)
                logger_debug('---> filter_contained_matches: next:    j=', j, next_match)

            # BREAK/shortcircuit rather than continue since continuing looking
            # next matches will yield no new findings. e.g. stop when no overlap
            # is possible. Based on sorting order if no overlap is possible,
            # then no future overlap will be possible with the current match.
            # Note that touching and overlapping matches have a zero distance.
            if next_match.qend > current_match.qend:
                if TRACE_FILTER_CONTAINED: logger_debug('    ---> ###filter_contained_matches: matches have a distance: NO OVERLAP POSSIBLE -->', 'qdist:', current_match.qdistance_to(next_match))
                j += 1
                break

            # equals matched spans
            if current_match.qspan == next_match.qspan:
                if current_match.coverage() >= next_match.coverage():
                    if TRACE_FILTER_CONTAINED: logger_debug('    ---> ###filter_contained_matches: next EQUALS current, removed next with lower or equal coverage', matches[j])
                    discarded_append(next_match)
                    del matches[j]
                    continue
                else:
                    if TRACE_FILTER_CONTAINED: logger_debug('    ---> ###filter_contained_matches: next EQUALS current, removed current with lower coverage', matches[i])
                    discarded_append(current_match)
                    del matches[i]
                    i -= 1
                    break

            # remove contained matched spans
            if current_match.qcontains(next_match):
                if TRACE_FILTER_CONTAINED: logger_debug('    ---> ###filter_contained_matches: next CONTAINED in current, removed next', matches[j])
                discarded_append(next_match)
                del matches[j]
                continue

            # remove contained matches the other way
            if next_match.qcontains(current_match):
                if TRACE_FILTER_CONTAINED: logger_debug('    ---> ###filter_contained_matches: current CONTAINED in next, removed current', matches[i])
                discarded_append(current_match)
                del matches[i]
                i -= 1
                break

            j += 1
        i += 1

    return matches, discarded

# FIXME: there are some corner cases where we discard small overalapping matches
# correctly but then we later also discard entirely the containing match meaning
# we underreport


def filter_overlapping_matches(matches, skip_contiguous_false_positive=True):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    certain overlapping matches using the importance of this overlap.

    For instance a shorter match mostly overlapping with another neighboring and
    larger match may be removed.
    """

    # do not bother if there is only one match
    if len(matches) < 2:
        return matches, []

    discarded = []
    discarded_append = discarded.append

    # overlap relationships and thresholds between two matches: based on
    # this containment we may prefer one match over the other and discard a
    # match
    OVERLAP_SMALL = 0.10
    OVERLAP_MEDIUM = 0.40
    OVERLAP_LARGE = 0.70
    OVERLAP_EXTRA_LARGE = 0.90

    # sort on start, longer high, longer match, matcher type
    sorter = lambda m: (m.qspan.start, -m.hilen(), -m.len(), m.matcher)
    matches = sorted(matches, key=sorter)

    if TRACE_FILTER_OVERLAPPING:
        logger_debug('filter_overlapping_matches: number of matches to process:', len(matches))
    if TRACE_FILTER_OVERLAPPING:
        logger_debug('filter_overlapping_matches: initial matches')
        for m in matches:
            logger_debug('  ', m)

    # compare two matches in the sorted sequence: current and next match we
    # progressively compare a pair and remove next or current
    i = 0
    while i < len(matches) - 1:
        j = i + 1
        while j < len(matches):
            current_match = matches[i]
            next_match = matches[j]
            if TRACE_FILTER_OVERLAPPING:
                logger_debug('  ---> filter_overlapping_matches: current: i=', i, current_match)
                logger_debug('  ---> filter_overlapping_matches: next:    j=', j, next_match)

            # BREAK/shortcircuit rather than continue since continuing looking
            # next matches will yield no new findings. e.g. stop when no overlap
            # is possible.
            if next_match.qstart > current_match.qend:
#                or not overlap and current_match.qdistance_to(next_match) >= 0
#             and ):
                if TRACE_FILTER_OVERLAPPING: logger_debug('    ---> ###filter_overlapping_matches: matches disjoint: NO OVERLAP POSSIBLE -->', 'qdist:', current_match.qdistance_to(next_match))
                j += 1
                break

            overlap = current_match.overlap(next_match)
            if not overlap:
                if TRACE_FILTER_OVERLAPPING: logger_debug('    ---> ###filter_overlapping_matches: matches do not overlap: NO OVERLAP POSSIBLE -->', 'qdist:', current_match.qdistance_to(next_match))
                j += 1
                continue

            if (skip_contiguous_false_positive
                and current_match.rule.is_false_positive
                and next_match.rule.is_false_positive
            ):
                if TRACE_FILTER_OVERLAPPING: logger_debug('    ---> ###filter_overlapping_matches: overlaping FALSE POSITIVES are not treated as overlaping.')
                j += 1
                continue

            # next match overlaps with current
            # handle overlapping matches: determine overlap and containment relationships
            overlap_ratio_to_next = overlap / next_match.len()

            extra_large_next = overlap_ratio_to_next >= OVERLAP_EXTRA_LARGE
            large_next = overlap_ratio_to_next >= OVERLAP_LARGE
            medium_next = overlap_ratio_to_next >= OVERLAP_MEDIUM
            small_next = overlap_ratio_to_next >= OVERLAP_SMALL

            # current match overlap to next
            overlap_ratio_to_current = overlap / current_match.len()

            extra_large_current = overlap_ratio_to_current >= OVERLAP_EXTRA_LARGE
            large_current = overlap_ratio_to_current >= OVERLAP_LARGE
            medium_current = overlap_ratio_to_current >= OVERLAP_MEDIUM
            small_current = overlap_ratio_to_current >= OVERLAP_SMALL

            if TRACE_FILTER_OVERLAPPING:
                logger_debug(
                    '  ---> ###filter_overlapping_matches:',
                    'overlap:', overlap,
                    'containment of next to current is:',
                    'overlap_ratio_to_next:', overlap_ratio_to_next,
                    (extra_large_next and 'EXTRA_LARGE')
                    or (large_next and 'LARGE')
                    or (medium_next and 'MEDIUM')
                    or (small_next and 'SMALL')
                    or 'NOT CONTAINED',
                    'containment of current to next is:',
                    'overlap_ratio_to_current:', overlap_ratio_to_current,
                    (extra_large_current and 'EXTRA_LARGE')
                    or (large_current and 'LARGE')
                    or (medium_current and 'MEDIUM')
                    or (small_current and 'SMALL')
                    or 'NOT CONTAINED',
                )

            if extra_large_next and current_match.len() >= next_match.len():
                if TRACE_FILTER_OVERLAPPING: logger_debug('      ---> ###filter_overlapping_matches: EXTRA_LARGE next included, removed shorter next', matches[j])
                discarded_append(next_match)
                del matches[j]
                continue

            if extra_large_current and current_match.len() <= next_match.len():
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug(
                        '      ---> ###filter_overlapping_matches: EXTRA_LARGE next includes '
                        'current, removed shorter current', matches[i])
                discarded_append(current_match)
                del matches[i]
                i -= 1
                break

            if large_next and current_match.len() >= next_match.len() and current_match.hilen() >= next_match.hilen():
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug(
                        '      ---> ###filter_overlapping_matches: LARGE next included, '
                        'removed shorter next', matches[j])
                discarded_append(next_match)
                del matches[j]
                continue

            if large_current and current_match.len() <= next_match.len() and current_match.hilen() <= next_match.hilen():
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug(
                        '      ---> ###filter_overlapping_matches: LARGE next includes '
                        'current, removed shorter current', matches[i])
                discarded_append(current_match)
                del matches[i]
                i -= 1
                break

            if medium_next:
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug('    ---> ###filter_overlapping_matches: MEDIUM NEXT')
                if (current_match.licensing_contains(next_match)
                    and current_match.len() >= next_match.len()
                    and current_match.hilen() >= next_match.hilen()
                ):
                    if TRACE_FILTER_OVERLAPPING:
                        logger_debug(
                            '      ---> ###filter_overlapping_matches: MEDIUM next included '
                            'with next licensing contained, removed next', matches[j],)
                    discarded_append(next_match)
                    del matches[j]
                    continue

                if (next_match.licensing_contains(current_match)
                    and current_match.len() <= next_match.len()
                    and current_match.hilen() <= next_match.hilen()
                ):
                    if TRACE_FILTER_OVERLAPPING:
                        logger_debug(
                            '      ---> ###filter_overlapping_matches: MEDIUM next includes '
                            'current with current licensing contained, removed current', matches[i])
                    discarded_append(current_match)
                    del matches[i]
                    i -= 1
                    break

            if medium_current:
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug('    ---> ###filter_overlapping_matches: MEDIUM CURRENT')
                if (current_match.licensing_contains(next_match)
                    and current_match.len() >= next_match.len()
                    and current_match.hilen() >= next_match.hilen()
                ):
                    if TRACE_FILTER_OVERLAPPING:
                        logger_debug(
                            '      ---> ###filter_overlapping_matches: MEDIUM current, '
                            'bigger current with next licensing contained, removed next', matches[j])
                    discarded_append(next_match)
                    del matches[j]
                    continue

                if (next_match.licensing_contains(current_match)
                    and current_match.len() <= next_match.len()
                    and current_match.hilen() <= next_match.hilen()
                ):
                    if TRACE_FILTER_OVERLAPPING:
                        logger_debug(
                            '      ---> ###filter_overlapping_matches: MEDIUM current, '
                            'bigger next current with current licensing contained, removed current', matches[i])
                    discarded_append(current_match)
                    del matches[i]
                    i -= 1
                    break

            if (small_next
                and current_match.surround(next_match)
                and current_match.licensing_contains(next_match)
                and current_match.len() >= next_match.len()
                and current_match.hilen() >= next_match.hilen()
            ):
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug(
                        '      ---> ###filter_overlapping_matches: SMALL next surrounded, '
                        'removed next', matches[j])
                discarded_append(next_match)
                del matches[j]
                continue

            if (small_current
                and next_match.surround(current_match)
                and next_match.licensing_contains(current_match)
                and current_match.len() <= next_match.len()
                and current_match.hilen() <= next_match.hilen()
            ):
                if TRACE_FILTER_OVERLAPPING:
                    logger_debug(
                        '      ---> ###filter_overlapping_matches: SMALL current surrounded, '
                        'removed current', matches[i])
                discarded_append(next_match)
                del matches[i]
                i -= 1
                break

            # check the previous current and next match: discard current if it
            # is entirely contained in a combined previous and next and previous
            # and next do not overlap

            # ensure that we have a previous
            if i:
                previous_match = matches[i - 1]
                # ensure previous and next do not overlap
                if not previous_match.overlap(next_match):
                    # ensure most of current is contained in the previous and next overlap
                    cpo = current_match.overlap(previous_match)
                    cno = current_match.overlap(next_match)
                    if cpo and cno:
                        overlap_len = cno + cpo
                        clen = current_match.len()
                        # we want at least 90% of the current that is in the overlap
                        if overlap_len >= (clen * 0.9):
                            if TRACE_FILTER_OVERLAPPING:
                                logger_debug(
                                    '      ---> ###filter_overlapping_matches: current mostly '
                                    'contained in previsou and next, removed current', matches[i])
                            discarded_append(next_match)
                            del matches[i]
                            i -= 1
                            break

            j += 1
        i += 1

    if TRACE_FILTER_OVERLAPPING:
        print('filter_overlapping_matches: final  matches')
        for m in matches:
            print('  ', m)
        print('filter_overlapping_matches: final  discarded')
        for m in discarded:
            print('  ', m)

    return matches, discarded


def restore_non_overlapping(matches, discarded):
    """
    Return a tuple of (matches, discarded) sequences of LicenseMatch given
    `matches` and `discarded` sequences of LicenseMatch. Reintegrate as matches
    these that may have been filtered too agressively.
    """
    all_matched_qspans = Span().union(*(m.qspan for m in matches))
    to_keep = []
    to_discard = []
    for disc in merge_matches(discarded):
        if not disc.qspan & all_matched_qspans:
            # keep previously discarded matches that do not intersect at all
            to_keep.append(disc)
        else:
            to_discard.append(disc)
    return to_keep, to_discard


def filter_rule_min_coverage(matches):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches that have a coverage below a rule-defined minimum coverage.
    """
    from licensedcode.match_seq import MATCH_SEQ

    kept = []
    discarded = []
    for match in matches:
        # always keep exact matches
        if match.matcher != MATCH_SEQ:
            kept.append(match)
            continue
        if match.coverage() < match.rule.minimum_coverage:
            if TRACE_FILTER_RULE_MIN_COVERAGE: logger_debug('    ==> DISCARDING rule.minimum_coverage:', type(match.rule.minimum_coverage), ':', repr(match.rule.minimum_coverage), 'match:', match)
            discarded.append(match)
        else:
            kept.append(match)
    return kept, discarded


def filter_if_only_known_words_rule(matches):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches to rules with the "only_known_words" attribute set to True and that
    contain unknown words in their matched range.
    """
    kept = []
    discarded = []

    for match in matches:
        if not match.rule.only_known_words:
            kept.append(match)
            continue

        if match.qrange() != match.qmagnitude() or match.qcontains_stopwords():
            if TRACE_FILTER_LOW_SCORE: logger_debug('    ==> DISCARDING small score:', match)

            # we have unknown tokens in the matched range
            discarded.append(match)
        else:
            kept.append(match)
    return kept, discarded


def filter_low_score(matches, min_score=100):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches scoring below `min_score`.
    """
    if not min_score:
        return matches, []

    kept = []
    discarded = []
    for match in matches:
        if match.score() < min_score:
            if TRACE_FILTER_LOW_SCORE: logger_debug('    ==> DISCARDING low score:', match)
            discarded.append(match)
        else:
            kept.append(match)
    return kept, discarded


def filter_spurious_single_token(matches, query=None, unknown_count=5,
                                 trace=TRACE_FILTER_SPURIOUS_SINGLE_TOKEN):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches to a single token considered as "spurious" matches.

    A "spurious" single token match is a match to a single token that is
    surrounded on both sides by at least `unknown_count` tokens that are either
    unknown tokens, short tokens composed of a single character or tokens
    composed only of digits.
    """
    from licensedcode.match_seq import MATCH_SEQ
    kept = []
    discarded = []
    if not query:
        return matches, discarded

    unknowns_by_pos = query.unknowns_by_pos
    shorts_and_digits = query.shorts_and_digits_pos
    for match in matches:
        if not match.len() == 1:
            kept.append(match)
            continue
        # always keep extact matches
        if match.matcher != MATCH_SEQ:
            kept.append(match)
            continue

        qstart = match.qstart
        qend = match.qend

        # compute the number of unknown tokens before and after this single
        # matched position note: unknowns_by_pos is a defaultdict(int),
        # shorts_and_digits is a set of integers
        before = unknowns_by_pos[qstart - 1]
        for p in range(qstart - 1 - unknown_count, qstart):
            if p in shorts_and_digits:
                before += 1
        if before < unknown_count:
            if trace: logger_debug('    ==> !!! NOT DISCARDING spurious_single_token, not enough before:', match, before)
            if trace: _debug_print_matched_query_text(match, query, extras=unknown_count, logger_debug=logger_debug)
            kept.append(match)
            continue

        after = unknowns_by_pos[qstart]
        for p in range(qend, qend + 1 + unknown_count):
            if p in shorts_and_digits:
                after += 1

        if after >= unknown_count:
            if trace: logger_debug('    ==> DISCARDING spurious_single_token:', match)
            if trace: _debug_print_matched_query_text(match, query, extras=unknown_count, logger_debug=logger_debug)
            discarded.append(match)
        else:
            if trace: logger_debug('    ==> !!! NOT DISCARDING spurious_single_token, not enough after:', match, before, after)
            if trace: _debug_print_matched_query_text(match, query, extras=unknown_count, logger_debug=logger_debug)
            kept.append(match)
    return kept, discarded


def filter_short_matches(matches, trace=TRACE_FILTER_SHORT):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches considered as too small to be relevant.
    """
    from licensedcode.match_seq import MATCH_SEQ
    kept = []
    discarded = []
    for match in matches:
        # always keep exact matches
        if match.matcher != MATCH_SEQ:
            kept.append(match)
            continue

        if match.is_small():
            if trace: logger_debug('    ==> DISCARDING SHORT:', match)
            discarded.append(match)
        else:
            if trace: logger_debug('  ===> NOT DISCARDING SHORT:', match)
            kept.append(match)
    return kept, discarded


def filter_spurious_matches(matches):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches considered as irrelevant or spurious.

    Spurious matches are matches with a low density (e.g. where the matched
    tokens are separated by many unmatched tokens.)
    """
    from licensedcode.match_seq import MATCH_SEQ
    kept = []
    discarded = []

    for match in matches:
        # always keep exact matches
        if match.matcher != MATCH_SEQ:
            kept.append(match)
            continue

        qdens = match.qdensity()
        idens = match.idensity()
        mlen = match.len()
        hilen = match.hilen()
        if (mlen < 10 and (qdens < 0.1 or idens < 0.1)):
            if TRACE_FILTER_SPURIOUS: logger_debug('    ==> DISCARDING Spurious1:', match)
            discarded.append(match)
        elif (mlen < 15 and (qdens < 0.2 or idens < 0.2)):
            if TRACE_FILTER_SPURIOUS: logger_debug('    ==> DISCARDING Spurious2:', match)
            discarded.append(match)
        elif (mlen < 20 and hilen < 5 and (qdens < 0.3 or idens < 0.3)):
            if TRACE_FILTER_SPURIOUS: logger_debug('    ==> DISCARDING Spurious3:', match)
            discarded.append(match)
        elif (mlen < 30 and hilen < 8 and (qdens < 0.4 or idens < 0.4)):
            if TRACE_FILTER_SPURIOUS: logger_debug('    ==> DISCARDING Spurious4:', match)
            discarded.append(match)
        elif (qdens < 0.5 or idens < 0.5):
            if TRACE_FILTER_SPURIOUS: logger_debug('    ==> DISCARDING Spurious5:', match)
            discarded.append(match)
        else:
            kept.append(match)
    return kept, discarded


def filter_false_positive_matches(matches, idx=None):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches to false positive rules.
    """
    kept = []
    discarded = []

    for match in matches:
        if match.rule.is_false_positive:
            if TRACE_REFINE: logger_debug('    ==> DISCARDING FALSE POSITIVE:', match)
            discarded.append(match)
            continue
        kept.append(match)

    return kept, discarded


def filter_already_matched_matches(matches, query):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches that have at least one position that is already matched in the Query
    `query`.
    """
    kept = []
    discarded = []
    matched_pos = query.matched
    for match in matches:
        # FIXME: do not use internals!!!
        match_qspan_set = match.qspan._set
        if match_qspan_set & matched_pos:
            # discard any match that has any position already matched
            if TRACE_REFINE: logger_debug('    ==> DISCARDING ALREADY MATCHED:', match)
            discarded.append(match)
        else:
            kept.append(match)

    return kept, discarded


def refine_matches(
    matches,
    idx,
    query=None,
    min_score=0,
    filter_false_positive=True,
    merge=True,
):
    """
    Return a filtered list of kept LicenseMatch matches and a list of
    discardable matches given a `matches` list of LicenseMatch by removing
    matches that do not mee certain criteria as defined in multiple filters.
    """
    if TRACE: logger_debug()
    if TRACE: logger_debug(' #####refine_matches: STARTING matches#', len(matches))
    if TRACE_REFINE:
        for m in matches:
            logger_debug(m)

    if merge:
        matches = merge_matches(matches)
        if TRACE: logger_debug('     ##### refine_matches: STARTING MERGED_matches#:', len(matches))

    all_discarded = []

    def _log(_matches, _discarded, msg):
        if TRACE: logger_debug('   #####refine_matches: ', msg, '#', len(matches))
        if TRACE_REFINE:
            for m in matches:
                logger_debug(m)
        if TRACE: logger_debug('   #####refine_matches: NOT', msg, '#', len(_discarded))
        if TRACE_REFINE:
            for m in matches:
                logger_debug(m)

    # FIXME: we should have only a single loop on all the matches at once!!
    # and not 10's of loops!!!

    matches, discarded = filter_rule_min_coverage(matches)
    all_discarded.extend(discarded)
    _log(matches, discarded, 'ABOVE MIN COVERAGE')

    matches, discarded = filter_spurious_single_token(matches, query)
    all_discarded.extend(discarded)
    _log(matches, discarded, 'MORE THAN ONE NON SPURIOUS TOKEN')

    matches, discarded = filter_short_matches(matches)
    all_discarded.extend(discarded)
    _log(matches, discarded, 'LONG ENOUGH')

    matches, discarded = filter_spurious_matches(matches)
    all_discarded.extend(discarded)
    _log(matches, discarded, 'GOOD')

    matches = merge_matches(matches)
    if TRACE: logger_debug(' #####refine_matches: before FILTER matches#', len(matches))
    if TRACE_REFINE:
        for m in matches:
            logger_debug(m)

    matches, discarded_contained = filter_contained_matches(matches)
    _log(matches, discarded_contained, 'NON CONTAINED')

    matches, discarded_overlapping = filter_overlapping_matches(matches)
    _log(matches, discarded_overlapping, 'NON OVERLAPPING')

    if discarded_contained:
        to_keep, discarded_contained = restore_non_overlapping(matches, discarded_contained)
        matches.extend(to_keep)
        all_discarded.extend(discarded_contained)
        _log(to_keep, discarded_contained, 'NON CONTAINED REFINED')

    if discarded_overlapping:
        to_keep, discarded_overlapping = restore_non_overlapping(matches, discarded_overlapping)
        matches.extend(to_keep)
        all_discarded.extend(discarded_overlapping)
        _log(to_keep, discarded_overlapping, 'NON OVERLAPPING REFINED')

    matches, discarded_contained = filter_contained_matches(matches)
    all_discarded.extend(discarded_contained)
    _log(matches, discarded_contained, 'NON CONTAINED')

    matches, discarded = filter_if_only_known_words_rule(matches)
    all_discarded.extend(discarded)
    _log(matches, discarded, 'ACCEPTABLE IF UNKNOWN WORDS')

    if filter_false_positive:
        matches, discarded = filter_false_positive_matches(matches, idx)
        all_discarded.extend(discarded)
        _log(matches, discarded, 'TRUE POSITIVE')

    if min_score:
        matches, discarded = filter_low_score(matches, min_score=min_score)
        all_discarded.extend(discarded)
        _log(matches, discarded, 'HIGH ENOUGH SCORE')

    if merge:
        matches = merge_matches(matches)

    logger_debug('   ##### refine_matches: FINAL MERGED_matches#:', len(matches))
    if TRACE_REFINE:
        for m in matches:
            logger_debug(m)

    return matches, all_discarded


@attr.s(slots=True, frozen=True)
class Token(object):
    """
    Used to represent a token in collected query-side matched texts and SPDX
    identifiers.
    """
    # original text value for this token.
    value = attr.ib()
    # line number, one-based
    line_num = attr.ib()
    # absolute position for known tokens, zero-based. -1 for unknown tokens
    pos = attr.ib(default=-1)
    # True if text/alpha False if this is punctuation or spaces
    is_text = attr.ib(default=False)
    # True if part of a match
    is_matched = attr.ib(default=False)
    # True if this is a known token
    is_known = attr.ib(default=False)


def tokenize_matched_text(location, query_string, dictionary, _cache={}):
    """
    Return a list of Token objects with pos and line number collected from the
    file at `location` or the `query_string` string. `dictionary` is the index
    mapping a token string to a token id.

    NOTE: the _cache={} arg IS A GLOBAL mutable by design.
    """
    key = location, query_string
    cached = _cache.get(key)
    if cached:
        return cached
    # we only cache the last call
    _cache.clear()
    _cache[key] = result = list(
        _tokenize_matched_text(location, query_string, dictionary))
    return result


def _tokenize_matched_text(location, query_string, dictionary):
    """
    Yield Token objects with pos and line number collected from the file at
    `location` or the `query_string` string. `dictionary` is the index mapping
    of tokens to token ids.
    """
    pos = 0
    for line_num, line in query.query_lines(location, query_string, strip=False):
        if TRACE_MATCHED_TEXT_DETAILS:
            logger_debug('  _tokenize_matched_text:',
                'line_num:', line_num,
                'line:', line)

        for is_text, token_str in matched_query_text_tokenizer(line):
            if TRACE_MATCHED_TEXT_DETAILS:
                logger_debug('     is_text:', is_text, 'token_str:', repr(token_str))
            # Determine if a token is is_known in the license index or not. This
            # is essential as we need to realign the query-time tokenization
            # with the full text to report proper matches.
            if is_text and token_str and token_str.strip():  

                # we retokenize using the query tokenizer:
                # 1. to lookup for is_known tokens in the index dictionary

                # 2. to ensure the number of tokens is the same in both
                # tokenizers (though, of course, the case will differ as the
                # regular query tokenizer ignores case and punctuations).

                # NOTE: we have a rare Unicode bug/issue because of some Unicode
                # codepoint such as some Turkish characters that decompose to
                # char + punct when casefolded. This should be fixed in Unicode
                # release 14 and up and likely implemented in Python 3.10 and up
                # See https://github.com/nexB/scancode-toolkit/issues/1872
                # See also: https://bugs.python.org/issue34723#msg359514
                qtokenized = list(index_tokenizer(token_str))
                if not qtokenized:
                    yield Token(
                        value=token_str,
                        line_num=line_num,
                        is_text=is_text,
                        is_known=False,
                        pos=-1,
                    )

                elif len(qtokenized) == 1:
                    is_known = qtokenized[0] in dictionary
                    if is_known:
                        p = pos
                        pos += 1
                    else:
                        p = -1
                    yield Token(
                        value=token_str,
                        line_num=line_num,
                        is_text=is_text,
                        is_known=is_known,
                        pos=p,
                    )
                else:
                    # we have two or more tokens from the original query mapped
                    # to a single matched text tokenizer token.
                    for qtoken in qtokenized:
                        is_known = qtoken in dictionary
                        if is_known:
                            p = pos
                            pos += 1
                        else:
                            p = -1

                        yield Token(
                            value=qtoken,
                            line_num=line_num,
                            is_text=is_text,
                            is_known=is_known,
                            pos=p,
                        )
            else:

                yield Token(
                    value=token_str,
                    line_num=line_num,
                    is_text=False,
                    is_known=False,
                    pos=-1,
                )


def reportable_tokens(tokens, match_qspan, start_line, end_line, whole_lines=False):
    """
    Yield Tokens from a `tokens` iterable of Token objects (built from a query-
    side scanned file or string) that are inside a `match_qspan` matched Span
    starting at `start_line` and ending at `end_line`. If whole_lines is True,
    also yield unmatched Tokens that are before and after the match and on the
    first and last line of a match (unless the lines are very long text lines or
    the match is from binary content.)

    As a side effect, known matched tokens are tagged as is_matched=True if they
    are matched.

    If `whole_lines` is True, any token within matched lines range is included.
    Otherwise, a token is included if its position is within the matched
    match_qspan or it is a punctuation token immediately after the matched
    match_qspan even though not matched.
    """
    start = match_qspan.start
    end = match_qspan.end

    started = False
    finished = False

    end_pos = 0
    last_pos = 0
    for real_pos, tok in enumerate(tokens):
        # ignore tokens outside the matched lines range
        if tok.line_num < start_line:
            continue
        if tok.line_num > end_line:
            break
        if TRACE_MATCHED_TEXT_DETAILS:
            logger_debug('reportable_tokens:', real_pos, tok)

        is_included = False

        # tagged known matched tokens (useful for highlighting)
        if tok.pos != -1 and tok.is_known and tok.pos in match_qspan:
            tok = attr.evolve(tok, is_matched=True)
            is_included = True
            if TRACE_MATCHED_TEXT_DETAILS:
                logger_debug('  tok.is_matched = True', 'match_qspan:', match_qspan)
        else:
            if TRACE_MATCHED_TEXT_DETAILS:
                logger_debug(
                    '  unmatched token: tok.is_matched = False',
                    'match_qspan:', match_qspan,
                    'tok.pos in match_qspan:', tok.pos in match_qspan,
                )

        if whole_lines:
            # we only work on matched lines so no need to test further
            # if start_line <= tok.line_num <= end_line.
            if TRACE_MATCHED_TEXT_DETAILS:
                logger_debug('  whole_lines')
            is_included = True

        else:
            # Are we in the match_qspan range or a punctuation right before or after
            # that range?

            # start
            if not started and tok.pos == start:
                started = True
                if TRACE_MATCHED_TEXT_DETAILS:
                    logger_debug('  start')
                is_included = True

            # middle
            if started and not finished:
                if TRACE_MATCHED_TEXT_DETAILS:
                    logger_debug('    middle')
                is_included = True

            if tok.pos == end:
                if TRACE_MATCHED_TEXT_DETAILS:
                    logger_debug('  at end')
                finished = True
                started = False
                end_pos = real_pos

            # one punctuation token after a match
            if finished and not started and end_pos and last_pos == end_pos:
                end_pos = 0
                if not tok.is_text:
                    # strip the trailing spaces of the last token
                    # tok.value = tok.value.rstrip()
                    if tok.value.strip():
                        if TRACE_MATCHED_TEXT_DETAILS:
                            logger_debug('  end yield')
                        is_included = True

        last_pos = real_pos
        if is_included:
            yield tok


def get_full_matched_text(
    match,
    location=None, query_string=None,
    idx=None,
    whole_lines=False,
    highlight=True, highlight_matched=u'%s', highlight_not_matched=u'[%s]',
    stopwords=STOPWORDS,
    _usecache=True,
):
    """
    Yield unicode strings corresponding to the full matched query text
    given a query file at `location` or a `query_string`, a `match` LicenseMatch
    and an `idx` LicenseIndex.

    This contains the full text including punctuations and spaces that are not
    participating in the match proper including leading and trailing punctuations.

    If `whole_lines` is True, the unmatched part at the start of the first
    matched line and the unmatched part at the end of the last matched lines are
    also included in the returned text (unless the line is very long).

    If `highlight` is True, each token is formatted for "highlighting" and
    emphasis with the `highlight_matched` format string for matched tokens or to
    the `highlight_not_matched` for tokens not matched. The default is to
    enclose an unmatched token sequence in [] square brackets. Punctuation is
    not highlighted.
    """
    if TRACE_MATCHED_TEXT:
        logger_debug('get_full_matched_text:  match:', match)
    assert location or query_string
    assert idx
    # Create and process a stream of Tokens
    if not _usecache:
        # for testing only, reset cache on each call
        tokens = tokenize_matched_text(
            location, query_string, dictionary=idx.dictionary, _cache={})
    else:
        tokens = tokenize_matched_text(
            location, query_string, dictionary=idx.dictionary)

    if TRACE_MATCHED_TEXT:
        tokens = list(tokens)
        print()
        logger_debug('get_full_matched_text:  tokens:')
        for t in tokens:
            print('    ', t)
        print()

    tokens = reportable_tokens(
        tokens, match.qspan, match.start_line, match.end_line, whole_lines=whole_lines)

    if TRACE_MATCHED_TEXT:
        tokens = list(tokens)
        logger_debug('get_full_matched_text:  reportable_tokens:')
        for t in tokens:
            print(t)
        print()

    # Finally yield strings with eventual highlightings
    for token in tokens:
        val = token.value
        if not highlight:
            yield val
        else:
            if token.is_text and val.lower() not in stopwords:
                if token.is_matched:
                    yield highlight_matched % val
                else:
                    yield highlight_not_matched % val
            else:
                # we do not highlight punctuation and stopwords.
                yield val
