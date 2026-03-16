ScanCode license detection overview and key design elements
===========================================================

License detection is about finding common texts between the text of a query file
being scanned and the texts of the indexed license texts and rule texts. The process
strives to be correct first and fast second.

Ideally we want to find the best alignment possible between two texts so we know
exactly where they match: the scanned text and one or more of the many license texts.
We settle for good alignments rather than optimal alignments by still returning
accurate and correct matches in a reasonable amount of time.

Correctness is essential but efficiency too: both in terms of speed and memory usage.
One key to efficient matching is to process not characters but whole words and use
internally not strings but integers to represent a word.


Rules and licenses
------------------

The detection uses an index of reference license texts and a set of "rules" that are
common notices or mentions of these licenses. The things that makes detection
sometimes difficult is that a license reference can be very short as in "this is GPL"
or very long as a full license text for the GPLv3. To cope with this we use different
matching strategies and also compute the resemblance and containment of texts that
are matched.


Words as integers
-----------------

A dictionary mapping words to a unique integer is used to transform a scanned text
"query" words and reference indexed license texts and rules words to numbers.
This is possible because we have a limited number of words across all the license
texts (about 15K). We further assign these ids to words such that very common words
have a low id and less common, more discriminant words have a higher id. And define a
thresholds for this ids range such that very common words below that threshold cannot
possible form a license text or mention together.

Once that mapping is applied, the detection then only deal with integers in two
dimensions:

- the token ids (and whether they are in the high or low range).
- their positions in the query (qpos) and the indexed rule (ipos).

We also use an integer id for a rule.

All operations are from then on dealing with list, arrays or sets of integers in
defined ranges.

Matches are reduced to sets of integers we call "Spans":

- matched positions on the query side
- matched positions on the index side

By using integers in known ranges throughout, several operations are reduced to
integer and integer sets or lists comparisons and intersection. These operations
are faster and more readily optimizable.

With integers, we also use less memory:

- we can use arrays of unsigned 16 bits ints that store each number on two bytes
  rather than bigger lists of ints.
- we can replace dictionaries by sparse lists or arrays where the index is an integer key.
- we can use succinct, bit level representations (e.g. bitmaps) of integer sets.

Smaller data structures also means faster processing as the processors need to move
less data in memory.

With integers we can also be faster:
    
- a dict key lookup is slower than a list of array index lookup.
- processing large list of small structures is faster (such as bitmaps, etc).
- we can leverage libraries that speed up integer set operations.


Common/junk tokens
------------------

The quality and speed of detection is supported by classifying each word as either
good/discriminant or common/junk. Junk tokens are either very frequent of tokens that
taken together together cannot form some valid license mention or notice. When a
numeric id is assigned to a token during initial indexing, junk tokens are assigned a
lower id than good tokens. These are then called low or junk tokens and high or good
tokens.


Query processing
----------------

When a file is scanned, it is first converted to a query object which is a list of
integer token ids. A query is further broken down in slices (a.k.a. query runs) based
on heuristics.

While the query is processed a set of matched and matchable positions for for high
and low token ids is kept to track what is left to do in matching.


Matching pipeline
-----------------

The matching pipeline consist of:

- we start with matching the whole query at once against hashes on the whole text
  looked up agains a mapping of hash to license rule. We exit if we have a match.
 
- then we match the whole query for exact matches using an automaton (Aho-Corasick).
  We exit if we have a match.

- then each query run is processed in sequence:

  - the best potentially matching rules are found with two rounds of approximate
    "set" matching.  This set matching uses a "bag of words" approach where the
    scanned text is transformed in a vector of integers based on the presence or
    absence of a word. It is compared against the index of vectors. This is similar
    conceptually to a traditional inverted index search for information retrieval.
    The best matches are ranked using a resemblance and containment comparison. A
    second round is performed on the best matches using multisets which are set where
    the number of occurrence of each word is also taken into account. The best matches
    are ranked again using a resemblance and containment comparison and is more
    accurate than the previous set matching.
    
  - using the ranked potential candidate matches from the two previous rounds, we
    then perform a pair-wise local sequence alignment between these candidates and
    the query run. This sequence alignment is essentially an optimized diff working
    on integer sequences and takes advantage of the fact that some very frequent
    words are considered less discriminant: this speeds up the sequence alignment
    significantly. The number of multiple local sequence alignments that are required
    in this step is also made much smaller by the pre-matching done using sets.
    
- finally all the collected matches are merged, refined and filtered to yield the
  final results. The merging considers the ressemblance, containment and overlap
  between scanned texts and the matched texts and several secondary factors.
  Filtering is based on the density and length of matches as well as the number of
  good or frequent tokens matched.
  Last, each match receives a score which is the based on the length of the rule text
  and how of this rule text was matched. Optionally we can also collect the exact
  matched texts and which part was not match for each match.


Comparison with other tools approaches
--------------------------------------

Most tools use regular expressions. The problem is that creating these expressions
requires a lot of intimate knowledge of the data set and the relation between each
license texts. The maintenance effort is high. And regex matches typically need a
complex second pass of disambiguation for similar matches.

Some tools use an index of pre-defined sentences and match these as regex and then
reassemble possible matches. They tend to suffer from the same issues as a pure regex
based approach and require an intimate knowledge of the license texts and how they
relate to each other.

Some tools use pair-wise comparisons like ScanCode. But in doing so they usually
perform poorly because a multiple local sequence alignment is an expensisve
computation. Say you scan 1000 files and you have 1000 reference texts. You would
need to recursively make multiple times 1000 comparisons with each scanned file very
quickly performing the equivalent 100 million diffs or more to process these files.
Because of the progressive matching pipeline used in ScanCode, sequence alignments
may not be needed at all in the common cases and when they are, only a few are
needed.

See also this list: https://wiki.debian.org/CopyrightReviewTools
