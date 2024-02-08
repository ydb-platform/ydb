# ruamel.yaml

`ruamel.yaml` is a YAML 1.2 loader/dumper package for Python.
<table class="docutils">
  <tr>    <td>version</td>
    <td>0.17.40</td>
  </tr>
  <tr>    <td>updated</td>
    <td>2023-10-20</td>
  </tr>
  <tr>    <td>documentation</td>
    <td><a href="http://yaml.readthedocs.io">http://yaml.readthedocs.io</a></td>
  </tr>
  <tr>    <td>repository</td>
    <td><a href="https://sourceforge.net/projects/ruamel-yaml">https://sourceforge.net/projects/ruamel-yaml</a></td>
  </tr>
  <tr>    <td>pypi</td>
    <td><a href="https://pypi.org/project/ruamel.yaml">https://pypi.org/project/ruamel.yaml</a></td>
  </tr>
</table>

*Starting with 0.17.22 only Python 3.7+ is supported. The 0.17 series is
also the last to support old PyYAML functions, replace it by creating a*
`YAML()` *instance and use its* `.load()` *and* `.dump()` *methods.*
**New(er) functionality is usually only available via the new API.**

The 0.17.21 was the last one tested to be working on Python 3.5 and 3.6
(the latter was not tested, because tox/virtualenv stopped supporting
that EOL version). The 0.16.13 release was the last that was tested to
be working on Python 2.7.

*Please adjust/pin your dependencies accordingly if necessary.*
(`ruamel.yaml<0.18`)

There are now two extra plug-in packages
(`ruamel.yaml.bytes` and `ruamel.yaml.string`)
for those not wanting to do the streaming to a
`io.BytesIO/StringIO` buffer themselves.

If your package uses `ruamel.yaml` and is not listed on PyPI, drop me an
email, preferably with some information on how you use the package (or a
link to the repository) and I'll keep you informed when the status of
the API is stable enough to make the transition.

-   [Overview](http://yaml.readthedocs.io/en/latest/overview/)
-   [Installing](http://yaml.readthedocs.io/en/latest/install/)
-   [Basic Usage](http://yaml.readthedocs.io/en/latest/basicuse/)
-   [Details](http://yaml.readthedocs.io/en/latest/detail/)
-   [Examples](http://yaml.readthedocs.io/en/latest/example/)
-   [API](http://yaml.readthedocs.io/en/latest/api/)
-   [Differences with
    PyYAML](http://yaml.readthedocs.io/en/latest/pyyaml/)

[![image](https://readthedocs.org/projects/yaml/badge/?version=latest)](https://yaml.readthedocs.org/en/latest?badge=latest)[![image](https://bestpractices.coreinfrastructure.org/projects/1128/badge)](https://bestpractices.coreinfrastructure.org/projects/1128)
[![image](https://sourceforge.net/p/ruamel-yaml/code/ci/default/tree/_doc/_static/license.svg?format=raw)](https://opensource.org/licenses/MIT)
[![image](https://sourceforge.net/p/ruamel-yaml/code/ci/default/tree/_doc/_static/pypi.svg?format=raw)](https://pypi.org/project/ruamel.yaml/)
[![image](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://pypi.org/project/oitnb/)
[![image](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)

# ChangeLog

0.17.40 (2023-10-20):

- flow style sets are now preserved ( `!!set {a, b, c} )`. Any values specified when loading are dropped, including `!!null ""`.
- potential workaround for issue 484: the long_description_content_type including the variant specification `CommonMark`
can result in problems on Azure. If you can install from `.tar.gz` using
`RUAMEL_NO_LONG_DESCRIPTION=1 pip install ruamel.yaml --no-binary :all:` then the long description, and its
offending type, are nog included (in the METADATA). 
(Reported by [Coury Ditch](https://sourceforge.net/u/cmditch/profile/))

- links in documentation update (reported by [David Hoese](https://sourceforge.net/u/daveydave400/profile/))
- Added some `__repr__` for internally used classes

0.17.39 (2023-10-19):

- update README generation, no code changes

0.17.36 (2023-10-19):

- fixed issue 480, dumping of a loaded empty flow-style mapping with comment failed (Reported by [Stéphane Brunner](https://sourceforge.net/u/stbrunner/profile/))
- fixed issue 482, caused by DEFAULT_MAPPING_TAG having changes to being a `Tag()` instance, not a string (reported by [yan12125](https://sourceforge.net/u/yan12125/profile/))
- updated documentation to use mkdocs

0.17.35 (2023-10-04):

- support for loading dataclasses with `InitVar` variables (some special coding was necessary to get the, unexecpected, default value in the corresponding instance attribute ( example of usage in [this question](https://stackoverflow.com/q/77228378/1307905))

0.17.34 (2023-10-03):

- Python 3.12 also loads C version when using `typ='safe'`
- initial support for loading invoking
`__post_init__()` on dataclasses that have that
method after loading a registered dataclass.
(Originally
[asked](https://stackoverflow.com/q/51529458/1307905) on
Stackoverflow by
[nyanpasu64](https://stackoverflow.com/users/2683842/nyanpasu64)
and as
[ticket](https://sourceforge.net/p/ruamel-yaml/tickets/355/) by
[Patrick Lehmann](https://sourceforge.net/u/paebbels/profile/))

```
@yaml.register_class
@dataclass
class ...
```

0.17.33 (2023-09-28):

- added `flow_seq_start`, `flow_seq_end`, `flow_seq_separator`, `flow_map_start`, `flow_map_end`, `flow_map_separator` **class** attributes to the `Emitter` class so flow style output can more easily be influenced (based on [this answer](https://stackoverflow.com/a/76547814/1307905) on a StackOverflow question by [Huw Walters](https://stackoverflow.com/users/291033/huw-walters)).

0.17.32 (2023-06-17):

- fix issue with scanner getting stuck in infinite loop

0.17.31 (2023-05-31):

- added tag.setter on `ScalarEvent` and on `Node`, that takes either a `Tag` instance, or a str (reported by [Sorin Sbarnea](https://sourceforge.net/u/ssbarnea/profile/))

0.17.30 (2023-05-30):

- fix issue 467, caused by Tag instances not being hashable (reported by [Douglas Raillard](https://bitbucket.org/%7Bcf052d92-a278-4339-9aa8-de41923bb556%7D/))

0.17.29 (2023-05-30):

- changed the internals of the tag property from a string to a class which allows for preservation of the original handle and suffix. This should result in better results using documents with %TAG directives, as well as preserving URI escapes in tag suffixes.

0.17.28 (2023-05-26):

- fix for issue 464: documents ending with document end marker
without final newline fail to load (reported by [Mariusz
Rusiniak](https://sourceforge.net/u/r2dan/profile/))

0.17.27 (2023-05-25):

- fix issue with inline mappings as value for merge keys (reported by Sirish on [StackOverflow](https://stackoverflow.com/q/76331049/1307905))
- fix for 468, error inserting after accessing merge attribute on `CommentedMap` (reported by [Bastien gerard](https://sourceforge.net/u/bagerard/))
- fix for issue 461 pop + insert on same `CommentedMap` key throwing error (reported by [John Thorvald Wodder II](https://sourceforge.net/u/jwodder/profile/))

0.17.26 (2023-05-09):

- fix for error on edge cage for issue 459

0.17.25 (2023-05-09):

- fix for regression while dumping wrapped strings with too many backslashes removed (issue 459, reported by [Lele Gaifax](https://sourceforge.net/u/lele/profile/))

0.17.24 (2023-05-06):

- rewrite of `CommentedMap.insert()`. If you have a merge key in the YAML document for the mapping you insert to, the position value should be the one as you look at the YAML input. This fixes issue 453 where other keys of a merged in mapping would show up after an insert (reported by [Alex Miller](https://sourceforge.net/u/millerdevel/profile/)). It also fixes a call to `.insert()` resulting into the merge key to move to be the first key if it wasn't already and it is also now possible to insert a key before a merge key (even if the fist key in the mapping).
- fix (in the pure Python implementation including default) for issue 447. (reported by [Jack Cherng](https://sourceforge.net/u/jfcherng/profile/), also brought up by brent on [StackOverflow](https://stackoverflow.com/q/40072485/1307905))

0.17.23 (2023-05-05):

- fix 458, error on plain scalars starting with word longer than width. (reported by [Kyle Larose](https://sourceforge.net/u/klarose/profile/))
- fix for `.update()` no longer correctly handling keyword arguments (reported by John Lin on [StackOverflow]( https://stackoverflow.com/q/76089100/1307905))
- fix issue 454: high Unicode (emojis) in quoted strings always
escaped (reported by [Michal
Čihař](https://sourceforge.net/u/nijel/profile/) based on a
question on StackOverflow).
- fix issue with emitter conservatively inserting extra backslashes in wrapped quoted strings (reported by thebenman on [StackOverflow](https://stackoverflow.com/q/75631454/1307905))

0.17.22 (2023-05-02):

- fix issue 449 where the second exclamation marks got URL encoded (reported and fixing PR provided by [John Stark](https://sourceforge.net/u/jods/profile/))
- fix issue with indent != 2 and literal scalars with empty first line (reported by wrdis on [StackOverflow](https://stackoverflow.com/q/75584262/1307905))
- updated `__repr__` of CommentedMap, now that Python's dict is ordered -> no more `ordereddict(list-of-tuples)`
- merge MR 4, handling OctalInt in YAML 1.1 (provided by [Jacob Floyd](https://sourceforge.net/u/cognifloyd/profile/))
- fix loading of `!!float 42` (reported by Eric on [Stack overflow](https://stackoverflow.com/a/71555107/1307905))
- line numbers are now set on `CommentedKeySeq` and `CommentedKeyMap` (which are created if you have a sequence resp. mapping as the key in a mapping)
- plain scalars: put single words longer than width on a line of
their own, instead of after the previous line (issue 427, reported
by [Antoine
Cotten](https://sourceforge.net/u/antoineco/profile/)). Caveat:
this currently results in a space ending the previous line.
- fix for folded scalar part of 421: comments after ">" on first
line of folded scalars are now preserved (as were those in the
same position on literal scalars). Issue reported by Jacob Floyd.
- added stacklevel to warnings
- typing changed from Py2 compatible comments to Py3, removed various Py2-isms

0.17.21 (2022-02-12):

- fix bug in calling `.compose()` method with `pathlib.Path` instance.

0.17.20 (2022-01-03):

- fix error in microseconds while rounding datetime fractions >= 9999995 (reported by [Luis Ferreira](https://sourceforge.net/u/ljmf00/))

0.17.19 (2021-12-26):

- fix mypy problems (reported by [Arun](https://sourceforge.net/u/arunppsg/profile/))

0.17.18 (2021-12-24):

- copy-paste error in folded scalar comment attachment (reported by [Stephan Geulette](https://sourceforge.net/u/sgeulette/profile/))
- fix 411, indent error comment between key empty seq value (reported by [Guillermo Julián](https://sourceforge.net/u/gjulianm/profile/))

0.17.17 (2021-10-31):

- extract timestamp matching/creation to util

0.17.16 (2021-08-28):

- 398 also handle issue 397 when comment is newline

0.17.15 (2021-08-28):

- fix issue 397, insert comment before key when a comment between key and value exists (reported by [Bastien gerard](https://sourceforge.net/u/bagerard/))

0.17.14 (2021-08-25):

- fix issue 396, inserting key/val in merged-in dictionary (reported by [Bastien gerard](https://sourceforge.net/u/bagerard/))

0.17.13 (2021-08-21):

- minor fix in attr handling

0.17.12 (2021-08-21):

- fix issue with anchor on registered class not preserved and those classes using package attrs with `@attr.s()` (both reported by [ssph](https://sourceforge.net/u/sph/))

0.17.11 (2021-08-19):

- fix error baseclass for `DuplicateKeyError` (reported by [Łukasz Rogalski](https://sourceforge.net/u/lrogalski/))
- fix typo in reader error message, causing `KeyError` during reader error (reported by [MTU](https://sourceforge.net/u/mtu/))

0.17.10 (2021-06-24):

- fix issue 388, token with old comment structure != two elements (reported by [Dimitrios Bariamis](https://sourceforge.net/u/dbdbc/))

0.17.9 (2021-06-10):

- fix issue with updating CommentedMap (reported by sri on [StackOverflow](https://stackoverflow.com/q/67911659/1307905))

0.17.8 (2021-06-09):

- fix for issue 387 where templated anchors on tagged object did get set resulting in potential id reuse. (reported by [Artem Ploujnikov](https://sourceforge.net/u/flexthink/))

0.17.7 (2021-05-31):

- issue 385 also affected other deprecated loaders (reported via email by Oren Watson)

0.17.6 (2021-05-31):

- merged type annotations update provided by [Jochen Sprickerhof](https://sourceforge.net/u/jspricke/)
- fix for issue 385: deprecated round_trip_loader function not
working (reported by [Mike
Gouline](https://sourceforge.net/u/gouline/))
- wasted a few hours getting rid of mypy warnings/errors

0.17.5 (2021-05-30):

- fix for issue 384 `!!set` with aliased entry resulting in broken YAML on rt reported by [William Kimball](https://sourceforge.net/u/william303/))

0.17.4 (2021-04-07):

- prevent (empty) comments from throwing assertion error (issue 351 reported by [William Kimball](https://sourceforge.net/u/william303/)) comments (or empty line) will be dropped

0.17.3 (2021-04-07):

- fix for issue 382 caused by an error in a format string (reported by [William Kimball](https://sourceforge.net/u/william303/))
- allow expansion of aliases by setting `yaml.composer.return_alias = lambda s: copy.deepcopy(s)`
(as per [Stackoverflow answer](https://stackoverflow.com/a/66983530/1307905))

0.17.2 (2021-03-29):

- change -py2.py3-none-any.whl to -py3-none-any.whl, and remove 0.17.1

0.17.1 (2021-03-29):

- added 'Programming Language :: Python :: 3 :: Only', and
removing 0.17.0 from PyPI (reported by [Alasdair
Nicol](https://sourceforge.net/u/alasdairnicol/))

0.17.0 (2021-03-26):

- removed because of incomplete classifiers
- this release no longer supports Python 2.7, most if not all Python 2 specific code is removed. The 0.17.x series is the last to support Python 3.5 (this also allowed for removal of the dependency on `ruamel.std.pathlib`)
- remove Python2 specific code branches and adaptations (u-strings)
- prepare % code for f-strings using `_F`
- allow PyOxidisation ([issue 324](https://sourceforge.net/p/ruamel-yaml/tickets/324/) resp. [issue 171](https://github.com/indygreg/PyOxidizer/issues/171))
- replaced Python 2 compatible enforcement of keyword arguments with '*'
- the old top level *functions* `load`, `safe_load`, `round_trip_load`, `dump`, `safe_dump`, `round_trip_dump`, `scan`, `parse`, `compose`, `emit`, `serialize` as well as their `_all` variants for multi-document streams, now issue a `PendingDeprecationning` (e.g. when run from pytest, but also Python is started with `-Wd`). Use the methods on `YAML()`, which have been extended.
- fix for issue 376: indentation changes could put literal/folded
scalar to start before the `#` column of a following comment.
Effectively making the comment part of the scalar in the output.
(reported by [Bence Nagy](https://sourceforge.net/u/underyx/))

0.16.13 (2021-03-05):

- fix for issue 359: could not update() CommentedMap with keyword
arguments (reported by [Steve
Franchak](https://sourceforge.net/u/binaryadder/))
- fix for issue 365: unable to dump mutated TimeStamp objects
(reported by [Anton Akmerov](https://sourceforge.net/u/akhmerov))
- fix for issue 371: unable to add comment without starting space
(reported by [Mark Grandi](https://sourceforge.net/u/mgrandi))
- fix for issue 373: recursive call to walk_tree not preserving
all params (reported by [eulores](https://sourceforge.net/u/eulores/))
- a None value in a flow-style sequence is now dumped as `null` instead of `!!null ''` (reported by mcarans on [StackOverflow](https://stackoverflow.com/a/66489600/1307905))

0.16.12 (2020-09-04):

- update links in doc

0.16.11 (2020-09-03):

- workaround issue with setuptools 0.50 and importing pip (fix by [jaraco](https://github.com/pypa/setuptools/issues/2355#issuecomment-685159580)

0.16.10 (2020-02-12):

- (auto) updated image references in README to sourceforge

0.16.9 (2020-02-11):

- update CHANGES

0.16.8 (2020-02-11):

- update requirements so that ruamel.yaml.clib is installed for 3.8, as it has become available (via manylinux builds)

0.16.7 (2020-01-30):

- fix typchecking issue on TaggedScalar (reported by Jens Nielsen)
- fix error in dumping literal scalar in sequence with comments before element (reported by [EJ Etherington](https://sourceforge.net/u/ejether/))

0.16.6 (2020-01-20):

- fix empty string mapping key roundtripping with preservation of quotes as `? ''` (reported via email by Tomer Aharoni).
- fix incorrect state setting in class constructor (reported by [Douglas Raillard](https://bitbucket.org/%7Bcf052d92-a278-4339-9aa8-de41923bb556%7D/))
- adjust deprecation warning test for Hashable, as that no longer warns (reported by [Jason Montleon](https://bitbucket.org/%7B8f377d12-8d5b-4069-a662-00a2674fee4e%7D/))

0.16.5 (2019-08-18):

- allow for `YAML(typ=['unsafe', 'pytypes'])`

0.16.4 (2019-08-16):

- fix output of TAG directives with `#` (reported by [Thomas Smith](https://bitbucket.org/%7Bd4c57a72-f041-4843-8217-b4d48b6ece2f%7D/))

0.16.3 (2019-08-15):

- split construct_object
- change stuff back to keep mypy happy
- move setting of version based on YAML directive to scanner, allowing to check for file version during TAG directive scanning

0.16.2 (2019-08-15):

- preserve YAML and TAG directives on roundtrip, correctly output `#` in URL for YAML 1.2 (both reported by [Thomas Smith](https://bitbucket.org/%7Bd4c57a72-f041-4843-8217-b4d48b6ece2f%7D/))

0.16.1 (2019-08-08):

- Force the use of new version of ruamel.yaml.clib (reported by [Alex Joz](https://bitbucket.org/%7B9af55900-2534-4212-976c-61339b6ffe14%7D/))
- Allow `#` in tag URI as these are allowed in YAML 1.2 (reported by [Thomas Smith](https://bitbucket.org/%7Bd4c57a72-f041-4843-8217-b4d48b6ece2f%7D/))

0.16.0 (2019-07-25):

- split of C source that generates `.so` file to [ruamel.yaml.clib]( https://pypi.org/project/ruamel.yaml.clib/)
- duplicate keys are now an error when working with the old API as well

------------------------------------------------------------------------

For older changes see the file
[CHANGES](https://sourceforge.net/p/ruamel-yaml/code/ci/default/tree/CHANGES)
