# dtl

`dtl` is the diff template library written in C++. The name of template is derived C++'s Template.

# Table of contents

 * [Features](#features)
 * [Getting started](#getting-started)
     * [Compare two strings](#compare-two-strings)
     * [Compare two data has arbitrary type](#compare-two-data-has-arbitrary-type)
     * [Merge three sequences](#merge-three-sequences)
     * [Patch function](#patch-function)
     * [Difference as Unified Format](#difference-as-unified-format)
     * [Compare large sequences](#compare-large-sequences)
     * [Unserious difference](#unserious-difference)
     * [Calculate only Edit Distance](#calculate-only-edit-distance)
 * [Algorithm](#algorithm)
     * [Computational complexity](#computational-complexity)
     * [Comparison when difference between two sequences is very large](#comparison-when-difference-between-two-sequences-is-very-large)
     * [Implementations with various programming languages](#implementations-with-various-programming-languages)
 * [Examples](#examples)
     * [strdiff](#strdiff)
     * [intdiff](#intdiff)
     * [unidiff](#unidiff)
     * [unistrdiff](#unistrdiff)
     * [strdiff3](#strdiff3)
     * [intdiff3](#intdiff3)
     * [patch](#patch)
     * [fpatch](#fpatch)
 * [Running tests](#running-tests)
     * [Building test programs](#building-test-programs)
     * [Running test programs](#running-test-programs)
 * [Old commit histories](#old-commit-histories)
 * [License](#license)

# Features

`dtl` provides the functions for comparing two sequences have arbitrary type. But sequences must support random access\_iterator.

# Getting started

To start using this library, all you need to do is include `dtl.hpp`.

```c++
#include "dtl/dtl.hpp"
```

## Compare two strings

First of all, calculate the difference between two strings.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("abc");
sequence B("abd");
dtl::Diff< elem, sequence > d(A, B);
d.compose();
```

When the above code is run, `dtl` calculates the difference between A and B as Edit Distance and LCS and SES.

The meaning of these three terms is below.

| Edit Distance | Edit Distance is numerical value for declaring a difference between two sequences. |
|:--------------|:-----------------------------------------------------------------------------------|
| LCS           | LCS stands for Longest Common Subsequence.                                         |
| SES           | SES stands for Shortest Edit Script. I mean SES is the shortest course of action for tranlating one sequence into another sequence.|

If one sequence is "abc" and another sequence is "abd", Edit Distance and LCS and SES is below.

| Edit Distance | 2               |
|:--------------|:----------------|
| LCS           | ab              |
| SES           | C a C b D c A d |

 * 「C」：Common
 * 「D」：Delete
 * 「A」：ADD

If you want to know in more detail, please see [examples/strdiff.cpp](https://github.com/cubicdaiya/dtl/blob/master/examples/strdiff.cpp).

This calculates Edit Distance and LCS and SES of two strings received as command line arguments and prints each.

When one string is "abc" and another string "abd", the output of `strdiff` is below.

```bash
$ ./strdiff abc abd
editDistance:2
LCS:ab
SES
 a
 b
-c
+d
$
```

## Compare two data has arbitrary type

`dtl` can compare data has aribtrary type because of the C++'s template.

But the compared data type must support the random access\_iterator.

In the previous example, the string data compared,

`dtl` can also compare two int vectors like the example below.

```c++
int a[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
int b[10] = {3, 5, 1, 4, 5, 1, 7, 9, 6, 10};
std::vector<int> A(&a[0], &a[10]);
std::vector<int> B(&b[0], &b[10]);
dtl::Diff< int > d(A, B);
d.compose();
```

If you want to know in more detail, please see [examples/intdiff.cpp](https://github.com/cubicdaiya/dtl/blob/master/examples/intdiff.cpp).

## Merge three sequences

`dtl` has the diff3 function.

This function is that `dtl` merges three sequences.

Additionally `dtl` detects the confliction.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("qqqabc");
sequence B("abc");
sequence C("abcdef");
dtl::Diff3<elem, sequence> diff3(A, B, C);
diff3.compose();
if (!diff3.merge()) {
  std::cerr << "conflict." << std::endl;
  return -1;
}
std::cout << "result:" << diff3.getMergedSequence() << std::endl;
```

When the above code is run, the output is below.

```console
result:qqqabcdef
```

If you want to know in more detail, please see [examples/strdiff3.cpp](https://github.com/cubicdaiya/dtl/blob/master/examples/strdiff3.cpp).

## Patch function

`dtl` can also translates one sequence to another sequence with SES.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("abc");
sequence B("abd");
dtl::Diff<elem, sequence> d(A, B);
d.compose();
string s1(A);
string s2 = d.patch(s1);
```

When the above code is run, s2 becomes "abd".
The SES of A("abc") and B("abd") is below.

```console
Common a
Common b
Delete c
Add    d
```

The patch function translates a sequence as argument with SES.
For this example, "abc" is translated to "abd" with above SES.

Please see dtl's header files about the data structure of SES.

## Difference as Unified Format

`dtl` can also treat difference as Unified Format. See the example below.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("acbdeaqqqqqqqcbed");
sequence B("acebdabbqqqqqqqabed");
dtl::Diff<elem, sequence > d(A, B);
d.compose();             // construct an edit distance and LCS and SES
d.composeUnifiedHunks(); // construct a difference as Unified Format with SES.
d.printUnifiedFormat();  // print a difference as Unified Format.
```

The difference as Unified Format of "acbdeaqqqqqqqcbed" and "acebdabbqqqqqqqabed" is below.

```diff
@@ -1,9 +1,11 @@
 a
 c
+e
 b
 d
-e
 a
+b
+b
 q
 q
 q
@@ -11,7 +13,7 @@
 q
 q
 q
-c
+a
 b
 e
 d
```

The data structure Unified Format is below.

```c++
/**
 * Structure of Unified Format Hunk
 */
template <typename sesElem>
struct uniHunk {
  int a, b, c, d;                   // @@ -a,b +c,d @@
  std::vector<sesElem> common[2];   // anteroposterior commons on changes
  std::vector<sesElem> change;      // changes
  int inc_dec_count;                // count of increace and decrease
};
```

The actual blocks of Unified Format is this structure's vector.

If you want to know in more detail, please see [examples/unistrdiff.cpp](https://github.com/cubicdaiya/dtl/blob/master/examples/unistrdiff.cpp)
and [examples/unidiff.cpp](https://github.com/cubicdaiya/dtl/blob/master/examples/unidiff.cpp) and dtl's header files.

In addtion, `dtl` has the function translates one sequence to another sequence with Unified Format.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("abc");
sequence B("abd");
dtl::Diff<elem, sequence> d(A, B);
d.compose();
d.composeUnifiedHunks()
string s1(A);
string s2 = d.uniPatch(s1);
```

When the above code is run, s2 becomes "abd".
The uniPatch function translates a sequence as argument with Unified Format blocks.

For this example, "abc" is translated to "abd" with the Unified Format block below.

```diff
@@ -1,3 +1,3 @@
 a
 b
-c
+d
```

## Compare large sequences

When compare two large sequences, `dtl` can optimizes the calculation of difference with the onHuge function.

This function is available when the compared data type is std::vector.

When you use this function, you may call this function before calling compose function.

```c++
typedef char elem;
typedef  std::vector<elem> sequence;
sequence A;
sequence B;
/* ・・・ */
dtl::Diff< elem, sequence > d(A, B);
d.onHuge();
d.compose();
```

## Unserious difference

The calculation of difference is very heavy.
`dtl` uses An O(NP) Sequence Comparison Algorithm.

Though this Algorithm is sufficiently fast,
when difference between two sequences is very large,

the calculation of LCS and SES needs massive amounts of memory.

`dtl` avoids above-described problem by dividing each sequence into plural subsequences
and joining the difference of each subsequence finally.

As this way repeats allocating massive amounts of memory,
`dtl` provides other way. It is the way of calculating unserious difference.

For example, The normal SES of "abc" and "abd" is below.

```console
Common a
Common b
Delete c
Add    d
```

The unserious SES of "abc" and "abd" is below.

```console
Delete a
Delete b
Delete c
Add    a
Add    b
Add    d
```

Of course, when "abc" and "abd" are compared with `dtl`, above difference is not derived.

`dtl` calculates the unserious difference when `dtl` judges the calculation of LCS and SES
needs massive amounts of memory and unserious difference function is ON.

`dtl` joins the calculated difference before `dtl` judges it and unserious difference finally.

As a result, all difference is not unserious difference when unserious difference function is ON.

When you use this function, you may call this function before calling compose function.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("abc");
sequence B("abd");
dtl::Diff< elem, sequence > d(A, B);
d.onUnserious();
d.compose();
```

## Calculate only Edit Distance

As using onOnlyEditDistance, `dtl` calculates the only edit distance.

If you need only edit distance, you may use this function,
because the calculation of edit distance is lighter than the calculation of LCS and SES.

When you use this function, you may call this function before calling compose function.

```c++
typedef char elem;
typedef std::string sequence;
sequence A("abc");
sequence B("abd");
dtl::Diff< elem, sequence > d(A, B);
d.onOnlyEditDistance();
d.compose();
```

# Algorithm

The algorithm `dtl` uses is based on "An O(NP) Sequence Comparison Algorithm" by described by Sun Wu, Udi Manber and Gene Myers.

An O(NP) Sequence Comparison Algorithm(following, Wu's O(NP) Algorithm) is the efficient algorithm for comparing two sequences.

## Computational complexity

The computational complexity of Wu's O(NP) Algorithm is averagely O(N+PD), in the worst case, is O(NP).

## Comparison when difference between two sequences is very large

Calculating LCS and SES efficiently at any time is a little difficult.

Because that the calculation of LCS and SES needs massive amounts of memory when a difference between two sequences is very large.

The program uses that algorithm don't consider that will burst in the worst case.

`dtl` avoids above-described problem by dividing each sequence into plural subsequences and joining the difference of each subsequence finally. (This feature is supported after version 0.04)

## Implementations with various programming languages

There are the Wu's O(NP) Algorithm implementations with various programming languages below.

https://github.com/cubicdaiya/onp

# Examples

There are examples in [dtl/examples](https://github.com/cubicdaiya/dtl/tree/master/examples).
`dtl` uses [SCons](http://scons.org/) for building examples and tests. If you build and run examples and tests, install SCons.

## strdiff

`strdiff` calculates a difference between two string sequences, but multi byte is not supported.

```bash
$ cd dtl/examples
$ scons strdiff
$ ./strdiff acbdeacbed acebdabbabed
editDistance:6
LCS:acbdabed
SES
  a
  c
+ e
  b
  d
- e
  a
- c
  b
+ b
+ a
+ b
  e
  d
$
```

## intdiff

`intdiff` calculates a diffrence between two int arrays sequences.

```bash
$ cd dtl/examples
$ scons intdiff
$ ./intdiff # There are data in intdiff.cpp
1 2 3 4 5 6 7 8 9 10 
3 5 1 4 5 1 7 9 6 10 
editDistance:8
LCS: 3 4 5 7 9 10 
SES
- 1
- 2
  3
+ 5
+ 1
  4
  5
- 6
+ 1
  7
- 8
  9
+ 6
  10
$
```

## unidiff

`unidiff` calculates a diffrence between two text file sequences,
and output the difference between files with unified format.

```bash
$ cd dtl/examples
$ scons unidiff
$ cat a.txt
a
e
c
z
z
d
e
f
a
b
c
d
e
f
g
h
i
$ cat b.txt
a
d
e
c
f
e
a
b
c
d
e
f
g
h
i
$ ./unidiff a.txt b.txt
--- a.txt       2008-08-26 07:03:28 +0900
+++ b.txt       2008-08-26 03:02:42 +0900
@@ -1,11 +1,9 @@
 a
-e
-c
-z
-z
 d
 e
+c
 f
+e
 a
 b
 c
$
```

## unistrdiff

`unistrdiff` calculates a diffrence between two string sequences.
and output the difference between strings with unified format.

```bash
$ cd dtl/examples
$ scons unistrdiff
$ ./unistrdiff acbdeacbed acebdabbabed
editDistance:6
LCS:acbdabed
@@ -1,10 +1,12 @@
 a
 c
+e
 b
 d
-e
 a
-c
 b
+b
+a
+b
 e
 d
$
```

## strdiff3

`strdiff3` merges three string sequence and output the merged sequence.
When the confliction has occured, output the string "conflict.".

```bash
$ cd dtl/examples
$ scons strdiff3
$ ./strdiff3 qabc abc abcdef
result:qabcdef
$
```

There is a output below when conflict occured.

```bash
$ ./strdiff3 adc abc aec
conflict.
$
```

## intdiff3

`intdiff3` merges three integer sequence(vector) and output the merged sequence.

```bash
$ cd dtl/examples
$ scons intdiff3
$ ./intdiff3
a:1 2 3 4 5 6 7 3 9 10
b:1 2 3 4 5 6 7 8 9 10
c:1 2 3 9 5 6 7 8 9 10
s:1 2 3 9 5 6 7 3 9 10
intdiff3 OK
$
```

## patch

`patch` is the test program. Supposing that there are two strings is called by A and B,
`patch` translates A to B with Shortest Edit Script or unified format difference.

```bash
$ cd dtl/examples
$ scons patch
$ ./patch abc abd
before:abc
after :abd
patch successed
before:abc
after :abd
unipatch successed
$
```

## fpatch

`fpatch` is the test program. Supposing that there are two files is called by A and B,
`fpatch` translates A to B with Shortest Edit Script or unified format difference.

```bash
$ cd dtl/examples
$ scons fpatch
$ cat a.txt
a
e
c
z
z
d
e
f
a
b
c
d
e
f
g
h
i
$ cat b.txt
$ cat b.txt
a
d
e
c
f
e
a
b
c
d
e
f
g
h
i
$ ./fpatch a.txt b.txt
fpatch successed
unipatch successed
$
```

# Running tests

`dtl` uses [googletest](https://github.com/google/googletest) and [SCons](http://www.scons.org/) with testing dtl-self.

# Building test programs

If you build test programs for `dtl`, run `scons` in test direcotry.

```bash
$ scons
```

# Running test programs

If you run all tests for `dtl`, run 'scons check' in test direcotry. (it is necessary that gtest is compiled)

```bash
$ scons check
```

If you run sectional tests, you may exeucte `dtl_test` directly after you run `scons`.
Following command is the example for testing only Strdifftest.

```bash
$ ./dtl_test --gtest_filter='Strdifftest.*'
```

`--gtest-filters` is the function of googletest. googletest has many useful functions for testing software flexibly.
If you want to know other functions of googletest, run `./dtl_test --help`.

# Old commit histories

Please see [cubicdaiya/dtl-legacy](https://github.com/cubicdaiya/dtl-legacy).

# License

Please read the file [COPYING](https://github.com/cubicdaiya/dtl/blob/master/COPYING).
