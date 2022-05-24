# Pcre

The Pcre library is currently an alias to [Hyperscan](hyperscan.md).

{% if tech or feature_mapreduce %}
If you depend on some features of a certain regular expression engine, it's better to internally implement UDF with a given library inside. Use Pcre as the current most-recommended option for simple matching (that can be potentially changed to something else).
{% endif %}

Currently available engines:

* [Hyperscan](hyperscan.md) <span style="color: gray;">(Intel)</span>
* [Pire](pire.md) <span style="color: gray;">(Yandex)</span>
* [Re2](re2.md) <span style="color: gray;">(Google)</span>

All three modules provide approximately the same set of functions with an identical interface. This lets you switch between them with minimal changes to a query.

Inside Hyperscan, there are several implementations that use different sets of processor instructions, with the relevant instruction automatically selected based on the current processor. In HyperScan, some functions support backtracking (referencing the previously found part of the string). Those functions are implemented through hybrid use of the two libraries: Hyperscan and libpcre.

[Pire](https://github.com/yandex/pire) (Perl Incompatible Regular Expressions) is a very fast library of regular expressions developed by Yandex. At the lower level, it scans the input string once, without any lookaheads or rollbacks, spending 5 machine instructions per character (on x86 and x86_64). However, since the library almost hasn't been developed since 2011-2013 and its name says "Perl incompatible", you may need to adapt your regular expressions a bit.

Hyperscan and Pire are best-suited for Grep and Match.

The Re2 module uses [google::RE2](https://github.com/google/re2) that offers a wide range of features ([see the official documentation](https://github.com/google/re2/wiki/Syntax)). The main benefit of the Re2 is its advanced Capture and Replace functionality. Use this library, if you need those functions.

