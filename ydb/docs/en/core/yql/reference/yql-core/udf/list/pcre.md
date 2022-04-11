# Pcre

The Pcre library is currently an alias to [Hyperscan](hyperscan.md).

{% if tech or feature_mapreduce %}
If you depend on some features of a certain regular expression engine, it's better to internally implement UDF with a given library inside. Use Pcre as the current most-recommended option for simple matching (that can be potentially changed to something else).
{% endif %}

Currently available engines:

* [Hyperscan](hyperscan.md) <span style="color: gray;">(Intel)</span>
* [Re2](re2.md) <span style="color: gray;">(Google)</span>
* [Pire](pire.md) <span style="color: gray;">(Yandex)</span>

Hyperscan and Pire are best-suited for Grep and Match. Inside Hyperscan, there are several implementations that use different sets of processor instructions, with the relevant instruction automatically selected based on the current processor. Pire is also known for its excellent performance. So, if you need high performance, test-run this library against your data and regular expressions. However, since the library almost hasn't been developed since 2011-2013 and its name says "Perl incompatible", you may need to adapt your regular expressions a bit.

The main benefit of the Re2 is its advanced Capture and Replace functionality. Use this library, if you need those functions.

In HyperScan, some functions support backtracking (referencing the previously found part of the string). Those functions are implemented through hybrid use of the two libraries: Hyperscan and libpcre.

