text-encoding
==============

This is a polyfill for the [Encoding Living Standard](http://encoding.spec.whatwg.org/)
API for the Web, allowing encoding and decoding of textual data to and from Typed Array
buffers for binary data in JavaScript.

By default it adheres to the spec and does not support *encoding* to non-UTF encodings,
only *decoding*. It is also implemented to match the specification's algorithms, rather
than for performance. The intended use is within Web pages, so it has no dependency
on server frameworks or particular module schemes.

Basic examples and unit tests are included.

### Install ###

There are a few ways you can get the `text-encoding` library.

#### Node ####

`text-encoding` is on `npm`. Simply run:

```js
npm install text-encoding
```

Or add it to your `package.json` dependencies.

#### Bower ####

`text-encoding` is on `bower` as well. Install with bower like so:

```js
bower install text-encoding
```

Or add it to your `bower.json` dependencies.

### HTML Page Usage ###

```html
  <!-- Required for non-UTF encodings -->
  <script src="encoding-indexes.js"></script>
  <script src="encoding.js"></script>
```

### API Overview ###

Basic Usage

```js
  var uint8array = TextEncoder(encoding).encode(string);
  var string = TextDecoder(encoding).decode(uint8array);
```

Streaming Decode

```js
  var string = "", decoder = TextDecoder(encoding), buffer;
  while (buffer = next_chunk()) { 
    string += decoder.decode(buffer, {stream:true});
  }
  string += decoder.decode(); // finish the stream
```

### Encodings ###

All encodings from the Encoding specification are supported:

utf-8 ibm866 iso-8859-2 iso-8859-3 iso-8859-4 iso-8859-5 iso-8859-6 iso-8859-7 iso-8859-8 iso-8859-8-i iso-8859-10 iso-8859-13 iso-8859-14 iso-8859-15 iso-8859-16 koi8-r koi8-u macintosh windows-874 windows-1250 windows-1251 windows-1252 windows-1253 windows-1254 windows-1255 windows-1256 windows-1257 windows-1258 x-mac-cyrillic gb18030 hz-gb-2312 big5 euc-jp iso-2022-jp shift_jis euc-kr replacement utf-16be utf-16le x-user-defined

(Some encodings may be supported under other names, e.g. ascii, iso-8859-1, etc.
See [Encoding](http://encoding.spec.whatwg.org/) for additional labels for each encoding.)

Encodings other than **utf-8**, **utf-16le** and **utf-16be** require an additional 
`encoding-indexes.js` file to be included. It is rather large 
(541kB uncompressed, 186kB gzipped); portions may be deleted if 
support for some encodings is not required.

As required by the specification, only encoding to **utf-8**,
**utf-16le** and **utf-16be** is supported. If you want to try it out, you can
force a non-standard behavior by passing the `NONSTANDARD_allowLegacyEncoding`
option to TextEncoder. For example:

```js
  var uint8array = TextEncoder('windows-1252', { NONSTANDARD_allowLegacyEncoding: true }).encode(text);
```
