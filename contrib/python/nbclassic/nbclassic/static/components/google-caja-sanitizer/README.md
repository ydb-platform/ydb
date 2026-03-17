# google-caja-sanitizer

Port of the google caja html sanitizer library.

forked from [node-google-caja](https://github.com/superkhau/node-google-caja)

modified to allow data-* attributes while sanitizing the html input.

## Build Status
[![Build Status](https://travis-ci.org/iyogeshjoshi/google-caja-sanitizer.svg?branch=master)](https://travis-ci.org/iyogeshjoshi/google-caja-sanitizer)

## Install
You can install using the following command

```shell
#!shell
<root-directory>$ npm i --save google-caja-sanitizer

```

## Use

Require the library and go

```javascript
#!nodejs

var sanitize = require('google-caja-sanitizer').sanitize;
var result = sanitize('test<script>console.log("hi there");</script><div data-fruit="Apple">Apple</div>');

// Output:
// 'test<div data-fruit="Apple">Apple</div>'
```

# Documentation Page
Please check the [google page](https://code.google.com/p/google-caja/wiki/JsHtmlSanitizer) for more info on google caja sanitizer
