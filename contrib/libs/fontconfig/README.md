# Fontconfig

Font configuration and customization library

[[_TOC_]]

## About Fontconfig

Fontconfig can:

* discover new fonts when installed automatically, removing a common source of configuration problems.
* perform font name substitution, so that appropriate alternative fonts can be selected if fonts are missing.
* identify the set of fonts required to completely cover a set of languages.
* have GUI configuration tools built as it uses an XML-based configuration file (though with autodiscovery, we believe this need is minimized).
* efficiently and quickly find the fonts you need among the set of fonts you have installed, even if you have installed thousands of fonts, while minimizing memory usage.
* be used in concert with the X Render Extension and [FreeType](https://www.freedesktop.org/wiki/Software/FreeType/) to implement high quality, anti-aliased and subpixel rendered text on a display.

Fontconfig does not:

* render the fonts themselves (this is left to FreeType or other rendering mechanisms)
* depend on the X Window System in any fashion, so that printer only applications do not have such dependencies

## Documentation

* [Fontconfig User Documentation](https://fontconfig.pages.freedesktop.org/fontconfig/fontconfig-user.html)
* [Fontconfig Developer Documentation](https://fontconfig.pages.freedesktop.org/fontconfig/fontconfig-devel/)

## Bug reports

If you have encountered any issues relating to Fontconfig, please file an issue in the [GitLab issue tracker](https://gitlab.freedesktop.org/fontconfig/fontconfig/issues).
