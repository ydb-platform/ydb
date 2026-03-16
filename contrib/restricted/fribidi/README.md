# GNU FriBidi

The Free Implementation of the [Unicode Bidirectional Algorithm].

## Background

One of the missing links stopping the penetration of free software in Middle
East is the lack of support for the Arabic and Hebrew alphabets. In order to
have proper Arabic and Hebrew support, the bidi algorithm needs to be implemented. It is our hope that this library will stimulate more free software in the Middle Eastern countries.

See [`HISTORY`](./HISTORY) on how the project started and evolved.


## Audience

It is our hope that this library will stimulate the implementation of Hebrew and
Arabic support in lots of Free Software. 

GNU FriBidi is already being used in projects like Pango (resulting in [GTK+] and [GNOME] using GNU FriBidi), AbiWord, MLTerm, MPlayer, BiCon, and vlc.

See [`USERS`](./USERS) for a list of projects using GNU FriBidi.


## Dependencies

GNU FriBidi does not depend on any other library. It uses either the GNU Build System or meson for build and installation.


## Downloading

The latest version of GNU FriBidi may be found at:
<https://github.com/fribidi/fribidi>


## Building

Start with running the [`autogen.sh`](./autogen.sh) script and follow the
instructions. Alternatively use `meson`.


## License

GNU FriBidi is Free Software; you can redistribute it and/or modify it under the
terms of the [GNU Lesser General Public License] as published by the Free Software

Foundation; either version 2.1 of the License, or (at your option) any later
version.

GNU FriBidi is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License along
with GNU FriBidi, in a file named COPYING; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

### Commercial licensing

For commercial licensing options, contact <fribidi.license@gmail.com>.

## Implementation

The library implements the algorithm described in the "Unicode Standard Annex
\#9, The Bidirectional Algorithm", available at
<http://www.unicode.org/unicode/reports/tr9/>.

The library uses Unicode (UTF-32) entirely. The character properties are
automatically extracted from the Unicode data files, available from
<http://www.unicode.org/Public/UNIDATA/>. This means that every Unicode
character is treated in strict accordance with the Unicode specification.

There is a limited support for character set conversion from/to the UTF-32
encoding. Data in these character sets must be converted into UTF-32 before the
library may be used. iconv(3) can always do a better job on that, so you may
find that the character sets conversion code is typically turned off on POSIX
machines.


### Conformance Status

GNU FriBidi has been tested exhaustively against the [Unicode Reference Code],
and to the best of our knowledge, it completely conforms to the specification,
always producing the same result as the Reference Code.


### API

The simplest way of accessing the API is through the convenience function `fribidi_log2vis` which has the following signature:

```c
fribidi_boolean fribidi_log2vis(
    /* input */
    FriBidiChar     *str,
    FriBidiStrIndex len,
    FriBidiCharType *pbase_dir,
    /* output */
    FriBidiChar     *visual_str,
    FriBidiStrIndex *position_L_to_V_list,
    FriBidiStrIndex *position_V_to_L_list,
    FriBidiLevel    *embedding_level_list
)
```

Where...

* `str` is the Unicode input string.
* `len` is the length of the Unicode string (`str`).
* `pbase_dir` is the input and output base direction. If `pbase_dir ==
  FRIBIDI_TYPE_ON` then `fribidi_log2vis()` calculates the base direction on its
  own, according to the bidi algorithm.
* `visual_str` is the reordered output unicode string.
* `position_L_to_V_list` maps the positions in the logical string to positions
  in the visual string.
* `position_V_to_L_list` maps the positions in the visual string to the
  positions in the logical string.
* `embedding_level_list` returns the classification of each character. Here,
  even numerical levels indicate LTR characters, and odd levels indicate RTL
  characters. The main use of this list is in interactive applications where,
  for example, the embedding level determines cursor display.

If any of the output pointers is equal to `NULL`, then that information is not
calculated.

Note that a call to `fribidi_log2vis()` is a convenience function to calling the following three functions in order:

1. `fribidi_get_bidi_types()`
2. `fribidi_get_par_embedding_levels_ex()`
3. `fribidi_reorder_line()`

## How it looks like

Have a look at [`test/`](./test) directory, to see some input and outputs.

The `CapRTL` charset means that CAPITAL letters are right to left, and digits
6, 7, 8, 9 are Arabic digits, try 'fribidi --charsetdesc CapRTL' for the full
description.


## Executable

There is also a command-line utilitity called `fribidi` that loops over the text
of a file and performs the bidi algorithm on each line, also used for testing
the algorithm.

Run `fribidi --help` to learn about usage.

The command-line utility is known to have problems with line-breaking and
logical-to-vertical/vertical-to-logical lists.


## Bug Reports and Feedback

Report bugs and general feedback at: <https://github.com/fribidi/fribidi/issues>

The mailing list is the place for additional technical discussions and user
questions: <https://lists.freedesktop.org/mailman/listinfo/fribidi>


## Maintainers and Contributors

* Dov Grobgeld <dov.grobgeld@gmail.com> - Original author and current maintainer
* Behdad Esfahbod <behdad@gnu.org> - Author of most of the code

See also [`AUTHORS`](./AUTHORS) and [`THANKS`](./THANKS) for the complete list
of contributors.


[Unicode Bidirectional Algorithm]: https://www.unicode.org/reports/tr9/
[Unicode Reference Code]: https://www.unicode.org/reports/tr9/#Reference_Code
[Mirroring]: https://www.unicode.org/reports/tr9/#Mirroring
[GTK+]: https://www.gtk.org/
[GNOME]: https://www.gnome.org/
[GNU Lesser General Public License]: https://www.gnu.org/licenses/old-licenses/lgpl-2.1.en.html
