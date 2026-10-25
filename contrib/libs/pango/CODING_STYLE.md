Formatting
==========

The Pango formatting style is basically the GNU style of formatting
(see http://www.gnu.org/prep/standards.html), with a few additions.
In brief overview:

 - Two character indents are used; braces go on a separate line, and 
   get a separate indentation level, so the total indent for an
   enclosed block is 4 characters.


    ```c
    if (x < foo (y, z))
      haha = bar[4] + 5;
    else
      {
        while (z)
          {
            haha += foo (z, z);
            z--;
          }
        return abc (haha);
      }
    ```

 - Spaces should be present between function name and argument block,
   and after commas.

         foo (z, z)

 - In pointer types, the '*' is grouped with the variable name,
   not with the base type. 

        int *a;

    Not:

        int* a;

   In cases where there is no variable name, for instance, return
   values, there should be a single space between the base type 
   and the '*'.

 - function and variable names are lower_case_with_underscores
   type names are StudlyCaps, macro names are UPPER_CASE_WITH_UNDERSCORES


Documentation comments
======================

All public API functions should have inline documentation headers
in the gtk-doc / gnome-doc style. For instance:

```c
/**
 * pango_layout_get_line:
 * @layout: a `PangoLayout`
 * @line: the index of a line, which must be between 0 and
 *   `pango_layout_get_line_count(layout) - 1`, inclusive.
 * 
 * Retrieves a particular line from a `PangoLayout` (or @layout.)
 * 
 * Return value: the requested `PangoLayoutLine`, or %NULL
 *   if the index is out of range. This layout line can
 *   be ref'ed and retained, but will become invalid
 *   if changes are made to the `PangoLayout`.
 *
 * Since: 1.6
 */
PangoLayoutLine *
pango_layout_get_line (PangoLayout *layout,
		       int          line)
[...]
```

Choosing Function Names
=======================

- Don't abbreviate in unexpected ways:

  ```c
  pango_layout_get_line_count ();
  ```

  Not:

  ```c
  pango_layout_ln_cnt ();
  ```

- function that retrieve a value in a side-effect free fashion, should
  include "get" in the name.

  ```c
  int pango_layout_get_line_count (PangoLayout *layout);
  ```

  Not: 

  ```c
  pango_layout_line_count ();
  ```


- functions that set a single parameter in a side-effect free fashion
  should include "set" in the name, for instance:

  ```c
  void pango_layout_set_width (PangoLayout    *layout,
                               int             width);
  ```

Other comments
==============

- Avoid unsigned values for all but flags fields. This is because
  the way C handles unsigned values generates bugs like crazy:

  If width is unsigned and 10, then:

  ```c
  int new_width = MAX (width - 15, 1);
  ```

  produces 4294967291, not 1.

