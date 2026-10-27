This file documents various major decisions which affect GLib development,
giving a brief rationale of each decision, plus a link to further discussion.


 * Compiler attributes: https://bugzilla.gnome.org/show_bug.cgi?id=113075#c46

   GLib uses GIR annotations instead of compiler attributes. They are tidier,
   already supported by GLib and GNOME tools, and accomplish the same task as
   compiler attributes. GLib does not provide macros for attributes like
   nonnull because it would not use them.

 * Main loop API:

   The ID-based mainloop APIs (g_idle_add, g_timeout_add, etc) are considered
   legacy, and new features (such as g_source_set_static_name) will only be
   added to the explicit GSource APIs.
