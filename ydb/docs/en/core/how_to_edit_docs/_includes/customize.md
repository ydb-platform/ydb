# Creating customized documentation for {{ ydb-short-name }}

The contents of the `core` subdirectory are linked to various documentation build environments as a library for creating customized documentation.

The `overlay` subdirectory is designed for customizing content and is technical for an OpenSource build, since the OpenSource build does not contain core customizations.

### Proxy articles

A file with an article in `core` never contains content directly, it only contains one or more instructions of include blocks of this content stored in the `_includes` subdirectory. As a result, replacing this file in `overlay` does not lead to the need to copy the content, but lets you make the following types of its adaptation:

- Add additional content to the beginning or end of an article.
- Insert additional content between include directives.
- Remove some of the content of the original article from the build by removing the corresponding include directive.

Instructions for placing articles in core and overlay are given in the [Content creation guide - Articles](content.md#articles).

### TOC (Table of Contents)

The TOC in the {{ ydb-short-name }} documentation is built from a set of files placed in directories with articles that are hierarchically included in each other by the include directive. As a result, each of these files can be redefined separately in the adapted documentation.

As with articles, a proxy approach is used for TOC files as follows:

1. A file with menu items for core is named `toc_i.yaml`. It is never redefined in `overlay`.
2. Next to the `toc_i.yaml` file in core, there is a file named `toc_p.yaml`. It contains a link to `toc_i.yaml` and is designed to be redefined in `overlay`.
3. Other sections are included in the TOC via a link to the `toc_p.yaml` file.
4. If there is no need to adjust the contents of the TOC in a certain directory, no toc* files are created in `overlay`. This results in using toc_p --> toc_i from core in the build.
5. If the contents of the TOC need to be adjusted in a certain directory, a file named `toc_p.yaml` is created in `overlay` with the `- include: { mode: link, path: toc_i.yaml }` from core added to its content and additional items for the adapted build added above or below.

It is good practice not to include the "Overview" in toc_i.yaml but rather include it directly in toc_p.yaml instead. This article must be the first in each submenu and always has the same article name (index.md). Including a separate item in toc_p lets you add new articles to the adapted documentation before the articles from core, but keeping the "Overview" in the first place:

toc_p.yaml in some corporate overlay:

```yaml
items:
- name: Overview
  href: index.md
- name: Employee roles
  href: corp_roles.md
- include: { mode: link, path: toc_i.yaml }
- name: Corporate authorization
  href: corp_auth.md
```

As with articles, core may have several toc_i*.yaml files included in toc_p with separate includes.

There are also a couple of toc_i and toc_p files in the root directory, representing the top level of the table of contents.

Since the inclusion of files is implemented through references to them from the parent toc, a file name may actually be technically anything and the above is only a naming convention. However, there is one restriction:

{% note warning %}

A TOC file name may not be `toc.yaml`, since the build tool searches for files with this name across all subdirectories and tries to add them to the TOC on its own. This include option is not used in the YDB documentation, any inclusion is always explicit by the include directive from another TOC.

{% endnote %}

The only exception is the initial `toc.yaml` that is placed in the documentation build root and always contains the command to copy the core content and proceed to TOC handling in core: `include: { mode: merge, path: core/toc_m.yaml }`.

Instructions for making a TOC in core and overlay are given in the [Content creation guide - TOC](content.md#toc).

