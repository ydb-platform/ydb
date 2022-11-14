# Content creation guide

## Introduction {#intro}

Documentation source code is created as [Markdown]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Markdown){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/Markdown){% endif %} with the [YFM (Yandex Flavoured Markdown)]{% if lang == "en" %}(https://ydocs.tech/en/){% endif %}{% if lang == "ru" %}(https://ydocs.tech/ru/){% endif %} extensions. An effective way to quickly learn these formats is to study the source code of the available {{ ydb-short-name }} documentation.

In addition to the formal rules described here, always keep in mind the following: if a rule is not formally described for some topic, you should look at how this has been done elsewhere in the documentation and do it by analogy.

Also remember that there are exceptions to any rules and it is impossible to describe them all.

## Documentation core and customization

The OpenSource {{ ydb-short-name }} documentation lets you create customized derivative documentation sets for enterprise environments using {{ ydb-short-name }} or for cloud providers that provide access to the {{ ydb-short-name }} service through their ecosystems.

When adding content, you should first choose where to add it: to the OpenSource core or to some customization. The general recommendation is to add content to the OpenSource block in such a way that it can be read in different contexts without the need for its specific adaptation. Only when this can't be done or the placement of some content in the OpenSource core is explicitly prohibited (for example, links to intranet resources in an enterprise context), you can proceed to adding content to the source code of the documentation customization.

{% include [custom_extension.md](content/custom_extension.md) %}

In the sections where the requirements differ for basic content and customization, there are two corresponding tabs:

- **Core**: The core of any documentation, the basic content.
- **Overlay**: Content that is overlaid on top of the core and adapting it to a custom build.

All the content of the OpenSource build is part of the core, and zero customization is applied when building it, so when you change it, you'll only need the "Core" bookmark.

## Directories {#placement}

The basic structure of the source code directories is based on the subject of the content and not on the type of files or their technical role during the build. Technical files and directories are placed inside the subject ones.

### "Overview" article {#index}

Each subject directory must contain the "Overview" article with the `index.md` filename. The "Overview" article:

- Describes what the articles in this directory tell about.
- Optionally, provides a list of links to all or some of the most important articles.
- Optionally, provides a set of "See also" links to other related articles and sections.

The presence of the "Overview" article lets you refer to the entire directory rather than a specific article and convert articles into directories without losing the referential integrity.

## Articles {#articles}

{% list tabs %}

- Core

  Every article in the basic OpenSource documentation is created with the understanding that it can be extended or adjusted in any customized documentation derived from it. At the same time, improvements to the base documentation should be applied in a customized version with no manual merge of content changes, otherwise maintaining a lot of derived documentation versions becomes a very labor-intensive task and monitoring the completeness of manual adaptations becomes impossible.

  To achieve this goal using the existing tools, a file with an article inside a subject directory does not directly contain any content and is designed to be redefined when customizing the documentation. The content is located in one or more files with content blocks included in the article file through include directives.

  The content blocks inserted into the article are located in a technical directory named `_includes` and nested directly in the subject directory.

  **A single-block article**

  In the simplest case, an article consists of a single block inserted by the `{% include ...%}` directive:

  `article1.md`:

  ```md
  
  {% include [article1.md](_includes/article1.md) %}
  ```

  In the `_includes` directory, a content file with the same name as the article file is created:

  `_includes/article1.md`:

  ```md
  # Article
  Article text.
  ```

  **A multi-block article**

  A single-block article lets you add arbitrary content before or after the content from the basic OpenSource documentation when customizing the documentation. This may not be enough, in which case the content can be divided into more blocks, letting you insert arbitrary content between them and exclude non-applicable blocks when customizing the documentation.

  In this case, a directory with the same name is created for the article inside `_includes`. For example, if you're inside the subject directory `subject1` and want to create an article named `article1` with two blocks, you should create three files with the following contents:

  `subject1/article1.md`:

  ```markdown
  
  {% include [definition.md](_includes/article1/definition.md) %}
  
  {% include [examples.md](_includes/article1/examples.md) %}
  ```

  `subject1/_includes/article1/definition.md`:

  ```md
  # Documentation article
  ## Definition {#definition}
  A documentation article is a set of content that reveals a specific topic that is interesting to the target audience.
  ```

  `subject1/_includes/article1/examples.md`:

  ```md
  ## Examples {#examples}
  - Cats aren't like people. Cats are cats.
  - A bush is a collection of branches sticking out of one place.
  ```

- Overlay

  When customizing the documentation, you can add articles and subject directories and redefine the content of articles from `core`, placing the article file at the same relative path inside `overlay` as it is placed inside `core`.

  When redefining the content, you can:

  - Add additional content to the beginning or end of an article.

    `subject1/article1.md`:

    ```md
    
    {% include [article1](_includes/article1.md) %}
    
    In addition to the basic authorization methods, our company uses nanotube-based authorization.
    ```

  - Insert additional content between include directives.

    `subject1/article1.md`:

    ```markdown
    
    {% include [definition.md](_includes/article1/definition.md) %}
    
    In our company, the amount of DB data is limited to 150ZB.
    
    {% include [examples.md](_includes/article1/examples.md) %}
    
    Example 2:
    The quick brown fox jumps over the lazy dog.
    ```

  - Remove some of the content of the original article from the build by removing the corresponding include directive.

    `subject1/article1.md`:

    ```markdown
    
    {% include [definition.md](_includes/article1/definition.md) %}
    
    In our company, the amount of DB data is limited to 150ZB.
    
    Example:
    The quick brown fox jumps over the lazy dog.
    ```

  If there are multiple blocks and a significant amount of content to add, it may be useful to design it in the form of include blocks as well, in the _includes subdirectory created inside the overlay subject directory. In this case, the article file will retain a clear and visible structure.

  Technically, a redefined article may contain its entire content, without including anything from the basic documentation at all. However, most likely this means that it is necessary to refactor the content in the basic documentation so that it can be effectively redefined rather than replaced.

{% endlist %}

### Specifics of using the include directive {#include-hints}

- The `_includes` contents are not published, so it is useless and forbidden to refer to them, it can only be included in articles by the `{% include ... %}` directive. Unfortunately, when building documentation locally, the contents of _includes remain available and operational, and the error only occurs when deploying the documentation on the farm.
- Make sure to leave empty lines around the `{% include ... %}` directive, otherwise, it won't be executed during the build.
- Write a file name in square brackets so that there is something to click on when viewing it in the default viewer on github (which does not understand what include is), and so that one include in it visually differs from the other.
- After `{%` at the beginning and before `%}`, a space is required at the end, otherwise the include won't be executed during the build.

### Other use cases for include {#include-reuse}

The `{% include ... %}` directive can be used to support the creation of derivative documentation versions. It can also reuse content in different articles, but we do not recommend this: seeing the same content in different articles may confuse the user and editing included content without knowing its context can easily distort the meaning.

A good way to leverage reusable content is to provide the same information in different grouping options within the same context. For example, the FAQ section contains articles on certain topics and the "All questions on all topics" article. Similarly, information about prices can be given in specialized articles on some chargeable functions and in the general article with the price list.


## Table of contents {#toc}

Each article must be mentioned in the Table Of Contents (TOC) file, otherwise it won't be published. The table of contents is the main tool for starting navigation through the documentation and is placed on the left of the documentation website pages.

{% list tabs %}

- Core

  There are two TOC files inside each subject directory:

  1. `toc_i.yaml`: Contains a list of articles to be included in the table of contents and located directly in this directory, with the exception of a special "Overview" article that is mandatory and must be present in any subsection of the TOC:

      ```yaml
      items:
      - name: Article 1
        href: article1.md
      - name: Article 2
        href: article2.md
      ...
      ```

  2. `toc_p.yaml`: Contains fixed content including the "Overview" article and a link to `toc_i.yaml`:

      ```yaml
      items:
      - name: Overview
        href: index.md
      - include: { mode: link, path: toc_i.yaml }
      ```

  If a directory has child directories, then the `toc_i.yaml` of the parent directory explicitly specifies relative references to their `toc_p.yaml`:

  ```yaml
  ...
  - name: Subtopic 1
    include: { mode: link, path: subject1/toc_p.yaml }
  ...
  ```

  In the `core` directory proper, there is a file with the root table of contents named `toc_p.yaml`, including `toc_p.yaml` tables of contents from all subject directories.

- Overlay

  If the customized documentation does not require changing the contents of the TOC in some subject directory as compared to the basic documentation, no toc files are created in `overlay`. The toc file from the basic documentation will be included in the build.

  If you need to adjust the list of articles, the `toc_p.yaml` file is included in overlay, which is originally copied from `core`, and you can add articles or subsections to the beginning or end of the list of articles from the basic documentation there:

  ```yaml
  items:
  # Adding the "Overview" article at the beginning of the list
  - name: Overview
    href: index.md
  # Adding additional items before the items from the basic documentation
  - name: Customization article 1 
    href: cust_article1.md
  # Including TOC items from the basic documentation
  - include: { mode: link, path: toc_i.yaml }
  # Adding additional items after the items from the basic documentation
  - name: Additional subsection
    include: { mode: link, path: cust_subdir/toc_p.yaml }
  - name: Customization article 2
    href: cust_article2.md
  ```

{% endlist %}

## Article section headings and anchors {#headers-and-anchors}

Each article should contain a first-level heading at the beginning that is marked with a single `#`. This heading is contained in the first content block.

Next level headings can be placed in different content blocks technically in an arbitrary way. However, it makes sense to place them at the beginning of the blocks. Keep in mind that building an article results in a single HTML page with headings.

Each heading of the second and further level is provided with an anchor that can be used in links like `host:path/article#anchor`. When you click on this link, the browser scrolls the contents of the page to the desired location. An anchor is set in a heading as follows:

```md
## Heading {#anchor}
```

It is allowed to specify several anchors, but you should have a good reason for that, such as maintaining the compatibility with previous anchors when refactoring the content, for example, when merging sections:

```md
## Heading of a merged section on topic X and Y {#X} {#Y} {#XY}
```

Specifying anchors is an extension of the YFM syntax in relation to Markdown, so publicly available Markdown display tools (like preview in an IDE) do not understand them.

## Links {#links}

Links in the text of an article to other articles are the main way to guide the user to other topics that are associated with the content they're reading just like traffic signs. Ideally, a documentation developer should understand how the user will get to the desired information. If some article is not linked to other topics in any way and is only posted somewhere in the table of contents in the "Other" section, the user may never reach it, and we'll keep responding to support requests saying "it's all described here, why don't you read the documentation?".

No one ever reads documentation as a work of art from beginning to end, users always read it to fix an issue, they have to find information about how to fix it, and links between articles are the main navigation tool once the user has started reading an article by selecting it in the TOC.

There are two types of links:

1. To the source code of the documentation. These are always relative links to some .md file. The more transitions "to a level up" or entries to nested directories this link has, the more likely it is that the documentation is poorly structured. Ideally, links should not navigate more than one level up or one directory down the hierarchy:

   ```md
   To keep a kitten active, you [need to feed it] (../feeding/index.md).
   ```
2. To resources external to the documentation. These are fully-qualified URLs in which no `index` is ever added:

   ```md
   You can find a good choice of cat food at the [Pushok store](http://www.pushok.ru/catalog).
   ```

Text inside the square brackets displayed when rendering documentation should be long enough to be easily clicked on with a mouse or tapped with a finger.

There are situations when the URL of a resource has its own value and should be displayed in the documentation, for example, when publishing links to a repository on Github. In this case, it must be duplicated inside both square and regular brackets, since YFM, unlike standard Markdown, does not automatically recognize URLs in text:

```md
{{ ydb-short-name }} repository on Github: [{{ ydb-doc-repo }}]({{ ydb-doc-repo }})
```

## Images {#pictures}

Images are part of an article's content and placed in the `_assets` technical subdirectory of the subject directory where the article is posted. Images are inserted in the article text using a link preceded by an exclamation mark `!`:

```md
![Text](../_assets/file.png)
```

The text specified in square brackets is displayed in the browser until the image itself is loaded. It can repeat the file name from the link with no extension.

Desirable image formats:

- Diagrams: [.SVG]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Scalable_Vector_Graphics){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/SVG){% endif %}
- Screenshots: [.PNG]{% if lang == "en" %}(https://en.wikipedia.org/wiki/Portable_Network_Graphics){% endif %}{% if lang == "ru" %}(https://ru.wikipedia.org/wiki/PNG){% endif %}

Since an image is part of an article, it is impossible to post the image without this article. If there is no text of the article yet, determine the subject directory for its placement and the file name of the future article and only specify a link to the image in the text (keep in mind that the [text of the article is placed in the `_includes` subdirectory](#articles)!), and do not include the article itself in the TOC until it is ready for publication.

When inserting images, you can also specify:

- A hint to be shown when hovering over an image: `![Text](path/file "Hint")`. We don't recommend using it, since lots of modern devices have no mouse pointer.
- Image size: `![Text](path/file =XSIZExYSIZE)`. We recommend that you use it when specifying the XSIZE for SVG images so that they are expanded properly by the width of the documentation screen, regardless of the resolution they were saved with: `![Diagram1](../_assets/diagram1.svg =800x)`. If you only specify the X-size in this way, the Y-size is selected automatically with the proportions maintained.

## Backward compatibility {#compatibility}

The development of documentation should not lead to a situation when users encounter saved links that are broken: both in browser bookmarks and those recorded in a variety of resources that are not controlled by the documentation developers, such as wiki pages.

This means that you generally can't rename articles or move them across directories.

If you need to convert an article into a set of articles within a new directory, to maintain the compatibility, this directory should be named as the original article and an `index.md` article should appear inside. As a result, following the old link to the article, users will get to the "Overview" page of the newly created directory.

If you cannot but move an article, do the following:

1. At the previous location, publish an article saying "This article no longer exists, a new location is <here>, please update the link" or something like that. The main thing is to ensure that, when following the old link, the user does not receive the HTTP 404 Not found error and can understand where it has been moved, what it has been broken into, or that it has been actually deleted and why.
2. At the old location of the article in the TOC, give a link to it with the `hidden` flag enabled:

   ```yaml
   - name: Deprecated article
     href: old_article.md
     hidden: true
   ```

As a result, this article won't be listed anywhere in the TOC, but will be available when following a direct link provided that you keep it somewhere.

