## Build technology

### Overlaying layers

Any version of the documentation is built using a common template:

1. The basic build directory contains two directories: `core` and `overlay`.
1. The `core` directory stores the basic documentation that can be added as an external dependency (such as git submodule).
1. The `overlay` directory stores content that is specific for this build.
1. The contents of the core directory are copied to the working directory.
1. The contents of the overlay directory are copied to the working directory, replacing any file whose relative path in the overlay directory matches that of any file from the core directory.

As a result, the working directory of the build contains the content from the core directory, in which the files present in the overlay directory are replaced. If a certain file is missing in `overlay`, a file from `core` gets to the build.

This overlay allows redefining the content, preserving the placement of articles, that is, keeping any links to them from other articles, whether they come from articles placed in `core` or `overlay`. As a result, content adaptation can be carried out within the article that requires adaptation, without the need to make technical adjustments in other articles.

{% note warning %}

Since the overlay of the `core` and `overlay` directories is only done when building the documentation, links from the articles in `overlay` to the articles in `core` won't be clickable in an integrated development environment (IDE) that knows nothing about the fact that some directories will be overlaid on top of each other during the build.

{% endnote %}

