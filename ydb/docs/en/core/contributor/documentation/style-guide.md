# {{ ydb-short-name }} documentation style guide

The {{ ydb-short-name }} documentation style guide is designed to help writers create clear, consistent, and developer-friendly documentation.

## Core principles {#core-principles}

- **Target audience understanding:**
  - Before starting a new article or improving an existing one, take a moment to narrow down its target audience. Consider their specific role ([application developers](../../dev/index.md), [DevOps engineers](../../dev/index.md), [security engineers](../../security/index.md), etc.), technical background, and familiarity with the subject and {{ ydb-short-name }} in general.
  - Ensure the text is understandable to the article's intended audience.
- **Clear language:**
  - Use plain, simple language that directly communicates ideas.
  - Avoid complicated phrases that might be challenging for non-native speakers.
  - Avoid jargon and slang unless they are industry-standard.
- **Consistent terminology:**
  - For each uncommon term in the article, ensure that the first usage links to an explanation:
    - For {{ ydb-short-name }}-specific terms, link to the [{#T}](../../concepts/glossary.md). If the glossary lists multiple synonyms for the term, use the primary one from the glossary header. If the glossary entry is missing, add it.
    - For generic terms that some readers might not know, link to their Wikipedia page or a similar well-known source. Avoid linking to blog posts, social media, or random third-party articles.
  - If there's an abbreviation, spell it out in full the first time it appears in the article and explicitly show the abbreviation in parentheses. For example, Structured Query Language (SQL).
- **Structured content:**
  - Each article should have only *one* goal for the reader to achieve. If the text pursues multiple goals, it likely needs to be split into multiple articles. If possible, explicitly state this goal at the beginning of the article and reiterate it at the end.
  - Each article should follow only one [genre](genres.md). If not, it likely needs to be split into multiple articles.
  - Organize articles with clear headings, subheadings, bullet lists, notes, tables, and code blocks to help readers quickly navigate the information.
  - Follow the overall [documentation structure](structure.md) when creating new articles.

## Voice and tone {#voice}

- **Conversational.** Aim for a tone that feels like a knowledgeable friend explaining concepts in an approachable manner.
- **Friendly and respectful.** Maintain a balance of professionalism and warmth that invites engagement.
- **Inclusive language.** Write in a neutral and respectful way, avoiding biased or exclusive language.
- **Context-dependent.** Adjust your tone based on the type of content. For tutorials, be more encouraging; for reference material, be more concise.
- **Active voice.** Prefer active constructions to ensure clarity and immediacy, making instructions easier to follow.

## Language-specific

Ensure that text follows proper language rules with no typos, grammar, punctuation, or spelling issues. Additionally, follow these {{ ydb-short-name }}-specific rules.

### English-only

- Use the [serial comma](https://en.wikipedia.org/wiki/Serial_comma) where appropriate.
- Use [title case](https://en.wikipedia.org/wiki/Title_case) for headers, preferably following the [Chicago Manual of Style](https://en.wikipedia.org/wiki/Title_case#Chicago_Manual_of_Style) rules.
- Use double quotes (`"`).

### Russian-only

- Use `ё` where appropriate.
- Use [guillemets](https://en.wikipedia.org/wiki/Guillemet) for quotes (`«` and `»`).
- Use `—` for long dashes.
- Use semicolons for bullet and numbered lists where appropriate, i.e., for all items except the last one, unless items contain multiple sentences.
- Do *not* use title case for headers.

## Formatting and structure

- **Clear hierarchy.** Use headings and subheadings to define sections and make the document easy to scan.
- **Lists and bullet points.** Use bullet points or numbered lists to break down steps, features, or recommendations.
- **Visual highlights.** If there's an important warning, information, or tip, highlight it using a `{% note info %} ... {% endnote %}` tag. For smaller inline highlights, sparingly use **bold** or *italic*.
- **Code and samples.** When including code, use proper syntax highlighting, formatting, and comments to ensure readability. Show example output for queries and CLI commands. Use `code blocks` for everything likely to appear in a console or IDE, but not for visual highlights.
- **Linking and references.** Provide clear and descriptive links to related resources or additional documentation. Instead of repeating the target's header for internal links, use `[{#T}](path/to/an/article.md)`.
- **No copy-pasting.** If a piece of information needs to be displayed more than once, create a separate Markdown file in the `_includes` folder, then add it to all necessary places via `{% include [...](_includes/path/to/a/file.md) %}` instead of duplicating content by copying and pasting.
- **Markdown syntax style.** {{ ydb-short-name }} uses an automated linter for Markdown files. Refer to [.yfmlint](https://github.com/ydb-platform/ydb/blob/main/ydb/docs/.yfmlint) for the up-to-date list of enforced rules.
- **Variable usage.** The {{ ydb-short-name }} documentation has a configuration file, [presets.yaml](https://github.com/ydb-platform/ydb/blob/main/ydb/docs/presets.yaml), that lists variables to prevent typos or conditionally hide content. Use these variables when appropriate, particularly `{{ ydb-short-name }}` instead of `YDB`.
- **Diagrams.** Prefer built-in [Mermaid](https://github.com/diplodoc-platform/mermaid-extension) diagrams when possible. If using an external tool, submit the source file in the same folder for future edits. Ensure diagrams look good in both light and dark modes.
- **Proper article placement.** New articles should be correctly placed in the [documentation structure](structure.md).
- **Genre consistency.** Articles should not mix multiple [genres](genres.md), and the genre should match the article's place in the documentation structure.

### Documentation integration

- **Cross-referencing.** Include links to all relevant existing documentation pages, either inline or in a "See also" section at the end.
- **Bidirectional linking.** Update relevant existing articles with links to new articles.
- **Index inclusion.** Mention all articles in table of contents (`toc_i.yaml` or `toc_p.yaml`) and their folder's `index.md`.
- **Source code links.** Link directly to source files on GitHub when relevant. If the target is likely to change significantly over time, use a link to a specific commit or stable branch.
- **Glossary links:**
  - When a {{ ydb-short-name}}-specific term is mentioned in an article for the first time, make it a clickable link to the related glossary entry.
  - If there's a separate detailed article covering the same topic as a glossary term, link to it from the glossary term description.

## Usage and flexibility

- **Guidelines, not rigid rules.** This style guide offers recommendations to improve clarity and consistency, but flexibility is allowed when deviations benefit the content.
- **Supplementary resources.** Writers are encouraged to consult external style guides (e.g., The Chicago Manual of Style, Merriam-Webster) when in doubt.
- **Updates.** This guide is meant to evolve with the rest of the content over time. If you contribute to {{ ydb-short-name }} documentation frequently, check back periodically for updates.

## Things to avoid

- Jargon and slang.
- Using "we".
- Jokes and other emotional content.
- Excessively long sentences.
- Unrelated topics and references, such as culture, religion, or politics.
- Overly detailed implementation specifics (except in [{#T}](../../contributor/index.md), [{#T}](../../changelog-server.md), and [{#T}](../../public-materials/videos.md)).
- Reusing third-party content without written permission or a compatible open-source license explicitly allowing it.
- HTML inside Markdown, except as a workaround for missing visual styles.

## See also

- [{#T}](index.md)
- [{#T}](review.md)
- [{#T}](structure.md)
- [{#T}](genres.md)
