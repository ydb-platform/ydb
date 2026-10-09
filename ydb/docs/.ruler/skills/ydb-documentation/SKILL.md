---
name: ydb-documentation
description: >-
  Use when documenting YDB features or functionality, including new articles,
  glossary entries, references, recipes, cross-links, TOC, or redirects in
  Russian and English.
---

# YDB Documentation Skill

## Required shared policy

Before planning or writing YDB documentation:

1. Read `ydb/docs/.ruler/DOCUMENTATION_POLICY.md` completely. If the workspace root is `ydb/docs`, use `.ruler/DOCUMENTATION_POLICY.md`.
2. Read every normative file it lists, in order.
3. Treat those files as the only source of shared writing rules.
4. If a workflow instruction conflicts with the policy, follow the policy and report the conflict.

Do not copy or summarize the shared rules in this skill.

This skill helps developers write documentation for YDB while following all structure, style, and language rules.

**Usage:** A developer describes what needs to be documented, the skill automatically determines where and how to place it, which sections to propagate to, and generates articles in RU and EN simultaneously.

---

## PR scope

Keep implementation and documentation separate. A documentation PR changes only
documentation files and must not include production code, code tests, or other
implementation changes. Use the implementation PR as factual evidence, then
open a separate documentation PR that references it.

---

## Workflow

### Stage 0: Information Preparation (IMPORTANT!)

**Before using the skill, the developer must prepare:**
- Feature description (what it does)
- Parameters, arguments, options
- Return values, results
- Usage examples (code, commands, config)
- Limitations and prerequisites
- Related functions or concepts
- Links to PR, Issue, specification

The skill analyzes this information and creates documentation. **An AI cannot invent details from nothing - factual information is required!**

### Stage 1: Gathering Information from User

The skill asks the following questions:

**Q1. Provide complete factual information** (required)
- All information about what needs to be documented
- Description, parameters, examples, limitations, context
- The more information provided, the higher quality the documentation
- Example:
  ```
  JDBC driver for YDB.
  - Supports standard JDBC API
  - Main classes: JdbcDriver, JdbcConnection, JdbcStatement
  - Connection parameters: url, user, password, poolSize, etc
  - Examples: (provide connection and query execution code)
  - Works with pooling, transactions, prepared statements
  - Limits: max connections, timeout settings
  ```

**Q2. Which PR/Issue implements this functionality?** (optional)
- List of PR URLs or numbers (e.g.: #38700, #39123)
- Helps the skill understand implementation context and details
- If provided - skill can read the PR for greater accuracy

**Q3. Target audience** (required, choose one or more)
- `all` — all users
- `newcomers` — beginners, first time working with YDB
- `app-developers` — application developers
- `devops` — DevOps engineers
- `security-engineers` — security engineers
- `contributors` — YDB contributors
- `analysts` — data analysts

**Q4. Content genre** (required, can choose multiple)
- `theory` — theoretical material, concepts
- `guide` — practical step-by-step guide
- `reference` — reference information, complete catalog
- `recipe` — mini-guide for a specific task
- `faq` — frequently asked question and answer

**Q5. Usage examples for context** (optional)
- Existing code examples, links to examples
- Helps the skill better understand the context

**Q6. Git branch name** (required)
- Format: `feature/description` or `docs/description`
- Example: `feature/jdbc-driver-docs` or `docs/nfs-backup`

**Git check:**
- Skill checks for uncommitted changes
- If found - suggests doing `git stash`
- Asks permission to create new branch

### Stage 2: Analysis and Plan Definition

The skill performs automatic analysis:

1. **Read all documentation**
   - All files in `ydb/docs/ru/core/**/*.md` and `ydb/docs/en/core/**/*.md`
   - DOCUMENTATION_MODEL.yaml
   - PRs if specified by user

2. **Determine main article**
   - Based on genre and audience, select appropriate section from DOCUMENTATION_MODEL
   - Determine exact file path (ydb/docs/ru/core/...)
   - Choose appropriate subsection (third level from model)

3. **Determine propagation to other sections**
   
   **Glossary:**
   - Highlight all new terms first mentioned in the article
   - Write definitions for glossary
   - Add glossary links in main article text
   - Update `concepts/glossary.md` (RU + EN)
   
   **Reference:**
   - If article contains parameters, options, API - highlight them
   - Create/update reference files in `reference/...`
   - Update corresponding references (RU + EN)
   
   **Include files:**
   - If there are reusable chunks (e.g., export procedure) - extract to `_includes/`
   - Use `{% include ... %}` in main article
   
   **Recipes:**
   - If there are code examples or step-by-step instructions - add to `recipes/...`
   - Create separate recipe or add to existing
   
   **Cross-references:**
   - Based on DOCUMENTATION_MODEL understand which other sections are related
   - Find existing articles where it makes sense to add link to new article
   - For each place specify where to add link and what text to use
   
   **Update DOCUMENTATION_MODEL:**
   - If new subsection is added (e.g. `dev/jdbc/`) - should the model be updated?
   - Add new entry to DOCUMENTATION_MODEL.yaml if needed
   
   **Update TOC (menu):**
   - Check `toc_p.yaml` and `toc_i.yaml` files in needed section
   - Determine where new article should be in menu hierarchy
   - Propose TOC updates (RU + EN)
   
   **Update redirects:**
   - If there is structure refactoring or file moves
   - ALWAYS add entry to `redirects.yaml` so old links don't become 404
   - Ensure all external links remain working

### Stage 3: Summary and Confirmation

The skill shows user the complete plan:

```
📋 DOCUMENTATION ADDITION PLAN

🎯 MAIN ARTICLE
  📄 Section: For Application Developers → JDBC
  📝 Path: ydb/docs/ru/core/dev/jdbc/jdbc-driver.md
  👥 Audience: app-developers
  📚 Genres: guide
  
📌 PROPAGATION TO OTHER SECTIONS
  
  ✅ GLOSSARY
     New terms (5 total):
     - Connection Pool — pool of connections for reuse
     - Prepared Statement — prepared expression for reuse
     - Transaction — atomic operation
     - Commit — transaction completion
     - Rollback — transaction rollback
     Files: concepts/glossary.md (RU + EN)
  
  ✅ REFERENCE
     Add JDBC driver API reference
     Files:
     - reference/languages-and-apis/java/jdbc-api-reference.md (RU + EN)
     - reference/ydb-sdk/java/jdbc-parameters.md (RU + EN)
  
  ✅ RECIPES
     Add mini-guides with examples:
     - recipes/ydb-sdk/java/jdbc-connection-pool.md
     - recipes/ydb-sdk/java/jdbc-transactions.md
     - recipes/ydb-sdk/java/jdbc-prepared-statements.md
     Files: RU + EN versions
  
  ✅ CROSS-REFERENCES
     Add links to new article in:
     - dev/example-app/ (JDBC usage example)
     - integrations/orm/ (ORM integration)
     - reference/languages-and-apis/index.md (in API list)
  
  ✅ UPDATE MODEL
     Add subsection to DOCUMENTATION_MODEL.yaml:
     - dev/jdbc/ (new subsection)
     With description: "JDBC driver for Java applications"
  
  ✅ UPDATE TOC (MENU)
     Files: toc_p.yaml and toc_i.yaml
     Add menu item:
     - section "For Application Developers"
     - subsection "JDBC"
  
  ✅ UPDATE REDIRECTS
     If there are file moves - add to redirects.yaml
     Example: /docs/ru/dev/old-jdbc-path → /docs/ru/dev/jdbc/jdbc-driver

🌍 LANGUAGES: RU + EN (simultaneously, not translation)
🔀 BRANCH: feature/jdbc-driver-documentation
📦 TOTAL FILES: approximately 15 new/updated (RU + EN)
```

**Skill asks user:**
```
✅ Does the plan look correct?

If not, write what needs to be changed:
- Add/remove propagation section
- Change article path
- Add/remove cross-references
- Other
```

If user wants to change something:
- Skill asks what specifically
- Updates plan
- Shows new summary
- Repeats question

Repeats until user says "OK, everything is correct"

### Stage 4: File Generation

When user confirms plan, skill generates ALL files simultaneously in one call.

**Mandatory rules during generation:**

#### Shared documentation policy

Apply `ydb/docs/.ruler/DOCUMENTATION_POLICY.md` and every normative file it
lists. Do not rely on an embedded summary.

#### Propagation by rules

When deciding what to generate, skill should follow these priorities:

| Place | When to generate | Workflow action |
|-------|------------------|-----------------|
| **Glossary** | Always if new terms exist | Add or update the RU and EN glossary entries |
| **Reference** | If API or parameters exist | Add or update the RU and EN reference pages |
| **Recipes** | If practical examples exist | Add or update the RU and EN recipes |
| **Include files** | If reusable chunks exist | Extract reusable content |
| **Cross-references** | Always where logically connected | Based on DOCUMENTATION_MODEL |
| **TOC** | If new level added | Update navigation in both languages |
| **Redirects** | ALWAYS on move | Guarantee old links work |

---

**Generated files:**

**Main files:**
- ✅ Main article in Russian (ydb/docs/ru/core/...)
- ✅ Main article in English (ydb/docs/en/core/...)

**Glossary:**
- ✅ Add entries to glossary.md (RU)
- ✅ Add entries to glossary.md (EN)

**Reference:**
- ✅ New/updated files in reference/ (RU + EN)

**Include files:**
- ✅ Reusable chunks in _includes/ (RU + EN)

**Recipes:**
- ✅ New recipes in recipes/ (RU + EN)

**Cross-reference updates:**
- ✅ Add links in existing articles (RU + EN)

**Documentation model:**
- ✅ Update DOCUMENTATION_MODEL.yaml (if needed)

**TOC (menu):**
- ✅ Update toc_p.yaml (RU + EN)
- ✅ Update toc_i.yaml (RU + EN) if needed

**Redirects:**
- ✅ Update redirects.yaml (always if moves exist)

### Stage 5: Completion

Skill informs user:

```
✅ DONE!

Branch created: feature/jdbc-driver-documentation
Total files created/updated: 16

Files are in branch. Next:

1️⃣  Check changes (git status, git diff)
2️⃣  Commit:
    git add -- <changed-documentation-paths>
    git commit -m "Add JDBC driver documentation

    - Main guide: dev/jdbc/jdbc-driver.md
    - Glossary entries for JDBC terms
    - API reference in reference/languages-and-apis/
    - Examples in recipes/ydb-sdk/java/
    - Cross-links in dev/example-app/ and integrations/orm/
    - Updated toc_p.yaml
    - Added DOCUMENTATION_MODEL.yaml entries"

3️⃣  Push to GitHub:
    git push -u origin feature/jdbc-driver-documentation

4️⃣  Create PR on GitHub and go through review
```

---

## Important Skill Principles

### 1. RU/EN simultaneously
- Skill writes both languages in one call, independently of each other
- Does not translate, but creates analogous articles for each language
- Respects local cultural norms (formatting, examples, etc.)

### 2. Model as navigator
- DOCUMENTATION_MODEL.yaml used as structure reference
- Skill understands where to place things based on model
- Model can be updated together with new article

### 3. Completeness and attentiveness
- Skill identifies ALL places where changes are needed
- Does not forget glossary, reference, include-files, cross-references, redirects
- Always asks user before generating

### 4. Reusability through include
- If reusable chunks exist - extract to `_includes/`
- Use `{% include 'path/to/file.md' %}` in main article

### 5. Cross-references always
- Skill always finds places for cross-references
- Looks at structure in DOCUMENTATION_MODEL to understand what's connected

### 6. Redirects always
- If any file moves exist - ALWAYS add to redirects.yaml
- Respects external links, ensures they won't become 404

---

## Instructions for Agents

**Using this skill:**

1. Understand what user wants to write
2. Ask 6 questions (Q1-Q6)
3. Check git status
4. Create detailed plan (Stage 2)
5. Show summary and ask for confirmation (Stage 3)
6. If changes needed - update plan and repeat
7. When user confirms - generate ALL files (Stage 4)
8. Report result and what to do next (Stage 5)

**Mandatory during generation:**
- ✅ Read ALL documentation (don't skimp on context)
- ✅ Use DOCUMENTATION_MODEL.yaml as reference
- ✅ Always verify user agrees with plan before generating
- ✅ Generate RU and EN simultaneously, independently of each other
- ✅ Don't forget glossary, include-files, cross-references, redirects, TOC, model
- ✅ Re-read `ydb/docs/.ruler/DOCUMENTATION_POLICY.md` and verify the generated files against every normative file it lists.

**Prohibited:**
- ❌ Generate without user confirmation
- ❌ Forget any parts (glossary, reference, cross-references, etc.)
- ❌ Violate any rule or priority from the shared documentation policy
- ❌ Create duplicate content instead of reuse through include
- ❌ Ignore redirects on file moves
