import { useEffect, useMemo, useState } from 'react'
import hljs from 'highlight.js/lib/core'
import type { LanguageFn } from 'highlight.js'
import githubDarkCss from 'highlight.js/styles/github-dark.css?raw'
import githubLightCss from 'highlight.js/styles/github.css?raw'
import { createHighlighterCore, type LanguageRegistration } from 'shiki/core'
import { createOnigurumaEngine } from 'shiki/engine/oniguruma'
import githubDark from 'shiki/themes/github-dark.mjs'
import githubLight from 'shiki/themes/github-light.mjs'

import highlightJsGrammar from './generated/YQL.highlightjs.json'
import ansiTextMateGrammar from './generated/YQL.ansi.tmLanguage.json'
import textMateGrammar from './generated/YQL.tmLanguage.json'
import yqlsHighlightJsGrammar from './generated/YQLs.highlightjs.json'
import yqlsTextMateGrammar from './generated/YQLs.tmLanguage.json'

export type PreviewKind = 'none' | 'textmate' | 'highlightjs'
export type Theme = 'dark' | 'light'
export type Mode = 'ansi' | 'default'
export type Syntax = 'yql' | 'yqls'

interface PreviewProps {
  code: string
  kind: Exclude<PreviewKind, 'none'>
  mode: Mode
  syntax: Syntax
  theme: Theme
}

const ansiYqlTextMateLanguage = {
  ...ansiTextMateGrammar,
  name: 'yql-ansi',
  scopeName: 'source.yql.ansi',
} as LanguageRegistration

const defaultYqlTextMateLanguage = {
  ...textMateGrammar,
  name: 'yql-default',
  scopeName: 'source.yql.default',
} as LanguageRegistration

const yqlsTextMateLanguage = {
  ...yqlsTextMateGrammar,
  name: 'yqls-default',
  scopeName: 'source.yqls.default',
} as LanguageRegistration

function getTextMateLanguage(syntax: Syntax, mode: Mode) {
  if (syntax === 'yqls') {
    return yqlsTextMateLanguage
  }

  return mode === 'ansi'
    ? ansiYqlTextMateLanguage
    : defaultYqlTextMateLanguage
}

hljs.registerLanguage(
  'yql',
  (() => highlightJsGrammar) as unknown as LanguageFn,
)
hljs.registerLanguage(
  'yqls',
  (() => yqlsHighlightJsGrammar) as unknown as LanguageFn,
)

const textMateHighlighter = createHighlighterCore({
  engine: createOnigurumaEngine(import('shiki/wasm')),
  themes: [githubDark, githubLight],
  langs: [
    ansiYqlTextMateLanguage,
    defaultYqlTextMateLanguage,
    yqlsTextMateLanguage,
  ],
})

export function Preview({ code, kind, mode, syntax, theme }: PreviewProps) {
  const [textMateHtml, setTextMateHtml] = useState('')

  useEffect(() => {
    if (kind !== 'textmate') {
      return
    }

    let active = true
    void textMateHighlighter.then((highlighter) => {
      const language = getTextMateLanguage(syntax, mode)
      const html = highlighter.codeToHtml(code, {
        lang: language.name,
        theme: theme === 'dark' ? 'github-dark' : 'github-light',
      })
      if (active) {
        setTextMateHtml(html)
      }
    })

    return () => {
      active = false
    }
  }, [code, kind, mode, syntax, theme])

  const highlightJsHtml = useMemo(
    () => hljs.highlight(code, { language: syntax }).value,
    [code, syntax],
  )

  if (kind === 'textmate') {
    return (
      <div
        className="preview__content preview__content--textmate"
        dangerouslySetInnerHTML={{ __html: textMateHtml }}
      />
    )
  }

  return (
    <div className="preview__content preview__content--highlightjs">
      <style>{theme === 'dark' ? githubDarkCss : githubLightCss}</style>
      <pre>
        <code
          className="hljs"
          dangerouslySetInnerHTML={{ __html: highlightJsHtml }}
        />
      </pre>
    </div>
  )
}
