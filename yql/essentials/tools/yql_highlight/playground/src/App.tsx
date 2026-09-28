import { useEffect, useState } from 'react'
import Editor from '@monaco-editor/react'
import type { languages } from 'monaco-editor'

import ansiMonarchGrammar from './generated/YQL.ansi.monarch.json'
import monarchGrammar from './generated/YQL.monarch.json'
import yqlsMonarchGrammar from './generated/YQLs.monarch.json'
import { monaco } from './monaco'
import {
  Preview,
  type Mode,
  type PreviewKind,
  type Syntax,
  type Theme,
} from './Preview'
import yqlQuery from './query.yql?raw'
import yqlsQuery from './query.yqls?raw'

type Query = 'yql' | 'yqls'

const queries: Record<Query, string> = {
  yql: yqlQuery,
  yqls: yqlsQuery,
}

function getMonarchGrammar(syntax: Syntax, mode: Mode) {
  if (syntax === 'yqls') {
    return yqlsMonarchGrammar
  }

  return mode === 'ansi' ? ansiMonarchGrammar : monarchGrammar
}

function App() {
  const [theme, setTheme] = useState<Theme>('dark')
  const [preview, setPreview] = useState<PreviewKind>('none')
  const [mode, setMode] = useState<Mode>('ansi')
  const [syntax, setSyntax] = useState<Syntax>('yql')
  const [query, setQuery] = useState<Query>('yql')
  const [code, setCode] = useState(yqlQuery)
  const hasAnsiMode = syntax === 'yql' && preview !== 'highlightjs'

  useEffect(() => {
    if (!monaco.languages.getLanguages().some(({ id }) => id === syntax)) {
      monaco.languages.register({ id: syntax })
    }

    const provider = monaco.languages.setMonarchTokensProvider(
      syntax,
      getMonarchGrammar(syntax, mode) as languages.IMonarchLanguage,
    )

    return () => provider.dispose()
  }, [mode, syntax])

  return (
    <main className={`app app--${theme}`}>
      <header className="toolbar">
        <h1>YQL Highlight Playground</h1>

        <div className="toolbar__controls">
          <label>
            Syntax
            <select
              value={syntax}
              onChange={(event) => {
                const nextSyntax = event.target.value as Syntax
                setSyntax(nextSyntax)
                if (nextSyntax === 'yqls') {
                  setMode('default')
                }
              }}
            >
              <option value="yql">YQL</option>
              <option value="yqls">YQLs</option>
            </select>
          </label>

          <label>
            Query
            <select
              value={query}
              onChange={(event) => {
                const nextQuery = event.target.value as Query
                setQuery(nextQuery)
                setCode(queries[nextQuery])
              }}
            >
              <option value="yql">YQL sample</option>
              <option value="yqls">YQLs sample</option>
            </select>
          </label>

          <label>
            Theme
            <select
              value={theme}
              onChange={(event) => setTheme(event.target.value as Theme)}
            >
              <option value="dark">VS Code Dark</option>
              <option value="light">VS Code Light</option>
            </select>
          </label>

          <label>
            Extra preview
            <select
              value={preview}
              onChange={(event) => {
                const nextPreview = event.target.value as PreviewKind
                setPreview(nextPreview)
                if (nextPreview === 'highlightjs') {
                  setMode('default')
                }
              }}
            >
              <option value="none">None</option>
              <option value="textmate">TextMate</option>
              <option value="highlightjs">highlight.js</option>
            </select>
          </label>

          <label>
            Mode
            <select
              value={mode}
              onChange={(event) => setMode(event.target.value as Mode)}
            >
              {hasAnsiMode && <option value="ansi">ANSI</option>}
              <option value="default">Default</option>
            </select>
          </label>
        </div>
      </header>

      <section className="workspace">
        <div className="editor" data-split={preview !== 'none'}>
          <Editor
            language={syntax}
            theme={theme === 'dark' ? 'vs-dark' : 'vs'}
            value={code}
            onChange={(value) => setCode(value ?? '')}
            options={{
              automaticLayout: true,
              fontSize: 14,
              minimap: { enabled: false },
              scrollBeyondLastLine: false,
            }}
          />
        </div>

        {preview !== 'none' && (
          <aside className="preview" aria-label={`${preview} preview`}>
            <Preview
              code={code}
              kind={preview}
              mode={mode}
              syntax={syntax}
              theme={theme}
            />
          </aside>
        )}
      </section>
    </main>
  )
}

export default App
