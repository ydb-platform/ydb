import { loader } from '@monaco-editor/react'
import * as monaco from 'monaco-editor/esm/vs/editor/editor.api'
import EditorWorker from 'monaco-editor/esm/vs/editor/editor.worker?worker'

const workerScope = self as typeof self & {
  MonacoEnvironment: { getWorker: () => Worker }
}

workerScope.MonacoEnvironment = {
  getWorker: () => new EditorWorker(),
}

loader.config({ monaco })

monaco.editor.defineTheme('vs', {
  base: 'vs',
  inherit: true,
  rules: [
    // Path to table
    { token: 'string.tablepath', foreground: '338186' },
    // Constants true
    { token: 'constant.yql', foreground: '608b4e' },
    // Data types JSON, Int32 etc
    { token: 'keyword.type', foreground: '4d932d' },
    // Strings
    { token: 'string.sql', foreground: 'a31515' },
    // UDF
    { token: 'support.function', foreground: '7a3e9d' },
    // Builtin functions COUNT(
    { token: 'constant.other.color', foreground: '7a3e9d' },
    // Comments
    { token: 'comment', foreground: '969896' },
  ],
  colors: {
    'editor.lineHighlightBackground': '#EFEFEF',
  },
})

monaco.editor.defineTheme('vs-dark', {
  base: 'vs-dark',
  inherit: true,
  rules: [
    // Path to table
    { token: 'string.tablepath', foreground: '338186' },
    // Constants true
    { token: 'constant.yql', foreground: '608b4e' },
    // Data types JSON, Int32 etc
    { token: 'keyword.type', foreground: '6A8759' },
    // Strings
    { token: 'string.sql', foreground: 'ce9178' },
    // UDF
    { token: 'support.function', foreground: '9e7bb0' },
    // Builtin functions COUNT(
    { token: 'constant.other.color', foreground: '9e7bb0' },
    // Comments
    { token: 'comment', foreground: '969896' },
  ],
  colors: {
    'editor.lineHighlightBackground': '#282A2E',
  },
})

export { monaco }
