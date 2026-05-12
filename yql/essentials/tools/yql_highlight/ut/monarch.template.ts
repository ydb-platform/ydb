const monarch = (syntax == 'yql')
    ? (/* {{YQL.monarch.json}} */)
    : (/* {{YQLs.monarch.json}} */);

const value = (syntax == 'yql')
    ? (`/* {{query.yql}} */`)
    : (`/* {{query.yqls}} */`);

/////////////////////////////////////////////////////////////////////////////

monaco.editor.defineTheme('vs', {
    base: 'vs',
    inherit: true,
    rules: [
        {token: 'string.tablepath', foreground: '338186'},
        {token: 'constant.yql', foreground: '608b4e'},
        {token: 'keyword.type', foreground: '4d932d'},
        {token: 'string.sql', foreground: 'a31515'},
        {token: 'support.function', foreground: '7a3e9d'},
        {token: 'constant.other.color', foreground: '7a3e9d'},
        {token: 'comment', foreground: '969896'},
    ],
    colors: {
        'editor.lineHighlightBackground': '#EFEFEF',
    },
});

monaco.editor.defineTheme('vs-dark', {
    base: 'vs-dark',
    inherit: true,
    rules: [
        {token: 'string.tablepath', foreground: '338186'},
        {token: 'constant.yql', foreground: '608b4e'},
        {token: 'keyword.type', foreground: '6A8759'},
        {token: 'string.sql', foreground: 'ce9178'},
        {token: 'support.function', foreground: '9e7bb0'},
        {token: 'constant.other.color', foreground: '9e7bb0'},
        {token: 'comment', foreground: '969896'},
    ],
    colors: {
        'editor.lineHighlightBackground': '#282A2E',
    },
});

/////////////////////////////////////////////////////////////////////////////

monaco.languages.register({ id: syntax });
monaco.languages.setMonarchTokensProvider(syntax, monarch);

const myEditor = monaco.editor.create(document.getElementById("container"), {
    value,
    language: syntax,
    theme: (theme == 'light') ? 'vs' : 'vs-dark',
    fontSize: 14,
    automaticLayout: true,
    lineNumbers: "off",
    minimap: {
        enabled: false,
    },
});
