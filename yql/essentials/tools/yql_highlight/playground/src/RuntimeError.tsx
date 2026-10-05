function formatError(error: unknown) {
  if (error instanceof Error) {
    return error.stack ?? `${error.name}: ${error.message}`
  }
  return String(error)
}

export function RuntimeError({ error }: { error: unknown }) {
  return (
    <main className="runtime-error" role="alert">
      <section className="runtime-error__panel">
        <h1>YQL Highlight Playground failed to start</h1>
        <p>The runtime error is shown below so it can be diagnosed.</p>
        <pre>{formatError(error)}</pre>
      </section>
    </main>
  )
}
