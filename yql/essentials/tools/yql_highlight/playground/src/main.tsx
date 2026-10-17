import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'

import { RuntimeError } from './RuntimeError'
import './styles.css'

const rootElement = document.getElementById('root')

if (!rootElement) {
  throw new Error('Root element is missing')
}

const root = createRoot(rootElement, {
  onCaughtError: (error) => root.render(<RuntimeError error={error} />),
  onRecoverableError: (error) => root.render(<RuntimeError error={error} />),
  onUncaughtError: (error) => root.render(<RuntimeError error={error} />),
})

window.addEventListener('error', (event) => {
  root.render(<RuntimeError error={event.error ?? event.message} />)
})

window.addEventListener('unhandledrejection', (event) => {
  root.render(<RuntimeError error={event.reason} />)
})

void import('./App')
  .then(({ default: App }) => {
    root.render(
      <StrictMode>
        <App />
      </StrictMode>,
    )
  })
  .catch((error: unknown) => {
    root.render(<RuntimeError error={error} />)
  })
