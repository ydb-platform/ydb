(()=>{
// Move style to head (replacing any old version) to preserve it when .tracerite elements are deleted
const current=document.currentScript?.parentElement
const style=current?.querySelector('style')
if(style){
  document.getElementById('tracerite-style')?.remove()
  style.id='tracerite-style'
  document.head.appendChild(style)
}
// Remove contents of any previous tracerite elements based on cleanup mode
if(current?.dataset.replacePrevious){
  const mode=current.dataset.cleanupMode||'replace'
  document.querySelectorAll('.tracerite').forEach(el=>{
    if(el!==current){
      // In replace mode, remove all direct children except h2
      if(mode==='replace')[...el.children].forEach(c=>{if(c.tagName!=='H2')c.remove()})
    }
  })
}
})()
