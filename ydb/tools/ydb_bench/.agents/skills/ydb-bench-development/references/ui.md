# Embedded UI conventions

Inspect a current neighboring page before changing layout. For design-only
requests show a compact example first; implementation/deployment require that
scope in the request.

- Reuse shell/navigation, `profile-tabs`, `runs-toolbar`, `runs-actions`, form
  controls and CSS variables. Scope new rules; avoid parallel component systems.
- Avoid repeating section titles already identified by navigation. Keep useful
  object names/breadcrumbs. Preserve tab baselines and active-tab outlines.
- Put contextual actions on the right in the existing toolbar. Keep metrics
  compact, units beside numbers, secondary facts muted and latency labeled with
  the configured percentile. Do not box every metric or reintroduce outer
  Result/Overview frames.
- Placement host/DC/tenant groups do have outer frames; avoid redundant nested
  body rectangles. Hide Unassigned groups when empty. Suggest rack names as
  `<dc-name>-R<number>` while preserving intentionally supplied names.
- Use `Add <entity>` consistently. Use the existing pencil and red cross for
  contextual edit/removal, with descriptive titles and accessible names.
  Explain whether removing an occupied group moves/unassigns nodes or deletes
  them; these are different actions.
- Card metadata depends on view: Physical shows name/type, tenant, DC/rack and
  affinity; Logical shows name/type, host and tenant; Tenants shows name/type,
  host and DC/rack. Omit values already supplied by the containing group.
  Do not repeat affinity everywhere or display actor-system settings here.
- Use an anchored affinity popup. Scope follows placement, not a second
  independent selector. Other reservations are grey; one line means chiplet
  mobility, two mean NUMA mobility. Explain notation through an accessible help
  control instead of permanent paragraphs under every widget.
- Keep Runs/Comparisons filtering consistent. Show Reset filters only when
  filters are active. Do not replace explicit saved comparisons with hidden
  persistent drafts.

Use real buttons/labels, visible focus, keyboard activation and contextual
accessible names. Separate node name and type text for screen readers.
Popups need Cancel/Escape and focus restoration. Drag-and-drop needs a
selector/button alternative.

Escape dynamic text and encode route components using existing helpers.
Preserve same-origin APIs/assets and supported HTTP operation; secure-context
browser APIs need fallbacks. Guard async results against stale routes, drafts
and closed popups. Cancel must not apply changes. Cross-view moves preserve
unrelated assignments; moving a manual mask between hosts requires explicit
reset confirmation because CPU IDs are host-local.

Check long names/FQDNs, narrow screens, empty/loading/error states, disabled
actions and unavailable hosts. A DOM snapshot alone is not visual acceptance.
