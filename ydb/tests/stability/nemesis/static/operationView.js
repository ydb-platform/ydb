export function targetLabel(target = {}) {
  const fields = [target.kind, target.host]
  if (target.slot_idx != null) fields.push(`slot ${target.slot_idx}`)
  else if (target.node_id != null) fields.push(`node ${target.node_id}`)
  if (target.ic_port != null) fields.push(`ic ${target.ic_port}`)
  if (target.group_id != null) fields.push(`group ${target.group_id}`)
  if (target.disk_path) fields.push(target.disk_path)
  return fields.filter(Boolean).join(' · ')
}

export function recoveryLabel(operation) {
  if (operation.state === 'recovered') return 'Recovered'
  if (operation.state === 'aborted') return 'Not sent'
  if (!operation.tracked) return 'No recovery tracking'
  const recovery = operation.recovery
  if (!recovery) return operation.state === 'injecting' ? 'Sending' : 'Awaiting observation'
  if (recovery.extracting) return 'Sending restore'
  if (recovery.stuck) return 'Stuck — budget held'
  if (recovery.dispatching) return 'Sending'
  if (recovery.phase === 'confirm') return recovery.extract_ok ? 'Confirming recovery' : 'Restore failed — retry pending'
  return recovery.toggle ? 'Holding until restore' : 'Waiting for recovery'
}

export function pileProgress(recovery) {
  return Object.entries(recovery?.pile_recovery || {}).map(([pile, counts]) => {
    if (counts.baseline == null) return `Pile ${pile}: storage recovery`
    return `Pile ${pile}: ${counts.alive == null ? 'unknown' : counts.alive} / ${counts.baseline} compute nodes`
  })
}
