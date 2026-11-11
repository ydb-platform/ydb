import { targetLabel, recoveryLabel, pileProgress } from './operationView.js'

export default {
  props: { operations: Array },
  emits: ['changed'],
  setup(props, { emit }) {
    const { ref, computed } = Vue
    const busy = ref({})
    const errors = ref({})
    const selectedPile = ref('')
    const piles = computed(() => [...new Set((props.operations || []).flatMap(o => o.piles || []))].sort((a, b) => a - b))
    const visible = computed(() => (props.operations || []).filter(o => selectedPile.value === '' || (o.piles || []).includes(Number(selectedPile.value))))
    async function restore(operation) {
      const id = operation.operation_id
      if (busy.value[id]) return
      busy.value = { ...busy.value, [id]: true }
      errors.value = { ...errors.value, [id]: '' }
      try { await axios.post(`/api/operations/${encodeURIComponent(id)}/restore`, {}) }
      catch (err) { errors.value = { ...errors.value, [id]: err.response?.data?.message || err.message } }
      finally {
        busy.value = { ...busy.value, [id]: false }
        emit('changed')
      }
    }
    return { busy, errors, selectedPile, piles, visible, restore, targetLabel, recoveryLabel, pileProgress }
  },
  template: `
    <section class="card bg-base-100 shadow-md" aria-labelledby="operations-title">
      <div class="card-body p-5">
        <div class="flex flex-wrap items-center justify-between gap-2">
          <h2 id="operations-title" class="card-title">Operations</h2>
          <select v-model="selectedPile" class="select select-bordered select-sm" aria-label="Filter operations by pile">
            <option value="">All piles</option><option v-for="pile in piles" :key="pile" :value="String(pile)">Pile {{ pile }}</option>
          </select>
        </div>
        <p class="text-xs opacity-70">Delivery acceptance is separate from execution and cluster recovery. Completed history is kept for this process lifetime.</p>
        <p v-if="!visible.length" class="py-4 opacity-60">No operations in this view.</p>
        <article v-for="o in visible" :key="o.operation_id" class="border border-base-300 rounded-lg p-3 my-2 space-y-2">
          <div class="flex flex-wrap justify-between gap-2">
            <strong class="break-all">{{ o.type }}</strong><span class="badge badge-outline">{{ o.source }}</span>
          </div>
          <div class="font-mono text-xs break-all">{{ targetLabel(o.target) }}</div>
          <div class="flex flex-wrap gap-2 text-sm">
            <span class="badge" :class="o.delivery === 'failed' ? 'badge-error' : 'badge-ghost'">Delivery: {{ o.delivery }}</span>
            <span class="badge" :class="o.recovery?.stuck ? 'badge-error' : o.state === 'recovered' ? 'badge-success' : 'badge-warning'">{{ recoveryLabel(o) }}</span>
            <span v-for="pile in o.piles" :key="pile" class="badge badge-outline">Pile {{ pile }}</span>
          </div>
          <div v-for="line in pileProgress(o.recovery)" :key="line" class="text-sm font-mono">{{ line }}</div>
          <p v-if="o.error" class="text-error text-sm break-words">{{ o.error }}</p>
          <p v-if="errors[o.operation_id]" role="alert" class="text-error text-sm">{{ errors[o.operation_id] }}</p>
          <div class="flex flex-wrap items-center justify-between gap-2">
            <small class="font-mono opacity-60 break-all">{{ o.operation_id }} · {{ new Date(o.created_at).toLocaleTimeString() }}</small>
            <button v-if="o.can_restore" class="btn btn-sm btn-outline" :disabled="busy[o.operation_id]" @click="restore(o)">{{ busy[o.operation_id] ? 'Restoring…' : 'Restore operation' }}</button>
          </div>
        </article>
      </div>
    </section>
  `
}
