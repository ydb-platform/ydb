import { targetLabel } from './operationView.js'

export default {
  props: { faults: Array, ready: Boolean },
  emits: ['changed'],
  setup(props, { emit }) {
    const { ref, computed, watch } = Vue
    const selectedType = ref('')
    const selectedTarget = ref('')
    const targets = ref([])
    const loading = ref(false)
    const busy = ref(false)
    const error = ref('')
    const receipt = ref('')
    const holdSec = ref('')
    let requestVersion = 0
    const fault = computed(() => (props.faults || []).find(f => f.name === selectedType.value))
    const choice = computed(() => targets.value[Number(selectedTarget.value)])
    const canInject = computed(() => props.ready && fault.value?.available && selectedTarget.value !== '' &&
      choice.value && !loading.value && !busy.value)
    watch(selectedType, async () => {
      const version = ++requestVersion
      selectedTarget.value = ''
      targets.value = []
      error.value = ''
      receipt.value = ''
      holdSec.value = fault.value?.hold_sec ?? ''
      if (!fault.value?.available) return
      loading.value = true
      try {
        const response = await axios.get('/api/targets', { params: { type: selectedType.value } })
        if (version === requestVersion) targets.value = response.data
      } catch (err) {
        if (version === requestVersion) error.value = err.response?.data?.message || err.message
      } finally {
        if (version === requestVersion) loading.value = false
      }
    })
    async function inject() {
      if (!canInject.value) return
      busy.value = true
      error.value = ''
      receipt.value = ''
      const body = { type: selectedType.value, target: choice.value.target }
      if (fault.value.recovery_mode === 'extract' && holdSec.value !== '') {
        const value = Number(holdSec.value)
        if (!Number.isFinite(value) || value < 1 || value > 86400) {
          error.value = 'Hold duration must be between 1 and 86400 seconds.'
          busy.value = false
          return
        }
        body.hold_sec = value
      }
      try {
        const response = await axios.post('/api/operations', body)
        receipt.value = `Operation ${response.data.operation_id} accepted. Follow recovery below.`
      } catch (err) {
        error.value = err.response?.data?.message || err.message
      } finally {
        busy.value = false
        emit('changed')
      }
    }
    return { selectedType, selectedTarget, targets, loading, busy, error, receipt, holdSec,
             fault, choice, canInject, targetLabel, inject }
  },
  template: `
    <section class="card bg-base-100 shadow-md" aria-labelledby="manual-title">
      <div class="card-body p-5 space-y-3">
        <h2 id="manual-title" class="card-title">Manual fault</h2>
        <p class="text-sm opacity-70">Inject once on a concrete target. Manual and scheduled operations share the same budgets.</p>
        <label class="form-control">
          <span class="label-text font-medium">Fault type</span>
          <select v-model="selectedType" :disabled="busy || !ready" class="select select-bordered w-full" aria-label="Fault type">
            <option value="">Choose a fault</option>
            <option v-for="f in faults" :key="f.name" :value="f.name">{{ f.name }}{{ f.available ? '' : ' — unavailable' }}</option>
          </select>
        </label>
        <p v-if="fault?.description" class="text-sm whitespace-pre-line opacity-80">{{ fault.description }}</p>
        <p v-if="fault && !fault.available" role="status" class="alert alert-warning">{{ fault.unavailable_reason }}</p>
        <template v-if="fault?.available">
          <label class="form-control">
            <span class="label-text font-medium">Target · {{ fault.target_kind }}</span>
            <select v-model="selectedTarget" :disabled="busy || loading" class="select select-bordered w-full" aria-label="Fault target">
              <option value="">{{ loading ? 'Loading targets…' : 'Choose a target' }}</option>
              <option v-for="(row, i) in targets" :key="i" :value="String(i)">{{ targetLabel(row.target) }} · {{ row.placement.map(p => 'pile ' + p[1]).filter((v, i, a) => a.indexOf(v) === i).join(', ') }}</option>
            </select>
          </label>
          <p v-if="!loading && !targets.length" class="text-sm">No targets available in this inventory.</p>
          <div v-if="selectedTarget !== '' && choice" class="overflow-x-auto">
            <table class="table table-xs"><thead><tr><th>Host</th><th>Pile</th><th>DC</th><th>Rack</th></tr></thead>
              <tbody><tr v-for="(p, i) in choice.placement" :key="i"><td v-for="(value, j) in p" :key="j">{{ value }}</td></tr></tbody>
            </table>
          </div>
          <p v-if="fault.target_kind === 'disk'" class="text-sm opacity-70">The target is a storage node; the current runner chooses the disk on that node.</p>
          <label v-if="fault.recovery_mode === 'extract'" class="form-control">
            <span class="label-text font-medium">Hold before automatic restore (seconds)</span>
            <input v-model="holdSec" :disabled="busy" type="number" min="1" max="86400" class="input input-bordered w-full" aria-label="Hold duration" placeholder="Catalog default">
          </label>
          <p class="text-sm opacity-70">{{ fault.recovery_mode === 'extract' ? 'Restore can also be requested from the operation below. The budget remains held until recovery is confirmed.' : 'This fault recovers without a manual restore. Follow its recovery below.' }}</p>
        </template>
        <p v-if="error" role="alert" class="alert alert-error break-words">{{ error }}</p>
        <p v-if="receipt" role="status" class="alert alert-success break-all">{{ receipt }}</p>
        <button class="btn btn-primary self-start" :disabled="!canInject" @click="inject">{{ busy ? 'Sending…' : 'Inject once' }}</button>
      </div>
    </section>
  `
}
