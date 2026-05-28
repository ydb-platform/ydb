import HostProcessItem from './HostProcessItem.js'

export default {
  components: {
    HostProcessItem
  },
  props: {
    type: String,
    description: String,
    hosts: Object, // { hostName: hostData }
    processes: Object, // { hostName: [ProcessInfo] }
    scheduleStatus: Object, // Schedule status for all nemesis types
    processTypes: Array // All process types with their configurations
  },
  setup(props) {
    const { ref, computed, reactive, watch } = Vue
    const isDescriptionExpanded = ref(false)
    const customInterval = ref(null)
    const showParamsModal = ref(false)
    // paramValues is a flat name->value map populated when the modal opens
    const paramValues = reactive({})

    // Get schedule state from props
    const isEnabled = computed(() => {
      const scheduleData = props.scheduleStatus[props.type]
      return scheduleData ? (scheduleData.enabled || false) : false
    })

    // Lookup of the catalog process type entry (carries schedule + params schema)
    const processTypeEntry = computed(() => {
      return props.processTypes.find(pt => pt.name === props.type) || {}
    })

    // Get default interval from process types
    const defaultInterval = computed(() => {
      return processTypeEntry.value.schedule || 60
    })

    // Parameter schema served by /api/process_types (may be empty)
    const paramSchema = computed(() => {
      return Array.isArray(processTypeEntry.value.params) ? processTypeEntry.value.params : []
    })

    const hasParams = computed(() => paramSchema.value.length > 0)

    // Initialize custom interval from schedule status or default
    const initializeCustomInterval = () => {
      const scheduleData = props.scheduleStatus[props.type]
      if (scheduleData && scheduleData.interval !== null && scheduleData.interval !== undefined) {
        customInterval.value = scheduleData.interval
      } else {
        customInterval.value = defaultInterval.value
      }
    }

    // Initialize on component mount
    initializeCustomInterval()

    // Watch for changes in schedule status to update custom interval
    watch(() => props.scheduleStatus[props.type], (newVal) => {
      if (newVal && newVal.interval !== null && newVal.interval !== undefined) {
        customInterval.value = newVal.interval
      }
    })

    function resetParamValues() {
      // Reset the reactive object to schema defaults.
      for (const k of Object.keys(paramValues)) {
        delete paramValues[k]
      }
      for (const p of paramSchema.value) {
        paramValues[p.name] = p.default !== undefined ? p.default : (p.type === 'bool' ? false : '')
      }
    }

    function openRunModal() {
      // For types without parameters, start immediately (skip modal).
      if (!hasParams.value) {
        startSchedule({})
        return
      }
      resetParamValues()
      showParamsModal.value = true
    }

    function cancelRunModal() {
      showParamsModal.value = false
    }

    function coerceParamValue(schema, raw) {
      if (schema.type === 'int') {
        const n = parseInt(raw, 10)
        if (Number.isNaN(n)) return null
        return n
      }
      if (schema.type === 'float') {
        const n = parseFloat(raw)
        if (Number.isNaN(n)) return null
        return n
      }
      if (schema.type === 'bool') {
        return Boolean(raw)
      }
      return raw
    }

    function submitParamsModal() {
      const collected = {}
      for (const p of paramSchema.value) {
        const v = coerceParamValue(p, paramValues[p.name])
        if (v === null) {
          alert(`Invalid value for parameter ${p.label || p.name}`)
          return
        }
        collected[p.name] = v
      }
      showParamsModal.value = false
      startSchedule(collected)
    }

    function startSchedule(params) {
      const interval = customInterval.value ? parseInt(customInterval.value) : null

      if (interval == null) {
        console.error(`Interval is null. Parsed from ${customInterval.value}`)
        alert('Schedule interval is null')
        return
      }

      axios.post('/api/schedule', {
        type: props.type,
        enabled: true,
        interval: interval,
        params: params
      })
        .catch(err => {
          console.error('Failed to start schedule', err)
        })
    }

    function stopSchedule() {
      axios.post('/api/schedule', {
        type: props.type,
        enabled: false
      })
        .catch(err => {
          console.error('Failed to stop schedule', err)
        })
    }

    function runProcess(host) {
      axios.post('/api/hosts/process', { host: host, type: props.type })
        .then(() => {
          console.log(`Started process ${props.type} on ${host}`)
        })
        .catch(err => {
          console.error(`Failed to start process ${props.type} on ${host}`, err)
        })
    }

    function getProcessesByTypeAndHost(host) {
      const hostProcs = props.processes[host] || []
      return hostProcs
        .sort((a, b) => b.id - a.id)
    }

    return {
      isEnabled,
      isDescriptionExpanded,
      customInterval,
      defaultInterval,
      paramSchema,
      hasParams,
      showParamsModal,
      paramValues,
      openRunModal,
      cancelRunModal,
      submitParamsModal,
      stopSchedule,
      runProcess,
      getProcessesByTypeAndHost
    }
  },
  template: `
    <details class="collapse collapse-arrow bg-base-100 shadow-xl mb-6">
      <summary class="collapse-title font-medium p-4">
        <div class="flex justify-between items-center pr-12">
          <div class="flex-1">
            <div class="flex items-center gap-3">
              <h2 class="text-xl font-bold">{{ type }}</h2>
              
              <div class="flex items-center gap-2">
                <label class="text-sm">Interval (sec):</label>
                <input
                  type="number"
                  v-model="customInterval"
                  :disabled="isEnabled"
                  class="input input-bordered input-sm w-20"
                  min="1"
                  placeholder="60"
                  @click.stop
                />
              </div>

              <button
                v-if="!isEnabled"
                @click.stop="openRunModal"
                class="btn btn-success btn-sm"
              >
                ▶ Run
              </button>
              <button
                v-else
                @click.stop="stopSchedule"
                class="btn btn-error btn-sm"
              >
                ■ Stop
              </button>

              <button
                v-if="description"
                @click.stop="isDescriptionExpanded = !isDescriptionExpanded"
                class="btn btn-ghost btn-xs"
              >
                {{ isDescriptionExpanded ? '📖 Hide Info' : '📖 Info' }}
              </button>
            </div>
            
            <div v-if="description && isDescriptionExpanded" class="mt-2" @click.stop>
              <div class="bg-base-200 rounded-box p-3">
                <p class="text-sm text-base-content/80 whitespace-pre-line">{{ description }}</p>
              </div>
            </div>
            
            <div class="flex items-center mt-2">
              <div class="tooltip tooltip-right" :data-tip="host" v-for="(hostData, host) in hosts" :key="host">
                <div
                  aria-label="status"
                  class='status p-1 m-1'
                  :class="{
                    'status-success animate-bounce': (processes[host] ?? []).length > 0 && processes[host].at(-1).status === 'running',
                    'status-success': (processes[host] ?? []).length > 0 && processes[host].at(-1).status === 'finished',
                    '': (processes[host] ?? []).length == 0,
                    'status-error': (processes[host] ?? []).length > 0 && ['failed', 'error'].includes(processes[host].at(-1).status),
                  }">
                </div>
              </div>
            </div>
          </div>
        </div>
      </summary>

      <!-- Run parameters modal (teleported to body so it sits above sibling stacking contexts) -->
      <teleport to="body">
        <div
          v-if="showParamsModal"
          class="fixed inset-0 z-[9999] flex items-center justify-center"
          @click.self="cancelRunModal"
        >
          <div class="absolute inset-0 bg-black/50"></div>
          <div class="relative bg-base-100 rounded-box shadow-2xl p-6 w-full max-w-md">
            <h3 class="font-bold text-lg mb-4">Run {{ type }}</h3>
            <div class="space-y-3">
              <div v-for="p in paramSchema" :key="p.name" class="form-control">
                <label class="label">
                  <span class="label-text font-medium">{{ p.label || p.name }}</span>
                </label>
                <input
                  v-if="p.type === 'int' || p.type === 'float'"
                  type="number"
                  v-model="paramValues[p.name]"
                  :min="p.min"
                  :max="p.max"
                  :step="p.type === 'float' ? 'any' : 1"
                  class="input input-bordered input-sm"
                />
                <input
                  v-else-if="p.type === 'bool'"
                  type="checkbox"
                  v-model="paramValues[p.name]"
                  class="toggle toggle-success"
                />
                <input
                  v-else
                  type="text"
                  v-model="paramValues[p.name]"
                  class="input input-bordered input-sm"
                />
                <p v-if="p.description" class="text-xs text-base-content/60 mt-1">{{ p.description }}</p>
              </div>
              <div v-if="paramSchema.length === 0" class="text-sm text-base-content/60">
                No configurable parameters for this nemesis.
              </div>
            </div>
            <div class="flex justify-end gap-2 mt-6">
              <button class="btn btn-ghost" @click="cancelRunModal">Cancel</button>
              <button class="btn btn-success" @click="submitParamsModal">▶ Run</button>
            </div>
          </div>
        </div>
      </teleport>

      <div class="collapse-content">
        <div class="pt-4">
          <host-process-item
            v-for="(hostData, host) in hosts"
            :key="host"
            :host="host"
            :processes="getProcessesByTypeAndHost(host)"
            :is-scheduled="isEnabled"
            @run-process="runProcess"
          ></host-process-item>
        </div>
      </div>
    </details>
  `
}
