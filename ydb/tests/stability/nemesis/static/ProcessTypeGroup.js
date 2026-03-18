import HostProcessItem from './HostProcessItem.js'

export default {
  components: {
    HostProcessItem
  },
  props: {
    type: String,
    description: String,
    hosts: Object, // { hostName: hostData }
    processes: Object // { hostName: [ProcessInfo] }
  },
  setup(props) {
    const { ref, computed } = Vue
    const isEnabled = ref(false)
    const isDescriptionExpanded = ref(false)
    const customInterval = ref(null)
    
    // Get default interval from PROCESS_TYPES
    const defaultInterval = computed(() => {
      // This will be fetched from backend via process_types endpoint
      return 60 // fallback
    })

    // Fetch initial schedule state
    axios.get('/api/schedule')
      .then(response => {
        const scheduleData = response.data[props.type]
        if (scheduleData !== undefined) {
          isEnabled.value = scheduleData.enabled || false
          if (scheduleData.interval !== null && scheduleData.interval !== undefined) {
            customInterval.value = scheduleData.interval
          }
        }
      })
    
    // Fetch default interval from process types
    axios.get('/api/process_types')
      .then(response => {
        const processType = response.data.find(pt => pt.name === props.type)
        if (processType && !customInterval.value) {
          // Set default from config if not already set
          customInterval.value = processType.schedule || 60
        }
      })

    function toggleSchedule() {
      const newState = !isEnabled.value
      const interval = customInterval.value ? parseInt(customInterval.value) : null
      
      axios.post('/api/schedule', {
        type: props.type,
        enabled: newState,
        interval: interval
      })
        .then(() => {
          isEnabled.value = newState
        })
        .catch(err => {
          console.error('Failed to update schedule', err)
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
      toggleSchedule,
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
              
              <input
                type="checkbox"
                :checked="isEnabled"
                @click.stop.prevent="toggleSchedule"
                class="toggle"
                :class="isEnabled ? 'toggle-success' : 'toggle-neutral'"
              />
              
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