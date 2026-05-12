export default {
  props: {
    host: String,
    processes: Array,
    isScheduled: Boolean
  },
  setup(props) {
    const { computed } = Vue

    const latestProcess = computed(() => {
      if (!props.processes || props.processes.length === 0) return null
      return props.processes[0]
    })

    const previousProcesses = computed(() => {
      if (!props.processes || props.processes.length <= 1) return []
      return props.processes.slice(1)
    })

    return {
      latestProcess,
      previousProcesses
    }
  },
  emits: ['run-process'],
  template: `
    <details class="collapse collapse-arrow bg-base-200 mb-2">
      <summary class="collapse-title font-medium flex justify-between items-center pr-12">
        <div class="flex items-center gap-2">
          <span class="font-mono font-bold">{{ host }}</span>
          <div class="tooltip tooltip-right" :data-tip="isScheduled ? 'Disable scheduling to run manually' : 'Run nemesis on this host'">
            <button
              class="btn btn-xs z-10"
              :class="isScheduled ? 'btn-disabled' : 'btn-primary'"
              :disabled="isScheduled"
              @click.stop="$emit('run-process', host)"
            >
              Run
            </button>
          </div>
        </div>
        
        <div v-if="latestProcess" class="flex items-center gap-4">
          <span class="text-sm opacity-70">Last run:</span>
          <div aria-label="status" class="badge" :class="{
            'badge-success': latestProcess.status === 'finished',
            'badge-error': latestProcess.status === 'failed' || latestProcess.status === 'error',
            'badge-warning': latestProcess.status === 'running'
          }">{{ latestProcess.status }}</div>
          <span v-if="latestProcess.ret_code !== null" class="font-mono text-sm">
            RC: {{ latestProcess.ret_code }}
          </span>
        </div>
        <div v-else class="text-sm opacity-50">
          No runs
        </div>
      </summary>
      
      <div class="collapse-content">
        <div v-if="latestProcess" class="pt-4">
          <!-- Latest Run Details -->
          <div class="mb-4">
            <h3 class="font-bold text-sm mb-2">Latest Run (#{{ latestProcess.id }})</h3>
            <div class="font-mono bg-base-300 p-2 rounded text-xs mb-2 break-all">
              $ {{ latestProcess.command }}
            </div>
            <div>
              <div class="text-xs font-bold mb-1">Logs</div>
              <pre class="bg-black text-green-400 p-2 rounded h-32 overflow-auto text-xs">{{ latestProcess.logs }}</pre>
            </div>
          </div>

          <!-- Previous Runs -->
          <div v-if="previousProcesses.length > 0">
            <div class="divider text-xs">Previous Runs</div>
            <div class="overflow-x-auto">
              <table class="table table-xs table-zebra w-full">
                <thead>
                  <tr>
                    <th>ID</th>
                    <th>Status</th>
                    <th>Exit Code</th>
                    <th>Output Preview</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="proc in previousProcesses" :key="proc.id">
                    <td>#{{ proc.id }}</td>
                    <td>
                      <span class="badge badge-xs" :class="{
                        'badge-success': proc.status === 'finished',
                        'badge-error': proc.status === 'failed' || proc.status === 'error',
                        'badge-warning': proc.status === 'running'
                      }">{{ proc.status }}</span>
                    </td>
                    <td>{{ proc.ret_code }}</td>
                    <td class="max-w-xs truncate font-mono text-xs opacity-70">
                      {{ (proc.logs || '').substring(0, 50) }}...
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>
        </div>
        <div v-else class="py-4 text-center opacity-50">
          No process history for this type on {{ host }}
        </div>
      </div>
    </details>
  `
}