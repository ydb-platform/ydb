export default {
  props: {
    wardenResults: Object,
    isLoading: Boolean
  },
  emits: ['run-checks'],
  setup(props, { emit }) {
    const { computed, ref, onMounted } = Vue

    // Modal state
    const showModal = ref(false)
    const modalCheck = ref(null)
    const activeTab = ref(0)  // Index of active tab (host)
    
    // Available checks state
    const availableChecks = ref([])
    const isLoadingChecks = ref(false)

    const hasResults = computed(() => {
      if (!props.wardenResults || Object.keys(props.wardenResults).length === 0) {
        return false
      }
      // Check if all results are idle - if so, treat as no results
      const allIdle = Object.values(props.wardenResults).every(
        result => result.status === 'idle'
      )
      return !allIdle
    })

    // Separate orchestrator results from agent results
    const orchestratorResult = computed(() => {
      if (!hasResults.value) return null
      return props.wardenResults['_orchestrator'] || null
    })

    const agentResults = computed(() => {
      if (!hasResults.value) return {}
      const results = {}
      for (const [host, result] of Object.entries(props.wardenResults)) {
        if (host !== '_orchestrator') {
          results[host] = result
        }
      }
      return results
    })

    // Total number of agents (all hosts that have any result, even 'running')
    const totalAgentCount = computed(() => {
      return Object.keys(agentResults.value).length
    })

    // Count of agents that have completed their checks
    const completedAgentCount = computed(() => {
      let count = 0
      for (const result of Object.values(agentResults.value)) {
        if (result.status === 'completed' || result.status === 'error') {
          count++
        }
      }
      return count
    })

    // Convert available checks to the same format as aggregatedResults (for display before running)
    const availableChecksAsResults = computed(() => {
      if (availableChecks.value.length === 0) {
        return { liveness: [], safety: [], totalAgents: 0, completedAgents: 0 }
      }
      
      const liveness = []
      const safety = []
      
      for (const check of availableChecks.value) {
        if (check.category === 'liveness') {
          liveness.push({
            name: check.name,
            status: 'idle',
            host: 'master'
          })
        } else if (check.category === 'safety') {
          safety.push({
            name: check.name,
            status: 'idle',
            isOrchestratorCheck: check.location === 'master',
            completedHosts: [],
            issues: []
          })
        }
      }
      
      return { liveness, safety, totalAgents: 0, completedAgents: 0 }
    })

    const aggregatedResults = computed(() => {
      if (!hasResults.value) return { liveness: [], safety: [], totalAgents: 0, completedAgents: 0 }
      
      const liveness = []
      const safetyMap = new Map()
      
      // Liveness checks come from orchestrator
      if (orchestratorResult.value && orchestratorResult.value.liveness_checks) {
        for (const check of orchestratorResult.value.liveness_checks) {
          liveness.push({ ...check, host: 'master' })
        }
      }
      
      // PDisk safety checks also come from orchestrator (not per-agent, so don't count in agent progress)
      if (orchestratorResult.value && orchestratorResult.value.safety_checks) {
        for (const check of orchestratorResult.value.safety_checks) {
          // Handle aggregated UnifiedAgentVerifyFailedSafetyWarden specially
          const isAggregatedVerifyFailed = check.name.includes('UnifiedAgentVerifyFailedAggregated')
          const baseName = isAggregatedVerifyFailed ? 'UnifiedAgentVerifyFailedAggregated' : check.name.split(' ')[0]
          
          if (!safetyMap.has(baseName)) {
            safetyMap.set(baseName, {
              name: baseName,
              status: 'ok',
              isOrchestratorCheck: true,  // Mark as orchestrator-only check
              completedHosts: [],
              issues: [],
              isAggregated: isAggregatedVerifyFailed,
              affectedHosts: check.affected_hosts || []
            })
          }
          const agg = safetyMap.get(baseName)
          agg.completedHosts.push('master')
          
          if (check.status === 'error') agg.status = 'error'
          else if (check.status === 'violation' && agg.status !== 'error') agg.status = 'violation'
          
          if (check.status !== 'ok') {
            agg.issues.push({
              host: isAggregatedVerifyFailed ? 'aggregated' : 'master',
              violations: check.violations,
              error_message: check.error_message,
              affectedHosts: check.affected_hosts || []
            })
          }
        }
      }
      
      // Safety checks come from agents
      for (const [host, result] of Object.entries(agentResults.value)) {
        if (result.safety_checks) {
          for (const check of result.safety_checks) {
            const baseName = check.name.split(' ')[0]
            if (!safetyMap.has(baseName)) {
              safetyMap.set(baseName, {
                name: baseName,
                status: 'ok',
                isOrchestratorCheck: false,  // This is an agent check
                completedHosts: [],
                issues: []
              })
            }
            const agg = safetyMap.get(baseName)
            // Mark as completed if the agent has finished
            if (result.status === 'completed' || result.status === 'error') {
              agg.completedHosts.push(host)
            }
            
            if (check.status === 'error') agg.status = 'error'
            else if (check.status === 'violation' && agg.status !== 'error') agg.status = 'violation'
            
            if (check.status !== 'ok') {
              agg.issues.push({
                host: host,
                violations: check.violations,
                error_message: check.error_message
              })
            }
          }
        }
      }
      
      return {
        liveness,
        safety: Array.from(safetyMap.values()),
        totalAgents: totalAgentCount.value,
        completedAgents: completedAgentCount.value,
        completedAt: orchestratorResult.value.completed_at
      }
    })

    // Display results - use available checks format before running, actual results after
    // If aggregatedResults has empty fields, fill them from availableChecksAsResults
    const displayResults = computed(() => {
      if (!hasResults.value) {
        return availableChecksAsResults.value
      }
      
      const agg = aggregatedResults.value
      const avail = availableChecksAsResults.value
      
      return {
        liveness: agg.liveness.length > 0 ? agg.liveness : avail.liveness,
        safety: agg.safety.length > 0 ? agg.safety : avail.safety,
        totalAgents: agg.totalAgents,
        completedAgents: agg.completedAgents,
        completedAt: agg.completedAt
      }
    })

    const overallStatus = computed(() => {
      if (!hasResults.value) return 'idle'
      
      let hasRunning = false
      let hasError = false
      let hasViolation = false
      
      // Check orchestrator status
      if (orchestratorResult.value) {
        if (orchestratorResult.value.status === 'running') hasRunning = true
        if (orchestratorResult.value.status === 'error') hasError = true
      }
      
      // Check all results
      for (const result of Object.values(props.wardenResults)) {
        if (result.status === 'running') hasRunning = true
        if (result.status === 'error') hasError = true
        
        const allChecks = [...(result.liveness_checks || []), ...(result.safety_checks || [])]
        for (const check of allChecks) {
          if (check.status === 'violation') hasViolation = true
          if (check.status === 'error') hasError = true
        }
      }
      
      if (hasRunning) return 'running'
      if (hasError) return 'error'
      if (hasViolation) return 'violation'
      return 'ok'
    })

    const statusBadgeClass = computed(() => {
      switch (overallStatus.value) {
        case 'ok': return 'badge-success'
        case 'violation': return 'badge-warning'
        case 'error': return 'badge-error'
        case 'running': return 'badge-info'
        default: return 'badge-ghost'
      }
    })

    function getCheckStatusClass(status) {
      switch (status) {
        case 'ok': return 'text-success'
        case 'violation': return 'text-warning'
        case 'error': return 'text-error'
        default: return 'text-base-content'
      }
    }

    function getCheckIcon(status) {
      switch (status) {
        case 'ok': return '✓'
        case 'violation': return '⚠'
        case 'error': return '✗'
        default: return '?'
      }
    }

    async function fetchAvailableChecks() {
      isLoadingChecks.value = true
      try {
        const response = await fetch('/api/warden/checks')
        if (response.ok) {
          availableChecks.value = await response.json()
        }
      } catch (error) {
        console.error('Failed to fetch available checks:', error)
      } finally {
        isLoadingChecks.value = false
      }
    }
    
    function runChecks() {
      emit('run-checks')
    }
    
    onMounted(() => {
      fetchAvailableChecks()
    })

    function formatHost(host) {
      if (host === 'master') return 'master'
      return host.split('.')[0]
    }

    function openCheckDetails(check) {
      if (check.issues && check.issues.length > 0) {
        modalCheck.value = check
        showModal.value = true
      }
    }

    function closeModal() {
      showModal.value = false
      modalCheck.value = null
      activeTab.value = 0
    }

    function setActiveTab(index) {
      activeTab.value = index
    }

    function getErrorHostCount(check) {
      return check.issues ? check.issues.length : 0
    }

    function getCheckProgress(check, totalAgents) {
      const completed = check.completedHosts ? check.completedHosts.length : 0
      // For orchestrator checks, total is 1 (orchestrator only)
      // For agent checks, total is the number of all agents
      const total = check.isOrchestratorCheck ? 1 : totalAgents
      return { completed, total }
    }

    function getCheckProgressPercent(check, totalAgents) {
      const { completed, total } = getCheckProgress(check, totalAgents)
      if (total === 0) return 100
      return Math.round((completed / total) * 100)
    }

    return {
      hasResults,
      orchestratorResult,
      agentResults,
      aggregatedResults,
      displayResults,
      overallStatus,
      statusBadgeClass,
      getCheckStatusClass,
      getCheckIcon,
      runChecks,
      formatHost,
      showModal,
      modalCheck,
      openCheckDetails,
      closeModal,
      getErrorHostCount,
      getCheckProgress,
      getCheckProgressPercent,
      availableChecks,
      isLoadingChecks,
      activeTab,
      setActiveTab
    }
  },
  template: `
    <div class="card bg-base-100 shadow-md">
      <div class="card-body p-4">
        <div class="flex justify-between items-center mb-4">
          <h2 class="card-title text-lg">
            Warden Checks
            <span class="badge" :class="statusBadgeClass">{{ overallStatus }}</span>
            <span>{{ displayResults.completedAt }}</span>
          </h2>
          <button 
            class="btn btn-sm btn-primary" 
            @click="runChecks"
            :disabled="isLoading || overallStatus === 'running'"
          >
            <span v-if="isLoading || overallStatus === 'running'" class="loading loading-spinner loading-xs"></span>
            {{ isLoading || overallStatus === 'running' ? 'Running...' : 'Run Checks' }}
          </button>
        </div>

        <div v-if="isLoadingChecks" class="text-center py-4 opacity-50">
          Loading available checks...
        </div>
        <div v-else-if="displayResults.liveness.length === 0 && displayResults.safety.length === 0" class="text-center py-4 opacity-50">
          No warden check results yet. Click "Run Checks" to start.
        </div>
        <div v-else class="space-y-4">

          <!-- Liveness Checks (from Orchestrator) -->
          <div>
            <h3 class="font-bold text-sm mb-2 flex items-center gap-2">
              <span class="badge badge-sm badge-info">Liveness</span>
              <span class="text-xs opacity-70">{{ displayResults.liveness.length }} check(s)</span>
              <span v-if="!hasResults" class="text-xs opacity-50">(runs on orchestrator)</span>
              <span v-else class="text-xs opacity-50">(centralized)</span>
            </h3>
            <div class="overflow-x-auto">
              <table class="table table-xs w-full">
                <thead>
                  <tr>
                    <th>Check</th>
                    <th>Status</th>
                    <th>Details</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(check, idx) in displayResults.liveness" :key="'liveness-' + idx">
                    <td class="font-mono text-xs">{{ check.name }}</td>
                    <td>
                      <span
                        v-if="check.status === 'ok'"
                        class="badge badge-success badge-sm gap-1"
                      >
                        <span>✓</span> OK
                      </span>
                      <span
                        v-else-if="check.status === 'idle'"
                        class="badge badge-ghost badge-sm gap-1"
                      >
                        <span>?</span> Idle
                      </span>
                      <span
                        v-else-if="check.status === 'violation'"
                        class="badge badge-warning badge-sm gap-1"
                      >
                        <span>⚠</span> Violation
                      </span>
                      <span
                        v-else-if="check.status === 'error'"
                        class="badge badge-error badge-sm gap-1"
                      >
                        <span>✗</span> Error
                      </span>
                      <span v-else class="badge badge-ghost badge-sm gap-1">
                        <span>?</span> {{ check.status }}
                      </span>
                    </td>
                    <td class="text-xs">
                      <span v-if="check.violations && check.violations.length > 0" class="text-warning">
                        {{ check.violations.join('; ') }}
                      </span>
                      <span v-else-if="check.error_message" class="text-error">
                        {{ check.error_message }}
                      </span>
                      <span v-else class="opacity-50">OK</span>
                    </td>
                  </tr>
                  <tr v-if="displayResults.liveness.length === 0">
                    <td v-if="displayResults.status === 'error'" colspan="3" class="text-center opacity-50">Error</td>
                    <td v-else colspan="3" class="text-center opacity-50">No liveness checks</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

          <!-- Safety Checks (Aggregated) -->
          <div>
            <h3 class="font-bold text-sm mb-2 flex items-center gap-2">
              <span class="badge badge-sm badge-warning">Safety</span>
              <span class="text-xs opacity-70">{{ displayResults.safety.length }} check type(s)</span>
            </h3>
            <div class="overflow-x-auto">
              <table class="table table-xs w-full">
                <thead>
                  <tr>
                    <th>Check</th>
                    <th>Status</th>
                    <th>Progress</th>
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="(check, idx) in displayResults.safety" :key="'safety-' + idx">
                    <td class="font-mono text-xs">{{ check.name }}</td>
                    <td>
                      <span
                        v-if="check.status === 'ok'"
                        class="badge badge-success badge-sm gap-1"
                      >
                        <span>✓</span> OK
                      </span>
                      <span
                        v-else-if="check.status === 'idle'"
                        class="badge badge-ghost badge-sm gap-1"
                      >
                        <span>?</span> Idle
                      </span>
                      <span
                        v-else
                        class="badge badge-sm gap-1 cursor-pointer hover:opacity-80"
                        :class="check.status === 'error' ? 'badge-error' : 'badge-warning'"
                        @click="openCheckDetails(check)"
                        :title="'Click to see details'"
                      >
                        <span>{{ check.status === 'error' ? '✗' : '⚠' }}</span>
                        {{ check.status === 'error' ? 'Error' : 'Violation' }}
                        <span class="font-bold">({{ getErrorHostCount(check) }})</span>
                      </span>
                    </td>
                    <td class="text-xs">
                      <div v-if="hasResults" class="flex items-center gap-2">
                        <progress
                          class="progress progress-sm w-16"
                          :class="{
                            'progress-success': getCheckProgressPercent(check, displayResults.totalAgents) === 100,
                            'progress-info': getCheckProgressPercent(check, displayResults.totalAgents) < 100
                          }"
                          :value="getCheckProgress(check, displayResults.totalAgents).completed"
                          :max="getCheckProgress(check, displayResults.totalAgents).total"
                        ></progress>
                        <span class="opacity-70">{{ getCheckProgress(check, displayResults.totalAgents).completed }}/{{ getCheckProgress(check, displayResults.totalAgents).total }}</span>
                      </div>
                      <span v-else class="opacity-50">-</span>
                    </td>
                  </tr>
                  <tr v-if="displayResults.safety.length === 0">
                    <td colspan="3" class="text-center opacity-50">No safety checks</td>
                  </tr>
                </tbody>
              </table>
            </div>
          </div>

        </div>
      </div>

      <!-- Modal for Safety Check Details -->
      <div v-if="showModal" class="modal modal-open">
        <div class="modal-box w-11/12 max-w-5xl max-h-[90vh]">
          <h3 class="font-bold text-lg mb-4">
            {{ modalCheck?.name }} - Details
            <span
              class="badge ml-2"
              :class="modalCheck?.status === 'error' ? 'badge-error' : 'badge-warning'"
            >
              {{ modalCheck?.status }}
            </span>
            <span class="text-sm font-normal opacity-70 ml-2">
              ({{ modalCheck?.issues?.length || 0 }} host(s) with issues)
            </span>
          </h3>
          
          <!-- Tabs for hosts -->
          <div v-if="modalCheck?.issues && modalCheck.issues.length > 0" class="tabs tabs-boxed mb-4">
            <a
              v-for="(issue, idx) in modalCheck.issues"
              :key="idx"
              class="tab"
              :class="{ 'tab-active': activeTab === idx }"
              @click="setActiveTab(idx)"
            >
              <span class="badge badge-sm badge-outline mr-2">{{ formatHost(issue.host) }}</span>
              <span v-if="issue.violations" class="text-xs opacity-70">({{ issue.violations.length }})</span>
            </a>
          </div>
          
          <!-- Tab content -->
          <div v-if="modalCheck?.issues && modalCheck.issues.length > 0" class="overflow-y-auto max-h-[calc(90vh-14rem)]">
            <div
              v-for="(issue, idx) in modalCheck.issues"
              :key="idx"
              v-show="activeTab === idx"
              class="bg-base-200 p-4 rounded-lg"
            >
              <div class="font-bold text-sm mb-3 flex items-center gap-2">
                <span v-if="issue.affectedHosts && issue.affectedHosts.length > 0" class="text-xs opacity-70">
                  ({{ issue.affectedHosts.length }} host(s) affected: {{ issue.affectedHosts.map(h => formatHost(h)).join(', ') }})
                </span>
              </div>
              
              <div v-if="issue.violations && issue.violations.length > 0" class="text-sm">
                <div class="font-semibold mb-2">Violations ({{ issue.violations.length }}):</div>
                <div class="bg-base-300 p-3 rounded">
                  <div v-for="(v, j) in issue.violations" :key="j" class="font-mono text-xs mb-2 break-all whitespace-pre-wrap">
                    {{ v }}
                  </div>
                </div>
              </div>
              
              <div v-else-if="issue.error_message" class="text-error text-sm">
                <div class="font-semibold mb-2">Error:</div>
                <div class="bg-base-300 p-3 rounded font-mono text-xs break-all whitespace-pre-wrap">
                  {{ issue.error_message }}
                </div>
              </div>
            </div>
          </div>
          
          <div class="modal-action">
            <button class="btn btn-primary" @click="closeModal">Close</button>
          </div>
        </div>
        <div class="modal-backdrop" @click="closeModal"></div>
      </div>
    </div>
  `
}