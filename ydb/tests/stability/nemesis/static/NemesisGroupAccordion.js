import ProcessTypeGroup from './ProcessTypeGroup.js'

export default {
  components: {
    ProcessTypeGroup
  },
  props: {
    groupName: String,
    groupDescription: String,
    nemesisList: Array, // Array of {name, description}
    hosts: Object,
    processes: Object,
    scheduleStatus: Object, // Schedule status for all nemesis types
    processTypes: Array // All process types with their configurations
  },
  setup(props) {
    const { ref, computed } = Vue
    const isExpanded = ref(false)

    // Count running processes across all nemesis in this group
    const runningCount = computed(() => {
      let count = 0
      for (const nemesis of props.nemesisList) {
        for (const host in props.processes) {
          const hostProcs = props.processes[host] || []
          const running = hostProcs.filter(p => p.type === nemesis.name && p.status === 'running')
          count += running.length
        }
      }
      return count
    })

    // Count enabled schedules (computed from scheduleStatus prop)
    const enabledCount = computed(() => {
      let count = 0
      for (const nemesis of props.nemesisList) {
        const scheduleData = props.scheduleStatus[nemesis.name]
        if (scheduleData && scheduleData.enabled) {
          count++
        }
      }
      return count
    })

    function getProcessesByType(type) {
      const result = {}
      for (const host in props.processes) {
        result[host] = (props.processes[host] || []).filter(p => p.type === type)
      }
      return result
    }

    function toggleExpanded() {
      isExpanded.value = !isExpanded.value
    }

    return {
      isExpanded,
      runningCount,
      enabledCount,
      getProcessesByType,
      toggleExpanded
    }
  },
  template: `
    <div class="mb-4">
      <!-- Group Header (Accordion Toggle) -->
      <div 
        class="bg-base-200 rounded-lg p-3 cursor-pointer hover:bg-base-300 transition-colors"
        @click="toggleExpanded"
      >
        <div class="flex justify-between items-center">
          <div class="flex items-center gap-3">
            <!-- Expand/Collapse Icon -->
            <span class="text-lg transition-transform" :class="{ 'rotate-90': isExpanded }">▶</span>
            
            <!-- Group Name -->
            <h2 class="text-lg font-bold">{{ groupName }}</h2>
            
            <!-- Badges -->
            <div class="flex gap-2">
              <span class="badge badge-sm badge-outline">{{ nemesisList.length }} types</span>
              <span v-if="enabledCount > 0" class="badge badge-sm badge-success">{{ enabledCount }} scheduled</span>
              <span v-if="runningCount > 0" class="badge badge-sm badge-warning animate-pulse">{{ runningCount }} running</span>
            </div>
          </div>
          
          <!-- Group Description -->
          <span class="text-sm text-base-content/60">{{ groupDescription }}</span>
        </div>
      </div>
      
      <!-- Expanded Content -->
      <div v-show="isExpanded" class="mt-2 ml-4 border-l-2 border-base-300 pl-4">
        <process-type-group
          v-for="nemesis in nemesisList"
          :key="nemesis.name"
          :type="nemesis.name"
          :description="nemesis.description"
          :hosts="hosts"
          :processes="getProcessesByType(nemesis.name)"
          :schedule-status="scheduleStatus"
          :process-types="processTypes"
        ></process-type-group>
      </div>
    </div>
  `
}