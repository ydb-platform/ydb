/**
 * Scheduler state, failure budget, recovery probe and problems
 * (GET /api/scheduler + GET /api/problems). This drives scheduled chaos; the per-type toggles in
 * the accordion belong to the legacy loop.
 */
export default {
  props: {
    scheduler: Object, // GET /api/scheduler payload
    problems: Object,  // GET /api/problems payload
    isBusy: Boolean
  },
  emits: ['start', 'stop'],
  setup(props, { emit }) {
    const { computed, ref } = Vue

    const showTypes = ref(false)
    const showProbe = ref(false)
    const showProblems = ref(true)

    const available = computed(() => props.scheduler?.available === true)
    const running = computed(() => props.scheduler?.running === true)

    const state = computed(() => {
      if (!available.value) return 'unavailable'
      return running.value ? 'running' : 'stopped'
    })

    const stateBadgeClass = computed(() => {
      switch (state.value) {
        case 'running': return 'badge-success'
        case 'stopped': return 'badge-ghost'
        default: return 'badge-error'
      }
    })

    const enabledTypes = computed(() => props.scheduler?.enabled_types || [])
    const budget = computed(() => props.scheduler?.failure_budget || {})
    const probe = computed(() => props.scheduler?.recovery_probe || null)
    const probeFaults = computed(() => probe.value?.faults || [])

    const intervalText = computed(() => {
      const base = props.scheduler?.base_interval
      const jitter = props.scheduler?.jitter
      if (base == null) return '-'
      if (!jitter) return `${base}s`
      return `${base}s ±${Math.round(jitter * 100)}%`
    })

    const impairedDomains = computed(() => budget.value.impaired_racks || [])

    const slotsText = computed(() => {
      const used = budget.value.impaired_slots
      const total = budget.value.total_slots
      const max = budget.value.max_slots
      if (used == null) return '-'
      if (!max) return `${used} (unbounded)`
      if (!total) return `${used} / ${max} (budget)`
      const pct = Math.round((used / total) * 100)
      return `${used} / ${total} (${pct}%; budget ${max})`
    })

    const problemList = computed(() => props.problems?.problems || [])
    const problemsByKind = computed(() => props.problems?.by_kind || {})

    function problemBadgeClass(kind) {
      // stuck_fault / recovery_probe_blind → error badge.
      return (kind === 'stuck_fault' || kind === 'recovery_probe_blind')
        ? 'badge-error' : 'badge-warning'
    }

    function probePhaseBadgeClass(phase, stuck) {
      if (stuck) return 'badge-error'
      return phase === 'confirm' ? 'badge-warning' : 'badge-ghost'
    }

    function shortHost(host) {
      if (!host) return '-'
      const bare = String(host).split('.')[0]
      return bare || host
    }

    return {
      available,
      running,
      state,
      stateBadgeClass,
      enabledTypes,
      budget,
      probe,
      probeFaults,
      intervalText,
      impairedDomains,
      slotsText,
      problemList,
      problemsByKind,
      problemBadgeClass,
      probePhaseBadgeClass,
      shortHost,
      showTypes,
      showProbe,
      showProblems,
      start: () => emit('start'),
      stop: () => emit('stop')
    }
  },
  template: `
    <div class="card bg-base-100 shadow-md">
      <div class="card-body p-4">
        <div class="flex justify-between items-center">
          <h2 class="card-title text-lg">
            Nemesis Scheduler
            <span class="badge" :class="stateBadgeClass">{{ state }}</span>
          </h2>
          <div class="flex gap-2">
            <button class="btn btn-xs btn-primary" :disabled="!available || running || isBusy" @click="start">
              Start
            </button>
            <button class="btn btn-xs" :disabled="!available || !running || isBusy" @click="stop">
              Stop
            </button>
          </div>
        </div>

        <div v-if="!available" class="text-sm opacity-60 pt-2">
          Scheduler not initialized — orchestrator startup did not complete.
        </div>

        <div v-else class="space-y-3 pt-2">
          <!-- Profile -->
          <div class="grid grid-cols-3 gap-2 text-xs">
            <div>
              <div class="opacity-60">Interval</div>
              <div class="font-mono">{{ intervalText }}</div>
            </div>
            <div>
              <div class="opacity-60"
                   title="Per-tick burst fuse for budgeted faults — the failure budget is the real limit; tablet (bypass) chaos is capped separately">
                Fuse / tick
              </div>
              <div class="font-mono">
                {{ scheduler.max_per_tick ?? '-' }}
                <span class="opacity-60">+{{ scheduler.max_bypass_per_tick ?? 1 }} bypass</span>
              </div>
            </div>
            <div>
              <div class="opacity-60">Types</div>
              <div class="font-mono cursor-pointer link link-hover" @click="showTypes = !showTypes">
                {{ enabledTypes.length }}
              </div>
            </div>
          </div>
          <div v-if="showTypes" class="flex flex-wrap gap-1">
            <span v-for="t in enabledTypes" :key="t" class="badge badge-outline badge-xs font-mono">{{ t }}</span>
            <span v-if="enabledTypes.length === 0" class="text-xs opacity-50">no types enabled</span>
          </div>

          <!-- Failure budget -->
          <div>
            <h3 class="font-bold text-sm mb-1 flex items-center gap-2">
              Failure budget
              <span class="badge badge-sm badge-ghost font-mono">{{ budget.erasure || 'unknown' }}</span>
              <span v-if="probe && probe.blind" class="badge badge-sm badge-error"
                    title="No fresh healthcheck data: budget releases and stuck detection are paused">
                probe blind
              </span>
            </h3>
            <div class="grid grid-cols-3 gap-2 text-xs">
              <div>
                <div class="opacity-60">Impaired domains</div>
                <div class="font-mono">{{ impairedDomains.length }}</div>
              </div>
              <div>
                <div class="opacity-60"
                     title="Dynamic nodes currently impaired vs total in the cluster; budget = max allowed down at once (30% of total)">
                  Slots down
                </div>
                <div class="font-mono">{{ slotsText }}</div>
              </div>
              <div>
                <div class="opacity-60">Probe tracked / confirm / stuck</div>
                <div class="font-mono cursor-pointer link link-hover"
                     :class="probe && (probe.stuck || probe.blind) ? 'text-error' : ''"
                     @click="showProbe = !showProbe">
                  {{ probe ? probe.tracked : '-' }} / {{ probe ? (probe.confirming ?? 0) : '-' }} / {{ probe ? probe.stuck : '-' }}
                </div>
              </div>
            </div>
            <div v-if="impairedDomains.length > 0" class="flex flex-wrap gap-1 pt-1">
              <span v-for="d in impairedDomains" :key="d" class="badge badge-warning badge-xs font-mono">{{ d }}</span>
            </div>
            <div v-if="showProbe" class="pt-1 space-y-1">
              <div v-if="probeFaults.length === 0" class="text-xs opacity-50">no faults tracked</div>
              <div v-for="f in probeFaults" :key="f.identity_key"
                   class="flex flex-wrap items-center gap-1 text-xs">
                <span class="badge badge-outline badge-xs font-mono">{{ f.nemesis_type }}</span>
                <span class="font-mono opacity-80" :title="f.identity_key">{{ shortHost(f.host) }}</span>
                <span class="badge badge-xs font-mono"
                      :class="probePhaseBadgeClass(f.phase, f.stuck)"
                      :title="f.phase === 'confirm'
                        ? 'extract dispatched, waiting for healthcheck confirm'
                        : (f.toggle ? 'holding until auto-extract' : 'waiting for recovery predicate')">
                  {{ f.stuck ? 'stuck' : f.phase }}
                </span>
                <span class="opacity-50 font-mono">{{ Math.round(f.held_sec) }}s</span>
              </div>
            </div>
          </div>

          <!-- Problems -->
          <div>
            <h3 class="font-bold text-sm mb-1 flex items-center gap-2 cursor-pointer"
                @click="showProblems = !showProblems">
              Problems
              <span class="badge badge-sm" :class="problemList.length ? 'badge-error' : 'badge-success'">
                {{ problemList.length }}
              </span>
              <span v-for="(count, kind) in problemsByKind" :key="kind"
                    class="badge badge-xs" :class="problemBadgeClass(kind)">
                {{ kind }}: {{ count }}
              </span>
            </h3>
            <div v-if="showProblems">
              <div v-if="problemList.length === 0" class="text-xs opacity-50">
                No problems — guard and probe are doing their job.
              </div>
              <div v-else class="overflow-x-auto">
                <table class="table table-xs w-full">
                  <thead>
                    <tr>
                      <th>Kind</th>
                      <th>Target</th>
                      <th>What happened</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr v-for="(p, idx) in problemList" :key="idx">
                      <td>
                        <span class="badge badge-xs" :class="problemBadgeClass(p.kind)">{{ p.kind }}</span>
                        <span v-if="p.details && p.details.phase"
                              class="badge badge-xs badge-ghost font-mono"
                              :title="p.details.phase === 'confirm'
                                ? 'extract dispatched, waiting for the healthcheck confirm'
                                : 'waiting for the recovery predicate'">
                          {{ p.details.phase }}
                        </span>
                        <span v-if="p.count > 1" class="text-xs opacity-70">x{{ p.count }}</span>
                      </td>
                      <td class="font-mono text-xs break-all">{{ p.target || '-' }}</td>
                      <td class="text-xs">{{ p.summary }}</td>
                    </tr>
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  `
}
