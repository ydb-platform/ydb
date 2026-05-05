
export default {
  props:{
    process_data: Object
  },
  template: `
    <div class="card w-full bg-base-100 shadow-xl mb-4">
      <div class="card-body">
        <div class="flex justify-between items-center">
          <h2 class="card-title">
            Process #{{ process_data.id }}
            <div class="badge badge-outline">{{ process_data.type }}</div>
            <div class="badge" :class="{
              'badge-success': process_data.status === 'finished',
              'badge-error': process_data.status === 'failed' || process_data.status === 'error',
              'badge-warning': process_data.status === 'running'
            }">{{ process_data.status }}</div>
          </h2>
          <div v-if="process_data.ret_code !== null" class="badge badge-ghost">
            Exit Code: {{ process_data.ret_code }}
          </div>
        </div>

        <div>
          <h3 class="font-bold text-sm mb-1">Logs</h3>
          <pre class="bg-black text-green-400 p-2 rounded h-40 overflow-auto text-xs">{{ process_data.logs }}</pre>
        </div>
      </div>
    </div>
  `
}