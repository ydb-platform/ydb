export default {
  props: {
    host_data: Object,
    host: String,
    healthcheck: Object
  },
  template: `
    <tr>
      <td class="font-mono text-sm"><a :href="'http://' + host + ':8765/monitoring/cluster/nodes'" target="_blank">{{ host }}</a></td>
      <td>
        <div class="badge badge-sm" :class="{
          'badge-success': host_data.status === 'ok',
          'badge-error': host_data.status != 'ok'
        }">{{ host_data.status }}</div>
      </td>
      <td>
        <div v-if="healthcheck" class="badge badge-sm" :class="{
          'badge-success': healthcheck.self_check_result === 'GOOD',
          'badge-warning': healthcheck.self_check_result === 'DEGRADED',
          'badge-neutral': healthcheck.self_check_result === 'HC_RESULT_ERROR',
          'badge-error': !['GOOD', 'DEGRADED', 'HC_RESULT_ERROR'].includes(healthcheck.self_check_result)
        }">{{ healthcheck.self_check_result }}</div>
        <div v-else class="badge badge-sm badge-ghost">N/A</div>
      </td>
    </tr>
  `
}