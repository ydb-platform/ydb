/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Intel Corporation
 */

#ifndef _RTE_SERVICE_H_
#define _RTE_SERVICE_H_

/**
 * @file
 *
 * Service functions
 *
 * The service functionality provided by this header allows a DPDK component
 * to indicate that it requires a function call in order for it to perform
 * its processing.
 *
 * An example usage of this functionality would be a component that registers
 * a service to perform a particular packet processing duty: for example the
 * eventdev software PMD. At startup the application requests all services
 * that have been registered, and the cores in the service-coremask run the
 * required services. The EAL removes these number of cores from the available
 * runtime cores, and dedicates them to performing service-core workloads. The
 * application has access to the remaining lcores as normal.
 */

#ifdef __cplusplus
extern "C" {
#endif

#include<stdio.h>
#include <stdint.h>
#include <sys/queue.h>

#include <rte_config.h>
#include <rte_lcore.h>

#define RTE_SERVICE_NAME_MAX 32

/* Capabilities of a service.
 *
 * Use the *rte_service_probe_capability* function to check if a service is
 * capable of a specific capability.
 */
/** When set, the service is capable of having multiple threads run it at the
 *  same time.
 */
#define RTE_SERVICE_CAP_MT_SAFE (1 << 0)

/**
 *  Return the number of services registered.
 *
 * The number of services registered can be passed to *rte_service_get_by_id*,
 * enabling the application to retrieve the specification of each service.
 *
 * @return The number of services registered.
 */
uint32_t rte_service_get_count(void);

/**
 * Return the id of a service by name.
 *
 * This function provides the id of the service using the service name as
 * lookup key. The service id is to be passed to other functions in the
 * rte_service_* API.
 *
 * Example usage:
 * @code
 *      uint32_t service_id;
 *      int32_t ret = rte_service_get_by_name("service_X", &service_id);
 *      if (ret) {
 *              // handle error
 *      }
 * @endcode
 *
 * @param name The name of the service to retrieve
 * @param[out] service_id A pointer to a uint32_t, to be filled in with the id.
 * @retval 0 Success. The service id is provided in *service_id*.
 * @retval -EINVAL Null *service_id* pointer provided
 * @retval -ENODEV No such service registered
 */
int32_t rte_service_get_by_name(const char *name, uint32_t *service_id);

/**
 * Return the name of the service.
 *
 * @return A pointer to the name of the service. The returned pointer remains
 *         in ownership of the service, and the application must not free it.
 */
const char *rte_service_get_name(uint32_t id);

/**
 * Check if a service has a specific capability.
 *
 * This function returns if *service* has implements *capability*.
 * See RTE_SERVICE_CAP_* defines for a list of valid capabilities.
 * @retval 1 Capability supported by this service instance
 * @retval 0 Capability not supported by this service instance
 */
int32_t rte_service_probe_capability(uint32_t id, uint32_t capability);

/**
 * Map or unmap a lcore to a service.
 *
 * Each core can be added or removed from running a specific service. This
 * function enables or disables *lcore* to run *service_id*.
 *
 * If multiple cores are enabled on a service, a lock is used to ensure that
 * only one core runs the service at a time. The exception to this is when
 * a service indicates that it is multi-thread safe by setting the capability
 * called RTE_SERVICE_CAP_MT_SAFE. With the multi-thread safe capability set,
 * the service function can be run on multiple threads at the same time.
 *
 * If the service is known to be mapped to a single lcore, setting the
 * capability of the service to RTE_SERVICE_CAP_MT_SAFE can achieve
 * better performance by avoiding the use of lock.
 *
 * @param service_id the service to apply the lcore to
 * @param lcore The lcore that will be mapped to service
 * @param enable Zero to unmap or disable the core, non-zero to enable
 *
 * @retval 0 lcore map updated successfully
 * @retval -EINVAL An invalid service or lcore was provided.
 */
int32_t rte_service_map_lcore_set(uint32_t service_id, uint32_t lcore,
		uint32_t enable);

/**
 * Retrieve the mapping of an lcore to a service.
 *
 * @param service_id the service to apply the lcore to
 * @param lcore The lcore that will be mapped to service
 *
 * @retval 1 lcore is mapped to service
 * @retval 0 lcore is not mapped to service
 * @retval -EINVAL An invalid service or lcore was provided.
 */
int32_t rte_service_map_lcore_get(uint32_t service_id, uint32_t lcore);

/**
 * Set the runstate of the service.
 *
 * Each service is either running or stopped. Setting a non-zero runstate
 * enables the service to run, while setting runstate zero disables it.
 *
 * @param id The id of the service
 * @param runstate The run state to apply to the service
 *
 * @retval 0 The service was successfully started
 * @retval -EINVAL Invalid service id
 */
int32_t rte_service_runstate_set(uint32_t id, uint32_t runstate);

/**
 * Get the runstate for the service with *id*. See *rte_service_runstate_set*
 * for details of runstates. A service can call this function to ensure that
 * the application has indicated that it will receive CPU cycles. Either a
 * service-core is mapped (default case), or the application has explicitly
 * disabled the check that a service-cores is mapped to the service and takes
 * responsibility to run the service manually using the available function
 * *rte_service_run_iter_on_app_lcore* to do so.
 *
 * @retval 1 Service is running
 * @retval 0 Service is stopped
 * @retval -EINVAL Invalid service id
 */
int32_t rte_service_runstate_get(uint32_t id);

/**
 * This function returns whether the service may be currently executing on
 * at least one lcore, or definitely is not. This function can be used to
 * determine if, after setting the service runstate to stopped, the service
 * is still executing a service lcore.
 *
 * Care must be taken if calling this function when the service runstate is
 * running, since the result of this function may be incorrect by the time the
 * function returns due to service cores running in parallel.
 *
 * @retval 1 Service may be running on one or more lcores
 * @retval 0 Service is not running on any lcore
 * @retval -EINVAL Invalid service id
 */
int32_t
rte_service_may_be_active(uint32_t id);

/**
 * Enable or disable the check for a service-core being mapped to the service.
 * An application can disable the check when takes the responsibility to run a
 * service itself using *rte_service_run_iter_on_app_lcore*.
 *
 * @param id The id of the service to set the check on
 * @param enable When zero, the check is disabled. Non-zero enables the check.
 *
 * @retval 0 Success
 * @retval -EINVAL Invalid service ID
 */
int32_t rte_service_set_runstate_mapped_check(uint32_t id, int32_t enable);

/**
 * This function runs a service callback from a non-service lcore.
 *
 * This function is designed to enable gradual porting to service cores, and
 * to enable unit tests to verify a service behaves as expected.
 *
 * When called, this function ensures that the service identified by *id* is
 * safe to run on this lcore. Multi-thread safe services are invoked even if
 * other cores are simultaneously running them as they are multi-thread safe.
 *
 * Multi-thread unsafe services are handled depending on the variable
 * *serialize_multithread_unsafe*:
 * - When set, the function will check if a service is already being invoked
 *   on another lcore, refusing to run it and returning -EBUSY.
 * - When zero, the application takes responsibility to ensure that the service
 *   indicated by *id* is not going to be invoked by another lcore. This setting
 *   avoids atomic operations, so is likely to be more performant.
 *
 * @param id The ID of the service to run
 * @param serialize_multithread_unsafe This parameter indicates to the service
 *           cores library if it is required to use atomics to serialize access
 *           to mult-thread unsafe services. As there is an overhead in using
 *           atomics, applications can choose to enable or disable this feature
 *
 * Note that any thread calling this function MUST be a DPDK EAL thread, as
 * the *rte_lcore_id* function is used to access internal data structures.
 *
 * @retval 0 Service was run on the calling thread successfully
 * @retval -EBUSY Another lcore is executing the service, and it is not a
 *         multi-thread safe service, so the service was not run on this lcore
 * @retval -ENOEXEC Service is not in a run-able state
 * @retval -EINVAL Invalid service id
 */
int32_t rte_service_run_iter_on_app_lcore(uint32_t id,
		uint32_t serialize_multithread_unsafe);

/**
 * Start a service core.
 *
 * Starting a core makes the core begin polling. Any services assigned to it
 * will be run as fast as possible. The application must ensure that the lcore
 * is in a launchable state: e.g. call *rte_eal_lcore_wait* on the lcore_id
 * before calling this function.
 *
 * @retval 0 Success
 * @retval -EINVAL Failed to start core. The *lcore_id* passed in is not
 *          currently assigned to be a service core.
 */
int32_t rte_service_lcore_start(uint32_t lcore_id);

/**
 * Stop a service core.
 *
 * Stopping a core makes the core become idle, but remains  assigned as a
 * service core. Note that the service lcore thread may not have returned from
 * the service it is running when this API returns.
 *
 * The *rte_service_lcore_may_be_active* API can be used to check if the
 * service lcore is * still active.
 *
 * @retval 0 Success
 * @retval -EINVAL Invalid *lcore_id* provided
 * @retval -EALREADY Already stopped core
 * @retval -EBUSY Failed to stop core, as it would cause a service to not
 *          be run, as this is the only core currently running the service.
 *          The application must stop the service first, and then stop the
 *          lcore.
 */
int32_t rte_service_lcore_stop(uint32_t lcore_id);

/**
 * Reports if a service lcore is currently running.
 *
 * This function returns if the core has finished service cores code, and has
 * returned to EAL control. If *rte_service_lcore_stop* has been called but
 * the lcore has not returned to EAL yet, it might be required to wait and call
 * this function again. The amount of time to wait before the core returns
 * depends on the duration of the services being run.
 *
 * @retval 0 Service thread is not active, and lcore has been returned to EAL.
 * @retval 1 Service thread is in the service core polling loop.
 * @retval -EINVAL Invalid *lcore_id* provided.
 */
__rte_experimental
int32_t rte_service_lcore_may_be_active(uint32_t lcore_id);

/**
 * Adds lcore to the list of service cores.
 *
 * This functions can be used at runtime in order to modify the service core
 * mask.
 *
 * @retval 0 Success
 * @retval -EBUSY lcore is busy, and not available for service core duty
 * @retval -EALREADY lcore is already added to the service core list
 * @retval -EINVAL Invalid lcore provided
 */
int32_t rte_service_lcore_add(uint32_t lcore);

/**
 * Removes lcore from the list of service cores.
 *
 * This can fail if the core is not stopped, see *rte_service_core_stop*.
 *
 * @retval 0 Success
 * @retval -EBUSY Lcore is not stopped, stop service core before removing.
 * @retval -EINVAL failed to add lcore to service core mask.
 */
int32_t rte_service_lcore_del(uint32_t lcore);

/**
 * Retrieve the number of service cores currently available.
 *
 * This function returns the integer count of service cores available. The
 * service core count can be used in mapping logic when creating mappings
 * from service cores to services.
 *
 * See *rte_service_lcore_list* for details on retrieving the lcore_id of each
 * service core.
 *
 * @return The number of service cores currently configured.
 */
int32_t rte_service_lcore_count(void);

/**
 * Resets all service core mappings. This does not remove the service cores
 * from duty, just unmaps all services / cores, and stops() the service cores.
 * The runstate of services is not modified.
 *
 * The cores that are stopped with this call, are in FINISHED state and
 * the application must take care of bringing them back to a launchable state:
 * e.g. call *rte_eal_lcore_wait* on the lcore_id.
 *
 * @retval 0 Success
 */
int32_t rte_service_lcore_reset_all(void);

/**
 * Enable or disable statistics collection for *service*.
 *
 * This function enables per core, per-service cycle count collection.
 * @param id The service to enable statistics gathering on.
 * @param enable Zero to disable statistics, non-zero to enable.
 * @retval 0 Success
 * @retval -EINVAL Invalid service pointer passed
 */
int32_t rte_service_set_stats_enable(uint32_t id, int32_t enable);

/**
 * Retrieve the list of currently enabled service cores.
 *
 * This function fills in an application supplied array, with each element
 * indicating the lcore_id of a service core.
 *
 * Adding and removing service cores can be performed using
 * *rte_service_lcore_add* and *rte_service_lcore_del*.
 * @param [out] array An array of at least *rte_service_lcore_count* items.
 *              If statically allocating the buffer, use RTE_MAX_LCORE.
 * @param [out] n The size of *array*.
 * @retval >=0 Number of service cores that have been populated in the array
 * @retval -ENOMEM The provided array is not large enough to fill in the
 *          service core list. No items have been populated, call this function
 *          with a size of at least *rte_service_core_count* items.
 */
int32_t rte_service_lcore_list(uint32_t array[], uint32_t n);

/**
 * Get the number of services running on the supplied lcore.
 *
 * @param lcore Id of the service core.
 * @retval >=0 Number of services registered to this core.
 * @retval -EINVAL Invalid lcore provided
 * @retval -ENOTSUP The provided lcore is not a service core.
 */
int32_t rte_service_lcore_count_services(uint32_t lcore);

/**
 * Dumps any information available about the service. When id is UINT32_MAX,
 * this function dumps info for all services.
 *
 * @retval 0 Statistics have been successfully dumped
 * @retval -EINVAL Invalid service id provided
 */
int32_t rte_service_dump(FILE *f, uint32_t id);

/**
 * Returns the number of cycles that this service has consumed
 */
#define RTE_SERVICE_ATTR_CYCLES 0

/**
 * Returns the count of invocations of this service function
 */
#define RTE_SERVICE_ATTR_CALL_COUNT 1

/**
 * Get an attribute from a service.
 *
 * @retval 0 Success, the attribute value has been written to *attr_value*.
 *         -EINVAL Invalid id, attr_id or attr_value was NULL.
 */
int32_t rte_service_attr_get(uint32_t id, uint32_t attr_id,
		uint64_t *attr_value);

/**
 * Reset all attribute values of a service.
 *
 * @param id The service to reset all statistics of
 * @retval 0 Successfully reset attributes
 *         -EINVAL Invalid service id provided
 */
int32_t rte_service_attr_reset_all(uint32_t id);

/**
 * Returns the number of times the service runner has looped.
 */
#define RTE_SERVICE_LCORE_ATTR_LOOPS 0

/**
 * Get an attribute from a service core.
 *
 * @param lcore Id of the service core.
 * @param attr_id Id of the attribute to be retrieved.
 * @param [out] attr_value Pointer to storage in which to write retrieved value.
 * @retval 0 Success, the attribute value has been written to *attr_value*.
 *         -EINVAL Invalid lcore, attr_id or attr_value was NULL.
 *         -ENOTSUP lcore is not a service core.
 */
int32_t
rte_service_lcore_attr_get(uint32_t lcore, uint32_t attr_id,
			   uint64_t *attr_value);

/**
 * Reset all attribute values of a service core.
 *
 * @param lcore The service core to reset all the statistics of
 * @retval 0 Successfully reset attributes
 *         -EINVAL Invalid service id provided
 *         -ENOTSUP lcore is not a service core.
 */
int32_t
rte_service_lcore_attr_reset_all(uint32_t lcore);

#ifdef __cplusplus
}
#endif


#endif /* _RTE_SERVICE_H_ */
