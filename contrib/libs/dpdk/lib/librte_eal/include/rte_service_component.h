/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2017 Intel Corporation
 */

#ifndef _SERVICE_PRIVATE_H_
#define _SERVICE_PRIVATE_H_

/* This file specifies the internal service specification.
 * Include this file if you are writing a component that requires CPU cycles to
 * operate, and you wish to run the component using service cores
 */

#ifdef __cplusplus
extern "C" {
#endif

#include <rte_compat.h>
#include <rte_service.h>

/**
 * Signature of callback function to run a service.
 */
typedef int32_t (*rte_service_func)(void *args);

/**
 * The specification of a service.
 *
 * This struct contains metadata about the service itself, the callback
 * function to run one iteration of the service, a userdata pointer, flags etc.
 */
struct rte_service_spec {
	/** The name of the service. This should be used by the application to
	 * understand what purpose this service provides.
	 */
	char name[RTE_SERVICE_NAME_MAX];
	/** The callback to invoke to run one iteration of the service. */
	rte_service_func callback;
	/** The userdata pointer provided to the service callback. */
	void *callback_userdata;
	/** Flags to indicate the capabilities of this service. See defines in
	 * the public header file for values of RTE_SERVICE_CAP_*
	 */
	uint32_t capabilities;
	/** NUMA socket ID that this service is affinitized to */
	int socket_id;
};

/**
 * Register a new service.
 *
 * A service represents a component that requires CPU time periodically to
 * achieve its purpose.
 *
 * For example the eventdev SW PMD requires CPU cycles to perform its
 * scheduling. This can be achieved by registering it as a service, and the
 * application can then assign CPU resources to that service.
 *
 * Note that when a service component registers itself, it is not permitted to
 * add or remove service-core threads, or modify lcore-to-service mappings. The
 * only API that may be called by the service-component is
 * *rte_service_component_runstate_set*, which indicates that the service
 * component is ready to be executed.
 *
 * If the service is known to be mapped to a single lcore, setting the
 * capability of the service to RTE_SERVICE_CAP_MT_SAFE can achieve
 * better performance.
 *
 * @param spec The specification of the service to register
 * @param[out] service_id A pointer to a uint32_t, which will be filled in
 *             during registration of the service. It is set to the integers
 *             service number given to the service. This parameter may be NULL.
 * @retval 0 Successfully registered the service.
 *         -EINVAL Attempted to register an invalid service (eg, no callback
 *         set)
 */
int32_t rte_service_component_register(const struct rte_service_spec *spec,
		uint32_t *service_id);

/**
 * Unregister a service component.
 *
 * The service being removed must be stopped before calling this function.
 *
 * @retval 0 The service was successfully unregistered.
 * @retval -EBUSY The service is currently running, stop the service before
 *          calling unregister. No action has been taken.
 */
int32_t rte_service_component_unregister(uint32_t id);

/**
 * Private function to allow EAL to initialized default mappings.
 *
 * This function iterates all the services, and maps then to the available
 * cores. Based on the capabilities of the services, they are set to run on the
 * available cores in a round-robin manner.
 *
 * @retval 0 Success
 * @retval -ENOTSUP No service lcores in use
 * @retval -EINVAL Error while iterating over services
 * @retval -ENODEV Error in enabling service lcore on a service
 * @retval -ENOEXEC Error when starting services
 */
int32_t rte_service_start_with_defaults(void);

/**
 * Set the backend runstate of a component.
 *
 * This function allows services to be registered at startup, but not yet
 * enabled to run by default. When the service has been configured (via the
 * usual method; eg rte_eventdev_configure, the service can mark itself as
 * ready to run. The differentiation between backend runstate and
 * service_runstate is that the backend runstate is set by the service
 * component while the service runstate is reserved for application usage.
 *
 * @retval 0 Success
 */
int32_t rte_service_component_runstate_set(uint32_t id, uint32_t runstate);

/**
 * Initialize the service library.
 *
 * In order to use the service library, it must be initialized. EAL initializes
 * the library at startup.
 *
 * @retval 0 Success
 * @retval -EALREADY Service library is already initialized
 */
int32_t rte_service_init(void);

/**
 * @internal Free up the memory that has been initialized.
 * This routine is to be invoked prior to process termination.
 *
 * @retval None
 */
void rte_service_finalize(void);

#ifdef __cplusplus
}
#endif

#endif /* _SERVICE_PRIVATE_H_ */
