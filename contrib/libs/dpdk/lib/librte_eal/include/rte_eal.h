/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2010-2018 Intel Corporation
 */

#ifndef _RTE_EAL_H_
#define _RTE_EAL_H_

/**
 * @file
 *
 * EAL Configuration API
 */

#include <stdint.h>
#include <sched.h>
#include <time.h>

#include <rte_config.h>
#include <rte_compat.h>
#include <rte_per_lcore.h>
#include <rte_bus.h>
#include <rte_uuid.h>

#include <rte_pci_dev_feature_defs.h>

#ifdef __cplusplus
extern "C" {
#endif

#define RTE_MAGIC 19820526 /**< Magic number written by the main partition when ready. */

/* Maximum thread_name length. */
#define RTE_MAX_THREAD_NAME_LEN 16

/**
 * The type of process in a linux, multi-process setup
 */
enum rte_proc_type_t {
	RTE_PROC_AUTO = -1,   /* allow auto-detection of primary/secondary */
	RTE_PROC_PRIMARY = 0, /* set to zero, so primary is the default */
	RTE_PROC_SECONDARY,

	RTE_PROC_INVALID
};

/**
 * Get the process type in a multi-process setup
 *
 * @return
 *   The process type
 */
enum rte_proc_type_t rte_eal_process_type(void);

/**
 * Request iopl privilege for all RPL.
 *
 * This function should be called by pmds which need access to ioports.

 * @return
 *   - On success, returns 0.
 *   - On failure, returns -1.
 */
int rte_eal_iopl_init(void);

/**
 * Initialize the Environment Abstraction Layer (EAL).
 *
 * This function is to be executed on the MAIN lcore only, as soon
 * as possible in the application's main() function.
 * It puts the WORKER lcores in the WAIT state.
 *
 * @param argc
 *   A non-negative value.  If it is greater than 0, the array members
 *   for argv[0] through argv[argc] (non-inclusive) shall contain pointers
 *   to strings.
 * @param argv
 *   An array of strings.  The contents of the array, as well as the strings
 *   which are pointed to by the array, may be modified by this function.
 * @return
 *   - On success, the number of parsed arguments, which is greater or
 *     equal to zero. After the call to rte_eal_init(),
 *     all arguments argv[x] with x < ret may have been modified by this
 *     function call and should not be further interpreted by the
 *     application.  The EAL does not take any ownership of the memory used
 *     for either the argv array, or its members.
 *   - On failure, -1 and rte_errno is set to a value indicating the cause
 *     for failure.  In some instances, the application will need to be
 *     restarted as part of clearing the issue.
 *
 *   Error codes returned via rte_errno:
 *     EACCES indicates a permissions issue.
 *
 *     EAGAIN indicates either a bus or system resource was not available,
 *            setup may be attempted again.
 *
 *     EALREADY indicates that the rte_eal_init function has already been
 *              called, and cannot be called again.
 *
 *     EFAULT indicates the tailq configuration name was not found in
 *            memory configuration.
 *
 *     EINVAL indicates invalid parameters were passed as argv/argc.
 *
 *     ENOMEM indicates failure likely caused by an out-of-memory condition.
 *
 *     ENODEV indicates memory setup issues.
 *
 *     ENOTSUP indicates that the EAL cannot initialize on this system.
 *
 *     EPROTO indicates that the PCI bus is either not present, or is not
 *            readable by the eal.
 *
 *     ENOEXEC indicates that a service core failed to launch successfully.
 */
int rte_eal_init(int argc, char **argv);

/**
 * Clean up the Environment Abstraction Layer (EAL)
 *
 * This function must be called to release any internal resources that EAL has
 * allocated during rte_eal_init(). After this call, no DPDK function calls may
 * be made. It is expected that common usage of this function is to call it
 * just before terminating the process.
 *
 * @return
 *  - 0 Successfully released all internal EAL resources.
 *  - -EFAULT There was an error in releasing all resources.
 */
int rte_eal_cleanup(void);

/**
 * Check if a primary process is currently alive
 *
 * This function returns true when a primary process is currently
 * active.
 *
 * @param config_file_path
 *   The config_file_path argument provided should point at the location
 *   that the primary process will create its config file. If NULL, the default
 *   config file path is used.
 *
 * @return
 *  - If alive, returns 1.
 *  - If dead, returns 0.
 */
int rte_eal_primary_proc_alive(const char *config_file_path);

/**
 * Disable multiprocess.
 *
 * This function can be called to indicate that multiprocess won't be used for
 * the rest of the application life.
 *
 * @return
 *   - true if called from a primary process that had no secondary processes
 *     attached,
 *   - false, otherwise.
 */
__rte_experimental
bool rte_mp_disable(void);

#define RTE_MP_MAX_FD_NUM	8    /* The max amount of fds */
#define RTE_MP_MAX_NAME_LEN	64   /* The max length of action name */
#define RTE_MP_MAX_PARAM_LEN	256  /* The max length of param */
struct rte_mp_msg {
	char name[RTE_MP_MAX_NAME_LEN];
	int len_param;
	int num_fds;
	uint8_t param[RTE_MP_MAX_PARAM_LEN];
	int fds[RTE_MP_MAX_FD_NUM];
};

struct rte_mp_reply {
	int nb_sent;
	int nb_received;
	struct rte_mp_msg *msgs; /* caller to free */
};

/**
 * Action function typedef used by other components.
 *
 * As we create  socket channel for primary/secondary communication, use
 * this function typedef to register action for coming messages.
 *
 * @note When handling IPC request callbacks, the reply must be sent even in
 *   cases of error handling. Simply returning success or failure will *not*
 *   send a response to the requestor.
 *   Implementation of error signalling mechanism is up to the application.
 *
 * @note No memory allocations should take place inside the callback.
 */
typedef int (*rte_mp_t)(const struct rte_mp_msg *msg, const void *peer);

/**
 * Asynchronous reply function typedef used by other components.
 *
 * As we create socket channel for primary/secondary communication, use
 * this function typedef to register action for coming responses to asynchronous
 * requests.
 *
 * @note When handling IPC request callbacks, the reply must be sent even in
 *   cases of error handling. Simply returning success or failure will *not*
 *   send a response to the requestor.
 *   Implementation of error signalling mechanism is up to the application.
 *
 * @note No memory allocations should take place inside the callback.
 */
typedef int (*rte_mp_async_reply_t)(const struct rte_mp_msg *request,
		const struct rte_mp_reply *reply);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Register an action function for primary/secondary communication.
 *
 * Call this function to register an action, if the calling component wants
 * to response the messages from the corresponding component in its primary
 * process or secondary processes.
 *
 * @note IPC may be unsupported in certain circumstances, so caller should check
 *    for ENOTSUP error.
 *
 * @param name
 *   The name argument plays as the nonredundant key to find the action.
 *
 * @param action
 *   The action argument is the function pointer to the action function.
 *
 * @return
 *  - 0 on success.
 *  - (<0) on failure.
 */
__rte_experimental
int
rte_mp_action_register(const char *name, rte_mp_t action);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Unregister an action function for primary/secondary communication.
 *
 * Call this function to unregister an action  if the calling component does
 * not want to response the messages from the corresponding component in its
 * primary process or secondary processes.
 *
 * @note IPC may be unsupported in certain circumstances, so caller should check
 *    for ENOTSUP error.
 *
 * @param name
 *   The name argument plays as the nonredundant key to find the action.
 *
 */
__rte_experimental
void
rte_mp_action_unregister(const char *name);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Send a message to the peer process.
 *
 * This function will send a message which will be responded by the action
 * identified by name in the peer process.
 *
 * @param msg
 *   The msg argument contains the customized message.
 *
 * @return
 *  - On success, return 0.
 *  - On failure, return -1, and the reason will be stored in rte_errno.
 */
__rte_experimental
int
rte_mp_sendmsg(struct rte_mp_msg *msg);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Send a request to the peer process and expect a reply.
 *
 * This function sends a request message to the peer process, and will
 * block until receiving reply message from the peer process.
 *
 * @note The caller is responsible to free reply->replies.
 *
 * @note This API must not be used inside memory-related or IPC callbacks, and
 *   no memory allocations should take place inside such callback.
 *
 * @note IPC may be unsupported in certain circumstances, so caller should check
 *    for ENOTSUP error.
 *
 * @param req
 *   The req argument contains the customized request message.
 *
 * @param reply
 *   The reply argument will be for storing all the replied messages;
 *   the caller is responsible for free reply->msgs.
 *
 * @param ts
 *   The ts argument specifies how long we can wait for the peer(s) to reply.
 *
 * @return
 *  - On success, return 0.
 *  - On failure, return -1, and the reason will be stored in rte_errno.
 */
__rte_experimental
int
rte_mp_request_sync(struct rte_mp_msg *req, struct rte_mp_reply *reply,
	       const struct timespec *ts);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Send a request to the peer process and expect a reply in a separate callback.
 *
 * This function sends a request message to the peer process, and will not
 * block. Instead, reply will be received in a separate callback.
 *
 * @note IPC may be unsupported in certain circumstances, so caller should check
 *    for ENOTSUP error.
 *
 * @param req
 *   The req argument contains the customized request message.
 *
 * @param ts
 *   The ts argument specifies how long we can wait for the peer(s) to reply.
 *
 * @param clb
 *   The callback to trigger when all responses for this request have arrived.
 *
 * @return
 *  - On success, return 0.
 *  - On failure, return -1, and the reason will be stored in rte_errno.
 */
__rte_experimental
int
rte_mp_request_async(struct rte_mp_msg *req, const struct timespec *ts,
		rte_mp_async_reply_t clb);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Send a reply to the peer process.
 *
 * This function will send a reply message in response to a request message
 * received previously.
 *
 * @note When handling IPC request callbacks, the reply must be sent even in
 *   cases of error handling. Simply returning success or failure will *not*
 *   send a response to the requestor.
 *   Implementation of error signalling mechanism is up to the application.
 *
 * @param msg
 *   The msg argument contains the customized message.
 *
 * @param peer
 *   The peer argument is the pointer to the peer socket path.
 *
 * @return
 *  - On success, return 0.
 *  - On failure, return -1, and the reason will be stored in rte_errno.
 */
__rte_experimental
int
rte_mp_reply(struct rte_mp_msg *msg, const char *peer);

/**
 * Usage function typedef used by the application usage function.
 *
 * Use this function typedef to define and call rte_set_application_usage_hook()
 * routine.
 */
typedef void	(*rte_usage_hook_t)(const char * prgname);

/**
 * Add application usage routine callout from the eal_usage() routine.
 *
 * This function allows the application to include its usage message
 * in the EAL system usage message. The routine rte_set_application_usage_hook()
 * needs to be called before the rte_eal_init() routine in the application.
 *
 * This routine is optional for the application and will behave as if the set
 * routine was never called as the default behavior.
 *
 * @param usage_func
 *   The func argument is a function pointer to the application usage routine.
 *   Called function is defined using rte_usage_hook_t typedef, which is of
 *   the form void rte_usage_func(const char * prgname).
 *
 *   Calling this routine with a NULL value will reset the usage hook routine and
 *   return the current value, which could be NULL.
 * @return
 *   - Returns the current value of the rte_application_usage pointer to allow
 *     the caller to daisy chain the usage routines if needing more then one.
 */
rte_usage_hook_t
rte_set_application_usage_hook(rte_usage_hook_t usage_func);

/**
 * Whether EAL is using huge pages (disabled by --no-huge option).
 * The no-huge mode is not compatible with all drivers or features.
 *
 * @return
 *   Nonzero if hugepages are enabled.
 */
int rte_eal_has_hugepages(void);

/**
 * Whether EAL is using PCI bus.
 * Disabled by --no-pci option.
 *
 * @return
 *   Nonzero if the PCI bus is enabled.
 */
int rte_eal_has_pci(void);

/**
 * Whether the EAL was asked to create UIO device.
 *
 * @return
 *   Nonzero if true.
 */
int rte_eal_create_uio_dev(void);

/**
 * The user-configured vfio interrupt mode.
 *
 * @return
 *   Interrupt mode configured with the command line,
 *   RTE_INTR_MODE_NONE by default.
 */
enum rte_intr_mode rte_eal_vfio_intr_mode(void);

/**
 * @warning
 * @b EXPERIMENTAL: this API may change without prior notice
 *
 * Copy the user-configured vfio VF token.
 *
 * @param vf_token
 *   vfio VF token configured with the command line is copied
 *   into this parameter, zero uuid by default.
 */
__rte_experimental
void rte_eal_vfio_get_vf_token(rte_uuid_t vf_token);

/**
 * A wrap API for syscall gettid.
 *
 * @return
 *   On success, returns the thread ID of calling process.
 *   It is always successful.
 */
int rte_sys_gettid(void);

RTE_DECLARE_PER_LCORE(int, _thread_id);

/**
 * Get system unique thread id.
 *
 * @return
 *   On success, returns the thread ID of calling process.
 *   It is always successful.
 */
static inline int rte_gettid(void)
{
	if (RTE_PER_LCORE(_thread_id) == -1)
		RTE_PER_LCORE(_thread_id) = rte_sys_gettid();
	return RTE_PER_LCORE(_thread_id);
}

/**
 * Get the iova mode
 *
 * @return
 *   enum rte_iova_mode value.
 */
enum rte_iova_mode rte_eal_iova_mode(void);

/**
 * Get user provided pool ops name for mbuf
 *
 * @return
 *   returns user provided pool ops name.
 */
const char *
rte_eal_mbuf_user_pool_ops(void);

/**
 * Get the runtime directory of DPDK
 *
 * @return
 *  The runtime directory path of DPDK
 */
const char *
rte_eal_get_runtime_dir(void);

#ifdef __cplusplus
}
#endif

#endif /* _RTE_EAL_H_ */
