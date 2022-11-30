#include "rte_config.h"
/* SPDX-License-Identifier: BSD-3-Clause
 * Copyright(c) 2018 Intel Corporation
 */
#include <string.h>

#include <rte_eal.h>
#include <rte_errno.h>
#include <rte_alarm.h>
#include <rte_string_fns.h>
#include <rte_devargs.h>

#include "hotplug_mp.h"
#include "eal_private.h"

#define MP_TIMEOUT_S 5 /**< 5 seconds timeouts */

struct mp_reply_bundle {
	struct rte_mp_msg msg;
	void *peer;
};

static int cmp_dev_name(const struct rte_device *dev, const void *_name)
{
	const char *name = _name;

	return strcmp(dev->name, name);
}

/**
 * Secondary to primary request.
 * start from function eal_dev_hotplug_request_to_primary.
 *
 * device attach on secondary:
 * a) secondary send sync request to the primary.
 * b) primary receive the request and attach the new device if
 *    failed goto i).
 * c) primary forward attach sync request to all secondary.
 * d) secondary receive the request and attach the device and send a reply.
 * e) primary check the reply if all success goes to j).
 * f) primary send attach rollback sync request to all secondary.
 * g) secondary receive the request and detach the device and send a reply.
 * h) primary receive the reply and detach device as rollback action.
 * i) send attach fail to secondary as a reply of step a), goto k).
 * j) send attach success to secondary as a reply of step a).
 * k) secondary receive reply and return.
 *
 * device detach on secondary:
 * a) secondary send sync request to the primary.
 * b) primary send detach sync request to all secondary.
 * c) secondary detach the device and send a reply.
 * d) primary check the reply if all success goes to g).
 * e) primary send detach rollback sync request to all secondary.
 * f) secondary receive the request and attach back device. goto h).
 * g) primary detach the device if success goto i), else goto e).
 * h) primary send detach fail to secondary as a reply of step a), goto j).
 * i) primary send detach success to secondary as a reply of step a).
 * j) secondary receive reply and return.
 */

static int
send_response_to_secondary(const struct eal_dev_mp_req *req,
			int result,
			const void *peer)
{
	struct rte_mp_msg mp_resp;
	struct eal_dev_mp_req *resp =
		(struct eal_dev_mp_req *)mp_resp.param;
	int ret;

	memset(&mp_resp, 0, sizeof(mp_resp));
	mp_resp.len_param = sizeof(*resp);
	strlcpy(mp_resp.name, EAL_DEV_MP_ACTION_REQUEST, sizeof(mp_resp.name));
	memcpy(resp, req, sizeof(*req));
	resp->result = result;

	ret = rte_mp_reply(&mp_resp, peer);
	if (ret != 0)
		RTE_LOG(ERR, EAL, "failed to send response to secondary\n");

	return ret;
}

static void
__handle_secondary_request(void *param)
{
	struct mp_reply_bundle *bundle = param;
		const struct rte_mp_msg *msg = &bundle->msg;
	const struct eal_dev_mp_req *req =
		(const struct eal_dev_mp_req *)msg->param;
	struct eal_dev_mp_req tmp_req;
	struct rte_devargs da;
	struct rte_device *dev;
	struct rte_bus *bus;
	int ret = 0;

	tmp_req = *req;

	if (req->t == EAL_DEV_REQ_TYPE_ATTACH) {
		ret = local_dev_probe(req->devargs, &dev);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Failed to hotplug add device on primary\n");
			if (ret != -EEXIST)
				goto finish;
		}
		ret = eal_dev_hotplug_request_to_secondary(&tmp_req);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Failed to send hotplug request to secondary\n");
			ret = -ENOMSG;
			goto rollback;
		}
		if (tmp_req.result != 0) {
			ret = tmp_req.result;
			RTE_LOG(ERR, EAL, "Failed to hotplug add device on secondary\n");
			if (ret != -EEXIST)
				goto rollback;
		}
	} else if (req->t == EAL_DEV_REQ_TYPE_DETACH) {
		ret = rte_devargs_parse(&da, req->devargs);
		if (ret != 0)
			goto finish;
		free(da.args); /* we don't need those */
		da.args = NULL;

		ret = eal_dev_hotplug_request_to_secondary(&tmp_req);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Failed to send hotplug request to secondary\n");
			ret = -ENOMSG;
			goto rollback;
		}

		bus = rte_bus_find_by_name(da.bus->name);
		if (bus == NULL) {
			RTE_LOG(ERR, EAL, "Cannot find bus (%s)\n", da.bus->name);
			ret = -ENOENT;
			goto finish;
		}

		dev = bus->find_device(NULL, cmp_dev_name, da.name);
		if (dev == NULL) {
			RTE_LOG(ERR, EAL, "Cannot find plugged device (%s)\n", da.name);
			ret = -ENOENT;
			goto finish;
		}

		if (tmp_req.result != 0) {
			RTE_LOG(ERR, EAL, "Failed to hotplug remove device on secondary\n");
			ret = tmp_req.result;
			if (ret != -ENOENT)
				goto rollback;
		}

		ret = local_dev_remove(dev);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Failed to hotplug remove device on primary\n");
			if (ret != -ENOENT)
				goto rollback;
		}
	} else {
		RTE_LOG(ERR, EAL, "unsupported secondary to primary request\n");
		ret = -ENOTSUP;
	}
	goto finish;

rollback:
	if (req->t == EAL_DEV_REQ_TYPE_ATTACH) {
		tmp_req.t = EAL_DEV_REQ_TYPE_ATTACH_ROLLBACK;
		eal_dev_hotplug_request_to_secondary(&tmp_req);
		local_dev_remove(dev);
	} else {
		tmp_req.t = EAL_DEV_REQ_TYPE_DETACH_ROLLBACK;
		eal_dev_hotplug_request_to_secondary(&tmp_req);
	}

finish:
	ret = send_response_to_secondary(&tmp_req, ret, bundle->peer);
	if (ret)
		RTE_LOG(ERR, EAL, "failed to send response to secondary\n");

	free(bundle->peer);
	free(bundle);
}

static int
handle_secondary_request(const struct rte_mp_msg *msg, const void *peer)
{
	struct mp_reply_bundle *bundle;
	const struct eal_dev_mp_req *req =
		(const struct eal_dev_mp_req *)msg->param;
	int ret = 0;

	bundle = malloc(sizeof(*bundle));
	if (bundle == NULL) {
		RTE_LOG(ERR, EAL, "not enough memory\n");
		return send_response_to_secondary(req, -ENOMEM, peer);
	}

	bundle->msg = *msg;
	/**
	 * We need to send reply on interrupt thread, but peer can't be
	 * parsed directly, so this is a temporal hack, need to be fixed
	 * when it is ready.
	 */
	bundle->peer = strdup(peer);
	if (bundle->peer == NULL) {
		free(bundle);
		RTE_LOG(ERR, EAL, "not enough memory\n");
		return send_response_to_secondary(req, -ENOMEM, peer);
	}

	/**
	 * We are at IPC callback thread, sync IPC is not allowed due to
	 * dead lock, so we delegate the task to interrupt thread.
	 */
	ret = rte_eal_alarm_set(1, __handle_secondary_request, bundle);
	if (ret != 0) {
		RTE_LOG(ERR, EAL, "failed to add mp task\n");
		free(bundle->peer);
		free(bundle);
		return send_response_to_secondary(req, ret, peer);
	}
	return 0;
}

static void __handle_primary_request(void *param)
{
	struct mp_reply_bundle *bundle = param;
	struct rte_mp_msg *msg = &bundle->msg;
	const struct eal_dev_mp_req *req =
		(const struct eal_dev_mp_req *)msg->param;
	struct rte_mp_msg mp_resp;
	struct eal_dev_mp_req *resp =
		(struct eal_dev_mp_req *)mp_resp.param;
	struct rte_devargs *da;
	struct rte_device *dev;
	struct rte_bus *bus;
	int ret = 0;

	memset(&mp_resp, 0, sizeof(mp_resp));

	switch (req->t) {
	case EAL_DEV_REQ_TYPE_ATTACH:
	case EAL_DEV_REQ_TYPE_DETACH_ROLLBACK:
		ret = local_dev_probe(req->devargs, &dev);
		break;
	case EAL_DEV_REQ_TYPE_DETACH:
	case EAL_DEV_REQ_TYPE_ATTACH_ROLLBACK:
		da = calloc(1, sizeof(*da));
		if (da == NULL) {
			ret = -ENOMEM;
			break;
		}

		ret = rte_devargs_parse(da, req->devargs);
		if (ret != 0)
			goto quit;

		bus = rte_bus_find_by_name(da->bus->name);
		if (bus == NULL) {
			RTE_LOG(ERR, EAL, "Cannot find bus (%s)\n", da->bus->name);
			ret = -ENOENT;
			goto quit;
		}

		dev = bus->find_device(NULL, cmp_dev_name, da->name);
		if (dev == NULL) {
			RTE_LOG(ERR, EAL, "Cannot find plugged device (%s)\n", da->name);
			ret = -ENOENT;
			goto quit;
		}

		if (!rte_dev_is_probed(dev)) {
			if (req->t == EAL_DEV_REQ_TYPE_ATTACH_ROLLBACK) {
				/**
				 * Don't fail the rollback just because there's
				 * nothing to do.
				 */
				ret = 0;
			} else
				ret = -ENODEV;

			goto quit;
		}

		ret = local_dev_remove(dev);
quit:
		free(da->args);
		free(da);
		break;
	default:
		ret = -EINVAL;
	}

	strlcpy(mp_resp.name, EAL_DEV_MP_ACTION_REQUEST, sizeof(mp_resp.name));
	mp_resp.len_param = sizeof(*req);
	memcpy(resp, req, sizeof(*resp));
	resp->result = ret;
	if (rte_mp_reply(&mp_resp, bundle->peer) < 0)
		RTE_LOG(ERR, EAL, "failed to send reply to primary request\n");

	free(bundle->peer);
	free(bundle);
}

static int
handle_primary_request(const struct rte_mp_msg *msg, const void *peer)
{
	struct rte_mp_msg mp_resp;
	const struct eal_dev_mp_req *req =
		(const struct eal_dev_mp_req *)msg->param;
	struct eal_dev_mp_req *resp =
		(struct eal_dev_mp_req *)mp_resp.param;
	struct mp_reply_bundle *bundle;
	int ret = 0;

	memset(&mp_resp, 0, sizeof(mp_resp));
	strlcpy(mp_resp.name, EAL_DEV_MP_ACTION_REQUEST, sizeof(mp_resp.name));
	mp_resp.len_param = sizeof(*req);
	memcpy(resp, req, sizeof(*resp));

	bundle = calloc(1, sizeof(*bundle));
	if (bundle == NULL) {
		RTE_LOG(ERR, EAL, "not enough memory\n");
		resp->result = -ENOMEM;
		ret = rte_mp_reply(&mp_resp, peer);
		if (ret)
			RTE_LOG(ERR, EAL, "failed to send reply to primary request\n");
		return ret;
	}

	bundle->msg = *msg;
	/**
	 * We need to send reply on interrupt thread, but peer can't be
	 * parsed directly, so this is a temporal hack, need to be fixed
	 * when it is ready.
	 */
	bundle->peer = (void *)strdup(peer);
	if (bundle->peer == NULL) {
		RTE_LOG(ERR, EAL, "not enough memory\n");
		free(bundle);
		resp->result = -ENOMEM;
		ret = rte_mp_reply(&mp_resp, peer);
		if (ret)
			RTE_LOG(ERR, EAL, "failed to send reply to primary request\n");
		return ret;
	}

	/**
	 * We are at IPC callback thread, sync IPC is not allowed due to
	 * dead lock, so we delegate the task to interrupt thread.
	 */
	ret = rte_eal_alarm_set(1, __handle_primary_request, bundle);
	if (ret != 0) {
		free(bundle->peer);
		free(bundle);
		resp->result = ret;
		ret = rte_mp_reply(&mp_resp, peer);
		if  (ret != 0) {
			RTE_LOG(ERR, EAL, "failed to send reply to primary request\n");
			return ret;
		}
	}
	return 0;
}

int eal_dev_hotplug_request_to_primary(struct eal_dev_mp_req *req)
{
	struct rte_mp_msg mp_req;
	struct rte_mp_reply mp_reply;
	struct timespec ts = {.tv_sec = MP_TIMEOUT_S, .tv_nsec = 0};
	struct eal_dev_mp_req *resp;
	int ret;

	memset(&mp_req, 0, sizeof(mp_req));
	memcpy(mp_req.param, req, sizeof(*req));
	mp_req.len_param = sizeof(*req);
	strlcpy(mp_req.name, EAL_DEV_MP_ACTION_REQUEST, sizeof(mp_req.name));

	ret = rte_mp_request_sync(&mp_req, &mp_reply, &ts);
	if (ret || mp_reply.nb_received != 1) {
		RTE_LOG(ERR, EAL, "Cannot send request to primary\n");
		if (!ret)
			return -1;
		return ret;
	}

	resp = (struct eal_dev_mp_req *)mp_reply.msgs[0].param;
	req->result = resp->result;

	free(mp_reply.msgs);
	return ret;
}

int eal_dev_hotplug_request_to_secondary(struct eal_dev_mp_req *req)
{
	struct rte_mp_msg mp_req;
	struct rte_mp_reply mp_reply;
	struct timespec ts = {.tv_sec = MP_TIMEOUT_S, .tv_nsec = 0};
	int ret;
	int i;

	memset(&mp_req, 0, sizeof(mp_req));
	memcpy(mp_req.param, req, sizeof(*req));
	mp_req.len_param = sizeof(*req);
	strlcpy(mp_req.name, EAL_DEV_MP_ACTION_REQUEST, sizeof(mp_req.name));

	ret = rte_mp_request_sync(&mp_req, &mp_reply, &ts);
	if (ret != 0) {
		/* if IPC is not supported, behave as if the call succeeded */
		if (rte_errno != ENOTSUP)
			RTE_LOG(ERR, EAL, "rte_mp_request_sync failed\n");
		else
			ret = 0;
		return ret;
	}

	if (mp_reply.nb_sent != mp_reply.nb_received) {
		RTE_LOG(ERR, EAL, "not all secondary reply\n");
		free(mp_reply.msgs);
		return -1;
	}

	req->result = 0;
	for (i = 0; i < mp_reply.nb_received; i++) {
		struct eal_dev_mp_req *resp =
			(struct eal_dev_mp_req *)mp_reply.msgs[i].param;
		if (resp->result != 0) {
			if (req->t == EAL_DEV_REQ_TYPE_ATTACH &&
				resp->result == -EEXIST)
				continue;
			if (req->t == EAL_DEV_REQ_TYPE_DETACH &&
				resp->result == -ENOENT)
				continue;
			req->result = resp->result;
		}
	}

	free(mp_reply.msgs);
	return 0;
}

int eal_mp_dev_hotplug_init(void)
{
	int ret;

	if (rte_eal_process_type() == RTE_PROC_PRIMARY) {
		ret = rte_mp_action_register(EAL_DEV_MP_ACTION_REQUEST,
					handle_secondary_request);
		/* primary is allowed to not support IPC */
		if (ret != 0 && rte_errno != ENOTSUP) {
			RTE_LOG(ERR, EAL, "Couldn't register '%s' action\n",
				EAL_DEV_MP_ACTION_REQUEST);
			return ret;
		}
	} else {
		ret = rte_mp_action_register(EAL_DEV_MP_ACTION_REQUEST,
					handle_primary_request);
		if (ret != 0) {
			RTE_LOG(ERR, EAL, "Couldn't register '%s' action\n",
				EAL_DEV_MP_ACTION_REQUEST);
			return ret;
		}
	}

	return 0;
}
