/* Simple LPGLed rtnetlink library */
#include <sys/socket.h>
#include <linux/rtnetlink.h>
#include <linux/netlink.h>
#include <netinet/in.h>
#include <errno.h>
#include <unistd.h>
#define hidden __attribute__((visibility("hidden")))
#include "rtnetlink.h"

hidden void *rta_put(struct nlmsghdr *m, int type, int len)
{
	struct rtattr *rta = (void *)m + NLMSG_ALIGN(m->nlmsg_len);
	int rtalen = RTA_LENGTH(len);

	rta->rta_type = type;
	rta->rta_len = rtalen;
	m->nlmsg_len = NLMSG_ALIGN(m->nlmsg_len) + RTA_ALIGN(rtalen);
	return RTA_DATA(rta);
}

hidden struct rtattr *rta_get(struct nlmsghdr *m, struct rtattr *p, int offset)
{
	struct rtattr *rta;

	if (p) {
		rta = RTA_NEXT(p, m->nlmsg_len);
		if (!RTA_OK(rta, m->nlmsg_len))
			return NULL;
	} else {
		rta = (void *)m + NLMSG_ALIGN(offset);
	}
	return rta;
}

hidden int
rta_put_address(struct nlmsghdr *msg, int type, struct sockaddr *adr)
{
	switch (adr->sa_family) {
	case AF_INET: {
		struct in_addr *i = rta_put(msg, type, 4);
		*i = ((struct sockaddr_in *)adr)->sin_addr;
		break;
	}
	case AF_INET6: {
		struct in6_addr *i6 = rta_put(msg, type, 16);
		*i6 = ((struct sockaddr_in6 *)adr)->sin6_addr;
		break;
	}
	default:
		return -1;
	}
	return 0;
}

/* Assumes no truncation. Make the buffer large enough. */
hidden int
rtnetlink_request(struct nlmsghdr *msg, int buflen, struct sockaddr_nl *adr)
{
	int rsk;
	int n;
	int e;

	/* Use a private socket to avoid having to keep state
	   for a sequence number. */
	rsk = socket(PF_NETLINK, SOCK_RAW, NETLINK_ROUTE);
	if (rsk < 0)
		return -1;
	n = sendto(rsk, msg, msg->nlmsg_len, 0, (struct sockaddr *)adr,
		   sizeof(struct sockaddr_nl));
	if (n >= 0) {
		socklen_t adrlen = sizeof(struct sockaddr_nl);
		n = recvfrom(rsk, msg, buflen, 0, (struct sockaddr *)adr,
			     &adrlen);
	}
	e = errno;
	close(rsk);
	errno = e;
	if (n < 0)
		return -1;
	/* Assume we only get a single reply back. This is (hopefully?)
	   safe because it's a single use socket. */
	if (msg->nlmsg_type == NLMSG_ERROR) {
		struct nlmsgerr *err = NLMSG_DATA(msg);
		errno = -err->error;
		return -1;
	}
	return 0;
}
