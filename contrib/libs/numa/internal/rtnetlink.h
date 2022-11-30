hidden int
rta_put_address(struct nlmsghdr *msg, int type, struct sockaddr *adr);
hidden struct rtattr *rta_get(struct nlmsghdr *m, struct rtattr *p, int offset);
hidden void *rta_put(struct nlmsghdr *m, int type, int len);
hidden int rtnetlink_request(struct nlmsghdr *msg, int buflen, struct sockaddr_nl *adr);
