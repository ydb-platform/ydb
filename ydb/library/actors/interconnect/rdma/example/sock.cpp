#include "sock.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>

#include <exception>

#include <util/stream/output.h>

int SockRead(int sockfd, char* buf, int len) {
    int n = 0;
    while (n < len) {
        int r = read(sockfd, buf + n, len - n);
        if (r <= 0) {
            Cerr << "Socket read error" << Endl;
            std::terminate();
            return r;
        }
        n += r;
    }
    return n;
}

int SockWrite(int sockfd, char* buf, int len) {
    int n = 0;
    while (n < len) {
        int r = write(sockfd, buf + n, len - n);
        if (r <= 0) {
            Cerr << "Socket write error" << Endl;
            std::terminate();
            return r;
        }
        n += r;
    }
    return n;
}

int SockBind(int port) {
    int sockfd;
    struct sockaddr_in6 servaddr;

    // Create an IPv6 socket
    sockfd = socket(AF_INET6, SOCK_STREAM, 0);
    if (sockfd < 0) {
        Cerr << "socket creation failed" << Endl;
        return -1;
    }

    // Allow dual-stack (IPv4 + IPv6)
    int opt = 0; // 0 means allow IPv4-mapped IPv6 addresses
    if (setsockopt(sockfd, IPPROTO_IPV6, IPV6_V6ONLY, &opt, sizeof(opt)) < 0) {
        Cerr << "setsockopt IPV6_V6ONLY failed" << Endl;
        close(sockfd);
        return -1;
    }

    // Zero out the server address structure
    memset(&servaddr, 0, sizeof(servaddr));

    // Set the address family to IPv6
    servaddr.sin6_family = AF_INET6;

    // Allow any IPv6 or IPv4-mapped address to bind
    servaddr.sin6_addr = in6addr_any;

    // Set the port number
    servaddr.sin6_port = htons(port);

    // Bind the socket to the specified port
    if (bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        Cerr << "socket bind failed" << Endl;
        close(sockfd);
        return -1;
    }

    // Start listening for incoming connections
    if (listen(sockfd, 5) < 0) {
        Cerr << "listen failed" << Endl;
        close(sockfd);
        return -1;
    }

    sockaddr_in6 peerAddr;
    socklen_t peerAddrLen = sizeof(peerAddr);
    return accept(sockfd, (struct sockaddr *)&peerAddr, &peerAddrLen);
}

int SockConnect(char* addr, int port) {
    int sockfd;
    struct sockaddr_in6 servaddr;

    // Create an IPv6 socket
    sockfd = socket(AF_INET6, SOCK_STREAM, 0);
    if (sockfd < 0) {
        Cerr << "socket creation failed" << Endl;
        return -1;
    }

    // Zero out the server address structure
    memset(&servaddr, 0, sizeof(servaddr));

    // Set the address family to IPv6
    servaddr.sin6_family = AF_INET6;

    // Set the port number
    servaddr.sin6_port = htons(port);

    // Convert IPv4 or IPv6 address from text to binary form
    if (inet_pton(AF_INET6, addr, &servaddr.sin6_addr) <= 0) {
        Cerr << "Invalid address/ Address not supported" << Endl;
        close(sockfd);
        return -1;
    }

    // Connect to the server
    if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        Cerr << "Connection failed" << Endl;
        close(sockfd);
        return -1;
    }

    return sockfd;
}


std::tuple<ibv_gid, ui32> ExchangeRdmaConnectionInfo(int sockfd, ibv_gid gid, ui32 qpNum) {
    SockWrite(sockfd, gid);
    SockWrite(sockfd, qpNum);

    ibv_gid remoteGid;
    ui32 remoteQpNum;
    SockRead(sockfd, remoteGid);
    SockRead(sockfd, remoteQpNum);
    return {remoteGid, remoteQpNum};
}

void SendRkey(int sockfd, int wrId, ui32 rkey, void* addr, ui32 size) {
    SockWrite(sockfd, ECommand::SendRkey);
    SockWrite(sockfd, wrId);
    SockWrite(sockfd, rkey);
    SockWrite(sockfd, addr);
    SockWrite(sockfd, size);
}

void RecvRkey(int sockfd, int& wrId, ui32& rkey, void*& addr, ui32& size) {
    SockRead(sockfd, wrId);
    SockRead(sockfd, rkey);
    SockRead(sockfd, addr);
    SockRead(sockfd, size);
}
