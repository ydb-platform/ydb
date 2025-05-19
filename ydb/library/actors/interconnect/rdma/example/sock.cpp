#include "sock.h"

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>
#include <string.h>

#include <util/stream/output.h>

int SockRead(int sockfd, char* buf, int len) {
    int n = 0;
    while (n < len) {
        int r = read(sockfd, buf + n, len - n);
        if (r <= 0) {
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
            return r;
        }
        n += r;
    }
    return n;
}

int SockBind(int port) {
    int sockfd;
    struct sockaddr_in servaddr;

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        Cerr << "socket creation failed" << Endl;
        return -1;
    }

    // Zero out the server address structure
    memset(&servaddr, 0, sizeof(servaddr));

    // Set the address family to IPv4
    servaddr.sin_family = AF_INET;

    // Set the port number
    servaddr.sin_port = htons(port);

    // Bind the socket to the specified port
    if (bind(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        Cerr << "socket creation failed" << Endl;
        close(sockfd);
        return -1;
    }

    listen(sockfd, 5);

    sockaddr peerAddr;
    ui32 peerAddrLen = sizeof(peerAddr);
    return accept(sockfd, &peerAddr, &peerAddrLen);
}

int SockConnect(char* addr, int port) {
    int sockfd;
    struct sockaddr_in servaddr;

    // Create socket
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        Cerr << "socket creation failed" << Endl;
        return -1;
    }

    // Zero out the server address structure
    memset(&servaddr, 0, sizeof(servaddr));

    // Set the address family to IPv4
    servaddr.sin_family = AF_INET;

    // Set the port number
    servaddr.sin_port = htons(port);

    // Convert IPv4 and IPv6 addresses from text to binary form
    if (inet_pton(AF_INET, addr, &servaddr.sin_addr) <= 0) {
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


std::tuple<ibv_gid_entry, ui32, ui32> ExchangeRdmaConnectionInfo(int sockfd, ibv_gid_entry entry, ui32 qpNum, ui32 lid) {
    SockWrite(sockfd, entry);
    SockWrite(sockfd, qpNum);
    SockWrite(sockfd, lid);

    ibv_gid_entry remoteEntry;
    ui32 remoteQpNum;
    ui32 remoteLid;
    SockRead(sockfd, remoteEntry);
    SockRead(sockfd, remoteQpNum);
    SockRead(sockfd, remoteLid);
    return {remoteEntry, remoteQpNum, remoteLid};
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
