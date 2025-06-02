#pragma once

#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/stream/output.h>


int SockBind(int port);
int SockConnect(char* addr, int port);

int SockRead(int sockfd, char* buf, int len);
int SockWrite(int sockfd, char* buf, int len);

template<class T>
int SockWrite(int sockfd, T obj) {
    return SockWrite(sockfd, reinterpret_cast<char*>(&obj), sizeof(T));
}
template<class T>
int SockRead(int sockfd, T& obj) {
    return SockRead(sockfd, reinterpret_cast<char*>(&obj), sizeof(T));
}

// ----------------------------------------------------------

enum class ECommand: ui8 {
    SendRkey,
    Done,
    Finish,

    _End = 0xFF,
};
std::tuple<ibv_gid, ui32> ExchangeRdmaConnectionInfo(int sockfd, ibv_gid gid, ui32 qpNum);
void SendRkey(int sockfd, int wrId, ui32 rkey, void* addr, ui32 size);
void RecvRkey(int sockfd, int& wrId, ui32& rkey, void*& addr, ui32& size);
