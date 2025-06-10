#include "sock.h"
#include "rdma.h"

#include <ydb/library/actors/interconnect/rdma/link_manager.h>
#include <ydb/library/actors/interconnect/rdma/ctx.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>

#include <ydb/library/actors/interconnect/rdma/ibdrv/include/infiniband/verbs.h>

#include <util/generic/scope.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

#include <thread>
#include <mutex>



static constexpr ui32 INFLYGHT = 10;
static constexpr ui32 BUF_SIZE = 4096;

struct WorkItem {
    std::mutex Mtx;
    NInterconnect::NRdma::TMemRegionPtr Mr;
};

int handleOneCompletion(std::vector<WorkItem>& workItems, int sockfd, ibv_wc& wc) {
    if (wc.wr_id >= INFLYGHT) {
        Cerr << "Invalid wr_id: " << wc.wr_id << Endl;
        return -1;
    }
    auto& item = workItems[wc.wr_id];
    std::lock_guard<std::mutex> lock(item.Mtx);
    char* buf = (char*)item.Mr->GetAddr();
    if (buf[0] != (ui8)wc.wr_id) {
        Cerr << "Invalid wr_id: " << (ui8)wc.wr_id << " != " << buf[0] << Endl;
        return -1;
    }
    SockWrite(sockfd, ECommand::Done);
    SockWrite(sockfd, wc.wr_id);
    return 0;
}


void completionThread(TContext& ctx, int sockfd, std::vector<WorkItem>& workItems) {
    const ui32 totalCount = 20;
    ui32 count = 0;

    const int wcBatchSize = 1024;
    std::vector<ibv_wc> wcs(wcBatchSize);
    while (true) {
        int numComp = ibv_poll_cq(ctx.Cq, wcBatchSize, &wcs.front());
        if (numComp < 0) {
            Cerr << "ibv_poll_cq failed: " << strerror(errno) << " " << errno << Endl;
            return;
        }
        if (numComp > 0) {
            Cerr << "Receive " << numComp << " completions" << Endl;
            for (int i = 0; i < numComp; ++i) {
                ibv_wc& wc = wcs[i];
                if (wc.status != IBV_WC_SUCCESS) {
                    Cerr << "ibv_poll_cq failed: " << ibv_wc_status_str(wc.status) << " " << (int)wc.status << "QP: " << wc.qp_num << " " << Endl;
                    return;
                }
                handleOneCompletion(workItems, sockfd, wc);
            }

            count += numComp;
            if (count >= totalCount) {
                Cerr << "Send finish" << Endl;
                SockWrite(sockfd, ECommand::Finish);
                return;
            }
        }
    }
}

void serverLogic(int sockfd, TContext& ctx) {
    std::vector<WorkItem> workItems(INFLYGHT);

    std::thread ct(completionThread, std::ref(ctx), sockfd, std::ref(workItems));
    Y_DEFER { ct.join(); };

    while (true) {
        ECommand cmd;
        SockRead(sockfd, cmd);
        switch (cmd) {
            case ECommand::SendRkey: {
                int wrId;
                ui32 rkey;
                void* addr;
                ui32 size;
                RecvRkey(sockfd, wrId, rkey, addr, size);
                std::lock_guard<std::mutex> lock(workItems[wrId].Mtx);
                workItems[wrId].Mr = std::move(ctx.MemPool->Alloc(size));
                SendRdmaReadWr(ctx, wrId, workItems[wrId].Mr->GetAddr(), workItems[wrId].Mr->GetLKey(ctx.DeviceIndex), addr, rkey, size);
                break;
            }
            case ECommand::Finish:
                SockWrite(sockfd, ECommand::Finish);
                return;
            default:
                Cerr << "Invalid command" << Endl;
        }
    }
}

void clientLogic(int sockfd, TContext& ctx) {
    std::vector<WorkItem> workItems(INFLYGHT);
    for (ui32 wrId = 0; wrId < INFLYGHT; ++wrId) {
        Cerr << "Send Rkey: " << wrId << Endl;
        workItems[wrId].Mr = std::move(ctx.MemPool->Alloc(BUF_SIZE));
        void* addr = workItems[wrId].Mr->GetAddr();
        ui32 rkey = workItems[wrId].Mr->GetRKey(ctx.DeviceIndex);
        ((char*)addr)[0] = (ui8)wrId;
        SendRkey(sockfd, wrId, rkey, addr, BUF_SIZE);
    }

    while (true) {
        ECommand cmd = ECommand::_End;
        SockRead(sockfd, cmd);
        switch (cmd) {
            case ECommand::Done: {
                ui64 wrId;
                SockRead(sockfd, wrId);
                Cerr << "Receive Done: " << wrId << Endl;

                workItems[wrId].Mr = ctx.MemPool->Alloc(BUF_SIZE);
                void* addr = workItems[wrId].Mr->GetAddr();
                ui32 rkey = workItems[wrId].Mr->GetRKey(ctx.DeviceIndex);
                ((char*)addr)[0] = (ui8)wrId;
                SendRkey(sockfd, wrId, rkey, addr, BUF_SIZE);
                break;
            }
            case ECommand::Finish: {
                Cout << "Receive Finish" << Endl;
                workItems.clear();
                SockWrite(sockfd, ECommand::Finish);
                return;
            }
            default:
                Cerr << "Invalid command" << Endl;
        }
    }
}

int main(int argc, char *argv[]) {
    int sockfd;
    bool isServer = false;
    if (argc == 2) {
        isServer = true;
        sockfd = SockBind(atoi(argv[1]));
        if (sockfd < 0) {
            Cerr << "bind failed" << Endl;
            return 1;
        }
    } else {
        sockfd = SockConnect(argv[2], atoi(argv[1]));
        if (sockfd < 0) {
            Cerr << "connect failed" << Endl;
            return 1;
        }
    }

    NInterconnect::NRdma::TRdmaCtx* rdmaCtx = NInterconnect::NRdma::NLinkMgr::GetCtx(sockfd);
    if (!rdmaCtx) {
        Cerr << "Failed to get RDMA context" << Endl;
        return 1;
    }
    char str[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &(rdmaCtx->GetGid()), str, INET6_ADDRSTRLEN);
    fprintf(stderr, "%s\n", str);

    TContext ctx(rdmaCtx, NInterconnect::NRdma::CreateDummyMemPool());

    ctx.InitQp();

    auto [dstGid, dstQpNum] = ExchangeRdmaConnectionInfo(sockfd, rdmaCtx->GetGid(), ctx.Qp->qp_num);

    ctx.MoveQpToRTS(dstGid, dstQpNum);
    Cerr << "QP state " <<(int)GetQpState(ctx.Qp) << Endl;

    if (isServer) {
        serverLogic(sockfd, ctx);
    } else {
        clientLogic(sockfd, ctx);
    }

    return 0;
}
