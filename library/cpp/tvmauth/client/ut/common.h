#pragma once

#include <library/cpp/tvmauth/client/logger.h>
#include <library/cpp/tvmauth/client/misc/disk_cache.h>
#include <library/cpp/tvmauth/client/misc/roles/entities_index.h>

#include <library/cpp/tvmauth/unittest.h>

#include <library/cpp/cgiparam/cgiparam.h> 
#include <library/cpp/testing/mock_server/server.h>
#include <library/cpp/testing/unittest/env.h> 
#include <library/cpp/testing/unittest/tests_data.h> 

#include <util/stream/str.h>
#include <util/system/fs.h>

class TLogger: public NTvmAuth::ILogger {
public:
    void Log(int lvl, const TString& msg) override {
        Cout << TInstant::Now() << " lvl=" << lvl << " msg: " << msg << "\n";
        Stream << lvl << ": " << msg << Endl;
    }

    TStringStream Stream;
};

static inline TString GetFilePath(const char* name) {
    return ArcadiaSourceRoot() + "/library/cpp/tvmauth/client/ut/files/" + name;
}

static inline TString GetCachePath(const TString& dir = {}) {
    if (dir) {
        Y_ENSURE(NFs::MakeDirectoryRecursive("./" + dir));
    }

    auto wr = [](const TString& p, const TStringBuf body) {
        NTvmAuth::TDiskWriter w(p);
        Y_ENSURE(w.Write(body, TInstant::ParseIso8601("2050-01-01T00:00:00.000000Z")));
    };
    wr("./" + dir + "/public_keys", NTvmAuth::NUnittest::TVMKNIFE_PUBLIC_KEYS);
    wr("./" + dir + "/service_tickets",
       R"({
   "19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"},
   "213" : { "ticket" : "service_ticket_2"},
   "234" : { "error" : "Dst is not found" },
   "185" : { "ticket" : "service_ticket_3"}
}	100500)");

    return "./" + dir;
}

static const TString AUTH_TOKEN = "strong_token";
static const TString META = R"(
{
"bb_env" : "ProdYaTeam",
"tenants" : [
    {
        "self": {
            "alias" : "me",
            "client_id": 100500
        },
        "dsts" : [
            {
                "alias" : "bbox",
                "client_id": 242
            },
            {
                "alias" : "pass_likers",
                "client_id": 11
            }
        ]
    },
    {
        "self": {
            "alias" : "push-client",
            "client_id": 100501
        },
        "dsts" : [
            {
                "alias" : "pass_likers",
                "client_id": 100502
            }
        ]
    },
    {
        "self": {
            "alias" : "multi_names_for_dst",
            "client_id": 100599
        },
        "dsts" : [
            {
                "alias" : "pass_likers",
                "client_id": 100502
            },
            {
                "alias" : "pass_haters",
                "client_id": 100502
            }
        ]
    },
    {
        "self": {
            "alias" : "something_else",
            "client_id": 100503
        },
        "dsts" : [
        ]
    }
]
})";

static const TString TICKETS_ME =
    R"({
        "pass_likers": {
            "ticket": "3:serv:CBAQ__________9_IgYIlJEGEAs:T-apeMNWFc_vHPQ3iLaZv9NjG-hf5-i23O4AhRu1M68ryN3FU5qvyqTSSiPbtJdFP6EE41QQBzEs59dHn9DRkqQNwwKf1is00Oewwj2XKO0uHukuzd9XxZnro7MfjPswsjWufxX28rmJtlfSXwAtyKt8TI5yKJnMeBPQ0m5R3k8",
            "tvm_id": 11
        },
        "bbox": {
            "ticket": "3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc",
            "tvm_id": 242
        }
    })";

static const TString SERVICE_TICKET_PC = "3:serv:CBAQ__________9_IggIlpEGEJaRBg:BAxaQJCdK4eFuJ6i_egqPwvJgWtlh0enDQRPr84Nx2phZ_8QtxKAUCwEa7KOU_jVvIBQIC5-ETTl2vjBt7UyygF8frdK4ab6zJoWj4n07np6vbmWd385l8KvzztLt4QkBrPiE7U46dK3pL0U8tfBkSXE8rvUIsl3RvvgSNH2J3c";
static const TString TICKETS_PC =
    R"({
        "pass_likers": {
            "ticket": "3:serv:CBAQ__________9_IggIlpEGEJaRBg:BAxaQJCdK4eFuJ6i_egqPwvJgWtlh0enDQRPr84Nx2phZ_8QtxKAUCwEa7KOU_jVvIBQIC5-ETTl2vjBt7UyygF8frdK4ab6zJoWj4n07np6vbmWd385l8KvzztLt4QkBrPiE7U46dK3pL0U8tfBkSXE8rvUIsl3RvvgSNH2J3c",
            "tvm_id": 100502
        }
    })";

static const TString TICKETS_MANY_DSTS =
    R"({
        "pass_likers": {
            "ticket": "3:serv:CBAQ__________9_IggI95EGEJaRBg:D0MOLDhKQyI-OhC0ON9gYukz2hOctUipu1yXsvkw6NRuLhcBfvGayyUqF4ILrqepjz9GtPWIR_wO6oLSW35Z0YaFn60QWp5tG6IcAnr80lm_OnLHJt4kmEoLtGg1V0aWBT0YyouzGB2-QFNOVO86G7sYzU8FC6-V3Iyc4X7XTNc",
            "tvm_id": 100502
        },
        "who_are_you??": {
            "ticket": "kek",
            "tvm_id": 100503
        },
        "pass_haters": {
            "ticket": "3:serv:CBAQ__________9_IggI95EGEJaRBg:D0MOLDhKQyI-OhC0ON9gYukz2hOctUipu1yXsvkw6NRuLhcBfvGayyUqF4ILrqepjz9GtPWIR_wO6oLSW35Z0YaFn60QWp5tG6IcAnr80lm_OnLHJt4kmEoLtGg1V0aWBT0YyouzGB2-QFNOVO86G7sYzU8FC6-V3Iyc4X7XTNc",
            "tvm_id": 100502
        }
    })";

static const TString TICKETS_SE = R"({})";

static const TInstant BIRTHTIME = TInstant::Seconds(14380887840);
class TTvmTool: public TRequestReplier {
public:
    TString Meta;
    HttpCodes Code;
    TInstant Birthtime;

    TTvmTool()
        : Meta(META)
        , Code(HTTP_OK)
        , Birthtime(BIRTHTIME)
    {
    }

    bool DoReply(const TReplyParams& params) override {
        const TParsedHttpFull http(params.Input.FirstLine());
        if (http.Path == "/tvm/ping") {
            THttpResponse resp(HTTP_OK);
            resp.SetContent("OK");
            resp.OutTo(params.Output);
            return true;
        }

        auto it = std::find_if(params.Input.Headers().begin(),
                               params.Input.Headers().end(),
                               [](const THttpInputHeader& h) { return h.Name() == "Authorization"; });
        if (it == params.Input.Headers().end() || it->Value() != AUTH_TOKEN) {
            THttpResponse resp(HTTP_UNAUTHORIZED);
            resp.SetContent("pong");
            resp.OutTo(params.Output);
            return true;
        }

        THttpResponse resp(Code);
        if (http.Path == "/tvm/keys") {
            resp.SetContent(NTvmAuth::NUnittest::TVMKNIFE_PUBLIC_KEYS);
        } else if (http.Path == "/tvm/tickets") {
            TCgiParameters cg;
            cg.ScanAddAll(http.Cgi);
            if (cg.Get("src") == "100500") {
                resp.SetContent(TICKETS_ME);
            } else if (cg.Get("src") == "100501") {
                resp.SetContent(TICKETS_PC);
            } else if (cg.Get("src") == "100599") {
                resp.SetContent(TICKETS_MANY_DSTS);
            }
        } else if (http.Path == "/tvm/private_api/__meta__") {
            resp.SetContent(Meta);
        }
        resp.AddHeader("X-Ya-Tvmtool-Data-Birthtime", IntToString<10>(Birthtime.Seconds()));
        resp.OutTo(params.Output);

        return true;
    }
};

static inline NTvmAuth::NRoles::TEntitiesIndex CreateEntitiesIndex() {
    using namespace NTvmAuth::NRoles;

    TEntitiesIndex index(
        {
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#11"},
            }),
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#11"},
                {"key#2", "value#22"},
                {"key#3", "value#33"},
            }),
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#11"},
                {"key#2", "value#23"},
                {"key#3", "value#33"},
            }),
            std::make_shared<TEntity>(TEntity{
                {"key#1", "value#13"},
                {"key#3", "value#33"},
            }),
        });

    return index;
}
