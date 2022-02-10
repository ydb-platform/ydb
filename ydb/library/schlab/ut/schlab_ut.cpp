#include <ydb/library/schlab/schoot/schoot_gen_cfg.h>
#include <ydb/library/schlab/schoot/schoot_gen.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(SchLab) {
    Y_UNIT_TEST(TestSchootGenCfgSet1) {
        TString json = R"__EOF__({
            "cfg":[{
                "label": "mygen1",
                "startTime": 0,
                "period": 100,
                "periodCount": 0,
                "reqSizeBytes": 2000000,
                "reqCount": 3,
                "reqInterval": 2,
                "user": "vdisk0",
                "desc": "write-log"
            },{
                "label": "mygen2",
                "startTime": 350,
                "period": 10,
                "periodCount": 100,
                "reqSizeBytes": 10240,
                "reqCount": 5,
                "reqInterval": 0.1,
                "user": "vdisk1"
            }]})__EOF__";
        NKikimr::NSchLab::TSchOotGenCfgSet cfg(json);
        UNIT_ASSERT_C(cfg.IsValid, "ErrorDetails# " << cfg.ErrorDetails);
    }

    Y_UNIT_TEST(TestSchootGenCfgSet2) {
        TString json = R"__EOF__({"cfg":[{"label":"gen0","startTime":0,"period":100,"periodCount":1,
                "reqSizeBytes":1024,"reqCount":1,"reqInterval":10,"user":"vdisk0","desc":"write"}]})__EOF__";
        NKikimr::NSchLab::TSchOotGenCfgSet cfg(json);
        UNIT_ASSERT_C(cfg.IsValid, "ErrorDetails# " << cfg.ErrorDetails);
    }

    Y_UNIT_TEST(TestSchootGenCfgSetInvalid1) {
        TString json = R"__EOF__({
            "cfg":[{
                "label": "mygen1i",
                "startTime": 0,
                "periodCount": 0,
                "reqSizeBytes": 2000000,
                "reqCount": 3,
                "reqInterval": 2,
                "user": "vdisk0",
                "desc": "write-log"
            }]})__EOF__";
        NKikimr::NSchLab::TSchOotGenCfgSet cfg(json);
        UNIT_ASSERT(!cfg.IsValid);
    }

    Y_UNIT_TEST(TestSchootGenCfgSetInvalid2) {
        TString json = R"__EOF__({
            "cfg":[{
                "label": "mygen2i",
                "startTime": 0,
                "period": 100,
                "periodCount": 0,
                "reqSizeBytes": 2000000,
                "reqCount": 3,
                "reqInterval": -2,
                "user": "vdisk0",
                "desc": "write-log"
            }]})__EOF__";
        NKikimr::NSchLab::TSchOotGenCfgSet cfg(json);
        UNIT_ASSERT(!cfg.IsValid);
    }

    Y_UNIT_TEST(TestSchootGen1) {
        TString json = R"__EOF__({
            "cfg":[{
                "label": "mygen1",
                "startTime": 100,
                "period": 200,
                "periodCount": 0,
                "reqSizeBytes": 234,
                "reqCount": 2,
                "reqInterval": 50,
                "user": "vdisk0",
                "desc": "write-log"
            }]})__EOF__";
        NKikimr::NSchLab::TSchOotGenCfgSet cfg(json);
        UNIT_ASSERT_C(cfg.IsValid, "ErrorDetails# \"" << cfg.ErrorDetails << "\"");
        NKikimr::NSchLab::TSchOotGenSet gen(cfg);
        double time = 0.0;
        double pause = 0.0;
        ui64 size = 0;
        bool isOk = false;
        TString user;
        TString desc;
        isOk = gen.Step(time, &size, &pause, &user, &desc);
        UNIT_ASSERT(isOk);
        UNIT_ASSERT_EQUAL_C(size, 0, "size# " << size);
        UNIT_ASSERT_EQUAL_C(pause, 100.0, "pause# " << pause);
        time = 100.0;
        isOk = gen.Step(time, &size, &pause, &user, &desc);
        UNIT_ASSERT(isOk);
        UNIT_ASSERT_EQUAL(size, 234);
        UNIT_ASSERT(pause <= 50.0);
        time = 125.0;
        isOk = gen.Step(time, &size, &pause, &user, &desc);
        UNIT_ASSERT(isOk);
        UNIT_ASSERT_EQUAL_C(size, 0, "size# " << size);
        UNIT_ASSERT_EQUAL_C(pause, 25.0, "pause# " << pause);
        time = 150.0;
        isOk = gen.Step(time, &size, &pause, &user, &desc);
        UNIT_ASSERT(isOk);
        UNIT_ASSERT_EQUAL(size, 234);
        UNIT_ASSERT(pause <= 150.0);
        time = 200.0;
        isOk = gen.Step(time, &size, &pause, &user, &desc);
        UNIT_ASSERT(isOk);
        UNIT_ASSERT_EQUAL(size, 0);
        UNIT_ASSERT(pause <= 100.0);
    }
}

