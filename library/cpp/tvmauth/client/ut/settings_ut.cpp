#include "common.h"

#include <library/cpp/tvmauth/client/misc/api/settings.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NTvmAuth;

Y_UNIT_TEST_SUITE(ClientSettings) {
#if !defined(_win_)
    Y_UNIT_TEST(CheckValid) {
        struct TTestCase {
            TString Name;
            NTvmApi::TClientSettings Settings;
            TString Err;
        };
        std::vector<TTestCase> cases = {
            TTestCase{
                .Name = "default",
                .Settings = {},
                .Err = "Invalid settings: nothing to do",
            },
            TTestCase{
                .Name = "only secret",
                .Settings = {
                    .Secret = TStringBuf("foobar"),
                },
                .Err = "Secret is present but destinations list is empty. It makes no sense",
            },
            TTestCase{
                .Name = "only dsts",
                .Settings = {
                    .FetchServiceTicketsForDsts = {42},
                },
                .Err = "SelfTvmId cannot be 0 if fetching of Service Tickets required",
            },
            TTestCase{
                .Name = "dsts with selfTvmId",
                .Settings = {
                    .SelfTvmId = 43,
                    .FetchServiceTicketsForDsts = {42},
                },
                .Err = "Secret is required for fetching of Service Tickets",
            },
            TTestCase{
                .Name = "correct service tickets fetching",
                .Settings = {
                    .SelfTvmId = 43,
                    .Secret = TStringBuf("foobar"),
                    .FetchServiceTicketsForDsts = {42},
                },
                .Err = "",
            },
            TTestCase{
                .Name = "only check srv flag",
                .Settings = {
                    .CheckServiceTickets = true,
                },
                .Err = "SelfTvmId cannot be 0 if checking of Service Tickets required",
            },
            TTestCase{
                .Name = "tirole without disk cache",
                .Settings = {
                    .SelfTvmId = 43,
                    .Secret = TStringBuf("foobar"),
                    .FetchRolesForIdmSystemSlug = "kek",
                },
                .Err = "Disk cache must be enabled to use roles: they can be heavy",
            },
        };

        for (const TTestCase& c : cases) {
            if (c.Err) {
                UNIT_ASSERT_EXCEPTION_CONTAINS_C(
                    c.Settings.CheckValid(),
                    TBrokenTvmClientSettings,
                    c.Err,
                    c.Name);
            } else {
                UNIT_ASSERT_NO_EXCEPTION_C(c.Settings.CheckValid(), c.Name);
            }
        }

        NTvmApi::TClientSettings s{.DiskCacheDir = "/impossible/dir"};
        UNIT_ASSERT_EXCEPTION(s.CheckValid(), TPermissionDenied);
    }

    Y_UNIT_TEST(CloneNormalized) {
        NTvmApi::TClientSettings original;
        original.FetchServiceTicketsForDsts = {43};

        UNIT_ASSERT_EXCEPTION_CONTAINS(original.CloneNormalized(),
                                       TBrokenTvmClientSettings,
                                       "SelfTvmId cannot be 0 if fetching of Service Tickets required");
        original.SelfTvmId = 15;
        original.Secret = "bar";
        original.DiskCacheDir = "./";

        NTvmApi::TClientSettings::TDstVector expected = {43};
        UNIT_ASSERT_VALUES_EQUAL(expected, original.CloneNormalized().FetchServiceTicketsForDsts);

        original.FetchServiceTicketsForDstsWithAliases = {{"foo", 42}};
        expected = {42, 43};
        UNIT_ASSERT_VALUES_EQUAL(expected, original.CloneNormalized().FetchServiceTicketsForDsts);

        original.FetchRolesForIdmSystemSlug = "kek";
        expected = {42, 43, 2028120};
        UNIT_ASSERT_VALUES_EQUAL(expected, original.CloneNormalized().FetchServiceTicketsForDsts);

        original.FetchServiceTicketsForDsts.push_back(2028120);
        expected = {42, 43, 2028120};
        UNIT_ASSERT_VALUES_EQUAL(expected, original.CloneNormalized().FetchServiceTicketsForDsts);
    }

    Y_UNIT_TEST(NeedServiceTicketsFetching) {
        NTvmApi::TClientSettings s;

        UNIT_ASSERT(!s.NeedServiceTicketsFetching());

        s.FetchServiceTicketsForDsts = {42};
        UNIT_ASSERT(s.NeedServiceTicketsFetching());
        s.FetchServiceTicketsForDsts.clear();

        s.FetchServiceTicketsForDstsWithAliases = {{"foo", 42}};
        UNIT_ASSERT(s.NeedServiceTicketsFetching());
        s.FetchServiceTicketsForDstsWithAliases.clear();

        s.FetchRolesForIdmSystemSlug = "bar";
        UNIT_ASSERT(s.NeedServiceTicketsFetching());
        s.FetchRolesForIdmSystemSlug.clear();
    }

    Y_UNIT_TEST(permitions) {
        UNIT_ASSERT_EXCEPTION(NTvmApi::TClientSettings::CheckPermissions("/qwerty"), TPermissionDenied);

        const TString tmpDir = "./cache_dir";

        NFs::RemoveRecursive(tmpDir);
        NFs::MakeDirectory(tmpDir, NFs::FP_OWNER_WRITE | NFs::FP_GROUP_WRITE | NFs::FP_ALL_WRITE);
        UNIT_ASSERT_EXCEPTION(NTvmApi::TClientSettings::CheckPermissions(tmpDir), TPermissionDenied);

        NFs::RemoveRecursive(tmpDir);
        NFs::MakeDirectory(tmpDir, NFs::FP_OWNER_READ | NFs::FP_GROUP_READ | NFs::FP_ALL_READ);
        UNIT_ASSERT_EXCEPTION(NTvmApi::TClientSettings::CheckPermissions(tmpDir), TPermissionDenied);

        NFs::RemoveRecursive(tmpDir);
        NFs::MakeDirectory(tmpDir, NFs::FP_COMMON_FILE);
        UNIT_ASSERT_NO_EXCEPTION(NTvmApi::TClientSettings::CheckPermissions(tmpDir));
    }
#endif

    Y_UNIT_TEST(Dst) {
        UNIT_ASSERT_EXCEPTION_CONTAINS(NTvmApi::TClientSettings::TDst(0), yexception, "TvmId cannot be 0");
        UNIT_ASSERT_EXCEPTION_CONTAINS(NTvmApi::TClientSettings::TDstMap({{"blackbox", 0}}),
                                       TBrokenTvmClientSettings,
                                       "TvmId cannot be 0");
    }

    Y_UNIT_TEST(Fetching) {
        NTvmApi::TClientSettings s;
        s.SetSelfTvmId(125);

        s.EnableServiceTicketsFetchOptions("qwerty", {{"blackbox", 19}});
        UNIT_ASSERT_NO_EXCEPTION(s.CheckValid());

        UNIT_ASSERT_VALUES_EQUAL(s.FetchServiceTicketsForDsts.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(s.FetchServiceTicketsForDsts[0], 19);
    }
}
