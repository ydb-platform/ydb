#include "file_storage.h"
#include <ydb/library/yql/core/file_storage/ut/test_http_server.h>

#include <ydb/library/yql/core/file_storage/proto/file_storage.pb.h>

#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/file.h>
#include <util/system/tempfile.h>
#include <util/thread/pool.h>

using namespace NYql;
using namespace NThreading;

Y_UNIT_TEST_SUITE(TFileStorageTests) {

    static TString ReadFileContent(const TString& path) {
        return TIFStream(path).ReadAll();
    }

    static std::unique_ptr<TTestHttpServer> CreateTestHttpServer() {
        TPortManager pm;
        const ui16 port = pm.GetPort();
        auto result = std::make_unique<TTestHttpServer>(port);
        result->Start();

        return result;
    }

    static void RemoveUrlMeta(const TFsPath& root) {
        TVector<TFsPath> children;
        root.List(children);

        auto it = FindIf(children, [](auto& c) {
            return c.GetExtension() == "url_meta";
        });

        UNIT_ASSERT(it != children.end());
        it->DeleteIfExists();
    }

    TString MakeWeakETag(const TString& strongETag) {
        return "W/" + strongETag;
    }

    Y_UNIT_TEST(PutUrlNoTokenNoETag) {
        auto server = CreateTestHttpServer();

        int downloadCount = 0;
        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto& request) {
            UNIT_ASSERT_VALUES_EQUAL(request.OAuthToken, "");
            ++downloadCount;
            return TTestHttpServer::TReply::Ok(currentContent);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();
        auto link1 = fs->PutUrl(url, {});
        currentContent = "UNUSED FILE"; // just to be just second download will drop data
        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount); // assume file was not consumed completely second time
        UNIT_ASSERT_VALUES_EQUAL(link1->GetStorageFileName(), link2->GetStorageFileName());
        const TString abcMd5 = "902fbdd2b1df0c4f70b4a5d23525e932";
        UNIT_ASSERT_VALUES_EQUAL(link1->GetMd5(), abcMd5);
        UNIT_ASSERT_VALUES_EQUAL(link1->GetMd5(), link2->GetMd5());
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetPath(), link2->GetPath());

        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));
    }

    Y_UNIT_TEST(PutUrlNoAccessForBadToken) {
        auto server = CreateTestHttpServer();

        TString okToken = "TOKEN_1";
        TString badToken = "TOKEN_2";

        server->SetRequestHandler([&](auto& request) {
            if (request.OAuthToken != okToken) {
                return TTestHttpServer::TReply::Forbidden();
            }

            const TString content = "ABC";
            return TTestHttpServer::TReply::Ok(content);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl(url, badToken), std::exception, "Failed to fetch url");

        auto link1 = fs->PutUrl(url, okToken);
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));

        // still no access, we downloaded resource though
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl(url, badToken), std::exception, "Failed to fetch url");
    }

    Y_UNIT_TEST(PutUrlETagChange) {
        auto server = CreateTestHttpServer();

        TString currentETag = "TAG_1";
        TString currentContent = "ABC";

        int downloadCount = 0;
        server->SetRequestHandler([&](auto& request) {
            if (request.IfNoneMatch == currentETag) {
                return TTestHttpServer::TReply::NotModified(currentETag);
            }

            ++downloadCount;
            return TTestHttpServer::TReply::OkETag(currentContent, currentETag);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        auto link1 = fs->PutUrl(url, {});
        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(1, downloadCount);

        // change etag:
        currentETag = "TAG_2";
        currentContent = "XYZPQAZWSXEDC"; // change length as well

        auto link3 = fs->PutUrl(url, {});
        auto link4 = fs->PutUrl(url, {});
        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);

        // check contents:
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));

        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link3->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link4->GetPath()));
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetMd5(), link3->GetMd5());
        UNIT_ASSERT_VALUES_EQUAL(link3->GetMd5(), link4->GetMd5());
    }

    Y_UNIT_TEST(PutUrlLastModifiedChange) {
        auto server = CreateTestHttpServer();

        TString currentLastModified = "Wed, 05 Jun 2019 13:39:26 GMT";
        TString currentContent = "ABC";

        int downloadCount = 0;
        server->SetRequestHandler([&](auto& request) {
            if (request.IfModifiedSince == currentLastModified) {
                return TTestHttpServer::TReply::NotModified({}, currentLastModified);
            }

            ++downloadCount;
            return TTestHttpServer::TReply::OkLastModified(currentContent, currentLastModified);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        auto link1 = fs->PutUrl(url, {});
        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(1, downloadCount);

        // change LastModified:
        currentLastModified = "Wed, 06 Jun 2019 13:39:26 GMT";
        currentContent = "XYZPQAZWSXEDC"; // change length as well

        auto link3 = fs->PutUrl(url, {});
        auto link4 = fs->PutUrl(url, {});
        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);

        // check contents:
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));

        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link3->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link4->GetPath()));
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetMd5(), link3->GetMd5());
        UNIT_ASSERT_VALUES_EQUAL(link3->GetMd5(), link4->GetMd5());
    }

    Y_UNIT_TEST(PutUrlETagChangeButNoSupportForIfNoneMatch) {
        auto server = CreateTestHttpServer();

        TString currentETag = "TAG_1";
        TString currentContent = "ABC";

        int downloadCount = 0;
        server->SetRequestHandler([&](auto& request) {
            Y_UNUSED(request);
            ++downloadCount;
            return TTestHttpServer::TReply::OkETag(currentContent, currentETag);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        auto link1 = fs->PutUrl(url, {});
        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);

        // change etag:
        currentETag = "TAG_2";
        currentContent = "XYZPQAZWSXEDC"; // change length as well

        auto link3 = fs->PutUrl(url, {});
        auto link4 = fs->PutUrl(url, {});
        UNIT_ASSERT_VALUES_EQUAL(4, downloadCount);

        // check contents:
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));

        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link3->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link4->GetPath()));
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetMd5(), link3->GetMd5());
        UNIT_ASSERT_VALUES_EQUAL(link3->GetMd5(), link4->GetMd5());
    }

    Y_UNIT_TEST(PutUrlWeakETagChange) {
        auto server = CreateTestHttpServer();

        TString currentETag = "TAG_1";
        TString currentContent = "ABC";

        int downloadCount = 0;
        server->SetRequestHandler([&](auto& request) {
            if (request.IfNoneMatch == currentETag) {
                return TTestHttpServer::TReply::NotModified(MakeWeakETag(currentETag));
            }

            ++downloadCount;
            return TTestHttpServer::TReply::OkETag(currentContent, MakeWeakETag(currentETag));
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        auto link1 = fs->PutUrl(url, {});
        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(1, downloadCount);

        // change etag:
        currentETag = "TAG_2";
        currentContent = "XYZPQAZWSXEDC"; // change length as well

        auto link3 = fs->PutUrl(url, {});
        auto link4 = fs->PutUrl(url, {});
        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);

        // check contents:
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));

        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link3->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("XYZPQAZWSXEDC", ReadFileContent(link4->GetPath()));
    }

    Y_UNIT_TEST(SecondPutUrlNoETagButFileRemoved) {
        auto server = CreateTestHttpServer();

        int downloadCount = 0;
        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto& request) {
            UNIT_ASSERT(!request.IfNoneMatch);
            ++downloadCount;
            return TTestHttpServer::TReply::Ok(currentContent);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();
        auto link1 = fs->PutUrl(url, {});

        auto storageFile = fs->GetRoot() / link1->GetStorageFileName();
        storageFile.DeleteIfExists();

        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);
        UNIT_ASSERT_VALUES_EQUAL(link1->GetStorageFileName(), link2->GetStorageFileName());
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetPath(), link2->GetPath());

        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));
    }

    Y_UNIT_TEST(SecondPutUrlETagButFileRemoved) {
        auto server = CreateTestHttpServer();

        int downloadCount = 0;
        TString currentETag = "TAG_1";
        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto& request) {
            UNIT_ASSERT(!request.IfNoneMatch);
            ++downloadCount;
            return TTestHttpServer::TReply::OkETag(currentContent, currentETag);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();
        auto link1 = fs->PutUrl(url, {});

        auto storageFile = fs->GetRoot() / link1->GetStorageFileName();
        storageFile.DeleteIfExists();

        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);
        UNIT_ASSERT_VALUES_EQUAL(link1->GetStorageFileName(), link2->GetStorageFileName());
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetPath(), link2->GetPath());

        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));
    }

    Y_UNIT_TEST(SecondPutUrlETagButMetaRemoved) {
        auto server = CreateTestHttpServer();

        int downloadCount = 0;
        TString currentETag = "TAG_1";
        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto& request) {
            UNIT_ASSERT(!request.IfNoneMatch);
            ++downloadCount;
            return TTestHttpServer::TReply::OkETag(currentContent, currentETag);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();
        auto link1 = fs->PutUrl(url, {});

        RemoveUrlMeta(fs->GetRoot());

        auto link2 = fs->PutUrl(url, {});

        UNIT_ASSERT_VALUES_EQUAL(2, downloadCount);
        UNIT_ASSERT_VALUES_EQUAL(link1->GetStorageFileName(), link2->GetStorageFileName());
        UNIT_ASSERT_VALUES_EQUAL(link1->GetMd5(), link2->GetMd5());
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetPath(), link2->GetPath());

        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link1->GetPath()));
        UNIT_ASSERT_VALUES_EQUAL("ABC", ReadFileContent(link2->GetPath()));
    }

    Y_UNIT_TEST(Md5ForPutFiles) {
        TString currentContent = "ABC";

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        TTempFileHandle h1;
        h1.Write("ABC", 3);
        const TString abcMd5 = "902fbdd2b1df0c4f70b4a5d23525e932";
        // no hardlinks to existing file in storage
        auto link1 = fs->PutInline("ABCD");
        auto link2 = fs->PutFile(h1.GetName());

        // hardlinks
        TTempFileHandle h2;
        h2.Write("ABCD", 4);
        auto link3 = fs->PutInline("ABC");
        auto link4 = fs->PutFile(h2.GetName());

        UNIT_ASSERT_VALUES_EQUAL(link1->GetMd5(), link4->GetMd5());
        UNIT_ASSERT_VALUES_EQUAL(link1->GetStorageFileName(), link4->GetStorageFileName());
        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetPath(), link4->GetPath());

        UNIT_ASSERT_VALUES_EQUAL(link2->GetMd5(), link3->GetMd5());
        UNIT_ASSERT_VALUES_EQUAL(link2->GetStorageFileName(), link3->GetStorageFileName());
        UNIT_ASSERT_VALUES_UNEQUAL(link2->GetPath(), link3->GetPath());

        UNIT_ASSERT_VALUES_UNEQUAL(link1->GetMd5(), link2->GetMd5());
        UNIT_ASSERT_VALUES_UNEQUAL(link3->GetMd5(), link4->GetMd5());
    }

    Y_UNIT_TEST(NoUrlDownloadRetryOnBadCode) {
        auto server = CreateTestHttpServer();

        int downloadCount = 0;
        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto& request) {
            UNIT_ASSERT(!request.IfNoneMatch);
            ++downloadCount;
            return TTestHttpServer::TReply::Forbidden();
        });

        TFileStorageConfig params;
        params.SetRetryCount(3);
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl(url, {}), std::exception, "FileStorage: Failed to download file by URL");
        // currently this test does not work and we retry even if forbidden code is returned
        // todo: optimize this
        UNIT_ASSERT_VALUES_EQUAL(3, downloadCount);
    }

    Y_UNIT_TEST(PutEmptyFiles) {
        auto server = CreateTestHttpServer();

        TString currentETag = "TAG_1";
        TString currentContent = "";

        server->SetRequestHandler([&](auto& ) {
            return TTestHttpServer::TReply::OkETag(currentContent, currentETag, 0);
        });

        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        auto link1 = fs->PutUrl(url, {});
        auto link2 = fs->PutInline("");

        TTempFileHandle tmpFile;
        auto link3 = fs->PutFile(tmpFile.GetName());
        const TString emptyStringMd5 = "d41d8cd98f00b204e9800998ecf8427e";

        for (auto link : { link1, link2, link3 }) {
            UNIT_ASSERT_VALUES_EQUAL("", ReadFileContent(link->GetPath()));
            UNIT_ASSERT_VALUES_EQUAL(emptyStringMd5, link->GetMd5());
        }
    }

    Y_UNIT_TEST(BadContentLength) {
        auto server = CreateTestHttpServer();

        int downloadCount = 0;
        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto&) {
            ++downloadCount;
            return TTestHttpServer::TReply::Ok(currentContent, 10);
        });

        TFileStorageConfig params;
        params.SetRetryCount(3);
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();

        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl(url, {}), std::exception, "Size mismatch while downloading url http");
        UNIT_ASSERT_VALUES_EQUAL(3, downloadCount);
    }

    Y_UNIT_TEST(AllowedUrls) {
        auto server = CreateTestHttpServer();

        TString currentContent = "ABC";
        server->SetRequestHandler([&](auto&) {
            return TTestHttpServer::TReply::Ok(currentContent);
        });

        auto url = server->GetUrl();

        // not in whitelist
        TFileStorageConfig params1;
        params1.AddAllowedUrlPatterns("^XXXX$");
        TFileStoragePtr fs1 = CreateFileStorage(params1);

        UNIT_ASSERT_EXCEPTION_CONTAINS(fs1->PutUrl(url, {}), std::exception, "It is not allowed to download url http://localhost:");
        fs1->AddAllowedUrlPattern("^http://localhost:");

        auto link1 = fs1->PutUrl(url, {});
        UNIT_ASSERT_VALUES_EQUAL(currentContent, ReadFileContent(link1->GetPath()));

        // have in whitelist
        TFileStorageConfig params2;
        params2.SetSocketTimeoutMs(4000);
        params2.AddAllowedUrlPatterns("^http://localhost:");
        TFileStoragePtr fs2 = CreateFileStorage(params2);
        fs2->AddAllowedUrlPattern("^XXXX$");

        auto link2 = fs1->PutUrl(url, {});
        UNIT_ASSERT_VALUES_EQUAL(currentContent, ReadFileContent(link2->GetPath()));
    }

    Y_UNIT_TEST(SocketTimeout) {
        auto server = CreateTestHttpServer();

        server->SetRequestHandler([&](auto& ) {
            Sleep(TDuration::Seconds(2));
            return TTestHttpServer::TReply::Ok("ABC");
        });

        TFileStorageConfig params;
        params.SetSocketTimeoutMs(1000);
        TFileStoragePtr fs = CreateFileStorage(params);

        auto url = server->GetUrl();
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl(url, {}), std::exception, "can not read from socket input stream");
    }

#ifndef OPENSOURCE
    Y_UNIT_TEST(ArcFileWithoutRev) {
        TFileStorageConfig params;
        TFileStoragePtr fs = CreateFileStorage(params);

        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl("arc://yql/a.txt", {}), std::exception, "Revision for Arcadia file must be specified");
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl("arc://yql/a.txt?rev=", {}), std::exception, "Revision for Arcadia file must be specified");
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl("arc://yql/a.txt?rev=0", {}), std::exception, "Revision for Arcadia file must be specified");
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl("arc://yql/a.txt?rev=head", {}), std::exception, "Revision for Arcadia file must be specified");
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl("arc://yql/a.txt?rev=HEAD", {}), std::exception, "Revision for Arcadia file must be specified");
        UNIT_ASSERT_EXCEPTION_CONTAINS(fs->PutUrl("arc://yql/a.txt?op_rev=123", {}), std::exception, "Revision for Arcadia file must be specified");
    }
#endif
}
