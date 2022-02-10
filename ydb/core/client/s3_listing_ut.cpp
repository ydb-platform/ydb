#include "flat_ut_client.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NFlatTests {

using namespace Tests;
using NClient::TValue;

Y_UNIT_TEST_SUITE(TS3ListingTest) {

    void S3WriteRow(TFlatMsgBusClient& annoyingClient, ui64 hash, TString name, TString path, ui64 version, ui64 ts, TString data, TString table) {
        TString insertRowQuery =  R"(
                    (
                    (let key '(
                        '('Hash (Uint64 '%llu))
                        '('Name (Utf8 '"%s"))
                        '('Path (Utf8 '"%s"))
                        '('Version (Uint64 '%llu))
                    ))
                    (let value '(
                        '('Timestamp (Uint64 '%llu))
                        '('Data (String '"%s"))
                    ))
                    (let ret_ (AsList
                        (UpdateRow '/dc-1/Dir/%s key value)
                    ))
                    (return ret_)
                    )
                )";

        annoyingClient.FlatQuery(Sprintf(insertRowQuery.data(), hash, name.data(), path.data(), version, ts, data.data(), table.data()));
    }

    void S3DeleteRow(TFlatMsgBusClient& annoyingClient, ui64 hash, TString name, TString path, ui64 version, TString table) {
        TString eraseRowQuery =  R"(
                    (
                    (let key '(
                        '('Hash (Uint64 '%llu))
                        '('Name (Utf8 '"%s"))
                        '('Path (Utf8 '"%s"))
                        '('Version (Uint64 '%llu))
                    ))
                    (let ret_ (AsList
                        (EraseRow '/dc-1/Dir/%s key)
                    ))
                    (return ret_)
                    )
                )";

        annoyingClient.FlatQuery(Sprintf(eraseRowQuery.data(), hash, name.data(), path.data(), version, table.data()));
    }

    void PrepareS3Data(TFlatMsgBusClient& annoyingClient) {
        annoyingClient.InitRoot();
        annoyingClient.MkDir("/dc-1", "Dir");
        annoyingClient.CreateTable("/dc-1/Dir",
            R"(Name: "Table"
                Columns { Name: "Hash"      Type: "Uint64"}
                Columns { Name: "Name"      Type: "Utf8"}
                Columns { Name: "Path"      Type: "Utf8"}
                Columns { Name: "Version"   Type: "Uint64"}
                Columns { Name: "Timestamp" Type: "Uint64"}
                Columns { Name: "Data"      Type: "String"}
                Columns { Name: "ExtraData" Type: "String"}
                Columns { Name: "Unused1"   Type: "Uint32"}
                KeyColumnNames: [
                    "Hash",
                    "Name",
                    "Path",
                    "Version"
                    ]
                SplitBoundary { KeyPrefix {
                    Tuple { Optional { Uint64 : 60 }}
                }}
                SplitBoundary { KeyPrefix {
                    Tuple { Optional { Uint64 : 100 }}
                    Tuple { Optional { Text : 'Bucket100' }}
                    Tuple { Optional { Text : '/Videos/Game of Thrones/Season 1/Episode 2' }}
                }}
                SplitBoundary { KeyPrefix {
                    Tuple { Optional { Uint64 : 100 }}
                    Tuple { Optional { Text : 'Bucket100' }}
                    Tuple { Optional { Text : '/Videos/Game of Thrones/Season 1/Episode 8' }}
                }}
                SplitBoundary { KeyPrefix {
                    Tuple { Optional { Uint64 : 100 }}
                    Tuple { Optional { Text : 'Bucket100' }}
                    Tuple { Optional { Text : '/Videos/Godfather 2.avi' }}
                }}
                PartitionConfig {
                    ExecutorCacheSize: 100

                                        CompactionPolicy {
                                                InMemSizeToSnapshot: 2000
                                                InMemStepsToSnapshot: 1 
                                                InMemForceStepsToSnapshot: 50
                                                InMemForceSizeToSnapshot: 16777216 
                                                InMemCompactionBrokerQueue: 0
                                                ReadAheadHiThreshold: 1048576
                                                ReadAheadLoThreshold: 16384
                                                MinDataPageSize: 300
                                                SnapBrokerQueue: 0

                                                LogOverheadSizeToSnapshot: 16777216
                                                LogOverheadCountToSnapshot: 500
                                                DroppedRowsPercentToCompact: 146

                                                Generation {
                                                  GenerationId: 0
                                                  SizeToCompact: 0
                                                  CountToCompact: 2000
                                                  ForceCountToCompact: 4000
                                                  ForceSizeToCompact: 100000000
                                                  #CompactionBrokerQueue: 4294967295
                                                  KeepInCache: false
                                                  ResourceBrokerTask: "compaction_gen1"
                                                  ExtraCompactionPercent: 100
                                                  ExtraCompactionMinSize: 16384
                                                  ExtraCompactionExpPercent: 110
                                                  ExtraCompactionExpMaxSize: 0
                                                  UpliftPartSize: 0
                                                }
                                        }

                }
            )");

        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Shoot to Thrill.mp3", 1, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Thunderstruck.mp3", 1, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/rock.m3u", 1, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana", 1, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana/Smeels Like Teen Spirit.mp3", 1, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana/In Bloom.mp3", 1, 20, "", "Table");

        S3WriteRow(annoyingClient, 100, "Bucket100", "/Photos/face.jpg", 1, 10, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Photos/facepalm.jpg", 1, 20, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Photos/palm.jpg", 1, 30, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 1.avi", 1, 100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 10.avi", 1, 300, "", "Table");

        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 2.avi", 1, 200, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 3.avi", 1, 300, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 4.avi", 1, 300, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 5.avi", 1, 300, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 6.avi", 1, 300, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 7.avi", 1, 300, "", "Table");

        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 8.avi", 1, 300, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 9.avi", 1, 300, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 2/Episode 1.avi", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Godfather 2.avi", 1, 500, "", "Table");

        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Godfather.avi", 1, 500, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Godmother.avi", 1, 500, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/House of Cards/Season 1/Chapter 1.avi", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/House of Cards/Season 1/Chapter 2.avi", 1, 1200, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/Videos/Terminator 2.avi", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/XXX/1.avi", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/XXX/2.avi", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/XXX/3.avi", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 100, "Bucket100", "/XXX/3d.avi", 1, 1100, "", "Table");

        S3WriteRow(annoyingClient, 333, "Bucket333", "asdf", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 333, "Bucket333", "boo/bar", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 333, "Bucket333", "boo/baz/xyzzy", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 333, "Bucket333", "cquux/thud", 1, 1100, "", "Table");
        S3WriteRow(annoyingClient, 333, "Bucket333", "cquux/bla", 1, 1100, "", "Table");

        S3DeleteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana/Smeels Like Teen Spirit.mp3", 1, "Table");
        S3DeleteRow(annoyingClient, 100, "Bucket100", "/Photos/palm.jpg", 1, "Table");
        S3DeleteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 2.avi", 1, "Table");
        S3DeleteRow(annoyingClient, 100, "Bucket100", "/Videos/Game of Thrones/Season 1/Episode 5.avi", 1, "Table");
        S3DeleteRow(annoyingClient, 100, "Bucket100", "/Videos/House of Cards/Season 1/Chapter 2.avi", 1, "Table");
    }

    void DoListingBySelectRange(TFlatMsgBusClient& annoyingClient,
                                ui64 bucket, const TString& pathPrefix, const TString& pathDelimiter, const TString& startAfter, ui32 maxKeys,
                                TSet<TString>& commonPrefixes, TSet<TString>& contents)
    {
        // Read all rows from the bucket
        TString table = "Table";
        ui64 hash = bucket;
        TString name = "Bucket" + ToString(bucket);
        TString selectBucketQuery =  R"(
                (
                    (let range '(
                        '('Hash (Uint64 '%llu) (Uint64 '%llu))
                        '('Name (Utf8 '"%s") (Utf8 '"%s"))
                        '('Path (Nothing (OptionalType (DataType 'Utf8))) (Void))
                        '('Version (Nothing (OptionalType (DataType 'Uint64))) (Void))
                    ))
                    (let columns '(
                        'Path
                    ))
                    (let res
                        (SelectRange '/dc-1/Dir/%s range columns '() )
                    )
                    (return (AsList (SetResult 'Objects res)))
                )
                )";

        TClient::TFlatQueryOptions opts;
        NKikimrMiniKQL::TResult res;
        annoyingClient.FlatQuery(Sprintf(selectBucketQuery.data(), hash, hash, name.data(), name.data(), table.data()), opts, res);

        //Cout << res << Endl;
        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue objects = value["Objects"];
        TValue l = objects["List"];
        TVector<TString> paths;
        for (ui32 i = 0; i < l.Size(); ++i) {
            TValue ps = l[i];
            paths.emplace_back(ps["Path"]);
        }

        // Make a list of common prefixes and a list of full paths that match the parameter
        commonPrefixes.clear();
        contents.clear();
        for (const auto& p : paths) {
            if (commonPrefixes.size() + contents.size() == maxKeys)
                break;

            if (!p.StartsWith(pathPrefix))
                continue;

            if (p <= startAfter)
                continue;

            size_t delimPos = p.find_first_of(pathDelimiter, pathPrefix.length());
            if (delimPos == TString::npos) {
                contents.insert(p);
            } else {
                TString prefix = p.substr(0, delimPos + pathDelimiter.length());
                if (prefix > startAfter) {
                    commonPrefixes.insert(prefix);
                }
            }
        }
    }

    void DoS3Listing(TFlatMsgBusClient& annoyingClient,
                    ui64 bucket, const TString& pathPrefix, const TString& pathDelimiter, const TString& startAfter,
                    const TVector<TString>& columnsToReturn, ui32 maxKeys,
                    TVector<TString>& commonPrefixes, TVector<TString>& contents)
    {
        TString pbPrefixCols =
            "Type {"
            "   Kind : Tuple "
            "   Tuple {"
            "       Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4 } } } }"
            "       Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
            "   }"
            "}"
            "Value { "
            "   Tuple { Optional { Uint64 : " + ToString(bucket) +" } } "
            "   Tuple { Optional { Text : 'Bucket" + ToString(bucket) + "' } } "
            "}";

        TString pbStartAfterSuffix;
        if (startAfter) {
            pbStartAfterSuffix =
                "Type {"
                "   Kind : Tuple "
                "   Tuple {"
                "       Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }"
                "   }"
                "}"
                "Value { "
                "   Tuple { Optional { Text : '" + startAfter + "' } } "
                "}";
        }

        NKikimrClient::TS3ListingResponse response;
        annoyingClient.S3Listing("/dc-1/Dir/Table", pbPrefixCols, pathPrefix, pathDelimiter, pbStartAfterSuffix, columnsToReturn, maxKeys, 0, response);

        //Cerr << response << Endl;

        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), NMsgBusProxy::MSTATUS_OK);

        const NKikimrMiniKQL::TResult& res = response.GetResult();

        commonPrefixes.clear();
        contents.clear();

        TValue value = TValue::Create(res.GetValue(), res.GetType());
        TValue prefixes = value["CommonPrefixes"];
        TValue objects = value["Contents"];
        for (ui32 i = 0; i < prefixes.Size(); ++i) {
            TValue ps = prefixes[i];
            commonPrefixes.emplace_back(ps["Path"]);
        }
        for (ui32 i = 0; i < objects.Size(); ++i) {
            TValue ps = objects[i];
            contents.emplace_back(ps["Path"]);
        }
    }

    void CompareS3Listing(TFlatMsgBusClient& annoyingClient, ui64 bucket, const TString& pathPrefix, const TString& pathDelimiter,
                       const TString& startAfter, ui32 maxKeys, const TVector<TString>& columnsToReturn)
    {
        TSet<TString> expectedCommonPrefixes;
        TSet<TString> expectedContents;
        DoListingBySelectRange(annoyingClient, bucket, pathPrefix, pathDelimiter, startAfter, maxKeys, expectedCommonPrefixes, expectedContents);

        TVector<TString> commonPrefixes;
        TVector<TString> contents;
        DoS3Listing(annoyingClient, bucket, pathPrefix, pathDelimiter, startAfter, columnsToReturn, maxKeys, commonPrefixes, contents);

        UNIT_ASSERT_VALUES_EQUAL(expectedCommonPrefixes.size(), commonPrefixes.size());
        ui32 i = 0;
        for (const auto& p : expectedCommonPrefixes) {
            Cout << "CommonPrefix: " << p << Endl;
            UNIT_ASSERT_VALUES_EQUAL(p, commonPrefixes[i]);
            ++i;
        }

        UNIT_ASSERT_VALUES_EQUAL(expectedContents.size(), contents.size());
        i = 0;
        for (const auto& p : expectedContents) {
            Cout << "Contents: " << p << Endl;
            UNIT_ASSERT_VALUES_EQUAL(p, contents[i]);
            ++i;
        }
    }

    void TestS3Listing(TFlatMsgBusClient& annoyingClient, ui64 bucket, const TString& pathPrefix, const TString& pathDelimiter,
                       ui32 maxKeys, const TVector<TString>& columnsToReturn)
    {
        Cout << Endl << "---------------------------------------" << Endl
             << "Bucket" << bucket << " : " << pathPrefix << Endl;

        CompareS3Listing(annoyingClient, bucket, pathPrefix, pathDelimiter, "", maxKeys, columnsToReturn);
        CompareS3Listing(annoyingClient, bucket, pathPrefix, pathDelimiter, pathPrefix, maxKeys, columnsToReturn);

        TSet<TString> expectedCommonPrefixes;
        TSet<TString> expectedContents;
        DoListingBySelectRange(annoyingClient, bucket, pathPrefix, pathDelimiter, "",  100500, expectedCommonPrefixes, expectedContents);

        for (const TString& after : expectedCommonPrefixes) {
            CompareS3Listing(annoyingClient, bucket, pathPrefix, pathDelimiter, after, maxKeys, columnsToReturn);
        }

        for (const TString& after : expectedContents) {
            CompareS3Listing(annoyingClient, bucket, pathPrefix, pathDelimiter, after, maxKeys, columnsToReturn);
        }
    }

    Y_UNIT_TEST(Listing) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        TFlatMsgBusClient annoyingClient(port);

        PrepareS3Data(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::MSGBUS_REQUEST, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);


        TestS3Listing(annoyingClient, 50, "", "", 10, {});
        TestS3Listing(annoyingClient, 50, "", "/", 7, {});
        TestS3Listing(annoyingClient, 50, "Music/", "/", 9, {});
        TestS3Listing(annoyingClient, 50, "Music/Nirvana", "/", 11, {});
        TestS3Listing(annoyingClient, 50, "Music/Nirvana/", "/", 2, {});
        TestS3Listing(annoyingClient, 50, "Photos/", "/", 3, {});

        TestS3Listing(annoyingClient, 100, "", "", 4, {});
        TestS3Listing(annoyingClient, 100, "", "/", 7, {});
        TestS3Listing(annoyingClient, 100, "/", "", 3, {});
        TestS3Listing(annoyingClient, 100, "/", "/", 1, {});
        TestS3Listing(annoyingClient, 100, "/Photos/", "/", 11, {});
        TestS3Listing(annoyingClient, 100, "/Videos/", "/", 18, {});
        TestS3Listing(annoyingClient, 100, "/Videos", "/", 3, {"Path", "Timestamp"});
        TestS3Listing(annoyingClient, 100, "/Videos/Game ", "/", 5, {"Path", "Timestamp"});
        TestS3Listing(annoyingClient, 100, "/Videos/Game of Thrones/Season 1/", "/", 6, {"Path", "Timestamp"});
        TestS3Listing(annoyingClient, 100, "/Videos/Game of Thr", " ", 4, {"Path", "Timestamp"});

        TestS3Listing(annoyingClient, 20, "", "/", 8, {"Path", "Timestamp"});
        TestS3Listing(annoyingClient, 200, "/", "/", 3, {"Path", "Timestamp"});

        // Request NULL columns
        TestS3Listing(annoyingClient, 50, "Photos/", "/", 7, {"ExtraData"});
        TestS3Listing(annoyingClient, 50, "Photos/", "", 2, {"Unused1"});
        TestS3Listing(annoyingClient, 50, "Music/", "/", 11, {"ExtraData"});
        TestS3Listing(annoyingClient, 50, "/", "", 8, {"Unused1"});

        TestS3Listing(annoyingClient, 333, "", "", 2, {});
        TestS3Listing(annoyingClient, 333, "", "/", 2, {});
        TestS3Listing(annoyingClient, 333, "", "", 3, {});
        TestS3Listing(annoyingClient, 333, "", "/", 3, {});
    }

    Y_UNIT_TEST(MaxKeysAndSharding) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        TFlatMsgBusClient annoyingClient(port);

        PrepareS3Data(annoyingClient);

//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::MSGBUS_REQUEST, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        for (auto commonPrefix: {"/", "/Videos", "/Videos/", "/W", "/X",
                "/Videos/Game of", "/Videos/Game of Thrones/",
                "/Videos/Game of Thrones/Season 1",
                "/Videos/Game of Thrones/Season 1/"})
        {
            for (ui32 maxKeys = 1; maxKeys < 20; ++maxKeys) {
                TestS3Listing(annoyingClient, 100, commonPrefix, "/", maxKeys, {});
            }
        }
    }

    TString MakeTuplePb(const TVector<TString>& values) {
        TStringStream pbPrefixCols;
        pbPrefixCols <<
            "Type {"
            "   Kind : Tuple "
            "   Tuple {";
        for (size_t i = 0; i < values.size(); ++i) {
            pbPrefixCols <<
                "       Element { Kind : Optional Optional { Item { Kind : Data Data { Scheme : 4608 } } } }";
        }
        pbPrefixCols <<
            "   }"
            "}"
            "Value { ";
        for (const auto& pc : values) {
            pbPrefixCols <<
                "   Tuple { Optional { Text : '" << pc <<  "' } } ";
        }
        pbPrefixCols << "}";

        return pbPrefixCols.Str();
    }

    void TestS3GenericListingRequest(TFlatMsgBusClient& annoyingClient,
                    const TVector<TString>& prefixColumns, const TString& pathPrefix, const TString& pathDelimiter,
                    const TVector<TString>& startAfterSuffixColumns,
                    const TVector<TString>& columnsToReturn, ui32 maxKeys, ui32 timeoutMillisec,
                    NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
                    const TString& expectedErrMessage = "")
    {
        TString pbPrefixCols = MakeTuplePb(prefixColumns);
        // Cerr << pbPrefixCols << Endl;

        TString pbStartAfterSuffixCols = MakeTuplePb(startAfterSuffixColumns);
        // Cerr << pbStartAfterSuffixCols << Endl;

        NKikimrClient::TS3ListingResponse response;
        annoyingClient.S3Listing("/dc-1/Dir/Table", pbPrefixCols, pathPrefix, pathDelimiter,
                                 pbStartAfterSuffixCols, columnsToReturn, maxKeys, timeoutMillisec, response);

        //
        Cerr << response << Endl;

        UNIT_ASSERT_VALUES_EQUAL(response.GetStatus(), expectedStatus);
        UNIT_ASSERT_VALUES_EQUAL(response.GetDescription(), expectedErrMessage);
    }

    void TestS3ListingRequest(TFlatMsgBusClient& annoyingClient,
                    const TVector<TString>& prefixColumns, const TString& pathPrefix, const TString& pathDelimiter,
                    const TString& startAfter, const TVector<TString>& columnsToReturn, ui32 maxKeys,
                    NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
                    const TString& expectedErrMessage = "")
    {
        TVector<TString> startAfterSuffix;
        if (!startAfter.empty())
            startAfterSuffix.push_back(startAfter);
        return TestS3GenericListingRequest(annoyingClient, prefixColumns, pathPrefix, pathDelimiter,
                                           startAfterSuffix,
                                           columnsToReturn, maxKeys, 0,
                                           expectedStatus, expectedErrMessage);
    }

    Y_UNIT_TEST(SchemaChecks) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        TFlatMsgBusClient annoyingClient(port);

        PrepareS3Data(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::MSGBUS_REQUEST, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TestS3ListingRequest(annoyingClient, {}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Value for path column 'Hash' has type Uint64, expected Utf8");

        TestS3ListingRequest(annoyingClient, {""}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Cannot parse value of type Uint64 from text '' in tuple at position 0");

        TestS3ListingRequest(annoyingClient, {"AAA"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Cannot parse value of type Uint64 from text 'AAA' in tuple at position 0");

        TestS3ListingRequest(annoyingClient, {"-1"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Cannot parse value of type Uint64 from text '-1' in tuple at position 0");

        TestS3ListingRequest(annoyingClient, {"1"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_OK,
            "");

        TestS3ListingRequest(annoyingClient, {"1", "Bucket1", "/"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Value for path column 'Version' has type Uint64, expected Utf8");

        TestS3ListingRequest(annoyingClient, {"1", "Bucket1", "/Photos", "1"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Tuple size 4 is greater that expected size 3");

        TestS3ListingRequest(annoyingClient, {"1", "Bucket1", "/Photos", "/"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Tuple size 4 is greater that expected size 3");

        TestS3ListingRequest(annoyingClient, {"1", "2", "3"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Value for path column 'Version' has type Uint64, expected Utf8");

        TestS3ListingRequest(annoyingClient, {"1", "2", "3", "4"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Tuple size 4 is greater that expected size 3");

        TestS3ListingRequest(annoyingClient, {"1", "2", "3", "4", "5"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Tuple size 5 is greater that expected size 3");

        TestS3ListingRequest(annoyingClient, {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid KeyPrefix: Tuple size 10 is greater that expected size 3");

        TestS3ListingRequest(annoyingClient, {"1"}, "/", "/", "", {"NonExistingColumn"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Unknown column 'NonExistingColumn'");

        TestS3ListingRequest(annoyingClient, {"1", "Bucket1"}, "/", "/", "abc", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid StartAfterKeySuffix: StartAfter parameter doesn't match PathPrefix");
    }

    Y_UNIT_TEST(Split) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));
        SetSplitMergePartCountLimit(cleverServer.GetRuntime(), -1);

        TFlatMsgBusClient annoyingClient(port);

        PrepareS3Data(annoyingClient);

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::MSGBUS_REQUEST, NActors::NLog::PRI_DEBUG);
//        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        TestS3ListingRequest(annoyingClient, {"100", "Bucket100"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_OK,
            "");

        // Split shard #1 (where Bucket100 is stored)
        TVector<ui64> shards = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        annoyingClient.SplitTablePartition("/dc-1/Dir/Table",
                "SourceTabletId: " + ToString(shards[1]) + " "
                "SplitBoundary { KeyPrefix { "
                "   Tuple { Optional { Uint64: 100 } } "
                "   Tuple { Optional { Text: 'Bucket100' } } "
                "   Tuple { Optional { Text: '/Vid' } } "
                "} }");

        TVector<ui64> shardsAfter = annoyingClient.GetTablePartitions("/dc-1/Dir/Table");
        UNIT_ASSERT_VALUES_EQUAL(shards.size() + 1, shardsAfter.size());

        TestS3ListingRequest(annoyingClient, {"100", "Bucket100"}, "/", "/", "", {"Path"}, 10,
            NMsgBusProxy::MSTATUS_OK,
            "");

        CompareS3Listing(annoyingClient, 100, "/", "/", "", 100500, {"Path"});
    }

    Y_UNIT_TEST(SuffixColumns) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServer cleverServer = TServer(TServerSettings(port));

        TFlatMsgBusClient annoyingClient(port);

        PrepareS3Data(annoyingClient);

        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Shoot to Thrill.mp3", 55, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Shoot to Thrill.mp3", 66, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Shoot to Thrill.mp3", 77, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Shoot to Thrill.mp3", 88, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Shoot to Thrill.mp3", 666, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/AC DC/Thunderstruck.mp3", 66, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/rock.m3u", 111, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/rock.m3u", 222, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/rock.m3u", 333, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana", 112, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana/Smeels Like Teen Spirit.mp3", 100, 10, "", "Table");
        S3WriteRow(annoyingClient, 50, "Bucket50", "Music/Nirvana/In Bloom.mp3", 120, 20, "", "Table");

        //
        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_TRACE);

        TestS3GenericListingRequest(annoyingClient, {"50", "Bucket50"}, "Music/AC DC/", "/", {"Music/AC DC/Shoot to Thrill.mp3", "66"}, {"Path", "Version", "Data"}, 10, 10000,
            NMsgBusProxy::MSTATUS_OK,
            "");

        TestS3GenericListingRequest(annoyingClient, {"50", "Bucket50"}, "Music/AC DC/", "/", {"Music/AC DC/Shoot to Thrill.mp3"}, {"Path", "Version", "Timestamp"}, 10, 10000,
            NMsgBusProxy::MSTATUS_OK,
            "");

        TestS3GenericListingRequest(annoyingClient, {"50", "Bucket50"}, "Music/AC DC/", "/", {"Music/AC DC/Shoot to Thrill.mp3", "66", "abcd"}, {"Path", "Version"}, 10, 10000,
            NMsgBusProxy::MSTATUS_ERROR,
            "Invalid StartAfterKeySuffix: Tuple size 3 is greater that expected size 2");
    }

    Y_UNIT_TEST(ManyDeletes) {
        TPortManager pm;
        ui16 port = pm.GetPort(2134);
        TServerSettings settings(port);
        settings.NodeCount = 1;
        TServer cleverServer = TServer(TServerSettings(port));

        // Disable shared cache to trigger restarts
        TAtomic unused = 42;
        cleverServer.GetRuntime()->GetAppData().Icb->SetValue("SharedPageCache_Size", 10, unused);
        cleverServer.GetRuntime()->GetAppData().Icb->SetValue("SharedPageCache_Size", 10, unused);
        UNIT_ASSERT_VALUES_EQUAL(unused, 10);

        TFlatMsgBusClient annoyingClient(port);

        PrepareS3Data(annoyingClient);

#ifdef NDEBUG
        const int N_ROWS = 10000;
#else
        const int N_ROWS = 5000;
#endif

        TString bigData(300, 'a');

        for (int i = 0; i < N_ROWS; ++i) {
            S3WriteRow(annoyingClient, 100, "Bucket100", "/A/Santa Barbara " + ToString(i), 1, 1100, bigData, "Table");
            S3WriteRow(annoyingClient, 100, "Bucket100", "/B/Santa Barbara " + ToString(i%4000), 1, 1100, bigData, "Table");
            S3WriteRow(annoyingClient, 100, "Bucket100", "/C/Santa Barbara " + ToString(i), 1, 1100, bigData, "Table");
            S3WriteRow(annoyingClient, 100, "Bucket100", "/D/Santa Barbara " + ToString(i), 1, 1100, bigData, "Table");
            if (i % 100 == 0)
                Cerr << ".";
        }
        Cerr << "\n";

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        CompareS3Listing(annoyingClient, 100, "/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/A/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/B/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/P/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/Photos/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/Videos/", "/", "", 1000, {});

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_ERROR);

        for (int i = 0; i < N_ROWS/2; ++i) {
            S3DeleteRow(annoyingClient, 100, "Bucket100", "/A/Santa Barbara " + ToString(i), 1, "Table");
            S3DeleteRow(annoyingClient, 100, "Bucket100", "/B/Santa Barbara " + ToString(i), 1, "Table");
            S3DeleteRow(annoyingClient, 100, "Bucket100", "/C/Santa Barbara " + ToString(i), 1, "Table");
            S3DeleteRow(annoyingClient, 100, "Bucket100", "/D/Santa Barbara " + ToString(i), 1, "Table");
            if (i % 100 == 0)
                Cerr << ".";
        }
        Cerr << "\n";

        cleverServer.GetRuntime()->SetLogPriority(NKikimrServices::TX_DATASHARD, NActors::NLog::PRI_DEBUG);

        CompareS3Listing(annoyingClient, 100, "/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/A/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/B/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/P/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/Photos/", "/", "", 1000, {});
        CompareS3Listing(annoyingClient, 100, "/Videos/", "/", "", 1000, {});
    }
}

}}
