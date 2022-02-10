#include <library/cpp/tvmauth/src/parser.h>
#include <library/cpp/tvmauth/src/utils.h>

#include <library/cpp/tvmauth/exception.h>
#include <library/cpp/tvmauth/ticket_status.h>

#include <library/cpp/testing/unittest/registar.h>

Y_UNIT_TEST_SUITE(ParserTestSuite) { 
    using namespace NTvmAuth;

    Y_UNIT_TEST(Keys) { 
        UNIT_ASSERT_EXCEPTION(TParserTvmKeys::ParseStrV1("2:asds"), TMalformedTvmKeysException);
        UNIT_ASSERT_EXCEPTION(TParserTvmKeys::ParseStrV1("3:asds"), TMalformedTvmKeysException);
        UNIT_ASSERT_EXCEPTION(TParserTvmKeys::ParseStrV1("1:+a/sds"), TMalformedTvmKeysException);

        UNIT_ASSERT_VALUES_EQUAL("sdsd", NUtils::Bin2base64url(TParserTvmKeys::ParseStrV1("1:sdsd")));
    }

    Y_UNIT_TEST(TicketsStrV3) { 
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Ok,
                                                   NUtils::Base64url2bin("CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg"),
                                                   NUtils::Base64url2bin("ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA"),
                                                   "3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:"}),
                          TParserTickets::ParseStrV3("3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::UnsupportedVersion,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("2:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::InvalidTicketType,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::UserFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("3:serv::ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA:asd",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("3:serv:CgY+-*/IDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERme/*-+H_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("",
                                                     TParserTickets::ServiceFlag()));
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Malformed,
                                                   {},
                                                   {},
                                                   {}}),
                          TParserTickets::ParseStrV3("'",
                                                     TParserTickets::ServiceFlag()));

        // Invalid proto
        UNIT_ASSERT_EQUAL(TParserTickets::TStrRes({ETicketStatus::Ok,
                                                   NUtils::Base64url2bin("YIDRCUkQYBgcIgdiYjpzZXNzIghiYjpzZXNzMg"),
                                                   NUtils::Base64url2bin("ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA"),
                                                   "3:serv:YIDRCUkQYBgcIgdiYjpzZXNzIghiYjpzZXNzMg:"}),
                          TParserTickets::ParseStrV3("3:serv:YIDRCUkQYBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                     TParserTickets::ServiceFlag()));
    }

    Y_UNIT_TEST(TicketsV3) { 
        NRw::TPublicKeys pub;

        UNIT_ASSERT_EQUAL(ETicketStatus::Malformed,
                          TParserTickets::ParseV3("3:serv:CgYIDRCUkQYQDBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERme/*-+H_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);

        // Invalid proto
        UNIT_ASSERT_EQUAL(ETicketStatus::Malformed,
                          TParserTickets::ParseV3("3:serv:YIDRCUkQYBgcIgdiYjpzZXNzIghiYjpzZXNzMg:ERmeH_yzC7K_QsoHTyw7llCsyExEz3CoEopPIuivA0ZAtTaFq_Pa0l9Fhhx_NX9WpOp2CPyY5cFc4PXhcO83jCB7-EGvHNxGN-j2NQalERzPiKqkDCO0Q5etLzSzrfTlvMz7sXDvELNBHyA0PkAQnbz4supY0l-0Q6JBYSEF3zOVMjjE-HeQIFL3ats3_PakaUMWRvgQQ88pVdYZqAtbDw9PlTla7ommygVZQjcfNFXV1pJKRgOCLs-YyCjOJHLKL04zYj0X6KsOCTUeqhj7ml96wLZ-g1X9tyOR2WAr2Ctq7wIEHwqhxOLgOSKqm05xH6Vi3E_hekf50oe2jPfKEA",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);

        // Expire time == 100500
        UNIT_ASSERT_EQUAL(ETicketStatus::Expired,
                          TParserTickets::ParseV3("3:serv:CBAQlJEGIhcIDBAcGgdiYjpzZXNzGghiYjpzZXNzMg:HEzPbsjULegBvgX3nqwFX0GfVhESmN1kEWyeT7U03KAR-sQnNYgm6IuN-b9-lQYQKAJSW6p8ffyucC1yDrWSWRxXVzHJUxAVW4hnbiFDtXrurnEdpMK3izKbmTY25PJ4vH3_TkRXk-_oSAE8RvIFKXlh-aw1tezbXBUpJKvyJ0w",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);

        UNIT_ASSERT_EQUAL(ETicketStatus::MissingKey,
                          TParserTickets::ParseV3("3:serv:CBAQ__________9_IhcIDBAcGgdiYjpzZXNzGghiYjpzZXNzMg:OKjKEbygehEZWH0XEeLzvf0q0aS0VvSk_CKSXGdpqxPbE4RzU70jeM-X9rXVpbYjt76VgBLlBpumJdyiclulfGPDPiL8nwJuu8AnWIR_o-QqyXbsloo2_syE6w2aYw2Yw_5_qjnipYdxGUWegHAGCj3yeMde6O2BmNZ0OCfg6qU",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);

        pub.emplace(16, NRw::TRwPublicKey(NUtils::Base64url2bin("MIGEAoGBALhrihbf3EpjDQS2sCQHazoFgN0nBbE9eesnnFTfzQELXb2gnJU9enmV_aDqaHKjgtLIPpCgn40lHrn5k6mvH5OdedyI6cCzE-N-GFp3nAq0NDJyMe0fhtIRD__CbT0ulcvkeow65ubXWfw6dBC2gR_34rdMe_L_TGRLMWjDULbN")));
        UNIT_ASSERT_EQUAL(ETicketStatus::SignBroken,
                          TParserTickets::ParseV3("3:serv:CBAQ__________9_IhcIDBAcGgdiYjpzZXNzGghiYjpzZXNzMa:OKjKEbygehEZWH0XEeLzvf0q0aS0VvSk_CKSXGdpqxPbE4RzU70jeM-X9rXVpbYjt76VgBLlBpumJdyiclulfGPDPiL8nwJuu8AnWIR_o-QqyXbsloo2_syE6w2aYw2Yw_5_qjnipYdxGUWegHAGCj3yeMde6O2BmNZ0OCfg6qU",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);
        UNIT_ASSERT_EQUAL(ETicketStatus::SignBroken,
                          TParserTickets::ParseV3("3:serv:CBAQ__________9_IhcIDBAcGgdiYjpzZXNzGghiYjpzZXNzMg:OKjKEbygehEZWH0XEeLzvf0q0aS0VvSk_CKSXGdpqxPbE4RzU70jeM-X9rXVpbYjt76VgBLlBpumJdyiclulfGPDPiL8nwJuu8AnWIR_o-QqyXbsloo2_syE6w2aYw2Yw_5_qjnipYdxGUWegHAGCj3yeMde6O2BmNZ0OCfg6qa",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);
        UNIT_ASSERT_EQUAL(ETicketStatus::SignBroken,
                          TParserTickets::ParseV3("3:serv:CBAQ__________9_IhcIDBAcGgdiYjpzZXNzGghiYjpzZXNzMg:EbygehEZWH0XEeLzvf0q0aS0VvSk_CKSXGdpqxPbE4RzU70jeM-X9rXVpbYjt76VgBLlBpumJdyiclulfGPDPiL8nwJuu8AnWIR_o-QqyXbsloo2_syE6w2aYw2Yw_5_qjnipYdxGUWegHAGCj3yeMde6O2BmNZ0OCfg6qU",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);

        UNIT_ASSERT_EQUAL(ETicketStatus::Ok,
                          TParserTickets::ParseV3("3:serv:CBAQ__________9_IhcIDBAcGgdiYjpzZXNzGghiYjpzZXNzMg:OKjKEbygehEZWH0XEeLzvf0q0aS0VvSk_CKSXGdpqxPbE4RzU70jeM-X9rXVpbYjt76VgBLlBpumJdyiclulfGPDPiL8nwJuu8AnWIR_o-QqyXbsloo2_syE6w2aYw2Yw_5_qjnipYdxGUWegHAGCj3yeMde6O2BmNZ0OCfg6qU",
                                                  pub,
                                                  TParserTickets::ServiceFlag())
                              .Status);
    }
}
