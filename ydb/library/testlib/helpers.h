#pragma once

#include <library/cpp/testing/unittest/registar.h>

#define Y_UNIT_TEST_TWIN(N, OPT)                                                                                   \
    template <bool OPT>                                                                                            \
    struct TTestCase##N : public TCurrentTestCase {                                                                \
        TTestCase##N() : TCurrentTestCase() {                                                                      \
            if constexpr (OPT) { Name_ = #N "+" #OPT; } else { Name_ = #N "-" #OPT; }                              \
        }                                                                                                          \
        static THolder<NUnitTest::TBaseTestCase> CreateOn()  { return ::MakeHolder<TTestCase##N<true>>();  }       \
        static THolder<NUnitTest::TBaseTestCase> CreateOff() { return ::MakeHolder<TTestCase##N<false>>(); }       \
        void Execute_(NUnitTest::TTestContext&) override;                                                          \
    };                                                                                                             \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(TTestCase##N<true>::CreateOn);                                                   \
            TCurrentTest::AddTest(TTestCase##N<false>::CreateOff);                                                 \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template <bool OPT>                                                                                            \
    void TTestCase##N<OPT>::Execute_(NUnitTest::TTestContext& ut_context Y_DECLARE_UNUSED)

#define Y_UNIT_TEST_QUAD(N, OPT1, OPT2)                                                                                              \
    template<bool OPT1, bool OPT2> void N(NUnitTest::TTestContext&);                                                                 \
    struct TTestRegistration##N {                                                                                                    \
        TTestRegistration##N() {                                                                                                     \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false); \
            TCurrentTest::AddTest(#N "+" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);  \
            TCurrentTest::AddTest(#N "-" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, true>), false);   \
        }                                                                                                                            \
    };                                                                                                                               \
    static TTestRegistration##N testRegistration##N;                                                                                 \
    template<bool OPT1, bool OPT2>                                                                                                   \
    void N(NUnitTest::TTestContext&)

// Y_UNIT_TEST_SUITE_TWIN allows to create a test suite that runs twice - 
// once with a boolean flag set to true, and once with it set to false.
// All tests in the suite can access the flag value via TCurrentTest::OPT.
//
// This is useful for testing code that has conditional behavior based on a feature flag.
//
// Usage:
//   #define MY_SUITE_TESTS(FlagName) \
//       Y_UNIT_TEST(TestFeatureBehavior) { \
//           if (TCurrentTest::FlagName) { \
//               /* Test with feature enabled */ \
//               UNIT_ASSERT(true); \
//           } else { \
//               /* Test with feature disabled */ \
//               UNIT_ASSERT(true); \
//           } \
//       } \
//       Y_UNIT_TEST(AnotherTest) { \
//           /* All tests can access TCurrentTest::FlagName */ \
//           bool featureEnabled = TCurrentTest::FlagName; \
//           /* ... */ \
//       }
//   
//   Y_UNIT_TEST_SUITE_TWIN(FeatureTestSuite, UseNewAlgorithm, MY_SUITE_TESTS)
//
// This will create two test suites:
//   - FeatureTestSuite+UseNewAlgorithm (with UseNewAlgorithm=true)
//   - FeatureTestSuite-UseNewAlgorithm (with UseNewAlgorithm=false)
//
// Note: Test definitions must be provided as a macro that takes the flag name as a parameter.
// This is required because the C preprocessor needs to expand the tests into both suite variants.
#define Y_UNIT_TEST_SUITE_TWIN(N, OPT, BODY)                                                                           \
    namespace NTestSuite##N##On {                                                                                      \
        class TCurrentTestCase: public ::NUnitTest::TBaseTestCase {                                                    \
        };                                                                                                             \
        class TCurrentTest: public TTestBase {                                                                         \
        private:                                                                                                       \
            typedef std::function<THolder<NUnitTest::TBaseTestCase>()> TTestCaseFactory;                               \
            typedef TVector<TTestCaseFactory> TTests;                                                                  \
                                                                                                                       \
            static TTests& Tests() {                                                                                   \
                static TTests tests;                                                                                   \
                return tests;                                                                                          \
            }                                                                                                          \
                                                                                                                       \
        public:                                                                                                        \
            static constexpr bool OPT = true;                                                                          \
            static TString StaticName() {                                                                              \
                return TString(#N) + "+" #OPT;                                                                         \
            }                                                                                                          \
            virtual TString Name() const noexcept {                                                                    \
                return StaticName();                                                                                   \
            }                                                                                                          \
                                                                                                                       \
            static void AddTest(const char* name,                                                                      \
                const std::function<void(NUnitTest::TTestContext&)>& body, bool forceFork)                             \
            {                                                                                                          \
                Tests().emplace_back([=]{ return MakeHolder<NUnitTest::TBaseTestCase>(name, body, forceFork); });      \
            }                                                                                                          \
                                                                                                                       \
            static void AddTest(TTestCaseFactory testCaseFactory) {                                                    \
                Tests().push_back(std::move(testCaseFactory));                                                         \
            }                                                                                                          \
                                                                                                                       \
            virtual void Execute() {                                                                                   \
                this->AtStart();                                                                                       \
                this->GlobalSuiteSetUp();                                                                              \
                for (TTests::iterator it = Tests().begin(), ie = Tests().end(); it != ie; ++it) {                      \
                    const auto i = (*it)();                                                                            \
                    if (!this->CheckAccessTest(i->Name_)) {                                                            \
                        continue;                                                                                      \
                    }                                                                                                  \
                    NUnitTest::TTestContext context(this->TTestBase::Processor());                                     \
                    try {                                                                                              \
                        this->BeforeTest(i->Name_);                                                                    \
                        {                                                                                              \
                            TCleanUp cleaner(this);                                                                    \
                            auto testCase = [this, &i, &context] {                                                     \
                                Y_DEFER {                                                                              \
                                    try {                                                                              \
                                        i->TearDown(context);                                                          \
                                    } catch (const ::NUnitTest::TAssertException&) {                                   \
                                    } catch (const yexception& e) {                                                    \
                                        CATCH_REACTION_BT(i->Name_, e, &context);                                      \
                                    } catch (const std::exception& e) {                                                \
                                        CATCH_REACTION(i->Name_, e, &context);                                         \
                                    } catch (...) {                                                                    \
                                        this->AddError("non-std exception!", &context);                                \
                                    }                                                                                  \
                                };                                                                                     \
                                i->SetUp(context);                                                                     \
                                i->Execute_(context);                                                                  \
                            };                                                                                         \
                            this->TTestBase::Run(testCase, StaticName(), i->Name_, i->ForceFork_);                     \
                        }                                                                                              \
                    } catch (const ::NUnitTest::TAssertException&) {                                                   \
                    } catch (const yexception& e) {                                                                    \
                        CATCH_REACTION_BT(i->Name_, e, &context);                                                      \
                    } catch (const std::exception& e) {                                                                \
                        CATCH_REACTION(i->Name_, e, &context);                                                         \
                    } catch (...) {                                                                                    \
                        this->AddError("non-std exception!", &context);                                                \
                    }                                                                                                  \
                    this->Finish(i->Name_, &context);                                                                  \
                }                                                                                                      \
                this->GlobalSuiteTearDown();                                                                           \
                this->AtEnd();                                                                                         \
            }                                                                                                          \
        };                                                                                                             \
        UNIT_TEST_SUITE_REGISTRATION(TCurrentTest)                                                                     \
        BODY(OPT)                                                                                                      \
    }                                                                                                                  \
    namespace NTestSuite##N##Off {                                                                                     \
        class TCurrentTestCase: public ::NUnitTest::TBaseTestCase {                                                    \
        };                                                                                                             \
        class TCurrentTest: public TTestBase {                                                                         \
        private:                                                                                                       \
            typedef std::function<THolder<NUnitTest::TBaseTestCase>()> TTestCaseFactory;                               \
            typedef TVector<TTestCaseFactory> TTests;                                                                  \
                                                                                                                       \
            static TTests& Tests() {                                                                                   \
                static TTests tests;                                                                                   \
                return tests;                                                                                          \
            }                                                                                                          \
                                                                                                                       \
        public:                                                                                                        \
            static constexpr bool OPT = false;                                                                         \
            static TString StaticName() {                                                                              \
                return TString(#N) + "-" #OPT;                                                                         \
            }                                                                                                          \
            virtual TString Name() const noexcept {                                                                    \
                return StaticName();                                                                                   \
            }                                                                                                          \
                                                                                                                       \
            static void AddTest(const char* name,                                                                      \
                const std::function<void(NUnitTest::TTestContext&)>& body, bool forceFork)                             \
            {                                                                                                          \
                Tests().emplace_back([=]{ return MakeHolder<NUnitTest::TBaseTestCase>(name, body, forceFork); });      \
            }                                                                                                          \
                                                                                                                       \
            static void AddTest(TTestCaseFactory testCaseFactory) {                                                    \
                Tests().push_back(std::move(testCaseFactory));                                                         \
            }                                                                                                          \
                                                                                                                       \
            virtual void Execute() {                                                                                   \
                this->AtStart();                                                                                       \
                this->GlobalSuiteSetUp();                                                                              \
                for (TTests::iterator it = Tests().begin(), ie = Tests().end(); it != ie; ++it) {                      \
                    const auto i = (*it)();                                                                            \
                    if (!this->CheckAccessTest(i->Name_)) {                                                            \
                        continue;                                                                                      \
                    }                                                                                                  \
                    NUnitTest::TTestContext context(this->TTestBase::Processor());                                     \
                    try {                                                                                              \
                        this->BeforeTest(i->Name_);                                                                    \
                        {                                                                                              \
                            TCleanUp cleaner(this);                                                                    \
                            auto testCase = [this, &i, &context] {                                                     \
                                Y_DEFER {                                                                              \
                                    try {                                                                              \
                                        i->TearDown(context);                                                          \
                                    } catch (const ::NUnitTest::TAssertException&) {                                   \
                                    } catch (const yexception& e) {                                                    \
                                        CATCH_REACTION_BT(i->Name_, e, &context);                                      \
                                    } catch (const std::exception& e) {                                                \
                                        CATCH_REACTION(i->Name_, e, &context);                                         \
                                    } catch (...) {                                                                    \
                                        this->AddError("non-std exception!", &context);                                \
                                    }                                                                                  \
                                };                                                                                     \
                                i->SetUp(context);                                                                     \
                                i->Execute_(context);                                                                  \
                            };                                                                                         \
                            this->TTestBase::Run(testCase, StaticName(), i->Name_, i->ForceFork_);                     \
                        }                                                                                              \
                    } catch (const ::NUnitTest::TAssertException&) {                                                   \
                    } catch (const yexception& e) {                                                                    \
                        CATCH_REACTION_BT(i->Name_, e, &context);                                                      \
                    } catch (const std::exception& e) {                                                                \
                        CATCH_REACTION(i->Name_, e, &context);                                                         \
                    } catch (...) {                                                                                    \
                        this->AddError("non-std exception!", &context);                                                \
                    }                                                                                                  \
                    this->Finish(i->Name_, &context);                                                                  \
                }                                                                                                      \
                this->GlobalSuiteTearDown();                                                                           \
                this->AtEnd();                                                                                         \
            }                                                                                                          \
        };                                                                                                             \
        UNIT_TEST_SUITE_REGISTRATION(TCurrentTest)                                                                     \
        BODY(OPT)                                                                                                      \
    }


